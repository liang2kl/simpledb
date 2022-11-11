#include "internal/Index.h"

#include <queue>

#include "internal/Comparer.h"
#include "internal/Logger.h"

namespace SimpleDB {
namespace Internal {

Index::~Index() { close(); }

void Index::open(const std::string &file) {
    Logger::log(VERBOSE, "Index: initializing index from %s\n", file.c_str());

    if (initialized) {
        Logger::log(WARNING,
                    "Index: index is already initailized, but attempting to "
                    "re-initialize from %s\n",
                    file.c_str());
    }

    try {
        fd = PF::open(file);
        PageHandle handle = PF::getHandle(fd, 0);
        meta = *PF::loadRaw<IndexMeta *>(handle);
    } catch (BaseError) {
        Logger::log(ERROR, "Index: fail to read index metadata from file %d\n",
                    fd.value);
        throw Internal::ReadIndexError();
    }

    if (meta.headCanary != INDEX_META_CANARY ||
        meta.tailCanary != INDEX_META_CANARY) {
        Logger::log(ERROR,
                    "Index: fail to read index metadata from file %d: "
                    "invalid canary values\n",
                    fd.value);
        throw Internal::ReadIndexError();
    }

    initialized = true;
}

void Index::create(const std::string &file, ColumnMeta column) {
    try {
        PF::create(file);
        fd = PF::open(file);
    } catch (Internal::FileExistsError) {
        Logger::log(
            WARNING,
            "Index: creating an empty index to file %s that already exists\n",
            file.c_str());
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to create table to %s\n", file.c_str());
        throw Internal::CreateIndexError();
    }

    if (meta.type == VARCHAR) {
        Logger::log(ERROR, "Index: index on varchar is not supported\n");
        throw Internal::InvalidIndexTypeError();
    }

    meta.type = column.type;
    meta.size = column.size;
    meta.numNode = 0;
    meta.numEntry = 0;
    meta.firstFreePage = 1;

    // Create root node.
    NodeIndex index = createNewLeafNode(IndexNode::NULL_NODE);
    meta.rootNode = index;

    initialized = true;
}

void Index::close() {
    if (!initialized) {
        return;
    }

    Logger::log(VERBOSE, "Index: closing index\n");

    flushMeta();

    PF::close(fd);
    initialized = false;
    // TODO: More cleanup
}

void Index::insert(const char *key, RecordID id) {
    Logger::log(VERBOSE,
                "Index: inserting index (key: %hhx-%hhx-%hhx-%hhx) at page %d, "
                "slot %d\n",
                key[0], key[1], key[2], key[3], id.page, id.slot);

    checkInit();

    auto result = findNode(key);
    NodeIndex nodeIndex = std::get<0>(result);
    bool found = std::get<2>(result);

    if (found) {
        // The record already exists.
        throw Internal::IndexKeyExistsError();
    }

    PageHandle handle = PF::getHandle(fd, nodeIndex);
    IndexNode *node = PF::loadRaw<IndexNode *>(handle);

    // First insert the record.
    IndexEntry entry;
    memcpy(entry.key, key, 4);  // FIXME: Hard code size.
    entry.record = id;

    // Insert the record into the node.
    insertEntry(node, entry);

    // Check if the insertion breaks the constraints. If so, split recusively to
    // fix them.
    checkOverflowFrom(nodeIndex);

    // Renew the handle and mark dirty.
    handle = PF::renew(handle);
    PF::markDirty(handle);

    meta.numEntry++;
}

void Index::remove(const char *key) {
    Logger::log(VERBOSE, "Index: removing record %hhx-%hhx-%hhx-%hhx\n", key[0],
                key[1], key[2], key[3]);
    checkInit();

    auto [nodeIndex, index, found] = findNode(key);

    if (!found) {
        throw Internal::IndexKeyNotExistsError();
    }

    PageHandle handle = PF::getHandle(fd, nodeIndex);
    IndexNode *node = PF::loadRaw<IndexNode *>(handle);

    // Remove the entry.
    // First, move the entry to the leaf node.
    if (!node->isLeaf()) {
        NodeIndex targetNodeIndex = node->children[index + 1];
        PageHandle targetHandle = getHandle(targetNodeIndex);
        IndexNode *targetNode = PF::loadRaw<IndexNode *>(targetHandle);
        while (!targetNode->isLeaf()) {
            targetNodeIndex = targetNode->children[0];
            targetNode = PF::loadRaw<IndexNode *>(getHandle(targetNodeIndex));
        }
        // Swap the entries (we don't actually care about the target entry).
        node->entry[index] = targetNode->entry[0];
        PF::markDirty(handle);

        node = targetNode;
        nodeIndex = targetNodeIndex;
        index = 0;
    }

    assert(node->isLeaf());

    // Remove the entry.
    for (int i = index; i < node->numEntry - 1; i++) {
        node->entry[i] = node->entry[i + 1];
    }
    node->numEntry--;

    // Solve underflow.
    checkUnderflowFrom(nodeIndex);

    // Mark dirty.
    PF::markDirty(getHandle(nodeIndex));

    meta.numEntry--;
}

RecordID Index::find(const char *key) {
    Logger::log(VERBOSE, "Index: finding record %hhx-%hhx-%hhx-%hhx\n", key[0],
                key[1], key[2], key[3]);
    checkInit();

    // TODO: Pin the root node.

    auto [nodeIndex, index, found] = findNode(key);

    if (!found) {
        return RecordID::NULL_RECORD;
    }

    PageHandle handle = PF::getHandle(fd, nodeIndex);
    IndexNode *node = PF::loadRaw<IndexNode *>(handle);
    return node->entry[index].record;
}

std::tuple<Index::NodeIndex, int, bool> Index::findNode(const char *key) {
    // Start from the root node.
    int currentNode = meta.rootNode;

    for (;;) {
        PageHandle handle = PF::getHandle(fd, currentNode);
        IndexNode *node = PF::loadRaw<IndexNode *>(handle);

        int nextChild = node->numEntry;

        for (int i = 0; i < node->numEntry; i++) {
            if (node->entry[i].eq(key, meta.type)) {
                // Found the exact node.
                return {currentNode, i, true};
            }
            if (node->entry[i].gt(key, meta.type)) {
                nextChild = i;
                break;
            }
        }

        if (node->isLeaf()) {
            // The leaf.
            return {currentNode, nextChild, false};
        }

        currentNode = node->children[nextChild];
    }

    assert(false);
    return {-1, -1, false};
}

Index::NodeIndex Index::createNewLeafNode(int parent) {
    meta.numNode++;

    // Get and update the free page.
    NodeIndex index = meta.firstFreePage;
    PageHandle handle = PF::getHandle(fd, index);
    EmptyPageMeta *pageMeta = PF::loadRaw<EmptyPageMeta *>(handle);

    if (pageMeta->headCanary == EMPTY_INDEX_PAGE_CANARY &&
        pageMeta->tailCanary == EMPTY_INDEX_PAGE_CANARY) {
        // A valid empty page meta.
        meta.firstFreePage = pageMeta->nextPage;
    } else {
        // Just bump the `nextFreePage`.
        meta.firstFreePage++;
    }

    IndexNode *node = PF::loadRaw<IndexNode *>(handle);
    node->numChildren = 0;
    node->numEntry = 0;
    node->parent = parent;
    node->index = index;

    PF::markDirty(handle);

    return index;
}

int Index::insertEntry(IndexNode *node, const IndexEntry &entry) {
    // Find the insert position.
    int index = node->numEntry;
    for (int i = 0; i < node->numEntry; i++) {
        if (node->entry[i].gt(entry.key, meta.type)) {
            index = i;
            break;
        }
    }

    // Shift the entries.
    for (int i = node->numEntry; i > index; i--) {
        node->entry[i] = node->entry[i - 1];
    }

    node->entry[index] = entry;
    node->numEntry++;

    return index;
}

void Index::checkOverflowFrom(Index::NodeIndex index) {
    PageHandle nodeHandle = getHandle(index);
    IndexNode *node = PF::loadRaw<IndexNode *>(nodeHandle);

    if (node->numEntry <= MAX_NUM_ENTRY_PER_NODE) {
        return;
    }

#ifdef _DEBUG
    if (!node->isLeaf()) {
        assert(node->numChildren == node->numEntry + 1);
    }
#endif

    // Select a victim entry at the middle.
    int victimIndex = node->numEntry / 2;
    IndexEntry victimEntry = node->entry[victimIndex];
    // Split the node.
    NodeIndex siblingIndex = createNewLeafNode(node->parent);
    PageHandle siblingHandle = getHandle(siblingIndex);
    IndexNode *siblingNode = PF::loadRaw<IndexNode *>(siblingHandle);

    // Move the "right" entries to the sibling node.
    siblingNode->numEntry = node->numEntry - victimIndex - 1;
    memcpy(siblingNode->entry, &node->entry[victimIndex + 1],
           sizeof(IndexEntry) * siblingNode->numEntry);
    node->numEntry = victimIndex;

    // Move the children.
    if (!node->isLeaf()) {
        siblingNode->numChildren = node->numChildren - victimIndex - 1;
        memcpy(siblingNode->children, &node->children[victimIndex + 1],
               sizeof(NodeIndex) * siblingNode->numChildren);
        node->numChildren = victimIndex + 1;

        // Update the parent of the children.
        // FIXME: This requires lots of IO.
        for (int i = 0; i < siblingNode->numChildren; i++) {
            PageHandle childHandle = getHandle(siblingNode->children[i]);
            IndexNode *childNode = PF::loadRaw<IndexNode *>(childHandle);
            childNode->parent = siblingIndex;
            PF::markDirty(childHandle);
        }

#ifdef _DEBUG
        assert(node->numChildren == node->numEntry + 1);
        assert(siblingNode->numChildren == siblingNode->numEntry + 1);
#endif
    }

    // Insert the victim entry into the parent node.

    // The victim entry is all the root node.
    if (node->parent == IndexNode::NULL_NODE) {
        NodeIndex newRootIndex = createNewLeafNode(IndexNode::NULL_NODE);
        PageHandle newRootHandle = getHandle(newRootIndex);
        IndexNode *newRootNode = PF::loadRaw<IndexNode *>(newRootHandle);
        newRootNode->numEntry = 1;
        newRootNode->numChildren = 2;
        newRootNode->entry[0] = victimEntry;
        newRootNode->children[0] = index;
        newRootNode->children[1] = siblingIndex;

        // Don't forget to update the parent of the children.
        node->parent = newRootIndex;
        siblingNode->parent = newRootIndex;

        // Mark dirty.
        PF::markDirty(siblingHandle);
        PF::markDirty(nodeHandle);
        PF::markDirty(newRootHandle);

        // And the meta...
        meta.rootNode = newRootIndex;

        return;
    }

    // The parent node exists.
    PageHandle parentHandle = PF::getHandle(fd, node->parent);
    IndexNode *parentNode = PF::loadRaw<IndexNode *>(parentHandle);
    int insertIndex = insertEntry(parentNode, victimEntry);

    // Update the children of the parent node.
    for (int i = parentNode->numChildren; i > insertIndex + 1; i--) {
        parentNode->children[i] = parentNode->children[i - 1];
    }
    parentNode->children[insertIndex + 1] = siblingNode->index;
    parentNode->numChildren++;

    // Mark dirty.
    PF::markDirty(siblingHandle);
    PF::markDirty(nodeHandle);
    PF::markDirty(parentHandle);

    // Check recursively if the parent node overflows.
    checkOverflowFrom(node->parent);
}

void Index::checkUnderflowFrom(NodeIndex index) {
    PageHandle nodeHandle = getHandle(index);
    IndexNode *node = PF::loadRaw<IndexNode *>(nodeHandle);

    if (node->numEntry >= MIN_NUM_ENTRY_PER_NODE ||
        node->parent == IndexNode::NULL_NODE) {
        return;
    }

    // Find the index of the node in the parent node.
    PageHandle parentHandle = getHandle(node->parent);
    IndexNode *parentNode = PF::loadRaw<IndexNode *>(parentHandle);
    int indexInParent = -1;
    for (int i = 0; i < parentNode->numChildren; i++) {
        if (parentNode->children[i] == index) {
            indexInParent = i;
            break;
        }
    }

    assert(indexInParent >= 0);

    if (indexInParent > 0) {
        // Got a left sibling.
        PageHandle leftSiblingHandle =
            getHandle(parentNode->children[indexInParent - 1]);
        IndexNode *leftSiblingNode =
            PF::loadRaw<IndexNode *>(leftSiblingHandle);

        if (leftSiblingNode->numEntry > MIN_NUM_ENTRY_PER_NODE) {
            // Transfer an entry from the parent to the node.
            insertEntry(node, parentNode->entry[indexInParent - 1]);
            // Transfer an entry from the left sibling to the parent.
            parentNode->entry[indexInParent - 1] =
                leftSiblingNode->entry[leftSiblingNode->numEntry - 1];
            leftSiblingNode->numEntry--;

            // Transfer the rightmost child of the left sibling to the node.
            if (!leftSiblingNode->isLeaf()) {
                // First, shift the children of the node.
                for (int i = node->numChildren; i > 0; i--) {
                    node->children[i] = node->children[i - 1];
                }
                node->children[0] =
                    leftSiblingNode->children[leftSiblingNode->numChildren - 1];
                node->numChildren++;
                leftSiblingNode->numChildren--;
                // Update the parent of the child.
                PageHandle childHandle = getHandle(node->children[0]);
                IndexNode *childNode = PF::loadRaw<IndexNode *>(childHandle);
                childNode->parent = node->index;
                PF::markDirty(childHandle);
            }

            // Mark dirty.
            PF::markDirty(leftSiblingHandle);
            PF::markDirty(nodeHandle);
            PF::markDirty(parentHandle);

            return;
        }
    }

    assert(parentHandle.validate());
    assert(nodeHandle.validate());

    if (indexInParent < parentNode->numChildren - 1) {
        // Got a right sibling.
        PageHandle rightSiblingHandle =
            getHandle(parentNode->children[indexInParent + 1]);
        IndexNode *rightSiblingNode =
            PF::loadRaw<IndexNode *>(rightSiblingHandle);

        if (rightSiblingNode->numEntry > MIN_NUM_ENTRY_PER_NODE) {
            // Transfer an entry from the parent to the node.
            insertEntry(node, parentNode->entry[indexInParent]);
            // Transfer an entry from the right sibling to the parent.
            parentNode->entry[indexInParent] = rightSiblingNode->entry[0];
            // Shift the entries of the right sibling.
            for (int i = 0; i < rightSiblingNode->numEntry - 1; i++) {
                rightSiblingNode->entry[i] = rightSiblingNode->entry[i + 1];
            }
            rightSiblingNode->numEntry--;

            // Transfer the leftmost child of the right sibling to the node.
            if (!rightSiblingNode->isLeaf()) {
                node->children[node->numChildren] =
                    rightSiblingNode->children[0];
                node->numChildren++;
                // Shift the children of the right sibling.
                for (int i = 0; i < rightSiblingNode->numChildren - 1; i++) {
                    rightSiblingNode->children[i] =
                        rightSiblingNode->children[i + 1];
                }
                rightSiblingNode->numChildren--;
                // Update the parent of the child.
                PageHandle childHandle =
                    getHandle(node->children[node->numChildren - 1]);
                IndexNode *childNode = PF::loadRaw<IndexNode *>(childHandle);
                childNode->parent = node->index;
                PF::markDirty(childHandle);
            }

            // Mark dirty.
            PF::markDirty(nodeHandle);
            PF::markDirty(parentHandle);
            PF::markDirty(rightSiblingHandle);

            return;
        }
    }

    assert(parentHandle.validate());
    assert(nodeHandle.validate());

    if (indexInParent > 0) {
        // Collapse with the left sibling.
        collapse(parentNode->children[indexInParent - 1], index,
                 indexInParent - 1);
    } else if (indexInParent < parentNode->numChildren - 1) {
        // Collapse with the right sibling.
        collapse(index, parentNode->children[indexInParent + 1], indexInParent);
    } else {
        // The parent only have one child (thus having no entry). Do some
        // compression.

        // All the ANCESTORS of this node should be empty.
        for (;;) {
            assert(parentNode->numEntry == 0);

            NodeIndex currentIndex = parentNode->index;
            if (parentNode->parent == IndexNode::NULL_NODE) {
                release(currentIndex);
                break;
            }

            parentHandle = getHandle(parentNode->parent);
            parentNode = PF::loadRaw<IndexNode *>(parentHandle);
            release(currentIndex);
        }

        // Now the parent is the root node, and the child node becomes the new
        // root.
        meta.rootNode = index;
        meta.numNode--;
        node->parent = IndexNode::NULL_NODE;

        nodeHandle = PF::renew(nodeHandle);

        PF::markDirty(nodeHandle);

        return;
    }

    assert(parentHandle.validate());
    assert(nodeHandle.validate());

    checkUnderflowFrom(node->parent);
}

void Index::collapse(NodeIndex left, NodeIndex right, int parentEntryIndex) {
    PageHandle leftHandle = getHandle(left);
    IndexNode *leftNode = PF::loadRaw<IndexNode *>(leftHandle);

    PageHandle rightHandle = getHandle(right);
    IndexNode *rightNode = PF::loadRaw<IndexNode *>(rightHandle);

    PageHandle parentHandle = getHandle(leftNode->parent);
    IndexNode *parentNode = PF::loadRaw<IndexNode *>(parentHandle);

    // Remove the entry from the parent.
    IndexEntry entry = parentNode->entry[parentEntryIndex];
    for (int i = parentEntryIndex; i < parentNode->numEntry - 1; i++) {
        parentNode->entry[i] = parentNode->entry[i + 1];
    }
    parentNode->numEntry--;

    // Insert it into the left sibling.
    insertEntry(leftNode, entry);

    // Remove the right node as a child of the parent.
    for (int i = parentEntryIndex + 1; i < parentNode->numChildren - 1; i++) {
        parentNode->children[i] = parentNode->children[i + 1];
    }
    parentNode->numChildren--;

    // Merge the children of the right node into the left sibling.
    for (int i = 0; i < rightNode->numChildren; i++) {
        leftNode->children[leftNode->numChildren + i] = rightNode->children[i];
        // Update the parent of the child.
        // FIXME: This requires lots of IO.
        PageHandle childHandle =
            getHandle(leftNode->children[leftNode->numChildren + i]);
        IndexNode *childNode = PF::loadRaw<IndexNode *>(childHandle);
        childNode->parent = leftNode->index;
        PF::markDirty(childHandle);
    }
    leftNode->numChildren += rightNode->numChildren;

    // Merge the entries of the right node into the left sibling.
    for (int i = 0; i < rightNode->numEntry; i++) {
        leftNode->entry[leftNode->numEntry + i] = rightNode->entry[i];
    }
    leftNode->numEntry += rightNode->numEntry;

    // Release the page of the deleted node.
    release(right);

    // Mark dirty.
    PF::markDirty(leftHandle);
    PF::markDirty(rightHandle);
    PF::markDirty(parentHandle);
}

void Index::release(NodeIndex index) {
    PageHandle handle = getHandle(index);
    EmptyPageMeta newMeta;
    newMeta.nextPage = meta.firstFreePage;

    EmptyPageMeta *pageMeta = PF::loadRaw<EmptyPageMeta *>(handle);
    *pageMeta = newMeta;

    meta.firstFreePage = index;

    PF::markDirty(handle);
}

void Index::flushMeta() {
    PageHandle handle = PF::getHandle(fd, 0);
    memcpy(PF::loadRaw(handle), &meta, sizeof(IndexMeta));
    PF::markDirty(handle);
}

void Index::checkInit() {
    if (!initialized) {
        Logger::log(ERROR, "Index: not initialized yet\n");
        throw Internal::IndexNotInitializedError();
    }
}

#ifdef _DEBUG
void Index::dump() {
    std::queue<NodeIndex> q;
    q.push(meta.rootNode);

    while (!q.empty()) {
        NodeIndex index = q.front();
        q.pop();

        PageHandle handle = getHandle(index);
        IndexNode *node = PF::loadRaw<IndexNode *>(handle);

        printf("Node %d: ", index);
        for (int i = 0; i < node->numEntry; i++) {
            printf("%d ", *(int *)(node->entry[i].key));
        }
        printf("( %d : ", node->parent);

        for (int i = 0; i < node->numChildren; i++) {
            q.push(node->children[i]);
            printf("%d ", node->children[i]);
        }
        printf(")\n");
    }
}
#endif

// ==== IndexEntry ====
bool Index::IndexEntry::gt(const char *value, DataType type) const {
    switch (type) {
        case INT:
            return *(int *)key > *(int *)value;
        case FLOAT:
            return Internal::_Float(value) > Internal::_Float(value);
        default:
            assert(false);
    }
}

bool Index::IndexEntry::eq(const char *value, DataType type) const {
    switch (type) {
        case INT:
            return *(int *)key == *(int *)value;
        case FLOAT:
            return Internal::_Float(value) == Internal::_Float(value);
        default:
            assert(false);
    }
}

}  // namespace Internal
}  // namespace SimpleDB