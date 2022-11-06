#include "Index.h"

#include <queue>

#include "Logger.h"
#include "internal/Comparer.h"

namespace SimpleDB {

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
        throw Error::ReadIndexError();
    }

    if (meta.headCanary != INDEX_META_CANARY ||
        meta.tailCanary != INDEX_META_CANARY) {
        Logger::log(ERROR,
                    "Index: fail to read index metadata from file %d: "
                    "invalid canary values\n",
                    fd.value);
        throw Error::ReadIndexError();
    }

    initialized = true;
}

void Index::create(const std::string &file, ColumnMeta column) {
    try {
        PF::create(file);
        fd = PF::open(file);
    } catch (Error::FileExistsError) {
        Logger::log(
            WARNING,
            "Index: creating an empty index to file %s that already exists\n",
            file.c_str());
    } catch (BaseError) {
        Logger::log(ERROR, "Table: fail to create table to %s\n", file.c_str());
        throw Error::CreateIndexError();
    }

    if (meta.type == VARCHAR) {
        Logger::log(ERROR, "Index: index on varchar is not supported\n");
        throw Error::InvalidIndexTypeError();
    }

    meta.type = column.type;
    meta.size = column.size;
    meta.numNode = 0;

    // Create root node.
    NodeIndex index = createNewLeafNode(IndexNode::NULL_NODE);
    meta.rootNode = index;

    flushMeta();

    initialized = true;
}

void Index::close() {
    if (!initialized) {
        return;
    }
    Logger::log(VERBOSE, "Index: closing index\n");

    PF::close(fd);
    // TODO: More cleanup
}

bool Index::insert(const char *data, RecordID id) {
    Logger::log(VERBOSE, "Index: inserting index at page %d, slot %d\n",
                id.page, id.slot);

    checkInit();

    auto result = findNode(data);
    NodeIndex nodeIndex = std::get<0>(result);
    int index = std::get<1>(result);
    bool found = std::get<2>(result);

    if (found) {
        // The record already exists.
        return false;
    }

    PageHandle handle = PF::getHandle(fd, nodeIndex);
    IndexNode *node = PF::loadRaw<IndexNode *>(handle);

    // First insert the record.
    IndexEntry entry;
    memcpy(entry.value, data, 4);  // FIXME: Hard code size.
    entry.type = meta.type;
    entry.record = id;

    // Insert the record into the node.
    insertEntry(node, entry);

    // Check if the insertion breaks the constraints. If so, split recusively to
    // fix them.
    checkOverflowFrom(nodeIndex);

    // Renew the handle and mark dirty.
    if (!handle.validate()) {
        handle = PF::getHandle(fd, nodeIndex);
    }
    PF::markDirty(handle);

    return true;
}

void Index::remove(RecordID id) {
    Logger::log(VERBOSE, "Index: removing record at page %d, slot %d\n",
                id.page, id.slot);
    checkInit();
}

RecordID Index::find(const char *data) {
    Logger::log(VERBOSE, "Index: finding record\n");
    checkInit();

    // TODO: Pin the root node.

    auto result = findNode(data);
    NodeIndex nodeIndex = std::get<0>(result);
    int index = std::get<1>(result);
    bool found = std::get<2>(result);

    if (!found) {
        return RecordID::NULL_RECORD;
    }

    PageHandle handle = PF::getHandle(fd, nodeIndex);
    IndexNode *node = PF::loadRaw<IndexNode *>(handle);
    return node->entry[index].record;
}

std::tuple<Index::NodeIndex, int, bool> Index::findNode(const char *value) {
    // Start from the root node.
    int currentNode = meta.rootNode;

    for (;;) {
        PageHandle handle = PF::getHandle(fd, currentNode);
        IndexNode *node = PF::loadRaw<IndexNode *>(handle);

        int nextChild = node->numEntry;

        for (int i = 0; i < node->numEntry; i++) {
            if (node->entry[i] == value) {
                // Found the exact node.
                return {currentNode, i, true};
            }
            if (node->entry[i] > value) {
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
    flushMeta();

    // The index starts from 1!
    NodeIndex index = meta.numNode;
    PageHandle handle = getHandle(index);
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
        if (node->entry[i] > entry.value) {
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
        flushMeta();

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

    // Check if the parent node overflows recursively.
    checkOverflowFrom(node->parent);
}

void Index::flushMeta() {
    PageHandle handle = PF::getHandle(fd, 0);
    memcpy(PF::loadRaw(handle), &meta, sizeof(IndexMeta));
    PF::markDirty(handle);
}

void Index::checkInit() {
    if (!initialized) {
        Logger::log(ERROR, "Index: not initialized yet\n");
        throw Error::IndexNotInitializedError();
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
            printf("%d ", *(int *)(node->entry[i].value));
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
bool Index::IndexEntry::operator<(const char *value) const {
    switch (type) {
        case INT:
            return *(int *)(this->value) < *(int *)value;
        case FLOAT:
            return Internal::_Float(value) < Internal::_Float(value);
        default:
            assert(false);
    }
}

bool Index::IndexEntry::operator>(const char *value) const {
    switch (type) {
        case INT:
            return *(int *)(this->value) > *(int *)value;
        case FLOAT:
            return Internal::_Float(value) > Internal::_Float(value);
        default:
            assert(false);
    }
}

bool Index::IndexEntry::operator==(const char *value) const {
    switch (type) {
        case INT:
            return *(int *)(this->value) == *(int *)value;
        case FLOAT:
            return Internal::_Float(value) == Internal::_Float(value);
        default:
            assert(false);
    }
}

bool Index::IndexEntry::operator>=(const char *value) const {
    return !operator<(value);
}

bool Index::IndexEntry::operator<=(const char *value) const {
    return !operator>(value);
}

}  // namespace SimpleDB