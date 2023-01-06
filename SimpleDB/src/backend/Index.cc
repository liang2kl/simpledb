#include "internal/Index.h"

#include <cassert>
#include <climits>
#include <queue>
#include <string>

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

    Logger::log(VERBOSE,
                "Index: the index uses %d pages, containing %d records\n",
                meta.numNode, meta.numEntry);

    initialized = true;
}

void Index::create(const std::string &file) {
    try {
        PF::create(file);
    } catch (Internal::FileExistsError) {
        Logger::log(
            WARNING,
            "Index: creating an empty index to file %s that already exists\n",
            file.c_str());
    } catch (BaseError) {
        Logger::log(ERROR, "Index: fail to create index to %s\n", file.c_str());
        throw Internal::CreateIndexError();
    }

    fd = PF::open(file);

    meta.numNode = 0;
    meta.numEntry = 0;
    meta.firstFreeSlot = 0;

    // Create root node.
    NodeIndex index = createNewLeafNode(NULL_NODE_INDEX);
    meta.rootNode = index;

    initialized = true;
}

void Index::setReadOnly() { readOnly = true; }

void Index::close() {
    if (!initialized) {
        return;
    }

    Logger::log(VERBOSE, "Index: closing index\n");

    if (!readOnly) {
        flushMeta();
    }

    PF::close(fd);
    initialized = false;
    // TODO: More cleanup
}

void Index::insert(int key, bool isNull, RecordID id) {
    Logger::log(VERBOSE,
                "Index: inserting index (key: %d) at page %d, slot %d\n", key,
                id.page, id.slot);

    checkInit();
    if (readOnly) {
        Logger::log(ERROR,
                    "Index: internal error: writing into a read-only index\n");
        throw Internal::WriteOnReadOnlyIndexError();
    }

    auto result = findEntry({key, isNull, id}, /*skipInvalid=*/false);
    NodeIndex nodeIndex = std::get<0>(result);
    int index = std::get<1>(result);
    bool found = std::get<2>(result);

    PageHandle handle = getHandle(nodeIndex);
    LeafNode *node = load<LeafNode *>(nodeIndex, handle);

    assert(node->shared.isLeaf);

    if (found) {
        // The record already exists.
        if (!node->valid(index)) {
            // The record was deleted, simply mark as valid.
            node->validBitmap |= (1L << index);
        } else {
            throw Internal::IndexKeyExistsError(std::to_string(key));
        }
    } else {
        // Insert the record into the node.
        insertEntry(&node->shared, {key, isNull, id});
        checkOverflowFrom(nodeIndex);
    }

    // Renew the handle and mark dirty.
    handle = PF::renew(handle);
    PF::markDirty(handle);

    meta.numEntry++;
}

void Index::remove(int key, bool isNull, RecordID rid) {
    Logger::log(VERBOSE, "Index: removing record %d\n", key);
    checkInit();

    if (readOnly) {
        Logger::log(ERROR,
                    "Index: internal error: writing into a read-only index\n");
        throw Internal::WriteOnReadOnlyIndexError();
    }

    auto [nodeIndex, index, found] =
        findEntry({key, isNull, rid}, /*skipInvalid=*/true);

    if (!found) {
        throw Internal::IndexKeyNotExistsError();
    }

    PageHandle handle = getHandle(nodeIndex);
    LeafNode *node = load<LeafNode *>(nodeIndex, handle);

    assert(node->shared.isLeaf);

    // Remove the entry (simply mark invalid here).
    node->validBitmap &= ~(1L << index);

    // Mark dirty.
    PF::markDirty(handle);

    meta.numEntry--;
}

bool Index::has(int key, bool isNull) {
    checkInit();

    bool ret = false;
    iterateEq(key, isNull, [&](RecordID id) {
        ret = true;
        return false;
    });

    return ret;
}

std::vector<RecordID> Index::findEq(int key, bool isNull) {
    std::vector<RecordID> ret;

    iterateEq(key, isNull, [&](RecordID id) {
        ret.push_back(id);
        return true;
    });

    return ret;
}

void Index::iterateEq(int key, bool isNull, IterateFunc func) {
    iterateRange({key, key}, func);
}

void Index::iterateRange(Range range, IterateFunc func) {
    auto [lo, hi] = range;

    Logger::log(VERBOSE, "Index: finding record in [%d, %d]\n", lo, hi);
    checkInit();

    // Just "find" the {INT_MIN, INT_MIN} record, which must be the start of the
    // sequenece, if the key matches the target key.
    auto [nodeIndex, index, _] = findEntry(
        {lo, /*isNull=*/false, {INT_MIN, INT_MIN}}, /*skipInvalid=*/true);

    // Iterate from the start of the sequence.
    PageHandle handle = getHandle(nodeIndex);
    LeafNode *node = load<LeafNode *>(nodeIndex, handle);
    NodeIndex startIndex = node->shared.index;

    assert(node->shared.isLeaf);

    for (;;) {
        for (int i = index; i < node->shared.numEntry; i++) {
            int key = node->shared.entry[i].key;
            if (key < lo) {
                return;
            }
            if (node->valid(i) && key >= lo && key <= hi) {
                bool continue_ = func(node->shared.entry[i].record);
                if (!continue_) {
                    return;
                }
            }
            if (node->shared.entry[i].key > hi) {
                return;
            }
        }

        if (node->next == startIndex) {
            return;
        }

        handle = getHandle(node->next);
        node = load<LeafNode *>(node->next, handle);
        index = 0;
    }
}

std::tuple<Index::NodeIndex, int, bool> Index::findEntry(
    const IndexEntry &entry, bool skipInvalid) {
    // Start from the root node.
    int currentNode = meta.rootNode;

    for (;;) {
        PageHandle handle = getHandle(currentNode);
        SharedNode *sharedNode = load<SharedNode *>(currentNode, handle);
        if (sharedNode->isLeaf) {
            LeafNode *node = (LeafNode *)sharedNode;
            int candidate = sharedNode->numEntry;
            for (int i = 0; i < sharedNode->numEntry; i++) {
                if (sharedNode->entry[i] == entry) {
                    if (!skipInvalid || node->valid(i)) {
                        return {currentNode, i, true};
                    }
                }
                if (sharedNode->entry[i] > entry) {
                    candidate = i;
                    break;
                }
            }
            return {currentNode, candidate, false};
        }

        InnerNode *node = (InnerNode *)(sharedNode);

        currentNode = node->children[node->numChildren - 1];

        for (int i = 0; i < sharedNode->numEntry; i++) {
            if (sharedNode->entry[i] > entry) {
                currentNode = node->children[i];
                break;
            }
        }
    }

    assert(false);
    return {-1, -1, false};
}

Index::NodeIndex Index::createNewLeafNode(NodeIndex parent) {
    SharedNode *sharedNode = getNewNode(parent);
    sharedNode->isLeaf = true;

    LeafNode *leafNode = (LeafNode *)(sharedNode);
    leafNode->validBitmap = ~0;
    leafNode->next = sharedNode->index;
    leafNode->previous = sharedNode->index;

    return sharedNode->index;
}

Index::NodeIndex Index::createNewInnerNode(NodeIndex parent) {
    SharedNode *sharedNode = getNewNode(parent);
    InnerNode *node = (InnerNode *)sharedNode;

    node->numChildren = 0;
    sharedNode->isLeaf = false;

    return sharedNode->index;
}

Index::SharedNode *Index::getNewNode(NodeIndex parent) {
    // Get and update the free slot.
    NodeIndex index = meta.firstFreeSlot;
    PageHandle handle = getHandle(meta.firstFreeSlot);

    SharedNode *node = load<SharedNode *>(index, handle);

    node->numEntry = 0;
    node->index = index;
    node->parent = parent;

    PF::markDirty(handle);

    meta.numNode++;
    meta.firstFreeSlot++;

    return node;
}

int Index::insertEntry(SharedNode *sharedNode, const IndexEntry &entry) {
    // Find the insert position.
    int index = sharedNode->numEntry;
    for (int i = 0; i < sharedNode->numEntry; i++) {
        if (sharedNode->entry[i] > entry) {
            index = i;
            break;
        }
    }

    // Shift the entries.
    for (int i = sharedNode->numEntry; i > index; i--) {
        sharedNode->entry[i] = sharedNode->entry[i - 1];
    }

    // Also, shift the valid bitmap!
    if (sharedNode->isLeaf) {
        LeafNode *node = (LeafNode *)sharedNode;
        for (int i = sharedNode->numEntry; i > index; i--) {
            if (node->valid(i - 1)) {
                node->validBitmap |= (1L << i);
            } else {
                node->validBitmap &= ~(1L << i);
            }
        }
        node->validBitmap |= (1L << index);
    }

    sharedNode->entry[index] = entry;
    sharedNode->numEntry++;

    return index;
}

void Index::checkOverflowFrom(Index::NodeIndex index) {
    PageHandle nodeHandle = getHandle(index);
    SharedNode *sharedNode = load<SharedNode *>(index, nodeHandle);

    if (sharedNode->numEntry <= MAX_NUM_ENTRY_PER_NODE) {
        return;
    }

#if DEBUG
    if (!sharedNode->isLeaf) {
        assert(((InnerNode *)sharedNode)->numChildren ==
               sharedNode->numEntry + 1);
    }
#endif

    // Select a victim entry at the middle.
    int victimIndex = sharedNode->numEntry / 2;
    IndexEntry victimEntry = sharedNode->entry[victimIndex];
    // Split the node.
    NodeIndex siblingIndex = sharedNode->isLeaf
                                 ? createNewLeafNode(sharedNode->parent)
                                 : createNewInnerNode(sharedNode->parent);
    PageHandle siblingHandle = getHandle(siblingIndex);
    SharedNode *siblingSharedNode =
        load<SharedNode *>(siblingIndex, siblingHandle);

    // Move the "right" (+victim) entries to the sibling node.
    siblingSharedNode->numEntry =
        sharedNode->numEntry - victimIndex - (sharedNode->isLeaf ? 0 : 1);
    memcpy(siblingSharedNode->entry,
           &sharedNode->entry[victimIndex + (sharedNode->isLeaf ? 0 : 1)],
           sizeof(IndexEntry) * siblingSharedNode->numEntry);
    sharedNode->numEntry = victimIndex;

    // Set the "next" and "previous" pointer in the leaf node.
    if (sharedNode->isLeaf) {
        LeafNode *leafNode = (LeafNode *)sharedNode;
        LeafNode *siblingLeafNode = (LeafNode *)siblingSharedNode;
        if (leafNode->previous == sharedNode->index) {
            leafNode->previous = siblingIndex;
        }
        siblingLeafNode->next = leafNode->next;
        leafNode->next = siblingIndex;
        siblingLeafNode->previous = sharedNode->index;
    }

    // Move the children.
    if (!sharedNode->isLeaf) {
        InnerNode *siblingNode = (InnerNode *)siblingSharedNode;
        InnerNode *node = (InnerNode *)sharedNode;

        siblingNode->numChildren = node->numChildren - victimIndex - 1;
        memcpy(siblingNode->children, &node->children[victimIndex + 1],
               sizeof(NodeIndex) * siblingNode->numChildren);
        node->numChildren = victimIndex + 1;

        // Update the parent of the children.
        // FIXME: This requires lots of IO.
        for (int i = 0; i < siblingNode->numChildren; i++) {
            NodeIndex childIndex = siblingNode->children[i];
            PageHandle childHandle = getHandle(childIndex);
            SharedNode *childNode = load<SharedNode *>(childIndex, childHandle);
            childNode->parent = siblingIndex;
            PF::markDirty(childHandle);
        }

#if DEBUG
        assert(node->numChildren == sharedNode->numEntry + 1);
        assert(siblingNode->numChildren == siblingSharedNode->numEntry + 1);
#endif
    }

    // Insert the victim entry into the parent node.

    // The victim entry is all the root node.
    if (sharedNode->parent == NULL_NODE_INDEX) {
        NodeIndex newRootIndex = createNewInnerNode(NULL_NODE_INDEX);
        PageHandle newRootHandle = getHandle(newRootIndex);
        InnerNode *newRootNode = load<InnerNode *>(newRootIndex, newRootHandle);
        newRootNode->shared.numEntry = 1;
        newRootNode->numChildren = 2;
        newRootNode->shared.entry[0] = victimEntry;
        newRootNode->children[0] = index;
        newRootNode->children[1] = siblingIndex;

        // Don't forget to update the parent of the children.
        sharedNode->parent = newRootIndex;
        siblingSharedNode->parent = newRootIndex;

        // Mark dirty.
        PF::markDirty(siblingHandle);
        PF::markDirty(nodeHandle);
        PF::markDirty(newRootHandle);

        // And the meta...
        meta.rootNode = newRootIndex;

        return;
    }

    // The parent node exists.
    PageHandle parentHandle = getHandle(sharedNode->parent);
    InnerNode *parentNode = load<InnerNode *>(sharedNode->parent, parentHandle);
    int insertIndex = insertEntry(&parentNode->shared, victimEntry);

    // Update the children of the parent node.
    for (int i = parentNode->numChildren; i > insertIndex + 1; i--) {
        parentNode->children[i] = parentNode->children[i - 1];
    }
    parentNode->children[insertIndex + 1] = siblingSharedNode->index;
    parentNode->numChildren++;

    // Mark dirty.
    PF::markDirty(siblingHandle);
    PF::markDirty(nodeHandle);
    PF::markDirty(parentHandle);

    // Check recursively if the parent node overflows.
    checkOverflowFrom(sharedNode->parent);
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

#if DEBUG
void Index::dump() {
    std::queue<NodeIndex> q;
    q.push(meta.rootNode);

    while (!q.empty()) {
        NodeIndex index = q.front();
        q.pop();

        PageHandle handle = getHandle(index);
        SharedNode *node = load<SharedNode *>(index, handle);

        printf("Node %d (is leaf: %d): ", index, node->isLeaf);
        if (node->isLeaf) {
            for (int i = 0; i < node->numEntry; i++) {
                printf("[%s%d, %d-%d] ",
                       ((LeafNode *)node)->valid(i) ? "" : "X: ",
                       node->entry[i].key, node->entry[i].record.page,
                       node->entry[i].record.slot);
            }
        }
        printf("( %d : ", node->parent);

        if (!node->isLeaf) {
            InnerNode *innerNode = (InnerNode *)node;
            for (int i = 0; i < innerNode->numChildren; i++) {
                q.push(innerNode->children[i]);
                printf("%d ", innerNode->children[i]);
            }
        }

        printf(")\n");
    }
}
#endif

// ==== IndexEntry ====
bool Index::IndexEntry::operator>(const IndexEntry &rhs) const {
    if (!isNull) {
        if (rhs.isNull) {
            return true;
        } else {
            return key > rhs.key || (key == rhs.key && record > rhs.record);
        }
    } else {
        if (!rhs.isNull) {
            return false;
        } else {
            return record > rhs.record;
        }
    }
}

bool Index::IndexEntry::operator==(const IndexEntry &rhs) const {
    return ((isNull && rhs.isNull) || (key == rhs.key)) &&
           isNull == rhs.isNull && record == rhs.record;
}

}  // namespace Internal
}  // namespace SimpleDB