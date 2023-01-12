#ifndef _SIMPLEDB_INDEX_H
#define _SIMPLEDB_INDEX_H

#include <functional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "internal/PageFile.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

class Index {
private:
    using NodeIndex = int;
    using IterateFunc = std::function<bool(RecordID)>;

public:
    Index() = default;
    ~Index();
    void open(const std::string &file);
    void create(const std::string &file);
    void close();

    void insert(int key, bool isNull, RecordID id);
    void remove(int key, bool isNull, RecordID rid);

    using Range = std::pair<int, int>;  // [first, second]

    bool has(int key, bool isNull);
    void iterateEq(int key, bool isNull, IterateFunc func);
    void iterateRange(Range, IterateFunc func);
    std::vector<RecordID> findEq(int key, bool isNull);
    void setReadOnly();

#ifndef TESTING
private:
#endif
    struct IndexMeta {
        uint16_t headCanary = INDEX_META_CANARY;
        int numNode;
        int numEntry;
        int firstFreeSlot;
        NodeIndex rootNode;
        uint16_t tailCanary = INDEX_META_CANARY;
    };

    struct IndexEntry {
        int key;
        bool isNull = false;
        RecordID record;

        bool operator==(const IndexEntry &rhs) const;
        bool operator>(const IndexEntry &rhs) const;
    };

    struct SharedNode {
        bool isLeaf;
        NodeIndex index;
        IndexEntry entry[MAX_NUM_ENTRY_PER_NODE + 1];
        int numEntry;
        int parent;
    };

    struct LeafNode {
        /* Keep first */
        SharedNode shared;

        int32_t validBitmap;
        NodeIndex next;
        NodeIndex previous;

        inline bool valid(int index) {
            return (validBitmap & (1L << index)) != 0;
        }

        static_assert(sizeof(decltype(validBitmap)) * 8 >=
                      MAX_NUM_ENTRY_PER_NODE);

        // TODO: Pointer to the next leaf node
    };

    struct InnerNode {
        static const int NULL_NODE = -1;

        /* Keep first */
        SharedNode shared;

        int children[MAX_NUM_CHILD_PER_NODE + 1];
        int numChildren;
    };

    static const NodeIndex NULL_NODE_INDEX = -1;

    static_assert(sizeof(IndexMeta) <= PAGE_SIZE);
    static_assert(sizeof(LeafNode) <= INDEX_SLOT_SIZE);
    static_assert(sizeof(InnerNode) <= INDEX_SLOT_SIZE);

    FileDescriptor fd;
    IndexMeta meta;

    bool initialized = false;
    bool readOnly = false;

    void flushMeta();
    void checkInit() noexcept(false);

    // === Internal helper methods ===

    std::tuple<NodeIndex, int, bool> findEntry(const IndexEntry &entry,
                                               bool skipInvalid);
    // std::tuple<NodeIndex, int, bool> findLeftmostLeafNode(int key);
    NodeIndex createNewLeafNode(NodeIndex parent);
    NodeIndex createNewInnerNode(NodeIndex parent);
    SharedNode *getNewNode(NodeIndex parent);

    // Insert the entry WITHOUT checking the constraints.
    int insertEntry(SharedNode *sharedNode, const IndexEntry &entry);
    void checkOverflowFrom(NodeIndex index);
    inline PageHandle getHandle(NodeIndex index) {
        int page = index / NUM_INDEX_SLOT;
        return PF::getHandle(fd, page + 1);
    }
    template <typename T>
    T load(NodeIndex index, PageHandle handle) {
        char *ptr = PF::loadRaw<char *>(handle);
        ptr += (index % NUM_INDEX_SLOT) * INDEX_SLOT_SIZE;
        return (T)ptr;
    }

#if DEBUG
    void dump();
#endif
};

}  // namespace Internal
}  // namespace SimpleDB

#endif