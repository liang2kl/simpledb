#ifndef _SIMPLEDB_INDEX_H
#define _SIMPLEDB_INDEX_H

#include <sys/_types/_int16_t.h>

#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include "internal/PageFile.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

class Index {
private:
    using NodeIndex = int;
    using IterateAllFunc = std::function<void(RecordID)>;

public:
    Index() = default;
    ~Index();
    void open(const std::string &file);
    void create(const std::string &file);
    void close();

    void insert(int key, RecordID id);
    void remove(int key, RecordID rid);
    void iterateEq(int key, IterateAllFunc func);
    std::vector<RecordID> findEq(int key);

#ifndef TESTING
private:
#endif
    struct IndexMeta {
        uint16_t headCanary = INDEX_META_CANARY;
        int numNode;
        int numEntry;
        int firstFreePage;
        NodeIndex rootNode;
        uint16_t tailCanary = INDEX_META_CANARY;
    };

    struct IndexEntry {
        int key;
        RecordID record;

        bool operator==(const IndexEntry &rhs) const;
        bool operator>(const IndexEntry &rhs) const;
    };

    struct EmptyPageMeta {
        uint16_t headCanary = EMPTY_INDEX_PAGE_CANARY;
        int nextPage;
        uint16_t tailCanary = EMPTY_INDEX_PAGE_CANARY;
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
        SharedNode shared = {true};

        int16_t validBitmap;
        NodeIndex next;
        NodeIndex previous;

        inline bool valid(int index) {
            return (validBitmap & (1L << index)) != 0;
        }

        // TODO: Pointer to the next leaf node
    };

    struct InnerNode {
        static const int NULL_NODE = -1;

        /* Keep first */
        SharedNode shared = {true};

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
        return PF::getHandle(fd, index);
    }
    void release(NodeIndex index);

#if DEBUG
    void dump();
#endif
};

}  // namespace Internal
}  // namespace SimpleDB

#endif