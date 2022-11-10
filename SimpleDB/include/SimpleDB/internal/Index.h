#ifndef _SIMPLEDB_INDEX_H
#define _SIMPLEDB_INDEX_H

#include <string>
#include <tuple>

#include "internal/PageFile.h"
#include "internal/Table.h"

namespace SimpleDB {
namespace Internal {

class Index {
private:
    using NodeIndex = int;

public:
    Index() = default;
    ~Index();
    void open(const std::string &file);
    void create(const std::string &file, ColumnMeta column);
    void close();

    void insert(const char *key, RecordID id);
    void remove(const char *key);
    RecordID find(const char *key);

#ifndef TESTING
private:
#endif
    struct IndexMeta {
        uint16_t headCanary = INDEX_META_CANARY;
        int numNode;
        int numEntry;
        int firstFreePage;
        NodeIndex rootNode;
        DataType type;
        ColumnSizeType size;
        uint16_t tailCanary = INDEX_META_CANARY;
    };

    struct IndexEntry {
        char key[4];
        RecordID record;

        bool eq(const char *value, DataType type) const;
        bool gt(const char *value, DataType type) const;
    };

    struct EmptyPageMeta {
        uint16_t headCanary = EMPTY_INDEX_PAGE_CANARY;
        int nextPage;
        uint16_t tailCanary = EMPTY_INDEX_PAGE_CANARY;
    };

    struct IndexNode {
        static const int NULL_NODE = -1;

        NodeIndex index;
        IndexEntry entry[MAX_NUM_ENTRY_PER_NODE + 1];
        int children[MAX_NUM_CHILD_PER_NODE + 1];
        int numChildren;
        int numEntry;
        int parent;

        inline bool isLeaf() { return numChildren == 0; }
    };

    static_assert(sizeof(IndexMeta) <= PAGE_SIZE);
    static_assert(sizeof(IndexNode) <= INDEX_SLOT_SIZE);

    FileDescriptor fd;
    IndexMeta meta;

    bool initialized = false;

    void flushMeta();
    void checkInit() noexcept(false);

    // === Internal helper methods ===

    std::tuple<NodeIndex, int, bool> findNode(const char *value);
    NodeIndex createNewLeafNode(int parent);
    // Insert the entry WITHOUT checking the constraints.
    int insertEntry(IndexNode *node, const IndexEntry &entry);
    void checkOverflowFrom(NodeIndex index);
    void checkUnderflowFrom(NodeIndex index);
    void collapse(NodeIndex left, NodeIndex right, int parentEntryIndex);
    inline PageHandle getHandle(NodeIndex index) {
        return PF::getHandle(fd, index);
    }
    void release(NodeIndex index);

#ifdef _DEBUG
    void dump();
#endif
};

}  // namespace Internal
}  // namespace SimpleDB

#endif