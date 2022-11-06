#ifndef _SIMPLEDB_INDEX_H
#define _SIMPLEDB_INDEX_H

#include <string>
#include <tuple>

#include "PageFile.h"
#include "Table.h"

namespace SimpleDB {

class Index {
private:
    using NodeIndex = int;

public:
    Index() = default;
    ~Index();
    void open(const std::string &file);
    void create(const std::string &file, ColumnMeta column);
    void close();

    bool insert(const char *key, RecordID id);
    bool remove(const char *key);
    RecordID find(const char *key);

#ifndef TESTING
private:
#endif
    struct IndexMeta {
        uint16_t headCanary = INDEX_META_CANARY;
        int numNode;
        int numEntry;
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

#ifdef _DEBUG
    void dump();
#endif
};

}  // namespace SimpleDB

#endif