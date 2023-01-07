# 具体设计

## 文件系统/缓存管理

支持文件的页式访问及其缓存，使用 LRU 替换策略。提供的主要接口包括：

- `createFile`：创建文件
- `openFile`：打开文件，返回标识符
- `closeFile`：关闭文件
- `getHandle`：返回对应页的一个“Page Handle”，用于访问页对应的内存及判断缓存是否失效
- `load`：使用 Page Handle 访问页对应的内存，返回对应指针
- `markDirty`：标记脏页
- `renew`：若 Page Handle 因为缓存被逐出而失效，将对应页重新载入缓存

使用可以判断失效的 Handle，使得可以在正在使用的页面被替换的情况下保证内存访问的正确性。

## 记录管理

将表的文件的第一页用于记录表的元数据，第二页及之后的页面用于存储数据。记录采用定长方式，在创建表时根据一行的大小将页面划分为槽，每个槽放置一行数据。

在每页起始地址记录页的元数据，包括一个记录槽是否占据的位图，以及下一个空闲的页（链表）。

对外提供的主要接口有：

- `open`：打开文件，加载元数据
- `create`：根据所给 schema 创建空表
- `get`：加载给定 id 的记录并反序列化
- `insert`：序列化给定数据，并寻找空位存储
- `update`：更新给定 id 的记录
- `remove`：删除给定 id 的记录

## 索引管理

使用 B+ 树进行记录的索引。由于需要支持 NULL key 和重复 key 的索引，将 `(key, isNull, recordId)` 三元组作为索引的键进行索引。

具体的存储上，每页对应 B+ 树的一个节点。由于 B+ 树存在叶子节点和内部节点的区别，为了和二进制存储对应，将两种节点的共同数据放置于开头，节点独有的数据放置在之后，即：

```c++
struct SharedNode {
    bool isLeaf;
    ...
};
struct InnerNode {
    SharedNode shared;
    ...
};
struct LeafNode {
    SharedNode shared;
    ...
}
```

这使得我们从内存中读入时可以根据 `SharedNode` 中的 `isLeaf` 判断节点类型，并转换对应的类型：

```c++
char *data = ...;
SharedNode *node = (SharedNode *)data;
if (node->isLeaf) {
    LeafNode *leaf = (LeafNode *)node;
} else {
    InnerNode *inner = (InnerNode *)node;
}
```

索引的插入根据标准的 B+ 树实现，删除实现为 lazy remove，只作 invalid 的标记。

另外，叶子结点存储指向下一叶子结点的指针，方便 range query。

提供的主要接口有：

- `open`：打开索引
- `create`：创建索引
- `insert`：插入索引项
- `remove`：删除索引项
- `iterateRange`：遍历特定范围内的索引项

## 系统管理

系统管理主要涉及几张系统表：

- `databases`：记录所有创建的数据库
- `tables`：记录所有创建的表（schema 直接存在表文件头中）
- `indexes`：记录所有创建的索引
- `foreign_key`：记录所有创建的外键

由于进行查询时这几张表频繁使用，因此常驻内存中。

## 查询处理

因需要支持种类繁多的 `SELECT` 语句，并且可能需要利用索引进行查询，因此需要设计一套通用性较强的查询抽象。

主要的抽象有 `QueryDataSource`、`QueryFilter` 和 `QueryBuilder`。Data source 提供查询数据，Filter 对数据进行筛选和转换，而 Builder 是提供给外界的查询接口。

`QueryDataSource` 为抽象类，需要实现的主要接口包括：

- `iterate`：遍历此数据源所有的记录，可随时停止
- `getColumnInfo`：返回类似于 schema 的信息（因涉及到 JOIN 和原本打算实现的嵌套查询，不能简单地使用表本身的 schema）

`QueryFilter` 负责对遍历的记录进行筛选，返回 (是否继续遍历，是否接受此记录)。Filter 包括：

- `ValueConditionFilter`：处理与字面值进行比较的条件
- `NullConditionFilter`：处理是否为 NULL 的条件
- `ColumnConditionFilter`：处理两个列之间的比较
- `SelectFilter`：处理选择部分列的语句以及聚合查询
- `LimitFilter`：处理 `LIMIT`
- `OffsetFilter`：处理 `OFFSET`

`QueryBuilder` 根据所有的条件，转换为对应的 Filter，并将其相连接。当执行查询时，数据从数据源出发，依次通过各个 Filter，当所有的 Filter 通过后，将其加入到结果中，否则丢弃。这样做避免了重复对整个数据集进行筛选，大大减少了内存使用。

在这套抽象的基础上，很容易实现 JOIN 和索引加速的查询，只需要实现对应的 Data source，给出遍历的方法即可（对应代码中的 `JoinedTable` 和 `IndexedTable`），而 Filter 是通用的。`QueryBuilder` 因为只需要用到 `QueryDataSource` 抽象类的接口，因此可以接受任意的 Data source，无论是原始的 `Table`，使用索引的 `IndexedTable`，还是多表连接的 `JoinedTable`。

另外，`QueryBuilder` 本身也可作为 Data source，可用来遍历符合条件的记录，从而可以直接用来实现 `DELETE` 和 `UPDATE` 的条件判断，以及支持嵌套查询（虽然未实现）。

## 错误处理及 Logger

错误处理不使用返回值，而是使用 `throw` 实现。这样最明显的好处在于，无论在多深的调用栈发生错误，都可以在任意想要处理错误的位置捕捉到，并根据对应的错误类型进行处理，而不需要在任何可能发生错误的调用处都进行处理。

错误类型包括内部使用的错误和对外暴露的错误，内部使用的错误在进行 SQL 执行时会被转换为暴露的错误并返回，因此客户端可以支持具体错误类型的显示。

另外，实现了一套 logging 系统，根据设定的重要程度打印日志。在各层的关键调用处使用 `VERBOSE` 等级打印调用信息，对调试有很大的帮助。
