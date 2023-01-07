# SimpleDB

清华大学计算机系《数据库系统概论》2022 年大作业项目 DBMS，支持基础 SQL 的解析和执行。

所有支持的功能（即支持的 SQL）见[文法文件](SQLParser/Sql.g4)。

<div>
<img style="width:45%" src="docs/server_screenshot.png">
<img style="width:45%" src="docs/client_screenshot.png">
</div>

## 架构

本项目为 C/S 架构，由 C++ 服务器负责 SQL 的解析和执行，Python 客户端负责处理用户输入，二者之间通过 [gRPC](https://grpc.io) 进行网络传输。

各个部分分别为：

- `SimpleDB`（依赖库）：DBMS，负责数据库管理、SQL 的解析和执行
- `SQLParser`（依赖库）：解析 SQL 并生成 AST
- `SimpleDBService`（依赖库）：gRPC 库，用于支持网络传输
- `SimpleDBServer`（二进制程序）：通过 `SimpleDB` 处理客户端请求并返回结果
- `SimpleDBClient`（二进制程序）：处理用户请求，向客户端发送 SQL 语句，并显示返回的结果
- `SimpleDBTest`（单元测试）：对 `SimpleDB` 进行测试

各个部分及其使用的第三方库的依赖关系如下：

![](docs/dep-graph.svg)

## 编译与运行

使用 [Bazel](https://bazel.build) 自动编译。

运行服务器：

```bash
bazel run -- :simpledb_server --dir=<data_directory> [--debug | --verbose] [--addr=<listening_address>]
```

运行交互式客户端：

```bash
bazel run -- :simpledb_client --server=<addr>
```

从 CSV 中导入数据：

```bash
bazel run -- :simpledb_client --server=<addr> --csv=<csv_file> --db=<db_name> --table=<table_name>
```

运行单元测试：

```bash
bazel test :test_all
```

编译所有 target：

```bash
bazel build //...
```

编译结束后，运行此命令以生成 `compile_commands.json`：

```bash
bazel run @hedron_compile_commands//:refresh_all
```

## 具体实现

与具体实现有关的设计，见 [docs/design.md](docs/design.md)。