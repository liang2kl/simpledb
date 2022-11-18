package(default_visibility = ["//visibility:public"])

alias(
    name = "simpledb",
    actual = "//SimpleDB:simpledb",
)

alias(
    name = "simpledb-test",
    actual = "//SimpleDB:simpledb-test",
)

alias(
    name = "sqlparser",
    actual = "//SQLParser:sqlparser",
)

alias(
    name = "simpledb_service",
    actual = "//SimpleDBService:simpledb_service",
)

alias(
    name = "simpledb_service_py",
    actual = "//SimpleDBService:simpledb_service_py"
)

alias(
    name = "simpledb_server",
    actual = "//SimpleDBServer:main",
)

alias(
    name = "simpledb_client",
    actual = "//SimpleDBClient:main",
)

test_suite(
    name = "test_all",
    tests = [
        "//SimpleDBTest:simpledb_test"
    ],
)