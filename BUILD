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

test_suite(
    name = "test_all",
    tests = [
        "//SimpleDBTest:simpledb_test"
    ],
)