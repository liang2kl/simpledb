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
    name = "simpledb_proto",
    actual = "//SimpleDBService:simpledb_proto",
)

test_suite(
    name = "test_all",
    tests = [
        "//SimpleDBTest:simpledb_test"
    ],
)