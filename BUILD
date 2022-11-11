load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

cmake(
    name = "antlr4-runtime",
    cache_entries = {
        "ANTLR_BUILD_CPP_TESTS": "OFF",
    },
    lib_source = "@antlr4-runtime//:all_srcs",
    includes = ["antlr4-runtime"],
    build_args = ["-j", "4"],
    working_directory = "runtime/Cpp",
    out_static_libs = ["libantlr4-runtime.a"],
)

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

test_suite(
    name = "test_all",
    tests = [
        "//Tests:simpledb_test"
    ],
)