load("@rules_cc//cc:defs.bzl", "cc_test")

cc_test(
    name = "simpledb_test",
    size = "small",
    srcs = glob(["*.cc", "*.h"]),
    copts = ["-std=c++17"],
    deps = [
        "@googletest//:gtest",
        "//SimpleDB:simpledb-test",
    ],
    visibility = ["//visibility:public"],
)
