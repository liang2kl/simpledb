load("@rules_cc//cc:defs.bzl", "cc_test")

cc_test(
    name = "simpledb_test",
    size = "small",
    srcs = glob(["*.cc", "*.h"]),
    copts = ["-std=c++17"],
    deps = [
        "@googletest//:gtest",
        "//:simpledb-test",
    ],
    local_defines = ["DEBUG=1", "TESTING=1"],
    visibility = ["//visibility:public"],
    linkstatic = True,
)
