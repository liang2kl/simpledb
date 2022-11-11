load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

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
    visibility = ["//visibility:public"],
)
