cc_library(
    name = "simpledb",
    srcs = glob(["src/**/*.cc"]),
    hdrs = glob(["include/**/*.h"]),
    deps = ["//:sqlparser", "//:simpledb_service"],
    strip_include_prefix = "include/SimpleDB",
    includes = ["include"],
    visibility = ["//visibility:public"],
    linkstatic = True,
)

cc_library(
    name = "simpledb-test",
    srcs = glob(["src/**/*.cc"]),
    hdrs = glob(["include/**/*.h"]),
    deps = ["//:sqlparser", "//:simpledb_service"],
    strip_include_prefix = "include/SimpleDB",
    includes = ["include"],
    local_defines = ["DEBUG=1", "TESTING=1"],
    visibility = ["//visibility:public"],
    linkstatic = True,
)
