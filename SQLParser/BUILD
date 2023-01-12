load("@rules_antlr//antlr:antlr4.bzl", "antlr")

antlr(
    name = "generated",
    srcs = ["Sql.g4"],
    package = "SQLParser",
    listener = False,
    visitor = True,
    language = "Cpp",
    visibility = ["//visibility:public"],
)

# load("@rules_cc//cc:defs.bzl", "cc_library")

# ANTLR_FLAGS = "-Dlanguage=Cpp -visitor -no-listener -package SQLParser " + \
#               "-Xexact-output-dir -o $(RULEDIR) "

# genrule(
#     name = "antlr-sources",
#     srcs = ["Sql.g4"],
#     outs = [
#         "SqlLexer.cpp", "SqlParser.cpp",
#         "SqlVisitor.cpp", "SqlBaseVisitor.cpp",
#         "SqlLexer.h", "SqlParser.h",
#         "SqlVisitor.h", "SqlBaseVisitor.h",
#     ],
#     cmd = "antlr4 " + ANTLR_FLAGS + " $<"
# )

cc_library(
    name = "sqlparser",
    srcs = [":generated"],
    deps = [
        ":generated",
        "@antlr4_runtimes//:cpp",
    ],
    includes = ["generated.inc"],
    visibility = ["//visibility:public"],
    linkstatic = True,
)
