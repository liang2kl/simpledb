load("@rules_cc//cc:defs.bzl", "cc_library")

ANTLR_FLAGS = "-Dlanguage=Cpp -visitor -no-listener -package SQLParser " + \
              "-Xexact-output-dir -o $(RULEDIR) "

genrule(
    name = "antlr-sources",
    srcs = ["src/Sql.g4"],
    outs = [
        "SqlLexer.cpp", "SqlParser.cpp",
        "SqlVisitor.cpp", "SqlBaseVisitor.cpp",
        "SqlLexer.h", "SqlParser.h",
        "SqlVisitor.h", "SqlBaseVisitor.h",
    ],
    cmd = "antlr4 " + ANTLR_FLAGS + " $<"
)

cc_library(
    name = "sqlparser",
    srcs = [":antlr-sources"],
    hdrs = [":antlr-sources"],
    deps = ["//:antlr4-runtime"],
    visibility = ["//visibility:public"]
)
