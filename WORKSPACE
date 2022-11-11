load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_foreign_cc",
    sha256 = "2a4d07cd64b0719b39a7c12218a3e507672b82a97b98c6a89d38565894cf7c51",
    strip_prefix = "rules_foreign_cc-0.9.0",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/refs/tags/0.9.0.tar.gz",
)

http_archive(
  name = "googletest",
  urls = ["https://github.com/google/googletest/archive/44c03643cfbc649488a0f437cd18e05f11960d19.zip"],
  strip_prefix = "googletest-44c03643cfbc649488a0f437cd18e05f11960d19",
  sha256 = "b82dda7e5f9fe2addf972eee106f787b4966d398fcdb06bb0e9942a190e7cfc2"
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

# This sets up some common toolchains for building targets. For more details, please see
# https://bazelbuild.github.io/rules_foreign_cc/0.9.0/flatten.html#rules_foreign_cc_dependencies
rules_foreign_cc_dependencies(
    register_built_tools = False,
)

_ALL_CONTENT = """
filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
"""

# antlr runtime sources
http_archive(
    name = "antlr4-runtime",
    build_file_content = _ALL_CONTENT,
    strip_prefix = "antlr4-4.11.1",
    url = "https://github.com/antlr/antlr4/archive/refs/tags/4.11.1.tar.gz",
    sha256 = "81f87f03bb83b48da62e4fc8bfdaf447efb9fb3b7f19eb5cbc37f64e171218cf"
)
