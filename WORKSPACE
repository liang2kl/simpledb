load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# rules_foreign_cc
http_archive(
    name = "rules_foreign_cc",
    sha256 = "2a4d07cd64b0719b39a7c12218a3e507672b82a97b98c6a89d38565894cf7c51",
    strip_prefix = "rules_foreign_cc-0.9.0",
    url = "https://github.com/bazelbuild/rules_foreign_cc/archive/refs/tags/0.9.0.tar.gz",
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")
rules_foreign_cc_dependencies(register_built_tools = False)

# googletest
http_archive(
  name = "googletest",
  urls = ["https://github.com/google/googletest/archive/44c03643cfbc649488a0f437cd18e05f11960d19.zip"],
  strip_prefix = "googletest-44c03643cfbc649488a0f437cd18e05f11960d19",
  sha256 = "b82dda7e5f9fe2addf972eee106f787b4966d398fcdb06bb0e9942a190e7cfc2"
)

# antlr runtime sources
_ALL_CONTENT = """
filegroup(
    name = "all_srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
"""

http_archive(
    name = "antlr4-runtime",
    build_file_content = _ALL_CONTENT,
    strip_prefix = "antlr4-4.11.1",
    url = "https://github.com/antlr/antlr4/archive/refs/tags/4.11.1.tar.gz",
    sha256 = "81f87f03bb83b48da62e4fc8bfdaf447efb9fb3b7f19eb5cbc37f64e171218cf"
)

# rules_proto_grpc
http_archive(
    name = "rules_proto_grpc",
    url = "https://github.com/rules-proto-grpc/rules_proto_grpc/archive/refs/tags/4.2.0.tar.gz",
    strip_prefix = "rules_proto_grpc-4.2.0",
    sha256 = "bbe4db93499f5c9414926e46f9e35016999a4e9f6e3522482d3760dc61011070"
)
load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")
rules_proto_grpc_toolchains()
rules_proto_grpc_repos()
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")
grpc_extra_deps()
load("@rules_proto_grpc//cpp:repositories.bzl", rules_proto_grpc_cpp_repos = "cpp_repos")
rules_proto_grpc_cpp_repos()

# Hedron's Compile Commands Extractor for Bazel
# https://github.com/hedronvision/bazel-compile-commands-extractor
http_archive(
    name = "hedron_compile_commands",

    # Replace the commit hash in both places (below) with the latest, rather than using the stale one here.
    # Even better, set up Renovate and let it do the work for you (see "Suggestion: Updates" in the README).
    url = "https://github.com/hedronvision/bazel-compile-commands-extractor/archive/c200ce8b3e0baa04fa5cc3fb222260c9ea06541f.tar.gz",
    strip_prefix = "bazel-compile-commands-extractor-c200ce8b3e0baa04fa5cc3fb222260c9ea06541f",
    sha256 = "b0a8af42e06108ec62382703daf27f7d8d247fd1b930f249045c70cd9d22f72e"
)
load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")
hedron_compile_commands_setup()
