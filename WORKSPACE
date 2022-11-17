load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# googletest
http_archive(
    name = "googletest",
    urls = ["https://github.com/google/googletest/archive/44c03643cfbc649488a0f437cd18e05f11960d19.zip"],
    strip_prefix = "googletest-44c03643cfbc649488a0f437cd18e05f11960d19",
    sha256 = "b82dda7e5f9fe2addf972eee106f787b4966d398fcdb06bb0e9942a190e7cfc2"
)

new_local_repository(
    name = "usr_local",
    path = "/usr/local",
    build_file = "bazel/usr_local.BUILD",
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

# gflags
http_archive(
    name = "com_github_gflags_gflags",
    url = "https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz",
    strip_prefix = "gflags-2.2.2",
    sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf"
)

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
