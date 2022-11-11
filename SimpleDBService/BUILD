load("@rules_proto_grpc//cpp:defs.bzl", "cpp_grpc_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "query_proto",
    srcs = ["query.proto"],
)

cpp_grpc_library(
    name = "simpledb_service",
    protos = [":query_proto"],
)
