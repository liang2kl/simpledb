load("@simpledb_service_dep//:requirements.bzl", "requirement")

py_binary(
    name = "main",
    srcs = glob(["*.py"]),
    deps = [
        "//:simpledb_service_py",
        requirement("prompt_toolkit"),
        requirement("Pygments"),
        requirement("prettytable"),
    ],
    visibility = ["//visibility:public"]
)
