package(default_visibility = ["//visibility:public"])
load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test", "go_prefix")

go_prefix("github.com/apigee-labs/transicator/common")

go_library(
  name = "go_default_library",
  srcs = [
    "common.go",
    "encoding.go",
    "error.go",
    "nbpipe.go",
    "operation_string.go",
    "sequence.go",
    "streaming.go",
    "transicator.pb.go",
    "value.go"
    ],
  deps = [
    "@com_github_golang_protobuf//proto:go_default_library",
  ]
)

go_test(
  name = "test",
  srcs = [
    "encoding_test.go",
    "nbpipe_test.go",
    "sequence_test.go",
    "streaming_test.go",
    "value_test.go",
  ],
  library = ":go_default_library",
  deps = [
    "@com_github_onsi_ginkgo//:go_default_library",
    "@com_github_onsi_ginkgo//reporters:go_default_library",
    "@com_github_onsi_gomega//:go_default_library",
    "@in_gopkg_yaml_v2//:go_default_library",
  ]
)
