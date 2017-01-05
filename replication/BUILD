package(default_visibility = ["//visibility:public"])
load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test", "go_prefix")

go_prefix("github.com/apigee-labs/transicator/replication")

go_library(
  name = "go_default_library",
  srcs = [
    "replication.go",
    "snapshot.go",
    "state_string.go",
    ],
  deps = [
    "//common:go_default_library",
    "//pgclient:go_default_library",
    "@com_github_sirupsen_logrus//:go_default_library",
  ]
)

go_test(
  name = "test",
  srcs = [
    "decoding_test.go",
    "replication_main_test.go",
    "replicator_test.go",
    "txid_test.go",
    "unit_test.go",
  ],
  library = ":go_default_library",
  deps = [
    "//common:go_default_library",
    "@com_github_onsi_ginkgo//:go_default_library",
    "@com_github_onsi_ginkgo//reporters:go_default_library",
    "@com_github_onsi_gomega//:go_default_library",
    "@in_gopkg_yaml_v2//:go_default_library",
  ]
)
