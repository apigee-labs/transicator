package(default_visibility = ["//visibility:public"])
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_test", "go_prefix")

go_prefix("github.com/apigee-labs/transicator/snapshotserver")

go_binary(
  name = "server",
  srcs = [
    "config.go",
    "main.go",
    "snapshot.go",
    ],
  deps = [
    "//common:go_default_library",
    "//pgclient:go_default_library",
    "@com_github_30x_goscaffold//:go_default_library",
    "@com_github_julienschmidt_httprouter//:go_default_library",
    "@com_github_sirupsen_logrus//:go_default_library",
    "@com_github_spf13_pflag//:go_default_library",
    "@com_github_spf13_viper//:go_default_library",
  ],
)

go_test(
  name = "test",
  srcs = [
    "snapshot_info_test.go",
  ],
  deps = [
    "//common:go_default_library",
    "//pgclient:go_default_library",
    "@com_github_sirupsen_logrus//:go_default_library",
    "@com_github_onsi_ginkgo//:go_default_library",
    "@com_github_onsi_ginkgo//reporters:go_default_library",
    "@com_github_onsi_gomega//:go_default_library",
  ]
)
