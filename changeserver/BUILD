package(default_visibility = ["//visibility:public"])
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_test", "go_prefix")

go_prefix("github.com/apigee-labs/transicator/changeserver")

go_binary(
  name = "server",
  srcs = [
    "changesapi.go",
    "changetracker.go",
    "cleanup.go",
    "config.go",
    "diagapi.go",
    "encoding.go",
    "errors.go",
    "main.go",
    "replicator.go",
    "server.go",
    ],
  deps = [
    "//common:go_default_library",
    "//replication:go_default_library",
    "//storage:go_default_library",
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
    "changes_api_test.go",
    "changetracker_test.go",
    "diagapi_test.go",
    "misc_test.go",
    "server_main_test.go",
    "tracker_perf_test.go",
  ],
  deps = [
    "@com_github_onsi_ginkgo//:go_default_library",
    "@com_github_onsi_ginkgo//reporters:go_default_library",
    "@com_github_onsi_gomega//:go_default_library",
  ]
)
