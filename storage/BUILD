package(default_visibility = ["//visibility:public"])
load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test", "go_prefix")

go_prefix("github.com/apigee-labs/transicator/storage")

go_library(
  name = "go_default_library",
  srcs = [
      "comparator.go",
      "convert.go",
      "sql.go",
      "storage_sqlite.go",
      "storage.go",
    ],
  deps = [
    "//common:go_default_library",
    "@com_github_sirupsen_logrus//:go_default_library",
    "@com_github_mattn_go-sqlite3//:go_default_library",
    "@org_golang_x_net//context:go_default_library",
  ]
)

go_test(
  name = "test",
  srcs = [
    "convert_test.go",
    "storage_main_test.go",
    "storage_test.go",
    "storage_bench_test.go",
  ],
  library = ":go_default_library",
  deps = [
    "@com_github_onsi_ginkgo//:go_default_library",
    "@com_github_onsi_ginkgo//reporters:go_default_library",
    "@com_github_onsi_gomega//:go_default_library",
    "@in_gopkg_yaml_v2//:go_default_library",
  ]
)
