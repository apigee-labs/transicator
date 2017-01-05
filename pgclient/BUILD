package(default_visibility = ["//visibility:public"])
load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test", "go_prefix")

go_prefix("github.com/apigee-labs/transicator/pgclient")

go_library(
  name = "go_default_library",
  srcs = [
    "connection.go",
    "connectstring.go",
    "copy.go",
    "driverconn.go",
    "driverstmt.go",
    "encoding.go",
    "fmtcommon.go",
    "messagetypes.go",
    "pginputtype_string.go",
    "pgoutputtype_string.go",
    "pgtype_string.go",
    "pgtypes.go",
    "query.go",
    ],
  deps = [
    "//common:go_default_library",
    "@com_github_sirupsen_logrus//:go_default_library",
  ]
)

go_test(
  name = "test",
  srcs = [
    "connection_test.go",
    "connectstring_test.go",
    "driver_test.go",
    "drivertypes_test.go",
    "encoding_test.go",
    "pgclient_main_test.go",
    "query_test.go",
  ],
  library = ":go_default_library",
  deps = [
    "@com_github_onsi_ginkgo//:go_default_library",
    "@com_github_onsi_ginkgo//reporters:go_default_library",
    "@com_github_onsi_gomega//:go_default_library",
    "@in_gopkg_yaml_v2//:go_default_library",
  ]
)
