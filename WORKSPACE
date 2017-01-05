# Load Go extensions to Bazel

git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go.git",
    commit = "f65dfc8b3579525ef217e91caecd1bb365498514",
)
load("@io_bazel_rules_go//go:def.bzl", "go_repositories", "go_repository", "new_go_repository")

go_repositories()

# Internal External dependencies

new_go_repository(
  name = "com_github_30x_goscaffold",
  importpath = "github.com/30x/goscaffold",
  commit = "bc27656c53f79500b27ef1648c3a12ac8d06a084",
)

# External dependencies

new_go_repository(
  name = "com_github_sirupsen_logrus",
  importpath = "github.com/Sirupsen/logrus",
  commit = "4b6ea7319e214d98c938f12692336f7ca9348d6b",
)

new_go_repository(
  name = "com_github_golang_protobuf",
  importpath = "github.com/golang/protobuf",
  commit = "8ee79997227bf9b34611aee7946ae64735e6fd93",
)

new_go_repository(
  name = "com_github_mattn_go-sqlite3",
  importpath = "github.com/mattn/go-sqlite3",
  commit = "ca5e3819723d8eeaf170ad510e7da1d6d2e94a08",
)

new_go_repository(
  name = "org_golang_x_net",
  importpath = "golang.org/x/net",
  commit = "cbed01e851ceabac8eab72f5e0f1086ecbc60338",
)

new_go_repository(
  name = "com_github_spf13_pflag",
  importpath = "github.com/spf13/pflag",
  commit = "5ccb023bc27df288a957c5e994cd44fd19619465",
)

new_go_repository(
  name = "com_github_spf13_viper",
  importpath = "github.com/spf13/viper",
  commit = "651d9d916abc3c3d6a91a12549495caba5edffd2",
)

new_go_repository(
  name = "com_github_fsnotify_fsnotify",
  importpath = "github.com/fsnotify/fsnotify",
  commit = "fd9ec7deca8bf46ecd2a795baaacf2b3a9be1197",
)

new_go_repository(
  name = "com_github_hashicorp_hcl",
  importpath = "github.com/hashicorp/hcl",
  commit = "37ab263305aaeb501a60eb16863e808d426e37f2",
)

new_go_repository(
  name = "com_github_julienschmidt_httprouter",
  importpath = "github.com/julienschmidt/httprouter",
  commit = "8c199fb6259ffc1af525cc3ad52ee60ba8359669",
)

new_go_repository(
  name = "com_github_magiconair_properties",
  importpath = "github.com/magiconair/properties",
  commit = "9c47895dc1ce54302908ab8a43385d1f5df2c11c",
)

new_go_repository(
  name = "com_github_mitchellh_mapstructure",
  importpath = "github.com/mitchellh/mapstructure",
  commit = "5a0325d7fafaac12dda6e7fb8bd222ec1b69875e",
)

new_go_repository(
  name = "com_github_pelletier_go_buffruneio",
  importpath = "github.com/pelletier/go-buffruneio",
  commit = "df1e16fde7fc330a0ca68167c23bf7ed6ac31d6d",
)

new_go_repository(
  name = "com_github_pelletier_go_toml",
  importpath = "github.com/pelletier/go-toml",
  commit = "ce7be745f09fe4ff89af8e3ea744e1deabf20ee3",
)

new_go_repository(
  name = "com_github_spf13_afero",
  importpath = "github.com/spf13/afero",
  commit = "2f30b2a92c0e5700bcfe4715891adb1f2a7a406d",
)

new_go_repository(
  name = "com_github_spf13_cast",
  importpath = "github.com/spf13/cast",
  commit = "24b6558033ffe202bf42f0f3b870dcc798dd2ba8",
)

new_go_repository(
  name = "com_github_spf13_jwalterweatherman",
  importpath = "github.com/spf13/jwalterweatherman",
  commit = "33c24e77fb80341fe7130ee7c594256ff08ccc46",
)

new_go_repository(
  name = "org_golang_x_sys",
  importpath = "golang.org/x/sys",
  commit = "478fcf54317e52ab69f40bb4c7a1520288d7f7ea",
)

new_go_repository(
  name = "org_golang_x_text",
  importpath = "golang.org/x/text",
  commit = "fd889fe3a20f4878f5f47672fd3ca5b86db005e2",
  build_file_name = "BUILD.bazel",
)

# For testing

new_go_repository(
  name = "com_github_onsi_ginkgo",
  importpath = "github.com/onsi/ginkgo",
  commit = "00054c0bb96fc880d4e0be1b90937fad438c5290",
)

new_go_repository(
  name = "com_github_onsi_gomega",
  importpath = "github.com/onsi/gomega",
  commit = "f1f0f388b31eca4e2cbe7a6dd8a3a1dfddda5b1c",
)

new_go_repository(
  name = "in_gopkg_yaml_v2",
  importpath = "gopkg.in/yaml.v2",
  commit = "a5b47d31c556af34a302ce5d659e6fea44d90de0",
)
