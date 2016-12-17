/*
Copyright 2016 The Transicator Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package snapshotserver

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func SetConfigDefaults() {
	pflag.IntP("port", "p", -1, "HTTP Binding port")
	viper.SetDefault("port", -1)
	pflag.IntP("secureport", "t", -1, "HTTPS listen port")
	viper.SetDefault("securePort", -1)
	pflag.Int("mgmtport", -1, "Management port (for health checks)")
	viper.SetDefault("mgmtPort", -1)

	pflag.StringP("pgurl", "u", "", "URL to connect to Postgres DB")
	viper.SetDefault("pgURL", "")

	pflag.String("key", "", "TLS key file (must be unencrypted)")
	viper.SetDefault("key", "")
	pflag.String("cert", "", "TLS certificate file")
	viper.SetDefault("cert", "")

	pflag.StringP("selectorcolumn", "S", "", "Set selector column")
	viper.SetDefault("selectorColumn", defaultSelectorColumn)

	pflag.StringP("tempdir", "T", "", "Set temporary directory for snapshot files")
	viper.SetDefault("tempdir", defaultTempDir)

	pflag.BoolP("config", "C", false, fmt.Sprintf("Use a config file named '%s' located in either /etc/%s/, ~/.%s or ./)", appName, packageName, packageName))
	pflag.BoolP("debug", "D", false, "Turn on debugging")
	viper.SetDefault("debug", false)
}

func getConfig() error {
	viper.BindPFlag("port", pflag.Lookup("port"))
	viper.BindPFlag("securePort", pflag.Lookup("secureport"))
	viper.BindPFlag("mgmtPort", pflag.Lookup("mgmtport"))

	viper.BindPFlag("pgURL", pflag.Lookup("pgurl"))
	viper.BindPFlag("key", pflag.Lookup("key"))
	viper.BindPFlag("cert", pflag.Lookup("cert"))

	viper.BindPFlag("configFile", pflag.Lookup("config"))
	viper.BindPFlag("debug", pflag.Lookup("debug"))
	viper.BindPFlag("help", pflag.Lookup("help"))
	viper.BindPFlag("selectorColumn", pflag.Lookup("selectorcolumn"))
	viper.BindPFlag("tempdir", pflag.Lookup("tempdir"))

	// Load config values from file
	if viper.GetBool("configFile") {
		viper.SetConfigName(appName)                                               // name of config file (without extension)
		viper.AddConfigPath(fmt.Sprintf("/etc/%s/", packageName))                  // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.%s", os.Getenv("HOME"), packageName)) // loof for config in the users home directory
		viper.AddConfigPath(".")                                                   // look for config in the working directory
		err := viper.ReadInConfig()                                                // Find and read the config file
		if err != nil {                                                            // Handle errors reading the config file
			return err
		}
	}

	// Load any config values from Environment variables who's name is prefixed TSS_ (Transicator Snaphot Server)
	viper.SetEnvPrefix("tss") // will be uppercased automatically
	viper.AutomaticEnv()

	return nil

}
