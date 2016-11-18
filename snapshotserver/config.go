package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func SetConfigDefaults() {

	// These defaults are already set in main.go for the 'flag' packages
	// However, we will set them again here to retain them if/when the
	// old CLI flags are removed.
	viper.SetDefault("port", -1)
	viper.SetDefault("securePort", -1)
	viper.SetDefault("mgmtPort", -1)

	viper.SetDefault("pgURL", "")
	viper.SetDefault("key", "")
	viper.SetDefault("cert", "")

	viper.SetDefault("debug", false)
	viper.SetDefault("help", false)
}

func GetConfig(goflags *flag.FlagSet) error {

	// Set some, hopefully sane, defaults
	SetConfigDefaults()

	// Parse legacy GO Flags in to pflags ready for Viper
	pflag.CommandLine.AddGoFlagSet(goflags)
	pflag.Parse()

	// Bind legacy GO Flags to viper to maintain backwards compatibility
	viper.BindPFlag("port", pflag.Lookup("p"))
	viper.BindPFlag("securePort", pflag.Lookup("sp"))
	viper.BindPFlag("mgmtPort", pflag.Lookup("mp"))

	viper.BindPFlag("pgURL", pflag.Lookup("u"))
	viper.BindPFlag("key", pflag.Lookup("key"))
	viper.BindPFlag("cert", pflag.Lookup("cert"))

	viper.BindPFlag("configFile", pflag.Lookup("C"))
	viper.BindPFlag("debug", pflag.Lookup("D"))
	viper.BindPFlag("help", pflag.Lookup("h"))

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

	viper.BindEnv("port")
	viper.BindEnv("securePort")
	viper.BindEnv("mgmtPort")

	viper.BindEnv("pgURL")
	viper.BindEnv("key")
	viper.BindEnv("cert")

	viper.BindEnv("configFile")
	viper.BindEnv("debug")
	viper.BindEnv("help")

	return nil

}
