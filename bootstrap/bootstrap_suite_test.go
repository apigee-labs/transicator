package bootstrap_test

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
	"testing"
)

func TestBootstrap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bootstrap Suite")
}

var backupURI, restoreURI string

var _ = BeforeSuite(func() {
	log.SetLevel(log.DebugLevel)

	err := viper.BindEnv("region", "REGION")
	Expect(err).NotTo(HaveOccurred())
	err = viper.BindEnv("projectID", "PROJECT_ID")
	Expect(err).NotTo(HaveOccurred())

	region := viper.GetString("region")
	projectID := viper.GetString("projectID")

	if region == "" || projectID == "" {
		Fail("Set env vars: REGION and PROJECT_ID before running test")
	}

	backupURI = fmt.Sprintf("https://%s-%s.cloudfunctions.net/bootstrapBackup", region, projectID)
	restoreURI = fmt.Sprintf("https://%s-%s.cloudfunctions.net/bootstrapRestore", region, projectID)
})
