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
package bootstrap

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/apigee-labs/transicator/storage"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

const (
	headerID     = "x-bootstrap-id"
	headerSecret = "x-bootstrap-secret"
	contentType  = "application/octet-stream"
)

type Error struct {
	Status  int
	Message string
}

func (e Error) Error() string {
	return fmt.Sprintf("backup error, status: %d, message: %s", e.Status, e.Message)
}

var client = http.Client{Timeout: 3 * time.Minute}

// note: current behavior is to always make the backup and then just allow the send to fail
// if the server doesn't want it.
// note: err returned is nil and uploaded is false if backup was rejected as being too soon
func Backup(db storage.DB, uri, id, secret string) (err error, uploaded bool) {

	// make backup
	tempDirURL, err := ioutil.TempDir("", "transicator_sqlite_backup")
	if err != nil {
		return err, false
	}
	defer os.RemoveAll(tempDirURL)
	backupDirName := fmt.Sprintf("%s/backup", tempDirURL)

	bc := db.Backup(backupDirName)
	for {
		br := <-bc
		log.Debugf("Backup %d remaining\n", br.PagesRemaining)
		if err = br.Error; err != nil {
			log.Error(err)
			return err, false
		}
		if br.Done {
			break
		}
	}

	backupFileName := fmt.Sprintf("%s/transicator", backupDirName)
	fileReader, err := os.Open(backupFileName)
	if err != nil {
		log.Error(err)
		return err, false
	}
	defer fileReader.Close()

	// send backup
	req, err := http.NewRequest("POST", uri, fileReader)
	if err != nil {
		return err, false
	}
	req.Header.Set(headerID, id)
	req.Header.Set(headerSecret, secret)
	req.Header.Set("Content-Type", contentType)

	res, err := client.Do(req)
	if err != nil {
		return err, false
	}
	defer req.Body.Close()

	switch res.StatusCode {
	case 200:
		log.Debug("backup uploaded successfully")
		return nil, true
	case 429:
		log.Debug("backup upload not needed")
		return nil, false
	default:
		msg, _ := ioutil.ReadAll(res.Body)
		return Error{
			res.StatusCode,
			string(msg),
		}, false
	}
}

func Restore(dbDir, uri, id, secret string) error {

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return err
	}
	req.Header.Set(headerID, id)
	req.Header.Set(headerSecret, secret)
	req.Header.Set("Accept", contentType)

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		msg, _ := ioutil.ReadAll(res.Body)
		return Error{
			res.StatusCode,
			string(msg),
		}
	}

	return storage.RestoreBackup(res.Body, dbDir)
}
