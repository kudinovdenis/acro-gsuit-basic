package acronis_gmail

import (
	"testing"
)

const email = "monica.geller@trueimage.eu"

func TestGmailClient_BackupIndividualMessages(t *testing.T) {
	testBackup(t, func(client *GmailClient, account string) error {
		return client.Backup()
	})
}

func TestGmailClient_BackupIncrementally(t *testing.T) {
	testBackup(t, func(client *GmailClient, account string) error {
		return client.BackupIncrementally( "./"+email+"/backup", "./"+email+"/backup.json")
	})
}

func TestGmailClient_RestoreIndividualMessages(t *testing.T) {
	testRestore(t, func(client *GmailClient, account, pathToBackup string) error {
		return client.Restore("./"+email+"/backup")
	})
}

func testBackup(t *testing.T, backupFun func(*GmailClient, string) error) {
	client, err := Init(email)
	if err != nil {
		t.Errorf("Failed to Create service backup %s, err: %v", email, err.Error())
		t.FailNow()
	}

	err = backupFun(client, email)
	if err != nil {
		t.Errorf("Failed to backup, err: %v", err.Error())
		t.FailNow()
	}
}

func testRestore(t *testing.T, restoreFun func(*GmailClient, string, string) error) {
	client, err := Init(email)
	if err != nil {
		t.Errorf("Failed to create the service: %s", err.Error())
		t.FailNow()
	}

	err = restoreFun(client, email, "./"+email+"/backup/")
	if err != nil {
		t.Errorf("Failed to restore the messages: %s", err.Error())
		t.FailNow()
	}
}
