package acronis_gmail

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"

	sched "git.acronis.com/scm/~alexander.gazarov/robust-networking"
	"github.com/kudinovdenis/logger"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"net/http"
	"net/http/httputil"
	"strconv"
	"time"
)

type LoggingTransport struct {
	delegate http.RoundTripper
}

func (transport *LoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	data, err := httputil.DumpRequest(req, true)
	if err != nil {
		println("Error while reading request")
		return nil, err
	} else {
		println(string(data))
	}

	response, err := transport.delegate.RoundTrip(req)
	if err != nil {
		return response, err
	}

	data, err = httputil.DumpResponse(response, true)
	if err != nil {
		println("Error while reading response")
		return nil, err
	} else {
		println(string(data))
	}

	return response, nil
}

const (
	throttlingChannelId   = "Gmail"
	bucketIdPerDay        = "PerDay"
	bucketIdPer100Seconds = "Per100Seconds"
)

const costMessagesList = 5
const costMessagesGet = 5
const costMessagesInsert = 25

var costMapMessagesList = map[sched.BucketId]int{bucketIdPerDay: costMessagesList}
var costMapMessagesGet = map[sched.BucketId]int{bucketIdPerDay: costMessagesGet}
var costMapMessagesInsert = map[sched.BucketId]int{bucketIdPerDay: costMessagesInsert}

type GmailClient struct {
	service   *gmail.Service
	scheduler sched.Scheduler
	account   string
}

type NetworkActionSequenceGenerator struct {
	retryAction      sched.Action
	maxRetries       int
	errorStatusCodes map[int]struct{}
}

func (generator *NetworkActionSequenceGenerator) NewActionSequence(id sched.ThrottlingChannelId,
	bucketCostMap map[sched.BucketId]int) sched.ActionSequence {
	return &NetworkActionSequence{generator, 0}
}

func NewNetworkActionSequenceGenerator(delay time.Duration, maxRetries int, errorStatusCodes map[int]struct{}) *NetworkActionSequenceGenerator {
	return &NetworkActionSequenceGenerator{
		sched.Action{sched.ActionTypeRetry, delay}, maxRetries, errorStatusCodes,
	}
}

type NetworkActionSequence struct {
	generator *NetworkActionSequenceGenerator
	retries   int
}

func (sequence *NetworkActionSequence) GetNextAction(result interface{}, err error) sched.Action {
	if err != nil {
		var errorCode int
		if googleErr, ok := err.(*googleapi.Error); !ok {
			return sched.ActionReturn
		} else {
			errorCode = googleErr.Code
		}

		if _, present := sequence.generator.errorStatusCodes[errorCode]; !present {
			return sched.ActionReturn
		}
	}

	sequence.retries += 1
	if err == nil || sequence.retries > sequence.generator.maxRetries {
		return sched.ActionReturn
	} else {
		return sequence.generator.retryAction
	}
}

func Init(account string) (*GmailClient, error) {
	client := GmailClient{}
	client.account = account

	throttlingConfig := map[sched.ThrottlingChannelId]sched.ThrottlingChannelConfig{throttlingChannelId: {
		map[sched.BucketId]sched.BucketConfig{
			bucketIdPerDay:        {TimePeriod: 24 * time.Hour, MaxUnits: 1000000000},
			bucketIdPer100Seconds: {TimePeriod: 100 * time.Second, MaxUnits: 2000000},
		},
		&sched.BucketConfig{TimePeriod: 100 * time.Second, MaxUnits: 25000},
	}}
	actionSequenceGenerator := NewNetworkActionSequenceGenerator(1*time.Second, 10, map[int]struct{}{500: {}})
	client.scheduler = sched.NewScheduler(sched.Policy{throttlingConfig, actionSequenceGenerator})

	httpClient := &http.Client{}
	httpClient.Transport = &LoggingTransport{
		http.DefaultTransport,
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

	jsonData, err := ioutil.ReadFile("./Acronis-backup-project-8b80e5be7c37.json")
	if err != nil {
		return nil, err
	}

	data, err := google.JWTConfigFromJSON(jsonData, gmail.GmailModifyScope)

	if err != nil {
		logger.Logf(logger.LogLevelError, "JWT Config failed, %v", err)
		return nil, err
	}

	data.Subject = account

	client.service, err = gmail.New(data.Client(ctx))
	if err != nil {
		logger.Logf(logger.LogLevelError, "New Gmail failed, %v", err)
		return nil, err
	}

	return &client, nil
}

func (client *GmailClient) Backup() (err error) {
	pathToBackup := "./backups/gmail/" + client.account + "/backup/"

	err = os.RemoveAll(pathToBackup)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Couldn't clear the backup folder, %v", err)
		return
	}

	err = os.MkdirAll(pathToBackup, 0777)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Directory create failed, %v", err)
		return
	}

	nextPageToken := ""
	var latestHistoryId uint64
	for {
		listCall := client.service.Users.Messages.List(client.account)
		if nextPageToken != "" {
			listCall.PageToken(nextPageToken)
		}

		result, err := client.scheduler.CallWithCostAndDynamicId(func(interface{}, error) (interface{}, error) {
			return listCall.Do(googleapi.QuotaUser(client.account))
		}, throttlingChannelId, costMapMessagesGet, client.account, costMessagesGet)
		messages := result.(*gmail.ListMessagesResponse)

		if err != nil {
			logger.Logf(logger.LogLevelError, "Message list Get failed , %v", err)
			return err
		}
		logger.Logf(logger.LogLevelDefault, "Got Message Count %v", len(messages.Messages))

		for _, mes := range messages.Messages {
			historyId, err := client.saveMessage(pathToBackup, mes.Id)
			if err != nil {
				return err
			}

			if historyId > latestHistoryId {
				latestHistoryId = historyId
			}
		}

		nextPageToken = messages.NextPageToken
		if nextPageToken == "" {
			break
		}
	}

	logger.Logf(logger.LogLevelDefault, "Latest history id: %d", latestHistoryId)
	return client.writeLatestHistoryId(latestHistoryId)
}

func (client *GmailClient) saveMessage(pathToBackup, messageId string) (uint64, error) {
	logger.Logf(logger.LogLevelDefault, "Started message w/ ID : %v", messageId)
	mc := client.service.Users.Messages.Get(client.account, messageId)
	mc = mc.Format("raw")

	result, err := client.scheduler.CallWithCostAndDynamicId(func(interface{}, error) (interface{}, error) {
		return mc.Do(googleapi.QuotaUser(client.account))
	}, throttlingChannelId, costMapMessagesList, client.account, costMessagesList)
	m := result.(*gmail.Message)

	if err != nil {
		logger.Logf(logger.LogLevelError, "Message Get failed , %v", err)
		return 0, err
	}
	logger.Logf(logger.LogLevelDefault, "Message snippet : %s", m.Snippet)

	marshalled, err := m.MarshalJSON()
	pb := pathToBackup + m.Id

	err = ioutil.WriteFile(pb, marshalled, 0777)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Write to File failed, %v", err)
		return 0, err
	}

	logger.Logf(logger.LogLevelDefault, "Ended message w/ ID : %v", messageId)
	return m.HistoryId, nil
}

func (client *GmailClient) writeLatestHistoryId(latestHistoryId uint64) error {
	data := []byte(strconv.FormatUint(latestHistoryId, 10))
	return ioutil.WriteFile("./backups/gmail/"+client.account+"/backup.json", data, os.ModePerm)
}

func (client *GmailClient) Restore(pathToBackup string) error {
	latestHistoryId, err := client.restoreMessages(pathToBackup)
	if err != nil {
		return err
	}

	return client.writeLatestHistoryId(latestHistoryId)
}

func (client *GmailClient) restoreMessages(pathToBackup string) (latestHistoryId uint64, err error) {
	fileList, err := createExistingFileList(pathToBackup)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to create file list: %v, err: %v", pathToBackup, err.Error())
		return
	}

	var lastMessageId string
	for _, fileName := range fileList {
		lastMessageId, err = client.restoreMessage(pathToBackup+"/"+fileName)
		if err != nil {
			logger.Logf(logger.LogLevelError, "Failed to restore thread id: %v, err: %v", fileName, err.Error())
			return
		}
	}

	message, err := client.service.Users.Messages.Get(client.account, lastMessageId).Format("metadata").Do()
	if err != nil {
		logger.Logf(logger.LogLevelError, "Message Get failed , %v", err)
		return 0, err
	}

	latestHistoryId = message.HistoryId
	return
}

func (client *GmailClient) restoreMessage(pathToMsg string) (messageId string, err error) {
	raw, err := ioutil.ReadFile(pathToMsg)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to restore message path: %v, err: %v", pathToMsg, err.Error())
		return
	}

	var msg = &gmail.Message{}

	err = json.Unmarshal(raw, msg)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to unmarshal message path: %v, err: %v", pathToMsg, err.Error())
		return "", err
	}

	var m = &gmail.Message{}
	m.Raw = msg.Raw
	m.LabelIds = msg.LabelIds
	m.ThreadId = msg.ThreadId

	ic := client.service.Users.Messages.Insert(client.account, m)

	result, err := client.scheduler.CallWithCostAndDynamicId(func(interface{}, error) (interface{}, error) {
		return ic.Do(googleapi.QuotaUser(client.account))
	}, throttlingChannelId, costMapMessagesInsert, client.account, costMessagesInsert)
	message := result.(*gmail.Message)

	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to restore message path: %v, err: %v", pathToMsg, err.Error())
		return "", err
	}

	logger.Logf(logger.LogLevelDefault, "Inserted msg: %v", message)
	messageId = message.Id
	return
}

func (client *GmailClient) BackupIncrementally(pathToBackup, pathToBackupDescriptor string) error {
	data, err := ioutil.ReadFile(pathToBackupDescriptor)
	if err != nil {
		return err
	}

	var lastHistoryId uint64
	lastHistoryId, err = strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return err
	}

	return client.backupIncrementally(pathToBackup, lastHistoryId)
}

func (client *GmailClient) backupIncrementally(pathToBackup string, lastHistoryId uint64) error {
	existingMessages, err := createExistingFileList(pathToBackup)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Failed to create file list: %v, err: %v", pathToBackup, err.Error())
		return err
	}

	existingMessagesSet := make(map[string]struct{})
	for _, message := range existingMessages {
		existingMessagesSet[message] = struct{}{}
	}

	statuses, err := client.createChangeSetFromHistory(lastHistoryId, existingMessagesSet)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Couldn't create a status list, err: %v", err.Error())
		return err
	}

	for message, status := range statuses {
		logger.Logf(logger.LogLevelDefault, "Message %s: %d", message, status)
		switch status {
		case statusAdded:
			client.saveMessage(pathToBackup, message)
		case statusRemoved:
			os.Remove(pathToBackup + "/" + message)
		}
	}

	return nil
}

func createExistingFileList(pathToFolder string) (fileNames []string, err error) {
	dir, err := os.Open(pathToFolder)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Directory open failed, %v", err)
		return
	}
	defer dir.Close()

	fileList, err := dir.Readdir(-1)
	if err != nil {
		logger.Logf(logger.LogLevelError, "Directory open failed, %v", err)
		return
	}

	for _, file := range fileList {
		if !file.IsDir() {
			logger.Logf(logger.LogLevelDefault, "Found file: %v", file.Name())
			fileNames = append(fileNames, file.Name())
		}
	}

	return
}
