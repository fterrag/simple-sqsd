package supervisor

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	log "github.com/sirupsen/logrus"
)

type Supervisor struct {
	sync.Mutex

	logger        *log.Entry
	sqs           sqsiface.SQSAPI
	httpClient    httpClient
	workerConfig  WorkerConfig
	hmacSignature string

	startOnce sync.Once
	wg        sync.WaitGroup

	shutdown bool
}

type WorkerConfig struct {
	QueueURL         string
	QueueMaxMessages int
	QueueWaitTime    int

	HTTPURL         string
	HTTPContentType string

	HTTPHMACHeader string
	HMACSecretKey  []byte
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type (
	DeleteMessage bool
	ChangeVisibility int64
)

func NewSupervisor(logger *log.Entry, sqs sqsiface.SQSAPI, httpClient httpClient, config WorkerConfig) *Supervisor {
	return &Supervisor{
		logger:        logger,
		sqs:           sqs,
		httpClient:    httpClient,
		workerConfig:  config,
		hmacSignature: fmt.Sprintf("POST %s\n", config.HTTPURL),
	}
}

func (s *Supervisor) Start(numWorkers int) {
	s.startOnce.Do(func() {
		s.wg.Add(numWorkers)

		for i := 0; i < numWorkers; i++ {
			go s.worker()
		}
	})
}

func (s *Supervisor) Wait() {
	s.wg.Wait()
}

func (s *Supervisor) Shutdown() {
	defer s.Unlock()
	s.Lock()

	s.shutdown = true
}

func (s *Supervisor) worker() {
	defer s.wg.Done()

	s.logger.Info("Starting worker")

	for {
		if s.shutdown {
			return
		}

		recInput := &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: aws.Int64(int64(s.workerConfig.QueueMaxMessages)),
			QueueUrl:            aws.String(s.workerConfig.QueueURL),
			WaitTimeSeconds:     aws.Int64(int64(s.workerConfig.QueueWaitTime)),
		}

		output, err := s.sqs.ReceiveMessage(recInput)
		if err != nil {
			s.logger.Errorf("Error while receiving messages from the queue: %s", err)
			continue
		}

		if len(output.Messages) == 0 {
			continue
		}

		deleteEntries := make([]*sqs.DeleteMessageBatchRequestEntry, 0)
		changeVisibilityEntries := make([]*sqs.ChangeMessageVisibilityBatchRequestEntry, 0)

		for _, msg := range output.Messages {
			result := s.httpRequest(*msg.Body)

			switch val := result.(type) {
			case DeleteMessage:
				deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
					Id:            msg.MessageId,
					ReceiptHandle: msg.ReceiptHandle,
				})
			case ChangeVisibility:
				changeVisibilityEntries = append(changeVisibilityEntries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
					Id:                msg.MessageId,
					ReceiptHandle:     msg.ReceiptHandle,
					VisibilityTimeout: aws.Int64(int64(val)),
				})
			case error:
				s.logger.Errorf("Error while making HTTP request: %s", val)
			}
		}

		if len(deleteEntries) > 0 {
			delInput := &sqs.DeleteMessageBatchInput{
				Entries:  deleteEntries,
				QueueUrl: aws.String(s.workerConfig.QueueURL),
			}

			_, err = s.sqs.DeleteMessageBatch(delInput)
			if err != nil {
				s.logger.Errorf("Error while deleting messages from SQS: %s", err)
			}
		}

		if len(changeVisibilityEntries) > 0 {
			changeVisibilityInput := &sqs.ChangeMessageVisibilityBatchInput{
				Entries:  changeVisibilityEntries,
				QueueUrl: aws.String(s.workerConfig.QueueURL),
			}

			_, err = s.sqs.ChangeMessageVisibilityBatch(changeVisibilityInput)
			if err != nil {
				s.logger.Errorf("Error while changing visibility on messages from SQS: %s", err)
			}
		}
	}
}

func (s *Supervisor) httpRequest(body string) interface{} {
	req, err := http.NewRequest("POST", s.workerConfig.HTTPURL, bytes.NewBufferString(body))
	if err != nil {
		return fmt.Errorf("Error while creating HTTP request: %s", err)
	}

	if len(s.workerConfig.HMACSecretKey) > 0 {
		hmac, err := makeHMAC(strings.Join([]string{s.hmacSignature, body}, ""), s.workerConfig.HMACSecretKey)
		if err != nil {
			return err
		}

		req.Header.Set(s.workerConfig.HTTPHMACHeader, hmac)
	}

	if len(s.workerConfig.HTTPContentType) > 0 {
		req.Header.Set("Content-Type", s.workerConfig.HTTPContentType)
	}

	res, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Error while making HTTP request: %s", err)
	}

	res.Body.Close()

	if res.StatusCode < http.StatusOK || res.StatusCode > http.StatusIMUsed {
		if res.StatusCode == http.StatusTooManyRequests {
			waitTime, err := getAwaitTimeFromHeader(res)
			if err != nil {
				return fmt.Errorf("Error while getting response header %s", err)
			}

			return ChangeVisibility(waitTime)
		}

		return fmt.Errorf("Non-Success status code received")
	}

	return DeleteMessage(true)
}

func makeHMAC(signature string, secretKey []byte) (string, error) {
	mac := hmac.New(sha256.New, secretKey)

	_, err := mac.Write([]byte(signature))
	if err != nil {
		return "", fmt.Errorf("Error while writing HMAC: %s", err)
	}

	return hex.EncodeToString(mac.Sum(nil)), nil
}

func getAwaitTimeFromHeader(res *http.Response) (int64, error) {
	retryAfter := res.Header.Get("Retry-After")
	if retryAfter == "" {
		return 0, fmt.Errorf("Unexpected value provided on Retry-After header \n")
	}

	seconds, err := strconv.ParseInt(retryAfter, 10, 0)
	if err == nil {
		return seconds, nil
	}

	parsedTime, err := time.Parse(time.RFC1123, retryAfter)
	if err != nil {
		return 0, fmt.Errorf("Unexpected value provided on Retry-After header \n %s", err)
	}

	delayUntil := time.Until(parsedTime)
	return int64(delayUntil.Seconds()), nil
}
