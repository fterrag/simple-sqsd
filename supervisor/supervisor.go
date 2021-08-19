package supervisor

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

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

    HTTPAUTHORIZATIONHeader string
	HTTPAUTHORIZATIONHeaderName string

	HTTPHMACHeader string
	HMACSecretKey  []byte
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

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
			MaxNumberOfMessages:   aws.Int64(int64(s.workerConfig.QueueMaxMessages)),
			QueueUrl:              aws.String(s.workerConfig.QueueURL),
			WaitTimeSeconds:       aws.Int64(int64(s.workerConfig.QueueWaitTime)),
			MessageAttributeNames: aws.StringSlice([]string{"All"}),
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
			res, err := s.httpRequest(msg)
			if err != nil {
				s.logger.Errorf("Error making HTTP request: %s", err)
				continue
			}

			if res.StatusCode < http.StatusOK || res.StatusCode > http.StatusIMUsed {

				if res.StatusCode == http.StatusTooManyRequests {
					sec, err := getRetryAfterFromResponse(res)
					if err != nil {
						s.logger.Errorf("Error getting retry after value from HTTP response: %s", err)
						continue
					}

					changeVisibilityEntries = append(changeVisibilityEntries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
						Id:                msg.MessageId,
						ReceiptHandle:     msg.ReceiptHandle,
						VisibilityTimeout: aws.Int64(sec),
					})
				}

				s.logger.Errorf("Non-successful status code: %d", res.StatusCode)

				continue

			}

			deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
				Id:            msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
			})

			s.logger.Debugf("Message %s successfully processed", *msg.MessageId)
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

func (s *Supervisor) httpRequest(msg *sqs.Message) (*http.Response, error) {
	body := *msg.Body
	req, err := http.NewRequest("POST", s.workerConfig.HTTPURL, bytes.NewBufferString(body))
	req.Header.Add("X-Aws-Sqsd-Msgid", *msg.MessageId)
	s.addMessageAttributesToHeader(msg.MessageAttributes, req.Header)
	if err != nil {
		return nil, fmt.Errorf("Error while creating HTTP request: %s", err)
	}

	if len(s.workerConfig.HMACSecretKey) > 0 {
		hmac, err := makeHMAC(strings.Join([]string{s.hmacSignature, body}, ""), s.workerConfig.HMACSecretKey)
		if err != nil {
			return nil, err
		}

		req.Header.Set(s.workerConfig.HTTPHMACHeader, hmac)
	}

	if len(s.workerConfig.HTTPAUTHORIZATIONHeader) > 0 {
		headerName := s.workerConfig.HTTPAUTHORIZATIONHeaderName
		if len(headerName) == 0 {
			headerName = "Authorization"
		}
		req.Header.Set(headerName, s.workerConfig.HTTPAUTHORIZATIONHeader)
	}

	if len(s.workerConfig.HTTPContentType) > 0 {
		req.Header.Set("Content-Type", s.workerConfig.HTTPContentType)
	}

	res, err := s.httpClient.Do(req)
	if err != nil {
		return res, err
	}

	res.Body.Close()

	return res, nil
}

func (s *Supervisor) addMessageAttributesToHeader(attrs map[string]*sqs.MessageAttributeValue, header http.Header) {
	for k, v := range attrs {
		header.Add("X-Aws-Sqsd-Attr-" + k, *v.StringValue)
	}
}

func makeHMAC(signature string, secretKey []byte) (string, error) {
	mac := hmac.New(sha256.New, secretKey)

	_, err := mac.Write([]byte(signature))
	if err != nil {
		return "", fmt.Errorf("Error while writing HMAC: %s", err)
	}

	return hex.EncodeToString(mac.Sum(nil)), nil
}

func getRetryAfterFromResponse(res *http.Response) (int64, error) {
	retryAfter := res.Header.Get("Retry-After")
	if len(retryAfter) == 0 {
		return 0, errors.New("Retry-After header value is empty")
	}

	seconds, err := strconv.ParseInt(retryAfter, 10, 0)
	if err != nil {
		return 0, err
	}

	return seconds, nil
}
