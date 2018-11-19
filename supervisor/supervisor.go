package supervisor

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
	"strconv"
	"net/http"
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

	HTTPURL                string
	HTTPContentType        string
	HTTPRetryAfterAttempts int

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

		for _, msg := range output.Messages {
			err := s.httpRequest(*msg.Body)
			if err != nil {
				s.logger.Errorf("Error while making HTTP request: %s", err)
				continue
			}

			deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
				Id:            msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
			})
		}

		if len(deleteEntries) == 0 {
			continue
		}

		delInput := &sqs.DeleteMessageBatchInput{
			Entries:  deleteEntries,
			QueueUrl: aws.String(s.workerConfig.QueueURL),
		}

		_, err = s.sqs.DeleteMessageBatch(delInput)
		if err != nil {
			s.logger.Errorf("Error while deleting messages from SQS: %s", err)
		}
	}
}

func (s *Supervisor) httpRequest(body string) error {
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

	retriesCount := s.workerConfig.HTTPRetryAfterAttempts
	return s.requestWithRetry(req, retryOnTooManyRequests(retriesCount))
}

type ShouldRetry = func (*http.Response) (bool, error)
func (s *Supervisor) requestWithRetry(req *http.Request, shouldRetry ShouldRetry) error {
	res, err := s.httpClient.Do(req)

	if err != nil {
		return fmt.Errorf("Error while making HTTP request: %s", err)
	}

	res.Body.Close()
	
	requestFailed := res.StatusCode < http.StatusOK || res.StatusCode > http.StatusIMUsed
	
	if requestFailed {
		retry, err := shouldRetry(res)
		if err != nil {
			return err
		}
		
		if retry {
			return s.requestWithRetry(req, shouldRetry)
		}

		return fmt.Errorf("Non-Success status code received")
	}

	return nil
}

func makeHMAC(signature string, secretKey []byte) (string, error) {
	mac := hmac.New(sha256.New, secretKey)

	_, err := mac.Write([]byte(signature))
	if err != nil {
		return "", fmt.Errorf("Error while writing HMAC: %s", err)
	}

	return hex.EncodeToString(mac.Sum(nil)), nil
}

func retryOnTooManyRequests(attempts int) ShouldRetry {
	retries := attempts

	return func (res *http.Response) (bool, error) {
		if retries == 0 {
			return false, nil
		}

		retries--
		retryAfter := res.Header.Get("Retry-After")

		if res.StatusCode != http.StatusTooManyRequests || retryAfter == "" {
			return false, nil
		}

		waitTime := time.Duration(1) * time.Second
		seconds, err := strconv.ParseInt(retryAfter, 10, 0)
		
		if err != nil {
			delayUntil, err := time.Parse(time.RFC1123, retryAfter)
			if err != nil {
				return false, fmt.Errorf("Unexpected value provided on Retry-After header \n %s", err)
			}

			waitTime = time.Until(delayUntil)
		} else {
			waitTime = time.Duration(seconds) * time.Second
		}
		
		time.Sleep(waitTime)
		return true, nil
	}
}
