package supervisor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type mockSQS struct {
	sqsiface.SQSAPI

	receiveMessageFunc               func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	deleteMessageBatchFunc           func(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	changeMessageVisibilityBatchFunc func(*sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

func (m *mockSQS) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	if m.receiveMessageFunc != nil {
		return m.receiveMessageFunc(input)
	}

	return nil, nil
}

func (m *mockSQS) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	if m.deleteMessageBatchFunc != nil {
		return m.deleteMessageBatchFunc(input)
	}

	return nil, nil
}

func (m *mockSQS) ChangeMessageVisibilityBatch(input *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	if m.changeMessageVisibilityBatchFunc != nil {
		return m.changeMessageVisibilityBatchFunc(input)
	}

	return nil, nil
}

func TestSupervisorSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	log.SetOutput(ioutil.Discard)
	logger := log.WithFields(log.Fields{})
	mockSQS := &mockSQS{}
	config := WorkerConfig{
		HTTPURL:         ts.URL,
		HTTPContentType: "application/json",
	}

	mockSQS.receiveMessageFunc = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		return &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{{
				Body:          aws.String("message 1"),
				MessageId:     aws.String("m1"),
				ReceiptHandle: aws.String("r1"),
			}, {
				Body:          aws.String("message 2"),
				MessageId:     aws.String("m2"),
				ReceiptHandle: aws.String("r2"),
			}, {
				Body:          aws.String("message 3"),
				MessageId:     aws.String("m3"),
				ReceiptHandle: aws.String("r3"),
			}},
		}, nil
	}

	supervisor := NewSupervisor(logger, mockSQS, &http.Client{}, config)

	mockSQS.deleteMessageBatchFunc = func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
		defer supervisor.Shutdown()

		assert.Len(t, input.Entries, 3)

		return nil, nil
	}

	mockSQS.changeMessageVisibilityBatchFunc = func(input *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
		assert.Fail(t, "ChangeMessageVisibilityBatchFunc was called")
		return nil, nil
	}

	supervisor.Start(1)
	supervisor.Wait()
}

func TestSupervisorHTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	log.SetOutput(ioutil.Discard)
	logger := log.WithFields(log.Fields{})
	mockSQS := &mockSQS{}
	config := WorkerConfig{
		HTTPURL: ts.URL,
	}

	supervisor := NewSupervisor(logger, mockSQS, &http.Client{}, config)

	receiveCount := 0
	mockSQS.receiveMessageFunc = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		receiveCount++

		if receiveCount == 2 {
			supervisor.Shutdown()

			return &sqs.ReceiveMessageOutput{
				Messages: []*sqs.Message{},
			}, nil
		}

		return &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{{
				Body:          aws.String("message 1"),
				MessageId:     aws.String("m1"),
				ReceiptHandle: aws.String("r1"),
			}, {
				Body:          aws.String("message 2"),
				MessageId:     aws.String("m2"),
				ReceiptHandle: aws.String("r2"),
			}, {
				Body:          aws.String("message 3"),
				MessageId:     aws.String("m3"),
				ReceiptHandle: aws.String("r3"),
			}},
		}, nil
	}

	mockSQS.deleteMessageBatchFunc = func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
		assert.Fail(t, "DeleteMessageBatchInput was called")

		return nil, nil
	}

	mockSQS.changeMessageVisibilityBatchFunc = func(input *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
		assert.Fail(t, "ChangeMessageVisibilityBatchFunc was called")
		return nil, nil
	}

	supervisor.Start(1)
	supervisor.Wait()
}

func TestSupervisorHMAC(t *testing.T) {
	hmacHeader := "hmac"
	hmacSecretKey := []byte("foobar")
	hmacSuccess := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mac := hmac.New(sha256.New, hmacSecretKey)

		body, _ := ioutil.ReadAll(r.Body)
		r.Body.Close()

		mac.Write([]byte(fmt.Sprintf("%s %s\n%s", r.Method, fmt.Sprintf("http://%s", r.Host), string(body))))
		expectedMAC := hex.EncodeToString(mac.Sum(nil))

		hmacSuccess = hmac.Equal([]byte(r.Header.Get(hmacHeader)), []byte(expectedMAC))
	}))
	defer ts.Close()

	log.SetOutput(ioutil.Discard)
	logger := log.WithFields(log.Fields{})
	mockSQS := &mockSQS{}
	config := WorkerConfig{
		HTTPURL: ts.URL,

		HTTPHMACHeader: hmacHeader,
		HMACSecretKey:  hmacSecretKey,
	}

	supervisor := NewSupervisor(logger, mockSQS, &http.Client{}, config)

	mockSQS.receiveMessageFunc = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		defer supervisor.Shutdown()

		return &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{{
				Body:          aws.String("message 1"),
				MessageId:     aws.String("m1"),
				ReceiptHandle: aws.String("r1"),
			}},
		}, nil
	}

	mockSQS.changeMessageVisibilityBatchFunc = func(input *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
		assert.Fail(t, "ChangeMessageVisibilityBatchFunc was called")
		return nil, nil
	}

	supervisor.Start(1)
	supervisor.Wait()

	assert.True(t, hmacSuccess)
}

func TestSupervisorTooManyRequests(t *testing.T) {
	delayTime := time.Duration(1 * time.Hour)
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		delayUntil := time.Now().Add(delayTime)
		if requestCount == 1 {
			w.Header().Set("Retry-After", fmt.Sprintf("%v", delayTime.Seconds()))
		} else {
			w.Header().Set("Retry-After", delayUntil.Format(time.RFC1123))
		}

		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	log.SetOutput(ioutil.Discard)
	logger := log.WithFields(log.Fields{})
	mockSQS := &mockSQS{}
	config := WorkerConfig{
		HTTPURL:         ts.URL,
		HTTPContentType: "application/json",
	}

	mockSQS.receiveMessageFunc = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		return &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{{
			Body:          aws.String("message 1"),
			MessageId:     aws.String("m1"),
			ReceiptHandle: aws.String("r1"),
		}, {
			Body:          aws.String("message 2"),
			MessageId:     aws.String("m2"),
			ReceiptHandle: aws.String("r2"),
		}, {
			Body:          aws.String("message 3"),
			MessageId:     aws.String("m3"),
			ReceiptHandle: aws.String("r3"),
		}},
		}, nil
	}

	mockSQS.deleteMessageBatchFunc = func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
		assert.Fail(t, "DeleteMessageBatchFunc was called")
		return nil, nil
	}

	supervisor := NewSupervisor(logger, mockSQS, &http.Client{}, config)

	mockSQS.changeMessageVisibilityBatchFunc = func(input *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
		defer supervisor.Shutdown()

		assert.Len(t, input.Entries, 3)
		for _, entry := range input.Entries {
			VisibilityTimeout := *entry.VisibilityTimeout
			timeoutDiff := int64(delayTime.Seconds()) - VisibilityTimeout
			assert.True(t, timeoutDiff >= 0)
			assert.True(t, timeoutDiff < 5)
		}

		return nil, nil
	}

	supervisor.Start(1)
	supervisor.Wait()
}

func TestSupervisorTooManyRequestsBadRetryAfter(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.Header().Set("Retry-After", "invalid")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	log.SetOutput(ioutil.Discard)
	logger := log.WithFields(log.Fields{})
	mockSQS := &mockSQS{}
	config := WorkerConfig{
		HTTPURL:         ts.URL,
		HTTPContentType: "application/json",
	}

	supervisor := NewSupervisor(logger, mockSQS, &http.Client{}, config)

	receiveCount := 0
	mockSQS.receiveMessageFunc = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		receiveCount++

		if receiveCount == 2 {
			supervisor.Shutdown()

			return &sqs.ReceiveMessageOutput{
				Messages: []*sqs.Message{},
			}, nil
		}

		return &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{{
				Body:          aws.String("message 1"),
				MessageId:     aws.String("m1"),
				ReceiptHandle: aws.String("r1"),
			}, {
				Body:          aws.String("message 2"),
				MessageId:     aws.String("m2"),
				ReceiptHandle: aws.String("r2"),
			}, {
				Body:          aws.String("message 3"),
				MessageId:     aws.String("m3"),
				ReceiptHandle: aws.String("r3"),
			}},
		}, nil
	}

	mockSQS.deleteMessageBatchFunc = func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
		assert.Fail(t, "DeleteMessageBatchInput was called")
		return nil, nil
	}

	mockSQS.changeMessageVisibilityBatchFunc = func(input *sqs.ChangeMessageVisibilityBatchInput) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
		assert.Fail(t, "ChangeMessageVisibilityBatchFunc was called")
		return nil, nil
	}

	supervisor.Start(1)
	supervisor.Wait()
}
