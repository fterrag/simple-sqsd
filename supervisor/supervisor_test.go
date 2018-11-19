package supervisor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

type mockSQS struct {
	sqsiface.SQSAPI

	receiveMessageFunc     func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	deleteMessageBatchFunc func(*sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
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

	supervisor.Start(1)
	supervisor.Wait()

	assert.True(t, hmacSuccess)
}

func TestSupervisorTooManyRequests(t *testing.T) {
	delayUntil := time.Now()
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		if requestCount == 2 || requestCount > 3 {
			now := time.Now().Unix()
			delayedUntil := delayUntil.Unix()
			assert.True(t, delayedUntil < now || delayedUntil == now)
			w.WriteHeader(http.StatusOK)
			return
		}

		delayTime := time.Duration(3 * time.Second)
		delayUntil = time.Now().Add(delayTime);
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
		HTTPRetryAfterAttempts: 1,
	}

	mockSQS.receiveMessageFunc = func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		return &sqs.ReceiveMessageOutput{ Messages: []*sqs.Message{{
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

	supervisor.Start(1)
	supervisor.Wait()

	assert.Equal(t, 5, requestCount)
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
		HTTPRetryAfterAttempts: 1,
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

	supervisor.Start(1)
	supervisor.Wait()
}

func TestSupervisorRepeatedTooManyRequests(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	log.SetOutput(ioutil.Discard)
	logger := log.WithFields(log.Fields{})
	mockSQS := &mockSQS{}
	config := WorkerConfig{
		HTTPURL:         ts.URL,
		HTTPContentType: "application/json",
		HTTPRetryAfterAttempts: 1,
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

	supervisor.Start(1)
	supervisor.Wait()
}

func TestSupervisorDoNotRetryOnTooManyRequests(t *testing.T) {
	sendCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sendCount++
		assert.True(t, sendCount <= 3)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	log.SetOutput(ioutil.Discard)
	logger := log.WithFields(log.Fields{})
	mockSQS := &mockSQS{}
	config := WorkerConfig{
		HTTPURL: ts.URL,
		HTTPRetryAfterAttempts: 0,
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

	supervisor.Start(1)
	supervisor.Wait()
}
