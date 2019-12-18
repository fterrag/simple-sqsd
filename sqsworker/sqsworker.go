package sqsworker

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/fterrag/simple-sqsd/sqshttp"
)

type Director struct {
	mu      sync.Mutex
	started bool
	ctx     context.Context
	canc    context.CancelFunc
	wg      sync.WaitGroup

	logger     Logger
	sqs        sqsiface.SQSAPI
	httpClient sqshttp.Client
}

func NewDirector(l Logger, s sqsiface.SQSAPI, c sqshttp.Client) *Director {
	ctx, cancel := context.WithCancel(context.Background())

	return &Director{
		ctx:  ctx,
		canc: cancel,

		logger:     l,
		sqs:        s,
		httpClient: c,
	}
}

func (d *Director) Start(n int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.started {
		return errors.New("already started")
	}

	d.started = true

	if n <= 0 {
		n = runtime.NumCPU()
	}

	d.logger.Printf("Starting %d workers", n)

	for i := 0; i < n; i++ {
		go d.worker(i + 1)
	}

	return nil
}

func (d *Director) Wait() {
	d.wg.Wait()
}

func (d *Director) Shutdown() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.logger.Print("Shutting down workers")
	d.canc()
}

func (d *Director) worker(id int) {
	d.logger.Printf("Worker #%d starting", id)
	d.wg.Add(1)

	for {
		select {
		case <-d.ctx.Done():
			d.logger.Printf("Worker #%d shutting down", id)
			d.wg.Done()
			return

		default:
			recInput := &sqs.ReceiveMessageInput{
				// MaxNumberOfMessages:   aws.Int64(int64(s.workerConfig.QueueMaxMessages)),
				// QueueUrl:              aws.String(s.workerConfig.QueueURL),
				// WaitTimeSeconds:       aws.Int64(int64(s.workerConfig.QueueWaitTime)),
				MessageAttributeNames: aws.StringSlice([]string{"All"}),
			}

			output, err := d.sqs.ReceiveMessage(recInput)
			if err != nil {
				d.logger.Printf("Error while receiving messages from the queue: %s", err)
				continue
			}

			if len(output.Messages) == 0 {
				continue
			}

			deleteEntries := make([]*sqs.DeleteMessageBatchRequestEntry, 0)
			changeVisibilityEntries := make([]*sqs.ChangeMessageVisibilityBatchRequestEntry, 0)

			for _, msg := range output.Messages {
				res, err := d.httpClient.Request(msg)
				if err != nil {
					d.logger.Printf("Error making HTTP request: %s", err)
					continue
				}

				if !res.Successful {
					if res.RetryAfterSec > 0 {
						changeVisibilityEntries = append(changeVisibilityEntries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
							Id:                msg.MessageId,
							ReceiptHandle:     msg.ReceiptHandle,
							VisibilityTimeout: aws.Int64(res.RetryAfterSec),
						})
					}

					d.logger.Printf("Non-successful HTTP status code received: %d", res.StatusCode)
					continue
				}

				deleteEntries = append(deleteEntries, &sqs.DeleteMessageBatchRequestEntry{
					Id:            msg.MessageId,
					ReceiptHandle: msg.ReceiptHandle,
				})
			}

			if len(deleteEntries) > 0 {
				delInput := &sqs.DeleteMessageBatchInput{
					Entries: deleteEntries,
					// QueueUrl: aws.String(s.workerConfig.QueueURL),
				}

				_, err = d.sqs.DeleteMessageBatch(delInput)
				if err != nil {
					d.logger.Printf("Error while deleting messages from SQS: %s", err)
				}
			}

			if len(changeVisibilityEntries) > 0 {
				changeVisibilityInput := &sqs.ChangeMessageVisibilityBatchInput{
					Entries: changeVisibilityEntries,
					// QueueUrl: aws.String(s.workerConfig.QueueURL),
				}

				_, err = d.sqs.ChangeMessageVisibilityBatch(changeVisibilityInput)
				if err != nil {
					d.logger.Printf("Error while changing visibility on messages: %s", err)
				}
			}
		}
	}
}
