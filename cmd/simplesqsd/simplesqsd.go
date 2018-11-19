package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fterrag/simple-sqsd/supervisor"
	log "github.com/sirupsen/logrus"
)

type config struct {
	QueueRegion      string
	QueueURL         string
	QueueMaxMessages int
	QueueWaitTime    int

	HTTPMaxConns           int
	HTTPURL                string
	HTTPContentType        string
	HTTPRetryAfterAttempts int

	AWSEndpoint    string
	HTTPHMACHeader string
	HMACSecretKey  []byte

	HTTPHealthPath        string
	HTTPHealthWait        int
	HTTPHealthInterval    int
	HTTPHealthSucessCount int
}

func main() {

	c := &config{}

	c.QueueRegion = os.Getenv("SQSD_QUEUE_REGION")
	c.QueueURL = os.Getenv("SQSD_QUEUE_URL")
	c.QueueMaxMessages = getEnvInt("SQSD_QUEUE_MAX_MSGS", 10)
	c.QueueWaitTime = getEnvInt("SQSD_QUEUE_WAIT_TIME", 10)

	c.HTTPMaxConns = getEnvInt("SQSD_HTTP_MAX_CONNS", 50)
	c.HTTPURL = os.Getenv("SQSD_HTTP_URL")
	c.HTTPContentType = os.Getenv("SQSD_HTTP_CONTENT_TYPE")
	c.HTTPRetryAfterAttempts = getEnvInt("SQSD_HTTP_RETRY_AFTER_MAX_ATTEMPTS", 0)

	c.HTTPHealthPath = os.Getenv("SQSD_HTTP_HEALTH_PATH")
	c.HTTPHealthWait = getEnvInt("SQSD_HTTP_HEALTH_WAIT", 5)
	c.HTTPHealthInterval = getEnvInt("SQSD_HTTP_HEALTH_INTERVAL", 5)
	c.HTTPHealthSucessCount = getEnvInt("SQSD_HTTP_HEALTH_SUCCESS_COUNT", 1)

	c.AWSEndpoint = os.Getenv("SQSD_AWS_ENDPOINT")
	c.HTTPHMACHeader = os.Getenv("SQSD_HTTP_HMAC_HEADER")
	c.HMACSecretKey = []byte(os.Getenv("SQSD_HMAC_SECRET_KEY"))

	if len(c.QueueRegion) == 0 {
		log.Fatal("SQSD_QUEUE_REGION cannot be empty")
	}

	if len(c.QueueURL) == 0 {
		log.Fatal("SQSD_QUEUE_URL cannot be empty")
	}

	if len(c.HTTPURL) == 0 {
		log.Fatal("SQSD_HTTP_URL cannot be empty")
	}

	log.SetFormatter(&log.JSONFormatter{})

	logLevel := os.Getenv("LOG_LEVEL")
	if len(logLevel) == 0 {
		logLevel = "info"
	}
	if parsedLevel, err := log.ParseLevel(logLevel); err == nil {
		log.SetLevel(parsedLevel)
	} else {
		log.Fatal(err)
	}

	logger := log.WithFields(log.Fields{
		"queueRegion":  c.QueueRegion,
		"queueUrl":     c.QueueURL,
		"httpMaxConns": c.HTTPMaxConns,
		"httpPath":     c.HTTPURL,
	})

	if len(c.HTTPHealthPath) != 0 {
		numSuccesses := 0
		healthURL := fmt.Sprintf("%s%s", c.HTTPURL, c.HTTPHealthPath)
		log.Infof("Waiting %d seconds before staring health check at '%s'", c.HTTPHealthWait, healthURL)
		time.Sleep(time.Duration(c.HTTPHealthWait) * time.Second)
		for {
			if resp, err := http.Get(healthURL); err == nil {
				log.Infof("%#v", resp)
				if numSuccesses == c.HTTPHealthSucessCount {
					break
				} else {
					numSuccesses++
				}
			} else {
				log.Debugf("Health check failed: %s. Waiting for %d seconds before next attempt", err, c.HTTPHealthInterval)
				time.Sleep(time.Duration(c.HTTPHealthInterval) * time.Second)
			}
		}
		log.Info("Health check succeeded. Starting message processing")
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        c.HTTPMaxConns,
			MaxIdleConnsPerHost: c.HTTPMaxConns,
		},
	}

	awsSess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	sqsConfig := aws.NewConfig().
		WithRegion(c.QueueRegion).
		WithHTTPClient(httpClient)

	if len(c.AWSEndpoint) > 0 {
		sqsConfig.WithEndpoint(c.AWSEndpoint)
	}

	sqsSvc := sqs.New(awsSess, sqsConfig)

	wConf := supervisor.WorkerConfig{
		QueueURL:         c.QueueURL,
		QueueMaxMessages: c.QueueMaxMessages,
		QueueWaitTime:    c.QueueWaitTime,

		HTTPURL:         c.HTTPURL,
		HTTPContentType: c.HTTPContentType,
		HTTPRetryAfterAttempts: c.HTTPRetryAfterAttempts,

		HTTPHMACHeader: c.HTTPHMACHeader,
		HMACSecretKey:  c.HMACSecretKey,
	}

	s := supervisor.NewSupervisor(logger, sqsSvc, httpClient, wConf)
	s.Start(c.HTTPMaxConns)
	s.Wait()
}

func getEnvInt(key string, def int) int {
	val, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return def
	}

	return val
}
