package sqshttp

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type Client struct {
	httpClient        *http.Client
	url               string
	contentTypeHeader string

	hmacOptions *HMACOptions
}

type HMACOptions struct {
	Signature  string
	Secret     string
	HTTPHeader string
}

type Result struct {
	StatusCode    int
	Successful    bool
	RetryAfterSec int64
}

func NewClient(httpClient *http.Client, url string, contentTypeHeader string, HMACOpts *HMACOptions) *Client {
	return &Client{
		httpClient:        httpClient,
		url:               url,
		contentTypeHeader: contentTypeHeader,

		hmacOptions: HMACOpts,
	}
}

func (c *Client) Request(msg *sqs.Message) (*Result, error) {
	msgID := *msg.MessageId
	body := *msg.Body

	req, err := http.NewRequest("POST", c.url, bytes.NewBufferString(body))
	if err != nil {
		return nil, fmt.Errorf("Error creating the HTTP request: %s", err)
	}

	// Add the X-Aws-Sqsd-Msgid header.
	req.Header.Add("X-Aws-Sqsd-Msgid", msgID)

	// Add a X-Aws-Sqsd-Attr-* header for each attribute included on the SQS message.
	for k, v := range msg.MessageAttributes {
		req.Header.Add(fmt.Sprintf("X-Aws-Sqsd-Attr-%s", k), *v.StringValue)
	}

	// If there are HMAC options, add the hash as a header.
	if c.hmacOptions != nil {
		hash, err := makeHMAC(fmt.Sprintf("%s%s", c.hmacOptions.Signature, body), c.hmacOptions.Secret)
		if err != nil {
			return nil, err
		}

		req.Header.Set(c.hmacOptions.HTTPHeader, hash)
	}

	// If there is a Content-Type header, add it as a header.
	if len(c.contentTypeHeader) > 0 {
		req.Header.Set("Content-Type", c.contentTypeHeader)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	res.Body.Close()

	statusCode := res.StatusCode
	successful := res.StatusCode >= http.StatusOK || res.StatusCode <= http.StatusIMUsed
	retryAfterSec := getRetryAfterFromResponse(res)

	return &Result{
		StatusCode:    statusCode,
		Successful:    successful,
		RetryAfterSec: retryAfterSec,
	}, nil
}

func makeHMAC(signature string, secret string) (string, error) {
	mac := hmac.New(sha256.New, []byte(secret))

	_, err := mac.Write([]byte(signature))
	if err != nil {
		return "", fmt.Errorf("Error while writing HMAC: %s", err)
	}

	return hex.EncodeToString(mac.Sum(nil)), nil
}

func getRetryAfterFromResponse(res *http.Response) int64 {
	retryAfter := res.Header.Get("Retry-After")
	if len(retryAfter) == 0 {
		return 0
	}

	seconds, err := strconv.ParseInt(retryAfter, 10, 0)
	if err != nil {
		return 0
	}

	return seconds
}
