[![Go Report Card](https://goreportcard.com/badge/github.com/fterrag/simple-sqsd)](https://goreportcard.com/report/github.com/fterrag/simple-sqsd)

# simple-sqsd

A simple version of the AWS Elastic Beanstalk Worker Environment SQS daemon ([sqsd](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/using-features-managing-env-tiers.html#worker-daemon)).

## Getting Started

```bash
$ SQSD_QUEUE_REGION=us-east-1 SQSD_QUEUE_URL=http://queue.url SQSD_HTTP_URL=http://service.url/endpoint go run cmd/simplesqsd/simplesqsd.go
```

Docker (uses a Docker Hub repository):
```bash
$ docker run -e SQSD_QUEUE_REGION=us-east-1 -e SQSD_QUEUE_URL=http://queue.url -e SQSD_HTTP_URL=http://service.url/endpoint fterrag/simple-sqsd:latest
```

## Configuration

| **Environment Variable**                | **Default Value**  | **Required**                       | **Description**                                                                                        |
|-----------------------------------------|--------------------|------------------------------------|--------------------------------------------------------------------------------------------------------|
| `SQSD_QUEUE_REGION`                     |                    | yes                                | The region of the SQS queue.                                                                           |
| `SQSD_QUEUE_URL`                        |                    | yes                                | The URL of the SQS queue.                                                                              |
| `SQSD_QUEUE_MAX_MSGS`                   | `10`               | no                                 | Max number of messages a worker should try to receive from the SQS queue.                              |
| `SQSD_QUEUE_WAIT_TIME`                  | `10`               | no                                 | Number of seconds for SQS to wait until a message is available in the queue before sending a response. |
| `SQSD_HTTP_MAX_CONNS`                   | `50`               | no                                 | Maximum number of concurrent HTTP requests to make to SQSD_HTTP_URL.                                   |
| `SQSD_HTTP_URL`                         |                    | yes                                | The URL of your service to make a request to.                                                          |
| `SQSD_HTTP_CONTENT_TYPE`                |                    | no                                 | The value to send for the HTTP header `Content-Type` when making a request to your service.            |

## Todo

- [ ] Tests
- [ ] Documentation

## Contributing

* Submit a PR
* Add or improve documentation
* Report issues
* Suggest new features or enhancements
