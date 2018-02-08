FROM golang:1.9.2-alpine as builder

RUN apk --no-cache add git ca-certificates wget

WORKDIR /go/src/github.com/fterrag/simple-sqsd
COPY . .

RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure

WORKDIR /go/src/github.com/fterrag/simple-sqsd/cmd/simplesqsd

RUN go build -o app .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /go/src/github.com/fterrag/simple-sqsd/cmd/simplesqsd/app .
CMD ["./app"]
