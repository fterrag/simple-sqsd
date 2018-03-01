FROM golang:1.9.2-alpine as builder

RUN apk --no-cache add git ca-certificates wget

WORKDIR /go/src/github.com/fterrag/simple-sqsd
COPY . .

RUN go get golang.org/x/tools/cmd/cover
RUN go get github.com/mattn/goveralls
RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure

RUN go build cmd/simplesqsd/simplesqsd.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /go/src/github.com/fterrag/simple-sqsd/simplesqsd .
CMD ["./simplesqsd"]
