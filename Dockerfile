FROM golang:1.12-alpine as builder

ENV GO111MODULE=on

WORKDIR /app
COPY . .

RUN apk --no-cache add git alpine-sdk build-base gcc

RUN go get
RUN go get golang.org/x/tools/cmd/cover
RUN go get github.com/mattn/goveralls

RUN go build cmd/simplesqsd/simplesqsd.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/simplesqsd .
CMD ["./simplesqsd"]
