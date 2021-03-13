FROM golang:1.16-alpine as builder

WORKDIR /app
COPY . .

RUN apk --no-cache add git alpine-sdk build-base gcc

RUN go build cmd/simplesqsd/simplesqsd.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/simplesqsd .
CMD ["./simplesqsd"]
