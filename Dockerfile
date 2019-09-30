# build stage
FROM golang:1.13 AS build-env
WORKDIR /go/src/github.com/turbosonic/event-hub-sidecar/
ADD . .
RUN CGO_ENABLED=0 GOOS=linux go build -o main.exe main.go

# certs stage
FROM alpine:latest as certs
RUN apk --update add ca-certificates

# final stage
FROM scratch
COPY --from=build-env /go/src/github.com/turbosonic/event-hub-sidecar/main.exe .
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
EXPOSE 8080
ENTRYPOINT ["./main.exe"]