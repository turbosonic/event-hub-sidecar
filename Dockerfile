# build stage
FROM golang:1.10.2 AS build-env
WORKDIR /go/src/github.com/turbosonic/event-hub-sidecar/
ADD . .
RUN CGO_ENABLED=0 GOOS=linux go build -o main.exe main.go

# final stage
FROM scratch
COPY --from=build-env /go/src/github.com/turbosonic/event-hub-sidecar/main.exe .
EXPOSE 8080
ENTRYPOINT ["./main.exe"]