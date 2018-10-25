# build stage
FROM golang:1.10.2 AS build-env
WORKDIR /go/src/event-delivery-sidecar
ADD . .
RUN CGO_ENABLED=0 GOOS=linux go build -o main.exe main.go

# final stage
FROM scratch
COPY --from=build-env /go/src/event-delivery-sidecar/main.exe .
EXPOSE 8080
ENTRYPOINT ["./main.exe"]