# build stage
FROM golang:1.18-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main

# final stage
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/main /app/
ENTRYPOINT [ "./main" ]