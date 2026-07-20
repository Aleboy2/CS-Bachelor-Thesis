FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o worker .

FROM alpine:3.20
COPY --from=builder /app/worker /usr/local/bin/worker
ENTRYPOINT ["worker"]
