FROM golang:1.26-alpine AS builder

RUN apk add --no-cache build-base git
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/drs-server .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && adduser -D -h /app drs
WORKDIR /app

COPY --from=builder /out/drs-server /usr/local/bin/drs-server

EXPOSE 8080
USER drs
ENTRYPOINT ["drs-server"]
