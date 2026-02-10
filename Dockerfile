# Multi-stage build for Go application
FROM golang:1.24-alpine AS builder

# Set the working directory
WORKDIR /app

# Install git and make for submodule operations and building
RUN apk add --no-cache git make docker

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Initialize git submodules
RUN git submodule update --init --recursive

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o drs-server ./cmd/server

# Final lightweight image
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/drs-server .

# Copy the OpenAPI spec file
COPY --from=builder /app/internal/apigen/api/openapi.yaml ./internal/apigen/api/openapi.yaml

# Change ownership to non-root user
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Expose port 8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

# Run the application
CMD ["./drs-server"]
