# syfon client (Go SDK)

This module provides a reusable Go client for Syfon APIs.

- Module: `github.com/calypr/syfon/client`
- Pattern: Vault-style grouped services (`Client.Data()`, `Client.Index()`, `Client.Buckets()`, etc.)
- Current surface:
  - Health: `/healthz`
  - Data: `/data/upload`, `/data/upload/{file_id}`, `/data/upload/bulk`, `/data/download/{file_id}`, multipart endpoints
  - Index: `/index`, `/index/{id}`, bulk endpoints
  - Buckets: `/data/buckets`, `/data/buckets/{bucket}`, `/data/buckets/{bucket}/scopes`
  - Core: `/index/v1/sha256/validity`
  - Metrics: `/index/v1/metrics/*`

## Usage

```go
package main

import (
  "context"
  "log"

  syclient "github.com/calypr/syfon/client"
)

func main() {
  c := syclient.New(
    "http://127.0.0.1:8080",
    syclient.WithBasicAuth("user", "pass"),
  )

  err := c.Buckets().Put(context.Background(), syclient.PutBucketRequest{
    Bucket:       "cbds",
    Provider:     "s3",
    Region:       "us-east-1",
    AccessKey:    "...",
    SecretKey:    "...",
    Organization: "syfon",
    ProjectID:    "e2e",
  })
  if err != nil {
    log.Fatal(err)
  }

  if err := c.Health().Ping(context.Background()); err != nil {
    log.Fatal(err)
  }
}
```
