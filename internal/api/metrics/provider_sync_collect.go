package metrics

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const providerTransferEventPrefix = ".syfon/provider-transfer-events/"

type providerTransferEventEnvelope struct {
	Events []providerTransferPayload `json:"events"`
}

func ValidateProviderTransferLogSource(ctx context.Context, cred models.S3Credential) error {
	provider := common.NormalizeProvider(cred.Provider, common.S3Provider)
	if provider == common.FileProvider {
		return nil
	}
	if strings.TrimSpace(cred.BillingLogBucket) == "" {
		return fmt.Errorf("billing_log_bucket is required for provider=%s", provider)
	}
	if strings.TrimSpace(cred.BillingLogPrefix) == "" {
		return fmt.Errorf("billing_log_prefix is required for provider=%s", provider)
	}
	if _, err := collectProviderTransferEventPayloads(ctx, billingLogCredential(cred)); err != nil {
		return fmt.Errorf("billing log source is not readable: %w", err)
	}
	return nil
}

func (s *MetricsServer) collectProviderTransferEvents(ctx context.Context, cred models.S3Credential, from, to time.Time, organization, project string) ([]models.ProviderTransferEvent, error) {
	rawEvents, err := collectProviderTransferEventPayloads(ctx, billingLogCredential(cred))
	if err != nil {
		return nil, err
	}

	events := make([]models.ProviderTransferEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		if strings.TrimSpace(raw.Provider) == "" {
			raw.Provider = common.NormalizeProvider(cred.Provider, common.S3Provider)
		}
		if strings.TrimSpace(raw.Bucket) == "" {
			raw.Bucket = strings.TrimSpace(cred.Bucket)
		}
		if raw.ProviderEventID == "" {
			raw.ProviderEventID = providerTransferEventID(raw)
		}
		ev, err := providerTransferPayloadToModel(raw)
		if err != nil {
			return nil, err
		}
		if ev.EventTime.Before(from) || !ev.EventTime.Before(to) {
			continue
		}
		if organization != "" && ev.Organization != "" && ev.Organization != organization {
			continue
		}
		if project != "" && ev.Project != "" && ev.Project != project {
			continue
		}
		events = append(events, ev)
	}
	return events, nil
}

func collectProviderTransferEventPayloads(ctx context.Context, cred models.S3Credential) ([]providerTransferPayload, error) {
	switch common.NormalizeProvider(cred.Provider, common.S3Provider) {
	case common.FileProvider:
		return collectFileProviderTransferEventPayloads(cred)
	case common.S3Provider:
		return collectS3ProviderTransferEventPayloads(ctx, cred)
	case common.GCSProvider:
		return collectGCSProviderTransferEventPayloads(ctx, cred)
	case common.AzureProvider:
		return collectAzureProviderTransferEventPayloads(ctx, cred)
	default:
		return nil, fmt.Errorf("unsupported provider %q", cred.Provider)
	}
}

func billingLogCredential(cred models.S3Credential) models.S3Credential {
	if strings.TrimSpace(cred.BillingLogBucket) != "" {
		cred.Bucket = strings.TrimSpace(cred.BillingLogBucket)
	}
	if strings.TrimSpace(cred.BillingLogPrefix) != "" {
		cred.BillingLogPrefix = strings.Trim(strings.TrimSpace(cred.BillingLogPrefix), "/")
	}
	return cred
}

func collectFileProviderTransferEventPayloads(cred models.S3Credential) ([]providerTransferPayload, error) {
	root := strings.TrimSpace(cred.Endpoint)
	if root == "" {
		root = strings.TrimSpace(cred.Bucket)
	}
	if root == "" {
		return nil, fmt.Errorf("file provider requires endpoint or bucket root")
	}

	dir := filepath.Join(root, filepath.FromSlash(providerTransferEventPrefixForCredential(cred)))
	var out []providerTransferPayload
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		events, err := parseProviderTransferEventPayloads(data, filepath.ToSlash(path))
		if err != nil {
			return err
		}
		out = append(out, events...)
		return nil
	})
	return out, err
}

func collectS3ProviderTransferEventPayloads(ctx context.Context, cred models.S3Credential) ([]providerTransferPayload, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(strings.TrimSpace(cred.Region)),
	}
	if strings.TrimSpace(cred.AccessKey) != "" || strings.TrimSpace(cred.SecretKey) != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cred.AccessKey, cred.SecretKey, "")))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load s3 config: %w", err)
	}
	if strings.TrimSpace(cred.Endpoint) != "" {
		cfg.BaseEndpoint = aws.String(strings.TrimSpace(cred.Endpoint))
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if strings.TrimSpace(cred.Endpoint) != "" {
			o.UsePathStyle = true
		}
	})

	var out []providerTransferPayload
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(strings.TrimSpace(cred.Bucket)),
		Prefix: aws.String(providerTransferEventPrefixForCredential(cred)),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list s3 provider events: %w", err)
		}
		for _, obj := range page.Contents {
			key := strings.TrimSpace(aws.ToString(obj.Key))
			if key == "" || strings.HasSuffix(key, "/") {
				continue
			}
			body, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(strings.TrimSpace(cred.Bucket)),
				Key:    aws.String(key),
			})
			if err != nil {
				return nil, fmt.Errorf("read s3 provider event %s: %w", key, err)
			}
			events, err := readProviderTransferEventPayloads(body.Body, "s3://"+cred.Bucket+"/"+key)
			body.Body.Close()
			if err != nil {
				return nil, err
			}
			out = append(out, events...)
		}
	}
	return out, nil
}

func collectGCSProviderTransferEventPayloads(ctx context.Context, cred models.S3Credential) ([]providerTransferPayload, error) {
	opts := []option.ClientOption{}
	if json.Valid([]byte(strings.TrimSpace(cred.SecretKey))) {
		opts = append(opts, option.WithCredentialsJSON([]byte(strings.TrimSpace(cred.SecretKey))))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create gcs client: %w", err)
	}
	defer client.Close()

	var out []providerTransferPayload
	it := client.Bucket(strings.TrimSpace(cred.Bucket)).Objects(ctx, &storage.Query{Prefix: providerTransferEventPrefixForCredential(cred)})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list gcs provider events: %w", err)
		}
		if attrs.Name == "" || strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		r, err := client.Bucket(strings.TrimSpace(cred.Bucket)).Object(attrs.Name).NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("read gcs provider event %s: %w", attrs.Name, err)
		}
		events, err := readProviderTransferEventPayloads(r, "gs://"+cred.Bucket+"/"+attrs.Name)
		r.Close()
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}
	return out, nil
}

func collectAzureProviderTransferEventPayloads(ctx context.Context, cred models.S3Credential) ([]providerTransferPayload, error) {
	accountName := strings.TrimSpace(cred.AccessKey)
	if accountName == "" {
		accountName = azureAccountFromEndpoint(cred.Endpoint)
	}
	accountKey := strings.TrimSpace(cred.SecretKey)
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("azure sync requires shared key credentials for bucket %s", cred.Bucket)
	}
	shared, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("parse azure shared key: %w", err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(azureServiceURL(accountName, cred.Endpoint), shared, nil)
	if err != nil {
		return nil, fmt.Errorf("create azure client: %w", err)
	}

	prefix := providerTransferEventPrefixForCredential(cred)
	var out []providerTransferPayload
	pager := client.NewListBlobsFlatPager(strings.TrimSpace(cred.Bucket), &azblob.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list azure provider events: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil || strings.TrimSpace(*item.Name) == "" || strings.HasSuffix(*item.Name, "/") {
				continue
			}
			name := strings.TrimSpace(*item.Name)
			resp, err := client.DownloadStream(ctx, strings.TrimSpace(cred.Bucket), name, nil)
			if err != nil {
				return nil, fmt.Errorf("read azure provider event %s: %w", name, err)
			}
			events, err := readProviderTransferEventPayloads(resp.Body, "azblob://"+cred.Bucket+"/"+name)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}
			out = append(out, events...)
		}
	}
	return out, nil
}

func readProviderTransferEventPayloads(r io.Reader, rawRef string) ([]providerTransferPayload, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return parseProviderTransferEventPayloads(data, rawRef)
}

func parseProviderTransferEventPayloads(data []byte, rawRef string) ([]providerTransferPayload, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	var envelope providerTransferEventEnvelope
	if err := json.Unmarshal(trimmed, &envelope); err == nil && len(envelope.Events) > 0 {
		for i := range envelope.Events {
			if envelope.Events[i].RawEventRef == "" {
				envelope.Events[i].RawEventRef = rawRef
			}
		}
		return envelope.Events, nil
	}

	var single providerTransferPayload
	if err := json.Unmarshal(trimmed, &single); err == nil && single.Direction != "" {
		if single.RawEventRef == "" {
			single.RawEventRef = rawRef
		}
		return []providerTransferPayload{single}, nil
	}
	if events, ok, err := providerLogJSONDocumentToPayloads(trimmed, rawRef); err != nil {
		return nil, err
	} else if ok {
		return events, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)
	var events []providerTransferPayload
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		event, ok, err := providerLogLineToPayload(line, rawRef)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if event.RawEventRef == "" {
			event.RawEventRef = rawRef
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func providerLogJSONDocumentToPayloads(data []byte, rawRef string) ([]providerTransferPayload, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var doc any
	if err := decoder.Decode(&doc); err != nil {
		return nil, false, nil
	}
	events, ok, err := providerLogAnyToPayloads(doc, rawRef)
	if err != nil || !ok {
		return events, ok, err
	}
	return events, true, nil
}

func providerLogAnyToPayloads(v any, rawRef string) ([]providerTransferPayload, bool, error) {
	switch typed := v.(type) {
	case []any:
		out := make([]providerTransferPayload, 0, len(typed))
		for _, item := range typed {
			events, ok, err := providerLogAnyToPayloads(item, rawRef)
			if err != nil {
				return nil, true, err
			}
			if ok {
				out = append(out, events...)
			}
		}
		return out, true, nil
	case map[string]any:
		if events, ok, err := providerLogContainerToPayloads(typed, rawRef); ok || err != nil {
			return events, ok, err
		}
		if event, ok := providerLogJSONToPayload(typed, rawRef); ok {
			return []providerTransferPayload{event}, true, nil
		}
		return nil, true, nil
	default:
		return nil, false, nil
	}
}

func providerLogContainerToPayloads(raw map[string]any, rawRef string) ([]providerTransferPayload, bool, error) {
	for _, key := range []string{"events", "records", "Records", "entries", "logEntries"} {
		v, ok := raw[key]
		if !ok {
			continue
		}
		events, _, err := providerLogAnyToPayloads(v, rawRef)
		return events, true, err
	}
	if v, ok := nestedAny(raw, "protoPayload", "metadata", "records"); ok {
		events, _, err := providerLogAnyToPayloads(v, rawRef)
		return events, true, err
	}
	return nil, false, nil
}

func providerLogLineToPayload(line []byte, rawRef string) (providerTransferPayload, bool, error) {
	if len(line) == 0 {
		return providerTransferPayload{}, false, nil
	}
	if line[0] == '{' {
		var event providerTransferPayload
		if err := json.Unmarshal(line, &event); err == nil && event.Direction != "" {
			return event, true, nil
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			return providerTransferPayload{}, false, fmt.Errorf("parse provider event %s: %w", rawRef, err)
		}
		event, ok := providerLogJSONToPayload(raw, rawRef)
		return event, ok, nil
	}
	event, ok, err := s3ServerAccessLogLineToPayload(string(line), rawRef)
	return event, ok, err
}

func providerLogJSONToPayload(raw map[string]any, rawRef string) (providerTransferPayload, bool) {
	op := firstString(raw, "operation", "OperationName", "operationName", "methodName")
	if op == "" {
		op = nestedString(raw, "protoPayload", "methodName")
	}
	direction := providerDirectionForOperation(op)
	if direction == "" {
		return providerTransferPayload{}, false
	}

	eventTime := firstString(raw, "event_time", "timestamp", "time", "TimeGenerated")
	bucket := firstString(raw, "bucket", "bucket_name", "BucketName", "container", "ContainerName")
	key := strings.TrimLeft(firstString(raw, "object_key", "key", "object", "ObjectKey", "blob", "BlobName"), "/")
	storageURL := firstString(raw, "storage_url", "uri", "Uri", "resourceName")
	if storageURL == "" {
		storageURL = nestedString(raw, "protoPayload", "resourceName")
	}
	if storageURL == "" {
		storageURL = nestedString(raw, "resource", "labels", "resource_name")
	}
	if bucket == "" || key == "" {
		if b, k := bucketKeyFromProviderURL(storageURL); bucket == "" || key == "" {
			if bucket == "" {
				bucket = b
			}
			if key == "" {
				key = k
			}
		}
	}
	if storageURL == "" && bucket != "" && key != "" {
		storageURL = "s3://" + bucket + "/" + key
	}

	bytesTransferred := firstInt64(raw, "bytes_transferred", "bytesSent", "bytes_sent", "ResponseBodySize", "responseBodySize")
	if direction == models.ProviderTransferDirectionUpload && bytesTransferred == 0 {
		bytesTransferred = firstInt64(raw, "requestBodySize", "RequestBodySize", "objectSize", "ObjectSize")
	}
	httpStatus := int(firstInt64(raw, "http_status", "status", "StatusCode", "statusCode"))
	if httpStatus == 0 {
		httpStatus = int(nestedInt64(raw, "httpRequest", "status"))
	}
	if bytesTransferred == 0 {
		bytesTransferred = nestedInt64(raw, "httpRequest", "responseSize")
		if direction == models.ProviderTransferDirectionUpload && bytesTransferred == 0 {
			bytesTransferred = nestedInt64(raw, "httpRequest", "requestSize")
		}
	}

	event := providerTransferPayload{
		ProviderEventID:    firstString(raw, "provider_event_id", "insertId", "RequestIdHeader", "requestId", "request_id"),
		Direction:          direction,
		EventTime:          eventTime,
		ProviderRequestID:  firstString(raw, "provider_request_id", "request_id", "requestId", "RequestIdHeader"),
		Bucket:             bucket,
		ObjectKey:          key,
		StorageURL:         storageURL,
		BytesTransferred:   bytesTransferred,
		HTTPMethod:         strings.ToUpper(firstString(raw, "http_method", "method", "requestMethod", "RequestMethod")),
		HTTPStatus:         httpStatus,
		RequesterPrincipal: firstString(raw, "requester_principal", "Requester", "callerIp", "RequesterObjectId"),
		SourceIP:           firstString(raw, "source_ip", "callerIp", "CallerIpAddress"),
		UserAgent:          firstString(raw, "user_agent", "userAgent", "UserAgentHeader"),
		ActorEmail:         nestedString(raw, "protoPayload", "authenticationInfo", "principalEmail"),
		RawEventRef:        rawRef,
	}
	if event.ActorEmail == "" {
		event.ActorEmail = firstString(raw, "principalEmail", "RequesterUpn")
	}
	if event.ProviderEventID == "" {
		event.ProviderEventID = providerTransferEventID(event)
	}
	return event, true
}

func s3ServerAccessLogLineToPayload(line, rawRef string) (providerTransferPayload, bool, error) {
	fields, err := splitProviderLogFields(line)
	if err != nil {
		return providerTransferPayload{}, false, fmt.Errorf("parse s3 server access log %s: %w", rawRef, err)
	}
	if len(fields) < 18 {
		return providerTransferPayload{}, false, nil
	}
	op := fields[6]
	direction := providerDirectionForOperation(op)
	if direction == "" {
		return providerTransferPayload{}, false, nil
	}
	when, err := time.Parse("02/Jan/2006:15:04:05 -0700", fields[2])
	if err != nil {
		return providerTransferPayload{}, false, fmt.Errorf("parse s3 log time %q: %w", fields[2], err)
	}
	bytesTransferred := parseProviderInt64(fields[11])
	objectSize := parseProviderInt64(fields[12])
	if direction == models.ProviderTransferDirectionUpload && bytesTransferred == 0 {
		bytesTransferred = objectSize
	}
	event := providerTransferPayload{
		ProviderEventID:    "s3-" + fields[5],
		Direction:          direction,
		EventTime:          when.UTC().Format(time.RFC3339Nano),
		ProviderRequestID:  fields[5],
		Bucket:             fields[1],
		ObjectKey:          strings.TrimLeft(fields[7], "/"),
		StorageURL:         "s3://" + fields[1] + "/" + strings.TrimLeft(fields[7], "/"),
		ObjectSize:         objectSize,
		BytesTransferred:   bytesTransferred,
		HTTPStatus:         int(parseProviderInt64(fields[9])),
		RequesterPrincipal: fields[4],
		SourceIP:           fields[3],
		UserAgent:          fields[17],
		RawEventRef:        rawRef,
	}
	if strings.Contains(fields[8], " ") {
		event.HTTPMethod = strings.ToUpper(strings.Fields(fields[8])[0])
	}
	return event, true, nil
}

func providerDirectionForOperation(op string) string {
	lower := strings.ToLower(strings.TrimSpace(op))
	switch {
	case strings.Contains(lower, "get") || strings.Contains(lower, "read") || strings.Contains(lower, "download"):
		return models.ProviderTransferDirectionDownload
	case strings.Contains(lower, "put") || strings.Contains(lower, "post") || strings.Contains(lower, "create") || strings.Contains(lower, "write") || strings.Contains(lower, "upload"):
		return models.ProviderTransferDirectionUpload
	default:
		return ""
	}
}

func splitProviderLogFields(line string) ([]string, error) {
	var out []string
	var b strings.Builder
	inQuote := false
	inBracket := false
	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch ch {
		case '"':
			inQuote = !inQuote
		case '[':
			if !inQuote {
				inBracket = true
				continue
			}
			b.WriteByte(ch)
		case ']':
			if !inQuote && inBracket {
				inBracket = false
				continue
			}
			b.WriteByte(ch)
		case ' ', '\t':
			if !inQuote && !inBracket {
				if b.Len() > 0 {
					out = append(out, cleanProviderLogField(b.String()))
					b.Reset()
				}
				continue
			}
			b.WriteByte(ch)
		default:
			b.WriteByte(ch)
		}
	}
	if inQuote || inBracket {
		return nil, fmt.Errorf("unterminated quoted/bracketed field")
	}
	if b.Len() > 0 {
		out = append(out, cleanProviderLogField(b.String()))
	}
	return out, nil
}

func cleanProviderLogField(v string) string {
	v = strings.TrimSpace(v)
	if v == "-" {
		return ""
	}
	return strings.Trim(v, `"`)
}

func bucketKeyFromProviderURL(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", ""
	}
	switch strings.ToLower(u.Scheme) {
	case "s3", "gs", "azblob":
		return strings.TrimSpace(u.Host), unescapeProviderObjectKey(strings.TrimLeft(u.Path, "/"))
	}
	parts := strings.Split(strings.TrimLeft(u.Path, "/"), "/")
	for i, part := range parts {
		switch part {
		case "b", "buckets":
			if i+2 < len(parts) && (parts[i+2] == "o" || parts[i+2] == "objects") {
				return parts[i+1], unescapeProviderObjectKey(strings.Join(parts[i+3:], "/"))
			}
		}
	}
	if len(parts) >= 2 {
		return parts[0], unescapeProviderObjectKey(strings.Join(parts[1:], "/"))
	}
	return "", ""
}

func unescapeProviderObjectKey(raw string) string {
	key, err := url.PathUnescape(raw)
	if err != nil {
		return raw
	}
	return key
}

func firstString(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := raw[key]; ok {
			if s := anyString(v); s != "" {
				return s
			}
		}
	}
	return ""
}

func firstInt64(raw map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if v, ok := raw[key]; ok {
			if n := anyInt64(v); n != 0 {
				return n
			}
		}
	}
	return 0
}

func nestedString(raw map[string]any, keys ...string) string {
	v, ok := nestedAny(raw, keys...)
	if !ok {
		return ""
	}
	return anyString(v)
}

func nestedInt64(raw map[string]any, keys ...string) int64 {
	v, ok := nestedAny(raw, keys...)
	if !ok {
		return 0
	}
	return anyInt64(v)
}

func nestedAny(raw map[string]any, keys ...string) (any, bool) {
	var cur any = raw
	for _, key := range keys {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[key]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func anyString(v any) string {
	switch typed := v.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int:
		return strconv.Itoa(typed)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func anyInt64(v any) int64 {
	switch typed := v.(type) {
	case float64:
		return int64(typed)
	case int64:
		return typed
	case int:
		return int64(typed)
	case json.Number:
		n, _ := typed.Int64()
		return n
	case string:
		return parseProviderInt64(typed)
	default:
		return 0
	}
}

func parseProviderInt64(v string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	return n
}

func providerTransferEventID(item providerTransferPayload) string {
	raw := strings.Join([]string{
		strings.TrimSpace(item.Provider),
		strings.TrimSpace(item.Bucket),
		strings.TrimSpace(item.ObjectKey),
		strings.TrimSpace(item.StorageURL),
		strings.TrimSpace(item.Direction),
		strings.TrimSpace(item.EventTime),
		strings.TrimSpace(item.ProviderRequestID),
		fmt.Sprintf("%d", item.BytesTransferred),
		int64PtrString(item.RangeStart),
		int64PtrString(item.RangeEnd),
	}, "\x00")
	sum := sha256.Sum256([]byte(raw))
	return "provider-" + hex.EncodeToString(sum[:16])
}

func int64PtrString(v *int64) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%d", *v)
}

func providerTransferEventPrefixForCredential(cred models.S3Credential) string {
	prefix := strings.Trim(strings.TrimSpace(cred.BillingLogPrefix), "/")
	if prefix == "" {
		return providerTransferEventPrefix
	}
	return prefix + "/"
}

func azureServiceURL(accountName, endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint != "" {
		return strings.TrimRight(endpoint, "/") + "/"
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/", strings.TrimSpace(accountName))
}

func azureAccountFromEndpoint(endpoint string) string {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u.Host == "" {
		return ""
	}
	host := strings.Split(u.Host, ":")[0]
	parts := strings.Split(host, ".")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}
