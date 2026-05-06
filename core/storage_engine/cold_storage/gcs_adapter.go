package coldstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// GCSConfig configures the GCSAdapter.
type GCSConfig struct {
	// Bucket is the GCS bucket name (required).
	Bucket string
	// KeyPrefix is an optional object key prefix.
	KeyPrefix string
	// AccessToken is a OAuth2 bearer token for authenticated requests.
	// In production, obtain tokens via the metadata server or a service-account
	// key using the golang.org/x/oauth2 package.
	AccessToken string
}

// GCSAdapter implements CloudObjectStore against Google Cloud Storage using
// the JSON API over plain HTTP.  For advanced features (resumable uploads, ACLs,
// encryption keys) wire in the official cloud.google.com/go/storage client.
type GCSAdapter struct {
	cfg    GCSConfig
	client *http.Client
}

// NewGCSAdapter returns a GCSAdapter ready to use.
func NewGCSAdapter(_ context.Context, cfg GCSConfig) (*GCSAdapter, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs: bucket name is required")
	}
	return &GCSAdapter{
		cfg:    cfg,
		client: &http.Client{Timeout: 120 * time.Second},
	}, nil
}

func (g *GCSAdapter) AdapterType() string { return "gcs" }

func (g *GCSAdapter) objectURL(key string) string {
	return fmt.Sprintf("https://storage.googleapis.com/upload/storage/v1/b/%s/o?uploadType=media&name=%s",
		g.cfg.Bucket, g.key(key))
}

func (g *GCSAdapter) downloadURL(key string) string {
	return fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media",
		g.cfg.Bucket, g.key(key))
}

func (g *GCSAdapter) deleteURL(key string) string {
	return fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s",
		g.cfg.Bucket, g.key(key))
}

func (g *GCSAdapter) key(objectKey string) string {
	if g.cfg.KeyPrefix == "" {
		return objectKey
	}
	return g.cfg.KeyPrefix + "/" + objectKey
}

func (g *GCSAdapter) addAuth(req *http.Request) {
	if g.cfg.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+g.cfg.AccessToken)
	}
}

// Write uploads data to GCS and returns a gs:// URI.
func (g *GCSAdapter) Write(ctx context.Context, objectKey string, data []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.objectURL(objectKey), bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("gcs: build upload request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	g.addAuth(req)

	resp, err := g.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gcs: upload %q: %w", objectKey, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("gcs: upload %q: HTTP %d: %s", objectKey, resp.StatusCode, body)
	}
	return fmt.Sprintf("gs://%s/%s", g.cfg.Bucket, g.key(objectKey)), nil
}

// Read downloads and returns the object stored at objectKey.
func (g *GCSAdapter) Read(ctx context.Context, objectKey string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.downloadURL(objectKey), nil)
	if err != nil {
		return nil, fmt.Errorf("gcs: build download request: %w", err)
	}
	g.addAuth(req)

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gcs: download %q: %w", objectKey, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("gcs: object not found: %s", objectKey)
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("gcs: download %q: HTTP %d: %s", objectKey, resp.StatusCode, body)
	}
	return io.ReadAll(resp.Body)
}

// Delete removes the object at objectKey.
func (g *GCSAdapter) Delete(ctx context.Context, objectKey string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, g.deleteURL(objectKey), nil)
	if err != nil {
		return fmt.Errorf("gcs: build delete request: %w", err)
	}
	g.addAuth(req)

	resp, err := g.client.Do(req)
	if err != nil {
		return fmt.Errorf("gcs: delete %q: %w", objectKey, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("gcs: delete %q: HTTP %d", objectKey, resp.StatusCode)
	}
	return nil
}

// readerFrom is a package-level helper that wraps a byte slice in a ReadCloser.
func readerFrom(data []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(data))
}
