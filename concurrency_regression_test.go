package bandwidthlimiter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentUploadsRace(t *testing.T) {
	h, err := New(context.Background(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body) }), CreateConfig(), "review")
	if err != nil {
		t.Fatal(err)
	}
	defer h.(*BandwidthLimiter).Shutdown()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				r := httptest.NewRequest("POST", "http://storage.example.invalid/bucket", strings.NewReader("synthetic"))
				h.ServeHTTP(httptest.NewRecorder(), r)
			}
		}()
	}
	wg.Wait()
}

func TestCanceledUploadDoesNotWaitForRefill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bucket := NewTokenBucket(1, 4096)
	if !bucket.Consume(4096) {
		t.Fatal("failed to exhaust burst")
	}
	reader := &limitedReadCloser{ReadCloser: io.NopCloser(strings.NewReader("x")), bucket: bucket, ctx: ctx, touch: func() {}}
	done := make(chan error, 1)
	go func() { _, err := reader.Read(make([]byte, 1)); done <- err }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("upload ignored cancellation")
	}
}

func TestCanceledDownloadDoesNotWriteAheadOfBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bucket := NewTokenBucket(1, 4096)
	bucket.Consume(4096)
	recorder := httptest.NewRecorder()
	writer := &limitedResponseWriter{ResponseWriter: recorder, bucket: bucket, ctx: ctx, touch: func() {}}
	done := make(chan error, 1)
	go func() { _, err := writer.Write([]byte("x")); done <- err }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("download ignored cancellation")
	}
	if recorder.Body.Len() != 0 {
		t.Fatal("wrote bytes without budget")
	}
}
