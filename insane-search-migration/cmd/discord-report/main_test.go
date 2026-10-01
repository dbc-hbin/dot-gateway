package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestInputBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	if _, err := readReport(ctx, r); err == nil {
		t.Fatal("blocked stdin ignored deadline")
	}
}
func TestInputRead(t *testing.T) {
	b, err := readReport(context.Background(), strings.NewReader("test"))
	if err != nil || string(b) != "test" {
		t.Fatal("input read failed")
	}
}
