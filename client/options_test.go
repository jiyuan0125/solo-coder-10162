package client

import (
	"testing"
	"time"
)

func TestWithConnectionTimeoutZeroPreserved(t *testing.T) {
	co := CallOptions{}
	WithConnectionTimeout(0)(&co)
	if co.ConnectionTimeout != 0 {
		t.Fatalf("WithConnectionTimeout(0) should preserve zero, got %v", co.ConnectionTimeout)
	}
	_ = time.Duration(0)
}

func TestWithConnectionTimeoutNonZeroPreserved(t *testing.T) {
	co := CallOptions{}
	want := 500 * time.Millisecond
	WithConnectionTimeout(want)(&co)
	if co.ConnectionTimeout != want {
		t.Fatalf("Expected %v, got %v", want, co.ConnectionTimeout)
	}
}

func TestNewClientDefaultConnectionTimeout(t *testing.T) {
	c := NewClient()
	opts := c.Options()
	if opts.CallOptions.ConnectionTimeout != DefaultConnectionTimeout {
		t.Fatalf("Default client ConnectionTimeout should be %v, got %v",
			DefaultConnectionTimeout, opts.CallOptions.ConnectionTimeout)
	}
}

func TestNewClientWithZeroConnectionTimeout(t *testing.T) {
	c := NewClient(ConnectionTimeout(0))
	opts := c.Options()
	if opts.CallOptions.ConnectionTimeout != 0 {
		t.Fatalf("ConnectionTimeout(0) should not be overridden, got %v", opts.CallOptions.ConnectionTimeout)
	}
}
