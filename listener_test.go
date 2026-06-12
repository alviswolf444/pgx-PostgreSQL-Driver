package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestListenerReconnect(t *testing.T) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	testConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	testConn.Close(context.Background())

	listener := NewListener(connStr)
	listener.reconnectDelay = 500 * time.Millisecond
	listener.Start()
	defer listener.Close()

	channelName := "test_events"
	err = listener.Listen(context.Background(), channelName)
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	sendNotification := func(payload string) {
		conn, err := pgx.Connect(context.Background(), connStr)
		if err != nil {
			t.Fatalf("Failed to connect for notification: %v", err)
		}
		defer conn.Close(context.Background())
		_, err = conn.Exec(context.Background(), "SELECT pg_notify($1, $2)", channelName, payload)
		if err != nil {
			t.Fatalf("Failed to notify: %v", err)
		}
	}

	sendNotification("payload1")

	select {
	case notification := <-listener.Notifications():
		if notification.Payload != "payload1" {
			t.Errorf("Expected payload1, got %s", notification.Payload)
		}
	case err := <-listener.Errors():
		t.Fatalf("Received error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for payload1")
	}

	killerConn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		t.Fatalf("Failed to connect killer: %v", err)
	}
	defer killerConn.Close(context.Background())

	_, err = killerConn.Exec(context.Background(), `
		SELECT pg_terminate_backend(pid) 
		FROM pg_stat_activity 
		WHERE query LIKE '%LISTEN%' AND pid <> pg_backend_pid()
	`)
	if err != nil {
		t.Fatalf("Failed to terminate backend: %v", err)
	}

	select {
	case err := <-listener.Errors():
		t.Logf("Successfully detected connection loss: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for connection loss detection")
	}

	time.Sleep(1 * time.Second)

	sendNotification("payload2")

	select {
	case notification := <-listener.Notifications():
		if notification.Payload != "payload2" {
			t.Errorf("Expected payload2, got %s", notification.Payload)
		}
	case err := <-listener.Errors():
		t.Fatalf("Received error after reconnect: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for payload2 after reconnect")
	}
}

func TestListenerRaceConditionDuringReconnect(t *testing.T) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	testConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	testConn.Close(context.Background())

	listener := NewListener(connStr)
	listener.reconnectDelay = 100 * time.Millisecond
	listener.Start()
	defer listener.Close()

	killerConn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		t.Fatalf("Failed to connect killer: %v", err)
	}
	defer killerConn.Close(context.Background())

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			channel := fmt.Sprintf("channel_%d", i)
			_ = listener.Listen(context.Background(), channel)
			time.Sleep(10 * time.Millisecond)
		}
		close(done)
	}()

	for i := 0; i < 5; i++ {
		_, _ = killerConn.Exec(context.Background(), `
			SELECT pg_terminate_backend(pid) 
			FROM pg_stat_activity 
			WHERE query LIKE '%LISTEN%' AND pid <> pg_backend_pid()
		`)
		time.Sleep(50 * time.Millisecond)
	}

	<-done

	listener.mu.RLock()
	channelCount := len(listener.subscribedChannels)
	listener.mu.RUnlock()

	if channelCount != 50 {
		t.Errorf("Expected 50 subscribed channels, got %d", channelCount)
	}
}
