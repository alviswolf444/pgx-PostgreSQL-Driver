package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

func main() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	fmt.Println("Starting PostgreSQL Listener...")
	listener := NewListener(connStr)
	listener.Start()
	defer listener.Close()

	err := listener.Listen(context.Background(), "test_events")
	if err != nil {
		fmt.Printf("Error subscribing: %v\n", err)
		return
	}
	fmt.Println("Subscribed to 'test_events'. Waiting for notifications...")

	go func() {
		for err := range listener.Errors() {
			fmt.Printf("Listener error: %v\n", err)
		}
	}()

	timeout := time.After(10 * time.Second)
	for {
		select {
		case notification := <-listener.Notifications():
			fmt.Printf("Received notification: Channel=%s, Payload=%s\n", notification.Channel, notification.Payload)
		case <-timeout:
			fmt.Println("Demo finished.")
			return
		}
	}
}
