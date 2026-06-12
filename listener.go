package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Listener wraps a pgx connection and manages LISTEN/UNLISTEN subscriptions,
// automatically reconnecting and re-subscribing when the connection is lost.
type Listener struct {
	connStr            string
	conn               *pgx.Conn
	mu                 sync.RWMutex
	subscribedChannels map[string]bool
	notificationChan   chan *pgx.Notification
	errChan            chan error
	ctx                context.Context
	cancel             context.CancelFunc
	reconnectDelay     time.Duration
	isClosed           bool
	wg                 sync.WaitGroup
}

// NewListener creates a new Listener instance.
func NewListener(connStr string) *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &Listener{
		connStr:            connStr,
		subscribedChannels: make(map[string]bool),
		notificationChan:   make(chan *pgx.Notification, 100),
		errChan:            make(chan error, 10),
		ctx:                ctx,
		cancel:             cancel,
		reconnectDelay:     2 * time.Second,
	}
}

// Start starts the connection and notification listening loop.
func (l *Listener) Start() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.listenLoop()
	}()
}

// Close closes the listener and the underlying connection.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.isClosed {
		l.mu.Unlock()
		return nil
	}
	l.isClosed = true
	l.cancel()
	if l.conn != nil {
		l.conn.Close(context.Background())
	}
	l.mu.Unlock()

	l.wg.Wait()
	close(l.notificationChan)
	close(l.errChan)
	return nil
}

// Listen subscribes to a channel.
func (l *Listener) Listen(ctx context.Context, channel string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isClosed {
		return fmt.Errorf("listener is closed")
	}

	l.subscribedChannels[channel] = true

	if l.conn != nil {
		_, err := l.conn.Exec(ctx, fmt.Sprintf("LISTEN %s", pgx.Identifier{channel}.Sanctify()))
		if err != nil {
			return fmt.Errorf("failed to listen on channel %s: %w", channel, err)
		}
	}
	return nil
}

// Unlisten unsubscribes from a channel.
func (l *Listener) Unlisten(ctx context.Context, channel string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isClosed {
		return fmt.Errorf("listener is closed")
	}

	delete(l.subscribedChannels, channel)

	if l.conn != nil {
		_, err := l.conn.Exec(ctx, fmt.Sprintf("UNLISTEN %s", pgx.Identifier{channel}.Sanctify()))
		if err != nil {
			return fmt.Errorf("failed to unlisten on channel %s: %w", channel, err)
		}
	}
	return nil
}

// Notifications returns the channel for receiving notifications.
func (l *Listener) Notifications() <-chan *pgx.Notification {
	return l.notificationChan
}

// Errors returns the channel for receiving errors (e.g., connection or subscription errors).
func (l *Listener) Errors() <-chan error {
	return l.errChan
}

func (l *Listener) listenLoop() {
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		err := l.connectAndSubscribe()
		if err != nil {
			l.sendErr(err)
			select {
			case <-l.ctx.Done():
				return
			case <-time.After(l.reconnectDelay):
				continue
			}
		}

		for {
			notification, err := l.conn.WaitForNotification(l.ctx)
			if err != nil {
				select {
				case <-l.ctx.Done():
					return
				default:
					l.sendErr(fmt.Errorf("connection lost: %w", err))
				}
				break
			}
			select {
			case l.notificationChan <- notification:
			case <-l.ctx.Done():
				return
			}
		}

		l.mu.Lock()
		if l.conn != nil {
			l.conn.Close(context.Background())
			l.conn = nil
		}
		l.mu.Unlock()
	}
}

func (l *Listener) connectAndSubscribe() error {
	conn, err := pgx.Connect(l.ctx, l.connStr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isClosed {
		conn.Close(context.Background())
		return fmt.Errorf("listener is closed")
	}

	// Re-subscribe to all channels in the registry
	for channel := range l.subscribedChannels {
		_, err := conn.Exec(l.ctx, fmt.Sprintf("LISTEN %s", pgx.Identifier{channel}.Sanctify()))
		if err != nil {
			conn.Close(context.Background())
			return fmt.Errorf("failed to re-subscribe to channel %s: %w", channel, err)
		}
	}

	l.conn = conn
	return nil
}

func (l *Listener) sendErr(err error) {
	select {
	case l.errChan <- err:
	default:
	}
}
