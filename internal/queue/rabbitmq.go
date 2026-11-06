package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Rabbit has a confirmed publisher; each consumer uses a separate connection.
// The message body contains only IDs; PostgreSQL remains the source of truth.
type Rabbit struct {
	url, name string
	mu        sync.Mutex
	conn      *amqp.Connection
	channel   *amqp.Channel
	confirms  <-chan amqp.Confirmation
	closed    bool
	logger    *slog.Logger
}

type Message struct {
	JobID     string `json:"job_id"`
	RequestID string `json:"request_id,omitempty"`
}

func NewRabbit(url, queueName string, logger *slog.Logger) (*Rabbit, error) {
	if url == "" || queueName == "" {
		return nil, errors.New("RABBITMQ_URL and RABBITMQ_QUEUE are required")
	}
	r := &Rabbit{url: url, name: queueName, logger: logger}
	if err := r.connectPublisher(); err != nil {
		return nil, err
	}
	return r, nil
}
func declare(ch *amqp.Channel, name string) error {
	if _, err := ch.QueueDeclare(name+".dead", true, false, false, false, nil); err != nil {
		return err
	}
	_, err := ch.QueueDeclare(name, true, false, false, false, amqp.Table{
		// A RabbitMQ quorum queue bounds the number of failed redeliveries.
		// Poison messages are retained in the durable dead-letter queue.
		"x-queue-type": "quorum", "x-delivery-limit": int32(10),
		"x-dead-letter-exchange": "", "x-dead-letter-routing-key": name + ".dead",
	})
	return err
}

// Caller must hold r.mu except during initial construction.
func (r *Rabbit) connectPublisher() error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return fmt.Errorf("connect RabbitMQ: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err = declare(ch, r.name); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return err
	}
	if err = ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return err
	}
	r.conn = conn
	r.channel = ch
	r.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return nil
}
func (r *Rabbit) dropPublisher() {
	if r.channel != nil {
		_ = r.channel.Close()
		r.channel = nil
	}
	if r.conn != nil {
		_ = r.conn.Close()
		r.conn = nil
	}
	r.confirms = nil
}

// Publish succeeds only after broker publisher confirmation. A timeout is
// ambiguous: the consumer must tolerate duplicate/replayed job IDs.
func (r *Rabbit) Publish(ctx context.Context, id, requestID string) error {
	payload, err := json.Marshal(Message{JobID: id, RequestID: requestID})
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("RabbitMQ publisher closed")
	}
	if r.channel == nil || r.conn == nil || r.conn.IsClosed() {
		r.dropPublisher()
		if err := r.connectPublisher(); err != nil {
			return err
		}
	}
	if err := r.channel.PublishWithContext(ctx, "", r.name, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: id,
		CorrelationId: requestID, Timestamp: time.Now().UTC(), Body: payload,
	}); err != nil {
		r.dropPublisher()
		return fmt.Errorf("publish job: %w", err)
	}
	select {
	case confirmation, ok := <-r.confirms:
		if !ok || !confirmation.Ack {
			r.dropPublisher()
			return errors.New("RabbitMQ did not confirm job")
		}
		return nil
	case <-ctx.Done():
		r.dropPublisher()
		return ctx.Err()
	}
}
func (r *Rabbit) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.dropPublisher()
	return nil
}

// Consume starts bounded workers with prefetch=1. Valid work is ACKed only
// after its result and final status are saved, otherwise NACKed for retry.
func (r *Rabbit) Consume(ctx context.Context, workers int, handle func(context.Context, string, string) error) *sync.WaitGroup {
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.consumeLoop(ctx, handle) }()
	}
	return &wg
}
func (r *Rabbit) consumeLoop(ctx context.Context, handle func(context.Context, string, string) error) {
	for ctx.Err() == nil {
		if err := r.consumeOnce(ctx, handle); err != nil && ctx.Err() == nil && r.logger != nil {
			r.logger.Error("rabbitmq consumer reconnecting", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
func (r *Rabbit) consumeOnce(ctx context.Context, handle func(context.Context, string, string) error) error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	if err := declare(ch, r.name); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(r.name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("RabbitMQ consumer channel closed")
			}
			var message Message
			if err := json.Unmarshal(delivery.Body, &message); err != nil || message.JobID == "" {
				if r.logger != nil {
					r.logger.Warn("dead-lettering invalid job message")
				}
				if ackErr := delivery.Nack(false, false); ackErr != nil {
					return ackErr
				}
				continue
			}
			err = handle(ctx, message.JobID, message.RequestID)
			if err == nil {
				if ackErr := delivery.Ack(false); ackErr != nil {
					return ackErr
				}
			} else {
				if r.logger != nil {
					r.logger.Error("job processing failed; message will retry", "job_id", message.JobID, "error", err)
				}
				if nackErr := delivery.Nack(false, true); nackErr != nil {
					return nackErr
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}
		}
	}
}
