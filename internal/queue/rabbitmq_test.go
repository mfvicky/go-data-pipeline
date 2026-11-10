package queue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitPublishConsume(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("set TEST_RABBITMQ_URL to run broker integration test")
	}
	name := fmt.Sprintf("pipeline.integration.%d", time.Now().UnixNano())
	rabbit, err := NewRabbit(url, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rabbit.Close() }()
	defer func() {
		conn, err := amqp.Dial(url)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer func() { _ = ch.Close() }()
		_, _ = ch.QueueDelete(name, false, false, false)
		_, _ = ch.QueueDelete(name+".dead", false, false, false)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	received := make(chan Message, 1)
	wait := rabbit.Consume(ctx, 1, func(_ context.Context, id, requestID string) error {
		select {
		case received <- Message{JobID: id, RequestID: requestID}:
		default:
		}
		return nil
	})
	if err := rabbit.Publish(ctx, "job-123", "request-abc"); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-received:
		if message.JobID != "job-123" || message.RequestID != "request-abc" {
			t.Fatalf("message: %+v", message)
		}
	case <-ctx.Done():
		t.Fatal("message not received")
	}
	cancel()
	wait.Wait()
}
