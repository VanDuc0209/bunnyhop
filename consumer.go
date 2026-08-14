package bunnyhop

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler processes a delivered AMQP message.
// Return nil to ack the message, return an error to nack+requeue.
type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

// ConsumeOptions configures a consumer.
type ConsumeOptions struct {
	Queue     string     // Queue name to consume from
	Consumer  string     // Consumer tag (empty = auto-generated)
	AutoAck   bool       // Auto-acknowledge messages (not recommended for production)
	Exclusive bool       // Exclusive consumer
	Args      amqp.Table // Additional arguments
}

// Consume starts consuming messages from a queue and calls handler for each message.
// It automatically re-subscribes after reconnect.
// Blocks until ctx is cancelled.
//
// Example:
//
//	err := client.Consume(ctx, bunnyhop.ConsumeOptions{Queue: "my-queue"}, func(ctx context.Context, d amqp.Delivery) error {
//	    // process d.Body
//	    return nil // ack
//	})
func (c *Client) Consume(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	for {
		// Get the current channel (returns error if not connected)
		ch, err := c.GetChannel()
		if err != nil {
			// Not connected yet — wait and retry
			c.logger().Warn("Consumer waiting for connection on queue %s...", opts.Queue)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.config.ReconnectInterval):
				continue
			}
		}

		deliveries, err := ch.Consume(
			opts.Queue,
			opts.Consumer,
			opts.AutoAck,
			opts.Exclusive,
			false, // noLocal — not supported by RabbitMQ
			false, // noWait
			opts.Args,
		)
		if err != nil {
			c.logger().Error("Failed to start consumer on queue %s: %v", opts.Queue, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.config.ReconnectInterval):
				continue
			}
		}

		c.logger().Info("Consumer started on queue %s", opts.Queue)

		// Process until channel closes or ctx is cancelled
		if ctxDone := c.runConsumer(ctx, deliveries, handler, opts.AutoAck); ctxDone {
			c.logger().Info("Consumer stopped (context cancelled) on queue %s", opts.Queue)
			return ctx.Err()
		}

		// Channel closed (e.g. reconnect) — wait then re-subscribe
		c.logger().Warn("Consumer channel closed on queue %s, re-subscribing...", opts.Queue)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.config.ReconnectInterval):
		}
	}
}

// runConsumer processes deliveries until the channel closes or ctx is cancelled.
// Returns true if ctx was cancelled, false if the delivery channel closed.
func (c *Client) runConsumer(
	ctx context.Context,
	deliveries <-chan amqp.Delivery,
	handler MessageHandler,
	autoAck bool,
) (ctxCancelled bool) {
	for {
		select {
		case <-ctx.Done():
			return true

		case d, ok := <-deliveries:
			if !ok {
				// Delivery channel closed — channel/connection died
				return false
			}

			if err := handler(ctx, d); err != nil {
				c.logger().Error("Message handler error on queue, nacking: %v", err)
				if !autoAck {
					// Nack with requeue — let RabbitMQ retry
					if nackErr := d.Nack(false, true); nackErr != nil {
						c.logger().Error("Failed to nack message: %v", nackErr)
					}
				}
			} else if !autoAck {
				if ackErr := d.Ack(false); ackErr != nil {
					c.logger().Error("Failed to ack message: %v", ackErr)
				}
			}
		}
	}
}
