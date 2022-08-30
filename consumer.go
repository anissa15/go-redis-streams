package streams

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Consumer interface {
	SetCount(count int64)
	SetTimeout(timeout time.Duration)

	ConsumeMessages(ctx context.Context) (messageIDs []string, messageValues []map[string]interface{}, err error)
	AckMessageByIDs(ctx context.Context, messageIDs ...string) error
	GetPendingMessageIDs(ctx context.Context) (messageIDs []string, err error)
}

type streamConsumer struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
	count    int64
	timeout  time.Duration
}

func NewConsumer(client *redis.Client, stream, group, consumer string) (Consumer, error) {
	ctx := context.Background()
	err := client.XGroupCreateMkStream(ctx, stream, group, StartFromBeginning).Err()
	if err != nil && !errConsumerGroupAlreadyExists(err) {
		return nil, fmt.Errorf("failed create group: %v", err)
	}
	err = client.XGroupCreateConsumer(ctx, stream, group, consumer).Err()
	if err != nil {
		return nil, fmt.Errorf("failed create consumer group: %v", err)
	}
	return &streamConsumer{
		client:   client,
		stream:   stream,
		group:    group,
		consumer: consumer,
		count:    1, // default count = 1
		timeout:  0, // default timeout = 0 (no timeout, always waiting for new message)
	}, nil
}

// SetCount set number of message to be consumed
func (c *streamConsumer) SetCount(count int64) {
	c.count = count
}

// SetTimeout set timeout of consumer waiting the message
func (c *streamConsumer) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

func (c *streamConsumer) ConsumeMessages(ctx context.Context) (messageIDs []string, messageValues []map[string]interface{}, err error) {
	a := &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumer,
		Streams:  []string{c.stream, IdNewMessage},
		Count:    c.count,
		Block:    c.timeout,
		NoAck:    false,
	}
	results, err := c.client.XReadGroup(ctx, a).Result()
	if err != nil && err != redis.Nil {
		return nil, nil, err
	}
	for _, result := range results {
		for _, v := range result.Messages {
			messageIDs = append(messageIDs, v.ID)
			messageValues = append(messageValues, v.Values)
		}
	}
	return messageIDs, messageValues, nil
}

func (c *streamConsumer) AckMessageByIDs(ctx context.Context, messageIDs ...string) error {
	return c.client.XAck(ctx, c.stream, c.group, messageIDs...).Err()
}

func (c *streamConsumer) GetPendingMessageIDs(ctx context.Context) (messageIDs []string, err error) {
	a := &redis.XPendingExtArgs{
		Stream:   c.stream,
		Group:    c.group,
		Consumer: c.consumer,
		Idle:     0,
		Start:    "-",
		End:      "+",
		Count:    c.count,
	}
	results, err := c.client.XPendingExt(ctx, a).Result()
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		messageIDs = append(messageIDs, result.ID)
	}
	return messageIDs, nil
}
