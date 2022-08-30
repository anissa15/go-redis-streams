package streams

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type Publisher interface {
	Publish(ctx context.Context, key string, values map[string]interface{}) error
}

type publisher struct {
	client *redis.Client
}

func NewPublisher(client *redis.Client) Publisher {
	return &publisher{
		client: client,
	}
}

func (p *publisher) Publish(ctx context.Context, stream string, values map[string]interface{}) error {
	a := &redis.XAddArgs{
		Stream:     stream,
		NoMkStream: false,
		MaxLen:     0,
		MinID:      "",
		Approx:     false,
		Limit:      0,
		ID:         "",
		Values:     values,
	}
	return p.client.XAdd(ctx, a).Err()
}
