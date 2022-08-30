package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	streams "github.com/anissa15/go-redis-streams"
	"github.com/go-redis/redis/v8"
)

func main() {
	var (
		redisURL = "redis://:@localhost:26379/0"
		start    = time.Now()
		timeout  = 5 * time.Second

		streamKey = "abcd"
	)

	// setup redis client
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		panic(err)
	}
	client := redis.NewClient(opt)

	// initiate services
	a1 := newService("svcA", "pod1", client)
	a2 := newService("svcA", "pod2", client)

	b1 := newService("svcB", "pod1", client)
	b2 := newService("svcB", "pod2", client)

	c1 := newService("svcC", "pod1", client)
	c2 := newService("svcC", "pod2", client)
	c3 := newService("svcC", "pod3", client)

	// initiate publisher (a1,a2)
	a1.initPublisher()
	a2.initPublisher()

	// initiate consumer (b1,b2,c1,c2,c3)
	b1.initConsumer(streamKey)
	b2.initConsumer(streamKey)
	c1.initConsumer(streamKey)
	c2.initConsumer(streamKey)
	c3.initConsumer(streamKey)

	// initiate async task
	async := newAsync()

	// publisher do publish message
	async.do(func() {
		a1.publishRandomMessages(start, timeout, streamKey)
	})
	async.do(func() {
		a2.publishRandomMessages(start, timeout, streamKey)
	})

	// make sure publishers run first
	time.Sleep(time.Second)

	// consumer do consume message
	async.do(func() {
		b1.consumeAndAckMessages(start, timeout)
		b1.ackPendingMessages(start, timeout)
	})
	async.do(func() {
		b2.consumeAndAckMessages(start, timeout)
		b2.ackPendingMessages(start, timeout)
	})
	async.do(func() {
		c1.consumeAndAckMessages(start, timeout)
		c1.ackPendingMessages(start, timeout)
	})
	async.do(func() {
		c2.consumeAndAckMessages(start, timeout)
		c2.ackPendingMessages(start, timeout)
	})
	async.do(func() {
		c3.consumeAndAckMessages(start, timeout)
		c3.ackPendingMessages(start, timeout)
	})

	async.wait()

	// track how many message A publish, and B C consumed
	a1.Println("count published message:", a1.countMsgPublished)
	a2.Println("count published message:", a2.countMsgPublished)
	b1.Println("count consumed message:", b1.countMsgConsumed)
	b2.Println("count consumed message:", b2.countMsgConsumed)
	c1.Println("count consumed message:", c1.countMsgConsumed)
	c2.Println("count consumed message:", c2.countMsgConsumed)
	c3.Println("count consumed message:", c3.countMsgConsumed)

}

type service struct {
	serviceName string
	podName     string
	client      *redis.Client

	publisher         streams.Publisher
	countMsgPublished int

	consumer         streams.Consumer
	countMsgConsumed int
}

func newService(name, pod string, client *redis.Client) *service {
	return &service{
		serviceName: name,
		podName:     pod,
		client:      client,
	}
}

func (s *service) initPublisher() {
	s.publisher = streams.NewPublisher(s.client)
}

func (s *service) initConsumer(streamKey string) {
	consumer, err := streams.NewConsumer(s.client, streamKey, s.serviceName, s.podName)
	if err != nil {
		panic(fmt.Errorf("failed create service %s pod %s: %v", s.serviceName, s.podName, err))
	}
	s.consumer = consumer
}

// publishRandomMessages do generate random messages and publish it to streamKey as long as given timeout, with 1 second sleep before publish next message
func (s *service) publishRandomMessages(start time.Time, timeout time.Duration, streamKey string) {
	if s.publisher == nil {
		panic(fmt.Errorf("publisher for service %s pod %s not initialized", s.serviceName, s.podName))
	}

	for time.Now().Sub(start) <= timeout {
		ctx := context.Background()

		// generate random message
		message := map[string]interface{}{
			"name":       generateRandomStr(5),
			"amount":     generateRandomInt(1, 10),
			"created_at": time.Now().Unix()}

		// publish
		err := s.publisher.Publish(ctx, streamKey, message)
		if err != nil {
			panic(fmt.Errorf("failed publish message for service %s pod %s: %v", s.serviceName, s.podName, err))
		}
		s.countMsgPublished++
		s.Println("publish message")

		// sleep for 1 second before publish another message
		time.Sleep(time.Second)
	}
}

func generateRandomInt(min, max int) int {
	return rand.Intn(max-min) + min
}

func generateRandomStr(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// consumeAndAckMessages do consume with timeout and ack messages as long as given timeout
func (s *service) consumeAndAckMessages(start time.Time, timeout time.Duration) {
	if s.consumer == nil {
		panic(fmt.Errorf("consumer for service %s pod %s not initialized", s.serviceName, s.podName))
	}

	for time.Now().Sub(start) <= timeout {
		ctx := context.Background()

		s.consumer.SetTimeout(timeout)
		ids, values, err := s.consumer.ConsumeMessages(ctx)
		if err != nil {
			s.Println("failed consume:", err)
			break
		}
		if len(ids) == 0 {
			s.Println("timeout, no more message to be consumed")
			break
		}

		s.Println("success consume messages:", values)

		if len(ids) > 0 {
			err = s.consumer.AckMessageByIDs(ctx, ids...)
			if err != nil {
				s.Println("failed ack:", err)
			} else {
				s.Println("success ack message ids:", ids)
			}
		}
		s.countMsgConsumed += len(ids)
	}
}

// ackPendingMessages do get pending message ids and ack the messages (if any) as long as given timeout
func (s *service) ackPendingMessages(start time.Time, timeout time.Duration) {
	if s.consumer == nil {
		panic(fmt.Errorf("consumer for service %s pod %s not initialized", s.serviceName, s.podName))
	}

	for time.Now().Sub(start) <= timeout {
		ctx := context.Background()

		ids, err := s.consumer.GetPendingMessageIDs(ctx)
		if err != nil {
			s.Println("failed get pending message:", err)
			break
		}
		if len(ids) == 0 {
			s.Println("no pending message")
			break
		}

		for _, id := range ids {
			err = s.consumer.AckMessageByIDs(ctx, id)
			if err != nil {
				s.Println("failed ack:", err)
			} else {
				s.Println("success ack message id:", id)
			}
		}
		s.countMsgConsumed += len(ids)
	}
}

func (s *service) Println(v ...any) {
	tag := "[" + s.serviceName + ":" + s.podName + "]"
	log.Println(tag, fmt.Sprint(v...))
}

type async struct {
	wg sync.WaitGroup
}

func newAsync() *async {
	return &async{}
}

func (a *async) do(callbacks ...func()) {
	go func() {
		for _, callback := range callbacks {
			defer a.wg.Done()
			a.wg.Add(1)
			callback()
		}
	}()
}

func (a *async) wait() {
	a.wg.Wait()
}
