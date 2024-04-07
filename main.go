package main

import (
	"encoding/json"
	"fmt"
	"os"

	"log"
	"math/rand"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (topic = "foobarbaztopic"

)
type MessageState int 
const (
	MessageStateCompleted  = iota
	MessageStateProgress
	MessageStateFailed
)

type Message struct{
	State MessageState
}
func main() {
	Produce()
	consume()
}
func consume(){
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093",
		
		
		"broker.address.family": "v4",
		"group.id":              "group1",
		"session.timeout.ms":    6000,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": "earliest",
		// Whether or not we store offsets automatically.
		"enable.auto.offset.store": false,
	})

	if err != nil {
		log.Fatal(err)
		
	}
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil{
		log.Fatal(err)
	}

	for {
		ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				// Process the message received.
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				

				
				_, err := c.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
						e.TopicPartition)
				}
			case kafka.Error:
				
				// fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					break
					
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
	} 
	
}
func Produce()  {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093"})
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	for i := 0; i <1000; i++{ 

		msg := Message{
			State: MessageState(rand.Intn(3)),
		}

		b,err := json.Marshal(msg)
		if err != nil{
			log.Fatal(err)
		} 

	

	
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic, Partition:
			 kafka.PartitionAny},
		Value:b,
	}, nil)

	if err != nil {
		log.Fatal(err)
	}
} 

}