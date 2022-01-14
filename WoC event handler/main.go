package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-redis/redis"
	"github.com/rs/xid"
)

type Data struct {
	events []Event
}

type Event struct {
	event_name string
	clients    []Client
}

type Client struct {
	name string
	url  string
}

func main() {
	data := Data{
		events: []Event{
			{
				event_name: "products",
				clients: []Client{
					{
						name: "client1",
						url:  "http://localhost:8081/new-data",
					},
				},
			},
		},
	}

	log.Println("Consumer started")

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", "127.0.0.1", "6379"),
	})
	_, err := redisClient.Ping().Result()
	if err != nil {
		log.Fatal("Unbale to connect to Redis", err)
	}

	log.Println("Connected to Redis server")

	subject := "products"
	consumersGroup := "products-consumer-group"

	err = redisClient.XGroupCreate(subject, consumersGroup, "0").Err()
	if err != nil {
		log.Println(err)
	}

	uniqueID := xid.New().String()

	for {
		entries, err := redisClient.XReadGroup(&redis.XReadGroupArgs{
			Group:    consumersGroup,
			Consumer: uniqueID,
			Streams:  []string{subject, ">"},
			Count:    2,
			Block:    0,
			NoAck:    false,
		}).Result()
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < len(entries[0].Messages); i++ {
			messageID := entries[0].Messages[i].ID
			values := entries[0].Messages[i].Values
			eventDescription := fmt.Sprintf("%v", values["whatHappened"])
			// productID := fmt.Sprintf("%v", values["productID"])
			// productData := fmt.Sprintf("%v", values["productData"])

			if eventDescription == "product received" {
				err := handleNewproduct(data)
				if err != nil {
					log.Fatal(err)
				}
				redisClient.XAck(subject, consumersGroup, messageID)
			}
		}
	}
}

func handleNewproduct(data Data) error {
	log.Printf("Handling new product")
	for i := 0; i < len(data.events); i++ {
		if data.events[i].event_name == "products" {
			for j := 0; j < len(data.events[i].clients); j++ {
				go makeApiCall(data.events[i].clients[j].url)
			}
		}
	}

	return nil
}

func makeApiCall(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	log.Printf(resp.Status)
	return nil
}
