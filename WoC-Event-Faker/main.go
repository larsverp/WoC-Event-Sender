package main

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/go-redis/redis"
)

func main() {
	log.Println("Publisher started")

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", "127.0.0.1", "6379"),
	})
	_, err := redisClient.Ping().Result()
	if err != nil {
		log.Fatal("Unbale to connect to Redis", err)
	}

	log.Println("Connected to Redis server")

	for i := 0; i < 1000; i++ {
		err = publishproductReceivedEvent(redisClient)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func publishproductReceivedEvent(client *redis.Client) error {
	log.Println("Publishing event to Redis")

	err := client.XAdd(&redis.XAddArgs{
		Stream:       "products",
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "",
		Values: map[string]interface{}{
			"whatHappened": string("product received"),
			"productID":    int(rand.Intn(100000000)),
			"productData":  string("some product data"),
		},
	}).Err()

	return err
}
