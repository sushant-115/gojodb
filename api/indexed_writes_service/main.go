package main

import (
	"context"
	"log"
	"time"

	pb "github.com/sushant-115/gojodb/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure" // Use insecure for local testing
)

const (
	// DefaultGatewayAddress is the address of the GojoDB Gateway.
	// This service (as a client) will connect to the gateway.
	DefaultGatewayAddress = "localhost:50051"
)

func main() {
	// Set up a connection to the gRPC server (the Gateway).
	conn, err := grpc.NewClient(DefaultGatewayAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to gateway: %v", err)
	}
	defer conn.Close()

	// Create a GatewayService client
	gatewayClient := pb.NewGatewayServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Println("--- Performing Put Operation via Gateway ---")
	putResponse, err := gatewayClient.Put(ctx, &pb.PutRequest{
		Key:   "user:123",
		Value: []byte(`{"name": "Alice", "age": 30}`),
	})
	if err != nil {
		log.Printf("could not put via gateway: %v", err)
	} else {
		log.Printf("Put Response: Success=%t, Message=%s", putResponse.GetSuccess(), putResponse.GetMessage())
	}

	log.Println("\n--- Performing another Put Operation (will update) ---")
	putResponse, err = gatewayClient.Put(ctx, &pb.PutRequest{
		Key:   "user:123",
		Value: []byte(`{"name": "Alice Smith", "age": 31}`),
	})
	if err != nil {
		log.Printf("could not put via gateway: %v", err)
	} else {
		log.Printf("Put Response: Success=%t, Message=%s", putResponse.GetSuccess(), putResponse.GetMessage())
	}

	log.Println("\n--- Performing Get Operation via Gateway (to verify) ---")
	getResponse, err := gatewayClient.Get(ctx, &pb.GetRequest{Key: "user:123"})
	if err != nil {
		log.Printf("could not get via gateway: %v", err)
	} else {
		log.Printf("Get Response: Found=%t, Value=%s", getResponse.GetFound(), string(getResponse.GetValue()))
	}

	log.Println("\n--- Performing Delete Operation via Gateway ---")
	deleteResponse, err := gatewayClient.Delete(ctx, &pb.DeleteRequest{Key: "user:123"})
	if err != nil {
		log.Printf("could not delete via gateway: %v", err)
	} else {
		log.Printf("Delete Response: Success=%t, Message=%s", deleteResponse.GetSuccess(), deleteResponse.GetMessage())
	}

	log.Println("\n--- Performing Get Operation via Gateway (to confirm deletion) ---")
	getResponse, err = gatewayClient.Get(ctx, &pb.GetRequest{Key: "user:123"})
	if err != nil {
		log.Printf("could not get via gateway: %v", err)
	} else {
		log.Printf("Get Response (after delete): Found=%t", getResponse.GetFound())
	}
}
