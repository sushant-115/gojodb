package main

import (
	"context"
	"log"
	"time"

	pb "github.com/sushant-115/gojodb/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultGatewayAddress = "localhost:50051"
)

func main() {
	conn, err := grpc.NewClient(DefaultGatewayAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to gateway: %v", err)
	}
	defer conn.Close()

	gatewayClient := pb.NewGatewayServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Println("--- Performing Bulk Put Operation via Gateway ---")
	bulkPutResponse, err := gatewayClient.BulkPut(ctx, &pb.BulkPutRequest{
		Entries: []*pb.KeyValuePair{
			{Key: "item:apple", Value: []byte(`{"price": 1.0, "quantity": 100}`)},
			{Key: "item:banana", Value: []byte(`{"price": 0.5, "quantity": 200}`)},
			{Key: "item:orange", Value: []byte(`{"price": 1.2, "quantity": 150}`)},
		},
	})
	if err != nil {
		log.Printf("could not bulk put via gateway: %v", err)
	} else {
		log.Printf("Bulk Put Response: Success=%t, Message=%s", bulkPutResponse.GetSuccess(), bulkPutResponse.GetMessage())
	}

	log.Println("\n--- Performing Get (to verify bulk put) ---")
	// Verify one of the items
	getResp, err := gatewayClient.Get(ctx, &pb.GetRequest{Key: "item:apple"})
	if err != nil {
		log.Printf("could not get 'item:apple': %v", err)
	} else {
		log.Printf("Get 'item:apple': Found=%t, Value=%s", getResp.GetFound(), string(getResp.GetValue()))
	}

	log.Println("\n--- Performing Bulk Delete Operation via Gateway ---")
	bulkDeleteResponse, err := gatewayClient.BulkDelete(ctx, &pb.BulkDeleteRequest{
		Keys: []string{"item:banana", "item:orange"},
	})
	if err != nil {
		log.Printf("could not bulk delete via gateway: %v", err)
	} else {
		log.Printf("Bulk Delete Response: Success=%t, Message=%s", bulkDeleteResponse.GetSuccess(), bulkDeleteResponse.GetMessage())
	}

	log.Println("\n--- Performing Get (to confirm bulk delete) ---")
	// Confirm deletion of one of the items
	getResp, err = gatewayClient.Get(ctx, &pb.GetRequest{Key: "item:banana"})
	if err != nil {
		log.Printf("could not get 'item:banana': %v", err)
	} else {
		log.Printf("Get 'item:banana' (after delete): Found=%t", getResp.GetFound())
	}
}
