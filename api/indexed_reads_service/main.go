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
	conn, err := grpc.Dial(DefaultGatewayAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to gateway: %v", err)
	}
	defer conn.Close()

	gatewayClient := pb.NewGatewayServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First, put some data for reading
	log.Println("--- Populating sample data for reads ---")
	_, err = gatewayClient.Put(ctx, &pb.PutRequest{Key: "doc:1", Value: []byte("The quick brown fox jumps over the lazy dog.")})
	if err != nil {
		log.Printf("Failed to put doc:1: %v", err)
	}
	_, err = gatewayClient.Put(ctx, &pb.PutRequest{Key: "doc:2", Value: []byte("Lazy cat sleeps on the mat.")})
	if err != nil {
		log.Printf("Failed to put doc:2: %v", err)
	}
	_, err = gatewayClient.Put(ctx, &pb.PutRequest{Key: "doc:3", Value: []byte("Fox is quick.")})
	if err != nil {
		log.Printf("Failed to put doc:3: %v", err)
	}
	log.Println("Sample data populated.")
	time.Sleep(500 * time.Millisecond) // Give time for data to be indexed

	log.Println("\n--- Performing Get Operation via Gateway ---")
	getResponse, err := gatewayClient.Get(ctx, &pb.GetRequest{Key: "doc:1"})
	if err != nil {
		log.Printf("could not get 'doc:1' via gateway: %v", err)
	} else {
		log.Printf("Get 'doc:1' Response: Found=%t, Value=%s", getResponse.GetFound(), string(getResponse.GetValue()))
	}

	log.Println("\n--- Performing GetRange Operation via Gateway ---")
	getRangeResp, err := gatewayClient.GetRange(ctx, &pb.GetRangeRequest{
		StartKey: "doc:1",
		EndKey:   "doc:3",
		Limit:    10,
	})
	if err != nil {
		log.Printf("could not get range 'doc:1' to 'doc:3' via gateway: %v", err)
	} else {
		log.Printf("GetRange Response for 'doc:1' to 'doc:3': %d entries retrieved.", len(getRangeResp.GetEntries()))
		for _, entry := range getRangeResp.GetEntries() {
			log.Printf("  Key: %s, Value: %s", entry.GetKey(), string(entry.GetValue()))
		}
	}

	log.Println("\n--- Performing TextSearch Operation via Gateway ---")
	// Note: The dummy index manager in gojodb_server performs a simple string contains check.
	// In a real system, 'index_name' would specify which inverted index to use.
	textSearchResp, err := gatewayClient.TextSearch(ctx, &pb.TextSearchRequest{
		Query:     "fox",
		IndexName: "content_index", // This would be the name of your inverted index
		Limit:     5,
	})
	if err != nil {
		log.Printf("could not text search 'fox' via gateway: %v", err)
	} else {
		log.Printf("TextSearch 'fox' Response: %d results.", len(textSearchResp.GetResults()))
		for _, result := range textSearchResp.GetResults() {
			log.Printf("  Key: %s, Score: %.2f", result.GetKey(), result.GetScore())
		}
	}
}
