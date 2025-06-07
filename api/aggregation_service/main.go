package main

import (
	"context"
	"encoding/json"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Println("--- Performing GetRange (simulating aggregation input) via Gateway ---")
	// For demonstration, we'll retrieve a range of keys.
	// In a real aggregation service, this data would then be processed.
	getRangeResp, err := gatewayClient.GetRange(ctx, &pb.GetRangeRequest{
		StartKey: "product:100",
		EndKey:   "product:999",
		Limit:    10,
	})
	if err != nil {
		log.Printf("could not get range via gateway: %v", err)
	} else {
		log.Printf("GetRange Response: %d entries retrieved.", len(getRangeResp.GetEntries()))
		for _, entry := range getRangeResp.GetEntries() {
			log.Printf("  Key: %s, Value: %s", entry.GetKey(), string(entry.GetValue()))
		}
	}

	// Simulate some data for aggregation example
	log.Println("\n--- Populating sample data for aggregation (e.g., sales data) ---")
	sampleData := []*pb.KeyValuePair{
		{Key: "sale:jan:1", Value: []byte(`{"amount": 100, "region": "north"}`)},
		{Key: "sale:jan:2", Value: []byte(`{"amount": 150, "region": "south"}`)},
		{Key: "sale:feb:1", Value: []byte(`{"amount": 200, "region": "north"}`)},
		{Key: "sale:feb:2", Value: []byte(`{"amount": 50, "region": "west"}`)},
	}
	bulkPutResp, err := gatewayClient.BulkPut(ctx, &pb.BulkPutRequest{Entries: sampleData})
	if err != nil {
		log.Printf("could not bulk put sample data: %v", err)
	} else {
		log.Printf("Bulk Put Sample Data Response: Success=%t, Message=%s", bulkPutResp.GetSuccess(), bulkPutResp.GetMessage())
	}

	time.Sleep(1 * time.Second) // Give some time for data to propagate if async

	// Simulate aggregation: fetch all sales and sum amounts (client-side aggregation)
	log.Println("\n--- Simulating client-side aggregation (sum of sales amounts) ---")
	allSales, err := gatewayClient.GetRange(ctx, &pb.GetRangeRequest{
		StartKey: "sale:jan:1",
		EndKey:   "sale:zzz", // A large enough range to cover all "sale:" keys
		Limit:    0,          // No limit, retrieve all
	})
	if err != nil {
		log.Printf("could not get all sales data for aggregation: %v", err)
	} else {
		totalAmount := 0.0
		for _, saleEntry := range allSales.GetEntries() {
			var sale struct {
				Amount float64 `json:"amount"`
				Region string  `json:"region"`
			}
			if err := json.Unmarshal(saleEntry.GetValue(), &sale); err != nil {
				log.Printf("Error unmarshaling sale data for key %s: %v", saleEntry.GetKey(), err)
				continue
			}
			totalAmount += sale.Amount
		}
		log.Printf("Aggregated Total Sales Amount: %.2f", totalAmount)
	}
}
