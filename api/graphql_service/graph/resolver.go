package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sushant-115/gojodb/api/graphql_service/graph/model"
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus" // Import FSM for SlotRangeInfo
)

// API_SERVICE_URL is the endpoint of your GojoDB API Service.
const API_SERVICE_URL = "http://localhost:8090/api/data"
const ADMIN_SERVICE_URL = "http://localhost:8090/admin/"
const STATUS_SERVICE_URL = "http://localhost:8090/status"
const QUERY_SERVICE_URL = "http://localhost:8090/api/query"
const TRANSACTION_SERVICE_URL = "http://localhost:8090/api/transaction"
const CLIENT_TIMEOUT = 10 * time.Second
const ADMIN_API_KEY = "GOJODB_ADMIN_KEY" // Must match the key in api_service/main.go

// Resolver is the root resolver for your GraphQL schema.
type Resolver struct{}

// APIRequest represents a client request received by the API service.
type APIRequest struct {
	Command  string `json:"command"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`     // Optional for GET/DELETE
	StartKey string `json:"start_key,omitempty"` // For range ops
	EndKey   string `json:"end_key,omitempty"`   // For range ops
}

// APIResponse represents a response sent by the API service to the client.
type APIResponse struct {
	Status  string          `json:"status"`            // OK, ERROR, NOT_FOUND, REDIRECT, COMMITTED, ABORTED
	Message string          `json:"message,omitempty"` // Details or value for GET, or target node address for REDIRECT
	Data    json.RawMessage `json:"data,omitempty"`    // Flexible field for returning structured data (e.g., array of entries, aggregation result)
}

// TransactionOperation defines a single operation within a distributed transaction.
type TransactionOperation struct {
	Command string `json:"command"` // PUT or DELETE
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// TransactionRequest represents a request for a distributed transaction.
type TransactionRequest struct {
	Operations []TransactionOperation `json:"operations"`
}

// Query returns the QueryResolver implementation.
func (r *Resolver) Query() QueryResolver { return &queryResolver{r} }

// Mutation returns the MutationResolver implementation.
func (r *Resolver) Mutation() MutationResolver { return &mutationResolver{r} }

type queryResolver struct{ *Resolver }

// Get resolves the 'get' query. It calls the GojoDB API Service.
func (r *queryResolver) Get(ctx context.Context, key string) (*model.Entry, error) {
	log.Printf("GraphQL: Resolving GET for key: %s", key)

	reqBody := APIRequest{
		Command: "GET",
		Key:     key,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GET request: %w", err)
	}

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Post(API_SERVICE_URL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send GET request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read API Service response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal API Service response: %w. Raw: %s", err, string(bodyBytes))
	}

	if apiResp.Status == "OK" {
		return &model.Entry{Key: key, Value: apiResp.Message}, nil
	} else if apiResp.Status == "NOT_FOUND" {
		return nil, nil // Return nil Entry, nil error for not found
	} else {
		return nil, fmt.Errorf("API Service error for GET %s: %s", key, apiResp.Message)
	}
}

// GetRange resolves the 'getRange' query.
func (r *queryResolver) GetRange(ctx context.Context, startKey string, endKey string) ([]*model.Entry, error) {
	log.Printf("GraphQL: Resolving GET_RANGE for keys: %s to %s", startKey, endKey)

	reqBody := APIRequest{
		Command:  "GET_RANGE",
		StartKey: startKey,
		EndKey:   endKey,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GET_RANGE request: %w", err)
	}

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Post(QUERY_SERVICE_URL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send GET_RANGE request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read API Service response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal API Service response: %w. Raw: %s", err, string(bodyBytes))
	}

	if apiResp.Status == "OK" {
		// The API Service returns a formatted string like "Result: Key: k1 Value: v1   Key: k2 Value: v2"
		// We need to parse this string into a slice of Entry objects.
		// This is fragile; ideally, the API service would return structured JSON data.
		var entries []*model.Entry
		message := apiResp.Message
		if apiResp.Data != nil {
			// If API Service returns structured JSON data, use it.
			// Assuming Data is a JSON array of { "Key": "...", "Value": "..." }
			var rawEntries []struct {
				Key   string `json:"Key"`
				Value string `json:"Value"`
			}
			if err := json.Unmarshal(apiResp.Data, &rawEntries); err == nil {
				for _, re := range rawEntries {
					entries = append(entries, &model.Entry{Key: re.Key, Value: re.Value})
				}
				return entries, nil
			}
			// Fallback to message parsing if Data is not the expected format
			message = string(apiResp.Data)
		}

		// Fallback: Parse the string message
		if strings.HasPrefix(message, "Result: ") {
			message = strings.TrimPrefix(message, "Result: ")
		}
		pairs := strings.Split(message, "\t") // Assuming tab as delimiter between entries
		for _, pair := range pairs {
			if strings.TrimSpace(pair) == "" {
				continue
			}
			keyValParts := strings.SplitN(pair, " Value: ", 2)
			if len(keyValParts) == 2 {
				keyPart := strings.TrimPrefix(keyValParts[0], "Key: ")
				valuePart := keyValParts[1]
				entries = append(entries, &model.Entry{Key: strings.TrimSpace(keyPart), Value: strings.TrimSpace(valuePart)})
			}
		}
		return entries, nil
	} else {
		return nil, fmt.Errorf("API Service error for GET_RANGE %s-%s: %s", startKey, endKey, apiResp.Message)
	}
}

// Status resolves the 'status' query.
func (r *queryResolver) Status(ctx context.Context) (*model.StatusResponse, error) {
	log.Printf("GraphQL: Resolving Status query.")

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Get(STATUS_SERVICE_URL)
	if err != nil {
		return nil, fmt.Errorf("failed to send status request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read API Service status response: %w", err)
	}

	// The status endpoint returns plain text.
	return &model.StatusResponse{Message: strings.TrimSpace(string(bodyBytes))}, nil
}

// AdminGetNodeForKey resolves the 'adminGetNodeForKey' query.
func (r *queryResolver) AdminGetNodeForKey(ctx context.Context, key string) (*model.NodeForKeyResponse, error) {
	log.Printf("GraphQL: Resolving AdminGetNodeForKey for key: %s", key)

	url := fmt.Sprintf("%sget_node_for_key?key=%s", ADMIN_SERVICE_URL, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-API-Key", ADMIN_API_KEY)

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read API Service response: %w", err)
	}

	responseMessage := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API Service error for AdminGetNodeForKey %s: %s", key, responseMessage)
	}

	// Example response: "Key 'test' (slot 123) is assigned to Storage Node: storage1"
	// This parsing is fragile and assumes a specific format.
	parts := strings.Split(responseMessage, "is assigned to Storage Node: ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("unexpected response format from API Service: %s", responseMessage)
	}
	nodeID := strings.TrimSpace(parts[1])

	slotStr := ""
	slotParts := strings.Split(parts[0], "(slot ")
	if len(slotParts) > 1 {
		slotStr = strings.TrimSuffix(slotParts[1], ")")
	}
	slot, err := strconv.Atoi(slotStr)
	if err != nil {
		log.Printf("WARNING: Could not parse slot from response: %s. Error: %v", responseMessage, err)
		slot = 0 // Default or error value
	}

	return &model.NodeForKeyResponse{NodeID: nodeID, Slot: slot}, nil
}

type mutationResolver struct{ *Resolver }

// Put resolves the 'put' mutation. It calls the GojoDB API Service.
func (r *mutationResolver) Put(ctx context.Context, key string, value string) (*model.Entry, error) {
	log.Printf("GraphQL: Resolving PUT for key: %s, value: %s", key, value)

	reqBody := APIRequest{
		Command: "PUT",
		Key:     key,
		Value:   value,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal PUT request: %w", err)
	}

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Post(API_SERVICE_URL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send PUT request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read API Service response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal API Service response: %w. Raw: %s", err, string(bodyBytes))
	}

	if apiResp.Status == "OK" {
		return &model.Entry{Key: key, Value: value}, nil
	} else {
		return nil, fmt.Errorf("API Service error for PUT %s=%s: %s", key, value, apiResp.Message)
	}
}

// Delete resolves the 'delete' mutation. It calls the GojoDB API Service.
func (r *mutationResolver) Delete(ctx context.Context, key string) (bool, error) {
	log.Printf("GraphQL: Resolving DELETE for key: %s", key)

	reqBody := APIRequest{
		Command: "DELETE",
		Key:     key,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return false, fmt.Errorf("failed to marshal DELETE request: %w", err)
	}

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Post(API_SERVICE_URL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return false, fmt.Errorf("failed to send DELETE request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read API Service response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		return false, fmt.Errorf("failed to unmarshal API Service response: %w. Raw: %s", err, string(bodyBytes))
	}

	if apiResp.Status == "OK" {
		return true, nil
	} else if apiResp.Status == "NOT_FOUND" {
		return false, nil // Key not found, so not deleted
	} else {
		return false, fmt.Errorf("API Service error for DELETE %s: %s", key, apiResp.Message)
	}
}

// ExecuteTransaction resolves the 'executeTransaction' mutation.
func (r *mutationResolver) ExecuteTransaction(ctx context.Context, operations []*model.TransactionOperationInput) (*model.TransactionResult, error) {
	log.Printf("GraphQL: Resolving ExecuteTransaction with %d operations.", len(operations))

	var txOps []TransactionOperation
	for _, op := range operations {
		txOps = append(txOps, TransactionOperation{
			Command: op.Command,
			Key:     op.Key,
			Value:   op.Value,
		})
	}

	txReq := TransactionRequest{
		Operations: txOps,
	}

	jsonBody, err := json.Marshal(txReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transaction request: %w", err)
	}

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Post(TRANSACTION_SERVICE_URL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send transaction request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read API Service transaction response: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal API Service transaction response: %w. Raw: %s", err, string(bodyBytes))
	}

	return &model.TransactionResult{Status: apiResp.Status, Message: apiResp.Message}, nil
}

// AdminAssignSlotRange resolves the 'adminAssignSlotRange' mutation.
func (r *mutationResolver) AdminAssignSlotRange(ctx context.Context, input model.AssignSlotRangeInput) (bool, error) {
	log.Printf("GraphQL: Resolving AdminAssignSlotRange: %v", input)

	url := fmt.Sprintf("%sassign_slot_range?startSlot=%d&endSlot=%d&assignedNodeID=%s",
		ADMIN_SERVICE_URL, input.StartSlot, input.EndSlot, input.AssignedNodeID)

	if len(input.ReplicaNodeIDs) > 0 {
		url += fmt.Sprintf("&replicaNodes=%s", strings.Join(input.ReplicaNodeIDs, ","))
	}

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-API-Key", ADMIN_API_KEY)

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to send request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read API Service response: %w", err)
	}

	responseMessage := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode == http.StatusOK {
		log.Printf("GraphQL: AdminAssignSlotRange successful: %s", responseMessage)
		return true, nil
	} else {
		return false, fmt.Errorf("API Service error for AdminAssignSlotRange: %s", responseMessage)
	}
}

// AdminSetMetadata resolves the 'adminSetMetadata' mutation.
func (r *mutationResolver) AdminSetMetadata(ctx context.Context, key string, value string) (bool, error) {
	log.Printf("GraphQL: Resolving AdminSetMetadata: %s = %s", key, value)

	url := fmt.Sprintf("%smetadata?key=%s&value=%s", ADMIN_SERVICE_URL, key, value)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-API-Key", ADMIN_API_KEY)

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to send request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read API Service response: %w", err)
	}

	responseMessage := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode == http.StatusOK {
		log.Printf("GraphQL: AdminSetMetadata successful: %s", responseMessage)
		return true, nil
	} else {
		return false, fmt.Errorf("API Service error for AdminSetMetadata: %s", responseMessage)
	}
}

// AdminSetPrimaryReplica resolves the 'adminSetPrimaryReplica' mutation.
func (r *mutationResolver) AdminSetPrimaryReplica(ctx context.Context, input model.SetPrimaryReplicaInput) (bool, error) {
	log.Printf("GraphQL: Resolving AdminSetPrimaryReplica: %v", input)

	// The API service expects a JSON marshaled SlotRangeInfo in the 'value' query param.
	// We need to construct a fsm.SlotRangeInfo to marshal.
	slotInfo := fsm.SlotRangeInfo{
		RangeID:        input.RangeID,
		PrimaryNodeID:  input.PrimaryNodeID,
		ReplicaNodeIDs: input.ReplicaNodeIDs,
		Status:         "active", // Assuming active status for this operation
	}
	slotInfoBytes, err := json.Marshal(slotInfo)
	if err != nil {
		return false, fmt.Errorf("failed to marshal slot info for SetPrimaryReplica: %w", err)
	}
	slotInfoValue := string(slotInfoBytes)

	url := fmt.Sprintf("%sset_primary_replica?key=%s&value=%s", ADMIN_SERVICE_URL, input.RangeID, slotInfoValue)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-API-Key", ADMIN_API_KEY)

	httpClient := http.Client{Timeout: CLIENT_TIMEOUT}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to send request to API Service: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read API Service response: %w", err)
	}

	responseMessage := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode == http.StatusOK {
		log.Printf("GraphQL: AdminSetPrimaryReplica successful: %s", responseMessage)
		return true, nil
	} else {
		return false, fmt.Errorf("API Service error for AdminSetPrimaryReplica: %s", responseMessage)
	}
}
