package graphql_service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// API_SERVICE_URL is the endpoint of your GojoDB API Service.
const API_SERVICE_URL = "http://localhost:8090/api/data"
const CLIENT_TIMEOUT = 10 * time.Second

type APIRequest struct {
	Command string `json:"command"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"` // Optional for GET/DELETE
}

// APIResponse represents a response sent by the API service to the client.
type APIResponse struct {
	Status  string `json:"status"`            // OK, ERROR, NOT_FOUND, REDIRECT
	Message string `json:"message,omitempty"` // Details or value for GET, or target node address for REDIRECT
}

// Resolver is the root resolver for your GraphQL schema.
type Resolver struct{}

// Query returns the QueryResolver implementation.
func (r *Resolver) Query() QueryResolver { return QueryResolver{r} }

// Mutation returns the MutationResolver implementation.
func (r *Resolver) Mutation() MutationResolver { return MutationResolver{r} }

type QueryResolver struct{ *Resolver }

// Get resolves the 'get' query. It calls the GojoDB API Service.
func (r *QueryResolver) Get(ctx context.Context, key string) (*Entry, error) {
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
		return &Entry{Key: key, Value: apiResp.Message}, nil
	} else if apiResp.Status == "NOT_FOUND" {
		return nil, nil // Return nil Entry, nil error for not found
	} else {
		return nil, fmt.Errorf("API Service error for GET %s: %s", key, apiResp.Message)
	}
}

type MutationResolver struct{ *Resolver }

// Put resolves the 'put' mutation. It calls the GojoDB API Service.
func (r *MutationResolver) Put(ctx context.Context, key string, value string) (*Entry, error) {
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
		return &Entry{Key: key, Value: value}, nil
	} else {
		return nil, fmt.Errorf("API Service error for PUT %s=%s: %s", key, value, apiResp.Message)
	}
}

// Delete resolves the 'delete' mutation. It calls the GojoDB API Service.
func (r *MutationResolver) Delete(ctx context.Context, key string) (bool, error) {
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
