package main

import (
	"bufio" // Added for interactive mode
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	// Assuming api_service is in a sibling directory or properly imported in your Go module
	// You might need to adjust this import path based on your project structure.
	// For example: "your_module_name/api_service"
)

const (
	apiServiceURL    = "http://localhost:8090/api/data" // Target URL for the API Service's data endpoint
	adminServiceURL  = "http://localhost:8090/admin/"   // Base URL for admin endpoints
	statusServiceURL = "http://localhost:8090/status"   // URL for cluster status
	clientTimeout    = 10 * time.Second
	adminAPIKey      = "GOJODB_ADMIN_KEY" // Must match the key in api_service/main.go
)

// APIRequest represents a client request received by the API service.
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

// performDataRequest sends a JSON request to the API Service's /api/data endpoint.
func performDataRequest(cmd, key, value string) {
	reqBody := APIRequest{
		Command: cmd,
		Key:     key,
		Value:   value,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		log.Printf("Error marshalling request: %v", err)
		return
	}

	httpClient := http.Client{Timeout: clientTimeout}
	resp, err := httpClient.Post(apiServiceURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Printf("Error sending request to API Service: %v", err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %v. Raw response: %s", err, string(bodyBytes))
		return
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		log.Printf("Error unmarshalling response: %v. Raw response: %s", err, string(bodyBytes))
		return
		// Fallback for non-JSON responses (e.g., plain text errors from API service)
		// fmt.Printf("Raw API Service Response: %s\n", string(bodyBytes))
		// return
	}

	fmt.Printf("Response: Status=%s, Message='%s'\n", apiResp.Status, apiResp.Message)
}

// performAdminRequest sends an authenticated JSON request to an admin endpoint on the API Service.
func performAdminRequest(subPath, method, key, value, startSlot, endSlot, assignedNodeID string) {
	url := adminServiceURL + subPath
	var req *http.Request
	var err error

	// Build query parameters
	queryParams := make([]string, 0)
	if key != "" {
		queryParams = append(queryParams, fmt.Sprintf("key=%s", key))
	}
	if value != "" {
		queryParams = append(queryParams, fmt.Sprintf("value=%s", value))
	}
	if startSlot != "" {
		queryParams = append(queryParams, fmt.Sprintf("startSlot=%s", startSlot))
	}
	if endSlot != "" {
		queryParams = append(queryParams, fmt.Sprintf("endSlot=%s", endSlot))
	}
	if assignedNodeID != "" {
		queryParams = append(queryParams, fmt.Sprintf("assignedNodeID=%s", assignedNodeID))
	}

	fullURL := url
	if len(queryParams) > 0 {
		fullURL = fmt.Sprintf("%s?%s", url, strings.Join(queryParams, "&"))
	}

	// For simplicity, all admin operations are POST for now, except get_node_for_key
	if method == "GET" {
		req, err = http.NewRequest(http.MethodGet, fullURL, nil)
	} else { // POST
		req, err = http.NewRequest(http.MethodPost, fullURL, nil)
	}

	if err != nil {
		log.Printf("Error creating admin request: %v", err)
		return
	}

	req.Header.Set("X-API-Key", adminAPIKey) // Set the API Key header

	httpClient := http.Client{Timeout: clientTimeout}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Error sending admin request to API Service: %v", err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading admin response body: %v", err)
		return
	}

	fmt.Printf("Admin Response (Status: %s): %s\n", resp.Status, strings.TrimSpace(string(bodyBytes)))
}

// getClusterStatus fetches and prints the overall cluster status.
func getClusterStatus() {
	httpClient := http.Client{Timeout: clientTimeout}
	resp, err := httpClient.Get(statusServiceURL)
	if err != nil {
		log.Printf("Error fetching cluster status: %v", err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading status response body: %v", err)
		return
	}

	fmt.Printf("Cluster Status (Status: %s):\n%s\n", resp.Status, strings.TrimSpace(string(bodyBytes)))
}

// processCommand handles a single command, either from args or interactive mode.
func processCommand(args []string) {
	if len(args) == 0 {
		fmt.Println("Error: No command provided.")
		return
	}

	command := strings.ToLower(args[0])

	switch command {
	case "put":
		if len(args) < 3 {
			fmt.Println("Error: put command requires a key and a value.")
			return
		}
		performDataRequest("PUT", args[1], strings.Join(args[2:], " "))
	case "get":
		if len(args) < 2 {
			fmt.Println("Error: get command requires a key.")
			return
		}
		performDataRequest("GET", args[1], "")
	case "delete":
		if len(args) < 2 {
			fmt.Println("Error: delete command requires a key.")
			return
		}
		performDataRequest("DELETE", args[1], "")
	case "status":
		getClusterStatus()
	case "admin":
		if len(args) < 2 {
			fmt.Println("Error: admin command requires a sub-command.")
			return
		}
		adminSubCommand := strings.ToLower(args[1])
		switch adminSubCommand {
		case "assign_slot_range":
			if len(args) < 5 {
				fmt.Println("Error: admin assign_slot_range requires <startSlot> <endSlot> <assignedNodeID>.")
				return
			}
			performAdminRequest("assign_slot_range", "POST", "", "", args[2], args[3], args[4])
		case "get_node_for_key":
			if len(args) < 3 {
				fmt.Println("Error: admin get_node_for_key requires <key>.")
				return
			}
			performAdminRequest("get_node_for_key", "GET", args[2], "", "", "", "")
		case "set_metadata":
			if len(args) < 4 {
				fmt.Println("Error: admin set_metadata requires <key> <value>.")
				return
			}
			performAdminRequest("metadata", "POST", args[2], strings.Join(args[3:], " "), "", "", "")
		default:
			fmt.Println("Error: Unknown admin sub-command. Supported: assign_slot_range, get_node_for_key, set_metadata.")
			return
		}
	case "help": // Added help command
		fmt.Println("Commands:")
		fmt.Println("  put <key> <value>")
		fmt.Println("  get <key>")
		fmt.Println("  delete <key>")
		fmt.Println("  admin assign_slot_range <startSlot> <endSlot> <assignedNodeID>")
		fmt.Println("  admin get_node_for_key <key>")
		fmt.Println("  admin set_metadata <key> <value>")
		fmt.Println("  status")
		fmt.Println("  help")
		fmt.Println("  exit / quit")
	case "exit", "quit": // Handle exit/quit in interactive mode
		fmt.Println("Exiting GojoDB CLI.")
		os.Exit(0)
	default:
		fmt.Println("Error: Unknown command. Type 'help' for a list of commands.")
	}
}

func main() {
	log.SetFlags(0) // No flags for simple CLI output

	args := os.Args[1:]

	if len(args) == 0 {
		// Enter interactive mode
		fmt.Println("GojoDB CLI (interactive mode). Type 'help' for commands, 'exit' or 'quit' to leave.")
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("gojodb> ")
			input, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					fmt.Println("\nExiting GojoDB CLI.")
					os.Exit(0)
				}
				fmt.Printf("Error reading input: %v\n", err)
				continue
			}

			line := strings.TrimSpace(input)
			if line == "" {
				continue
			}

			// Split the line into arguments, handling quoted strings if necessary (basic split for now)
			cmdArgs := strings.Fields(line)

			processCommand(cmdArgs)
		}
	} else {
		// Process command from command-line arguments
		processCommand(args)
	}
}
