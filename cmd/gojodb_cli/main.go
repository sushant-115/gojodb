package main

import (
	// Added for interactive mode
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"
	fsm "github.com/sushant-115/gojodb/core/replication/raft_consensus"
	// For advanced CLI features like cursor movement and rich history,
	// consider using a library like github.com/chzyer/readline or github.com/c-bata/go-prompt.
	// These require external dependencies and cannot be directly integrated here.
)

const (
	apiServiceURL         = "http://localhost:8082/api/data"        // Target URL for the API Service's data endpoint
	adminServiceURL       = "http://localhost:8082/admin/"          // Base URL for admin endpoints
	statusServiceURL      = "http://localhost:8082/status"          // URL for cluster status
	queryServiceURL       = "http://localhost:8082/api/query"       // URL for range queries
	transactionServiceURL = "http://localhost:8082/api/transaction" // URL for 2PC transactions
	clientTimeout         = 10 * time.Second
	adminAPIKey           = "GOJODB_ADMIN_KEY" // Must match the key in api_service/main.go
)

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
	Status  string          `json:"status"`            // OK, ERROR, NOT_FOUND, REDIRECT
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

// cliHistory stores the command history.
var cliHistory []string

const maxHistorySize = 100 // Maximum number of commands to store in history

// addCommandToHistory adds a command to the history.
func addCommandToHistory(cmd string) {
	if cmd == "" || strings.HasPrefix(cmd, "!") || cmd == "history" {
		return // Don't add empty or history commands to history
	}
	cliHistory = append(cliHistory, cmd)
	if len(cliHistory) > maxHistorySize {
		cliHistory = cliHistory[1:] // Trim oldest command if history exceeds limit
	}
}

// getCommandFromHistory retrieves a command from history by index.
func getCommandFromHistory(index int) (string, error) {
	if index < 1 || index > len(cliHistory) {
		return "", fmt.Errorf("invalid history index: %d. History size: %d", index, len(cliHistory))
	}
	return cliHistory[index-1], nil // History is 1-indexed for user
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

// performQueryRequest sends a JSON request to the API Service's /api/query endpoint.
func performQueryRequest(cmd, startKey, endKey string) {
	reqBody := APIRequest{
		Command:  cmd,
		StartKey: startKey,
		EndKey:   endKey,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		log.Printf("Error marshalling query request: %v", err)
		return
	}

	httpClient := http.Client{Timeout: clientTimeout}
	resp, err := httpClient.Post(queryServiceURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Printf("Error sending query request to API Service: %v", err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading query response body: %v. Raw response: %s", err, string(bodyBytes))
		return
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		log.Printf("Error unmarshalling query response: %v. Raw response: %s", err, string(bodyBytes))
		fmt.Printf("Raw API Service Query Response: %s\n", string(bodyBytes)) // Show raw if unmarshal fails
		return
	}

	if apiResp.Status == "OK" {
		if apiResp.Data != nil {
			fmt.Printf("Response: Status=%s, Data=%s\n", apiResp.Status, string(apiResp.Data))
		} else {
			fmt.Printf("Response: Status=%s, Message='%s'\n", apiResp.Status, apiResp.Message)
		}
	} else {
		fmt.Printf("Response: Status=%s, Message='%s'\n", apiResp.Status, apiResp.Message)
	}
}

// performTransactionRequest sends a JSON request to the API Service's /api/transaction endpoint.
func performTransactionRequest(operations []TransactionOperation) {
	txReq := TransactionRequest{
		Operations: operations,
	}

	jsonBody, err := json.Marshal(txReq)
	if err != nil {
		log.Printf("Error marshalling transaction request: %v", err)
		return
	}

	httpClient := http.Client{Timeout: clientTimeout}
	resp, err := httpClient.Post(transactionServiceURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Printf("Error sending transaction request to API Service: %v", err)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading transaction response body: %v. Raw response: %s", err, string(bodyBytes))
		return
	}

	var apiResp APIResponse
	if err := json.Unmarshal(bodyBytes, &apiResp); err != nil {
		log.Printf("Error unmarshalling transaction response: %v. Raw response: %s", err, string(bodyBytes))
		fmt.Printf("Raw API Service Transaction Response: %s\n", string(bodyBytes)) // Show raw if unmarshal fails
		return
	}

	fmt.Printf("Transaction Response: Status=%s, Message='%s'\n", apiResp.Status, apiResp.Message)
}

// performAdminRequest sends an authenticated JSON request to an admin endpoint on the API Service.
func performAdminRequest(subPath, method, key, value, startSlot, endSlot, assignedNodeID, replicaNodes string) {
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
	if replicaNodes != "" {
		queryParams = append(queryParams, fmt.Sprintf("replicaNodes=%s", replicaNodes))
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
	case "get_range":
		if len(args) < 3 {
			fmt.Println("Error: get_range command requires a start key and an end key.")
			return
		}
		performQueryRequest("GET_RANGE", args[1], args[2])
	case "txn": // Distributed transaction commands
		if len(args) < 2 {
			fmt.Println("Error: txn command requires a sub-command (e.g., put, delete).")
			return
		}
		txnSubCommand := strings.ToLower(args[1])
		var ops []TransactionOperation
		switch txnSubCommand {
		case "put":
			if len(args) < 4 {
				fmt.Println("Error: txn put requires <key> <value>.")
				return
			}
			ops = append(ops, TransactionOperation{Command: "PUT", Key: args[2], Value: strings.Join(args[3:], " ")})
		case "delete":
			if len(args) < 3 {
				fmt.Println("Error: txn delete requires <key>.")
				return
			}
			ops = append(ops, TransactionOperation{Command: "DELETE", Key: args[2]})
		default:
			fmt.Println("Error: Unsupported txn sub-command. Use 'put' or 'delete'.")
			return
		}
		performTransactionRequest(ops)

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
			replicaNodes := ""
			if len(args) >= 6 {
				replicaNodes = args[5] // Optional: comma-separated replica node IDs
			}
			performAdminRequest("assign_slot_range", "POST", "", "", args[2], args[3], args[4], replicaNodes)
		case "get_node_for_key":
			if len(args) < 3 {
				fmt.Println("Error: admin get_node_for_key requires <key>.")
				return
			}
			performAdminRequest("get_node_for_key", "GET", args[2], "", "", "", "", "")
		case "set_metadata":
			if len(args) < 4 {
				fmt.Println("Error: admin set_metadata requires <key> <value>.")
				return
			}
			performAdminRequest("metadata", "POST", args[2], strings.Join(args[3:], " "), "", "", "", "")
		case "set_primary_replica":
			if len(args) < 5 {
				fmt.Println("Error: admin set_primary_replica requires <rangeID> <primaryNodeID> <replicaNodeIDs>.")
				return
			}
			rangeID := args[2]
			primaryNodeID := args[3]
			replicaNodeIDs := args[4] // Comma-separated
			// Construct a dummy SlotRangeInfo to marshal for the value
			slotInfo := fsm.SlotRangeInfo{
				RangeID:        rangeID,
				PrimaryNodeID:  primaryNodeID,
				ReplicaNodeIDs: strings.Split(replicaNodeIDs, ","),
				Status:         "active", // Assume active status
			}
			slotInfoBytes, err := json.Marshal(slotInfo)
			if err != nil {
				fmt.Printf("Error marshalling slot info: %v\n", err)
				return
			}
			performAdminRequest("set_primary_replica", "POST", rangeID, string(slotInfoBytes), "", "", "", "")
		default:
			fmt.Println("Error: Unknown admin sub-command. Supported: assign_slot_range, get_node_for_key, set_metadata, set_primary_replica.")
			return
		}
	case "history":
		if len(cliHistory) == 0 {
			fmt.Println("No history available.")
			return
		}
		fmt.Println("Command History:")
		for i, cmd := range cliHistory {
			fmt.Printf("  %d: %s\n", i+1, cmd)
		}
	case "!": // Re-execute command from history
		if len(args) < 2 {
			fmt.Println("Error: ! requires a history index (e.g., !1).")
			return
		}
		idx, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("Error: Invalid history index. Must be a number.")
			return
		}
		cmdToExecute, err := getCommandFromHistory(idx)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Executing: %s\n", cmdToExecute)
		// Recursively process the command
		processCommand(strings.Fields(cmdToExecute))
	case "help": // Added help command
		fmt.Println("GojoDB CLI Commands:")
		fmt.Println("  Data Operations:")
		fmt.Println("    put <key> <value>            - Inserts or updates a key-value pair.")
		fmt.Println("    get <key>                    - Retrieves the value for a key.")
		fmt.Println("    delete <key>                 - Deletes a key-value pair.")
		fmt.Println("    get_range <start_key> <end_key> - Retrieves key-value pairs within a range.")
		fmt.Println("  Transaction Operations (2PC):")
		fmt.Println("    txn put <key> <value>        - Performs a transactional PUT.")
		fmt.Println("    txn delete <key>             - Performs a transactional DELETE.")
		fmt.Println("  Admin Operations:")
		fmt.Println("    admin assign_slot_range <startSlot> <endSlot> <assignedNodeID> [replicaNodeIDs] - Assigns a slot range to a primary node with optional replicas.")
		fmt.Println("    admin get_node_for_key <key> - Finds the storage node responsible for a key.")
		fmt.Println("    admin set_metadata <key> <value> - Sets cluster-wide metadata.")
		fmt.Println("    admin set_primary_replica <rangeID> <primaryNodeID> <replicaNodeIDs> - Sets primary/replicas for a slot range.")
		fmt.Println("  Cluster Status:")
		fmt.Println("    status                       - Displays the current cluster status.")
		fmt.Println("  CLI Utilities:")
		fmt.Println("    history                      - Shows recent command history.")
		fmt.Println("    !<index>                     - Re-executes a command from history (e.g., !1).")
		fmt.Println("    help                         - Displays this help message.")
		fmt.Println("    exit / quit                  - Exits the CLI.")
	case "exit", "quit": // Handle exit/quit in interactive mode
		fmt.Println("Exiting GojoDB CLI.")
		os.Exit(0)
	default:
		fmt.Println("Error: Unknown command. Type 'help' for a list of commands.")
	}
}

func main() {
	log.SetFlags(0) // No flags for simple CLI output

	// Cool GojoDB Intro Text
	fmt.Println(`
  ██████╗  ███████╗  ██████╗ ███████╗           ██████╗  ██████╗
  ██╔════╝ ██    ██╔════╝ ██╔██║═╝ ██         ╔════╝ ██╔════╝
  ██║  ███╗██║   ██╗██║   ██╗██║   ██╗           ███╗██║  ███╗
  ██║   ██║██║   ██║██║   ██║██║   ██║          ██║   ██║██║   ██║
  ╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝
   ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝
    GojoDB CLI - Your data, unbound.

    Type 'help' for commands, 'exit' or 'quit' to leave.
	`)

	args := os.Args[1:]

	if len(args) == 0 {
		// Enter interactive mode
		// Interactive mode with line editing
		rl, err := readline.New("gojodb> ")
		if err != nil {
			log.Fatalf("failed to create readline: %v", err)
		}
		defer rl.Close()

		for {
			line, err := rl.Readline()
			if err != nil { // handles Ctrl+D, etc.
				fmt.Println("\nExiting GojoDB CLI.")
				break
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			addCommandToHistory(line)
			cmdArgs := strings.Fields(line)
			processCommand(cmdArgs)
		}
	} else {
		// Process command from command-line arguments
		processCommand(args)
	}
}
