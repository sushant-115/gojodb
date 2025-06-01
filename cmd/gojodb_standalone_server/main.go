package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync" // For global B-Tree lock

	btree_core "github.com/sushant-115/gojodb/core/indexing/btree" // Adjust this import path to your actual module path
)

const (
	serverHost     = "localhost"
	serverPort     = "9090"
	dbFilePath     = "data/gojodb.db"
	logDir         = "data/logs"
	archiveDir     = "data/archives"
	logBufferSize  = 4096      // 4KB log buffer
	logSegmentSize = 16 * 1024 // 16KB log segment size for quick rotation demo
	bTreeDegree    = 3         // B-tree order (min degree)
	bufferPoolSize = 10        // Number of pages in buffer pool
	dbPageSize     = 4096      // Database page size
)

// Global database instance and its lock for simple concurrency control
var (
	dbInstance *btree_core.BTree[string, string]
	dbLock     sync.RWMutex // Global lock for the entire B-Tree operations
	logManager *btree_core.LogManager
)

// Request represents a parsed client request.
type Request struct {
	Command string
	Key     string
	Value   string // Only for PUT
}

// Response represents a server's reply to a client request.
type Response struct {
	Status  string // OK, ERROR
	Message string // Details or value for GET
}

// initDatabase initializes or opens the B-tree database and LogManager.
func initDatabase() error {
	var err error

	// Initialize LogManager
	logManager, err = btree_core.NewLogManager(logDir, archiveDir, logBufferSize, logSegmentSize)
	if err != nil {
		return fmt.Errorf("failed to create LogManager: %w", err)
	}

	// Try to open existing database first
	dbInstance, err = btree_core.OpenBTreeFile[string, string](
		dbFilePath,
		btree_core.DefaultKeyOrder[string],
		btree_core.KeyValueSerializer[string, string]{
			SerializeKey:     btree_core.SerializeString,
			DeserializeKey:   btree_core.DeserializeString,
			SerializeValue:   btree_core.SerializeString,
			DeserializeValue: btree_core.DeserializeString,
		},
		bufferPoolSize,
		dbPageSize,
		logManager,
	)

	if err != nil {
		// If file not found, create a new one
		if os.IsNotExist(err) || strings.Contains(err.Error(), "database file not found") { // Check for specific error message
			log.Printf("INFO: Database file %s not found. Creating new database.", dbFilePath)
			dbInstance, err = btree_core.NewBTreeFile[string, string](
				dbFilePath,
				bTreeDegree,
				btree_core.DefaultKeyOrder[string],
				btree_core.KeyValueSerializer[string, string]{
					SerializeKey:     btree_core.SerializeString,
					DeserializeKey:   btree_core.DeserializeString,
					SerializeValue:   btree_core.SerializeString,
					DeserializeValue: btree_core.DeserializeString,
				},
				bufferPoolSize,
				dbPageSize,
				logManager,
			)
			if err != nil {
				return fmt.Errorf("failed to create new BTree file: %w", err)
			}
		} else {
			// Other errors during open are fatal
			return fmt.Errorf("failed to open existing BTree file: %w", err)
		}
	}
	log.Printf("INFO: Database initialized successfully. Root PageID: %d", dbInstance.GetRootPageID())
	return nil
}

// closeDatabase closes the B-tree and LogManager cleanly.
func closeDatabase() {
	log.Println("INFO: Shutting down database...")
	if dbInstance != nil {
		if err := dbInstance.Close(); err != nil {
			log.Printf("ERROR: Failed to close BTree: %v", err)
		}
	}
	if logManager != nil {
		if err := logManager.Close(); err != nil {
			log.Printf("ERROR: Failed to close LogManager: %v", err)
		}
	}
	log.Println("INFO: Database shutdown complete.")
}

// parseRequest parses a raw string command into a Request struct.
func parseRequest(raw string) (Request, error) {
	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return Request{}, fmt.Errorf("empty command")
	}

	command := strings.ToUpper(parts[0])
	req := Request{Command: command}

	switch command {
	case "PUT":
		if len(parts) < 3 {
			return Request{}, fmt.Errorf("PUT requires key and value")
		}
		key := parts[1]
		req.Key = key
		req.Value = strings.Join(parts[2:], " ") // Value can contain spaces
	case "GET", "DELETE":
		if len(parts) < 2 {
			return Request{}, fmt.Errorf("%s requires a key", command)
		}
		key := parts[1]
		req.Key = key
	case "SIZE":
		// No additional arguments needed
	default:
		return Request{}, fmt.Errorf("unknown command: %s", command)
	}
	return req, nil
}

// handleRequest processes a parsed Request and returns a Response.
// This includes basic transaction semantics (each operation is atomic via WAL)
// and global B-Tree locking for concurrency control.
func handleRequest(req Request) Response {
	var resp Response
	var err error

	switch req.Command {
	case "PUT":
		dbLock.Lock() // Acquire write lock for PUT
		err = dbInstance.Insert(req.Key, req.Value, 0)
		dbLock.Unlock() // Release write lock
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("PUT failed: %v", err)}
		} else {
			resp = Response{Status: "OK", Message: "Key-value pair inserted/updated."}
		}
	case "GET":
		dbLock.RLock() // Acquire read lock for GET
		val, found, searchErr := dbInstance.Search(req.Key)
		dbLock.RUnlock() // Release read lock
		if searchErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("GET failed: %v", searchErr)}
		} else if found {
			resp = Response{Status: "OK", Message: val}
		} else {
			resp = Response{Status: "NOT_FOUND", Message: fmt.Sprintf("Key %d not found.", req.Key)}
		}
	case "DELETE":
		dbLock.Lock() // Acquire write lock for DELETE
		err = dbInstance.Delete(req.Key, 0)
		dbLock.Unlock() // Release write lock
		if err != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("DELETE failed: %v", err)}
		} else {
			resp = Response{Status: "OK", Message: "Key deleted."}
		}
	case "SIZE":
		dbLock.RLock() // Acquire read lock for SIZE
		size, sizeErr := dbInstance.GetSize()
		dbLock.RUnlock() // Release read lock
		if sizeErr != nil {
			resp = Response{Status: "ERROR", Message: fmt.Sprintf("SIZE failed: %v", sizeErr)}
		} else {
			resp = Response{Status: "OK", Message: fmt.Sprintf("%d", size)}
		}
	default:
		resp = Response{Status: "ERROR", Message: fmt.Sprintf("Unsupported command: %s", req.Command)}
	}
	return resp
}

// handleConnection manages a single client connection.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("INFO: Client connected: %s", conn.RemoteAddr().String())

	reader := bufio.NewReader(conn)
	for {
		// Read client command, delimited by newline
		netData, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Printf("INFO: Client disconnected: %s", conn.RemoteAddr().String())
			} else {
				log.Printf("ERROR: Error reading from client %s: %v", conn.RemoteAddr().String(), err)
			}
			return
		}

		rawCommand := strings.TrimSpace(netData)
		if rawCommand == "" {
			continue // Ignore empty lines
		}
		log.Printf("DEBUG: Received command from %s: '%s'", conn.RemoteAddr().String(), rawCommand)

		// Parse request
		req, err := parseRequest(rawCommand)
		if err != nil {
			resp := Response{Status: "ERROR", Message: fmt.Sprintf("Invalid request: %v", err)}
			_, writeErr := conn.Write([]byte(fmt.Sprintf("%s %s\n", resp.Status, resp.Message)))
			if writeErr != nil {
				log.Printf("ERROR: Error writing response to client: %v", writeErr)
			}
			continue
		}

		// Process request
		resp := handleRequest(req)

		// Send response back to client
		responseString := fmt.Sprintf("%s %s\n", resp.Status, resp.Message)
		_, writeErr := conn.Write([]byte(responseString))
		if writeErr != nil {
			log.Printf("ERROR: Error writing response to client: %v", writeErr)
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for debugging

	// Initialize database
	if err := initDatabase(); err != nil {
		log.Fatalf("FATAL: Database initialization failed: %v", err)
	}
	defer closeDatabase() // Ensure database is closed on program exit

	// Start TCP listener
	listener, err := net.Listen("tcp", serverHost+":"+serverPort)
	if err != nil {
		log.Fatalf("FATAL: Error listening: %v", err)
	}
	defer listener.Close()

	log.Printf("INFO: GojoDB server listening on %s:%s", serverHost, serverPort)
	log.Println("INFO: Use 'nc localhost 8080' or a custom CLI to connect.")
	log.Println("INFO: Commands: PUT <key> <value>, GET <key>, DELETE <key>, SIZE")

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("ERROR: Error accepting connection: %v", err)
			continue
		}
		// Handle connections in a new goroutine
		go handleConnection(conn)
	}
}
