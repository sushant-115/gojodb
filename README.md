# **# **GojoDB - A Universal, Horizontally Scalable Database****

GojoDB is an ambitious project to build a universal database designed for extreme extensibility, horizontal scalability, and robust resource management. It aims to provide high availability and fault tolerance for modern data-intensive applications. It is a distributed key-value database designed for high availability, fault tolerance, and efficient data retrieval, particularly for text-based content. It features built-in log replication, tiered storage management, and an inverted indexing system for fast full-text search capabilities.

Milestone Plan: https://docs.google.com/document/d/1h7EVUI1Cq3jjjEcOZ7-OIjjvjgEIi4mn-BAsKBBYFE4/edit?usp=sharing
High level and low level design: https://docs.google.com/document/d/1kh8X3yKDaBk03Aa7z_Pp-rm8j2Sb-1Z8Owm_eKGecxY/edit?usp=sharing

## **Table of Contents**

* [Features](#bookmark=id.k5zvhpjvts40)  
* [Architecture](#bookmark=id.4jbqgzqjj3t9)  
* [Getting Started](#bookmark=id.umldwf2e0i06)  
  * [Prerequisites](#bookmark=id.kdo370vpj78h)  
  * [Building the Project](#bookmark=id.hynjg25smlah)  
  * [Running Standalone Server](#bookmark=id.7k52i5rge9by)  
  * [Running a Cluster](#bookmark=id.7fug6qbm9ac0)  
* [Usage](#bookmark=id.nrdatfveijaq)  
* [Contributing](#bookmark=id.yyg2cwpezt6l)  
* [License](#bookmark=id.h2mv664i2ufv)

## **Features**

GojoDB provides a robust set of features to handle various data storage and retrieval needs:

* **Inverted Indexing:** Enables lightning-fast full-text search queries across stored values. This is ideal for applications requiring efficient search capabilities.  
* **Log Replication:** Ensures data durability and high availability through a robust log-based replication mechanism. Data changes on the primary node are asynchronously or synchronously replicated to secondary nodes.  
* **Tiered Storage Management:** Optimizes storage costs and performance by intelligently moving data between different storage tiers:  
  * **Hot Storage:** In-memory or fast local disk (e.g., filestore_adapter.go, efs_adapter.go).  
  * **Cold Storage:** Cost-effective, highly durable cloud storage (e.g., AWS S3 via s3_adapter.go, Google Cloud Storage via gcs_adapter.go).  
* **B-Tree Indexing:** Utilizes B-trees for efficient key-value lookups and range queries, managed by a buffer pool and disk manager.  
* **Data Archival & Restore:** Includes mechanisms for archiving old log segments to cold storage and restoring them when needed (archive_job.go, restore_job.go).  
* **Security:** Basic cryptographic utilities (crypto_utils.go) for potential data encryption at rest or in transit.  
* **Multiple API Endpoints:**  
  * **Basic API:** For fundamental put/get operations (api/basic/main.go).  
  * **Bulk Writes API:** Optimized for high-throughput data ingestion (api/bulk_writes_service/main.go).  
  * **Indexed Writes/Reads API:** Specific endpoints for operations leveraging the inverted index (api/indexed_writes_service/main.go, api/indexed_reads_service/main.go).  
  * **Aggregation Service:** For data aggregation queries (api/aggregation_service/main.go).  
  * **GraphQL API:** A flexible GraphQL interface for querying and mutating data (api/graphql_service/server.go).  
* **Command Line Interface (CLI):** A simple CLI tool (cmd/gojodb_cli/main.go) for interacting with the database.

## **Architecture**

GojoDB is designed as a distributed system, composed of several key components:

* **API Servers:** (e.g., api/basic/main.go, api/graphql_service/server.go)  
  * Frontend services that expose various APIs to clients.  
  * They route client requests to the appropriate DB servers.  
* **Controller:** (cmd/gojodb_controller/main.go)  
  * Manages the cluster state using a Raft-based Finite State Machine (FSM).  
  * Responsible for leader election, membership changes, and metadata management.  
* **DB Servers (Storage Nodes):** (cmd/gojodb_cluster_server/main.go, cmd/gojodb_standalone_server/main.go)  
  * The core data storage units.  
  * Each DB server manages its local B-tree index, inverted index, and interacts with the tiered storage manager.  
  * They participate in log replication as either primary or replica nodes.  
* **Replication Manager:** (core/replication/log_replication/replication_manager.go)  
  * Manages the replication of write-ahead logs (WAL) between primary and replica DB servers to ensure data consistency and fault tolerance.  
* **Storage Engine:** (core/storage_engine/tiered_storage_manager.go)  
  * Abstracts away the underlying storage mechanisms (hot, cold).  
  * Handles data movement and retrieval across different tiers.  
* **Client SDK:** (pkg/client_sdk/gojodb_client.go)  
  * A Go client library for programmatic interaction with GojoDB.
```
+----------------+       +----------------+       +----------------+  
|                |       |                |       |                |  
|  Client (CLI/  | ----> |  API Server    | ----> |  Controller    |  
|  SDK/App)      |       |  (Basic,GraphQL)|       |  (Raft FSM)    |  
|                |       |                |       |                |  
+----------------+       +-------+--------+       +-------+--------+  
                                 |                          |  
                                 | Request/Response         | Cluster State  
                                 |                          |  
                                 v                          v  
                       +------------------------------------------------+  
                       |                                                |  
                       |             DB Servers (Storage Nodes)         |  
                       |   +-------------------+   +------------------+ |  
                       |   | DB Server 1       |   | DB Server 2      | |  
                       |   | (Primary/Replica) |   | (Primary/Replica)| |  
                       |   | - B-Tree Index    |   | - B-Tree Index   | |  
                       |   | - Inverted Index  |   | - Inverted Index | |  
                       |   | - Replication Mgr |   | - Replication Mgr| |  
                       |   | - Tiered Storage  |   | - Tiered Storage | |  
                       |   +---------+---------+   +---------+--------+ |  
                       |             |                       |          |  
                       |             | Log Replication       |          |  
                       |             +-----------------------+          |  
                       |                                                |  
                       +------------------------------------------------+  
                                       |  
                                       | Tiered Storage  
                                       v  
                             +--------------------+  
                             | Hot Storage        |  
                             | (Local Disk/EFS)   |  
                             +--------------------+  
                                       |  
                                       v  
                             +--------------------+  
                             | Cold Storage       |  
                             | (S3/GCS)           |  
                             +--------------------+
```
## **Getting Started**

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### **Prerequisites**

* Go (version 1.18 or higher recommended)  
* Git

### **Building the Project**

Clone the repository and build the executables:
```
git clone https://github.com/sushant-115/gojodb.git
cd gojodb 
go mod tidy  
```
This will install the gojodb_cli, gojodb_standalone_server, gojodb_controller, gojodb_cluster_server, and various API server binaries into your Go GOPATH/bin directory.

### **Running a Cluster**

To run a full GojoDB cluster, you'll typically start the controller, followed by multiple DB servers, and then the API servers.

**1. Start the Controller (e.g., on port 8000):**
Note : Run each command in separate terminal and make sure to start the controller 1 first so that other controllers can join the leader
```
cd cmd/gojodb_controller
```
Controller 1
```
NODE_ID=node1 RAFT_BIND_ADDR=localhost:8081 HTTP_ADDR=localhost:8080 HEARTBEAT_LISTEN_PORT=8086 go run main.go
```
Controller 2
```
NODE_ID=node2 RAFT_BIND_ADDR=localhost:8082 HTTP_ADDR=localhost:8083 HEARTBEAT_LISTEN_PORT=8087 JOIN_ADDR=localhost:8080 go run main.go
```
Controller 3
```
NODE_ID=node3 RAFT_BIND_ADDR=localhost:8084 HTTP_ADDR=localhost:8085 JOIN_ADDR=localhost:8080 HEARTBEAT_LISTEN_PORT=8088 go run main.go
```
**2. Start DB Servers (e.g., two instances, one primary, one replica):**

* **Primary DB Server (e.g., on port 9090, connecting to controller on 8000):**
cd ../gojodb_cluster_server
```
  STORAGE_NODE_ID=storage_node_1 STORAGE_NODE_ADDR=localhost:9090 BTREE_REPLICATION_LISTEN_PORT=9117 INVERTED_IDX_REPLICATION_LISTEN_PORT=9120 go run main.go
```
* **Replica DB Server (e.g., on port 9091, connecting to controller on 8000):**  
```
  STORAGE_NODE_ID=storage_node_2 STORAGE_NODE_ADDR=localhost:9091 BTREE_REPLICATION_LISTEN_PORT=9116 INVERTED_IDX_REPLICATION_LISTEN_PORT=9121 go run main.go
```
  *Note: In a real distributed setup, localhost would be replaced with actual IP addresses or hostnames.*

You can launch as many DB server you want
**3. Start API Servers:**

* **Basic API Server (e.g., on port 8080, connecting to DB server on 9090):**  
```
go run api/basic/main.go
```
* **GraphQL API Server (e.g., on port 8081, connecting to DB server on 9090):**  
```
  go run api/graphql_service/server.go
```
  *You can start other API services similarly.*

* **Configure the shard slots :**
* For now we will configure storage_node_1 as primary and storage_node_2 as replica and configure only one slot range 0-1023
```
gojodb> admin assign_slot_range 0 1023 storage_node_1 storage_node_2
Admin Response (Status: 200 OK): Slot range 0-1023 assigned to storage_node_1 successfully.
gojodb> status
Cluster Status (Status: 200 OK):
GojoDB Controller Cluster Status:
  - Controller: node1 (Addr: localhost:8080)
    State: Leader, Leader: 127.0.0.1:8081
  - Controller: node2 (Addr: localhost:8083)
    State: Follower, Leader: 127.0.0.1:8081
  - Controller: node3 (Addr: localhost:8085)
    State: Follower, Leader: 127.0.0.1:8081

Registered Storage Nodes (4):
  - ID: storage_node_1, Addr: localhost:9090, Health: HEALTHY
  - ID: storage_node_2, Addr: localhost:9091, Health: HEALTHY

Slot Assignments (1 ranges):
  - RangeID: 0-1023 (0-1023), Assigned To: storage_node_1, Status: active, Primary: storage_node_1, Replicas: storage_node_2
gojodb>
```
If you have more replica you can configure it by just providing a comma-separated value like storage_node_2,storage_node_3,storage_node_4
## **Usage**

You can interact with GojoDB using the CLI or by making HTTP requests to the API servers.

**Using the CLI:**

# Assuming gojodb_cli is in your PATH and connected to an API server (e.g., localhost:8080)  
```
go run cmd/gojodb_cli/main.go

sushant@Sushants-MacBook-Pro gojodb_cli % go run main.go

  ██████╗  ███████╗  ██████╗ ███████╗           ██████╗  ██████╗
  ██╔════╝ ██    ██╔════╝ ██╔██║═╝ ██         ╔════╝ ██╔════╝
  ██║  ███╗██║   ██╗██║   ██╗██║   ██╗           ███╗██║  ███╗
  ██║   ██║██║   ██║██║   ██║██║   ██║          ██║   ██║██║   ██║
  ╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝
   ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝
    GojoDB CLI - Your data, unbound.

    Type 'help' for commands, 'exit' or 'quit' to leave.
	
gojodb> 
```
Type help to get more details

```gojodb> help
GojoDB CLI Commands:
  Data Operations:
    put <key> <value>            - Inserts or updates a key-value pair.
    get <key>                    - Retrieves the value for a key.
    delete <key>                 - Deletes a key-value pair.
    get_range <start_key> <end_key> - Retrieves key-value pairs within a range.
  Transaction Operations (2PC):
    txn put <key> <value>        - Performs a transactional PUT.
    txn delete <key>             - Performs a transactional DELETE.
  Admin Operations:
    admin assign_slot_range <startSlot> <endSlot> <assignedNodeID> [replicaNodeIDs] - Assigns a slot range to a primary node with optional replicas.
    admin get_node_for_key <key> - Finds the storage node responsible for a key.
    admin set_metadata <key> <value> - Sets cluster-wide metadata.
    admin set_primary_replica <rangeID> <primaryNodeID> <replicaNodeIDs> - Sets primary/replicas for a slot range.
  Cluster Status:
    status                       - Displays the current cluster status.
  CLI Utilities:
    history                      - Shows recent command history.
    !<index>                     - Re-executes a command from history (e.g., !1).
    help                         - Displays this help message.
    exit / quit                  - Exits the CLI.
```
**Example HTTP Request (Basic API):**

```
INFO: API Endpoints:
  - /api/data (POST): { "command": "PUT/GET/DELETE", "key": "...", "value": "..." }
    (Use GOJODB.TEXT(your text) for text indexing in PUT value)
  - /api/transaction (POST): { "operations": [ {"command":"PUT/DELETE", "key":"...", "value":"..."}, ... ] }
  - /api/query (POST): { "command": "GET_RANGE/COUNT_RANGE/SUM_RANGE/MIN_RANGE/MAX_RANGE", "start_key": "...", "end_key": "..." }
  - /api/text_search (POST): { "query": "your search terms" } // NEW: Text search endpoint
  - /admin/assign_slot_range (POST, authenticated): Proxy to Controller Leader
  - /admin/get_node_for_key (GET, authenticated): Proxy to Controller Leader
  - /admin/set_metadata (POST, authenticated): Proxy to Controller Leader
  - /status (GET): Get aggregated status of Controller cluster and Storage Nodes
```
## **Contributing**

Contributions are welcome! Please feel free to open issues or submit pull requests.

## **License**

This project is licensed under the MIT License - see the LICENSE file for details (if applicable, otherwise state "No specific license defined yet").
