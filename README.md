# **GojoDB: A Distributed Key-Value Store with Inverted Indexing and Replication**

GojoDB is a distributed key-value database designed for high availability, fault tolerance, and efficient data retrieval, particularly for text-based content. It features built-in log replication, tiered storage management, and an inverted indexing system for fast full-text search capabilities.

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
go install ./...
```
This will install the gojodb_cli, gojodb_standalone_server, gojodb_controller, gojodb_cluster_server, and various API server binaries into your Go GOPATH/bin directory.

### **Running Standalone Server**

For a quick start, you can run a single standalone DB server:
```
gojodb_standalone_server
```
By default, it will listen on localhost:9090.

### **Running a Cluster**

To run a full GojoDB cluster, you'll typically start the controller, followed by multiple DB servers, and then the API servers.

**1. Start the Controller (e.g., on port 8000):**
```
gojodb_controller --port 8000
```
**2. Start DB Servers (e.g., two instances, one primary, one replica):**

* **Primary DB Server (e.g., on port 9090, connecting to controller on 8000):**  
```
  gojodb_cluster_server --port 9090 --controller-addr localhost:8000 --is-primary true
```
* **Replica DB Server (e.g., on port 9091, connecting to controller on 8000):**  
```
  gojodb_cluster_server --port 9091 --controller-addr localhost:8000 --is-primary false --primary-addr localhost:9090
```
  *Note: In a real distributed setup, localhost would be replaced with actual IP addresses or hostnames.*

**3. Start API Servers:**

* **Basic API Server (e.g., on port 8080, connecting to DB server on 9090):**  
```
  gojodb_basic_api --port 8080 --db-server-addr localhost:9090
```
* **GraphQL API Server (e.g., on port 8081, connecting to DB server on 9090):**  
```
  gojodb_graphql_service --port 8081 --db-server-addr localhost:9090
```
  *You can start other API services similarly.*

## **Usage**

You can interact with GojoDB using the CLI or by making HTTP requests to the API servers.

**Using the CLI:**

# Assuming gojodb_cli is in your PATH and connected to an API server (e.g., localhost:8080)  
```
gojodb_cli put mykey "Hello GojoDB"  
gojodb_cli get mykey  
gojodb_cli search "GojoDB"
```
**Example HTTP Request (Basic API):**

# PUT request  
```
curl -X PUT -H "Content-Type: application/json" -d '{"key": "mykey", "value": "Hello World"}'  
```
# GET request  
```
curl http://localhost:8080/get?key=mykey
```
**GraphQL Example:**

The GraphQL API typically runs on http://localhost:8081/query (or /graphql depending on configuration). You can use a GraphQL client or curl to send queries.
```
curl -X POST  -H "Content-Type: application/json"  -d '{ "query": "{ getValue(key: "mykey") }" }'  http://localhost:8081/query
```
## **Contributing**

Contributions are welcome! Please feel free to open issues or submit pull requests.

## **License**

This project is licensed under the MIT License - see the LICENSE file for details (if applicable, otherwise state "No specific license defined yet").
