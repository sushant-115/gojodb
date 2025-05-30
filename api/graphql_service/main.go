package main

import (
	"log"
	"net/http"
	"os"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"

	// --- FIX: Import from the generated package ---
	generated "github.com/sushant-115/gojodb/api/graphql_service/dataloaders/graph" // Import the generated code
	// Still need this for your Resolver struct
	// --- END FIX ---
)

const (
	defaultPort = "8091" // Default port for the GraphQL server
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile) // Include file and line number in logs for debugging

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	// Create a new GraphQL server handler
	// This uses the generated `ExecutableSchema` which wires up your resolvers to the schema.
	// --- FIX: Use generated.NewExecutableSchema and generated.Config ---
	srv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: &generated.Resolver{}}))
	// --- END FIX ---

	// Set up GraphQL Playground (a web-based IDE for GraphQL queries)
	http.Handle("/", playground.Handler("GraphQL Playground", "/query"))
	// Set up the GraphQL query endpoint
	http.Handle("/query", srv)

	log.Printf("INFO: GojoDB GraphQL server listening on :%s", port)
	log.Printf("INFO: Connect to GraphQL Playground at http://localhost:%s/", port)
	log.Printf("INFO: GraphQL endpoint is http://localhost:%s/query", port)

	log.Fatal(http.ListenAndServe(":"+port, nil))
}
