# Use a Go base image for building the application
FROM golang:1.24.3-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to download dependencies
# This step is optimized for Docker caching: if dependencies don't change,
# this layer won't be rebuilt.
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project source code into the container
# This assumes the Dockerfile is in a subdirectory (e.g., cmd/gojodb_controller)
# and needs access to the root go.mod and other packages.
COPY . .

# Declare a build argument for the component's main package path
ARG COMPONENT_PATH

# Build the Go application binary
# The output binary will be named 'app' and placed in /usr/local/bin/
RUN go build -o /usr/local/bin/app ${COMPONENT_PATH}

# Use a minimal Alpine Linux image for the final stage
# This keeps the final Docker image small
FROM alpine:latest

# Set the working directory for the final image
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/local/bin/app .

# Expose the ports that the application will listen on.
# You will need to uncomment and adjust these lines based on the specific component:
# For cmd/gojodb_controller:
# EXPOSE 8080
# EXPOSE 8081
# EXPOSE 8086
# For cmd/gojodb_server:
EXPOSE 9090
EXPOSE 9117
EXPOSE 9120
# For api/basic:
# EXPOSE 8082
# For api/graphql_service:
# EXPOSE 8083

# Define the command to run the application when the container starts
CMD ["/app/app"]