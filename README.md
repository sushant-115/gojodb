# gojodb
A universal database, which will allow resource management of operation like reads &amp; writes. everything in this DB will be horizontally scalable

## Goals
- Resource management for operations like read queries, write queries
- Resource management for users
- High availability
- Sharding
- Persistent
- Security
- Encryption
- Support GraphQL
- Archival
- Restore
- Support for read from S3

## High level design
- Hot storage in a shared file system like AWS EFS & GCP FileStore
- Cold storage in S3 or Google cloud storage
- Controller
- API microservices for indexed reads/quick reads
- API microservices for indexed updates/quick writes
- API microservices for bulk writes
- API microservices for aggregation jobs from Hot storage and cold storage

