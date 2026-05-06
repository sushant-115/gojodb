// Package request_validation provides request validators for GojoDB's gRPC API.
package request_validation

import (
	pb "github.com/sushant-115/gojodb/api/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxKeyLength   = 1024
	maxValueLength = 16384
	maxBulkItems   = 10000
	maxRangeLimit  = 10000
)

func ValidatePutRequest(r *pb.PutRequest) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if len(r.Key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(r.Key) > maxKeyLength {
		return status.Errorf(codes.InvalidArgument, "key length %d exceeds maximum %d", len(r.Key), maxKeyLength)
	}
	if len(r.Value) > maxValueLength {
		return status.Errorf(codes.InvalidArgument, "value length %d exceeds maximum %d", len(r.Value), maxValueLength)
	}
	return nil
}

func ValidateGetRequest(r *pb.GetRequest) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if len(r.Key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(r.Key) > maxKeyLength {
		return status.Errorf(codes.InvalidArgument, "key length %d exceeds maximum %d", len(r.Key), maxKeyLength)
	}
	return nil
}

func ValidateDeleteRequest(r *pb.DeleteRequest) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if len(r.Key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(r.Key) > maxKeyLength {
		return status.Errorf(codes.InvalidArgument, "key length %d exceeds maximum %d", len(r.Key), maxKeyLength)
	}
	return nil
}

func ValidateGetRangeRequest(r *pb.GetRangeRequest) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if len(r.StartKey) == 0 {
		return status.Error(codes.InvalidArgument, "start_key must not be empty")
	}
	if len(r.StartKey) > maxKeyLength || len(r.EndKey) > maxKeyLength {
		return status.Error(codes.InvalidArgument, "key exceeds maximum length")
	}
	if r.Limit > maxRangeLimit {
		return status.Errorf(codes.InvalidArgument, "limit %d exceeds maximum %d", r.Limit, maxRangeLimit)
	}
	return nil
}

func ValidateTextSearchRequest(r *pb.TextSearchRequest) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if r.Query == "" {
		return status.Error(codes.InvalidArgument, "query must not be empty")
	}
	return nil
}

func ValidateBulkPutRequest(r *pb.BulkPutRequest) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if len(r.Pairs) == 0 {
		return status.Error(codes.InvalidArgument, "pairs must not be empty")
	}
	if len(r.Pairs) > maxBulkItems {
		return status.Errorf(codes.InvalidArgument, "bulk size %d exceeds maximum %d", len(r.Pairs), maxBulkItems)
	}
	for i, p := range r.Pairs {
		if len(p.Key) == 0 {
			return status.Errorf(codes.InvalidArgument, "pair[%d]: key must not be empty", i)
		}
		if len(p.Key) > maxKeyLength {
			return status.Errorf(codes.InvalidArgument, "pair[%d]: key length exceeds maximum", i)
		}
		if len(p.Value) > maxValueLength {
			return status.Errorf(codes.InvalidArgument, "pair[%d]: value length exceeds maximum", i)
		}
	}
	return nil
}

func ValidateBulkDeleteRequest(r *pb.BulkDeleteRequest) error {
	if r == nil {
		return status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if len(r.Keys) == 0 {
		return status.Error(codes.InvalidArgument, "keys must not be empty")
	}
	if len(r.Keys) > maxBulkItems {
		return status.Errorf(codes.InvalidArgument, "bulk size %d exceeds maximum %d", len(r.Keys), maxBulkItems)
	}
	for i, k := range r.Keys {
		if len(k) == 0 {
			return status.Errorf(codes.InvalidArgument, "key[%d] must not be empty", i)
		}
		if len(k) > maxKeyLength {
			return status.Errorf(codes.InvalidArgument, "key[%d] length exceeds maximum", i)
		}
	}
	return nil
}
