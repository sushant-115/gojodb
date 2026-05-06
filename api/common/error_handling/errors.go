// Package error_handling provides standardised gRPC error construction helpers
// and a recovery interceptor for GojoDB's API services.
package error_handling

import (
	"context"
	"fmt"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NotFound(format string, args ...interface{}) error {
	return status.Errorf(codes.NotFound, format, args...)
}

func InvalidArgument(format string, args ...interface{}) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

func Internal(format string, args ...interface{}) error {
	return status.Errorf(codes.Internal, format, args...)
}

func AlreadyExists(format string, args ...interface{}) error {
	return status.Errorf(codes.AlreadyExists, format, args...)
}

func Unavailable(format string, args ...interface{}) error {
	return status.Errorf(codes.Unavailable, format, args...)
}

func ResourceExhausted(format string, args ...interface{}) error {
	return status.Errorf(codes.ResourceExhausted, format, args...)
}

func ToGRPC(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func AnnotateError(err error, prefix string) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return status.Errorf(st.Code(), "%s: %s", prefix, st.Message())
}

func RecoveryUnaryInterceptor(ctx context.Context, req interface{},
	_ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			err = Internal("internal server panic: %v", r)
		}
	}()
	return handler(ctx, req)
}

func RecoveryStreamInterceptor(srv interface{}, ss grpc.ServerStream,
	_ *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			err = Internal("internal server panic: %v", r)
		}
	}()
	return handler(srv, ss)
}
