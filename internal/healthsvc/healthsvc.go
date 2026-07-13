// Package healthsvc implements the self-check half of HealthService. Every
// gRPC-serving component registers it; ClusterCheck stays Unimplemented here
// and is overridden only by the observer.
package healthsvc

import (
	"context"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
)

// Server answers Check for the process it runs in.
type Server struct {
	ironworkv1.UnimplementedHealthServiceServer

	component string
	instance  string
}

// New returns a health server identifying itself with the given component
// kind and instance name.
func New(component, instance string) *Server {
	return &Server{component: component, instance: instance}
}

// Check reports the health of this process. Reaching the handler at all means
// the gRPC server (and the mTLS handshake) works, which is Phase 0's
// definition of healthy.
func (s *Server) Check(context.Context, *ironworkv1.CheckRequest) (*ironworkv1.CheckResponse, error) {
	return &ironworkv1.CheckResponse{
		Component: s.component,
		Instance:  s.instance,
		Status:    ironworkv1.ServingStatus_SERVING_STATUS_SERVING,
	}, nil
}
