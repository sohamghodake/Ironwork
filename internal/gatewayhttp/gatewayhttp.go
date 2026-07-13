// Package gatewayhttp serves the gateway's REST API. In Phase 0 that is the
// cluster health endpoint (backed by the observer's ClusterCheck over mTLS
// gRPC) plus a local liveness probe.
package gatewayhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
)

// clusterCheckTimeout bounds the observer round trip; the observer's own
// per-target timeout is shorter, so a healthy observer always answers in time.
const clusterCheckTimeout = 10 * time.Second

// healthResponse is the JSON shape of GET /health.
type healthResponse struct {
	Status     string            `json:"status"`
	CheckedAt  time.Time         `json:"checked_at"`
	Components []componentHealth `json:"components,omitempty"`
	Error      string            `json:"error,omitempty"`
}

type componentHealth struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// NewRouter builds the gateway's HTTP handler. health is a client for the
// observer's HealthService.
func NewRouter(health ironworkv1.HealthServiceClient, log zerolog.Logger) http.Handler {
	r := chi.NewRouter()

	// Liveness of the gateway process itself (compose healthcheck).
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// Aggregated cluster health: gateway -> observer -> every component + DB.
	r.Get("/health", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), clusterCheckTimeout)
		defer cancel()

		resp, err := health.ClusterCheck(ctx, &ironworkv1.ClusterCheckRequest{})
		if err != nil {
			log.Error().Err(err).Msg("observer cluster check failed")
			writeJSON(w, http.StatusBadGateway, healthResponse{
				Status:    "UNREACHABLE",
				CheckedAt: time.Now().UTC(),
				Error:     err.Error(),
			})
			return
		}

		body := healthResponse{
			Status:    statusString(resp.Status),
			CheckedAt: resp.CheckedAt.AsTime(),
		}
		for _, c := range resp.Components {
			body.Components = append(body.Components, componentHealth{
				Name:    c.Name,
				Status:  statusString(c.Status),
				Message: c.Message,
			})
		}

		code := http.StatusOK
		if resp.Status != ironworkv1.ServingStatus_SERVING_STATUS_SERVING {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, body)
	})

	return r
}

func statusString(s ironworkv1.ServingStatus) string {
	switch s {
	case ironworkv1.ServingStatus_SERVING_STATUS_SERVING:
		return "SERVING"
	case ironworkv1.ServingStatus_SERVING_STATUS_NOT_SERVING:
		return "NOT_SERVING"
	default:
		return "UNKNOWN"
	}
}

func writeJSON(w http.ResponseWriter, code int, body healthResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
