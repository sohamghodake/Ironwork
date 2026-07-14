// Package protoconv converts between store rows and ironwork.v1 protos.
package protoconv

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	ironworkv1 "github.com/sohamghodake/ironwork/gen/ironwork/v1"
	"github.com/sohamghodake/ironwork/internal/store"
)

var statusToProto = map[string]ironworkv1.JobStatus{
	store.StatusPending:   ironworkv1.JobStatus_JOB_STATUS_PENDING,
	store.StatusScheduled: ironworkv1.JobStatus_JOB_STATUS_SCHEDULED,
	store.StatusRunning:   ironworkv1.JobStatus_JOB_STATUS_RUNNING,
	store.StatusSucceeded: ironworkv1.JobStatus_JOB_STATUS_SUCCEEDED,
	store.StatusFailed:    ironworkv1.JobStatus_JOB_STATUS_FAILED,
}

var statusFromProto = func() map[ironworkv1.JobStatus]string {
	m := make(map[ironworkv1.JobStatus]string, len(statusToProto))
	for s, p := range statusToProto {
		m[p] = s
	}
	return m
}()

// StatusToProto maps a jobs.status column value to the proto enum;
// unknown strings map to UNSPECIFIED.
func StatusToProto(s string) ironworkv1.JobStatus {
	return statusToProto[s]
}

// StatusFromProto maps the proto enum back to the column value;
// UNSPECIFIED maps to "" (no filter).
func StatusFromProto(s ironworkv1.JobStatus) string {
	return statusFromProto[s]
}

// JobToProto converts a store row to the wire Job.
func JobToProto(j *store.Job) *ironworkv1.Job {
	out := &ironworkv1.Job{
		Id:               j.ID,
		Name:             j.Name,
		Payload:          j.Payload,
		Status:           StatusToProto(j.Status),
		AssignedWorkerId: j.AssignedWorker,
		Attempts:         uint32(j.Attempts), //nolint:gosec // attempts is small and non-negative
		Error:            j.Error,
		CreatedAt:        timestamppb.New(j.CreatedAt),
		UpdatedAt:        timestamppb.New(j.UpdatedAt),
	}
	if j.StartedAt != nil {
		out.StartedAt = timestamppb.New(*j.StartedAt)
	}
	if j.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*j.FinishedAt)
	}
	return out
}
