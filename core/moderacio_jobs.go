package core

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type moderacioBulkJob struct {
	ID        string    `json:"id"`
	Action    string    `json:"action"`
	Scope     string    `json:"scope"`
	Type      string    `json:"type"`
	Total     int       `json:"total"`
	Processed int       `json:"processed"`
	Done      bool      `json:"done"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type moderacioBulkStore struct {
	mu   sync.Mutex
	jobs map[string]*moderacioBulkJob
}

var moderacioBulkSeq uint64

func newModeracioBulkStore() *moderacioBulkStore {
	return &moderacioBulkStore{jobs: map[string]*moderacioBulkJob{}}
}

func (a *App) moderacioBulkStore() *moderacioBulkStore {
	if a == nil {
		return newModeracioBulkStore()
	}
	if a.moderacioBulkJobs == nil {
		a.moderacioBulkJobs = newModeracioBulkStore()
	}
	return a.moderacioBulkJobs
}

func nextModeracioBulkID() string {
	seq := atomic.AddUint64(&moderacioBulkSeq, 1)
	return fmt.Sprintf("moderacio-bulk-%d-%d", time.Now().UnixNano(), seq)
}

func (s *moderacioBulkStore) newJob(action, scope, objType string) *moderacioBulkJob {
	job := &moderacioBulkJob{
		ID:        nextModeracioBulkID(),
		Action:    action,
		Scope:     scope,
		Type:      objType,
		Total:     0,
		Processed: 0,
		Done:      false,
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	s.mu.Lock()
	s.jobs[job.ID] = job
	s.mu.Unlock()
	return job
}

func (s *moderacioBulkStore) snapshot(id string) (*moderacioBulkJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	copyJob := *job
	return &copyJob, true
}

func (s *moderacioBulkStore) setTotal(id string, total int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return
	}
	job.Total = total
	job.UpdatedAt = time.Now()
}

func (s *moderacioBulkStore) setProcessed(id string, processed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return
	}
	job.Processed = processed
	job.UpdatedAt = time.Now()
}

func (s *moderacioBulkStore) finish(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return
	}
	if err != nil {
		job.Error = err.Error()
	}
	job.Done = true
	job.UpdatedAt = time.Now()
}
