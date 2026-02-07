package core

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type nivellRebuildJob struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Total     int       `json:"total"`
	Processed int       `json:"processed"`
	Done      bool      `json:"done"`
	Error     string    `json:"error,omitempty"`
	Logs      []string  `json:"logs,omitempty"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type nivellRebuildStore struct {
	mu   sync.Mutex
	jobs map[string]*nivellRebuildJob
}

const nivellRebuildLogLimit = 200

var nivellRebuildSeq uint64

func newNivellRebuildStore() *nivellRebuildStore {
	return &nivellRebuildStore{jobs: map[string]*nivellRebuildJob{}}
}

func (a *App) nivellRebuildStore() *nivellRebuildStore {
	if a == nil {
		return newNivellRebuildStore()
	}
	if a.nivellRebuildJobs == nil {
		a.nivellRebuildJobs = newNivellRebuildStore()
	}
	return a.nivellRebuildJobs
}

func nextNivellRebuildID() string {
	seq := atomic.AddUint64(&nivellRebuildSeq, 1)
	return fmt.Sprintf("nivell-rebuild-%d-%d", time.Now().UnixNano(), seq)
}

func (s *nivellRebuildStore) newJob(kind string, total int) *nivellRebuildJob {
	job := &nivellRebuildJob{
		ID:        nextNivellRebuildID(),
		Kind:      kind,
		Total:     total,
		Processed: 0,
		Done:      false,
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
		Logs:      []string{},
	}
	s.mu.Lock()
	s.jobs[job.ID] = job
	s.mu.Unlock()
	return job
}

func (s *nivellRebuildStore) snapshot(id string) (*nivellRebuildJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	copyJob := *job
	copyJob.Logs = append([]string{}, job.Logs...)
	return &copyJob, true
}

func (s *nivellRebuildStore) appendLog(id string, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return
	}
	job.Logs = append(job.Logs, message)
	if len(job.Logs) > nivellRebuildLogLimit {
		job.Logs = job.Logs[len(job.Logs)-nivellRebuildLogLimit:]
	}
	job.UpdatedAt = time.Now()
}

func (s *nivellRebuildStore) setProcessed(id string, processed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return
	}
	job.Processed = processed
	job.UpdatedAt = time.Now()
}

func (s *nivellRebuildStore) setTotal(id string, total int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return
	}
	job.Total = total
	job.UpdatedAt = time.Now()
}

func (s *nivellRebuildStore) finish(id string, err error) {
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
