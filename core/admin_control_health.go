package core

import (
	"net/http"
	"time"

	"github.com/marcmoiagese/CercaGenealogica/db"
)

type adminControlHealthDB struct {
	OK        bool   `json:"ok"`
	LatencyMs int64  `json:"latency_ms"`
	State     string `json:"state"`
	Error     string `json:"error,omitempty"`
}

type adminControlHealthJobs struct {
	Queued  int    `json:"queued"`
	Running int    `json:"running"`
	Failed  int    `json:"failed"`
	State   string `json:"state"`
}

type adminControlHealthResponse struct {
	Status      string                 `json:"status"`
	DB          adminControlHealthDB   `json:"db"`
	Jobs        adminControlHealthJobs `json:"jobs"`
	GeneratedAt string                 `json:"generated_at"`
}

type adminControlMetricsResponse struct {
	Users7d     int                     `json:"users_7d"`
	Imports24h  db.AdminImportRunSummary `json:"imports_24h"`
	JobsTotal   int                     `json:"jobs_total"`
	JobsFailed  int                     `json:"jobs_failed"`
	GeneratedAt string                  `json:"generated_at"`
}

func (a *App) AdminControlHealthAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}

	resp := adminControlHealthResponse{
		Status:      "ok",
		GeneratedAt: time.Now().Format(time.RFC3339),
	}

	start := time.Now()
	if _, err := a.DB.ListPlatformSettings(); err != nil {
		resp.DB.OK = false
		resp.DB.Error = err.Error()
		resp.DB.State = "crit"
	} else {
		resp.DB.OK = true
		resp.DB.LatencyMs = time.Since(start).Milliseconds()
		resp.DB.State = healthStateFromLatency(resp.DB.LatencyMs, 200, 800)
	}

	queued := a.safeCountAdminJobs(db.AdminJobFilter{Status: adminJobStatusQueued})
	running := a.safeCountAdminJobs(db.AdminJobFilter{Status: adminJobStatusRunning})
	failed := a.safeCountAdminJobs(db.AdminJobFilter{Status: adminJobStatusError})
	resp.Jobs = adminControlHealthJobs{
		Queued:  queued,
		Running: running,
		Failed:  failed,
		State:   healthStateForJobs(queued, running, failed),
	}

	resp.Status = mergeHealthStates(resp.DB.State, resp.Jobs.State)
	writeJSON(w, resp)
}

func (a *App) AdminControlMetricsAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if _, _, ok := a.requirePermission(w, r, permAdmin); !ok {
		return
	}

	now := time.Now()
	users7d, _ := a.DB.CountUsersSince(now.AddDate(0, 0, -7))
	imports24h, _ := a.DB.CountAdminImportRunsSince(now.Add(-24 * time.Hour))
	jobsTotal := a.safeCountAdminJobs(db.AdminJobFilter{})
	jobsFailed := a.safeCountAdminJobs(db.AdminJobFilter{Status: adminJobStatusError})

	writeJSON(w, adminControlMetricsResponse{
		Users7d:     users7d,
		Imports24h:  imports24h,
		JobsTotal:   jobsTotal,
		JobsFailed:  jobsFailed,
		GeneratedAt: now.Format(time.RFC3339),
	})
}

func healthStateFromLatency(latencyMs int64, warnMs int64, critMs int64) string {
	if latencyMs >= critMs {
		return "crit"
	}
	if latencyMs >= warnMs {
		return "warn"
	}
	return "ok"
}

func healthStateForJobs(queued, running, failed int) string {
	if failed > 0 {
		return "crit"
	}
	if queued > 0 || running > 0 {
		return "warn"
	}
	return "ok"
}

func mergeHealthStates(states ...string) string {
	state := "ok"
	for _, s := range states {
		if s == "crit" {
			return "crit"
		}
		if s == "warn" {
			state = "warn"
		}
	}
	return state
}

func (a *App) safeCountAdminJobs(filter db.AdminJobFilter) int {
	if a == nil || a.DB == nil {
		return 0
	}
	val, err := a.DB.CountAdminJobs(filter)
	if err != nil {
		return 0
	}
	return val
}
