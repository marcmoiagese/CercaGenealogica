package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestModeracioSummaryCacheHitConsistency(t *testing.T) {
	app, database := newTestAppForLogin(t, "test_f30_10_summary_cache.sqlite3")

	admin := createTestUser(t, database, "admin_summary_cache")
	assignPolicyByName(t, database, admin.ID, "admin")
	session := createSessionCookie(t, database, admin.ID, "sess_summary_cache")

	createPendingPersona(t, database, admin.ID, "Persona Cache")

	callSummary := func() (int, string, map[string]int) {
		req := httptest.NewRequest(http.MethodGet, "/api/admin/control/moderacio/summary", nil)
		req.AddCookie(session)
		rr := httptest.NewRecorder()
		app.AdminControlModeracioSummaryAPI(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("summary esperava 200, got %d", rr.Code)
		}
		var payload struct {
			Ok           bool   `json:"ok"`
			SummaryScope string `json:"summary_scope"`
			Summary      struct {
				Total  int `json:"total"`
				ByType []struct {
					Type  string `json:"type"`
					Total int    `json:"total"`
				} `json:"by_type"`
			} `json:"summary"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
			t.Fatalf("summary response invalid: %v", err)
		}
		if !payload.Ok {
			t.Fatalf("summary ok esperat true")
		}
		got := map[string]int{}
		for _, item := range payload.Summary.ByType {
			got[item.Type] = item.Total
		}
		return payload.Summary.Total, payload.SummaryScope, got
	}

	total1, scope1, byType1 := callSummary()
	total2, scope2, byType2 := callSummary()
	if scope1 != "global" || scope2 != "global" {
		t.Fatalf("summary_scope esperat global, got %s / %s", scope1, scope2)
	}
	if total1 != total2 {
		t.Fatalf("summary total inconsistent: %d vs %d", total1, total2)
	}
	if byType1["persona"] != byType2["persona"] {
		t.Fatalf("summary persona inconsistent: %d vs %d", byType1["persona"], byType2["persona"])
	}
	if byType1["persona"] != 1 {
		t.Fatalf("summary persona esperat 1, got %d", byType1["persona"])
	}
}
