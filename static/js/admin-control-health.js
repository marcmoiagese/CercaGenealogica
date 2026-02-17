document.addEventListener("DOMContentLoaded", function () {
    var panel = document.querySelector("[data-control-health-api]");
    if (!panel) {
        return;
    }
    var healthApi = panel.getAttribute("data-control-health-api");
    var metricsApi = panel.getAttribute("data-control-metrics-api");
    var refreshBtn = panel.querySelector("[data-control-health-refresh]") || document.querySelector("[data-control-health-refresh]");
    var updatedEl = panel.querySelector("[data-control-health-updated]");
    var errorEl = panel.querySelector("[data-control-health-error]");
    var stateLabels = {
        ok: panel.getAttribute("data-state-ok") || "OK",
        warn: panel.getAttribute("data-state-warn") || "WARN",
        crit: panel.getAttribute("data-state-crit") || "CRIT"
    };
    var updatedLabel = panel.getAttribute("data-updated-label") || "Updated";
    var formatInt = new Intl.NumberFormat();
    var latestUpdated = null;

    var setText = function (selector, value) {
        var el = panel.querySelector(selector);
        if (el) {
            el.textContent = value;
        }
    };

    var setState = function (cardKey, state) {
        var card = panel.querySelector('[data-health-card="' + cardKey + '"]');
        if (!card) {
            return;
        }
        card.dataset.state = state;
        var statusEl = card.querySelector("[data-health-status]");
        if (statusEl) {
            statusEl.textContent = stateLabels[state] || state.toUpperCase();
        }
    };

    var updateTimestamp = function (value) {
        if (!updatedEl || !value) {
            return;
        }
        var ts = new Date(value);
        if (isNaN(ts.getTime())) {
            return;
        }
        if (!latestUpdated || ts > latestUpdated) {
            latestUpdated = ts;
            updatedEl.textContent = updatedLabel + " " + ts.toLocaleTimeString();
        }
    };

    var applyHealth = function (data) {
        var db = data.db || {};
        if (db.ok === false) {
            setText('[data-health="db_latency"]', "-");
        } else if (typeof db.latency_ms === "number") {
            setText('[data-health="db_latency"]', formatInt.format(db.latency_ms) + " ms");
        } else {
            setText('[data-health="db_latency"]', "-");
        }
        setState("db", db.state || (db.ok === false ? "crit" : "ok"));

        var jobs = data.jobs || {};
        setText('[data-health="jobs_queued"]', formatInt.format(Number(jobs.queued || 0)));
        setText('[data-health="jobs_running"]', formatInt.format(Number(jobs.running || 0)));
        setText('[data-health="jobs_failed"]', formatInt.format(Number(jobs.failed || 0)));
        setState("jobs", jobs.state || "ok");

        updateTimestamp(data.generated_at);
    };

    var applyMetrics = function (data) {
        var users7d = Number(data.users_7d || 0);
        setText('[data-metric="users_7d"]', formatInt.format(users7d));

        var imports = data.imports_24h || {};
        var importsOk = Number(imports.ok || 0);
        var importsError = Number(imports.error || 0);
        setText('[data-metric="imports_ok"]', formatInt.format(importsOk));
        setText('[data-metric="imports_error"]', formatInt.format(importsError));

        var jobsFailed = Number(data.jobs_failed || 0);
        var state = "ok";
        if (jobsFailed >= 3 || importsError >= 3) {
            state = "crit";
        } else if (jobsFailed > 0 || importsError > 0) {
            state = "warn";
        }
        setState("metrics", state);

        updateTimestamp(data.generated_at);
    };

    var fetchHealth = function () {
        if (!healthApi) {
            return Promise.resolve();
        }
        return fetch(healthApi, { headers: { "Accept": "application/json" } })
            .then(function (resp) {
                if (!resp.ok) {
                    throw new Error("bad response");
                }
                return resp.json();
            })
            .then(applyHealth);
    };

    var fetchMetrics = function () {
        if (!metricsApi) {
            return Promise.resolve();
        }
        return fetch(metricsApi, { headers: { "Accept": "application/json" } })
            .then(function (resp) {
                if (!resp.ok) {
                    throw new Error("bad response");
                }
                return resp.json();
            })
            .then(applyMetrics);
    };

    var refresh = function () {
        if (errorEl) {
            errorEl.hidden = true;
        }
        var errors = 0;
        return Promise.all([
            fetchHealth().catch(function () {
                errors += 1;
            }),
            fetchMetrics().catch(function () {
                errors += 1;
            })
        ]).then(function () {
            if (errorEl) {
                errorEl.hidden = errors === 0;
            }
        });
    };

    if (refreshBtn) {
        refreshBtn.addEventListener("click", function () {
            refresh();
        });
    }

    refresh();
    setInterval(refresh, 60000);
});
