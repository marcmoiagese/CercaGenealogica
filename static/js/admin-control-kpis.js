document.addEventListener("DOMContentLoaded", function () {
    var panel = document.querySelector("[data-control-kpis-api]");
    if (!panel) {
        return;
    }
    var api = panel.getAttribute("data-control-kpis-api");
    var refreshBtn = panel.querySelector("[data-control-kpis-refresh]") || document.querySelector("[data-control-kpis-refresh]");
    var updatedEl = panel.querySelector("[data-control-kpis-updated]");
    var errorEl = panel.querySelector("[data-control-kpis-error]");
    var pendingList = panel.querySelector("[data-kpi-list='pending_moderation_by_type']");
    var pendingEmpty = panel.getAttribute("data-pending-empty") || "-";
    var typeLabels = window.controlModerationTypeLabels || {};
    var stateLabels = {
        ok: panel.getAttribute("data-state-ok") || "OK",
        warn: panel.getAttribute("data-state-warn") || "WARN",
        crit: panel.getAttribute("data-state-crit") || "CRIT"
    };
    var updatedLabel = panel.getAttribute("data-updated-label") || "Updated";
    var formatInt = new Intl.NumberFormat();

    var setValue = function (key, value) {
        var el = panel.querySelector('[data-kpi="' + key + '"]');
        if (el) {
            el.textContent = value;
        }
    };

    var setState = function (cardKey, state) {
        var card = panel.querySelector('[data-kpi-card="' + cardKey + '"]');
        if (!card) {
            return;
        }
        card.dataset.state = state;
        var statusEl = card.querySelector("[data-kpi-status]");
        if (statusEl) {
            statusEl.textContent = stateLabels[state] || state.toUpperCase();
        }
    };

    var renderPendingTypes = function (items) {
        if (!pendingList) {
            return;
        }
        pendingList.innerHTML = "";
        if (!items || !items.length) {
            var empty = document.createElement("span");
            empty.className = "muted";
            empty.textContent = pendingEmpty;
            pendingList.appendChild(empty);
            return;
        }
        items.forEach(function (item) {
            var label = typeLabels[item.type] || item.type;
            var pill = document.createElement("span");
            pill.className = "ops-pill";
            pill.textContent = label;

            var count = document.createElement("span");
            count.className = "ops-pill__count";
            count.textContent = formatInt.format(item.total || 0);
            pill.appendChild(count);
            pendingList.appendChild(pill);
        });
    };

    var applyData = function (data) {
        var pendingTotal = Number(data.pending_moderation_total || 0);
        setValue("pending_moderation_total", formatInt.format(pendingTotal));
        renderPendingTypes(data.pending_moderation_by_type || []);
        if (pendingTotal === 0) {
            setState("pending_moderation", "ok");
        } else if (pendingTotal <= 20) {
            setState("pending_moderation", "warn");
        } else {
            setState("pending_moderation", "crit");
        }

        var users7d = Number(data.new_users_7d || 0);
        var users30d = Number(data.new_users_30d || 0);
        setValue("new_users_7d", formatInt.format(users7d));
        setValue("new_users_30d", formatInt.format(users30d));
        setState("new_users", users7d === 0 ? "warn" : "ok");

        var imports = data.imports_last_24h || {};
        var importsOk = Number(imports.ok || 0);
        var importsError = Number(imports.error || 0);
        setValue("imports_ok", formatInt.format(importsOk));
        setValue("imports_error", formatInt.format(importsError));
        if (importsError === 0) {
            setState("imports", "ok");
        } else if (importsError < 3) {
            setState("imports", "warn");
        } else {
            setState("imports", "crit");
        }

        var running = Number(data.rebuild_jobs_running || 0);
        var failed = Number(data.rebuild_jobs_failed || 0);
        setValue("rebuild_jobs_running", formatInt.format(running));
        setValue("rebuild_jobs_failed", formatInt.format(failed));
        if (failed === 0) {
            setState("rebuild_jobs", "ok");
        } else if (failed < 3) {
            setState("rebuild_jobs", "warn");
        } else {
            setState("rebuild_jobs", "crit");
        }

        if (updatedEl) {
            var ts = data.generated_at ? new Date(data.generated_at) : new Date();
            if (!isNaN(ts.getTime())) {
                updatedEl.textContent = updatedLabel + " " + ts.toLocaleTimeString();
            }
        }
    };

    var fetchKPIs = function () {
        if (errorEl) {
            errorEl.hidden = true;
        }
        return fetch(api, { headers: { "Accept": "application/json" } })
            .then(function (resp) {
                if (!resp.ok) {
                    throw new Error("bad response");
                }
                return resp.json();
            })
            .then(applyData)
            .catch(function () {
                if (errorEl) {
                    errorEl.hidden = false;
                }
            });
    };

    if (refreshBtn) {
        refreshBtn.addEventListener("click", function () {
            fetchKPIs();
        });
    }

    fetchKPIs();
    setInterval(fetchKPIs, 60000);
});
