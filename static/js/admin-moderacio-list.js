(function () {
    var selectAll = document.getElementById("select-all");
    if (selectAll) {
        var checkboxes = document.querySelectorAll(".moderacio-select");
        selectAll.addEventListener("change", function () {
            checkboxes.forEach(function (box) {
                if (box.id === "select-all") {
                    return;
                }
                box.checked = selectAll.checked;
            });
        });
    }

    var actionSelect = document.getElementById("bulk-action");
    var reasonField = document.getElementById("bulk-reason");
    var scopeSelect = document.getElementById("bulk-scope");
    var typeGroup = document.getElementById("bulk-type-group");
    var userGroup = document.getElementById("bulk-user-group");
    if (scopeSelect && typeGroup) {
        var toggleType = function () {
            var show = scopeSelect.value === "all";
            typeGroup.style.display = show ? "flex" : "none";
            if (userGroup) {
                userGroup.style.display = show ? "flex" : "none";
            }
        };
        toggleType();
        scopeSelect.addEventListener("change", toggleType);
    }
    if (actionSelect && reasonField) {
        var toggleReason = function () {
            var show = actionSelect.value === "reject";
            reasonField.disabled = !show;
            reasonField.style.opacity = show ? "1" : "0.5";
        };
        toggleReason();
        actionSelect.addEventListener("change", toggleReason);
    }

    var perPage = document.getElementById("per-page");
    if (perPage) {
        perPage.addEventListener("change", function () {
            var base = perPage.dataset.base || "/moderacio";
            try {
                var url = new URL(base, window.location.origin);
                url.searchParams.set("per_page", perPage.value);
                url.searchParams.delete("page");
                window.location.href = url.pathname + "?" + url.searchParams.toString();
            } catch (err) {
                window.location.href = "/moderacio?per_page=" + perPage.value;
            }
        });
    }

    var bulkForm = document.getElementById("bulk-form");
    var bulkStatus = document.getElementById("bulk-status");
    if (bulkForm && bulkStatus && scopeSelect) {
        var runningLabel = bulkStatus.dataset.running || "";
        var doneLabel = bulkStatus.dataset.done || "";
        var errorLabel = bulkStatus.dataset.error || "";
        var doneErrorsLabel = bulkStatus.dataset.doneErrors || errorLabel;
        var updatedLabel = bulkStatus.dataset.updatedLabel || "Updated";
        var errorsCountLabel = bulkStatus.dataset.errorsLabel || "Errors";
        var viewLabel = bulkStatus.dataset.viewLabel || "View";
        var jobBase = bulkStatus.dataset.jobBase || "";
        var resolveBulkError = function (err) {
            if (!err || !err.message) {
                return errorLabel;
            }
            var message = String(err.message || "").trim();
            if (!message || message === "request" || message === "status" || message === "job" || message === "missing") {
                return errorLabel;
            }
            return message;
        };
        var setBulkStatus = function (message, show, detailURL) {
            if (!bulkStatus) {
                return;
            }
            bulkStatus.textContent = "";
            bulkStatus.appendChild(document.createTextNode(message));
            if (detailURL) {
                bulkStatus.appendChild(document.createTextNode(" "));
                var link = document.createElement("a");
                link.href = detailURL;
                link.textContent = viewLabel;
                bulkStatus.appendChild(link);
            }
            bulkStatus.style.display = show ? "block" : "none";
        };
        var setBulkDisabled = function (disabled) {
            var inputs = bulkForm.querySelectorAll("input, select, button");
            inputs.forEach(function (input) {
                input.disabled = disabled;
            });
        };
        var pollJob = function (jobID) {
            var poll = function () {
                fetch(jobBase + jobID, { credentials: "same-origin" })
                    .then(function (resp) {
                        if (!resp.ok) {
                            return resp.text().then(function (text) {
                                throw new Error((text || "").trim() || "status");
                            });
                        }
                        return resp.json();
                    })
                    .then(function (data) {
                        var job = data && data.job ? data.job : null;
                        if (!job) {
                            throw new Error("missing");
                        }
                        if (!job.done) {
                            var label = runningLabel || "";
                            if (job.total && job.total > 0) {
                                label = label + " (" + (job.processed || 0) + "/" + job.total + ")";
                            }
                            setBulkStatus(label, true, job.detail_url || "");
                            window.setTimeout(poll, 1000);
                            return;
                        }
                        var summary = job.summary || null;
                        if (job.error) {
                            var errorMessage = job.error;
                            if (summary && summary.errors > 0) {
                                errorMessage = (doneErrorsLabel || errorLabel) + " (" + updatedLabel + ": " + (summary.updated || 0) + ", " + errorsCountLabel + ": " + (summary.errors || 0) + ")";
                            }
                            setBulkStatus(errorMessage || errorLabel, true, job.detail_url || "");
                        } else {
                            setBulkStatus(doneLabel, true, job.detail_url || "");
                        }
                        setBulkDisabled(false);
                    })
                    .catch(function (err) {
                        setBulkStatus(resolveBulkError(err), true, "");
                        setBulkDisabled(false);
                    });
            };
            poll();
        };
        bulkForm.addEventListener("submit", function (event) {
            if (scopeSelect.value !== "all") {
                return;
            }
            event.preventDefault();
            var formData = new FormData(bulkForm);
            formData.set("async", "1");
            setBulkDisabled(true);
            setBulkStatus(runningLabel, true);
            fetch(bulkForm.action, {
                method: "POST",
                body: formData,
                credentials: "same-origin",
                headers: {
                    "Accept": "application/json"
                }
            })
                .then(function (resp) {
                    if (!resp.ok) {
                        return resp.text().then(function (text) {
                            throw new Error((text || "").trim() || "request");
                        });
                    }
                    return resp.json();
                })
                .then(function (data) {
                    if (data && data.job_id) {
                        pollJob(data.job_id);
                        return;
                    }
                    throw new Error("job");
                })
                .catch(function (err) {
                    setBulkStatus(resolveBulkError(err), true, "");
                    setBulkDisabled(false);
                });
        });
    }
})();
