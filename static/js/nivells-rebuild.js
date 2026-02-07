document.addEventListener("DOMContentLoaded", () => {
    const buttons = Array.from(document.querySelectorAll("[data-rebuild-kind]"));
    if (!buttons.length) {
        return;
    }
    const status = document.getElementById("nivellsRebuildStatus");
    const log = document.getElementById("nivellsRebuildLog");
    const csrf = document.getElementById("nivellsRebuildCsrf");
    const csrfToken = csrf ? csrf.value : "";
    const runningLabel = status ? status.dataset.running || "" : "";
    const doneLabel = status ? status.dataset.done || "" : "";
    const errorLabel = status ? status.dataset.error || "" : "";

    const setStatus = (message) => {
        if (status) {
            status.textContent = message;
        }
    };

    const setDisabled = (disabled) => {
        buttons.forEach((btn) => {
            btn.disabled = disabled;
        });
    };

    const appendLog = (message) => {
        if (!log) {
            return;
        }
        const line = document.createElement("div");
        line.textContent = message;
        log.appendChild(line);
    };

    const callRebuild = async (kind) => {
        const resp = await fetch(`/api/admin/nivells/0/${kind}/rebuild?all=1&async=1`, {
            method: "POST",
            headers: {
                "X-CSRF-Token": csrfToken,
            },
            credentials: "same-origin",
        });
        if (!resp.ok) {
            throw new Error("request failed");
        }
        return resp.json();
    };

    const pollJob = (jobID) =>
        new Promise((resolve, reject) => {
            let lastLogLength = 0;
            const poll = async () => {
                try {
                    const resp = await fetch(`/api/admin/nivells/rebuild/${jobID}`, {
                        method: "GET",
                        credentials: "same-origin",
                    });
                    if (!resp.ok) {
                        throw new Error("status error");
                    }
                    const data = await resp.json();
                    const job = data && data.job ? data.job : null;
                    if (!job) {
                        throw new Error("missing job");
                    }
                    const processed = job.processed || 0;
                    const total = job.total || 0;
                    if (!job.done) {
                        if (runningLabel) {
                            if (total > 0) {
                                setStatus(`${runningLabel} (${processed}/${total})`);
                            } else {
                                setStatus(runningLabel);
                            }
                        }
                    }
                    if (Array.isArray(job.logs) && job.logs.length > lastLogLength) {
                        for (let i = lastLogLength; i < job.logs.length; i += 1) {
                            appendLog(job.logs[i]);
                        }
                        lastLogLength = job.logs.length;
                    }
                    if (job.done) {
                        if (job.error) {
                            appendLog(job.error);
                            setStatus(errorLabel || "");
                            reject(new Error(job.error));
                        } else {
                            if (doneLabel) {
                                setStatus(doneLabel);
                            }
                            resolve();
                        }
                        return;
                    }
                    window.setTimeout(poll, 1000);
                } catch (err) {
                    appendLog(errorLabel || "error");
                    setStatus(errorLabel || "");
                    reject(err);
                }
            };
            poll();
        });

    buttons.forEach((button) => {
        button.addEventListener("click", async () => {
            const kind = button.dataset.rebuildKind || "";
            const confirmText = button.dataset.confirm || "";
            if (confirmText && !window.confirm(confirmText)) {
                return;
            }
            setDisabled(true);
            setStatus(runningLabel);
            appendLog(`${runningLabel} (${kind})`);
            try {
                if (kind === "all") {
                    const demoResp = await callRebuild("demografia");
                    if (demoResp && demoResp.job_id) {
                        await pollJob(demoResp.job_id);
                    }
                    const statsResp = await callRebuild("stats");
                    if (statsResp && statsResp.job_id) {
                        await pollJob(statsResp.job_id);
                    }
                } else {
                    const resp = await callRebuild(kind);
                    if (resp && resp.job_id) {
                        await pollJob(resp.job_id);
                    }
                }
            } catch (err) {
                appendLog(errorLabel || "error");
                setStatus(errorLabel || "");
            } finally {
                setDisabled(false);
            }
        });
    });
});
