(function () {
    var root = document.querySelector("[data-admin-jobs]");
    if (!root) {
        return;
    }
    var csrf = root.getAttribute("data-csrf") || "";
    var errorMsg = root.getAttribute("data-retry-error") || "Error";
    var buttons = document.querySelectorAll("[data-job-retry]");
    buttons.forEach(function (btn) {
        btn.addEventListener("click", function () {
            var url = btn.getAttribute("data-job-retry");
            if (!url) {
                return;
            }
            btn.disabled = true;
            fetch(url, {
                method: "POST",
                credentials: "same-origin",
                headers: {
                    "X-CSRF-Token": csrf
                }
            })
                .then(function (resp) {
                    if (!resp.ok) {
                        throw new Error("failed");
                    }
                    return resp.json();
                })
                .then(function (payload) {
                    if (payload && payload.ok) {
                        if (payload.job_id) {
                            window.location.href = "/admin/jobs/" + payload.job_id;
                            return;
                        }
                        window.location.reload();
                        return;
                    }
                    throw new Error("failed");
                })
                .catch(function () {
                    alert(errorMsg);
                })
                .finally(function () {
                    btn.disabled = false;
                });
        });
    });
})();
