(function () {
    var form = document.querySelector(".import-form");
    var overlay = document.getElementById("import-overlay");
    if (!form || !overlay) {
        return;
    }

    var bar = document.getElementById("import-progress-bar");
    var percent = document.getElementById("import-percent");
    var status = document.getElementById("import-status");
    var progress = bar ? bar.closest(".import-progress") : null;
    var processing = document.getElementById("import-processing");
    var keepOpen = document.getElementById("import-keep-open");
    var doneBox = document.getElementById("import-complete");
    var summary = document.getElementById("import-summary");
    var importedOut = document.getElementById("import-imported");
    var failedOut = document.getElementById("import-failed");
    var updatedOut = document.getElementById("import-updated");
    var errorsLink = document.getElementById("import-errors-link");
    var closeBtn = document.getElementById("import-close");
    var errorBanner = document.getElementById("import-error-banner");
    var modelSelect = document.getElementById("import-model");
    var templateField = document.querySelector("[data-template-field]");
    var templateSelect = templateField ? templateField.querySelector("select") : null;
    var separatorSelect = document.getElementById("import-sep");
    var lastSeparator = separatorSelect ? separatorSelect.value : "";
    var titleUpload = overlay.getAttribute("data-title-upload") || "";
    var titleProcessing = overlay.getAttribute("data-title-processing") || "";
    var titleFinalizing = overlay.getAttribute("data-title-finalizing") || "";
    var titleError = overlay.getAttribute("data-title-error") || "";
    var msgKeepOpen = keepOpen ? keepOpen.textContent : titleProcessing;
    var originalTitle = document.title;

    var setStatus = function (label) {
        if (status) {
            status.textContent = label;
        }
        document.title = label + " · " + originalTitle;
    };

    var setProgress = function (val) {
        if (bar) {
            bar.style.width = val + "%";
        }
        if (percent) {
            percent.textContent = val + "%";
        }
    };

    var showOverlay = function () {
        overlay.classList.add("is-open");
    };

    var hideOverlay = function () {
        overlay.classList.remove("is-open");
        document.title = originalTitle;
    };

    var showProcessing = function () {
        if (processing) {
            processing.hidden = false;
        }
        if (progress) {
            progress.hidden = true;
        }
        if (percent) {
            percent.hidden = true;
        }
        if (summary) {
            summary.hidden = true;
        }
        if (doneBox) {
            doneBox.hidden = true;
        }
        if (closeBtn) {
            closeBtn.hidden = true;
        }
        setStatus(titleProcessing);
    };

    var showFinalizing = function () {
        if (processing) {
            processing.hidden = true;
        }
        if (progress) {
            progress.hidden = true;
        }
        if (percent) {
            percent.hidden = true;
        }
        if (summary) {
            summary.hidden = true;
        }
        if (doneBox) {
            doneBox.hidden = false;
        }
        if (closeBtn) {
            closeBtn.hidden = true;
        }
        setStatus(titleFinalizing);
    };

    var showSummary = function (summaryData) {
        if (summary) {
            summary.hidden = false;
        }
        if (doneBox) {
            doneBox.hidden = true;
        }
        if (processing) {
            processing.hidden = true;
        }
        if (progress) {
            progress.hidden = true;
        }
        if (percent) {
            percent.hidden = true;
        }
        if (closeBtn) {
            closeBtn.hidden = false;
        }
        if (importedOut) {
            importedOut.textContent = summaryData.imported || 0;
        }
        if (updatedOut) {
            updatedOut.textContent = summaryData.updated || 0;
        }
        if (failedOut) {
            failedOut.textContent = summaryData.failed || 0;
        }
        if (errorsLink) {
            if (summaryData.errors_url) {
                errorsLink.href = summaryData.errors_url;
                errorsLink.hidden = false;
            } else {
                errorsLink.hidden = true;
            }
        }
        setStatus(titleUpload);
    };

    var showError = function () {
        if (errorBanner) {
            errorBanner.hidden = false;
        }
        if (closeBtn) {
            closeBtn.hidden = false;
        }
        if (progress) {
            progress.hidden = true;
        }
        if (percent) {
            percent.hidden = true;
        }
        if (processing) {
            processing.hidden = true;
        }
        if (doneBox) {
            doneBox.hidden = true;
        }
        if (summary) {
            summary.hidden = true;
        }
        setStatus(titleError);
    };

    var poll = function (url) {
        return fetch(url, { credentials: "same-origin" })
            .then(function (resp) {
                if (!resp.ok) {
                    throw new Error("bad");
                }
                return resp.json();
            })
            .then(function (data) {
                if (!data) {
                    throw new Error("bad");
                }
                if (data.status === "processing") {
                    showProcessing();
                    if (data.progress != null) {
                        setProgress(data.progress);
                    }
                    if (keepOpen) {
                        keepOpen.textContent = data.note || msgKeepOpen;
                    }
                    if (data.redirect_url) {
                        window.location.href = data.redirect_url;
                        return;
                    }
                    setTimeout(function () {
                        poll(url);
                    }, 800);
                    return;
                }
                if (data.status === "finalizing") {
                    showFinalizing();
                    setTimeout(function () {
                        poll(url);
                    }, 1200);
                    return;
                }
                if (data.status === "done") {
                    if (data.summary) {
                        showSummary(data.summary);
                    } else {
                        hideOverlay();
                    }
                    if (data.redirect_url) {
                        setTimeout(function () {
                            window.location.href = data.redirect_url;
                        }, 1200);
                    }
                    return;
                }
                showError();
            })
            .catch(function () {
                showError();
            });
    };

    var maybeToggleTemplate = function () {
        if (!modelSelect || !templateField) {
            return;
        }
        if (modelSelect.value === "template") {
            templateField.hidden = false;
        } else {
            templateField.hidden = true;
            if (templateSelect) {
                templateSelect.value = "";
            }
        }
    };

    var maybeUpdateSeparator = function () {
        if (!separatorSelect || !templateSelect) {
            return;
        }
        var selected = templateSelect.selectedOptions[0];
        if (!selected) {
            return;
        }
        var value = selected.getAttribute("data-separator") || "";
        if (value && value !== lastSeparator) {
            separatorSelect.value = value;
            lastSeparator = value;
        }
    };

    form.addEventListener("submit", function () {
        showOverlay();
        setStatus(titleUpload);
        setProgress(0);
        if (errorBanner) {
            errorBanner.hidden = true;
        }
    });

    if (closeBtn) {
        closeBtn.addEventListener("click", function () {
            hideOverlay();
        });
    }

    if (modelSelect) {
        modelSelect.addEventListener("change", maybeToggleTemplate);
        maybeToggleTemplate();
    }

    if (templateSelect) {
        templateSelect.addEventListener("change", maybeUpdateSeparator);
        maybeUpdateSeparator();
    }

    if (form.getAttribute("data-import-status")) {
        showOverlay();
        poll(form.getAttribute("data-import-status"));
    }
})();
