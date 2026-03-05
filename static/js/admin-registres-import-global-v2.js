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

    var updateTemplateField = function () {
        if (!modelSelect || !templateField) {
            return;
        }
        var isTemplate = modelSelect.value === "template";
        templateField.hidden = !isTemplate;
        if (templateSelect) {
            templateSelect.required = isTemplate;
            if (!isTemplate) {
                templateSelect.value = "";
            }
        }
        if (separatorSelect) {
            if (isTemplate) {
                if (separatorSelect.value !== "") {
                    lastSeparator = separatorSelect.value;
                }
                separatorSelect.value = "";
            } else if (separatorSelect.value === "") {
                if (lastSeparator) {
                    separatorSelect.value = lastSeparator;
                } else {
                    var options = Array.from(separatorSelect.options || []);
                    var fallback = options.find(function (opt) {
                        return opt.value !== "";
                    });
                    if (fallback) {
                        separatorSelect.value = fallback.value;
                    }
                }
            }
        }
    };

    if (modelSelect) {
        modelSelect.addEventListener("change", updateTemplateField);
        updateTemplateField();
    }

    form.addEventListener("submit", function (event) {
        event.preventDefault();
        var submitBtn = form.querySelector('button[type="submit"]');
        if (submitBtn) {
            submitBtn.disabled = true;
        }
        overlay.classList.add("is-open");
        if (progress) {
            progress.classList.remove("is-processing");
        }
        if (bar) {
            bar.style.width = "0%";
        }
        if (percent) {
            percent.textContent = "0%";
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
        if (status) {
            status.textContent = titleUpload;
        }
        document.title = titleUpload;
        window.onbeforeunload = function () {
            return msgKeepOpen;
        };
        if (errorBanner) {
            errorBanner.hidden = true;
        }
        if (closeBtn) {
            closeBtn.hidden = true;
        }

        var xhr = new XMLHttpRequest();
        xhr.open("POST", form.action, true);
        xhr.upload.addEventListener("progress", function (e) {
            if (e.lengthComputable) {
                var pct = Math.round((e.loaded / e.total) * 100);
                if (bar) {
                    bar.style.width = pct + "%";
                }
                if (percent) {
                    percent.textContent = pct + "%";
                }
            }
        });
        xhr.upload.addEventListener("load", function () {
            if (progress) {
                progress.classList.add("is-processing");
            }
            if (status) {
                status.textContent = titleProcessing;
            }
            if (percent) {
                percent.textContent = "";
            }
            if (processing) {
                processing.hidden = false;
            }
            if (doneBox) {
                doneBox.hidden = true;
            }
            document.title = titleProcessing;
        });
        xhr.addEventListener("load", function () {
            if (xhr.status >= 200 && xhr.status < 400) {
                var nextURL = xhr.responseURL || form.action;
                var url = new URL(nextURL, window.location.origin);
                var imported = url.searchParams.get("imported");
                var failed = url.searchParams.get("failed");
                var updated = url.searchParams.get("updated");
                var token = url.searchParams.get("errors_token");
                if (processing) {
                    processing.hidden = true;
                }
                if (progress) {
                    progress.classList.remove("is-processing");
                }
                if (doneBox) {
                    doneBox.hidden = false;
                }
                if (summary && importedOut && failedOut) {
                    importedOut.textContent = imported || "0";
                    failedOut.textContent = failed || "0";
                    if (updatedOut) {
                        updatedOut.textContent = updated || "0";
                    }
                    summary.hidden = false;
                }
                if (errorsLink) {
                    if (token) {
                        errorsLink.setAttribute("href", "/documentals/llibres/importar/errors?token=" + encodeURIComponent(token));
                        errorsLink.hidden = false;
                    } else {
                        errorsLink.hidden = true;
                    }
                }
                if (status) {
                    status.textContent = titleFinalizing;
                }
                document.title = titleFinalizing;
                window.onbeforeunload = null;
                window.location.assign(nextURL);
                return;
            }
            if (progress) {
                progress.classList.remove("is-processing");
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
            if (status) {
                status.textContent = titleError;
            }
            document.title = titleError;
            window.onbeforeunload = null;
            if (closeBtn) {
                closeBtn.hidden = false;
            }
            if (errorBanner) {
                errorBanner.hidden = false;
            }
            if (submitBtn) {
                submitBtn.disabled = false;
            }
        });
        xhr.addEventListener("loadstart", function () {
            if (progress) {
                progress.classList.remove("is-processing");
            }
            if (status) {
                status.textContent = titleUpload;
            }
            if (processing) {
                processing.hidden = true;
            }
            if (summary) {
                summary.hidden = true;
            }
            document.title = titleUpload;
        });
        xhr.addEventListener("error", function () {
            if (progress) {
                progress.classList.remove("is-processing");
            }
            if (processing) {
                processing.hidden = true;
            }
            if (summary) {
                summary.hidden = true;
            }
            if (status) {
                status.textContent = titleError;
            }
            document.title = titleError;
            window.onbeforeunload = null;
            if (closeBtn) {
                closeBtn.hidden = false;
            }
            if (errorBanner) {
                errorBanner.hidden = false;
            }
            if (submitBtn) {
                submitBtn.disabled = false;
            }
        });
        xhr.addEventListener("loadend", function () {
            if (xhr.status >= 200 && xhr.status < 400) {
                return;
            }
            document.title = originalTitle;
        });
        xhr.send(new FormData(form));
    });

    if (closeBtn) {
        closeBtn.addEventListener("click", function () {
            overlay.classList.remove("is-open");
        });
    }
})();
