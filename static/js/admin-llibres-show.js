(function () {
    var arxiuModal = document.getElementById("arxiu-links-modal");
    if (arxiuModal) {
        var openBtns = document.querySelectorAll("[data-open-arxiu-links]");
        var closeBtns = arxiuModal.querySelectorAll("[data-close-arxiu-links]");
        var form = document.getElementById("arxiu-links-form");
        var arxiuSearch = document.getElementById("arxiu-links-search");
        var arxiuHidden = document.getElementById("arxiu-links-id");
        var bookWrap = document.getElementById("arxiu-link-book-wrap");
        var bookSearch = document.getElementById("arxiu-link-book-search");
        var bookHidden = document.getElementById("arxiu-link-book-id");
        var bookSuggestions = document.getElementById("arxiu-link-book-suggestions");
        var urlWrap = document.getElementById("arxiu-link-url-wrap");
        var urlInput = document.getElementById("arxiu-link-url");
        var currentArxiuID = "";

        var clearBookSearch = function () {
            if (bookSearch) {
                bookSearch.value = "";
                bookSearch.dataset.api = "";
            }
            if (bookHidden) {
                bookHidden.value = "";
            }
            if (bookSuggestions) {
                bookSuggestions.innerHTML = "";
                bookSuggestions.classList.remove("is-open");
            }
        };

        var updateArxiuState = function () {
            var arxiuValue = arxiuHidden ? arxiuHidden.value.trim() : "";
            var hasArxiu = arxiuValue !== "";
            if (arxiuValue !== currentArxiuID) {
                clearBookSearch();
                currentArxiuID = arxiuValue;
            }
            if (bookWrap) {
                bookWrap.style.display = hasArxiu ? "flex" : "none";
            }
            if (urlWrap) {
                urlWrap.style.display = hasArxiu ? "none" : "flex";
            }
            if (urlInput) {
                urlInput.required = !hasArxiu;
                if (hasArxiu) {
                    urlInput.value = "";
                }
            }
            if (hasArxiu && bookSearch) {
                bookSearch.dataset.api = "/api/documentals/llibres/suggest?arxiu_id=" + encodeURIComponent(arxiuValue);
            }
            if (!hasArxiu) {
                clearBookSearch();
            }
        };

        if (openBtns.length > 0) {
            openBtns.forEach(function (btn) {
                btn.addEventListener("click", function () {
                    if (arxiuSearch) {
                        arxiuSearch.value = "";
                    }
                    if (arxiuHidden) {
                        arxiuHidden.value = "";
                    }
                    clearBookSearch();
                    currentArxiuID = "";
                    updateArxiuState();
                    arxiuModal.classList.add("is-open");
                });
            });
        }

        closeBtns.forEach(function (btn) {
            btn.addEventListener("click", function () {
                arxiuModal.classList.remove("is-open");
            });
        });

        if (arxiuSearch) {
            arxiuSearch.addEventListener("input", function () {
                updateArxiuState();
            });
            arxiuSearch.addEventListener("suggest:select", function () {
                updateArxiuState();
            });
        }
        if (arxiuHidden) {
            arxiuHidden.addEventListener("change", function () {
                updateArxiuState();
            });
        }
        if (form) {
            form.addEventListener("submit", function () {
                arxiuModal.classList.remove("is-open");
            });
        }
    }

    var mediaModal = document.getElementById("media-links-modal");
    if (mediaModal) {
        var mediaOpenBtns = document.querySelectorAll("[data-open-media-links]");
        var mediaCloseBtns = mediaModal.querySelectorAll("[data-close-media-links]");
        var searchInput = mediaModal.querySelector("#media-search");

        mediaOpenBtns.forEach(function (btn) {
            btn.addEventListener("click", function () {
                mediaModal.classList.add("is-open");
                if (searchInput) {
                    searchInput.focus();
                }
            });
        });

        mediaCloseBtns.forEach(function (btn) {
            btn.addEventListener("click", function () {
                mediaModal.classList.remove("is-open");
            });
        });

        mediaModal.addEventListener("click", function (event) {
            if (event.target === mediaModal) {
                mediaModal.classList.remove("is-open");
            }
        });
    }

    var optionsBtn = document.getElementById("botoOpcions");
    var dropdown = document.getElementById("dropdownOpcions");
    if (optionsBtn && dropdown) {
        optionsBtn.addEventListener("click", function (event) {
            event.preventDefault();
            dropdown.style.display = dropdown.style.display === "block" ? "none" : "block";
        });
        document.addEventListener("click", function (event) {
            if (!event.target.closest(".opcions-dropdown")) {
                dropdown.style.display = "none";
            }
        });
    }

    var markState = document.getElementById("llibre-mark-state");
    var markModal = document.getElementById("modalMarcarLlibre");
    var openBtn = document.querySelector(".btn-marcar");
    var closeBtns = document.querySelectorAll('[data-modal-close="modalMarcarLlibre"]');
    if (markState && markModal) {
        var markType = markModal.querySelector("#llibre-mark-type");
        var markPublic = markModal.querySelector("#llibre-mark-public");
        var markSave = markModal.querySelector("#llibre-mark-save");
        var markClear = markModal.querySelector("#llibre-mark-clear");
        var llibreID = parseInt(markState.dataset.llibreId || "0", 10);
        var csrfToken = markState.dataset.csrf || "";

        var openModal = function () {
            var existingType = markState.dataset.markType || "";
            var existingPublic = markState.dataset.markPublic !== "0";
            var own = markState.dataset.markOwn === "1";
            if (markType) {
                markType.value = existingType;
            }
            if (markPublic) {
                markPublic.checked = existingType ? existingPublic : true;
            }
            if (markClear) {
                markClear.disabled = !own;
            }
            markModal.classList.add("is-open");
        };

        var closeModal = function () {
            markModal.classList.remove("is-open");
        };

        if (openBtn) {
            openBtn.addEventListener("click", function (event) {
                event.preventDefault();
                if (!llibreID) {
                    return;
                }
                openModal();
            });
        }
        closeBtns.forEach(function (btn) {
            btn.addEventListener("click", closeModal);
        });

        if (markSave) {
            markSave.addEventListener("click", function () {
                if (!llibreID) {
                    return;
                }
                var typeValue = markType ? markType.value : "";
                if (!typeValue) {
                    return;
                }
                var body = new URLSearchParams();
                body.set("csrf_token", csrfToken);
                body.set("type", typeValue);
                body.set("public", markPublic && markPublic.checked ? "1" : "0");
                fetch("/documentals/llibres/" + llibreID + "/marcar", {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: body.toString(),
                    credentials: "same-origin"
                })
                    .then(function (response) {
                        if (!response.ok) {
                            throw new Error("mark_failed");
                        }
                        return response.json();
                    })
                    .then(function (data) {
                        if (data && data.ok) {
                            markState.dataset.markType = data.type || "";
                            markState.dataset.markPublic = data.is_public ? "1" : "0";
                            markState.dataset.markOwn = "1";
                            closeModal();
                        }
                    })
                    .catch(function () {});
            });
        }

        if (markClear) {
            markClear.addEventListener("click", function () {
                if (!llibreID) {
                    return;
                }
                if (markState.dataset.markOwn !== "1") {
                    closeModal();
                    return;
                }
                var body = new URLSearchParams();
                body.set("csrf_token", csrfToken);
                fetch("/documentals/llibres/" + llibreID + "/desmarcar", {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: body.toString(),
                    credentials: "same-origin"
                })
                    .then(function (response) {
                        if (!response.ok) {
                            throw new Error("mark_clear_failed");
                        }
                        return response.json();
                    })
                    .then(function (data) {
                        if (data && data.ok) {
                            markState.dataset.markType = "";
                            markState.dataset.markPublic = "0";
                            markState.dataset.markOwn = "0";
                            closeModal();
                        }
                    })
                    .catch(function () {});
            });
        }
    }

    var purgeModal = document.getElementById("purge-modal");
    if (purgeModal) {
        var purgeForm = document.getElementById("purge-form");
        var purgeTitle = document.getElementById("purge-modal-book");
        var purgeCloseButtons = purgeModal.querySelectorAll("[data-close-purge]");
        var purgeOpenButtons = document.querySelectorAll(".purge-trigger");
        var actionTemplate = "/documentals/llibres/__ID__/registres/purge";

        purgeOpenButtons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                var id = btn.getAttribute("data-llibre-id");
                var label = btn.getAttribute("data-llibre-title") || "";
                if (purgeForm) {
                    purgeForm.action = actionTemplate.replace("__ID__", id || "");
                }
                if (purgeTitle) {
                    purgeTitle.textContent = label;
                }
                purgeModal.classList.add("is-open");
                var input = purgeModal.querySelector("#purge-password");
                if (input) {
                    input.value = "";
                    input.focus();
                }
            });
        });

        purgeCloseButtons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                purgeModal.classList.remove("is-open");
            });
        });

        purgeModal.addEventListener("click", function (event) {
            if (event.target === purgeModal) {
                purgeModal.classList.remove("is-open");
            }
        });
    }
})();
