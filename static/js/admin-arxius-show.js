(function () {
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

    var markState = document.getElementById("arxiu-mark-state");
    var modal = document.getElementById("modalMarcarArxiu");
    var openBtn = document.querySelector(".btn-marcar");
    var closeBtns = document.querySelectorAll('[data-modal-close="modalMarcarArxiu"]');
    if (!markState || !modal) {
        return;
    }
    var markType = modal.querySelector("#arxiu-mark-type");
    var markPublic = modal.querySelector("#arxiu-mark-public");
    var markSave = modal.querySelector("#arxiu-mark-save");
    var markClear = modal.querySelector("#arxiu-mark-clear");
    var arxiuID = parseInt(markState.dataset.arxiuId || "0", 10);
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
        modal.classList.add("is-open");
    };

    var closeModal = function () {
        modal.classList.remove("is-open");
    };

    if (openBtn) {
        openBtn.addEventListener("click", function (event) {
            event.preventDefault();
            if (!arxiuID) {
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
            if (!arxiuID) {
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
            fetch("/documentals/arxius/" + arxiuID + "/marcar", {
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
            if (!arxiuID) {
                return;
            }
            if (markState.dataset.markOwn !== "1") {
                closeModal();
                return;
            }
            var body = new URLSearchParams();
            body.set("csrf_token", csrfToken);
            fetch("/documentals/arxius/" + arxiuID + "/desmarcar", {
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
})();
