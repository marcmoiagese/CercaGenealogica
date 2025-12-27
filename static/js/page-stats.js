(() => {
    function initPageStats() {
        const table = document.getElementById("pageStatsTable");
        const form = document.getElementById("pageStatsForm");
        if (!table || !form) {
            return;
        }

    const editLabel = table.dataset.editLabel || "Editar";
    const confirmLabel = table.dataset.confirmLabel || "Guardar";
    const cancelLabel = table.dataset.cancelLabel || "Cancel·lar";

    function scrollKey() {
        return `scroll:${window.location.pathname}`;
    }

    function storeScroll() {
        try {
            sessionStorage.setItem(scrollKey(), String(window.scrollY || 0));
        } catch (_e) {
            // ignore storage errors
        }
    }

    function resetCell(cell, value) {
        const textEl = cell.querySelector(".cell-text");
        if (textEl) {
            textEl.textContent = value === "" ? "—" : value;
        }
        cell.classList.remove("is-editing");
    }

    function valueForKey(row, key) {
        if (!row) {
            return "";
        }
        if (key === "num_pagina_text") {
            return row.dataset.numPagina || "";
        }
        if (key === "pagina_id") {
            return row.dataset.paginaId || "";
        }
        if (key === "total_registres") {
            return row.dataset.totalRegistres || "";
        }
        if (key === "tipus_pagina") {
            return row.dataset.tipusPagina || "normal";
        }
        if (key === "exclosa") {
            return row.dataset.exclosa || "0";
        }
        if (key === "duplicada_de") {
            return row.dataset.duplicadaDe || "";
        }
        if (key === "indexacio_completa") {
            return row.dataset.indexacioCompleta || "0";
        }
        return "";
    }

    function openEditor(cell) {
        if (cell.classList.contains("is-editing")) {
            return;
        }
        const row = cell.closest("tr");
        if (!row) {
            return;
        }
        const key = cell.dataset.key;
        const type = cell.dataset.type || "text";
        const currentValue = valueForKey(row, key);
        cell.classList.add("is-editing");
        const wrapper = document.createElement("div");
        wrapper.className = "editable-cell";
        const input = document.createElement("input");
        input.className = "edit-input";
        input.type = type === "number" ? "number" : "text";
        input.value = currentValue;
        input.placeholder = currentValue === "" ? "—" : currentValue;
        const confirmBtn = document.createElement("button");
        confirmBtn.type = "button";
        confirmBtn.className = "confirm-icon";
        confirmBtn.innerHTML = '<i class="fa fa-check"></i>';
        confirmBtn.setAttribute("aria-label", confirmLabel);
        const cancelBtn = document.createElement("button");
        cancelBtn.type = "button";
        cancelBtn.className = "cancel-icon";
        cancelBtn.innerHTML = '<i class="fa fa-times"></i>';
        cancelBtn.setAttribute("aria-label", cancelLabel);
        wrapper.appendChild(input);
        wrapper.appendChild(confirmBtn);
        wrapper.appendChild(cancelBtn);
        cell.innerHTML = "";
        cell.appendChild(wrapper);
        input.focus();

        cancelBtn.addEventListener("click", () => {
            cell.innerHTML = `
                <div class="editable-cell">
                    <span class="cell-text page-stat-value">${currentValue === "" ? "—" : currentValue}</span>
                    <span class="edit-icons">
                        <button type="button" class="edit-icon page-stat-edit" aria-label="${editLabel}">
                            <i class="fa fa-pencil"></i>
                        </button>
                    </span>
                </div>
            `;
            bindCell(cell);
            cell.classList.remove("is-editing");
        });

        confirmBtn.addEventListener("click", () => {
            const newValue = input.value.trim();
            if (key === "num_pagina_text") {
                row.dataset.numPagina = newValue;
            } else if (key === "pagina_id") {
                row.dataset.paginaId = newValue;
            } else if (key === "total_registres") {
                row.dataset.totalRegistres = newValue;
            } else if (key === "indexacio_completa") {
                row.dataset.indexacioCompleta = newValue;
            }
            submitRow(row);
        });
    }

    function submitRow(row) {
        const returnTo = form.querySelector("[name='return_to']");
        if (returnTo) {
            returnTo.value = `${window.location.pathname}${window.location.search}#page-stats-controls`;
        }
        storeScroll();
        form.querySelector("[name='stat_id']").value = row.dataset.statId || "0";
        form.querySelector("[name='llibre_id']").value = row.dataset.llibreId || "";
        form.querySelector("[name='num_pagina_text']").value = row.dataset.numPagina || "";
        form.querySelector("[name='pagina_id']").value = row.dataset.paginaId || "";
        form.querySelector("[name='tipus_pagina']").value = row.dataset.tipusPagina || "normal";
        form.querySelector("[name='exclosa']").value = row.dataset.exclosa || "0";
        form.querySelector("[name='indexacio_completa']").value = row.dataset.indexacioCompleta || "0";
        form.querySelector("[name='duplicada_de']").value = row.dataset.duplicadaDe || "";
        form.querySelector("[name='total_registres']").value = row.dataset.totalRegistres || "0";
        form.submit();
    }

    function bindCell(cell) {
        const editButton = cell.querySelector(".page-stat-edit");
        if (!editButton) {
            return;
        }
        editButton.addEventListener("click", (event) => {
            event.preventDefault();
            openEditor(cell);
        });
    }

    table.querySelectorAll(".page-stat-cell").forEach((cell) => {
        bindCell(cell);
    });

    const modal = document.getElementById("page-stats-modal");
    const modalForm = document.getElementById("page-stats-modal-form");
    const modalType = document.getElementById("page-stats-type");
    const modalExclude = document.getElementById("page-stats-exclosa");
    const modalDuplicate = document.getElementById("page-stats-duplicada");
    const modalSave = document.getElementById("page-stats-modal-save");
    const modalCloseButtons = modal ? modal.querySelectorAll("[data-page-stats-close]") : [];
    let activeRow = null;

    function closeModal() {
        if (!modal) {
            return;
        }
        modal.classList.remove("is-open");
        activeRow = null;
    }

    function openModal(row) {
        if (!modal || !row) {
            return;
        }
        activeRow = row;
        if (modalForm) {
            modalForm.querySelector("[name='stat_id']").value = row.dataset.statId || "";
        }
        if (modalType) {
            modalType.value = row.dataset.tipusPagina || "normal";
        }
        if (modalExclude) {
            modalExclude.checked = row.dataset.exclosa === "1";
        }
        if (modalDuplicate) {
            modalDuplicate.value = row.dataset.duplicadaDe || "";
        }
        modal.classList.add("is-open");
    }

        table.addEventListener("click", (event) => {
            const target = event.target;
            if (!(target instanceof Element)) {
                return;
            }
            const configButton = target.closest(".page-stat-config");
            if (configButton) {
                const row = configButton.closest("tr");
                openModal(row);
                return;
            }
            const toggleButton = target.closest(".page-stat-toggle-indexed");
            if (toggleButton) {
                event.preventDefault();
                const row = toggleButton.closest("tr");
                if (!row) {
                    return;
                }
                const current = row.dataset.indexacioCompleta === "1";
                const nextValue = current ? "0" : "1";
                row.dataset.indexacioCompleta = nextValue;
                toggleButton.classList.toggle("is-active", nextValue === "1");
                toggleButton.setAttribute("aria-pressed", nextValue === "1" ? "true" : "false");
                const icon = toggleButton.querySelector("i");
                if (icon) {
                    icon.classList.toggle("fa-check-circle", nextValue === "1");
                    icon.classList.toggle("fa-circle", nextValue !== "1");
                }
                submitRow(row);
            }
        });

    if (modalSave) {
        modalSave.addEventListener("click", () => {
            if (!activeRow) {
                closeModal();
                return;
            }
            if (modalType) {
                activeRow.dataset.tipusPagina = modalType.value || "normal";
            }
            if (modalExclude) {
                activeRow.dataset.exclosa = modalExclude.checked ? "1" : "0";
            }
            if (modalDuplicate) {
                activeRow.dataset.duplicadaDe = modalDuplicate.value.trim();
            }
            submitRow(activeRow);
        });
    }

    modalCloseButtons.forEach((btn) => {
        btn.addEventListener("click", closeModal);
    });

    if (modal) {
        modal.addEventListener("click", (event) => {
            if (event.target === modal) {
                closeModal();
            }
        });
    }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initPageStats);
    } else {
        initPageStats();
    }
})();
