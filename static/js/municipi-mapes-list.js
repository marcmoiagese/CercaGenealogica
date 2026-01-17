(function () {
    const root = document.getElementById("mapesList");
    if (!root) return;
    const municipiId = root.dataset.municipiId;
    const canEdit = root.dataset.canEdit === "1";
    const labels = {
        actual: root.dataset.labelActual || "Actual",
        historic: root.dataset.labelHistoric || "Historic",
        community: root.dataset.labelCommunity || "Community",
    };
    const tbody = document.getElementById("mapesListBody");
    const form = document.getElementById("mapesCreateForm");
    const statusEl = document.getElementById("mapesCreateStatus");
    const csrf = root.dataset.csrf;

    function groupLabel(val) {
        return labels[val] || val;
    }

    function setStatus(msg) {
        if (statusEl) statusEl.textContent = msg || "";
    }

    function render(items) {
        if (!tbody) return;
        tbody.innerHTML = "";
        if (!items || !items.length) {
            const tr = document.createElement("tr");
            const td = document.createElement("td");
            td.colSpan = 4;
            td.textContent = "—";
            tr.appendChild(td);
            tbody.appendChild(tr);
            return;
        }
        items.forEach((item) => {
            const tr = document.createElement("tr");
            const title = document.createElement("td");
            title.textContent = item.title || "-";

            const group = document.createElement("td");
            group.textContent = groupLabel(item.group_type || "-");

            const period = document.createElement("td");
            period.textContent = item.period_label || "-";

            const actions = document.createElement("td");
            const viewLink = document.createElement("a");
            viewLink.className = "boto-secundari btn-mini";
            viewLink.href = "/territori/municipis/" + municipiId + "/mapes/" + item.id;
            viewLink.textContent = "Veure";
            actions.appendChild(viewLink);

            if (canEdit) {
                const editLink = document.createElement("a");
                editLink.className = "boto-secundari btn-mini";
                editLink.href = "/territori/municipis/" + municipiId + "/mapes/" + item.id + "/editor";
                editLink.textContent = "Editar";
                actions.appendChild(editLink);
            }

            tr.appendChild(title);
            tr.appendChild(group);
            tr.appendChild(period);
            tr.appendChild(actions);
            tbody.appendChild(tr);
        });
    }

    function load() {
        if (!municipiId) return;
        fetch("/api/municipis/" + municipiId + "/mapes")
            .then((res) => res.json())
            .then((payload) => render(payload.items || []))
            .catch(() => render([]));
    }

    if (form) {
        form.addEventListener("submit", (evt) => {
            evt.preventDefault();
            const payload = {
                title: form.title.value,
                group_type: form.group_type.value,
                period_label: form.period_label.value,
                topic: form.topic.value,
                csrf_token: csrf,
            };
            setStatus("Desant...");
            fetch("/api/municipis/" + municipiId + "/mapes", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            })
                .then(async (res) => {
                    if (!res.ok) {
                        const text = await res.text();
                        throw new Error(text || "Error creant.");
                    }
                    return res.json();
                })
                .then(() => {
                    setStatus("Creat.");
                    form.reset();
                    load();
                })
                .catch((err) => setStatus(err && err.message ? err.message : "Error creant."));
        });
    }

    load();
})();
