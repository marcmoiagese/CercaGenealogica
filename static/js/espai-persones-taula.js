document.addEventListener("DOMContentLoaded", () => {
    const table = document.getElementById("espaiPersonesTable");
    if (!table) {
        return;
    }

    const filterRow = table.querySelector("#filtraFila");
    const inputs = filterRow
        ? Array.from(filterRow.querySelectorAll("input[data-key], select[data-key]"))
        : [];
    if (!inputs.length) {
        return;
    }

    const scrollKey = "espai.overview.scroll";
    const savedScroll = sessionStorage.getItem(scrollKey);
    if (savedScroll) {
        sessionStorage.removeItem(scrollKey);
        const y = parseInt(savedScroll, 10);
        if (!Number.isNaN(y) && y > 0) {
            window.requestAnimationFrame(() => window.scrollTo(0, y));
        }
    }

    const toggleForms = Array.from(table.querySelectorAll("form"));
    toggleForms.forEach((form) => {
        form.addEventListener("submit", () => {
            sessionStorage.setItem(scrollKey, String(window.scrollY || 0));
        });
    });

    let filterOrder = [];
    const storedOrder = (table.dataset.filterOrder || "")
        .split(",")
        .map((val) => val.trim())
        .filter(Boolean);
    if (storedOrder.length) {
        filterOrder = storedOrder;
    }

    function updateFilterOrder(input) {
        const key = input.dataset.key || "";
        if (!key) {
            return;
        }
        const value = input.value.trim();
        const index = filterOrder.indexOf(key);
        if (value && index === -1) {
            filterOrder.push(key);
        }
        if (!value && index !== -1) {
            filterOrder.splice(index, 1);
        }
    }

    let debounceTimer = null;
    const triggerReload = () => {
        sessionStorage.setItem(scrollKey, String(window.scrollY || 0));
        const params = new URLSearchParams(window.location.search);
        params.delete("page");
        inputs.forEach((input) => {
            updateFilterOrder(input);
            const key = input.dataset.key;
            const value = input.value.trim();
            const paramKey = `f_${key}`;
            if (value) {
                params.set(paramKey, value);
            } else {
                params.delete(paramKey);
            }
        });
        const ordered = filterOrder.filter((key) => params.get(`f_${key}`));
        if (ordered.length) {
            params.set("order", ordered.join(","));
        } else {
            params.delete("order");
        }
        const query = params.toString();
        const url = query ? `${window.location.pathname}?${query}` : window.location.pathname;
        window.location.assign(url);
    };

    inputs.forEach((input) => {
        if (input.tagName === "SELECT") {
            input.addEventListener("change", triggerReload);
            return;
        }
        input.addEventListener("input", () => {
            if (debounceTimer) {
                window.clearTimeout(debounceTimer);
            }
            debounceTimer = window.setTimeout(triggerReload, 350);
        });
    });
});
