document.addEventListener("DOMContentLoaded", function () {
    var grid = document.querySelector("[data-kpis-api]");
    if (!grid) {
        return;
    }
    var api = grid.getAttribute("data-kpis-api");
    var errorEl = document.querySelector("[data-kpis-error]");
    var formatInt = new Intl.NumberFormat();
    var setValue = function (key, value) {
        var el = grid.querySelector('[data-kpi="' + key + '"]');
        if (!el) {
            return;
        }
        el.textContent = value;
    };
    var formatPct = function (value) {
        var num = Number(value) || 0;
        var text = num % 1 === 0 ? num.toFixed(0) : num.toFixed(1);
        return text + "%";
    };
    fetch(api, { headers: { "Accept": "application/json" } })
        .then(function (resp) {
            if (!resp.ok) {
                throw new Error("bad response");
            }
            return resp.json();
        })
        .then(function (data) {
            setValue("total_users", formatInt.format(data.total_users || 0));
            setValue("active_users", formatInt.format(data.active_users || 0));
            setValue("contributors_users", formatInt.format(data.contributors_users || 0));
            setValue("contributors_pct", formatPct(data.contributors_pct));
            setValue("validated_contributions", formatInt.format(data.validated_contributions || 0));
        })
        .catch(function () {
            if (errorEl) {
                errorEl.hidden = false;
            }
        });
});
