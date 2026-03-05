(function () {
    document.querySelectorAll("[data-tabs]").forEach(function (container) {
        var buttons = Array.from(container.querySelectorAll(".tab-boto[data-tab]"))
            .filter(function (btn) { return btn.closest("[data-tabs]") === container; });
        var panes = Array.from(container.querySelectorAll(".tab-pane[data-tab-panel]"))
            .filter(function (pane) { return pane.closest("[data-tabs]") === container; });
        if (!buttons.length || !panes.length) {
            return;
        }
        buttons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                var target = btn.dataset.tab;
                if (!target) {
                    return;
                }
                buttons.forEach(function (b) {
                    var active = b === btn;
                    b.classList.toggle("actiu", active);
                    b.setAttribute("aria-selected", active ? "true" : "false");
                });
                panes.forEach(function (pane) {
                    pane.classList.toggle("actiu", pane.dataset.tabPanel === target);
                });
            });
        });
    });
})();
