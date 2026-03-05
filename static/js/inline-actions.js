(function () {
    function closestWithAttr(el, attr) {
        if (!el || !el.closest) {
            return null;
        }
        return el.closest("[" + attr + "]");
    }

    document.addEventListener("click", function (event) {
        var nav = closestWithAttr(event.target, "data-nav-href");
        if (nav) {
            var href = nav.getAttribute("data-nav-href");
            if (href) {
                event.preventDefault();
                window.location.href = href;
            }
            return;
        }

        var closeBtn = closestWithAttr(event.target, "data-window-close");
        if (closeBtn) {
            event.preventDefault();
            window.close();
        }
    });

    document.addEventListener("change", function (event) {
        var submitEl = closestWithAttr(event.target, "data-submit-form");
        if (submitEl) {
            var form = submitEl.form || submitEl.closest("form");
            if (form) {
                form.submit();
            }
            return;
        }

        var navSelect = closestWithAttr(event.target, "data-nav-base");
        if (navSelect) {
            var base = navSelect.getAttribute("data-nav-base") || "";
            var anchor = navSelect.getAttribute("data-nav-anchor") || "";
            window.location.href = base + navSelect.value + anchor;
        }
    });

    document.addEventListener("submit", function (event) {
        var form = event.target;
        if (!form || !form.matches || !form.matches("[data-confirm-message]")) {
            return;
        }
        var msg = form.getAttribute("data-confirm-message");
        if (msg && !window.confirm(msg)) {
            event.preventDefault();
        }
    });
})();
