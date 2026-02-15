document.addEventListener("DOMContentLoaded", function () {
    var banner = document.querySelector(".maintenance-banner");
    if (!banner) {
        return;
    }
    var dismissible = banner.dataset.dismissible === "1";
    var windowId = banner.dataset.windowId || "unknown";
    var storageKey = "maintenance_dismissed_" + windowId;
    if (dismissible && window.localStorage) {
        try {
            if (window.localStorage.getItem(storageKey) === "1") {
                banner.remove();
                return;
            }
        } catch (err) {
            // Ignore storage errors (privacy mode).
        }
    }
    var closeBtn = banner.querySelector("[data-maintenance-close]");
    if (!dismissible || !closeBtn) {
        return;
    }
    closeBtn.addEventListener("click", function () {
        if (window.localStorage) {
            try {
                window.localStorage.setItem(storageKey, "1");
            } catch (err) {
                // Ignore storage errors (privacy mode).
            }
        }
        banner.remove();
    });
});
