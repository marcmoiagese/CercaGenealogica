document.addEventListener("DOMContentLoaded", () => {
    const key = `scroll:${window.location.pathname}`;
    const stored = sessionStorage.getItem(key);
    if (stored) {
        const value = parseInt(stored, 10);
        if (!Number.isNaN(value)) {
            window.scrollTo(0, value);
        }
        sessionStorage.removeItem(key);
        return;
    }
    if (window.location.hash) {
        const target = document.querySelector(window.location.hash);
        if (target) {
            requestAnimationFrame(() => {
                target.scrollIntoView({behavior: "auto", block: "start"});
            });
            return;
        }
    }

    window.addEventListener("beforeunload", () => {
        try {
            sessionStorage.setItem(key, String(window.scrollY || 0));
        } catch (_e) {
            // ignore storage errors
        }
    });
});
