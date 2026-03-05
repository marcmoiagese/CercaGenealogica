(function () {
    var modal = document.getElementById("purge-modal");
    if (!modal) {
        return;
    }
    var form = document.getElementById("purge-form");
    var title = document.getElementById("purge-modal-book");
    var closeButtons = modal.querySelectorAll("[data-close-purge]");
    var openButtons = document.querySelectorAll(".purge-trigger");
    var actionTemplate = "/documentals/llibres/__ID__/registres/purge";

    openButtons.forEach(function (btn) {
        btn.addEventListener("click", function () {
            var id = btn.getAttribute("data-llibre-id");
            var label = btn.getAttribute("data-llibre-title") || "";
            if (form) {
                form.action = actionTemplate.replace("__ID__", id || "");
            }
            if (title) {
                title.textContent = label;
            }
            modal.classList.add("is-open");
            var input = modal.querySelector("#purge-password");
            if (input) {
                input.value = "";
                input.focus();
            }
        });
    });

    closeButtons.forEach(function (btn) {
        btn.addEventListener("click", function () {
            modal.classList.remove("is-open");
        });
    });

    modal.addEventListener("click", function (event) {
        if (event.target === modal) {
            modal.classList.remove("is-open");
        }
    });
})();
