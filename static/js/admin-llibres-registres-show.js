(function () {
    document.querySelectorAll("[data-modal-open]").forEach(function (button) {
        button.addEventListener("click", function () {
            var target = document.getElementById(button.getAttribute("data-modal-open"));
            if (target) {
                target.classList.add("is-open");
            }
        });
    });
    document.querySelectorAll("[data-modal-close]").forEach(function (button) {
        button.addEventListener("click", function () {
            var target = document.getElementById(button.getAttribute("data-modal-close"));
            if (target) {
                target.classList.remove("is-open");
            }
        });
    });
    var botoOpcions = document.getElementById("botoOpcions");
    var dropdownOpcions = document.getElementById("dropdownOpcions");
    if (botoOpcions && dropdownOpcions) {
        botoOpcions.addEventListener("click", function (event) {
            event.preventDefault();
            dropdownOpcions.style.display = dropdownOpcions.style.display === "block" ? "none" : "block";
        });
        document.addEventListener("click", function (event) {
            if (!event.target.closest(".opcions-dropdown")) {
                dropdownOpcions.style.display = "none";
            }
        });
    }
})();
