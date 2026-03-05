(function () {
    var cb = document.getElementById("accepta_donacions");
    var wrap = document.getElementById("donacionsUrlWrap");
    if (!cb || !wrap) {
        return;
    }
    var toggle = function () {
        wrap.style.display = cb.checked ? "" : "none";
    };
    cb.addEventListener("change", toggle);
    toggle();
})();
