(function () {
    var cb = document.getElementById("accepta_donacions");
    var wrap = document.getElementById("donacionsUrlWrap");
    if (!cb || !wrap) {
        return;
    }
    var toggle = function () {
        wrap.hidden = !cb.checked;
    };
    cb.addEventListener("change", toggle);
    toggle();
})();
