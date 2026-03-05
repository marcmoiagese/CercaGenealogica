(function () {
    var ajudaButtons = document.querySelectorAll(".ajuda-boto");
    var modals = document.querySelectorAll(".modal-ajuda");
    var tipusSearch = document.getElementById("tipus_search");
    var tipusSelect = document.getElementById("tipus_nivell");
    var parentSearch = document.getElementById("parent_search");
    var parentSelect = document.getElementById("parent_id");

    ajudaButtons.forEach(function (btn) {
        btn.addEventListener("click", function () {
            var target = btn.getAttribute("aria-controls");
            var modal = document.getElementById(target);
            if (modal) {
                modal.hidden = false;
                modal.focus();
            }
        });
    });

    modals.forEach(function (modal) {
        modal.addEventListener("click", function (e) {
            if (e.target === modal) {
                modal.hidden = true;
            }
        });
        var close = modal.querySelector(".tancar-ajuda");
        if (close) {
            close.addEventListener("click", function () {
                modal.hidden = true;
            });
        }
    });

    if (tipusSearch && tipusSelect) {
        tipusSearch.addEventListener("input", function () {
            var q = tipusSearch.value.toLowerCase();
            for (var i = 0; i < tipusSelect.options.length; i += 1) {
                var opt = tipusSelect.options[i];
                if (!opt.value) {
                    opt.style.display = "";
                    continue;
                }
                opt.style.display = opt.textContent.toLowerCase().includes(q) ? "" : "none";
            }
        });
    }

    if (parentSearch && parentSelect) {
        parentSearch.addEventListener("input", function () {
            var q = parentSearch.value.toLowerCase();
            for (var i = 0; i < parentSelect.options.length; i += 1) {
                var opt = parentSelect.options[i];
                if (!opt.value) {
                    opt.style.display = "";
                    continue;
                }
                opt.style.display = opt.textContent.toLowerCase().includes(q) ? "" : "none";
            }
        });
    }
})();
