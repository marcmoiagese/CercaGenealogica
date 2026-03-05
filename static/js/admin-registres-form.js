(function () {
    var personContainer = document.getElementById("persones-container");
    var attrContainer = document.getElementById("atributs-container");
    var addPerson = document.getElementById("add-person");
    var addAttr = document.getElementById("add-attr");
    if (!personContainer || !attrContainer || !addPerson || !addAttr) {
        return;
    }

    var form = personContainer.closest("form");
    var selectLabel = form ? form.getAttribute("data-select-label") : "";
    var sexMaleLabel = form ? form.getAttribute("data-sex-male-label") : "";
    var sexFemaleLabel = form ? form.getAttribute("data-sex-female-label") : "";

    var readJSON = function (id, fallback) {
        var el = document.getElementById(id);
        if (!el || !el.textContent) {
            return fallback;
        }
        try {
            return JSON.parse(el.textContent);
        } catch (err) {
            return fallback;
        }
    };

    var qualityOptions = readJSON("registres-quality-options", []);
    var qualityLabels = readJSON("registres-quality-labels", {});
    var typeOptions = readJSON("registres-type-options", []);
    var roleOptions = readJSON("registres-role-options", []);

    function qualitySelect(name) {
        var select = document.createElement("select");
        select.name = name;
        qualityOptions.forEach(function (opt) {
            var o = document.createElement("option");
            o.value = opt;
            o.textContent = qualityLabels[opt] || opt;
            select.appendChild(o);
        });
        return select;
    }

    function roleSelect(name) {
        var select = document.createElement("select");
        select.name = name;
        var emptyOpt = document.createElement("option");
        emptyOpt.value = "";
        emptyOpt.textContent = selectLabel || "";
        select.appendChild(emptyOpt);
        roleOptions.forEach(function (opt) {
            var o = document.createElement("option");
            o.value = opt.Value;
            o.textContent = opt.Label || opt.Value;
            select.appendChild(o);
        });
        return select;
    }

    function sexSelect(name) {
        var select = document.createElement("select");
        select.name = name;
        var emptyOpt = document.createElement("option");
        emptyOpt.value = "";
        emptyOpt.textContent = selectLabel || "";
        select.appendChild(emptyOpt);
        var optM = document.createElement("option");
        optM.value = "masculi";
        optM.textContent = sexMaleLabel || "masculi";
        select.appendChild(optM);
        var optF = document.createElement("option");
        optF.value = "femeni";
        optF.textContent = sexFemaleLabel || "femeni";
        select.appendChild(optF);
        return select;
    }

    addPerson.addEventListener("click", function () {
        var row = document.createElement("tr");
        row.innerHTML =
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_nom\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_cognom1\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_cognom2\"></td>" +
            "<td></td>" +
            "<td></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_edat\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_estat_civil\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_municipi\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_ofici\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_casa\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"person_notes\"></td>";
        row.children[0].appendChild(roleSelect("person_rol"));
        row.children[2].appendChild(qualitySelect("person_nom_estat"));
        row.children[4].appendChild(qualitySelect("person_cognom1_estat"));
        row.children[6].appendChild(qualitySelect("person_cognom2_estat"));
        row.children[7].appendChild(sexSelect("person_sexe"));
        row.children[8].appendChild(qualitySelect("person_sexe_estat"));
        row.children[10].appendChild(qualitySelect("person_edat_estat"));
        row.children[12].appendChild(qualitySelect("person_estat_civil_estat"));
        row.children[14].appendChild(qualitySelect("person_municipi_estat"));
        row.children[16].appendChild(qualitySelect("person_ofici_estat"));
        row.children[18].appendChild(qualitySelect("person_casa_estat"));
        personContainer.appendChild(row);
    });

    addAttr.addEventListener("click", function () {
        var row = document.createElement("tr");
        var typeSelect = document.createElement("select");
        typeSelect.name = "attr_type";
        typeOptions.forEach(function (opt) {
            var o = document.createElement("option");
            o.value = opt;
            o.textContent = opt;
            typeSelect.appendChild(o);
        });
        row.innerHTML =
            "<td><input type=\"text\" name=\"attr_key\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"attr_value\"></td>" +
            "<td></td>" +
            "<td><input type=\"text\" name=\"attr_notes\"></td>";
        row.children[1].appendChild(typeSelect);
        row.children[3].appendChild(qualitySelect("attr_state"));
        attrContainer.appendChild(row);
    });
})();
