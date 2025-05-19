document.addEventListener('DOMContentLoaded', function () {
    const checkboxes = document.querySelectorAll('.duplicate-checkbox');
    const selectedIdsInput = document.getElementById('selectedIds');
    const form = document.getElementById('accioForm');

    // Actualitzar valors ocults quan es marca un checkbox
    checkboxes.forEach(cb => {
        cb.addEventListener('change', function () {
            updateSelectedIds();
        });
    });

    // Funció per recollir els IDs seleccionats
    function updateSelectedIds() {
        const selected = [];
        checkboxes.forEach(cb => {
            if (cb.checked) {
                selected.push(cb.getAttribute('data-id'));
            }
        });
        selectedIdsInput.value = selected.join(',');
    }

    // Assignem acció als botons
    window.setAction = function (url) {
        form.action = url;
        return true; // Permetre submit
    };
});

document.addEventListener('DOMContentLoaded', function () {
    const selectAllCheckbox = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.duplicate-checkbox');

    if (selectAllCheckbox && checkboxes.length > 0) {
        // Marca o desmarca tots els checkboxes quan es marca 'selectAll'
        selectAllCheckbox.addEventListener('change', function () {
            checkboxes.forEach(cb => {
                cb.checked = selectAllCheckbox.checked;
            });
        });

        // Si tots estan marcats, marca també 'selectAll'
        checkboxes.forEach(cb => {
            cb.addEventListener('change', function () {
                const allChecked = Array.from(checkboxes).every(cb => cb.checked);
                selectAllCheckbox.checked = allChecked;
            });
        });
    }
});