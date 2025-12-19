document.addEventListener('DOMContentLoaded', function () {
    const checkboxes = document.querySelectorAll('.revisio-check');
    const botoComparar = document.querySelector('.boto-comparar');
    const botoRevertir = document.querySelector('.boto-revertir');
    const infoSeleccio = document.querySelector('.seleccio-info p');

    // Funció per actualitzar estat dels botons i mostrar nombre de versions seleccionades
    function actualitzarBotons() {
        const seleccionats = document.querySelectorAll('.revisio-check:checked');
        
        // Limitar a com a molt 2 versions seleccionades
        if (seleccionats.length > 2) {
            alert("Màxim pots seleccionar 2 versions per comparar.");
            this.checked = false; // Desmarquem la darrera seleccionada
            return;
        }

        // Actualitzem botons
        botoComparar.disabled = seleccionats.length !== 2;
        botoRevertir.disabled = seleccionats.length !== 1;

        // Mostra el nombre de versions seleccionades
        infoSeleccio.innerHTML = `<strong>Versions seleccionades:</strong> ${seleccionats.length}`;
    }

    // Afegim event listener a cada checkbox
    checkboxes.forEach(box => {
        box.addEventListener('change', function () {
            actualitzarBotons.call(this); // 'this' és el checkbox actual
        });
    });

    // Inicialitzem estat inicial
    actualitzarBotons.call(null);
});