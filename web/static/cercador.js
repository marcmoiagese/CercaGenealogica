document.addEventListener('DOMContentLoaded', function() {
    const cercaInput = document.getElementById('cerca');
    const resultatsDiv = document.getElementById('resultats');
    
    cercaInput.addEventListener('input', function() {
        const query = this.value.trim();
        
        if (query.length === 0) {
            resultatsDiv.innerHTML = '';
            return;
        }
        
        fetch(`/cerca?q=${encodeURIComponent(query)}`)
            .then(response => response.json())
            .then(data => {
                if (data.length === 0) {
                    resultatsDiv.innerHTML = '<div class="resultat-item">No s\'han trobat resultats</div>';
                    return;
                }
                
                let html = '';
                data.forEach(item => {
                    html += `<div class="resultat-item">
                        <strong>${item.nom}</strong> ${item.cognom1} ${item.cognom2}
                    </div>`;
                });
                
                resultatsDiv.innerHTML = html;
            })
            .catch(error => {
                console.error('Error:', error);
                resultatsDiv.innerHTML = '<div class="resultat-item">Error en la cerca</div>';
            });
    });
});