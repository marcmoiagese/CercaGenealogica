document.addEventListener('DOMContentLoaded', function () {
    // Aquí pots carregar les dades dinàmicament
    const arxiu = {
        nom: "Arxiu Parroquial de Vic",
        ubicacio: "Plaça Major, 1 – Vic",
        periode: "S.XVI – Avui dia",
        indexat: [
            { tipus: "Bateigs", periode: "1900–1950", punts: 50 },
            { tipus: "Casaments", periode: "1920–1970", punts: 50 },
            { tipus: "Defuncions", periode: "1930–1980", punts: 50 }
        ],
        pendents: [
            { tipus: "Bateigs", periode: "1580–1899", punts: 150 },
            { tipus: "Casaments", periode: "1600–1919", punts: 150 },
            { tipus: "Defuncions", periode: "1650–1929", punts: 150 }
        ]
    };

    document.querySelector('.hero h1').textContent = arxiu.nom;
});