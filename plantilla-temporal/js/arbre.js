class MyFamilyTree {
    constructor(containerId, options) {
        this.container = document.getElementById(containerId);
        this.nodes = options.data || [];
        this.spacingX = options.spacingX || 250;
        this.spacingY = options.spacingY || 150;
        this.scale = 1;

        this.init();
    }

    init() {
        // Aquí anirà el codi de generació d’arbre
        console.log("Arbre inicialitzat");
        this.drawNodes();
    }

    drawNodes() {
        console.log("Dibuixant nodes...", familyData);
        let root = this.nodes.find(n => !n.pid); // persona arrel
        if (!root) return console.error("No hi ha node arrel");

        this.addNodeToDOM(root, window.innerWidth / 2 - 80, 100);

        for (let node of this.nodes) {
            if (node.pid) {
                const parent = this.nodes.find(n => n.id === node.pid);
                if (parent) {
                    const x = parent.x + this.spacingX;
                    const y = parent.y + (this.nodes.filter(n => n.pid === parent.id).indexOf(node) * this.spacingY) || 0;
                    this.addNodeToDOM(node, x, y);
                }
            }
        }
    }

    addNodeToDOM(node, x, y) {
        const div = document.createElement('div');
        div.className = 'nod-arbre ' + (node.gender === 'male' ? 'masculi' : 'femeni');
        div.style.left = `${x}px`;
        div.style.top = `${y}px`;

        div.innerHTML = `
            <div class="foto"><i class="fas fa-${node.gender === 'male' ? 'mars' : 'venus'}"></i></div>
            <div class="nom">${node.name}</div>
            <div class="dates">${node.birth} - ${node.death || ''}</div>
        `;

        this.container.appendChild(div);
        node.x = x;
        node.y = y;
    }
}

function debugNodes() {
    const nodes = document.querySelectorAll('.nod-arbre');
    let x = 100;
    let y = 100;

    nodes.forEach((node, index) => {
        if (!node.style.left || !node.style.top) {
            node.style.position = 'absolute';
            node.style.left = `${x}px`;
            node.style.top = `${y}px`;
            x += 200;
            if ((index + 1) % 3 === 0) {
                x = 100;
                y += 150;
            }
        }
    });
}

document.addEventListener('DOMContentLoaded', function () {
    const zona = document.getElementById('arbreZona');
    const svgContainer = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svgContainer.style.position = "absolute";
    svgContainer.style.top = "0";
    svgContainer.style.left = "0";
    svgContainer.style.width = "100%";
    svgContainer.style.height = "100%";
    svgContainer.style.pointerEvents = "none";
    zona.appendChild(svgContainer);

    let scale = 1;
    let isDragging = false;
    let startX, startY, scrollLeft, scrollTop;

    // Dades de prova
    const familyData = [
        { id: 1, name: "Joan Moya", gender: "male", birth: "1985", death: "" },
        { id: 2, pid: 1, name: "Maria Riera", gender: "female", birth: "1960", death: "2020" },
        { id: 3, pid: 1, name: "Pere Moya", gender: "male", birth: "1958", death: "" },
        { id: 4, pid: 2, name: "Marc Moyà", gender: "male", birth: "1988", death: "" }
    ];

    const spacingX = 250;
    const spacingY = 150;

    function renderNodes() {
        zona.innerHTML = '';
        svgContainer.innerHTML = '';

        const root = familyData.find(n => !n.pid);
        if (!root) return console.error("No hi ha node arrel");

        root.x = window.innerWidth / 2 - 80;
        root.y = 200;
        drawNode(root);

        familyData.forEach(node => {
            if (node.pid && node.id !== root.id) {
                const parent = familyData.find(n => n.id === node.pid);
                if (parent) {
                    node.x = parent.x + spacingX;
                    node.y = parent.y + (familyData.filter(n => n.pid === parent.id).indexOf(node) * spacingY) || 0;
                    drawNode(node);
                    drawLine(parent, node);
                }
            }
        });
    }

    function drawNode(node) {
        const div = document.createElement('div');
        div.className = 'nod-arbre ' + (node.gender === 'male' ? 'masculi' : 'femeni');
        div.style.left = `${node.x}px`;
        div.style.top = `${node.y}px`;

        const icona = node.gender === 'male' ? 'mars' : 'venus';

        div.innerHTML = `
            <div class="foto"><i class="fas fa-${icona}"></i></div>
            <div class="nom">${node.name}</div>
            <div class="dates">${node.birth} - ${node.death || '...'}</div>
        `;

        zona.appendChild(div);
    }

    function drawLine(parent, child) {
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1", parent.x + 75);
        line.setAttribute("y1", parent.y + 75);
        line.setAttribute("x2", child.x + 75);
        line.setAttribute("y2", child.y + 75);
        line.setAttribute("stroke", "#aaa");
        line.setAttribute("stroke-width", "2");

        svgContainer.appendChild(line);
    }

    // Zoom
    document.getElementById('zoomIn').addEventListener('click', () => {
        scale += 0.2;
        if (scale > 2) scale = 2;
        zona.style.transform = `scale(${scale}) translate(-50%, -50%)`;
        svgContainer.style.transform = `scale(${scale}) translate(-50%, -50%)`;
    });

    document.getElementById('zoomOut').addEventListener('click', () => {
        scale -= 0.2;
        if (scale < 0.5) scale = 0.5;
        zona.style.transform = `scale(${scale}) translate(-50%, -50%)`;
        svgContainer.style.transform = `scale(${scale}) translate(-50%, -50%)`;
    });

    // Drag & move
    zona.addEventListener('mousedown', e => {
        isDragging = true;
        startX = e.clientX;
        startY = e.clientY;
        scrollLeft = zona.scrollLeft;
        scrollTop = zona.scrollTop;
        zona.style.cursor = 'grabbing';
    });

    zona.addEventListener('mouseup', () => {
        isDragging = false;
        zona.style.cursor = 'grab';
    });

    zona.addEventListener('mousemove', e => {
        if (!isDragging) return;
        e.preventDefault();
        zona.scrollLeft = scrollLeft - (e.clientX - startX);
        zona.scrollTop = scrollTop - (e.clientY - startY);
    });

    // Carrega inicial
    renderNodes();
});