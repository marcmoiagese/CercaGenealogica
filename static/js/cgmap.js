(function () {
    const NS = "http://www.w3.org/2000/svg";

    function create(tag) {
        return document.createElementNS(NS, tag);
    }

    function clear(svg) {
        while (svg.firstChild) {
            svg.removeChild(svg.firstChild);
        }
    }

    function toPoints(points) {
        if (!Array.isArray(points)) return "";
        const out = [];
        points.forEach((pt) => {
            if (!Array.isArray(pt) || pt.length < 2) return;
            const x = Number(pt[0]);
            const y = Number(pt[1]);
            if (Number.isFinite(x) && Number.isFinite(y)) {
                out.push(x + "," + y);
            }
        });
        return out.join(" ");
    }

    function centroid(points) {
        if (!Array.isArray(points) || points.length === 0) return { x: 0, y: 0 };
        let x = 0;
        let y = 0;
        let n = 0;
        points.forEach((pt) => {
            if (!Array.isArray(pt) || pt.length < 2) return;
            x += Number(pt[0]) || 0;
            y += Number(pt[1]) || 0;
            n += 1;
        });
        if (n === 0) return { x: 0, y: 0 };
        return { x: x / n, y: y / n };
    }

    function markSelectable(node, layer, index) {
        if (!node) return;
        node.setAttribute("data-layer-item", layer);
        node.setAttribute("data-index", String(index));
    }

    function renderHouses(svg, houses) {
        const group = create("g");
        group.setAttribute("data-layer", "houses");
        if (Array.isArray(houses)) {
            houses.forEach((house, idx) => {
                const points = toPoints(house.points);
                if (!points) return;
                const wrap = create("g");
                markSelectable(wrap, "houses", idx);
                const poly = create("polygon");
                poly.setAttribute("points", points);
                poly.setAttribute("class", "cg-house");
                markSelectable(poly, "houses", idx);
                wrap.appendChild(poly);

                if (house.label) {
                    wrap.setAttribute("data-tooltip", String(house.label));
                    wrap.setAttribute("data-sub", "Casa");
                    const label = create("text");
                    const center = centroid(house.points);
                    label.setAttribute("x", center.x);
                    label.setAttribute("y", center.y);
                    label.setAttribute("class", "cg-house-label");
                    label.setAttribute("text-anchor", "middle");
                    label.setAttribute("dominant-baseline", "middle");
                    label.textContent = house.label;
                    wrap.appendChild(label);
                }
                group.appendChild(wrap);
            });
        }
        svg.appendChild(group);
    }

    function streetKindClass(kind) {
        const k = String(kind || "").toLowerCase();
        switch (k) {
            case "asfaltat":
                return "cg-street--asfaltat";
            case "empedrat":
                return "cg-street--empedrat";
            case "terra":
                return "cg-street--terra";
            case "carretera":
                return "cg-street--carretera";
            case "autopista":
                return "cg-street--autopista";
            default:
                return "cg-street--default";
        }
    }

    function renderStreets(svg, streets) {
        const group = create("g");
        group.setAttribute("data-layer", "streets");
        if (Array.isArray(streets)) {
            streets.forEach((street, idx) => {
                const points = toPoints(street.points);
                if (!points) return;
                const wrap = create("g");
                markSelectable(wrap, "streets", idx);
                const line = create("polyline");
                line.setAttribute("points", points);
                line.setAttribute("class", "cg-street " + streetKindClass(street.kind));
                markSelectable(line, "streets", idx);
                wrap.appendChild(line);
                if (street.label) {
                    wrap.setAttribute("data-tooltip", String(street.label));
                    wrap.setAttribute("data-sub", "Carrer");
                    const mid = centroid(street.points);
                    const label = create("text");
                    label.setAttribute("x", mid.x);
                    label.setAttribute("y", mid.y);
                    label.setAttribute("class", "cg-street-label");
                    label.setAttribute("text-anchor", "middle");
                    label.setAttribute("dominant-baseline", "middle");
                    label.textContent = street.label;
                    wrap.appendChild(label);
                }
                group.appendChild(wrap);
            });
        }
        svg.appendChild(group);
    }

    function riverKindClass(kind) {
        const k = String(kind || "").toLowerCase();
        switch (k) {
            case "riera":
                return "cg-river--riera";
            case "rierol":
                return "cg-river--rierol";
            case "riu":
            default:
                return "cg-river--riu";
        }
    }

    function renderRivers(svg, rivers) {
        const group = create("g");
        group.setAttribute("data-layer", "rivers");
        if (Array.isArray(rivers)) {
            rivers.forEach((river, idx) => {
                const points = toPoints(river.points);
                if (!points) return;
                const wrap = create("g");
                markSelectable(wrap, "rivers", idx);
                const line = create("polyline");
                line.setAttribute("points", points);
                line.setAttribute("class", "cg-river " + riverKindClass(river.kind));
                markSelectable(line, "rivers", idx);
                wrap.appendChild(line);
                if (river.label) {
                    wrap.setAttribute("data-tooltip", String(river.label));
                    wrap.setAttribute("data-sub", "Riu");
                }
                group.appendChild(wrap);
            });
        }
        svg.appendChild(group);
    }

    function elementKindClass(kind) {
        const k = String(kind || "").toLowerCase();
        switch (k) {
            case "fountain":
                return "cg-element--fountain";
            case "well":
                return "cg-element--well";
            case "bench":
                return "cg-element--bench";
            case "tree":
            default:
                return "cg-element--tree";
        }
    }

    function renderElements(svg, elements) {
        const group = create("g");
        group.setAttribute("data-layer", "elements");
        if (Array.isArray(elements)) {
            elements.forEach((item, idx) => {
                const x = Number(item.x);
                const y = Number(item.y);
                if (!Number.isFinite(x) || !Number.isFinite(y)) return;
                const wrap = create("g");
                markSelectable(wrap, "elements", idx);
                const kind = elementKindClass(item.kind);
                let node = null;
                if (item.kind === "bench") {
                    node = create("rect");
                    node.setAttribute("x", x - 10);
                    node.setAttribute("y", y - 4);
                    node.setAttribute("width", 20);
                    node.setAttribute("height", 8);
                    node.setAttribute("rx", 3);
                } else if (item.kind === "well") {
                    node = create("rect");
                    node.setAttribute("x", x - 7);
                    node.setAttribute("y", y - 7);
                    node.setAttribute("width", 14);
                    node.setAttribute("height", 14);
                    node.setAttribute("rx", 4);
                } else {
                    node = create("circle");
                    node.setAttribute("cx", x);
                    node.setAttribute("cy", y);
                    node.setAttribute("r", 7);
                }
                node.setAttribute("class", "cg-element " + kind);
                markSelectable(node, "elements", idx);
                wrap.appendChild(node);

                if (item.label) {
                    wrap.setAttribute("data-tooltip", String(item.label));
                    wrap.setAttribute("data-sub", "Element");
                    const label = create("text");
                    label.setAttribute("x", x + 12);
                    label.setAttribute("y", y + 6);
                    label.setAttribute("class", "cg-toponym-label");
                    label.textContent = item.label;
                    markSelectable(label, "elements", idx);
                    wrap.appendChild(label);
                }

                group.appendChild(wrap);
            });
        }
        svg.appendChild(group);
    }

    function renderToponyms(svg, toponyms) {
        const group = create("g");
        group.setAttribute("data-layer", "toponyms");
        if (Array.isArray(toponyms)) {
            toponyms.forEach((item, idx) => {
                const x = Number(item.x);
                const y = Number(item.y);
                if (!Number.isFinite(x) || !Number.isFinite(y)) return;
                const wrap = create("g");
                markSelectable(wrap, "toponyms", idx);
                if (item.label) {
                    wrap.setAttribute("data-tooltip", String(item.label));
                    wrap.setAttribute("data-sub", "Toponim");
                }
                if (item.kind === "marker") {
                    const dot = create("circle");
                    dot.setAttribute("cx", x);
                    dot.setAttribute("cy", y);
                    dot.setAttribute("r", 8);
                    dot.setAttribute("class", "cg-marker");
                    markSelectable(dot, "toponyms", idx);
                    wrap.appendChild(dot);
                }
                if (item.label) {
                    const label = create("text");
                    label.setAttribute("x", x + 12);
                    label.setAttribute("y", y + 6);
                    label.setAttribute("class", "cg-toponym-label");
                    label.textContent = item.label;
                    markSelectable(label, "toponyms", idx);
                    wrap.appendChild(label);
                }
                group.appendChild(wrap);
            });
        }
        svg.appendChild(group);
    }

    function renderBounds(svg, bounds) {
        const group = create("g");
        group.setAttribute("data-layer", "bounds");
        if (Array.isArray(bounds)) {
            bounds.forEach((boundary, idx) => {
                const points = toPoints(boundary.points);
                if (!points) return;
                const poly = create("polygon");
                poly.setAttribute("points", points);
                poly.setAttribute("class", "cg-boundary");
                markSelectable(poly, "bounds", idx);
                group.appendChild(poly);
            });
        }
        svg.appendChild(group);
    }

    function render(svg, model) {
        if (!svg) return;
        clear(svg);
        const data = model || {};
        const viewBox = Array.isArray(data.viewBox) && data.viewBox.length === 4 ? data.viewBox : [0, 0, 1000, 700];
        svg.setAttribute("viewBox", viewBox.join(" "));
        svg.setAttribute("preserveAspectRatio", "xMidYMid meet");

        const layers = data.layers && typeof data.layers === "object" ? data.layers : {};
        renderBounds(svg, layers.bounds || []);
        renderStreets(svg, layers.streets || []);
        renderRivers(svg, layers.rivers || []);
        renderHouses(svg, layers.houses || []);
        renderElements(svg, layers.elements || []);
        renderToponyms(svg, layers.toponyms || []);
    }

    window.CGMap = {
        render: render,
    };
})();
