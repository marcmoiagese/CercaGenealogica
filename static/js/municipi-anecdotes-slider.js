(function () {
    const slider = document.getElementById("muniAnecdoteSlider");
    if (!slider) return;

    const track = document.getElementById("muniAnecdoteTrack");
    const btnPrev = document.getElementById("muniAnecdotePrev");
    const btnNext = document.getElementById("muniAnecdoteNext");
    const dotsEl = document.getElementById("muniAnecdoteDots");
    const api = slider.dataset.api || "";
    const detailBase = slider.dataset.detailBase || "";
    const emptyLabel = slider.dataset.empty || "";
    const loadingLabel = slider.dataset.loading || "";
    const readLabel = slider.dataset.read || "Read";

    if (!track || !api) return;

    let slides = [];
    let dots = [];
    let currentIdx = 0;
    let timer = null;

    const renderMessage = (text) => {
        track.innerHTML = "";
        const msg = document.createElement("div");
        msg.className = "muni-anecdote-empty";
        msg.textContent = text;
        track.appendChild(msg);
        if (dotsEl) dotsEl.innerHTML = "";
    };

    const setActive = (idx) => {
        if (!slides.length) return;
        const safeIdx = ((idx % slides.length) + slides.length) % slides.length;
        slides.forEach((slide, i) => {
            slide.classList.toggle("is-active", i === safeIdx);
        });
        dots.forEach((dot, i) => {
            dot.classList.toggle("is-active", i === safeIdx);
        });
        currentIdx = safeIdx;
    };

    const nextSlide = () => setActive(currentIdx + 1);
    const prevSlide = () => setActive(currentIdx - 1);

    const stopAuto = () => {
        if (timer) {
            clearInterval(timer);
            timer = null;
        }
    };

    const startAuto = () => {
        stopAuto();
        timer = setInterval(nextSlide, 7000);
    };

    if (loadingLabel) {
        renderMessage(loadingLabel);
    }

    fetch(`${api}?limit=6`)
        .then((res) => res.json())
        .then((data) => {
            const items = Array.isArray(data.items) ? data.items : [];
            if (!items.length) {
                renderMessage(emptyLabel || "");
                return;
            }
            track.innerHTML = "";
            if (dotsEl) dotsEl.innerHTML = "";
            slides = [];
            dots = [];
            items.forEach((item, idx) => {
                const title = item.title || "";
                const tag = item.tag_label || item.tag || "";
                const date = item.data_ref || "";
                const snippet = item.snippet || "";
                const link = detailBase ? `${detailBase}${item.item_id}` : "#";
                const slide = document.createElement("article");
                slide.className = "muni-anecdote-slide";
                const meta = document.createElement("div");
                meta.className = "muni-anecdote-meta";
                if (tag) {
                    const tagEl = document.createElement("span");
                    tagEl.className = "muni-anecdote-tag";
                    tagEl.textContent = tag;
                    meta.appendChild(tagEl);
                }
                if (date) {
                    const dateEl = document.createElement("span");
                    dateEl.className = "muni-anecdote-date";
                    dateEl.textContent = date;
                    meta.appendChild(dateEl);
                }
                const titleEl = document.createElement("h3");
                titleEl.className = "muni-anecdote-title";
                titleEl.textContent = title;
                slide.appendChild(meta);
                slide.appendChild(titleEl);
                if (snippet) {
                    const snippetEl = document.createElement("p");
                    snippetEl.className = "muni-anecdote-snippet";
                    snippetEl.textContent = snippet;
                    slide.appendChild(snippetEl);
                }
                const linkEl = document.createElement("a");
                linkEl.className = "muni-anecdote-link";
                linkEl.href = link;
                linkEl.textContent = readLabel;
                slide.appendChild(linkEl);
                track.appendChild(slide);
                slides.push(slide);
                if (dotsEl) {
                    const dot = document.createElement("button");
                    dot.type = "button";
                    dot.setAttribute("aria-label", `Slide ${idx + 1}`);
                    dot.addEventListener("click", () => {
                        setActive(idx);
                        startAuto();
                    });
                    dotsEl.appendChild(dot);
                    dots.push(dot);
                }
            });
            setActive(0);
            if (slides.length > 1) {
                startAuto();
            }
        })
        .catch(() => {
            renderMessage(emptyLabel || "");
        });

    if (btnPrev) {
        btnPrev.addEventListener("click", () => {
            prevSlide();
            startAuto();
        });
    }
    if (btnNext) {
        btnNext.addEventListener("click", () => {
            nextSlide();
            startAuto();
        });
    }

    slider.addEventListener("mouseenter", stopAuto);
    slider.addEventListener("mouseleave", startAuto);
    slider.addEventListener("focusin", stopAuto);
    slider.addEventListener("focusout", startAuto);
})();
