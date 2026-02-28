/* Persona Pro - tabs + timeline filters + carousel + documents quick filter */
document.addEventListener('DOMContentLoaded', () => {
  // --- Tabs (panells) ---
  const tabs = Array.from(document.querySelectorAll('.persona-tabs .tab'));
  const panels = Array.from(document.querySelectorAll('.persona-panel'));

  function activateTab(targetId, pushHash = true) {
    tabs.forEach(t => t.classList.toggle('is-active', t.dataset.target === targetId));
    panels.forEach(p => p.classList.toggle('is-active', p.id === targetId));

    if (pushHash) {
      history.replaceState(null, '', `#${targetId}`);
    }
  }

  tabs.forEach(t => {
    t.addEventListener('click', () => activateTab(t.dataset.target));
  });

  // Inicial: hash o primer tab
  const initial = (location.hash || '').replace('#', '');
  if (initial && panels.some(p => p.id === initial)) activateTab(initial, false);

  // --- Timeline filters ---
  const filterBtns = Array.from(document.querySelectorAll('.filters .filter'));
  const timelineEvents = Array.from(document.querySelectorAll('.timeline .t-event'));

  function setFilter(type) {
    filterBtns.forEach(b => b.classList.toggle('is-active', b.dataset.filter === type));
    timelineEvents.forEach(ev => {
      const evType = ev.dataset.type || '';
      const show = (type === 'all') || (evType === type);
      ev.style.display = show ? '' : 'none';
    });
  }

  filterBtns.forEach(b => b.addEventListener('click', () => setFilter(b.dataset.filter)));

  // --- Documents (search + type) ---
  const docSearch = document.querySelector('.doc-search');
  const docSelect = document.querySelector('.doc-select');
  const docCards = Array.from(document.querySelectorAll('.doc-card'));

  function norm(s){ return (s || '').toLowerCase().trim(); }

  function filterDocs() {
    const q = norm(docSearch?.value);
    const t = norm(docSelect?.value || 'all');

    docCards.forEach(card => {
      const text = norm(card.innerText);
      const type = norm(card.getAttribute('data-type') || 'all');
      const okQ = !q || text.includes(q);
      const okT = (t === 'all') || (type === t);
      card.style.display = (okQ && okT) ? '' : 'none';
    });
  }

  if (docSearch) docSearch.addEventListener('input', filterDocs);
  if (docSelect) docSelect.addEventListener('change', filterDocs);

  // --- Carousel (anecdotes) ---
  const carousel = document.querySelector('[data-carousel]');
  if (carousel) {
    const track = carousel.querySelector('[data-carousel-track]');
    const slides = Array.from(track.querySelectorAll('.slide'));
    const prevBtn = carousel.querySelector('[data-carousel-prev]');
    const nextBtn = carousel.querySelector('[data-carousel-next]');
    const dotsWrap = carousel.querySelector('[data-carousel-dots]');

    let index = slides.findIndex(s => s.classList.contains('is-active'));
    if (index < 0) index = 0;

    function renderDots() {
      if (!dotsWrap) return;
      dotsWrap.innerHTML = '';
      slides.forEach((_, i) => {
        const dot = document.createElement('button');
        dot.type = 'button';
        dot.className = 'carousel__dot' + (i === index ? ' is-active' : '');
        dot.setAttribute('aria-label', `Anecdote ${i + 1}`);
        dot.addEventListener('click', () => goTo(i));
        dotsWrap.appendChild(dot);
      });
    }

    function update() {
      slides.forEach((s, i) => s.classList.toggle('is-active', i === index));
      track.style.transform = `translateX(${-index * 100}%)`;
      renderDots();
    }

    function goTo(i) {
      index = (i + slides.length) % slides.length;
      update();
      restartAuto();
    }

    prevBtn?.addEventListener('click', () => goTo(index - 1));
    nextBtn?.addEventListener('click', () => goTo(index + 1));

    // Auto-play (pausa hover)
    let timer = null;
    function startAuto() {
      stopAuto();
      timer = setInterval(() => goTo(index + 1), 8000);
    }
    function stopAuto() {
      if (timer) clearInterval(timer);
      timer = null;
    }
    function restartAuto() { startAuto(); }

    carousel.addEventListener('mouseenter', stopAuto);
    carousel.addEventListener('mouseleave', startAuto);

    // Keyboard
    carousel.setAttribute('tabindex', '0');
    carousel.addEventListener('keydown', (e) => {
      if (e.key === 'ArrowLeft') goTo(index - 1);
      if (e.key === 'ArrowRight') goTo(index + 1);
    });

    update();
    startAuto();
  }

});
