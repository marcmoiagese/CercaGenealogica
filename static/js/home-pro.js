document.addEventListener('DOMContentLoaded', function () {
  const loginBtn = document.getElementById('ctaOpenLogin');
  const loginBtn2 = document.getElementById('ctaOpenLogin2');
  const regBtn = document.getElementById('ctaOpenRegister');
  const regBtn2 = document.getElementById('ctaOpenRegister2');

  function trigger(id){
    const el = document.getElementById(id);
    if (el) el.click();
  }

  function bind(anchor, targetId){
    if (!anchor) return;
    anchor.addEventListener('click', function (e) {
      e.preventDefault();
      trigger(targetId);
    });
  }

  bind(loginBtn, 'botoLogin');
  bind(loginBtn2, 'botoLogin');
  bind(regBtn, 'botoRegistre');
  bind(regBtn2, 'botoRegistre');
});



(function(){
  function qs(sel, root){ return (root||document).querySelector(sel); }
  function qsa(sel, root){ return Array.from((root||document).querySelectorAll(sel)); }

  function initShowcaseSlider(){
    const radios = qsa('input.showcase-radio[name="showcase"]');
    if (!radios.length) return;

    const slider = qs('.showcase-slider');
    const prevBtn = slider ? qs('[data-showcase="prev"]', slider) : null;
    const nextBtn = slider ? qs('[data-showcase="next"]', slider) : null;

    const state = { idx: radios.findIndex(r => r.checked) >= 0 ? radios.findIndex(r => r.checked) : 0 };

    function setIndex(i){
      state.idx = (i + radios.length) % radios.length;
      radios[state.idx].checked = true;
    }
    function next(){ setIndex(state.idx + 1); }
    function prev(){ setIndex(state.idx - 1); }

    if (prevBtn) prevBtn.addEventListener('click', prev);
    if (nextBtn) nextBtn.addEventListener('click', next);

    window.addEventListener('keydown', function(e){
      if (e.key === 'ArrowRight') next();
      if (e.key === 'ArrowLeft') prev();
    });

    let timer = null;
    function start(){
      stop();
      timer = window.setInterval(next, 8000);
    }
    function stop(){
      if (timer) window.clearInterval(timer);
      timer = null;
    }
    if (slider){
      slider.addEventListener('mouseenter', stop);
      slider.addEventListener('mouseleave', start);
      slider.addEventListener('focusin', stop);
      slider.addEventListener('focusout', start);
    }
    start();
  }

  async function loadHeroMetrics(){
    const nodes = qsa('[data-metric]');
    if (!nodes.length) return;

    // Punt d'integració opcional: retorna { indexed_records, countries, regions, ecclesiastical }
    // Exemple: GET /api/public/metrics
    const url = '/api/public/metrics';

    let data = null;
    try{
      const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
      if (res.ok) data = await res.json();
    }catch(_e){
      data = null;
    }

    function fmt(n){
      try{
        return new Intl.NumberFormat('ca-ES').format(n);
      }catch(_e){
        return String(n);
      }
    }

    if (data){
      nodes.forEach(el => {
        const k = el.getAttribute('data-metric');
        if (Object.prototype.hasOwnProperty.call(data, k)){
          el.textContent = fmt(data[k]);
        }
      });
    }
  }

  document.addEventListener('DOMContentLoaded', function(){
    initShowcaseSlider();
    loadHeroMetrics();
  });
})();


(function(){
  function setHeaderHeightVar(){
    const header = document.querySelector('header.header-genealogic');
    if (!header) return;
    const h = header.getBoundingClientRect().height || 0;
    document.documentElement.style.setProperty('--header-h', Math.round(h) + 'px');
  }

  function initSplashSnap(){
    const splash = document.getElementById('splash');
    const target = document.getElementById('home-start');
    if (!splash || !target) return;

    let interacted = false;
    let autoTimer = null;
    const mark = () => {
      interacted = true;
      if (autoTimer) {
        window.clearTimeout(autoTimer);
        autoTimer = null;
      }
    };

    window.addEventListener('touchstart', mark, { passive:true, once:true });
    window.addEventListener('keydown', mark, { passive:true, once:true });
    window.addEventListener('mousedown', mark, { passive:true, once:true });

    function go(){
      // evita saltar si l'usuari ja ha començat a baixar
      if (interacted) return;
      if (window.scrollY > 10) return;
      smoothScrollTo(target);
    }

    // Auto després de 3 segons
    autoTimer = window.setTimeout(go, 3000);

    // Wheel: quan l'usuari fa scroll avall al splash, salta a la següent secció (i desactiva)
    function onWheel(e){
      if (window.scrollY > 10) return window.removeEventListener('wheel', onWheel, { passive:false });
      if (e.deltaY > 10){
        mark();
        e.preventDefault();
        smoothScrollTo(target);
        window.removeEventListener('wheel', onWheel, { passive:false });
      }
    }

    // només mentre estem al splash
    window.addEventListener('wheel', onWheel, { passive:false });
  }

  function smoothScrollTo(target){
    if (!target || typeof target.getBoundingClientRect !== 'function') return;
    const startY = window.scrollY || window.pageYOffset;
    const targetY = target.getBoundingClientRect().top + startY;
    const distance = targetY - startY;
    const duration = Math.min(900, Math.max(450, Math.abs(distance) * 0.6));
    const startTime = performance.now();

    function easeOutCubic(t){
      return 1 - Math.pow(1 - t, 3);
    }

    function step(now){
      const elapsed = now - startTime;
      const progress = Math.min(1, elapsed / duration);
      const eased = easeOutCubic(progress);
      window.scrollTo(0, Math.round(startY + distance * eased));
      if (progress < 1) {
        requestAnimationFrame(step);
      }
    }

    requestAnimationFrame(step);
  }

  function onResize(){
    setHeaderHeightVar();
  }

  document.addEventListener('DOMContentLoaded', function(){
    setHeaderHeightVar();
    initSplashSnap();
    window.addEventListener('resize', onResize, { passive:true });
  });
})();
