(() => {
  const select = document.getElementById("pais_selector");
  const search = document.getElementById("pais_search");
  const iso2 = document.getElementById("codi_iso2");
  const iso3 = document.getElementById("codi_iso3");
  const num = document.getElementById("codi_pais_num");

  if (!select || !iso2 || !iso3 || !num) {
    return;
  }

  const setCodes = (option) => {
    if (!option) return;
    iso2.value = (option.getAttribute("data-iso2") || "").toUpperCase();
    iso3.value = (option.getAttribute("data-iso3") || "").toUpperCase();
    num.value = option.getAttribute("data-num") || "";
  };

  if (select.selectedIndex > 0) {
    setCodes(select.options[select.selectedIndex]);
  }

  select.addEventListener("change", () => {
    setCodes(select.options[select.selectedIndex]);
  });

  if (search) {
    search.addEventListener("input", () => {
      const q = search.value.toLowerCase();
      for (let i = 0; i < select.options.length; i++) {
        const opt = select.options[i];
        if (i === 0 && !opt.value) {
          opt.style.display = "";
          continue;
        }
        const label = opt.textContent.toLowerCase();
        opt.style.display = label.includes(q) ? "" : "none";
      }
    });
  }
})();
