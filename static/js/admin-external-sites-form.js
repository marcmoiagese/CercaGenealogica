(() => {
  const uploader = document.querySelector(".icon-uploader");
  if (!uploader) return;
  const input = uploader.querySelector('input[type="file"]');
  const title = uploader.querySelector(".icon-uploader__title");
  const meta = uploader.querySelector(".icon-uploader__meta");
  const initialTitle = title ? title.textContent : "";
  const initialMeta = meta ? meta.textContent : "";

  const updateLabel = () => {
    if (!input || !title) return;
    if (input.files && input.files.length > 0) {
      const file = input.files[0];
      title.textContent = file.name;
      if (meta) {
        const kb = Math.max(1, Math.round(file.size / 1024));
        meta.textContent = `${kb} KB`;
      }
      return;
    }
    title.textContent = initialTitle;
    if (meta) meta.textContent = initialMeta;
  };

  const stop = (event) => {
    event.preventDefault();
    event.stopPropagation();
  };

  ["dragenter", "dragover", "dragleave", "drop"].forEach((evt) => {
    uploader.addEventListener(evt, stop);
  });
  ["dragenter", "dragover"].forEach((evt) => {
    uploader.addEventListener(evt, () => uploader.classList.add("is-dragover"));
  });
  ["dragleave", "drop"].forEach((evt) => {
    uploader.addEventListener(evt, () => uploader.classList.remove("is-dragover"));
  });
  uploader.addEventListener("drop", (event) => {
    if (!input) return;
    const files = event.dataTransfer && event.dataTransfer.files ? event.dataTransfer.files : null;
    if (!files || files.length === 0) return;
    const dt = new DataTransfer();
    dt.items.add(files[0]);
    input.files = dt.files;
    updateLabel();
  });
  if (input) {
    input.addEventListener("change", updateLabel);
  }
})();
