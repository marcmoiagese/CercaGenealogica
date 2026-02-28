(function () {
    const uploaders = document.querySelectorAll('.file-uploader');
    if (!uploaders.length) return;

    const stop = (event) => {
        event.preventDefault();
        event.stopPropagation();
    };

    uploaders.forEach((uploader) => {
        const input = uploader.querySelector('input[type="file"]');
        const selected = uploader.querySelector('.file-uploader__selected');
        if (!input || !selected) return;

        const emptyLabel = selected.getAttribute('data-empty') || '';
        const multipleTemplate = selected.getAttribute('data-multiple') || '';
        if (!selected.hasAttribute('aria-live')) {
            selected.setAttribute('aria-live', 'polite');
        }

        const update = () => {
            const files = input.files;
            if (!files || files.length === 0) {
                selected.textContent = emptyLabel;
                uploader.classList.remove('has-file');
                return;
            }
            uploader.classList.add('has-file');
            if (files.length === 1) {
                selected.textContent = files[0].name;
                return;
            }
            if (multipleTemplate.includes('%d')) {
                selected.textContent = multipleTemplate.replace('%d', files.length);
                return;
            }
            selected.textContent = `${files.length} files`;
        };

        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach((evt) => {
            uploader.addEventListener(evt, stop);
        });
        ['dragenter', 'dragover'].forEach((evt) => {
            uploader.addEventListener(evt, () => uploader.classList.add('is-dragover'));
        });
        ['dragleave', 'drop'].forEach((evt) => {
            uploader.addEventListener(evt, () => uploader.classList.remove('is-dragover'));
        });
        uploader.addEventListener('drop', (event) => {
            const files = event.dataTransfer && event.dataTransfer.files ? event.dataTransfer.files : null;
            if (!files || files.length === 0) return;
            if (typeof DataTransfer === 'undefined') return;
            const dt = new DataTransfer();
            const maxFiles = input.multiple ? files.length : 1;
            for (let i = 0; i < maxFiles; i += 1) {
                dt.items.add(files[i]);
            }
            input.files = dt.files;
            update();
        });

        input.addEventListener('change', update);
        update();
    });
})();
