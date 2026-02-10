(() => {
    const selectionStore = new WeakMap();

    const saveSelection = (editor) => {
        const selection = window.getSelection();
        if (!selection || selection.rangeCount === 0) {
            return;
        }
        const range = selection.getRangeAt(0);
        if (!editor.contains(range.commonAncestorContainer)) {
            return;
        }
        selectionStore.set(editor, range.cloneRange());
    };

    const restoreSelection = (editor) => {
        const range = selectionStore.get(editor);
        if (!range) {
            return false;
        }
        const selection = window.getSelection();
        if (!selection) {
            return false;
        }
        selection.removeAllRanges();
        selection.addRange(range);
        return true;
    };

    const wrapSelection = (editor, before, after, fallback) => {
        let selection = window.getSelection();
        if (!selection || selection.rangeCount === 0 || !editor.contains(selection.getRangeAt(0).commonAncestorContainer)) {
            if (!restoreSelection(editor)) {
                return;
            }
            selection = window.getSelection();
        }
        const range = selection.getRangeAt(0);
        const selectedText = selection.toString();
        const inner = selectedText || fallback || "";
        const fragment = document.createRange().createContextualFragment(`${before}${inner}${after}`);
        range.deleteContents();
        range.insertNode(fragment);
        selection.removeAllRanges();
    };

    const insertAtCursor = (editor, node, followNode) => {
        const selection = window.getSelection();
        if (!selection || selection.rangeCount === 0) {
            editor.appendChild(node);
            if (followNode) {
                editor.appendChild(followNode);
            }
            return;
        }
        const range = selection.getRangeAt(0);
        if (!editor.contains(range.commonAncestorContainer)) {
            editor.appendChild(node);
            if (followNode) {
                editor.appendChild(followNode);
            }
            return;
        }
        range.deleteContents();
        range.insertNode(node);
        if (followNode) {
            node.after(followNode);
        }
        const caretRange = document.createRange();
        const focusNode = followNode || node;
        caretRange.selectNodeContents(focusNode);
        caretRange.collapse(followNode ? false : false);
        selection.removeAllRanges();
        selection.addRange(caretRange);
    };

    const handleToolbarAction = (editor, action, value, anchor) => {
        restoreSelection(editor);
        switch (action) {
        case "bold":
            document.execCommand("bold");
            return;
        case "italic":
            document.execCommand("italic");
            return;
        case "indent":
            wrapSelection(editor, `<div class="msg-indent">`, "</div>", "text");
            return;
        case "size":
            if (!value) {
                return;
            }
            wrapSelection(editor, `<span class="msg-size-${value}">`, "</span>", "text");
            return;
        case "table":
            showTablePicker(editor, anchor);
            return;
        case "table-row":
            insertTableRow(editor);
            return;
        case "table-col":
            insertTableCol(editor);
            return;
        case "table-delete":
            deleteTable(editor);
            return;
        case "link": {
            const selected = window.getSelection ? window.getSelection().toString() : "";
            const suggested = selected && /^https?:\/\//i.test(selected) ? selected : "https://";
            const url = window.prompt("Enllac (URL)", suggested);
            if (!url) {
                return;
            }
            const normalized = /^https?:\/\//i.test(url) ? url : `https://${url}`;
            const label = selected && selected !== normalized ? selected : normalized;
            wrapSelection(editor, `<a href="${normalized}" class="msg-link" rel="noopener noreferrer" target="_blank">`, "</a>", label);
            return;
        }
        default:
            return;
        }
    };

    const getEditorByTarget = (target) => {
        if (!target) {
            return null;
        }
        return document.querySelector(`[data-editor='${target}']`);
    };

    document.querySelectorAll("[data-message-toolbar]").forEach((toolbar) => {
        const target = toolbar.getAttribute("data-target");
        if (!target) {
            return;
        }
        const editor = getEditorByTarget(target);
        if (!editor) {
            return;
        }
        toolbar.addEventListener("click", (event) => {
            const button = event.target.closest("button[data-action]");
            if (!button) {
                return;
            }
            event.preventDefault();
            handleToolbarAction(editor, button.dataset.action || "", "", button);
        });
        const sizeSelect = toolbar.querySelector("select[data-action='size']");
        if (sizeSelect) {
            sizeSelect.addEventListener("change", (event) => {
                const value = event.target.value;
                handleToolbarAction(editor, "size", value, sizeSelect);
                event.target.value = "";
            });
        }
    });

    const htmlToBBCode = (root) => {
        const cleanText = (text) => text.replace(/\s+\n/g, "\n");
        const nodeToBBCode = (node) => {
            if (!node) {
                return "";
            }
            if (node.nodeType === Node.TEXT_NODE) {
                return node.textContent || "";
            }
            if (node.nodeType !== Node.ELEMENT_NODE) {
                return "";
            }
            const tag = node.tagName.toLowerCase();
            const content = Array.from(node.childNodes).map(nodeToBBCode).join("");
            if (tag === "strong" || tag === "b") {
                return `[b]${content}[/b]`;
            }
            if (tag === "em" || tag === "i") {
                return `[i]${content}[/i]`;
            }
            if (tag === "u") {
                return `[u]${content}[/u]`;
            }
            if (tag === "div" && node.classList.contains("msg-indent")) {
                return `[indent]${content}[/indent]`;
            }
            if (tag === "span") {
                if (node.classList.contains("msg-size-small")) {
                    return `[size=small]${content}[/size]`;
                }
                if (node.classList.contains("msg-size-normal")) {
                    return `[size=normal]${content}[/size]`;
                }
                if (node.classList.contains("msg-size-large")) {
                    return `[size=large]${content}[/size]`;
                }
                if (node.classList.contains("msg-size-xl")) {
                    return `[size=xl]${content}[/size]`;
                }
            }
            if (tag === "br") {
                return "\n";
            }
            if (tag === "table") {
                const rows = Array.from(node.querySelectorAll("tr")).map((row) => {
                    const cells = Array.from(row.querySelectorAll("td")).map((cell) => `[td]${nodeToBBCode(cell)}[/td]`).join("");
                    return `[tr]${cells}[/tr]`;
                });
                return `[table]\n${rows.join("\n")}\n[/table]`;
            }
            if (tag === "tr" || tag === "td") {
                return content;
            }
            if (tag === "a") {
                const href = node.getAttribute("href") || "";
                const safe = /^https?:\/\//i.test(href) ? href : "";
                if (!safe) {
                    return content;
                }
                const label = content || safe;
                return `[url=${safe}]${label}[/url]`;
            }
            if (tag === "div" || tag === "p") {
                return content + "\n";
            }
            return content;
        };
        return cleanText(nodeToBBCode(root)).trim();
    };

    document.querySelectorAll(".messages-editor").forEach((editor) => {
        const target = editor.getAttribute("data-editor");
        const textarea = target ? document.getElementById(target) : null;
        if (!textarea) {
            return;
        }
        const sync = () => {
            textarea.value = htmlToBBCode(editor);
        };
        editor.addEventListener("input", sync);
        editor.addEventListener("blur", sync);
        editor.addEventListener("keyup", () => saveSelection(editor));
        editor.addEventListener("mouseup", () => saveSelection(editor));
        editor.addEventListener("touchend", () => saveSelection(editor));
        editor.addEventListener("focus", () => saveSelection(editor));
        sync();
        const form = editor.closest("form");
        if (form) {
            form.addEventListener("submit", sync);
        }
    });

    document.addEventListener("click", (event) => {
        const link = event.target.closest(".message-body a, .messages-preview-body a");
        if (!link) {
            return;
        }
        let url;
        try {
            url = new URL(link.href, window.location.href);
        } catch (err) {
            return;
        }
        if (url.host && url.host !== window.location.host) {
            const ok = window.confirm("Estas sortint de la web. Vols continuar?");
            if (!ok) {
                event.preventDefault();
            }
        }
    });

    const sidebarSearch = document.getElementById("messagesSidebarSearch");
    if (sidebarSearch) {
        const items = Array.from(document.querySelectorAll(".messages-sidebar-item"));
        sidebarSearch.addEventListener("input", (event) => {
            const term = (event.target.value || "").toLowerCase();
            items.forEach((item) => {
                const text = item.textContent ? item.textContent.toLowerCase() : "";
                item.style.display = text.includes(term) ? "" : "none";
            });
        });
    }

    document.querySelectorAll("[data-action='new-folder']").forEach((button) => {
        button.addEventListener("click", () => {
            const inputId = button.getAttribute("data-folder-input");
            const input = inputId ? document.getElementById(inputId) : null;
            if (!input) {
                return;
            }
            const name = window.prompt("Nom de la carpeta", "");
            if (!name) {
                return;
            }
            input.value = name.trim();
            const form = input.closest("form");
            if (form) {
                form.submit();
            }
        });
    });

    const showTablePicker = (editor, anchor) => {
        let picker = document.getElementById("messagesTablePicker");
        if (!picker) {
            picker = document.createElement("div");
            picker.id = "messagesTablePicker";
            picker.className = "messages-table-picker";
            const grid = document.createElement("div");
            grid.className = "messages-table-grid";
            picker.appendChild(grid);
            document.body.appendChild(picker);
            for (let r = 0; r < 6; r++) {
                for (let c = 0; c < 6; c++) {
                    const cell = document.createElement("div");
                    cell.className = "messages-table-cell";
                    cell.dataset.row = String(r + 1);
                    cell.dataset.col = String(c + 1);
                    grid.appendChild(cell);
                }
            }
        }
        const rect = (anchor || editor).getBoundingClientRect();
        picker.style.left = `${rect.left + window.scrollX}px`;
        picker.style.top = `${rect.bottom + window.scrollY + 6}px`;
        picker.style.display = "block";
        const cells = Array.from(picker.querySelectorAll(".messages-table-cell"));
        const highlight = (row, col) => {
            cells.forEach((cell) => {
                const r = Number(cell.dataset.row);
                const c = Number(cell.dataset.col);
                cell.classList.toggle("is-active", r <= row && c <= col);
            });
        };
        const reset = () => {
            cells.forEach((cell) => cell.classList.remove("is-active"));
        };
        const handleMove = (event) => {
            const cell = event.target.closest(".messages-table-cell");
            if (!cell) {
                return;
            }
            highlight(Number(cell.dataset.row), Number(cell.dataset.col));
        };
        const handlePick = (event) => {
            const cell = event.target.closest(".messages-table-cell");
            if (!cell) {
                return;
            }
            const rows = Number(cell.dataset.row);
            const cols = Number(cell.dataset.col);
            const table = buildTableElement(rows, cols);
            const gap = document.createElement("div");
            gap.className = "msg-editor-gap";
            gap.appendChild(document.createElement("br"));
            insertAtCursor(editor, table, gap);
            picker.style.display = "none";
            picker.removeEventListener("mousemove", handleMove);
            picker.removeEventListener("click", handlePick);
            picker.removeEventListener("mouseleave", reset);
        };
        picker.addEventListener("mousemove", handleMove);
        picker.addEventListener("click", handlePick);
        picker.addEventListener("mouseleave", reset);
    };

    const buildTableElement = (rows, cols) => {
        const table = document.createElement("table");
        table.className = "msg-table";
        for (let r = 0; r < rows; r++) {
            const tr = document.createElement("tr");
            for (let c = 0; c < cols; c++) {
                const td = document.createElement("td");
                td.contentEditable = "true";
                if (r === 0) {
                    td.textContent = `Capcalera ${c + 1}`;
                }
                tr.appendChild(td);
            }
            table.appendChild(tr);
        }
        return table;
    };

    const findClosestTable = (editor) => {
        const selection = window.getSelection();
        if (!selection || selection.rangeCount === 0) {
            return null;
        }
        let node = selection.anchorNode;
        while (node && node !== editor) {
            if (node.nodeType === Node.ELEMENT_NODE && node.tagName.toLowerCase() === "table") {
                return node;
            }
            node = node.parentNode;
        }
        return null;
    };

    const insertTableRow = (editor) => {
        restoreSelection(editor);
        const table = findClosestTable(editor);
        if (!table) {
            const gap = document.createElement("div");
            gap.className = "msg-editor-gap";
            gap.appendChild(document.createElement("br"));
            insertAtCursor(editor, buildTableElement(2, 2), gap);
            return;
        }
        const row = table.rows[0];
        const cols = row ? row.cells.length : 1;
        const tr = table.insertRow(-1);
        for (let i = 0; i < cols; i++) {
            const cell = tr.insertCell(-1);
            cell.contentEditable = "true";
        }
    };

    const insertTableCol = (editor) => {
        restoreSelection(editor);
        const table = findClosestTable(editor);
        if (!table) {
            const gap = document.createElement("div");
            gap.className = "msg-editor-gap";
            gap.appendChild(document.createElement("br"));
            insertAtCursor(editor, buildTableElement(2, 2), gap);
            return;
        }
        const rows = Array.from(table.rows);
        rows.forEach((row) => {
            const cell = row.insertCell(-1);
            cell.contentEditable = "true";
        });
    };

    const deleteTable = (editor) => {
        restoreSelection(editor);
        const table = findClosestTable(editor);
        if (!table) {
            return;
        }
        table.remove();
    };
})();
