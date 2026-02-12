document.addEventListener("DOMContentLoaded", function () {
    var viewerEl = document.getElementById("mediaViewer");
    if (!viewerEl || typeof OpenSeadragon === "undefined") {
        return;
    }

    var dzi = viewerEl.getAttribute("data-dzi");
    var token = viewerEl.getAttribute("data-token") || "";
    if (!dzi) {
        return;
    }

    function appendToken(url) {
        if (!token) {
            return url;
        }
        if (url.indexOf("t=") !== -1) {
            return url;
        }
        var sep = url.indexOf("?") === -1 ? "?" : "&";
        return url + sep + "t=" + encodeURIComponent(token);
    }

    var tileSource = token ? appendToken(dzi) : dzi;
    var viewer = OpenSeadragon({
        id: "mediaViewer",
        tileSources: tileSource,
        showNavigator: true,
        navigatorId: "mediaNavigator",
        showNavigationControl: false,
        showZoomControl: false,
        showHomeControl: false,
        showFullPageControl: false,
        showRotationControl: false,
        preserveViewport: true,
        visibilityRatio: 1.0,
        minZoomLevel: 0.5,
        maxZoomPixelRatio: 2.0,
        gestureSettingsMouse: {
            clickToZoom: true,
            dblClickToZoom: true,
            flickEnabled: true,
            pinchToZoom: true,
            scrollToZoom: true,
            dragToPan: true
        }
    });

    var panel = document.getElementById("mediaAdjustPanel");
    var btnAdjust = document.getElementById("mediaBtnAdjust");
    var btnReset = document.getElementById("mediaBtnReset");
    var btnClosePanel = document.getElementById("mediaBtnClosePanel");

    var brightness = document.getElementById("mediaBrightness");
    var contrast = document.getElementById("mediaContrast");
    var invert = document.getElementById("mediaInvert");

    var brightnessVal = document.getElementById("mediaBrightnessVal");
    var contrastVal = document.getElementById("mediaContrastVal");

    function getCanvases() {
        var main = viewer && viewer.drawer ? viewer.drawer.canvas : null;
        var nav = viewer && viewer.navigator && viewer.navigator.drawer ? viewer.navigator.drawer.canvas : null;
        return { main: main, nav: nav };
    }

    function applyFilters() {
        if (!brightness || !contrast || !invert) {
            return;
        }
        var b = Number(brightness.value);
        var c = Number(contrast.value);
        var inv = invert.checked ? 1 : 0;

        if (brightnessVal) {
            brightnessVal.textContent = b + "%";
        }
        if (contrastVal) {
            contrastVal.textContent = c + "%";
        }

        var filter = "brightness(" + b + "%) contrast(" + c + "%) invert(" + inv + ")";
        var canvases = getCanvases();
        if (canvases.main) {
            canvases.main.style.filter = filter;
        }
        if (canvases.nav) {
            canvases.nav.style.filter = filter;
        }
    }

    function clamp(value, min, max) {
        return Math.max(min, Math.min(max, value));
    }

    function resizeNavigatorToImage() {
        var wrap = document.getElementById("mediaNavigatorWrap");
        if (!wrap || !viewer || !viewer.world) {
            return;
        }
        var item = viewer.world.getItemAt(0);
        if (!item) {
            return;
        }
        var size = item.getContentSize();
        var w = size.x;
        var h = size.y;
        if (!w || !h) {
            return;
        }
        var ratio = h / w;
        var targetW = 170;
        var targetH = clamp(Math.round(targetW * ratio), 90, 170);
        wrap.style.width = targetW + "px";
        wrap.style.height = targetH + "px";
    }

    if (btnAdjust && panel) {
        btnAdjust.addEventListener("click", function () {
            panel.classList.toggle("media-hidden");
        });
    }
    if (btnClosePanel && panel) {
        btnClosePanel.addEventListener("click", function () {
            panel.classList.add("media-hidden");
        });
    }

    if (brightness) {
        brightness.addEventListener("input", applyFilters);
    }
    if (contrast) {
        contrast.addEventListener("input", applyFilters);
    }
    if (invert) {
        invert.addEventListener("change", applyFilters);
    }

    if (btnReset) {
        btnReset.addEventListener("click", function () {
            if (brightness) {
                brightness.value = 100;
            }
            if (contrast) {
                contrast.value = 100;
            }
            if (invert) {
                invert.checked = false;
            }
            applyFilters();
        });
    }

    var btnHome = document.getElementById("mediaBtnHome");
    var btnZoomIn = document.getElementById("mediaBtnZoomIn");
    var btnZoomOut = document.getElementById("mediaBtnZoomOut");
    var btnFull = document.getElementById("mediaBtnFull");
    var btnRotL = document.getElementById("mediaBtnRotL");
    var btnRotR = document.getElementById("mediaBtnRotR");
    var shell = document.getElementById("mediaViewerShell");
    var pageSelect = document.getElementById("mediaPageSelect");
    var prevBtn = document.getElementById("mediaPrevBtn");
    var nextBtn = document.getElementById("mediaNextBtn");
    var fullKey = "mediaViewerMaximized";
    var detailsKey = "mediaViewerDetailsOpen";
    var isNavigating = false;
    var pageCount = document.querySelector(".media-tool-muted");
    var detailsPanel = document.getElementById("mediaDetailsPanel");
    var detailsBtn = document.getElementById("mediaBtnDetails");
    var detailsClose = document.getElementById("mediaDetailsClose");
    var detailsTabs = detailsPanel ? detailsPanel.querySelectorAll(".media-details-tab") : [];

    function setMaximized(active) {
        if (!shell) {
            return;
        }
        if (active) {
            shell.classList.add("is-maximized");
            document.body.classList.add("media-viewer-maximized");
            try {
                window.localStorage.setItem(fullKey, "1");
            } catch (e) {}
        } else {
            shell.classList.remove("is-maximized");
            document.body.classList.remove("media-viewer-maximized");
            try {
                window.localStorage.removeItem(fullKey);
            } catch (e) {}
        }
    }

    function setDetailsOpen(active) {
        if (!detailsPanel) {
            return;
        }
        detailsPanel.classList.toggle("media-hidden", !active);
        if (detailsBtn) {
            detailsBtn.setAttribute("aria-pressed", active ? "true" : "false");
        }
        try {
            if (active) {
                window.localStorage.setItem(detailsKey, "1");
            } else {
                window.localStorage.removeItem(detailsKey);
            }
        } catch (e) {}
    }

    function toggleDetailsSection(section, show) {
        if (!detailsPanel) {
            return;
        }
        detailsPanel.querySelectorAll("[data-section='" + section + "']").forEach(function (el) {
            el.style.display = show ? "" : "none";
        });
    }

    function setDetailsField(field, value, emptyFallback) {
        if (!detailsPanel) {
            return;
        }
        detailsPanel.querySelectorAll("[data-field='" + field + "']").forEach(function (el) {
            if (el.tagName === "INPUT" || el.tagName === "TEXTAREA" || el.tagName === "SELECT") {
                if (el.type === "checkbox") {
                    el.checked = !!value;
                } else {
                    el.value = value || "";
                }
                return;
            }
            if (!value && emptyFallback) {
                el.textContent = emptyFallback;
                return;
            }
            el.textContent = value || "";
        });
    }

    function updateDetailsPanel(ctx) {
        if (!detailsPanel) {
            return;
        }
        var hasContext = !!(ctx && ctx.has_book);
        var hasLink = !!(ctx && ctx.has_link);
        detailsPanel.dataset.hasContext = hasContext ? "1" : "0";
        detailsPanel.dataset.hasLink = hasLink ? "1" : "0";
        toggleDetailsSection("book-grid", hasContext);
        toggleDetailsSection("book-empty", !hasContext);
        toggleDetailsSection("page-form", hasLink);
        toggleDetailsSection("page-empty", !hasLink);
        toggleDetailsSection("index-content", hasLink);
        toggleDetailsSection("index-empty", !hasLink);

        var book = ctx && ctx.book ? ctx.book : {};
        var page = ctx && ctx.page ? ctx.page : {};
        var counts = ctx && ctx.counts ? ctx.counts : {};
        var perms = ctx && ctx.permissions ? ctx.permissions : {};
        var links = ctx && ctx.links ? ctx.links : {};
        var total = page.total_registres || 0;

        var showInlineIndexer = hasLink && !!perms.can_index;
        toggleDetailsSection("indexer-inline", showInlineIndexer);
        toggleDetailsSection("page-linker", !!perms.can_link);
        toggleDetailsSection("page-link-form", !hasLink && !!perms.can_link);
        toggleDetailsSection("page-link-unlink", hasLink && !!perms.can_link);

        setDetailsField("book_title", book.title || "", "—");
        setDetailsField("book_type", book.type_label || book.type || "", "—");
        setDetailsField("book_cronologia", book.cronologia || "", "—");
        setDetailsField("book_pagines", book.pagines > 0 ? String(book.pagines) : "", "—");
        setDetailsField("book_municipi", book.municipi || "", "—");
        detailsPanel.querySelectorAll("[data-field='book_url']").forEach(function (el) {
            el.setAttribute("href", book.url || "");
        });

        setDetailsField("stat_id", page.stat_id || "", "");
        setDetailsField("page_num_text", page.num_text || "", "");
        setDetailsField("page_id", page.id || "", "");
        setDetailsField("page_total", total || "", "");
        setDetailsField("page_type", page.type || "normal", "");
        setDetailsField("page_excluded", page.excluded === 1, "");
        setDetailsField("page_indexed", page.indexed === 1, "");
        setDetailsField("page_duplicate", page.duplicate || "", "");
        var linkLabel = page.num_text || (page.id ? String(page.id) : "");
        setDetailsField("page_link_label", linkLabel, "—");

        detailsPanel.querySelectorAll("[data-field='page_type']").forEach(function (el) {
            if (el.tagName === "SELECT" && page.type) {
                el.value = page.type;
            }
        });

        detailsPanel.querySelectorAll("[data-field='page_num_text'],[data-field='page_id'],[data-field='page_total'],[data-field='page_type'],[data-field='page_excluded'],[data-field='page_indexed'],[data-field='page_duplicate']").forEach(function (el) {
            el.disabled = !perms.can_edit;
        });
        toggleDetailsSection("page-save", !!perms.can_edit);
        toggleDetailsSection("page-no-edit", !perms.can_edit);

        setDetailsField("page_existing", typeof counts.existing === "number" ? String(counts.existing) : "0", "");
        setDetailsField("page_limit", total > 0 ? String(total) : "", "—");
        setDetailsField("page_remaining", total > 0 ? String(counts.remaining || 0) : "", "—");
        toggleDetailsSection("index-note", hasLink && total > 0);

        var recordsLink = detailsPanel.querySelector("[data-field='records_url']");
        if (recordsLink) {
            recordsLink.setAttribute("href", links.registres || "");
        }
        toggleDetailsSection("records-link", !!(perms.can_view_records && links.registres));

        var indexerLink = detailsPanel.querySelector("[data-field='indexer_url']");
        if (indexerLink) {
            indexerLink.setAttribute("href", links.indexar || "");
            if (total > 0 && (counts.remaining || 0) <= 0) {
                indexerLink.setAttribute("aria-disabled", "true");
            } else {
                indexerLink.removeAttribute("aria-disabled");
            }
        }
        toggleDetailsSection("indexer-link", !!(perms.can_index && links.indexar) && !showInlineIndexer);
        toggleDetailsSection("index-no-access", !perms.can_index);

        var form = detailsPanel.querySelector(".media-details-form");
        if (form && book.id) {
            form.setAttribute("action", "/documentals/llibres/" + book.id + "/pagines/stats/save");
        }
        detailsPanel.querySelectorAll("input[name='llibre_id']").forEach(function (el) {
            el.value = book.id || "";
        });

        var indexerRoot = document.getElementById("indexer-root");
        if (indexerRoot) {
            var remaining = total > 0 ? Number(counts.remaining || 0) : null;
            var existingRows = ctx && ctx.existing_rows ? ctx.existing_rows : [];
            indexerRoot.dataset.pageValue = page.num_text || "";
            indexerRoot.dataset.pageRemaining = remaining === null ? "" : String(remaining);
            indexerRoot.dataset.pageTotal = total > 0 ? String(total) : "";
            indexerRoot.dataset.inlineEdit = perms.can_inline_edit ? "1" : "0";
            indexerRoot.dispatchEvent(new CustomEvent("indexer:context", {
                detail: {
                    pageValue: page.num_text || "",
                    pageLimit: remaining,
                    pageTotal: total > 0 ? total : null,
                    existingRows: existingRows,
                    canInlineEdit: !!perms.can_inline_edit
                }
            }));
        }
    }

    try {
        if (window.localStorage.getItem(fullKey) === "1") {
            setMaximized(true);
        }
    } catch (e) {}

    try {
        if (window.localStorage.getItem(detailsKey) === "1") {
            setDetailsOpen(true);
        }
    } catch (e) {}

    if (btnHome) {
        btnHome.addEventListener("click", function () {
            viewer.viewport.goHome(true);
        });
    }
    if (btnZoomIn) {
        btnZoomIn.addEventListener("click", function () {
            viewer.viewport.zoomBy(1.2, null, true);
        });
    }
    if (btnZoomOut) {
        btnZoomOut.addEventListener("click", function () {
            viewer.viewport.zoomBy(1 / 1.2, null, true);
        });
    }
    if (btnFull) {
        btnFull.addEventListener("click", function () {
            var isActive = shell && shell.classList.contains("is-maximized");
            setMaximized(!isActive);
            if (!document.fullscreenElement && !isActive) {
                if (shell && shell.requestFullscreen) {
                    shell.requestFullscreen().catch(function () {});
                } else if (document.documentElement.requestFullscreen) {
                    document.documentElement.requestFullscreen().catch(function () {});
                }
            } else if (document.fullscreenElement) {
                document.exitFullscreen();
            }
        });
    }

    if (detailsBtn && detailsPanel) {
        detailsBtn.addEventListener("click", function () {
            var isHidden = detailsPanel.classList.contains("media-hidden");
            setDetailsOpen(isHidden);
        });
    }
    if (detailsClose && detailsPanel) {
        detailsClose.addEventListener("click", function () {
            setDetailsOpen(false);
        });
    }

    if (detailsTabs && detailsTabs.length) {
        detailsTabs.forEach(function (tab) {
            tab.addEventListener("click", function () {
                var target = tab.getAttribute("data-tab-target");
                if (!target) {
                    return;
                }
                detailsTabs.forEach(function (btn) {
                    btn.classList.toggle("is-active", btn === tab);
                    btn.setAttribute("aria-selected", btn === tab ? "true" : "false");
                });
                if (!detailsPanel) {
                    return;
                }
                detailsPanel.querySelectorAll(".media-details-pane").forEach(function (pane) {
                    pane.classList.toggle("is-active", pane.getAttribute("data-tab-panel") === target);
                });
            });
        });
    }

    var rotation = 0;
    if (btnRotL) {
        btnRotL.addEventListener("click", function () {
            rotation -= 90;
            viewer.viewport.setRotation(rotation);
        });
    }
    if (btnRotR) {
        btnRotR.addEventListener("click", function () {
            rotation += 90;
            viewer.viewport.setRotation(rotation);
        });
    }

    function updateNavButtons(prevId, nextId) {
        if (prevBtn) {
            prevBtn.setAttribute("data-target", prevId || "");
            prevBtn.disabled = !prevId;
        }
        if (nextBtn) {
            nextBtn.setAttribute("data-target", nextId || "");
            nextBtn.disabled = !nextId;
        }
    }

    function updatePageSelect(id) {
        if (!pageSelect) {
            return;
        }
        pageSelect.value = id;
    }

    function updatePageCount(current, total) {
        if (!pageCount) {
            return;
        }
        var text = pageCount.textContent || "";
        if (text) {
            var replaced = text.replace(/\d+/, String(current)).replace(/\d+/, String(total));
            pageCount.textContent = replaced;
            return;
        }
        pageCount.textContent = current + " / " + total;
    }

    function updateTitle(title) {
        if (title) {
            document.title = title;
        }
    }

    function updateReturnTo(url) {
        if (!detailsPanel || !url) {
            return;
        }
        detailsPanel.querySelectorAll("input[name='return_to']").forEach(function (el) {
            el.value = url;
        });
    }

    function loadItemData(itemId, replaceHistory) {
        if (!itemId || isNavigating) {
            return;
        }
        isNavigating = true;
        fetch("/media/items/" + itemId + "/data", { credentials: "same-origin" })
            .then(function (resp) {
                if (!resp.ok) {
                    throw new Error("bad response");
                }
                return resp.json();
            })
            .then(function (data) {
                if (data.status) {
                    window.location.href = "/media/items/" + itemId;
                    return;
                }
                token = data.grant_token || "";
                dzi = data.dzi || "";
                if (dzi) {
                    viewerEl.setAttribute("data-dzi", dzi);
                    viewerEl.setAttribute("data-token", token || "");
                    viewer.open(token ? appendToken(dzi) : dzi);
                }
                updateNavButtons(data.prev_id || "", data.next_id || "");
                updatePageSelect(itemId);
                updatePageCount(data.current_index || 1, data.total_items || 1);
                updateTitle(data.item && data.item.title ? data.item.title : document.title);
                updateDetailsPanel(data.page_context || null);
                var url = "/media/items/" + itemId;
                updateReturnTo(url);
                if (replaceHistory) {
                    history.replaceState({ mediaItem: itemId }, "", url);
                } else {
                    history.pushState({ mediaItem: itemId }, "", url);
                }
            })
            .catch(function () {
                window.location.href = "/media/items/" + itemId;
            })
            .finally(function () {
                isNavigating = false;
            });
    }

    function navigateToItem(value) {
        loadItemData(value, false);
    }

    if (pageSelect) {
        pageSelect.addEventListener("change", function () {
            navigateToItem(pageSelect.value);
        });
    }
    if (prevBtn) {
        prevBtn.addEventListener("click", function () {
            navigateToItem(prevBtn.getAttribute("data-target"));
        });
    }
    if (nextBtn) {
        nextBtn.addEventListener("click", function () {
            navigateToItem(nextBtn.getAttribute("data-target"));
        });
    }

    window.addEventListener("popstate", function () {
        var parts = window.location.pathname.split("/media/items/");
        if (parts.length > 1) {
            var id = parts[1].split("/")[0];
            if (id) {
                loadItemData(id, true);
            }
        }
    });

    viewer.addHandler("open", function () {
        if (token) {
            var tiled = viewer.world.getItemAt(0);
            if (tiled && tiled.source && typeof tiled.source.getTileUrl === "function") {
                var originalGet = tiled.source.getTileUrl.bind(tiled.source);
                tiled.source.getTileUrl = function (level, x, y) {
                    return appendToken(originalGet(level, x, y));
                };
            }
        }
        applyFilters();
        resizeNavigatorToImage();
        window.addEventListener("resize", resizeNavigatorToImage);
    });

    document.addEventListener("fullscreenchange", function () {
        window.setTimeout(function () {
            resizeNavigatorToImage();
            viewer.viewport.applyConstraints(true);
        }, 120);
        if (!document.fullscreenElement) {
            // Keep CSS maximized state for persistence across pages.
            return;
        }
        setMaximized(true);
    });
});
