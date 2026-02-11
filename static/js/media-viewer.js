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
    var isNavigating = false;
    var pageCount = document.querySelector(".media-tool-muted");

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

    try {
        if (window.localStorage.getItem(fullKey) === "1") {
            setMaximized(true);
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
                var url = "/media/items/" + itemId;
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
