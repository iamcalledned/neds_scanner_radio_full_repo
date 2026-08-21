/* scanner_heatmap.js — Responsive scanner activity map with useful filters and summaries. */

(function () {
  "use strict";

  const DEFAULT_CENTER = { lat: 42.13, lng: -71.52 };
  const HEAT_COLORS = [
    [0, [30, 58, 138]],
    [0.24, [37, 99, 235]],
    [0.43, [56, 189, 248]],
    [0.62, [250, 204, 21]],
    [0.78, [249, 115, 22]],
    [0.93, [239, 68, 68]],
    [1, [255, 77, 120]],
  ];

  let map = null;
  let heatLayer = null;
  let HeatmapOverlay = null;
  let TownMarkerOverlay = null;
  let heatColorLookup = null;
  let infoWindow = null;
  let coverageBounds = null;
  let townMarkers = [];
  let allTowns = [];
  let currentPoints = [];
  let selectedRange = "week";
  let selectedDepartment = "all";

  let elTown;
  let elPointCount;
  let elLoading;
  let elEmpty;
  let elTogglePins;
  let elToggleHeat;
  let elUnavailable;
  let elSummaryCount;
  let elSummaryBusiest;
  let elSummaryTowns;
  let elTownList;

  function normalizeTown(value) {
    return String(value || "").trim().toLowerCase();
  }

  function displayTown(value) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .replace(/\b\w/g, (letter) => letter.toUpperCase());
  }

  function escapeHTML(value) {
    return String(value ?? "").replace(/[&<>"']/g, (character) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    })[character]);
  }

  function rangeLabel(range) {
    return {
      day: "today",
      week: "in 7 days",
      month: "in 30 days",
      all: "all time",
    }[range] || "in this range";
  }

  function loadMapsAPI(apiKey) {
    return new Promise((resolve, reject) => {
      if (window.google?.maps) {
        resolve();
        return;
      }

      const callbackName = "__scannerMapsReady";
      const script = document.createElement("script");
      script.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(apiKey)}&loading=async&callback=${callbackName}`;
      script.async = true;
      script.defer = true;
      script.onerror = () => reject(new Error("The map service did not load."));
      window[callbackName] = () => {
        delete window[callbackName];
        resolve();
      };
      document.head.appendChild(script);
    });
  }

  function darkMapStyles() {
    return [
      { elementType: "geometry", stylers: [{ color: "#0b1423" }] },
      { elementType: "labels.text.stroke", stylers: [{ color: "#0b1423" }] },
      { elementType: "labels.text.fill", stylers: [{ color: "#7c8da5" }] },
      { featureType: "administrative", elementType: "geometry.stroke", stylers: [{ color: "#26364b" }] },
      { featureType: "landscape", elementType: "geometry", stylers: [{ color: "#0d1727" }] },
      { featureType: "poi", elementType: "geometry", stylers: [{ color: "#101b2d" }] },
      { featureType: "poi", elementType: "labels.text.fill", stylers: [{ color: "#50627a" }] },
      { featureType: "poi.park", elementType: "geometry", stylers: [{ color: "#0b1b22" }] },
      { featureType: "road", elementType: "geometry", stylers: [{ color: "#243247" }] },
      { featureType: "road", elementType: "geometry.stroke", stylers: [{ color: "#101827" }] },
      { featureType: "road", elementType: "labels.text.fill", stylers: [{ color: "#718198" }] },
      { featureType: "road.highway", elementType: "geometry", stylers: [{ color: "#34445b" }] },
      { featureType: "road.highway", elementType: "labels.text.fill", stylers: [{ color: "#b6c3d3" }] },
      { featureType: "transit", elementType: "geometry", stylers: [{ color: "#172235" }] },
      { featureType: "water", elementType: "geometry", stylers: [{ color: "#020817" }] },
      { featureType: "water", elementType: "labels.text.fill", stylers: [{ color: "#355a78" }] },
    ];
  }

  function initMap() {
    map = new google.maps.Map(document.getElementById("map"), {
      center: DEFAULT_CENTER,
      zoom: 11,
      styles: darkMapStyles(),
      mapTypeControl: false,
      streetViewControl: false,
      fullscreenControl: true,
      zoomControl: true,
      gestureHandling: "cooperative",
      clickableIcons: false,
    });
    infoWindow = new google.maps.InfoWindow({ maxWidth: 240 });
  }

  function getTownMarkerOverlayClass() {
    if (TownMarkerOverlay) return TownMarkerOverlay;

    TownMarkerOverlay = class ScannerTownMarker extends google.maps.OverlayView {
      constructor(options) {
        super();
        this.options = options;
        this.position = new google.maps.LatLng(options.position);
        this.element = null;
        this.setMap(options.map || null);
      }

      onAdd() {
        const button = document.createElement("button");
        button.type = "button";
        button.className = `scanner-town-marker${this.options.selected ? " is-selected" : ""}${this.options.active ? " is-active" : ""}`;
        button.title = this.options.title;
        button.setAttribute("aria-label", this.options.title);
        button.style.setProperty("--marker-size", `${Math.max(8, this.options.scale * 2)}px`);
        button.innerHTML = `<span class="scanner-town-marker-dot" aria-hidden="true"></span><span class="scanner-town-marker-label">${escapeHTML(this.options.label)}</span>`;
        button.addEventListener("click", this.options.onClick);
        google.maps.OverlayView.preventMapHitsAndGesturesFrom(button);
        this.getPanes().overlayMouseTarget.appendChild(button);
        this.element = button;
      }

      draw() {
        if (!this.element) return;
        const pixel = this.getProjection().fromLatLngToDivPixel(this.position);
        this.element.style.left = `${pixel.x}px`;
        this.element.style.top = `${pixel.y}px`;
        this.element.style.zIndex = String(this.options.zIndex);
      }

      onRemove() {
        if (this.element) {
          this.element.removeEventListener("click", this.options.onClick);
          this.element.remove();
          this.element = null;
        }
      }

      getPosition() {
        return this.position;
      }
    };

    return TownMarkerOverlay;
  }

  function heatColor(intensity) {
    for (let index = 1; index < HEAT_COLORS.length; index += 1) {
      const [stop, color] = HEAT_COLORS[index];
      const [previousStop, previousColor] = HEAT_COLORS[index - 1];
      if (intensity <= stop) {
        const mix = (intensity - previousStop) / Math.max(0.001, stop - previousStop);
        return previousColor.map((channel, channelIndex) => (
          Math.round(channel + ((color[channelIndex] - channel) * mix))
        ));
      }
    }
    return HEAT_COLORS[HEAT_COLORS.length - 1][1];
  }

  function getHeatColorLookup() {
    if (!heatColorLookup) {
      heatColorLookup = Array.from({ length: 256 }, (_, index) => heatColor(index / 255));
    }
    return heatColorLookup;
  }

  function getHeatmapOverlayClass() {
    if (HeatmapOverlay) return HeatmapOverlay;

    HeatmapOverlay = class ScannerHeatmapOverlay extends google.maps.OverlayView {
      constructor(points, options = {}) {
        super();
        this.points = points.map((point) => new google.maps.LatLng(Number(point.lat), Number(point.lng)));
        this.radius = Number(options.radius || 34);
        this.container = null;
        this.canvas = null;
        this.drawFrame = null;
      }

      onAdd() {
        this.container = document.createElement("div");
        this.container.className = "scanner-heat-overlay";
        this.container.style.position = "absolute";
        this.container.style.pointerEvents = "none";

        this.canvas = document.createElement("canvas");
        this.canvas.style.display = "block";
        this.container.appendChild(this.canvas);
        this.getPanes().overlayLayer.appendChild(this.container);
      }

      draw() {
        if (this.drawFrame) window.cancelAnimationFrame(this.drawFrame);
        this.drawFrame = window.requestAnimationFrame(() => {
          this.drawFrame = null;
          this.render();
        });
      }

      render() {
        if (!this.canvas || !this.container || !map?.getBounds()) return;

        const projection = this.getProjection();
        const bounds = map.getBounds();
        const southWest = projection.fromLatLngToDivPixel(bounds.getSouthWest());
        const northEast = projection.fromLatLngToDivPixel(bounds.getNorthEast());
        const width = Math.max(1, Math.ceil(northEast.x - southWest.x));
        const height = Math.max(1, Math.ceil(southWest.y - northEast.y));
        const pixelRatio = Math.min(1.5, window.devicePixelRatio || 1);

        this.container.style.left = `${Math.floor(southWest.x)}px`;
        this.container.style.top = `${Math.floor(northEast.y)}px`;
        this.container.style.width = `${width}px`;
        this.container.style.height = `${height}px`;
        this.canvas.style.width = `${width}px`;
        this.canvas.style.height = `${height}px`;
        this.canvas.width = Math.ceil(width * pixelRatio);
        this.canvas.height = Math.ceil(height * pixelRatio);

        const context = this.canvas.getContext("2d", { willReadFrequently: true });
        context.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
        context.clearRect(0, 0, width, height);

        const binSize = window.matchMedia("(max-width: 767px)").matches ? 10 : 8;
        const bins = new Map();
        this.points.forEach((point) => {
          const pixel = projection.fromLatLngToDivPixel(point);
          const x = pixel.x - southWest.x;
          const y = pixel.y - northEast.y;
          if (x < -this.radius || y < -this.radius || x > width + this.radius || y > height + this.radius) return;

          const binX = Math.round(x / binSize) * binSize;
          const binY = Math.round(y / binSize) * binSize;
          const key = `${binX}:${binY}`;
          const bin = bins.get(key);
          if (bin) {
            bin.count += 1;
          } else {
            bins.set(key, { x: binX, y: binY, count: 1 });
          }
        });

        const maxBinCount = Math.max(1, ...[...bins.values()].map((bin) => bin.count));
        const maxLogCount = Math.log1p(maxBinCount);
        bins.forEach((bin) => {
          const relativeDensity = Math.log1p(bin.count) / maxLogCount;
          const strength = 0.12 + (relativeDensity * 0.36);
          const gradient = context.createRadialGradient(bin.x, bin.y, 0, bin.x, bin.y, this.radius);
          gradient.addColorStop(0, `rgba(0, 0, 0, ${strength})`);
          gradient.addColorStop(0.32, `rgba(0, 0, 0, ${strength * 0.72})`);
          gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
          context.fillStyle = gradient;
          context.fillRect(
            bin.x - this.radius,
            bin.y - this.radius,
            this.radius * 2,
            this.radius * 2,
          );
        });

        const image = context.getImageData(0, 0, this.canvas.width, this.canvas.height);
        const colors = getHeatColorLookup();
        for (let index = 0; index < image.data.length; index += 4) {
          const alpha = image.data[index + 3];
          if (alpha < 4) {
            image.data[index + 3] = 0;
            continue;
          }
          const intensity = Math.min(255, Math.round(alpha * 2));
          const color = colors[intensity];
          image.data[index] = color[0];
          image.data[index + 1] = color[1];
          image.data[index + 2] = color[2];
          image.data[index + 3] = Math.min(210, Math.round(alpha * 1.08));
        }
        context.setTransform(1, 0, 0, 1, 0, 0);
        context.putImageData(image, 0, 0);
      }

      onRemove() {
        if (this.drawFrame) window.cancelAnimationFrame(this.drawFrame);
        this.container?.remove();
        this.container = null;
        this.canvas = null;
        this.drawFrame = null;
      }
    };

    return HeatmapOverlay;
  }

  function countByTown(points) {
    const counts = new Map();
    points.forEach((point) => {
      const town = normalizeTown(point.town);
      if (!town || town === "unknown") return;
      counts.set(town, (counts.get(town) || 0) + 1);
    });
    return counts;
  }

  function activitySummary(points) {
    const counts = countByTown(points);
    const busiestEntry = [...counts.entries()].sort((a, b) => b[1] - a[1])[0];

    elSummaryCount.textContent = points.length.toLocaleString();
    elSummaryBusiest.textContent = busiestEntry ? displayTown(busiestEntry[0]) : "—";
    elSummaryTowns.textContent = counts.size.toLocaleString();
    elPointCount.textContent = points.length
      ? `${points.length.toLocaleString()} mapped call${points.length === 1 ? "" : "s"} ${rangeLabel(selectedRange)}`
      : `No mapped calls ${rangeLabel(selectedRange)}`;

    return counts;
  }

  function townInfoHTML(town, filteredCount) {
    const slug = normalizeTown(town.name);
    return `<div class="iw-town">
      <div class="iw-town-name">${escapeHTML(displayTown(town.name))}</div>
      <div class="iw-town-row">Mapped in this view: <span>${Number(filteredCount || 0).toLocaleString()}</span></div>
      <div class="iw-town-row">All calls logged: <span>${Number(town.call_count || 0).toLocaleString()}</span></div>
      <div class="iw-town-row">Known streets: <span>${Number(town.street_count || 0).toLocaleString()}</span></div>
      <a class="iw-town-link" href="/scanner/town?town=${encodeURIComponent(slug)}">Open town scanner →</a>
    </div>`;
  }

  function renderTownMarkers(towns, filteredCounts) {
    townMarkers.forEach((marker) => marker.setMap(null));
    townMarkers = [];

    const selectedTown = normalizeTown(elTown.value);
    const maxCount = Math.max(1, ...filteredCounts.values());

    towns.forEach((town) => {
      const slug = normalizeTown(town.name);
      const count = filteredCounts.get(slug) || 0;
      const selected = selectedTown !== "all" && selectedTown === slug;
      const active = count > 0;
      const scale = active ? 5 + Math.min(4, Math.round((count / maxCount) * 4)) : 4;

      let marker;
      const MarkerOverlay = getTownMarkerOverlayClass();
      const onClick = () => {
        infoWindow.setContent(townInfoHTML(town, count));
        infoWindow.setPosition(marker.getPosition());
        infoWindow.open({ map });
      };
      marker = new MarkerOverlay({
        position: { lat: Number(town.lat), lng: Number(town.lng) },
        map: elTogglePins.checked ? map : null,
        title: `${displayTown(town.name)} — ${count} mapped calls`,
        label: displayTown(town.name),
        selected,
        active,
        scale,
        zIndex: selected ? 40 : (active ? 30 : 20),
        onClick,
      });
      townMarkers.push(marker);
    });
  }

  function buildHeatLayer(points) {
    if (heatLayer) {
      heatLayer.setMap(null);
      heatLayer = null;
    }
    if (!points.length) return;

    const OverlayClass = getHeatmapOverlayClass();
    heatLayer = new OverlayClass(points, {
      radius: window.matchMedia("(max-width: 767px)").matches ? 26 : 34,
    });
    if (elToggleHeat.checked) heatLayer.setMap(map);
  }

  function renderTownList(towns, filteredCounts = new Map()) {
    if (!towns.length) {
      elTownList.innerHTML = '<p class="text-slate-500 italic text-sm col-span-full">No town activity is available.</p>';
      return;
    }

    const selectedTown = normalizeTown(elTown.value);
    const maxCount = Math.max(1, ...filteredCounts.values());
    const sorted = [...towns].sort((a, b) => {
      const difference = (filteredCounts.get(normalizeTown(b.name)) || 0) - (filteredCounts.get(normalizeTown(a.name)) || 0);
      return difference || displayTown(a.name).localeCompare(displayTown(b.name));
    });

    elTownList.innerHTML = sorted.map((town) => {
      const slug = normalizeTown(town.name);
      const count = filteredCounts.get(slug) || 0;
      const width = count > 0 ? Math.max(6, Math.round((count / maxCount) * 100)) : 0;
      const activeClass = selectedTown === slug ? " is-active" : "";
      return `<button
          type="button"
          class="hm-town-card${activeClass}"
          data-town="${escapeHTML(slug)}"
          aria-label="Focus map on ${escapeHTML(displayTown(town.name))}, ${count} mapped calls">
        <span class="hm-town-card-top">
          <span class="hm-town-name">${escapeHTML(displayTown(town.name))}</span>
          <span class="hm-town-count">${count.toLocaleString()}</span>
        </span>
        <span class="hm-town-bar" aria-hidden="true"><span style="--activity-width:${width}%"></span></span>
        <span class="hm-town-meta">${Number(town.street_count || 0).toLocaleString()} known streets</span>
      </button>`;
    }).join("");
  }

  function setLoading(loading) {
    elLoading.classList.toggle("hidden", !loading);
  }

  function showUnavailable(show) {
    elUnavailable.classList.toggle("hidden", !show);
  }

  function setEmpty(show) {
    elEmpty.classList.toggle("hidden", !show);
  }

  function validPoints(rawPoints) {
    return (rawPoints || []).filter((point) => (
      Number.isFinite(Number(point.lat)) &&
      Number.isFinite(Number(point.lng)) &&
      Math.abs(Number(point.lat)) <= 90 &&
      Math.abs(Number(point.lng)) <= 180
    ));
  }

  function fitPoints(points) {
    if (!map || !points.length) return;
    const bounds = new google.maps.LatLngBounds();
    points.forEach((point) => bounds.extend({ lat: Number(point.lat), lng: Number(point.lng) }));
    map.fitBounds(bounds, { top: 70, right: 55, bottom: 80, left: 55 });
    google.maps.event.addListenerOnce(map, "idle", () => {
      if ((map.getZoom() || 0) > 15) map.setZoom(15);
    });
  }

  function fitCoverage() {
    if (map && coverageBounds && !coverageBounds.isEmpty()) {
      map.fitBounds(coverageBounds, { top: 45, right: 45, bottom: 45, left: 45 });
    }
  }

  async function loadCallCoords(options = {}) {
    if (!map) return;
    setLoading(true);
    setEmpty(false);
    showUnavailable(false);

    const town = normalizeTown(elTown.value) || "all";
    const params = new URLSearchParams({
      range: selectedRange,
      town,
      department: selectedDepartment,
    });

    try {
      const response = await fetch(`/scanner/api/call_coords?${params}`, { cache: "no-store" });
      if (!response.ok) throw new Error(`Call location request failed with ${response.status}`);
      const data = await response.json();
      currentPoints = validPoints(data.points);

      const counts = activitySummary(currentPoints);
      buildHeatLayer(currentPoints);
      renderTownMarkers(allTowns, counts);
      renderTownList(allTowns, counts);
      setEmpty(currentPoints.length === 0);

      if (currentPoints.length) {
        if (town !== "all" || options.fitPoints) fitPoints(currentPoints);
        else if (options.resetView) fitCoverage();
      } else if (town === "all") {
        fitCoverage();
      }
    } catch (error) {
      console.error("[HeatMap] Could not load call coordinates:", error);
      currentPoints = [];
      activitySummary([]);
      buildHeatLayer([]);
      renderTownMarkers(allTowns, new Map());
      renderTownList(allTowns, new Map());
      showUnavailable(true);
    } finally {
      setLoading(false);
    }
  }

  async function loadTownData() {
    const response = await fetch("/scanner/api/geo_towns", { cache: "no-store" });
    if (!response.ok) throw new Error(`Town location request failed with ${response.status}`);
    const data = await response.json();
    return (data.towns || []).filter((town) => (
      Number.isFinite(Number(town.lat)) && Number.isFinite(Number(town.lng))
    ));
  }

  function populateTownFilter(towns) {
    const fragment = document.createDocumentFragment();
    towns.forEach((town) => {
      const option = document.createElement("option");
      option.value = normalizeTown(town.name);
      option.textContent = displayTown(town.name);
      fragment.appendChild(option);
    });
    elTown.appendChild(fragment);
  }

  function setSegmentActive(container, attribute, value) {
    container.querySelectorAll(`[${attribute}]`).forEach((button) => {
      const active = button.getAttribute(attribute) === value;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  function bindControls() {
    const rangeSegments = document.getElementById("range-segments");
    const departmentSegments = document.getElementById("department-segments");

    rangeSegments.addEventListener("click", (event) => {
      const button = event.target.closest("[data-range]");
      if (!button || button.dataset.range === selectedRange) return;
      selectedRange = button.dataset.range;
      setSegmentActive(rangeSegments, "data-range", selectedRange);
      loadCallCoords({ fitPoints: elTown.value !== "all", resetView: elTown.value === "all" });
    });

    departmentSegments.addEventListener("click", (event) => {
      const button = event.target.closest("[data-department]");
      if (!button || button.dataset.department === selectedDepartment) return;
      selectedDepartment = button.dataset.department;
      setSegmentActive(departmentSegments, "data-department", selectedDepartment);
      loadCallCoords({ fitPoints: elTown.value !== "all" });
    });

    elTown.addEventListener("change", () => {
      loadCallCoords({ fitPoints: elTown.value !== "all", resetView: elTown.value === "all" });
    });

    elTownList.addEventListener("click", (event) => {
      const card = event.target.closest("[data-town]");
      if (!card) return;
      const town = card.dataset.town;
      elTown.value = elTown.value === town ? "all" : town;
      loadCallCoords({ fitPoints: elTown.value !== "all", resetView: elTown.value === "all" });
    });

    elTogglePins.addEventListener("change", () => {
      townMarkers.forEach((marker) => marker.setMap(elTogglePins.checked ? map : null));
    });

    elToggleHeat.addEventListener("change", () => {
      if (heatLayer) heatLayer.setMap(elToggleHeat.checked ? map : null);
    });

    document.getElementById("reset-map-view").addEventListener("click", () => {
      const hadTownFilter = elTown.value !== "all";
      elTown.value = "all";
      if (hadTownFilter) loadCallCoords({ resetView: true });
      else fitCoverage();
    });
  }

  async function main() {
    elTown = document.getElementById("filter-town");
    elPointCount = document.getElementById("point-count");
    elLoading = document.getElementById("loading-overlay");
    elEmpty = document.getElementById("empty-overlay");
    elTogglePins = document.getElementById("toggle-town-pins");
    elToggleHeat = document.getElementById("toggle-heatmap");
    elUnavailable = document.getElementById("map-unavailable");
    elSummaryCount = document.getElementById("hm-summary-count");
    elSummaryBusiest = document.getElementById("hm-summary-busiest");
    elSummaryTowns = document.getElementById("hm-summary-towns");
    elTownList = document.getElementById("town-list");

    const apiKey = window.GOOGLE_MAPS_API_KEY || "";

    try {
      allTowns = await loadTownData();
      populateTownFilter(allTowns);
      renderTownList(allTowns);
    } catch (error) {
      console.error("[HeatMap] Could not load town data:", error);
      elTownList.innerHTML = '<p class="text-slate-500 text-sm col-span-full">Town activity is temporarily unavailable.</p>';
    }

    if (!apiKey) {
      showUnavailable(true);
      setLoading(false);
      return;
    }

    try {
      await loadMapsAPI(apiKey);
      initMap();

      coverageBounds = new google.maps.LatLngBounds();
      allTowns.forEach((town) => coverageBounds.extend({
        lat: Number(town.lat),
        lng: Number(town.lng),
      }));
      fitCoverage();

      bindControls();
      await loadCallCoords();
    } catch (error) {
      console.error("[HeatMap] Map startup failed:", error);
      showUnavailable(true);
      setLoading(false);
    }
  }

  window.gm_authFailure = () => {
    showUnavailable(true);
    setLoading(false);
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main);
  } else {
    main();
  }
})();
