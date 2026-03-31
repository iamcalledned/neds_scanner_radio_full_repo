/* scanner_heatmap.js — Geographic call activity map with heat layer + town pins */

(function () {
  "use strict";

  // ── Constants ────────────────────────────────────────────────────
  const COLOR_ACTIVE   = "#38bdf8";
  const COLOR_INACTIVE = "#475569";
  const STROKE_COLOR   = "#0f172a";

  // ── State ────────────────────────────────────────────────────────
  let map         = null;
  let heatLayer   = null;
  let townMarkers = [];
  let infoWindow  = null;
  let allTowns    = [];     // from geo_towns API

  // ── DOM refs (set after DOMContentLoaded) ────────────────────────
  let elRange, elTown, elPointCount, elLoading, elTogglePins, elToggleHeat;

  // ── Load Google Maps JS API ──────────────────────────────────────
  function loadMapsAPI(apiKey) {
    return new Promise((resolve, reject) => {
      if (window.google && window.google.maps) { resolve(); return; }
      const s = document.createElement("script");
      s.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(apiKey)}&libraries=visualization&callback=__mapsReady`;
      s.async = true;
      s.onerror = () => reject(new Error("Failed to load Google Maps API"));
      window.__mapsReady = resolve;
      document.head.appendChild(s);
    });
  }

  // ── Dark map style ───────────────────────────────────────────────
  function darkMapStyles() {
    return [
      { elementType: "geometry",   stylers: [{ color: "#0f172a" }] },
      { elementType: "labels.text.stroke", stylers: [{ color: "#0f172a" }] },
      { elementType: "labels.text.fill",   stylers: [{ color: "#64748b" }] },
      { featureType: "road",         elementType: "geometry",           stylers: [{ color: "#1e293b" }] },
      { featureType: "road",         elementType: "geometry.stroke",    stylers: [{ color: "#0f172a" }] },
      { featureType: "road",         elementType: "labels.text.fill",   stylers: [{ color: "#475569" }] },
      { featureType: "road.highway", elementType: "geometry",           stylers: [{ color: "#334155" }] },
      { featureType: "road.highway", elementType: "labels.text.fill",   stylers: [{ color: "#94a3b8" }] },
      { featureType: "water",        elementType: "geometry",           stylers: [{ color: "#020617" }] },
      { featureType: "water",        elementType: "labels.text.fill",   stylers: [{ color: "#1e3a5f" }] },
      { featureType: "poi",          elementType: "geometry",           stylers: [{ color: "#0f172a" }] },
      { featureType: "poi",          elementType: "labels.text.fill",   stylers: [{ color: "#334155" }] },
      { featureType: "poi.park",     elementType: "geometry",           stylers: [{ color: "#0a1628" }] },
      { featureType: "transit",      elementType: "geometry",           stylers: [{ color: "#0f172a" }] },
      { featureType: "administrative",             elementType: "geometry.stroke",  stylers: [{ color: "#1e293b" }] },
      { featureType: "administrative.land_parcel", elementType: "labels.text.fill", stylers: [{ color: "#334155" }] },
    ];
  }

  // ── Initialise the map (once, on first load) ─────────────────────
  function initMap() {
    map = new google.maps.Map(document.getElementById("map"), {
      center: { lat: 42.13, lng: -71.52 },
      zoom: 11,
      styles: darkMapStyles(),
      mapTypeControl: false,
      streetViewControl: false,
      fullscreenControl: true,
    });
    infoWindow = new google.maps.InfoWindow({ maxWidth: 220 });
  }

  // ── Town centroid markers ────────────────────────────────────────
  function dotRadius(callCount) {
    if (!callCount) return 6;
    return Math.min(6 + Math.sqrt(callCount) * 1.2, 28);
  }

  function infoHTML(town) {
    const slug = town.name.toLowerCase();
    return `<div style="font-family:Inter,sans-serif;padding:4px 2px;min-width:160px">
      <div style="font-size:1rem;font-weight:600;color:#f8fafc;margin-bottom:6px;text-transform:capitalize">${town.name}</div>
      <div style="font-size:.8rem;color:#94a3b8;margin:2px 0">Streets: <span style="color:#38bdf8;font-weight:600">${town.street_count.toLocaleString()}</span></div>
      <div style="font-size:.8rem;color:#94a3b8;margin:2px 0">Calls logged: <span style="color:#38bdf8;font-weight:600">${town.call_count.toLocaleString()}</span></div>
      <a style="display:inline-block;margin-top:8px;font-size:.8rem;color:#38bdf8;text-decoration:none"
         href="/scanner/town?town=${encodeURIComponent(slug)}">View scanner feed &rarr;</a>
    </div>`;
  }

  function renderTownMarkers(towns) {
    townMarkers.forEach(m => m.setMap(null));
    townMarkers = [];

    towns.forEach(town => {
      const hasActivity = town.call_count > 0;
      const marker = new google.maps.Marker({
        position: { lat: town.lat, lng: town.lng },
        map: elTogglePins.checked ? map : null,
        title: town.name,
        icon: {
          path:        google.maps.SymbolPath.CIRCLE,
          scale:       dotRadius(town.call_count),
          fillColor:   hasActivity ? COLOR_ACTIVE : COLOR_INACTIVE,
          fillOpacity: hasActivity ? 0.85 : 0.5,
          strokeColor: STROKE_COLOR,
          strokeWeight: 2,
        },
        zIndex: hasActivity ? 10 : 5,
      });
      marker.addListener("click", () => {
        infoWindow.setContent(infoHTML(town));
        infoWindow.open(map, marker);
      });
      townMarkers.push(marker);
    });
  }

  // ── Heat layer ───────────────────────────────────────────────────
  function buildHeatLayer(points) {
    if (heatLayer) {
      heatLayer.setMap(null);
      heatLayer = null;
    }
    if (!points.length) return;

    const latLngs = points.map(p => new google.maps.LatLng(p.lat, p.lng));
    heatLayer = new google.maps.visualization.HeatmapLayer({
      data: latLngs,
      map: elToggleHeat.checked ? map : null,
      radius: 20,
      opacity: 0.75,
      gradient: [
        "rgba(0,0,0,0)",
        "rgba(14,165,233,0.4)",
        "rgba(56,189,248,0.6)",
        "rgba(125,211,252,0.75)",
        "rgba(186,230,253,0.85)",
        "rgba(255,255,255,1)",
      ],
    });
  }

  // ── Fetch call coords and refresh heat layer ─────────────────────
  async function loadCallCoords() {
    setLoading(true);
    const range = elRange.value;
    const town  = elTown.value;
    const url   = `/scanner/api/call_coords?range=${range}&town=${encodeURIComponent(town)}`;

    try {
      const res  = await fetch(url);
      const data = await res.json();
      const pts  = data.points || [];

      buildHeatLayer(pts);

      elPointCount.textContent = pts.length
        ? `${pts.length.toLocaleString()} call${pts.length === 1 ? "" : "s"} plotted`
        : "No geocoded calls for this filter";

      // Auto-fit bounds when filtering to a specific town
      if (town !== "all" && pts.length > 0) {
        const bounds = new google.maps.LatLngBounds();
        pts.forEach(p => bounds.extend(p));
        map.fitBounds(bounds, { top: 60, right: 60, bottom: 60, left: 60 });
      }
    } catch (err) {
      console.error("call_coords fetch failed:", err);
      elPointCount.textContent = "Error loading data";
    } finally {
      setLoading(false);
    }
  }

  // ── Fetch town geo data ──────────────────────────────────────────
  async function loadTownData() {
    try {
      const res  = await fetch("/scanner/api/geo_towns");
      const data = await res.json();
      return data.towns || [];
    } catch (err) {
      console.error("geo_towns fetch failed:", err);
      return [];
    }
  }

  // ── Populate town filter dropdown ────────────────────────────────
  function populateTownFilter(towns) {
    const sel = elTown;
    // keep "All Towns" option, append the rest
    towns.forEach(t => {
      const opt = document.createElement("option");
      opt.value = t.name.toLowerCase();
      opt.textContent = t.name.charAt(0).toUpperCase() + t.name.slice(1).toLowerCase();
      sel.appendChild(opt);
    });
  }

  // ── Town grid below the map ──────────────────────────────────────
  function renderTownList(towns) {
    const container = document.getElementById("town-list");
    if (!towns.length) {
      container.innerHTML = '<p class="text-slate-500 italic text-sm col-span-full">No town data found.</p>';
      return;
    }
    container.innerHTML = towns.map(t => {
      const slug = t.name.toLowerCase();
      return `<a href="/scanner/town?town=${encodeURIComponent(slug)}"
                 class="flex flex-col gap-0.5 px-3 py-2 rounded-lg bg-slate-800/50 hover:bg-slate-700/60 transition group">
        <span class="text-sm font-medium text-slate-200 capitalize group-hover:text-scannerBlue transition">${t.name}</span>
        <span class="text-[0.7rem] text-slate-500">${t.street_count.toLocaleString()} streets</span>
      </a>`;
    }).join("");
  }

  // ── Loading overlay ──────────────────────────────────────────────
  function setLoading(on) {
    elLoading.classList.toggle("visible", on);
  }

  // ── Toggle visibility helpers ────────────────────────────────────
  function applyTownPinsToggle() {
    const show = elTogglePins.checked;
    townMarkers.forEach(m => m.setMap(show ? map : null));
  }

  function applyHeatToggle() {
    if (!heatLayer) return;
    heatLayer.setMap(elToggleHeat.checked ? map : null);
  }

  // ── Entry point ──────────────────────────────────────────────────
  async function main() {
    // Grab DOM refs
    elRange       = document.getElementById("filter-range");
    elTown        = document.getElementById("filter-town");
    elPointCount  = document.getElementById("point-count");
    elLoading     = document.getElementById("loading-overlay");
    elTogglePins  = document.getElementById("toggle-town-pins");
    elToggleHeat  = document.getElementById("toggle-heatmap");

    const apiKey = window.GOOGLE_MAPS_API_KEY || "";

    if (!apiKey) {
      document.getElementById("no-api-key-banner").style.display = "block";
      document.getElementById("map").innerHTML =
        '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#64748b;font-size:.875rem;font-style:italic">Map unavailable — no API key configured.</div>';
    }

    // Load town data + Maps API in parallel
    const [towns] = await Promise.all([
      loadTownData(),
      apiKey ? loadMapsAPI(apiKey).catch(err => console.error("Maps API load failed:", err)) : Promise.resolve(),
    ]);

    allTowns = towns;
    populateTownFilter(towns);
    renderTownList(towns);

    if (!apiKey || !window.google || !window.google.maps) return;

    initMap();
    renderTownMarkers(towns);

    // Fit map to town bounds initially
    if (towns.length > 0) {
      const bounds = new google.maps.LatLngBounds();
      towns.forEach(t => bounds.extend({ lat: t.lat, lng: t.lng }));
      map.fitBounds(bounds, { top: 40, right: 40, bottom: 40, left: 40 });
    }

    // Load initial call coords (last 7 days, all towns)
    await loadCallCoords();

    // Filter change handlers
    elRange.addEventListener("change", loadCallCoords);
    elTown.addEventListener("change", () => {
      // When switching to a specific town, re-fit after load
      loadCallCoords();
    });

    // Toggle handlers
    elTogglePins.addEventListener("change", applyTownPinsToggle);
    elToggleHeat.addEventListener("change", applyHeatToggle);
  }

  // ── Mobile menu toggle ───────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", () => {
    const btn      = document.getElementById("menu-btn");
    const dropdown = document.getElementById("menu-dropdown");
    if (btn && dropdown) {
      btn.addEventListener("click", e => {
        e.stopPropagation();
        dropdown.classList.toggle("hidden");
      });
      document.addEventListener("click", () => dropdown.classList.add("hidden"));
    }

    main();
  });

})();
