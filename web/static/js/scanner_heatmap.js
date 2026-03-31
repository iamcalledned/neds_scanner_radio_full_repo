/* scanner_heatmap.js — Geographic town coverage map */

(function () {
  "use strict";

  // ── Marker color palette ─────────────────────────────────────────
  // Towns with calls get a brighter blue; others get a muted slate dot.
  const COLOR_ACTIVE   = "#38bdf8";   // scannerBlue — has call data
  const COLOR_INACTIVE = "#475569";   // slate-600   — address data only
  const STROKE_COLOR   = "#0f172a";

  let map = null;
  let infoWindow = null;

  // ── Load Google Maps JS API dynamically ──────────────────────────
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

  // ── Fetch town geo data ──────────────────────────────────────────
  async function fetchTowns() {
    const res = await fetch("/scanner/api/geo_towns");
    if (!res.ok) throw new Error(`API error ${res.status}`);
    const data = await res.json();
    return data.towns || [];
  }

  // ── Update summary stats ─────────────────────────────────────────
  function updateStats(towns) {
    const totalStreets = towns.reduce((s, t) => s + (t.street_count || 0), 0);
    const totalCalls   = towns.reduce((s, t) => s + (t.call_count   || 0), 0);
    document.getElementById("stat-towns").textContent   = towns.length;
    document.getElementById("stat-streets").textContent = totalStreets.toLocaleString();
    document.getElementById("stat-calls").textContent   = totalCalls.toLocaleString();
  }

  // ── Render the town list below the map ───────────────────────────
  function renderTownList(towns) {
    const container = document.getElementById("town-list");
    if (!towns.length) {
      container.innerHTML = '<p class="text-slate-500 italic text-sm col-span-full">No town data found.</p>';
      return;
    }
    container.innerHTML = towns.map(t => {
      const townSlug = t.name.toLowerCase();
      return `<a href="/scanner/town?town=${encodeURIComponent(townSlug)}"
                 class="flex flex-col gap-0.5 px-3 py-2 rounded-lg bg-slate-800/50 hover:bg-slate-700/60 transition group">
        <span class="text-sm font-medium text-slate-200 capitalize group-hover:text-scannerBlue transition">${t.name}</span>
        <span class="text-[0.7rem] text-slate-500">${t.street_count.toLocaleString()} streets</span>
      </a>`;
    }).join("");
  }

  // ── Dot radius based on call count (min 6px, max 28px) ──────────
  function dotRadius(callCount) {
    if (!callCount) return 6;
    return Math.min(6 + Math.sqrt(callCount) * 1.2, 28);
  }

  // ── Build dark-mode Google Maps style ────────────────────────────
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
      { featureType: "administrative",              elementType: "geometry.stroke", stylers: [{ color: "#1e293b" }] },
      { featureType: "administrative.land_parcel",  elementType: "labels.text.fill", stylers: [{ color: "#334155" }] },
    ];
  }

  // ── Build info window HTML for a town ────────────────────────────
  function infoHTML(town) {
    const townSlug = town.name.toLowerCase();
    return `<div class="iw-town">
      <div class="iw-town-name">${town.name}</div>
      <div class="iw-town-row">Streets: <span>${town.street_count.toLocaleString()}</span></div>
      <div class="iw-town-row">Calls logged: <span>${town.call_count.toLocaleString()}</span></div>
      <a class="iw-town-link" href="/scanner/town?town=${encodeURIComponent(townSlug)}">View scanner feed &rarr;</a>
    </div>`;
  }

  // ── Initialize and render map ─────────────────────────────────────
  function initMap(towns) {
    // Center on south-central Massachusetts
    const center = { lat: 42.13, lng: -71.52 };

    map = new google.maps.Map(document.getElementById("map"), {
      center,
      zoom: 11,
      styles: darkMapStyles(),
      disableDefaultUI: false,
      zoomControl: true,
      mapTypeControl: false,
      streetViewControl: false,
      fullscreenControl: true,
    });

    infoWindow = new google.maps.InfoWindow({ maxWidth: 220 });

    towns.forEach(town => {
      const hasActivity = town.call_count > 0;
      const radius      = dotRadius(town.call_count);
      const fillColor   = hasActivity ? COLOR_ACTIVE : COLOR_INACTIVE;

      const marker = new google.maps.Marker({
        position: { lat: town.lat, lng: town.lng },
        map,
        title: town.name,
        icon: {
          path:          google.maps.SymbolPath.CIRCLE,
          scale:         radius,
          fillColor,
          fillOpacity:   hasActivity ? 0.85 : 0.5,
          strokeColor:   STROKE_COLOR,
          strokeWeight:  2,
        },
        zIndex: hasActivity ? 10 : 5,
      });

      marker.addListener("click", () => {
        infoWindow.setContent(infoHTML(town));
        infoWindow.open(map, marker);
      });
    });

    // Fit bounds to all town markers
    if (towns.length > 0) {
      const bounds = new google.maps.LatLngBounds();
      towns.forEach(t => bounds.extend({ lat: t.lat, lng: t.lng }));
      map.fitBounds(bounds, { top: 40, right: 40, bottom: 40, left: 40 });
    }
  }

  // ── Entry point ──────────────────────────────────────────────────
  async function main() {
    const apiKey = window.GOOGLE_MAPS_API_KEY || "";

    if (!apiKey) {
      document.getElementById("no-api-key-banner").style.display = "block";
      document.getElementById("map").innerHTML =
        '<div class="flex items-center justify-center h-full text-slate-500 text-sm italic">Map unavailable — no API key configured.</div>';
    }

    // Fetch town data in parallel with Maps API load
    const [towns] = await Promise.all([
      fetchTowns().catch(err => { console.error("geo_towns fetch failed:", err); return []; }),
      apiKey ? loadMapsAPI(apiKey).catch(err => { console.error("Maps API load failed:", err); }) : Promise.resolve(),
    ]);

    updateStats(towns);
    renderTownList(towns);

    if (apiKey && window.google && window.google.maps) {
      initMap(towns);
    }
  }

  document.addEventListener("DOMContentLoaded", main);

  // Mobile menu toggle (same pattern as other pages)
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
  });

})();
