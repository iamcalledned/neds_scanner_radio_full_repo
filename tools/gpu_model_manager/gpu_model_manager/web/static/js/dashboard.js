/* GPU Model Manager — dashboard.js
   Minimal vanilla JS. No framework. No build chain. */

"use strict";

// Auto-refresh the /api/status endpoint and patch VRAM bar on dashboard
function initDashboard() {
  const vramBar = document.querySelector('.vram-bar');
  if (!vramBar) return;

  async function refreshVram() {
    try {
      const resp = await fetch('/api/gpu');
      if (!resp.ok) return;
      const gpu = await resp.json();
      if (!gpu.available) return;
      const pct = (gpu.memory_used_mb / gpu.memory_total_mb * 100).toFixed(1);
      vramBar.style.width = pct + '%';
    } catch (_) { /* silent fail */ }
  }

  // Refresh VRAM bar every 15 seconds
  setInterval(refreshVram, 15000);
}

document.addEventListener('DOMContentLoaded', initDashboard);
