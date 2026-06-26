/* dashboard.js — Modular Scanner Dashboard shared helpers */

function refreshPage() {
  location.reload();
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/* Highlight the current nav link */
(function () {
  const path = location.pathname.replace(/\/$/, '') || '/';
  document.querySelectorAll('.site-nav a').forEach(a => {
    const href = a.getAttribute('href').replace(/\/$/, '') || '/';
    if (href === path) {
      a.style.color = 'var(--accent)';
      a.style.fontWeight = '600';
    }
  });
})();
