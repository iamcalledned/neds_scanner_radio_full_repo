/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './web/templates/**/*.html',
    './web/static/js/**/*.js',
  ],
  theme: {
    extend: {
      colors: {
        pd: '#1e3a8a',
        fd: '#7f1d1d',
        bg: '#07101d',
        scannerBlue: '#38bdf8',
        scannerGlow: '#7dd3fc',
        scannerRed: '#fb7185',
      },
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
        mono: ['Roboto Mono', 'monospace'],
        display: ['Space Grotesk', 'sans-serif'],
      },
      boxShadow: {
        scanner: '0 20px 48px rgba(2, 8, 23, 0.45)',
      },
    },
  },
  plugins: [],
};
