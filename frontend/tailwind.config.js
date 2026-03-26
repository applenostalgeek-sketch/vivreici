/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,jsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        display: ['Bricolage Grotesque', 'system-ui', 'sans-serif'],
        sans: ['Epilogue', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
      colors: {
        paper: '#F8F4EE',
        ink: '#1C1917',
        'ink-light': '#6B5E52',
        border: '#E5DDD1',
        surface: '#FFFFFF',
        score: {
          A: '#16A34A',
          B: '#65A30D',
          C: '#CA8A04',
          D: '#EA580C',
          E: '#DC2626',
        },
      },
    },
  },
  plugins: [],
}
