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
          A: '#5a9e72',
          B: '#3d9788',
          C: '#c49020',
          D: '#c96040',
          E: '#b84040',
        },
      },
    },
  },
  plugins: [],
}
