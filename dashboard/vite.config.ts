import { defineConfig } from 'vite';

// Built assets are served by the proxy under /dashboard/, so every emitted
// URL must be prefixed accordingly. Dev server proxies /api to the local
// proxy so the dashboard can be developed against a running instance.
export default defineConfig({
  base: '/dashboard/',
  build: {
    target: 'es2022',
    assetsDir: 'assets',
    modulePreload: { polyfill: false },
    sourcemap: false,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:9997',
        changeOrigin: true,
      },
    },
  },
});
