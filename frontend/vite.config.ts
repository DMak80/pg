import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Сборка SPA: prod-бандл кладём в wwwroot Api, dev — проксируем /api на Kestrel (spec §6.2).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target: 'http://localhost:5050', changeOrigin: true },
    },
  },
  build: {
    outDir: '../src/AdminPanel.Api/wwwroot', // вне root — явно очищаем
    emptyOutDir: true,
  },
});
