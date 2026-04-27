import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const operatorApiProxyTarget =
  process.env.OPERATOR_API_URL ??
  process.env.VITE_OPERATOR_API_PROXY_TARGET ??
  'http://127.0.0.1:18000';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      '/api': {
        target: operatorApiProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
