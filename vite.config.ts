import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  root: '.',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src/frontend'),
    },
  },
  server: {
    allowedHosts: ['operone.duckdns.org'],
    port: 28588,
    proxy: {
      '/api': 'http://localhost:28580',
      '/ws': {
        target: 'ws://localhost:28580',
        ws: true,
      },
    },
  },
  build: {
    outDir: 'dist',
  },
})
