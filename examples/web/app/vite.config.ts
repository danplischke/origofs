import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The origofs document server (examples/web/server) runs on :8000. We proxy /fs and
// /api to it so the app can use same-origin relative URLs in dev (no CORS, and
// the SSE feed streams through cleanly).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/fs": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/api": { target: "http://127.0.0.1:8000", changeOrigin: true },
    },
  },
});
