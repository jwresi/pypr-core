import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 4174,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8787",
        changeOrigin: true,
      },
      "/tiles": {
        target: "https://tile.openstreetmap.org",
        changeOrigin: true,
        rewrite: (path) => {
          const parts = path.split("/").filter(Boolean).slice(1);
          return `/${parts.join("/")}`;
        },
      },
    },
  },
});
