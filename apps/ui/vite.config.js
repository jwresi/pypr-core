import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

function rewriteLegacyJakeApi(path) {
  const url = new URL(path, "http://localhost");
  const params = url.searchParams;

  if (url.pathname === "/api/site-summary" && params.get("site_id")) {
    const siteId = params.get("site_id");
    params.delete("site_id");
    return `/v1/jake/sites/${siteId}/summary${params.toString() ? `?${params.toString()}` : ""}`;
  }

  if (url.pathname === "/api/site-topology" && params.get("site_id")) {
    const siteId = params.get("site_id");
    params.delete("site_id");
    return `/v1/jake/sites/${siteId}/topology${params.toString() ? `?${params.toString()}` : ""}`;
  }

  if (url.pathname === "/api/building-health" && params.get("building_id")) {
    const buildingId = params.get("building_id");
    params.delete("building_id");
    return `/v1/jake/buildings/${buildingId}/health${params.toString() ? `?${params.toString()}` : ""}`;
  }

  if (url.pathname === "/api/building-customer-count" && params.get("building_id")) {
    const buildingId = params.get("building_id");
    params.delete("building_id");
    return `/v1/jake/buildings/${buildingId}/customers${params.toString() ? `?${params.toString()}` : ""}`;
  }

  if (url.pathname === "/api/building-model" && params.get("building_id")) {
    const buildingId = params.get("building_id");
    params.delete("building_id");
    return `/v1/jake/buildings/${buildingId}/model${params.toString() ? `?${params.toString()}` : ""}`;
  }

  if (url.pathname === "/api/building-flap-history" && params.get("building_id")) {
    const buildingId = params.get("building_id");
    params.delete("building_id");
    return `/v1/jake/buildings/${buildingId}/flap-history${params.toString() ? `?${params.toString()}` : ""}`;
  }

  if (url.pathname === "/api/rogue-dhcp-suspects") {
    return `/v1/jake/rogue-dhcp-suspects${params.toString() ? `?${params.toString()}` : ""}`;
  }

  if (url.pathname === "/api/recovery-ready-cpes") {
    return `/v1/jake/recovery-ready-cpes${params.toString() ? `?${params.toString()}` : ""}`;
  }

  return path;
}

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 4174,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8787",
        changeOrigin: true,
        rewrite: rewriteLegacyJakeApi,
      },
      "/v1": {
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
