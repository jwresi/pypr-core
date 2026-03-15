import React from "react";
import ReactDOM from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";
import NychaNoc from "./nycha-noc";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <NychaNoc />
  </React.StrictMode>,
);
