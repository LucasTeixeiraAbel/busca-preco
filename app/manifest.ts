import type { MetadataRoute } from "next";

export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "Radar de Ofertas", short_name: "Radar", description: "Alertas de preços e ofertas verificadas.",
    start_url: "/", display: "standalone", background_color: "#f8fafc", theme_color: "#0f172a",
    icons: [{ src: "/icon.svg", sizes: "any", type: "image/svg+xml", purpose: "maskable" }],
  };
}
