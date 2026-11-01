import type { Metadata, Viewport } from "next";
import "./globals.css";

export const metadata: Metadata = { title: "Radar de Ofertas", description: "Acompanhe preços e receba alertas de ofertas." };
export const viewport: Viewport = { themeColor: "#0f172a", width: "device-width", initialScale: 1 };

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="pt-BR"><body>{children}<script dangerouslySetInnerHTML={{ __html: `if ('serviceWorker' in navigator) window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js').catch(() => {}));` }} /></body></html>;
}
