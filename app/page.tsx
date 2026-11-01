import AuthAndAlerts from "@/components/AuthAndAlerts";
import OfferFeed from "@/components/OfferFeed";

export default function Home() {
  return <main className="mx-auto min-h-screen max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
    <header className="mb-8 flex items-center justify-between"><div><p className="text-sm font-semibold uppercase tracking-widest text-emerald-700">Radar</p><h1 className="mt-1 text-3xl font-bold tracking-tight">Ofertas verificadas</h1></div><span className="rounded-full bg-emerald-100 px-3 py-1 text-sm font-medium text-emerald-800">● Ao vivo</span></header>
    <AuthAndAlerts />
    <OfferFeed />
  </main>;
}
