"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ProductMonitor, { ProductOffer } from "@/components/ProductMonitor";

type Watchlist = { product_ids: string[] };

export default function OfferFeed() {
  const [items, setItems] = useState<ProductOffer[]>([]);
  const [starredIds, setStarredIds] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [message, setMessage] = useState("");

  const refresh = useCallback(async () => {
    try {
      const response = await fetch("/api/feed", { cache: "no-store" });
      if (!response.ok) throw new Error("Feed indisponível");
      const data: ProductOffer[] = await response.json();
      setItems(data);
      setError(false);
    } catch {
      setError(true);
    } finally {
      setLoading(false);
    }
  }, []);

  const refreshWatchlist = useCallback(async () => {
    const response = await fetch("/api/watchlist", { cache: "no-store" });
    if (!response.ok) return;
    const data = await response.json() as Watchlist;
    setStarredIds(new Set(data.product_ids));
  }, []);

  useEffect(() => {
    void refresh();
    void refreshWatchlist();
    const timer = window.setInterval(() => { void refresh(); void refreshWatchlist(); }, 15_000);
    return () => window.clearInterval(timer);
  }, [refresh, refreshWatchlist]);

  const groups = useMemo(() => {
    const grouped = new Map<string, ProductOffer[]>();
    for (const item of items) grouped.set(item.product_id, [...(grouped.get(item.product_id) ?? []), item]);
    return Array.from(grouped.entries()).map(([productId, offers]) => ({ productId, offers, title: offers[0]?.title || productId }))
      .filter((group) => !watchlistOnly || starredIds.has(group.productId))
      .sort((a, b) => Number(b.offers.some((offer) => offer.anomaly)) - Number(a.offers.some((offer) => offer.anomaly)) || a.title.localeCompare(b.title));
  }, [items, starredIds, watchlistOnly]);

  async function toggleStar(productId: string, next: boolean) {
    setMessage("");
    try {
      const response = await fetch(`/api/watchlist/${encodeURIComponent(productId)}`, { method: next ? "PUT" : "DELETE" });
      if (response.status === 401) throw new Error("Entre na sua conta para salvar produtos acompanhados.");
      if (!response.ok) throw new Error("Não foi possível atualizar sua lista de acompanhamento.");
      setStarredIds((current) => {
        const updated = new Set(current);
        if (next) updated.add(productId); else updated.delete(productId);
        return updated;
      });
    } catch (cause) {
      setMessage(cause instanceof Error ? cause.message : "Falha ao salvar acompanhamento.");
    }
  }

  if (loading) return <p className="py-12 text-center text-slate-500">Carregando ofertas...</p>;
  if (error && items.length === 0) return <p role="alert" className="rounded-xl bg-amber-50 p-5 text-amber-900">Não foi possível carregar o feed. Tente novamente em alguns instantes.</p>;

  return <div className="space-y-4">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="text-sm text-slate-600">{groups.length} {groups.length === 1 ? "produto" : "produtos"} · atualiza a cada 15 s</p>
      <button type="button" aria-pressed={watchlistOnly} onClick={() => setWatchlistOnly((value) => !value)} className={`min-h-11 rounded-xl border px-4 font-semibold ${watchlistOnly ? "border-amber-300 bg-amber-50 text-amber-900" : "border-slate-300 bg-white text-slate-700"}`}>
        {watchlistOnly ? "★ Minha lista" : "☆ Ver minha lista"}
      </button>
    </div>
    {message ? <p role="alert" className="rounded-lg bg-amber-50 p-3 text-sm text-amber-900">{message}</p> : null}
    {groups.length === 0 ? <p className="rounded-xl border border-dashed border-slate-300 p-8 text-center text-slate-600">{watchlistOnly ? "Sua lista está vazia. Marque produtos com a estrela para acompanhá-los." : "Nenhum produto coletado ainda."}</p> : <section aria-label="Produtos monitorados" className="grid grid-cols-1 gap-4 xl:grid-cols-2">
      {groups.map((group) => <ProductMonitor key={group.productId} productId={group.productId} title={group.title} offers={group.offers} starred={starredIds.has(group.productId)} onToggleStar={toggleStar} />)}
    </section>}
  </div>;
}
