"use client";

import { useEffect, useMemo, useState } from "react";

export type ProductOffer = {
  product_id: string;
  title?: string;
  store: string;
  price: number;
  previousPrice?: number | null;
  url: string;
  anomaly?: boolean;
  observed_at?: string;
};

type HistoryPoint = { day: string; store: string; low_price: number; high_price: number; average_price: number; closing_price: number };
type HistoryResponse = { available_from: string | null; points: HistoryPoint[] };

const trustedHosts = new Set(["amazon.com.br", "www.amazon.com.br", "kabum.com.br", "www.kabum.com.br"]);
const chartColors = ["#059669", "#2563eb", "#d97706", "#7c3aed", "#dc2626", "#0891b2"];

function safeUrl(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "https:" && trustedHosts.has(url.hostname.toLowerCase()) && !url.username && !url.password ? url.toString() : null;
  } catch { return null; }
}

function money(value: number): string {
  return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(value);
}

export default function ProductMonitor({
  productId,
  title,
  offers,
  starred,
  onToggleStar,
}: {
  productId: string;
  title: string;
  offers: ProductOffer[];
  starred: boolean;
  onToggleStar: (productId: string, next: boolean) => Promise<void>;
}) {
  const [historyOpen, setHistoryOpen] = useState(false);
  const [years, setYears] = useState(1);
  const [history, setHistory] = useState<HistoryResponse | null>(null);
  const [historyError, setHistoryError] = useState("");
  const [busy, setBusy] = useState(false);

  const latestOffers = useMemo(() => {
    const byUrl = new Map<string, ProductOffer>();
    for (const offer of [...offers].sort((a, b) => (b.observed_at ?? "").localeCompare(a.observed_at ?? ""))) {
      if (!byUrl.has(offer.url)) byUrl.set(offer.url, offer);
    }
    return Array.from(byUrl.values()).sort((a, b) => a.price - b.price);
  }, [offers]);

  useEffect(() => {
    if (!historyOpen || !starred) return;
    let cancelled = false;
    setHistoryError("");
    void fetch(`/api/products/${encodeURIComponent(productId)}/history?years=${years}`, { cache: "no-store" })
      .then(async (response) => {
        if (!response.ok) throw new Error(response.status === 401 ? "Entre na sua conta para consultar o histórico." : "Não foi possível carregar o histórico.");
        return response.json() as Promise<HistoryResponse>;
      })
      .then((data) => { if (!cancelled) setHistory(data); })
      .catch((error: unknown) => { if (!cancelled) setHistoryError(error instanceof Error ? error.message : "Falha ao carregar histórico."); });
    return () => { cancelled = true; };
  }, [historyOpen, productId, starred, years]);

  async function toggleStar() {
    setBusy(true);
    try { await onToggleStar(productId, !starred); }
    finally { setBusy(false); }
  }

  const points = history?.points ?? [];
  const stores = Array.from(new Set(points.map((point) => point.store)));
  const prices = points.map((point) => point.closing_price);
  const low = Math.min(...prices);
  const high = Math.max(...prices);
  const chartLines = stores.map((store, storeIndex) => {
    const series = points.filter((point) => point.store === store);
    const days = Array.from(new Set(points.map((point) => point.day))).sort();
    const dayIndexes = new Map(days.map((day, index) => [day, days.length < 2 ? 0 : index / (days.length - 1) * 580 + 10] as [string, number]));
    const polyline = series.map((point) => {
      const x = dayIndexes.get(point.day) ?? 10;
      const y = high === low ? 80 : 145 - (point.closing_price - low) / (high - low) * 130;
      return `${x},${y}`;
    }).join(" ");
    return { store, color: chartColors[storeIndex % chartColors.length], polyline };
  });

  return <article className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
    <div className="flex flex-wrap items-start justify-between gap-3 border-b border-slate-100 p-4 sm:p-5">
      <div className="min-w-0"><p className="text-xs font-semibold uppercase tracking-wide text-slate-500">{latestOffers.length} {latestOffers.length === 1 ? "oferta monitorada" : "ofertas monitoradas"}</p><h2 className="mt-1 text-lg font-semibold leading-snug">{title}</h2></div>
      <button type="button" disabled={busy} onClick={() => void toggleStar()} aria-pressed={starred} aria-label={starred ? "Remover dos acompanhados" : "Acompanhar produto"} className={`flex min-h-11 items-center gap-2 rounded-xl border px-3 font-semibold transition disabled:opacity-50 ${starred ? "border-amber-300 bg-amber-50 text-amber-800" : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50"}`}>
        <span aria-hidden="true" className="text-xl">{starred ? "★" : "☆"}</span>{starred ? "Acompanhando" : "Acompanhar"}
      </button>
    </div>
    <div className="divide-y divide-slate-100">
      {latestOffers.map((offer) => {
        const href = safeUrl(offer.url);
        return <div key={offer.url} className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-5">
          <div><p className="font-medium text-slate-700">{offer.store}{offer.anomaly ? <span className="ml-2 rounded-full bg-rose-100 px-2 py-1 text-xs font-bold text-rose-700">Preço fora do padrão</span> : null}</p><p className="text-sm text-slate-500">{offer.observed_at ? `Coletado ${new Date(offer.observed_at).toLocaleString("pt-BR")}` : "Coleta recente"}</p></div>
          <div className="text-right"><p className="text-lg font-bold tabular-nums">{money(offer.price)}</p>{href ? <a href={href} target="_blank" rel="noopener noreferrer" className="text-sm font-medium text-emerald-700 underline">Abrir loja</a> : <span className="text-xs text-rose-700">Link não verificado</span>}</div>
        </div>;
      })}
    </div>
    <div className="border-t border-slate-100 px-4 py-3 sm:px-5">
      <button type="button" onClick={() => { setHistoryOpen((open) => !open); setHistory(null); }} className="min-h-10 font-semibold text-emerald-800 hover:text-emerald-950">{historyOpen ? "Fechar histórico" : "Ver histórico de preços"}</button>
      {historyOpen ? <div className="mt-3 rounded-xl bg-slate-50 p-3 sm:p-4">
        <div className="flex flex-wrap items-center justify-between gap-3"><p className="font-semibold">Histórico por loja</p><label className="flex items-center gap-2 text-sm">Período<select value={years} onChange={(event) => { setYears(Number(event.target.value)); setHistory(null); }} className="min-h-10 rounded-lg border border-slate-300 bg-white px-2"><option value={1}>1 ano</option><option value={2}>2 anos</option><option value={3}>3 anos</option><option value={5}>5 anos</option></select></label></div>
        {!starred ? <p className="mt-3 text-sm text-slate-600">Marque a estrela para salvar este produto na sua lista de acompanhamento e abrir o histórico.</p> : historyError ? <p role="alert" className="mt-3 text-sm text-rose-700">{historyError}</p> : !history ? <p className="mt-3 text-sm text-slate-500">Carregando histórico...</p> : points.length === 0 ? <p className="mt-3 text-sm text-slate-600">Ainda não há coletas neste período. O histórico começa na primeira coleta registrada.</p> : <>
          <div className="mt-4 overflow-hidden rounded-lg bg-white p-2"><svg viewBox="0 0 600 160" role="img" aria-label={`Histórico de preços por loja nos últimos ${years} anos`} className="h-48 w-full"><line x1="10" y1="150" x2="590" y2="150" stroke="#cbd5e1" />{chartLines.map((line) => <polyline key={line.store} points={line.polyline} fill="none" stroke={line.color} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" />)}</svg></div>
          <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs">{chartLines.map((line) => <span key={line.store} className="flex items-center gap-1.5"><i className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: line.color }} />{line.store}</span>)}</div>
          <p className="mt-2 text-xs text-slate-500">{points.length} pontos diários · Menor {money(low)} · Maior {money(high)}{history.available_from ? ` · dados desde ${new Date(history.available_from).toLocaleDateString("pt-BR")}` : ""}</p>
        </>}
      </div> : null}
    </div>
  </article>;
}
