type OfferCardProps = { title: string; store: string; price: number; previousPrice?: number; anomaly?: boolean; href: string };

const approvedHosts = new Set(["amazon.com.br", "www.amazon.com.br", "kabum.com.br", "www.kabum.com.br"]);
function approvedUrl(value: string): string | null {
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" || !approvedHosts.has(url.hostname.toLowerCase()) || url.username || url.password) return null;
    return url.toString();
  } catch { return null; }
}

export default function OfferCard({ title, store, price, previousPrice, anomaly, href }: OfferCardProps) {
  const safeHref = approvedUrl(href);
  const discount = previousPrice && previousPrice > price ? Math.round((1 - price / previousPrice) * 100) : null;
  return <article className="flex flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm transition hover:shadow-md md:flex-row">
    <div aria-hidden="true" className="flex min-h-40 items-center justify-center bg-slate-100 text-5xl md:w-40 md:shrink-0">🎮</div>
    <div className="flex min-w-0 flex-1 flex-col p-5">
      <div className="mb-3 flex items-center justify-between gap-3"><span className="truncate text-sm font-medium text-slate-500">{store}</span>{anomaly ? <span className="shrink-0 rounded-full bg-rose-100 px-2.5 py-1 text-xs font-bold text-rose-700">Preço fora do padrão</span> : discount !== null ? <span className="shrink-0 rounded-full bg-emerald-100 px-2.5 py-1 text-xs font-bold text-emerald-800">-{discount}%</span> : null}</div>
      <h2 className="line-clamp-2 text-lg font-semibold leading-snug">{title}</h2>
      <div className="mt-4">{previousPrice ? <p className="text-sm text-slate-500">Mediana anterior: R$ {previousPrice.toFixed(2).replace(".", ",")}</p> : <p className="text-sm text-slate-500">Primeira coleta</p>}<p className="text-2xl font-bold tracking-tight">R$ {price.toFixed(2).replace(".", ",")}</p></div>
      {safeHref ? <a href={safeHref} target="_blank" rel="noopener noreferrer" className="mt-5 flex min-h-12 items-center justify-center rounded-xl bg-slate-900 px-5 text-center font-semibold text-white hover:bg-slate-700 focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:ring-offset-2 md:mt-auto md:pt-4">Ver oferta</a> : <p className="mt-4 text-sm font-medium text-rose-700">Link indisponível: domínio não verificado.</p>}
    </div>
  </article>;
}
