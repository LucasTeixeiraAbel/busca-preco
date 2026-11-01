"use client";

import { useState } from "react";

export default function AuthCallback() {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("Confirme para concluir seu acesso ao Radar.");

  async function confirmAccess() {
    const token = new URLSearchParams(window.location.hash.slice(1)).get("token");
    if (!token) { setMessage("Link sem token. Solicite um novo link de acesso."); return; }
    setBusy(true);
    try {
      const response = await fetch("/api/auth/verify", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token }),
      });
      if (!response.ok) throw new Error("Este link expirou ou já foi usado. Solicite outro.");
      window.location.replace("/");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Não foi possível validar o link.");
      setBusy(false);
    }
  }

  return <section className="w-full rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
    <p className="text-sm font-semibold uppercase tracking-widest text-emerald-700">Radar</p>
    <h1 className="mt-2 text-2xl font-bold">Acesso por e-mail</h1>
    <p role="status" className="mt-3 text-slate-600">{message}</p>
    <button onClick={() => void confirmAccess()} disabled={busy} className="mt-6 min-h-12 w-full rounded-xl bg-slate-900 px-5 font-semibold text-white disabled:opacity-60">{busy ? "Validando…" : "Confirmar acesso"}</button>
  </section>;
}
