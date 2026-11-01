"use client";

import { FormEvent, useEffect, useState } from "react";

type Profile = { email: string; email_alerts: boolean; telegram_enabled: boolean };

function decodeVapidKey(value: string): ArrayBuffer {
  const padding = "=".repeat((4 - value.length % 4) % 4);
  const base64 = (value + padding).replace(/-/g, "+").replace(/_/g, "/");
  const raw = window.atob(base64);
  const key = new ArrayBuffer(raw.length);
  const view = new DataView(key);
  for (let index = 0; index < raw.length; index++) view.setUint8(index, raw.charCodeAt(index));
  return key;
}

export default function AuthAndAlerts() {
  const [email, setEmail] = useState("");
  const [profile, setProfile] = useState<Profile | null>(null);
  const [pushEnabled, setPushEnabled] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");

  async function refreshProfile() {
    const response = await fetch("/api/me", { cache: "no-store" });
    if (!response.ok) { setProfile(null); return; }
    const data = await response.json() as Profile;
    setProfile(data);
    setEmail(data.email);
    if ("serviceWorker" in navigator && "PushManager" in window) {
      const registration = await navigator.serviceWorker.ready;
      setPushEnabled(Boolean(await registration.pushManager.getSubscription()));
    }
  }

  useEffect(() => { void refreshProfile(); }, []);
  useEffect(() => {
    const onReturn = () => { if (document.visibilityState === "visible" && profile) void refreshProfile(); };
    document.addEventListener("visibilitychange", onReturn);
    return () => document.removeEventListener("visibilitychange", onReturn);
  }, [profile]);

  async function requestLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); setBusy(true); setMessage("");
    try {
      const response = await fetch("/api/auth/magic-link", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ email }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.detail ?? "Não foi possível enviar o link.");
      setMessage("Se o endereço estiver correto, enviaremos um link de acesso válido por 15 minutos.");
    } catch (error) { setMessage(error instanceof Error ? error.message : "Falha ao solicitar acesso."); }
    finally { setBusy(false); }
  }

  async function setEmailConsent(enabled: boolean) {
    setBusy(true); setMessage("");
    try {
      const response = await fetch("/api/alerts/email", { method: enabled ? "PUT" : "DELETE" });
      if (!response.ok) throw new Error("Não foi possível atualizar sua preferência.");
      setProfile((current) => current ? { ...current, email_alerts: enabled } : current);
    } catch (error) { setMessage(error instanceof Error ? error.message : "Falha ao salvar preferência."); }
    finally { setBusy(false); }
  }

  async function togglePush() {
    setBusy(true); setMessage("");
    try {
      if (!("serviceWorker" in navigator) || !("PushManager" in window)) throw new Error("Push não é compatível com este navegador.");
      const registration = await navigator.serviceWorker.ready;
      const existing = await registration.pushManager.getSubscription();
      if (existing) {
        const response = await fetch(`/api/push-subscriptions?endpoint=${encodeURIComponent(existing.endpoint)}`, { method: "DELETE" });
        if (!response.ok) throw new Error("Não foi possível desativar as notificações.");
        await existing.unsubscribe(); setPushEnabled(false); return;
      }
      const publicKey = process.env.NEXT_PUBLIC_VAPID_PUBLIC_KEY;
      if (!publicKey) throw new Error("Notificações push ainda não foram configuradas.");
      const permission = await Notification.requestPermission();
      if (permission !== "granted") throw new Error("Permissão de notificações não concedida.");
      const subscription = await registration.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: decodeVapidKey(publicKey) });
      const response = await fetch("/api/push-subscriptions", {
        method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(subscription.toJSON()),
      });
      if (!response.ok) { await subscription.unsubscribe(); throw new Error("Não foi possível registrar este dispositivo."); }
      setPushEnabled(true);
    } catch (error) { setMessage(error instanceof Error ? error.message : "Falha ao configurar notificações."); }
    finally { setBusy(false); }
  }

  async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    setProfile(null); setPushEnabled(false); setMessage("Sessão encerrada.");
  }

  async function toggleTelegram() {
    setBusy(true); setMessage("");
    try {
      if (profile?.telegram_enabled) {
        const response = await fetch("/api/telegram/link", { method: "DELETE" });
        if (!response.ok) throw new Error("Não foi possível desconectar o Telegram.");
        setProfile((current) => current ? { ...current, telegram_enabled: false } : current);
        return;
      }
      const response = await fetch("/api/telegram/link", { method: "POST" });
      const result = await response.json();
      if (!response.ok) throw new Error(result.detail ?? "Não foi possível gerar o link do bot.");
      window.location.href = result.url;
    } catch (error) { setMessage(error instanceof Error ? error.message : "Falha ao conectar Telegram."); }
    finally { setBusy(false); }
  }

  return <section aria-label="Alertas pessoais" className="mb-8 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
    {!profile ? <form onSubmit={requestLogin} className="flex flex-col gap-3 sm:flex-row sm:items-end">
      <label className="flex-1 text-sm font-medium text-slate-700">Entre para configurar alertas
        <input type="email" required autoComplete="email" value={email} onChange={(event) => setEmail(event.target.value)} placeholder="voce@exemplo.com" className="mt-1 min-h-12 w-full rounded-xl border border-slate-300 px-4 text-base" />
      </label>
      <button disabled={busy} className="min-h-12 rounded-xl bg-slate-900 px-5 font-semibold text-white disabled:opacity-60">Enviar link de acesso</button>
    </form> : <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
      <div><p className="font-semibold">Alertas de {profile.email}</p><label className="mt-2 flex min-h-11 items-center gap-3 text-sm text-slate-700"><input type="checkbox" className="size-5 accent-emerald-600" checked={profile.email_alerts} disabled={busy} onChange={(event) => void setEmailConsent(event.target.checked)} />Quero receber alertas de ofertas por e-mail</label></div>
      <div className="flex flex-wrap gap-2"><button onClick={() => void togglePush()} disabled={busy} className="min-h-11 rounded-xl border border-slate-300 px-4 font-medium hover:bg-slate-50">{pushEnabled ? "Desativar push neste dispositivo" : "Ativar push neste dispositivo"}</button><button onClick={() => void toggleTelegram()} disabled={busy} className="min-h-11 rounded-xl border border-slate-300 px-4 font-medium hover:bg-slate-50">{profile.telegram_enabled ? "Desconectar Telegram" : "Conectar Telegram"}</button><button onClick={() => void logout()} className="min-h-11 rounded-xl px-4 font-medium text-slate-600 hover:bg-slate-100">Sair</button></div>
    </div>}
    {message && <p role="status" className="mt-3 text-sm text-slate-600">{message}</p>}
  </section>;
}
