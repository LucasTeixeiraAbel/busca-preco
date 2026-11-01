/* Minimal push service worker. Serve only over HTTPS (localhost is allowed). */
self.addEventListener("push", (event) => {
  let payload = { title: "Nova oferta", body: "Uma oferta que você segue teve alteração.", url: "/" };
  try { if (event.data) payload = { ...payload, ...event.data.json() }; } catch { /* retain safe defaults */ }
  const target = typeof payload.url === "string" && payload.url.startsWith("/") ? payload.url : "/";
  event.waitUntil(self.registration.showNotification(payload.title, {
    body: payload.body, icon: "/icon.svg", badge: "/icon.svg", data: { url: target },
  }));
});
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const path = event.notification.data?.url;
  const target = typeof path === "string" && path.startsWith("/") ? path : "/";
  event.waitUntil(self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
    const existing = clients.find((client) => "focus" in client);
    return existing ? existing.navigate(target).then(() => existing.focus()) : self.clients.openWindow(target);
  }));
});
