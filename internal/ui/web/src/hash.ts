// hash.ts: shared URL-hash helpers for the Dashboards page.
// The active dashboard id is kept in the hash as #/dashboards/<id> so a
// dashboard is shareable/reloadable and survives browser back/forward.

export function parseDashId(): string | null {
  const h = window.location.hash.replace(/^#\/?/, '');
  const m = h.match(/^dashboards\/([^/]+)$/);
  return m ? decodeURIComponent(m[1]) : null;
}

export function setDashHash(id: string | null): void {
  window.location.hash = id ? `/dashboards/${encodeURIComponent(id)}` : '/dashboards';
}
