/**
 * 26AS Recon Portal — Web Adapters
 * Replaces Electron APIs with web equivalents:
 *   - electron-store  → localStorage + Firebase server sync
 *   - Odoo XML-RPC    → server proxy (CORS bypass)
 *   - Gmail           → server-side OAuth + API proxy
 *   - File dialogs    → browser <input type="file">
 */

// ── SERVER BASE ──────────────────────────────────────────────
function getServerBase() {
  if (typeof window === 'undefined') return '';
  if (window.location.protocol === 'file:') return 'http://localhost:3003';
  return window.location.origin;
}

// ══════════════════════════════════════════════════════════════
//  STORAGE (replaces electron-store)
// ══════════════════════════════════════════════════════════════
export async function saveToStore(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
    // Also push to server
    const base = getServerBase();
    if (base) {
      fetch(`${base}/api/state/${encodeURIComponent(key)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value })
      }).catch(() => {});
    }
  } catch (e) {
    console.warn('Store save failed:', e);
  }
}

export async function loadFromStore(key) {
  try {
    const raw = localStorage.getItem(key);
    if (raw) return JSON.parse(raw);
  } catch (e) {}
  return null;
}

export async function clearStore() {
  try {
    const keys = [
      'tds_cfg', 'tds_companies', 'tds_26as', 'tds_ais',
      'tds_books', 'tds_recon', 'tds_files', 'tds_tanmaster',
      'tds_gmail', 'tds_invoices'
    ];
    keys.forEach(k => localStorage.removeItem(k));
    const base = getServerBase();
    if (base) {
      fetch(`${base}/api/state`, { method: 'DELETE' }).catch(() => {});
    }
  } catch (e) {
    console.warn('Store clear failed:', e);
  }
}

// Push all data to Firebase server
export async function pushToServer() {
  const base = getServerBase();
  if (!base) return;
  const keys = [
    'tds_cfg', 'tds_companies', 'tds_26as', 'tds_ais',
    'tds_books', 'tds_recon', 'tds_files', 'tds_tanmaster',
    'tds_gmail', 'tds_invoices'
  ];
  for (const k of keys) {
    const v = localStorage.getItem(k);
    if (!v) continue;
    try {
      await fetch(`${base}/api/state/${encodeURIComponent(k)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: JSON.parse(v) })
      });
    } catch (e) {}
  }
}

// Pull all data from Firebase server
export async function pullFromServer() {
  const base = getServerBase();
  if (!base) return 0;
  try {
    const res = await fetch(`${base}/api/state`);
    const json = await res.json();
    if (!json.ok || !json.state) return 0;
    let count = 0;
    Object.keys(json.state).forEach(k => {
      if (json.state[k] !== undefined) {
        localStorage.setItem(k, JSON.stringify(json.state[k]));
        count++;
      }
    });
    return count;
  } catch (e) {
    console.warn('Pull from server failed:', e);
    return 0;
  }
}

// ══════════════════════════════════════════════════════════════
//  ODOO PROXY (replaces direct XML-RPC from Electron)
// ══════════════════════════════════════════════════════════════
export async function testOdooConnection(url, db, username, apiKey) {
  const base = getServerBase();
  const res = await fetch(`${base}/api/odoo/test`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url, db, username, apiKey })
  });
  return res.json();
}

export async function syncTDSFromOdoo({ url, db, username, apiKey, fyStart, fyEnd, tdsAccountCode, debtorAccountCode, prefixes }) {
  const base = getServerBase();
  const res = await fetch(`${base}/api/odoo/sync-tds`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url, db, username, apiKey, fyStart, fyEnd, tdsAccountCode, debtorAccountCode, prefixes })
  });
  return res.json();
}

// ══════════════════════════════════════════════════════════════
//  GMAIL (OAuth + API via server proxy)
// ══════════════════════════════════════════════════════════════
export async function getGmailAuthUrl(clientId) {
  const base = getServerBase();
  const redirectUri = `${base}/api/gmail/callback`;
  const res = await fetch(`${base}/api/gmail/auth-url`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ clientId, redirectUri })
  });
  return res.json();
}

export async function exchangeGmailCode(code, clientId, clientSecret) {
  const base = getServerBase();
  const redirectUri = `${base}/api/gmail/callback`;
  const res = await fetch(`${base}/api/gmail/exchange`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code, clientId, clientSecret, redirectUri })
  });
  return res.json();
}

export async function refreshGmailToken(clientId, clientSecret, refreshToken) {
  const base = getServerBase();
  const res = await fetch(`${base}/api/gmail/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ clientId, clientSecret, refreshToken })
  });
  return res.json();
}

export async function callGmailAPI(accessToken, endpoint, method = 'GET', body = null) {
  const base = getServerBase();
  const res = await fetch(`${base}/api/gmail/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ accessToken, endpoint, method, body })
  });
  return res.json();
}

// Open Gmail OAuth in popup
export function openGmailOAuthPopup(authUrl) {
  return new Promise((resolve) => {
    const popup = window.open(authUrl, 'Gmail Sign In', 'width=500,height=650,left=200,top=100');
    const handler = (event) => {
      if (event.data?.type === 'gmail-oauth') {
        window.removeEventListener('message', handler);
        resolve(event.data);
      }
    };
    window.addEventListener('message', handler);
    // Timeout after 3 minutes
    setTimeout(() => {
      window.removeEventListener('message', handler);
      resolve({ error: 'timeout' });
    }, 180000);
  });
}

// ══════════════════════════════════════════════════════════════
//  FILE HANDLING (replaces Electron file dialogs)
// ══════════════════════════════════════════════════════════════

// Read file as text
export function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = reject;
    reader.readAsText(file);
  });
}

// Read file as ArrayBuffer (for ZIP/Excel)
export function readFileAsArrayBuffer(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = reject;
    reader.readAsArrayBuffer(file);
  });
}

// Read file as base64 (for Excel)
export function readFileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result.split(',')[1]);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

// Download data as file
export function downloadFile(data, filename, mimeType = 'application/json') {
  const blob = new Blob([typeof data === 'string' ? data : JSON.stringify(data, null, 2)], { type: mimeType });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}
