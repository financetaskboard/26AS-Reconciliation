/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║   26AS RECONCILIATION PORTAL — Web Server  v2.0              ║
 * ║   Runs on http://localhost:3003  (or Render.com online)      ║
 * ║   Data stored in Firebase Firestore                          ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 *  v2.0 Changes:
 *    - Odoo: /jsonrpc endpoint (works with API keys AND passwords)
 *    - Firebase: key sanitization everywhere (colons → underscores)
 *    - Gmail: force https redirect_uri on Render
 */

const express  = require('express');
const cors     = require('cors');
const fetch    = require('node-fetch');
const fs       = require('fs');
const path     = require('path');
const admin    = require('firebase-admin');

const app  = express();
const PORT = process.env.PORT || 3003;

app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '50mb' }));

// ── Serve built React app (Vite output in /dist) ─────────────
const distPath = path.join(__dirname, 'dist');
if (fs.existsSync(distPath)) {
  app.use(express.static(distPath));
}
app.use(express.static(__dirname));

// ── Helper: get base URL (force https on Render) ─────────────
function getBaseUrl(req) {
  const proto = req.headers['x-forwarded-proto'] || req.protocol || 'http';
  return `${proto}://${req.get('host')}`;
}

// ══════════════════════════════════════════════════════════════
//  FIREBASE INITIALISATION
// ══════════════════════════════════════════════════════════════
let db = null;
try {
  let serviceAccount;
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    console.log('🔥 Firebase: credentials from env var');
  } else {
    const keyPath = path.join(__dirname, 'serviceAccountKey.json');
    if (fs.existsSync(keyPath)) {
      serviceAccount = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
      console.log('🔥 Firebase: credentials from serviceAccountKey.json');
    }
  }
  if (serviceAccount) {
    if (!admin.apps.length) {
      admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    }
    db = admin.firestore();
    console.log('✅ Firebase Firestore connected');
  } else {
    console.warn('⚠️  No Firebase credentials — data will NOT persist.');
  }
} catch (e) {
  console.error('❌ Firebase init error:', e.message);
}

// ══════════════════════════════════════════════════════════════
//  CHUNKED FIRESTORE HELPERS
// ══════════════════════════════════════════════════════════════
const CHUNK_SIZE  = 400;
const CHUNK_LIMIT = 900000;

// Sanitize keys for Firestore doc IDs (no slashes, colons)
function sanitizeKey(key) {
  return String(key).replace(/[\/\\:]/g, '_');
}

async function fbSave(key, value) {
  if (!db) return;
  const safeKey = sanitizeKey(key);
  const col = db.collection('tds_state');
  const jsonStr  = JSON.stringify(value);
  const byteSize = Buffer.byteLength(jsonStr, 'utf8');
  const sizeKB   = Math.round(byteSize / 1024);
  const needChunk = Array.isArray(value) && byteSize > CHUNK_LIMIT;

  if (!needChunk) {
    await col.doc(safeKey).set({ value, updatedAt: new Date().toISOString() });
    console.log(`  💾 [${safeKey}] ${sizeKB} KB`);
    return;
  }

  const chunks = [];
  for (let i = 0; i < value.length; i += CHUNK_SIZE) {
    chunks.push(value.slice(i, i + CHUNK_SIZE));
  }
  const batch = db.batch();
  batch.set(col.doc(safeKey), {
    chunked: true, chunkCount: chunks.length,
    totalCount: value.length, updatedAt: new Date().toISOString()
  });
  chunks.forEach((chunk, i) => {
    batch.set(col.doc(`${safeKey}_chunk_${i}`), { items: chunk });
  });
  await batch.commit();
  console.log(`  💾 [${safeKey}] ${sizeKB} KB → ${chunks.length} chunks`);
}

async function fbLoad(key) {
  if (!db) return undefined;
  const safeKey = sanitizeKey(key);
  const col  = db.collection('tds_state');
  const meta = await col.doc(safeKey).get();
  if (!meta.exists) return undefined;
  const data = meta.data();
  if (!data.chunked) return data.value;

  const chunkDocs = await Promise.all(
    Array.from({ length: data.chunkCount }, (_, i) => col.doc(`${safeKey}_chunk_${i}`).get())
  );
  const full = [];
  chunkDocs.forEach(d => { if (d.exists) full.push(...(d.data().items || [])); });
  return full;
}

async function fbDelete(key) {
  if (!db) return;
  const safeKey = sanitizeKey(key);
  const col  = db.collection('tds_state');
  const meta = await col.doc(safeKey).get();
  if (!meta.exists) return;
  const data = meta.data();
  const batch = db.batch();
  batch.delete(col.doc(safeKey));
  if (data.chunked) {
    for (let i = 0; i < data.chunkCount; i++) {
      batch.delete(col.doc(`${safeKey}_chunk_${i}`));
    }
  }
  await batch.commit();
}

// ══════════════════════════════════════════════════════════════
//  STATE PERSISTENCE API  (/api/state)
// ══════════════════════════════════════════════════════════════
const STATE_KEYS = [
  'tds_cfg', 'tds_companies', 'tds_26as', 'tds_ais', 'tds_books',
  'tds_recon', 'tds_files', 'tds_tanmaster', 'tds_gmail', 'tds_invoices',
  'companies', 'selCompanyId', 'selYear', 'tanEmails', 'emailLog',
  'datasets', 'files', 'reconResults', 'reconDone', 'activeCompanyIndex',
  'tracesCredsMap', 'localBackupFolder', 'driveBackupIndex', 'driveFolderId',
  'gmail_client_id', 'gmail_client_secret', 'gmail_access_token',
  'gmail_token_expiry', 'gmail_refresh_token', 'gmail_user_email'
];

app.get('/api/state', async (req, res) => {
  try {
    if (!db) return res.json({ ok: true, state: {} });
    const state = {};
    await Promise.all(STATE_KEYS.map(async (key) => {
      try {
        const val = await fbLoad(key);
        if (val !== undefined) state[key] = val;
      } catch (e) { /* skip */ }
    }));
    res.json({ ok: true, state });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/api/state/:key', async (req, res) => {
  try {
    const val = await fbLoad(req.params.key);
    res.json({ ok: true, value: val });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/state/:key', async (req, res) => {
  try {
    const key   = req.params.key;
    const value = req.body?.value;
    if (value === undefined) return res.status(400).json({ ok: false, error: 'Missing value' });
    await fbSave(key, value);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.delete('/api/state', async (req, res) => {
  try {
    await Promise.all(STATE_KEYS.map(key => fbDelete(key).catch(() => {})));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
//  ODOO JSON-RPC PROXY  (uses /jsonrpc endpoint)
//
//  /jsonrpc accepts BOTH passwords AND API keys (unlike
//  /web/session/authenticate which only accepts passwords).
//  Stateless: credentials passed with every call, no cookies.
// ══════════════════════════════════════════════════════════════

async function odooAuth(baseUrl, database, username, password) {
  const url = baseUrl.replace(/\/$/, '');
  const resp = await fetch(`${url}/jsonrpc`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0', method: 'call', id: 1,
      params: {
        service: 'common',
        method: 'authenticate',
        args: [database, username, password, {}]
      }
    })
  });
  const data = await resp.json();
  if (data.error) {
    throw new Error(data.error.data?.message || data.error.message || 'Odoo auth error');
  }
  const uid = data.result;
  if (!uid || uid === false) {
    throw new Error('Authentication failed — check URL, database, login & password/API key');
  }
  console.log(`   ✅ Odoo auth OK — UID ${uid}`);
  return { uid, baseUrl: url, database, password };
}

async function odooExecute(session, model, method, args = [], kwargs = {}) {
  const resp = await fetch(`${session.baseUrl}/jsonrpc`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0', method: 'call', id: Math.floor(Math.random() * 99999),
      params: {
        service: 'object',
        method: 'execute_kw',
        args: [session.database, session.uid, session.password, model, method, args, kwargs]
      }
    })
  });
  const data = await resp.json();
  if (data.error) {
    throw new Error(data.error.data?.message || data.error.message || 'Odoo call failed');
  }
  return data.result;
}

app.post('/api/odoo/test', async (req, res) => {
  try {
    const { url, db: database, username, apiKey } = req.body;
    console.log(`\n🔌 Odoo test: ${url} | db=${database} | user=${username}`);
    const session = await odooAuth(url, database, username, apiKey);
    res.json({ ok: true, uid: session.uid, message: `Connected as UID ${session.uid}` });
  } catch (e) {
    console.error('   ❌ Odoo test:', e.message);
    res.json({ ok: false, error: e.message });
  }
});

app.post('/api/odoo/sync-tds', async (req, res) => {
  try {
    const { url, db: database, username, apiKey, fyStart, fyEnd, tdsAccountCode, debtorAccountCode, prefixes } = req.body;
    console.log(`\n🔄 TDS Sync: ${url} | db=${database} | ${fyStart} → ${fyEnd}`);

    const session = await odooAuth(url, database, username, apiKey);

    // Lookup account IDs
    const tdsAccIds = await odooExecute(session, 'account.account', 'search', [
      [['code', '=', tdsAccountCode || '231110']]
    ]);
    const debtorAccIds = await odooExecute(session, 'account.account', 'search', [
      [['code', '=', debtorAccountCode || '251000']]
    ]);
    if (!tdsAccIds?.length) return res.json({ ok: false, error: `TDS account (${tdsAccountCode || '231110'}) not found` });
    if (!debtorAccIds?.length) return res.json({ ok: false, error: `Debtor account (${debtorAccountCode || '251000'}) not found` });
    const tdsAccId = tdsAccIds[0], debtorAccId = debtorAccIds[0];
    console.log(`   TDS acc=${tdsAccId}, Debtor acc=${debtorAccId}`);

    // Fetch TDS lines
    const BATCH = 200;
    const domain = [
      ['account_id', '=', tdsAccId], ['date', '>=', fyStart], ['date', '<=', fyEnd],
      ['debit', '>', 0], ['parent_state', '=', 'posted']
    ];
    const allLines = [];
    let offset = 0;
    while (true) {
      const batch = await odooExecute(session, 'account.move.line', 'search_read', [domain], {
        fields: ['date', 'move_id', 'partner_id', 'company_id', 'name', 'debit', 'credit', 'balance'],
        limit: BATCH, offset, order: 'date asc'
      });
      allLines.push(...batch);
      console.log(`   TDS lines offset=${offset} → ${batch.length} (total: ${allLines.length})`);
      if (batch.length < BATCH) break;
      offset += BATCH;
    }

    // Filter by prefixes
    const prefixList = (prefixes || '').split(',').map(p => p.trim().toUpperCase()).filter(Boolean);
    const filtered = prefixList.length > 0
      ? allLines.filter(l => { const p = (l.name || '').split('/')[0].toUpperCase(); return prefixList.includes(p); })
      : allLines;
    console.log(`   Filtered: ${filtered.length} of ${allLines.length}`);

    // Enrich with debtor amounts
    const moveIds = [...new Set(filtered.map(l => l.move_id?.[0]).filter(Boolean))];
    const invoiceAmounts = {};
    for (let i = 0; i < moveIds.length; i += BATCH) {
      const batchIds = moveIds.slice(i, i + BATCH);
      const dl = await odooExecute(session, 'account.move.line', 'search_read',
        [[ ['move_id', 'in', batchIds], ['account_id', '=', debtorAccId] ]],
        { fields: ['move_id', 'credit'] }
      );
      dl.forEach(d => {
        const mid = d.move_id?.[0];
        if (mid) invoiceAmounts[mid] = (invoiceAmounts[mid] || 0) + (d.credit || 0);
      });
    }

    // Transform
    const getQ = (d) => {
      if (!d) return 'Q1';
      const m = new Date(d).getMonth() + 1;
      if (m >= 4 && m <= 6) return 'Q1'; if (m >= 7 && m <= 9) return 'Q2';
      if (m >= 10 && m <= 12) return 'Q3'; return 'Q4';
    };
    const data = filtered.map(l => ({
      deductorName: l.partner_id?.[1] || '', tan: '',
      amount: invoiceAmounts[l.move_id?.[0]] || 0, tdsDeducted: l.debit || 0,
      section: '', date: l.date || '', invoiceNo: l.name || '',
      quarter: getQ(l.date), source: 'Odoo ERP',
      journalEntry: l.move_id?.[1] || '', odooCompany: l.company_id?.[1] || ''
    }));

    console.log(`✅ TDS Sync: ${data.length} records`);
    res.json({ ok: true, count: data.length, total: allLines.length, data });
  } catch (e) {
    console.error('❌ TDS sync error:', e.message);
    res.status(400).json({ ok: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
//  GMAIL OAUTH2 + API PROXY
// ══════════════════════════════════════════════════════════════
const GMAIL_SCOPES = 'https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send';

app.post('/api/gmail/auth-url', (req, res) => {
  const { clientId, redirectUri } = req.body;
  const callbackUrl = redirectUri || `${getBaseUrl(req)}/api/gmail/callback`;
  console.log(`📧 Gmail auth-url → redirect: ${callbackUrl}`);
  const params = new URLSearchParams({
    client_id: clientId,
    redirect_uri: callbackUrl,
    response_type: 'code',
    scope: GMAIL_SCOPES,
    access_type: 'offline',
    prompt: 'consent'
  });
  res.json({ ok: true, url: `https://accounts.google.com/o/oauth2/v2/auth?${params}` });
});

app.post('/api/gmail/exchange', async (req, res) => {
  try {
    const { code, clientId, clientSecret, redirectUri } = req.body;
    const callbackUrl = redirectUri || `${getBaseUrl(req)}/api/gmail/callback`;
    console.log(`📧 Gmail exchange → redirect: ${callbackUrl}`);
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code, client_id: clientId, client_secret: clientSecret,
        redirect_uri: callbackUrl, grant_type: 'authorization_code'
      })
    });
    const tokens = await tokenRes.json();
    console.log(`📧 Gmail token response:`, tokens.error || 'OK');
    if (tokens.error) return res.json({ ok: false, error: tokens.error_description || tokens.error });

    if (db && tokens.refresh_token) {
      await db.collection('tds_config').doc('gmail_tokens').set({
        refreshToken: tokens.refresh_token, updatedAt: new Date().toISOString()
      });
    }
    res.json({ ok: true, accessToken: tokens.access_token, expiresIn: tokens.expires_in, refreshToken: tokens.refresh_token });
  } catch (e) {
    console.error('📧 Gmail exchange error:', e.message);
    res.json({ ok: false, error: e.message });
  }
});

app.post('/api/gmail/refresh', async (req, res) => {
  try {
    const { clientId, clientSecret, refreshToken } = req.body;
    let token = refreshToken;
    if (!token && db) {
      const doc = await db.collection('tds_config').doc('gmail_tokens').get();
      if (doc.exists) token = doc.data().refreshToken;
    }
    if (!token) return res.json({ ok: false, error: 'No refresh token' });

    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: clientId, client_secret: clientSecret,
        refresh_token: token, grant_type: 'refresh_token'
      })
    });
    const data = await tokenRes.json();
    if (data.error) return res.json({ ok: false, error: data.error });
    res.json({ ok: true, accessToken: data.access_token, expiresIn: data.expires_in });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

app.post('/api/gmail/api', async (req, res) => {
  try {
    const { accessToken, endpoint, method, body } = req.body;
    const url = `https://gmail.googleapis.com/gmail/v1/users/me/${endpoint}`;
    const opts = {
      method: method || 'GET',
      headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' }
    };
    if (body) opts.body = JSON.stringify(body);
    const response = await fetch(url, opts);
    const data = await response.json();
    res.json({ ok: true, data });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

app.get('/api/gmail/callback', (req, res) => {
  const code = req.query.code;
  const error = req.query.error;
  res.send(`<!DOCTYPE html><html><body><script>
    window.opener && window.opener.postMessage(${JSON.stringify({ type: 'gmail-oauth', code, error })}, '*');
    setTimeout(() => window.close(), 1500);
  </script><p>${code ? '✅ Connected! This window will close.' : '❌ Error: ' + (error || 'unknown')}</p></body></html>`);
});

// ══════════════════════════════════════════════════════════════
//  HEALTH CHECK + SPA FALLBACK
// ══════════════════════════════════════════════════════════════
app.get('/health', (req, res) => {
  res.json({ ok: true, firebase: !!db, version: '2.0.0' });
});

app.get('*', (req, res) => {
  const indexPath = fs.existsSync(path.join(distPath, 'index.html'))
    ? path.join(distPath, 'index.html')
    : path.join(__dirname, 'index.html');
  if (fs.existsSync(indexPath)) res.sendFile(indexPath);
  else res.send('<h2 style="font-family:Segoe UI;padding:40px">26AS Reconciliation Portal — run: npm run build</h2>');
});

app.listen(PORT, () => {
  console.log(`╔══════════════════════════════════════════════════╗`);
  console.log(`║  26AS RECON PORTAL  v2.0  →  port ${PORT}            ║`);
  console.log(`╠══════════════════════════════════════════════════╣`);
  console.log(`║  Odoo   : /jsonrpc (API key + password)         ║`);
  console.log(`║  Storage: Firebase Firestore (chunked)          ║`);
  console.log(`║  Gmail  : OAuth2 + API proxy (https forced)     ║`);
  console.log(`╚══════════════════════════════════════════════════╝`);
  console.log(`  ➡  http://localhost:${PORT}\n`);
});
