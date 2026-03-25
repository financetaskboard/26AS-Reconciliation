/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║   26AS RECONCILIATION PORTAL — Web Server  v1.0              ║
 * ║   Runs on http://localhost:3003  (or Render.com online)      ║
 * ║   Data stored in Firebase Firestore                          ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 *  Features:
 *    - Odoo XML-RPC proxy (browser can't do CORS XML-RPC directly)
 *    - Gmail OAuth2 flow + API proxy
 *    - Firebase Firestore state persistence
 *    - Team sync (Push/Pull)
 *    - Serves the React app (built via Vite)
 *
 *  LOCAL:   node tds-server.js
 *  RENDER:  Set FIREBASE_SERVICE_ACCOUNT env variable
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
// Also serve from root for dev (index.html etc.)
app.use(express.static(__dirname));

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
      console.log(`🔥 Firebase: credentials from serviceAccountKey.json`);
    }
  }
  if (serviceAccount) {
    if (!admin.apps.length) {
      admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    }
    db = admin.firestore();
    console.log('✅ Firebase Firestore connected');
  } else {
    console.warn('⚠️  No Firebase credentials — data will NOT persist to Firestore.');
  }
} catch (e) {
  console.error('❌ Firebase init error:', e.message);
}

// ══════════════════════════════════════════════════════════════
//  CHUNKED FIRESTORE HELPERS (same pattern as GST portal)
// ══════════════════════════════════════════════════════════════
const CHUNK_SIZE  = 400;
const CHUNK_LIMIT = 900000;

async function fbSave(key, value) {
  if (!db) return;
  const col = db.collection('tds_state');
  const jsonStr  = JSON.stringify(value);
  const byteSize = Buffer.byteLength(jsonStr, 'utf8');
  const sizeKB   = Math.round(byteSize / 1024);
  const needChunk = Array.isArray(value) && byteSize > CHUNK_LIMIT;

  if (!needChunk) {
    await col.doc(key).set({ value, updatedAt: new Date().toISOString() });
    console.log(`  💾 [${key}] ${sizeKB} KB (single doc)`);
    return;
  }

  const chunks = [];
  for (let i = 0; i < value.length; i += CHUNK_SIZE) {
    chunks.push(value.slice(i, i + CHUNK_SIZE));
  }
  const batch = db.batch();
  batch.set(col.doc(key), {
    chunked: true, chunkCount: chunks.length,
    totalCount: value.length, updatedAt: new Date().toISOString()
  });
  chunks.forEach((chunk, i) => {
    batch.set(col.doc(`${key}_chunk_${i}`), { items: chunk });
  });
  await batch.commit();
  console.log(`  💾 [${key}] ${sizeKB} KB → ${chunks.length} chunks`);
}

async function fbLoad(key) {
  if (!db) return undefined;
  const col  = db.collection('tds_state');
  const meta = await col.doc(key).get();
  if (!meta.exists) return undefined;
  const data = meta.data();
  if (!data.chunked) return data.value;

  const chunkDocs = await Promise.all(
    Array.from({ length: data.chunkCount }, (_, i) => col.doc(`${key}_chunk_${i}`).get())
  );
  const full = [];
  chunkDocs.forEach(d => { if (d.exists) full.push(...(d.data().items || [])); });
  return full;
}

async function fbDelete(key) {
  if (!db) return;
  const col  = db.collection('tds_state');
  const meta = await col.doc(key).get();
  if (!meta.exists) return;
  const data = meta.data();
  const batch = db.batch();
  batch.delete(col.doc(key));
  if (data.chunked) {
    for (let i = 0; i < data.chunkCount; i++) {
      batch.delete(col.doc(`${key}_chunk_${i}`));
    }
  }
  await batch.commit();
}

// ══════════════════════════════════════════════════════════════
//  STATE PERSISTENCE API  (/api/state)
// ══════════════════════════════════════════════════════════════
const STATE_KEYS = [
  'tds_cfg',           // Company config, Odoo settings, FY
  'tds_companies',     // Company master (multi-company)
  'tds_26as',          // 26AS data (per company)
  'tds_ais',           // AIS data
  'tds_books',         // Books/Odoo TDS data
  'tds_recon',         // Reconciliation results
  'tds_files',         // Imported file metadata
  'tds_tanmaster',     // TAN master
  'tds_gmail',         // Gmail config (tokens stored encrypted)
  'tds_invoices'       // Invoice-level data
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
//  ODOO XML-RPC PROXY
//  Browser can't call Odoo's XML-RPC endpoint due to CORS.
//  We proxy through our server.
// ══════════════════════════════════════════════════════════════

// Generic Odoo XML-RPC call
app.post('/api/odoo/xmlrpc', async (req, res) => {
  try {
    const { url, endpoint, body } = req.body;
    if (!url || !endpoint || !body) {
      return res.status(400).json({ ok: false, error: 'Missing url, endpoint, or body' });
    }
    const fullUrl = url.replace(/\/$/, '') + endpoint;
    const response = await fetch(fullUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'text/xml' },
      body: body
    });
    const text = await response.text();
    res.set('Content-Type', 'text/xml');
    res.send(text);
  } catch (e) {
    console.error('Odoo proxy error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Test Odoo connection
app.post('/api/odoo/test', async (req, res) => {
  try {
    const { url, db: database, username, apiKey } = req.body;
    const authBody = buildXMLRPC('authenticate', [database, username, apiKey, {}]);
    const response = await fetch(url.replace(/\/$/, '') + '/xmlrpc/2/common', {
      method: 'POST',
      headers: { 'Content-Type': 'text/xml' },
      body: authBody
    });
    const text = await response.text();
    // Check if we got a valid UID (not false/nil)
    const uidMatch = text.match(/<int>(\d+)<\/int>/);
    if (uidMatch) {
      res.json({ ok: true, uid: parseInt(uidMatch[1]), message: `Connected as UID ${uidMatch[1]}` });
    } else {
      res.json({ ok: false, error: 'Authentication failed — check credentials' });
    }
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// TDS Sync from Odoo (full flow — authenticate + search + read + transform)
app.post('/api/odoo/sync-tds', async (req, res) => {
  try {
    const { url, db: database, username, apiKey, fyStart, fyEnd, tdsAccountCode, debtorAccountCode, prefixes } = req.body;
    
    // Step 1: Authenticate
    const uid = await odooAuth(url, database, username, apiKey);
    if (!uid || typeof uid !== 'number') return res.json({ ok: false, error: 'Authentication failed — UID not a number' });

    // Step 2: Lookup account IDs
    const tdsAccId = await odooSearchOne(url, database, uid, apiKey, 'account.account', [['code', '=', tdsAccountCode || '231110']]);
    const debtorAccId = await odooSearchOne(url, database, uid, apiKey, 'account.account', [['code', '=', debtorAccountCode || '251000']]);
    
    if (!tdsAccId) return res.json({ ok: false, error: `TDS account (${tdsAccountCode || '231110'}) not found` });
    if (!debtorAccId) return res.json({ ok: false, error: `Debtor account (${debtorAccountCode || '251000'}) not found` });

    // Step 3: Search TDS lines
    const domain = [
      ['account_id', '=', tdsAccId],
      ['date', '>=', fyStart],
      ['date', '<=', fyEnd],
      ['debit', '>', 0]
    ];
    const tdsLineIds = await odooSearch(url, database, uid, apiKey, 'account.move.line', domain);

    // Step 4: Read TDS lines
    const fields = ['date', 'move_id', 'partner_id', 'company_id', 'name', 'account_id', 'debit', 'credit', 'balance'];
    const tdsLines = await odooRead(url, database, uid, apiKey, 'account.move.line', tdsLineIds, fields);

    // Step 5: Filter by company prefixes
    const prefixList = (prefixes || '').split(',').map(p => p.trim().toUpperCase()).filter(Boolean);
    const filtered = prefixList.length > 0
      ? tdsLines.filter(l => {
          const prefix = (l.name || '').split('/')[0].toUpperCase();
          return prefixList.includes(prefix);
        })
      : tdsLines;

    // Step 6: Enrich with invoice amounts (debtor lines)
    const BATCH = 50;
    for (let i = 0; i < filtered.length; i += BATCH) {
      const batch = filtered.slice(i, i + BATCH);
      await Promise.all(batch.map(async (line) => {
        try {
          const debtorIds = await odooSearch(url, database, uid, apiKey, 'account.move.line', [
            ['move_id', '=', line.move_id[0]],
            ['account_id', '=', debtorAccId],
            ['partner_id', '=', line.partner_id[0]]
          ]);
          if (debtorIds.length > 0) {
            const debtorLines = await odooRead(url, database, uid, apiKey, 'account.move.line', [debtorIds[0]], ['credit']);
            line.invoiceAmount = debtorLines[0]?.credit || 0;
          } else {
            line.invoiceAmount = 0;
          }
        } catch (e) {
          line.invoiceAmount = 0;
        }
      }));
    }

    // Step 7: Transform
    const getQ = (d) => { if(!d) return 'Q1'; const m = new Date(d).getMonth()+1; if(m>=4&&m<=6)return'Q1'; if(m>=7&&m<=9)return'Q2'; if(m>=10&&m<=12)return'Q3'; return'Q4'; };
    const data = filtered.map(l => ({
      deductorName: l.partner_id?.[1] || '',
      tan: '',
      amount: l.invoiceAmount || 0,
      tdsDeducted: l.debit || 0,
      section: '',
      date: l.date || '',
      invoiceNo: l.name || '',
      quarter: getQ(l.date),
      source: 'Odoo ERP',
      journalEntry: l.move_id?.[1] || '',
      odooCompany: l.company_id?.[1] || ''
    }));

    console.log(`✅ TDS Sync: ${data.length} records (from ${tdsLineIds.length} total)`);
    res.json({ ok: true, count: data.length, total: tdsLineIds.length, data });
  } catch (e) {
    console.error('❌ TDS sync error:', e.message);
    res.status(400).json({ ok: false, error: e.message });
  }
});

// ── Odoo XML-RPC Helpers ─────────────────────────────────────
function escXML(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function serVal(v) {
  if (v === null || v === undefined) return '<value><nil/></value>';
  if (typeof v === 'string') return `<value><string>${escXML(v)}</string></value>`;
  if (typeof v === 'number') return Number.isInteger(v) ? `<value><int>${v}</int></value>` : `<value><double>${v}</double></value>`;
  if (typeof v === 'boolean') return `<value><boolean>${v?'1':'0'}</boolean></value>`;
  if (Array.isArray(v)) return `<value><array><data>${v.map(serVal).join('')}</data></array></value>`;
  if (typeof v === 'object') {
    const m = Object.entries(v).map(([k,val]) => `<member><name>${escXML(k)}</name>${serVal(val)}</member>`).join('');
    return `<value><struct>${m}</struct></value>`;
  }
  return '<value><nil/></value>';
}

function buildXMLRPC(method, params) {
  return `<?xml version="1.0"?><methodCall><methodName>${method}</methodName><params>${params.map(p=>`<param>${serVal(p)}</param>`).join('')}</params></methodCall>`;
}

function parseXMLResponse(text) {
  // Simple XML-RPC response parser (server-side)
  const faultMatch = text.match(/<fault>[\s\S]*?<string>([\s\S]*?)<\/string>/);
  if (faultMatch) throw new Error(faultMatch[1]);
  
  // Extract values
  const values = [];
  // Handle array of integers (search results)
  const intMatches = [...text.matchAll(/<int>(-?\d+)<\/int>/g)];
  const doubleMatches = [...text.matchAll(/<double>([\d.]+)<\/double>/g)];
  const stringMatches = [...text.matchAll(/<string>([\s\S]*?)<\/string>/g)];
  
  // For search results (array of ints)
  if (text.includes('<array>') && intMatches.length > 0 && !text.includes('<struct>')) {
    return intMatches.map(m => parseInt(m[1]));
  }
  
  // For single int (authenticate)
  if (!text.includes('<array>') && intMatches.length === 1 && !text.includes('<struct>')) {
    return parseInt(intMatches[0][1]);
  }
  
  // For complex responses (structs/read), return raw XML for client parsing
  // or parse structs server-side
  return parseStructArray(text);
}

function parseStructArray(xml) {
  const results = [];
  // Split by struct tags
  const structPattern = /<struct>([\s\S]*?)<\/struct>/g;
  let match;
  while ((match = structPattern.exec(xml)) !== null) {
    const obj = {};
    const memberPattern = /<member>\s*<name>([\s\S]*?)<\/n>\s*<value>([\s\S]*?)<\/value>\s*<\/member>/g;
    let mMatch;
    while ((mMatch = memberPattern.exec(match[1])) !== null) {
      const name = mMatch[1].trim();
      const valXml = mMatch[2];
      obj[name] = parseSimpleValue(valXml);
    }
    results.push(obj);
  }
  return results;
}

function parseSimpleValue(valXml) {
  let m;
  if ((m = valXml.match(/<int>(-?\d+)<\/int>/))) return parseInt(m[1]);
  if ((m = valXml.match(/<double>([\d.-]+)<\/double>/))) return parseFloat(m[1]);
  if ((m = valXml.match(/<boolean>([01])<\/boolean>/))) return m[1] === '1';
  if (valXml.includes('<nil/>')) return null;
  if ((m = valXml.match(/<string>([\s\S]*?)<\/string>/))) return m[1];
  // Array — check for nested arrays (e.g. partner_id = [id, "name"])
  if (valXml.includes('<array>')) {
    const items = [];
    const itemPattern = /<value>([\s\S]*?)<\/value>/g;
    const dataMatch = valXml.match(/<data>([\s\S]*?)<\/data>/);
    if (dataMatch) {
      let iMatch;
      // Parse each value in the array
      const inner = dataMatch[1];
      const vals = inner.split('</value>').filter(v => v.includes('<value>'));
      vals.forEach(v => {
        const cleaned = v.substring(v.indexOf('<value>')) + '</value>';
        items.push(parseSimpleValue(cleaned.replace(/^<value>/, '').replace(/<\/value>$/, '')));
      });
    }
    return items;
  }
  // Plain text
  const textMatch = valXml.match(/^([^<]+)$/);
  if (textMatch) return textMatch[1].trim();
  return valXml;
}

async function odooCall(url, endpoint, body) {
  const fullUrl = url.replace(/\/$/, '') + endpoint;
  const response = await fetch(fullUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'text/xml' },
    body
  });
  const text = await response.text();
  return parseXMLResponse(text);
}

async function odooAuth(url, database, username, apiKey) {
  const body = buildXMLRPC('authenticate', [database, username, apiKey, {}]);
  const fullUrl = url.replace(/\/$/, '') + '/xmlrpc/2/common';
  const response = await fetch(fullUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'text/xml' },
    body
  });
  const text = await response.text();

  // Check for fault
  const faultMatch = text.match(/<fault>[\s\S]*?<string>([\s\S]*?)<\/string>/);
  if (faultMatch) throw new Error(`Odoo auth fault: ${faultMatch[1]}`);

  // Authenticate returns a single <int> (the UID) or <boolean>0</boolean> for failure
  const boolMatch = text.match(/<boolean>([01])<\/boolean>/);
  if (boolMatch && boolMatch[1] === '0') throw new Error('Authentication failed — check credentials');

  const uidMatch = text.match(/<int>(\d+)<\/int>/);
  if (!uidMatch) throw new Error('Authentication failed — no UID returned');

  const uid = parseInt(uidMatch[1], 10);
  if (isNaN(uid) || uid <= 0) throw new Error('Authentication failed — invalid UID');
  return uid;
}

async function odooSearch(url, database, uid, apiKey, model, domain) {
  const body = buildXMLRPC('execute_kw', [database, uid, apiKey, model, 'search', [domain]]);
  const result = await odooCall(url, '/xmlrpc/2/object', body);
  return Array.isArray(result) ? result : [];
}

async function odooSearchOne(url, database, uid, apiKey, model, domain) {
  const ids = await odooSearch(url, database, uid, apiKey, model, domain);
  return ids.length > 0 ? ids[0] : null;
}

async function odooRead(url, database, uid, apiKey, model, ids, fields) {
  if (!ids.length) return [];
  const body = buildXMLRPC('execute_kw', [database, uid, apiKey, model, 'read', [ids], { fields }]);
  const result = await odooCall(url, '/xmlrpc/2/object', body);
  return Array.isArray(result) ? result : [];
}

// ══════════════════════════════════════════════════════════════
//  GMAIL OAUTH2 + API PROXY
//  Flow: Client opens Google OAuth popup → gets code → server
//  exchanges code for tokens → stores refresh_token in Firebase
// ══════════════════════════════════════════════════════════════
const GMAIL_SCOPES = 'https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send';

// Step 1: Get OAuth URL
app.post('/api/gmail/auth-url', (req, res) => {
  const { clientId, redirectUri } = req.body;
  const params = new URLSearchParams({
    client_id: clientId,
    redirect_uri: redirectUri || `${req.protocol}://${req.get('host')}/api/gmail/callback`,
    response_type: 'code',
    scope: GMAIL_SCOPES,
    access_type: 'offline',
    prompt: 'consent'
  });
  res.json({ ok: true, url: `https://accounts.google.com/o/oauth2/v2/auth?${params}` });
});

// Step 2: Exchange code for tokens
app.post('/api/gmail/exchange', async (req, res) => {
  try {
    const { code, clientId, clientSecret, redirectUri } = req.body;
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id: clientId,
        client_secret: clientSecret,
        redirect_uri: redirectUri || `${req.protocol}://${req.get('host')}/api/gmail/callback`,
        grant_type: 'authorization_code'
      })
    });
    const tokens = await tokenRes.json();
    if (tokens.error) return res.json({ ok: false, error: tokens.error_description || tokens.error });
    
    // Save refresh token to Firebase
    if (db && tokens.refresh_token) {
      await db.collection('tds_config').doc('gmail_tokens').set({
        refreshToken: tokens.refresh_token,
        updatedAt: new Date().toISOString()
      });
    }
    
    res.json({ ok: true, accessToken: tokens.access_token, expiresIn: tokens.expires_in, refreshToken: tokens.refresh_token });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Step 3: Refresh access token
app.post('/api/gmail/refresh', async (req, res) => {
  try {
    const { clientId, clientSecret, refreshToken } = req.body;
    let token = refreshToken;
    
    // If no token provided, load from Firebase
    if (!token && db) {
      const doc = await db.collection('tds_config').doc('gmail_tokens').get();
      if (doc.exists) token = doc.data().refreshToken;
    }
    if (!token) return res.json({ ok: false, error: 'No refresh token available' });
    
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: clientId,
        client_secret: clientSecret,
        refresh_token: token,
        grant_type: 'refresh_token'
      })
    });
    const data = await tokenRes.json();
    if (data.error) return res.json({ ok: false, error: data.error });
    res.json({ ok: true, accessToken: data.access_token, expiresIn: data.expires_in });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Gmail API proxy — list messages, get message, send
app.post('/api/gmail/api', async (req, res) => {
  try {
    const { accessToken, endpoint, method, body } = req.body;
    const url = `https://gmail.googleapis.com/gmail/v1/users/me/${endpoint}`;
    const opts = {
      method: method || 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      }
    };
    if (body) opts.body = JSON.stringify(body);
    const response = await fetch(url, opts);
    const data = await response.json();
    res.json({ ok: true, data });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// OAuth callback page (opened in popup)
app.get('/api/gmail/callback', (req, res) => {
  const code = req.query.code;
  const error = req.query.error;
  res.send(`<!DOCTYPE html><html><body><script>
    window.opener && window.opener.postMessage(${JSON.stringify({ type: 'gmail-oauth', code, error })}, '*');
    window.close();
  </script><p>${code ? 'Connected! This window will close.' : 'Error: ' + (error || 'unknown')}</p></body></html>`);
});

// ══════════════════════════════════════════════════════════════
//  HEALTH CHECK
// ══════════════════════════════════════════════════════════════
app.get('/health', (req, res) => {
  res.json({ ok: true, firebase: !!db, version: '1.0.0' });
});

// ── SPA fallback (serve index.html for all non-API routes) ───
app.get('*', (req, res) => {
  const indexPath = fs.existsSync(path.join(distPath, 'index.html'))
    ? path.join(distPath, 'index.html')
    : path.join(__dirname, 'index.html');
  if (fs.existsSync(indexPath)) {
    res.sendFile(indexPath);
  } else {
    res.send(`<h2 style="font-family:Segoe UI;padding:40px">26AS Reconciliation Portal — build the React app first: npm run build</h2>`);
  }
});

// ── Start ────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`╔══════════════════════════════════════════════════╗`);
  console.log(`║  26AS RECON PORTAL  v1.0  →  port ${PORT}            ║`);
  console.log(`╠══════════════════════════════════════════════════╣`);
  console.log(`║  Storage : Firebase Firestore                    ║`);
  console.log(`║  POST /api/odoo/test       — test Odoo login     ║`);
  console.log(`║  POST /api/odoo/sync-tds   — sync TDS from Odoo  ║`);
  console.log(`║  POST /api/gmail/auth-url  — Gmail OAuth URL     ║`);
  console.log(`║  POST /api/gmail/exchange  — exchange OAuth code  ║`);
  console.log(`║  POST /api/gmail/api       — proxy Gmail API     ║`);
  console.log(`║  GET  /api/state           — load all state      ║`);
  console.log(`║  POST /api/state/:key      — save a state key    ║`);
  console.log(`╚══════════════════════════════════════════════════╝\n`);
  console.log(`  ➡  Open http://localhost:${PORT}\n`);
});
