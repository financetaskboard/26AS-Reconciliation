/**
 * 26AS RECONCILIATION PORTAL — Web Server v3.0
 * 
 * Fixes:
 *   - Odoo: tries /web/session/authenticate (password) THEN /jsonrpc (API key)
 *   - Firebase: sanitizeKey in ALL storage functions
 *   - Gmail: force https redirect on Render (x-forwarded-proto)
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

const distPath = path.join(__dirname, 'dist');
if (fs.existsSync(distPath)) app.use(express.static(distPath));
app.use(express.static(__dirname));

function getBaseUrl(req) {
  const proto = req.headers['x-forwarded-proto'] || req.protocol || 'http';
  return `${proto}://${req.get('host')}`;
}

// ══════════════════════════════════════════════════════════════
//  FIREBASE
// ══════════════════════════════════════════════════════════════
let db = null;
try {
  let sa;
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    sa = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    const kp = path.join(__dirname, 'serviceAccountKey.json');
    if (fs.existsSync(kp)) sa = JSON.parse(fs.readFileSync(kp, 'utf8'));
  }
  if (sa) {
    if (!admin.apps.length) admin.initializeApp({ credential: admin.credential.cert(sa) });
    db = admin.firestore();
    console.log('✅ Firebase connected');
  }
} catch (e) { console.error('❌ Firebase:', e.message); }

const CHUNK_SIZE = 400, CHUNK_LIMIT = 900000;
const sanitizeKey = k => String(k).replace(/[\/\\:]/g, '_');

async function fbSave(key, value) {
  if (!db) return;
  const sk = sanitizeKey(key), col = db.collection('tds_state');
  const js = JSON.stringify(value);
  const sz = Buffer.byteLength(js, 'utf8');
  const sizeKB = Math.round(sz / 1024);

  // Small enough for single doc (< 900KB)
  if (sz <= CHUNK_LIMIT) {
    await col.doc(sk).set({ value, updatedAt: new Date().toISOString() });
    console.log(`  💾 [${sk}] ${sizeKB} KB`);
    return;
  }

  // Large data: split JSON string into ~800KB text chunks
  // This works for ANY data type (arrays, objects, nested structures)
  console.log(`  💾 [${sk}] ${sizeKB} KB — chunking as text...`);
  const STR_CHUNK = 750000; // ~750KB per chunk (safe under 1MB Firestore limit)
  const strChunks = [];
  for (let i = 0; i < js.length; i += STR_CHUNK) {
    strChunks.push(js.substring(i, i + STR_CHUNK));
  }

  // Delete old chunks first (in case count changed)
  try {
    const oldMeta = await col.doc(sk).get();
    if (oldMeta.exists && oldMeta.data().chunked) {
      const oldCount = oldMeta.data().chunkCount || 0;
      const delBatch = db.batch();
      for (let i = 0; i < oldCount; i++) delBatch.delete(col.doc(`${sk}_chunk_${i}`));
      await delBatch.commit();
    }
  } catch(e) {}

  // Write new chunks (Firestore batch max 500 ops, we'll be well under)
  const batch = db.batch();
  batch.set(col.doc(sk), {
    chunked: true, chunkCount: strChunks.length, textMode: true,
    totalSize: sz, updatedAt: new Date().toISOString()
  });
  strChunks.forEach((chunk, i) => {
    batch.set(col.doc(`${sk}_chunk_${i}`), { text: chunk });
  });
  await batch.commit();
  console.log(`  💾 [${sk}] ${sizeKB} KB → ${strChunks.length} text chunks`);
}

async function fbLoad(key) {
  if (!db) return undefined;
  const sk = sanitizeKey(key), col = db.collection('tds_state');
  const meta = await col.doc(sk).get();
  if (!meta.exists) return undefined;
  const d = meta.data();
  if (!d.chunked) return d.value;

  const docs = await Promise.all(
    Array.from({ length: d.chunkCount }, (_, i) => col.doc(`${sk}_chunk_${i}`).get())
  );

  if (d.textMode) {
    // Text-mode chunks: reassemble JSON string and parse
    const jsonStr = docs.map(doc => doc.exists ? (doc.data().text || '') : '').join('');
    try { return JSON.parse(jsonStr); } catch(e) { console.error('Chunk parse failed for', sk); return undefined; }
  }

  // Legacy array-mode chunks
  const full = [];
  docs.forEach(doc => { if (doc.exists) full.push(...(doc.data().items || [])); });
  return full;
}

async function fbDelete(key) {
  if (!db) return;
  const sk = sanitizeKey(key), col = db.collection('tds_state');
  const meta = await col.doc(sk).get();
  if (!meta.exists) return;
  const d = meta.data(), batch = db.batch();
  batch.delete(col.doc(sk));
  if (d.chunked) for (let i = 0; i < d.chunkCount; i++) batch.delete(col.doc(`${sk}_chunk_${i}`));
  await batch.commit();
}

// ══════════════════════════════════════════════════════════════
//  STATE API
// ══════════════════════════════════════════════════════════════
const STATE_KEYS = [
  'tds_cfg','tds_companies','tds_26as','tds_ais','tds_books','tds_recon',
  'tds_files','tds_tanmaster','tds_gmail','tds_invoices',
  'companies','selCompanyId','selYear','tanEmails','emailLog',
  'datasets','files','reconResults','reconDone','activeCompanyIndex',
  'tracesCredsMap','localBackupFolder','driveBackupIndex','driveFolderId',
  'gmail_client_id','gmail_client_secret','gmail_access_token',
  'gmail_token_expiry','gmail_refresh_token','gmail_user_email'
];

app.get('/api/state', async (req, res) => {
  try {
    if (!db) return res.json({ ok: true, state: {} });
    const state = {};
    await Promise.all(STATE_KEYS.map(async k => {
      try { const v = await fbLoad(k); if (v !== undefined) state[k] = v; } catch(e) {}
    }));
    res.json({ ok: true, state });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/api/state/:key', async (req, res) => {
  try {
    const val = await fbLoad(req.params.key);
    res.json({ ok: true, value: val });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/api/state/:key', async (req, res) => {
  try {
    const value = req.body?.value;
    if (value === undefined) return res.status(400).json({ ok: false, error: 'Missing value' });
    await fbSave(req.params.key, value);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.delete('/api/state', async (req, res) => {
  try {
    await Promise.all(STATE_KEYS.map(k => fbDelete(k).catch(() => {})));
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ══════════════════════════════════════════════════════════════
//  ODOO — DUAL AUTH (password via web API, API key via jsonrpc)
//
//  Try 1: /web/session/authenticate (works with passwords)
//         → use /web/dataset/call_kw with session cookie
//  Try 2: /jsonrpc service=common (works with API keys)
//         → use /jsonrpc service=object for data calls
//
//  This is why the GST portal works (uses password) but TDS
//  portal failed (uses API key with /web/session/authenticate).
// ══════════════════════════════════════════════════════════════

async function odooAuth(baseUrl, database, username, credential) {
  const url = baseUrl.replace(/\/$/, '');
  
  // ── Try 1: Web session auth (works with passwords) ─────────
  console.log(`   🔑 Try 1: /web/session/authenticate...`);
  try {
    const resp = await fetch(`${url}/web/session/authenticate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0', method: 'call', id: 1,
        params: { db: database, login: username, password: credential }
      })
    });
    const data = await resp.json();
    if (data.result && data.result.uid && data.result.uid !== false) {
      const uid = data.result.uid;
      const cookie = resp.headers.get('set-cookie') || '';
      console.log(`   ✅ Web auth OK — UID ${uid} (using /web/dataset/call_kw)`);
      
      // Fetch company IDs for multi-company access
      const companyIds = await getCompanyIds({ uid, baseUrl: url, cookie, mode: 'web' });
      return { uid, baseUrl: url, cookie, mode: 'web', database, credential, companyIds };
    }
    console.log(`   ⚠ Web auth: uid=false (credential may be API key, not password)`);
  } catch (e) {
    console.log(`   ⚠ Web auth error: ${e.message}`);
  }

  // ── Try 2: /jsonrpc external API (works with API keys) ─────
  console.log(`   🔑 Try 2: /jsonrpc service=common...`);
  try {
    const resp = await fetch(`${url}/jsonrpc`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0', method: 'call', id: 1,
        params: {
          service: 'common', method: 'authenticate',
          args: [database, username, credential, {}]
        }
      })
    });
    const text = await resp.text();
    let data;
    try { data = JSON.parse(text); } catch(e) {
      console.log(`   ⚠ /jsonrpc returned non-JSON (${text.substring(0,100)})`);
      throw new Error('/jsonrpc endpoint not available');
    }
    if (data.error) throw new Error(data.error.data?.message || data.error.message);
    if (data.result && data.result !== false) {
      const uid = data.result;
      console.log(`   ✅ JSON-RPC auth OK — UID ${uid} (using /jsonrpc service=object)`);
      return { uid, baseUrl: url, mode: 'jsonrpc', database, credential, companyIds: [] };
    }
    console.log(`   ⚠ /jsonrpc auth: result=${data.result}`);
  } catch (e) {
    console.log(`   ⚠ /jsonrpc error: ${e.message}`);
  }

  // ── Try 3: XML-RPC (last resort, most compatible with API keys) ──
  console.log(`   🔑 Try 3: XML-RPC /xmlrpc/2/common...`);
  try {
    const xmlBody = `<?xml version="1.0"?><methodCall><methodName>authenticate</methodName><params><param><value><string>${database}</string></value></param><param><value><string>${username}</string></value></param><param><value><string>${credential}</string></value></param><param><value><struct></struct></value></param></params></methodCall>`;
    const resp = await fetch(`${url}/xmlrpc/2/common`, {
      method: 'POST',
      headers: { 'Content-Type': 'text/xml' },
      body: xmlBody
    });
    const text = await resp.text();
    const uidMatch = text.match(/<(?:int|i4)>(\d+)<\/(?:int|i4)>/);
    const falseMatch = text.match(/<boolean>0<\/boolean>/);
    if (falseMatch) throw new Error('XML-RPC auth returned false');
    if (uidMatch) {
      const uid = parseInt(uidMatch[1]);
      console.log(`   ✅ XML-RPC auth OK — UID ${uid}`);
      // Now establish a web session for JSON data calls
      // Try web auth one more time — sometimes XML-RPC works but we still need web session
      try {
        const webResp = await fetch(`${url}/web/session/authenticate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            jsonrpc: '2.0', method: 'call', id: 2,
            params: { db: database, login: username, password: credential }
          })
        });
        const webData = await webResp.json();
        if (webData.result?.uid) {
          const cookie = webResp.headers.get('set-cookie') || '';
          console.log(`   ✅ Web session established after XML-RPC auth`);
          return { uid, baseUrl: url, cookie, mode: 'web', database, credential, companyIds: [] };
        }
      } catch(e) {}
      // Fall back to jsonrpc for data calls
      return { uid, baseUrl: url, mode: 'jsonrpc', database, credential, companyIds: [] };
    }
    throw new Error('No UID in XML-RPC response');
  } catch (e) {
    console.log(`   ⚠ XML-RPC error: ${e.message}`);
  }

  throw new Error('All 3 auth methods failed. Please enter your Odoo LOGIN PASSWORD (same password you use to login at ' + url + '). API keys may not work on Odoo.com — use your actual password instead.');
}

async function getCompanyIds(session) {
  try {
    const result = await odooCall(session, 'res.users', 'read', [[session.uid]], { fields: ['company_ids', 'company_id'] });
    return result?.[0]?.company_ids || (result?.[0]?.company_id?.[0] ? [result[0].company_id[0]] : []);
  } catch(e) { return []; }
}

async function odooCall(session, model, method, args = [], kwargs = {}) {
  const ctx = {
    lang: 'en_IN',
    ...(session.companyIds?.length ? { allowed_company_ids: session.companyIds } : {})
  };

  if (session.mode === 'web') {
    // Use /web/dataset/call_kw with session cookie (same as GST portal)
    const resp = await fetch(`${session.baseUrl}/web/dataset/call_kw`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(session.cookie ? { Cookie: session.cookie } : {})
      },
      body: JSON.stringify({
        jsonrpc: '2.0', method: 'call', id: Math.floor(Math.random() * 99999),
        params: { model, method, args, kwargs: { context: ctx, ...kwargs } }
      })
    });
    const data = await resp.json();
    if (data.error) throw new Error(data.error.data?.message || data.error.message || 'Odoo call failed');
    return data.result;
  }

  // Use /jsonrpc service=object (external API — works with API keys)
  const resp = await fetch(`${session.baseUrl}/jsonrpc`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0', method: 'call', id: Math.floor(Math.random() * 99999),
      params: {
        service: 'object', method: 'execute_kw',
        args: [session.database, session.uid, session.credential, model, method, args, kwargs]
      }
    })
  });
  const data = await resp.json();
  if (data.error) throw new Error(data.error.data?.message || data.error.message || 'Odoo call failed');
  return data.result;
}

app.post('/api/odoo/test', async (req, res) => {
  try {
    const { url, db: database, username, apiKey } = req.body;
    console.log(`\n🔌 Odoo test: ${url} | db=${database} | user=${username}`);
    const session = await odooAuth(url, database, username, apiKey);
    res.json({ ok: true, uid: session.uid, mode: session.mode, message: `Connected as UID ${session.uid} via ${session.mode}` });
  } catch (e) {
    console.error('❌ Odoo test:', e.message);
    res.json({ ok: false, error: e.message });
  }
});

app.post('/api/odoo/sync-tds', async (req, res) => {
  try {
    const { url, db: database, username, apiKey, fyStart, fyEnd, tdsAccountCode, debtorAccountCode, prefixes } = req.body;
    console.log(`\n🔄 TDS Sync: ${url} | ${fyStart} → ${fyEnd}`);
    const session = await odooAuth(url, database, username, apiKey);

    const tdsAccIds = await odooCall(session, 'account.account', 'search', [[['code', '=', tdsAccountCode || '231110']]]);
    const debtorAccIds = await odooCall(session, 'account.account', 'search', [[['code', '=', debtorAccountCode || '251000']]]);
    if (!tdsAccIds?.length) return res.json({ ok: false, error: `TDS account (${tdsAccountCode || '231110'}) not found` });
    if (!debtorAccIds?.length) return res.json({ ok: false, error: `Debtor account (${debtorAccountCode || '251000'}) not found` });
    const tdsAccId = tdsAccIds[0], debtorAccId = debtorAccIds[0];

    const BATCH = 200;
    const domain = [['account_id','=',tdsAccId],['date','>=',fyStart],['date','<=',fyEnd],['debit','>',0],['parent_state','=','posted']];
    const allLines = [];
    let offset = 0;
    while (true) {
      const batch = await odooCall(session, 'account.move.line', 'search_read', [domain], {
        fields: ['date','move_id','partner_id','company_id','name','debit','credit','balance'],
        limit: BATCH, offset, order: 'date asc'
      });
      allLines.push(...batch);
      console.log(`   TDS lines offset=${offset} → ${batch.length} (total: ${allLines.length})`);
      if (batch.length < BATCH) break;
      offset += BATCH;
    }

    const prefixList = (prefixes || '').split(',').map(p => p.trim().toUpperCase()).filter(Boolean);
    const filtered = prefixList.length > 0
      ? allLines.filter(l => prefixList.includes((l.name||'').split('/')[0].toUpperCase()))
      : allLines;
    console.log(`   Filtered: ${filtered.length} of ${allLines.length}`);

    const moveIds = [...new Set(filtered.map(l => l.move_id?.[0]).filter(Boolean))];
    const invoiceAmounts = {};
    for (let i = 0; i < moveIds.length; i += BATCH) {
      const ids = moveIds.slice(i, i + BATCH);
      const dl = await odooCall(session, 'account.move.line', 'search_read',
        [[['move_id','in',ids],['account_id','=',debtorAccId]]],
        { fields: ['move_id','credit'] });
      dl.forEach(d => { const m = d.move_id?.[0]; if(m) invoiceAmounts[m] = (invoiceAmounts[m]||0) + (d.credit||0); });
    }

    const getQ = d => { if(!d) return 'Q1'; const m=new Date(d).getMonth()+1; if(m>=4&&m<=6)return'Q1';if(m>=7&&m<=9)return'Q2';if(m>=10&&m<=12)return'Q3';return'Q4'; };
    const data = filtered.map(l => ({
      deductorName: l.partner_id?.[1]||'', tan: '',
      amount: invoiceAmounts[l.move_id?.[0]]||0, tdsDeducted: l.debit||0,
      section: '', date: l.date||'', invoiceNo: l.name||'',
      quarter: getQ(l.date), source: 'Odoo ERP',
      journalEntry: l.move_id?.[1]||'', odooCompany: l.company_id?.[1]||''
    }));
    console.log(`✅ TDS Sync: ${data.length} records via ${session.mode}`);
    res.json({ ok: true, count: data.length, total: allLines.length, data });
  } catch (e) {
    console.error('❌ TDS sync:', e.message);
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.post('/api/odoo/sync-invoices', async (req, res) => {
  try {
    const { url, db: database, username, apiKey, fyStart, fyEnd, prefixes } = req.body;
    console.log(`\n🧾 Invoice Sync: ${url} | ${fyStart} → ${fyEnd}`);
    const session = await odooAuth(url, database, username, apiKey);

    const prefixList = (prefixes || '').split(',').map(p => p.trim().toUpperCase()).filter(Boolean);

    const BATCH = 200;
    const domain = [
      ['move_type', '=', 'out_invoice'],
      ['state', '=', 'posted'],
      ['invoice_date', '>=', fyStart],
      ['invoice_date', '<=', fyEnd]
    ];

    const allInvoices = [];
    let offset = 0;
    while (true) {
      const batch = await odooCall(session, 'account.move', 'search_read', [domain], {
        fields: ['name', 'partner_id', 'invoice_date', 'amount_untaxed', 'amount_total', 'id'],
        limit: BATCH, offset, order: 'invoice_date asc'
      });
      allInvoices.push(...batch);
      console.log(`   Invoices offset=${offset} → ${batch.length} (total: ${allInvoices.length})`);
      if (batch.length < BATCH) break;
      offset += BATCH;
    }

    const filtered = prefixList.length > 0
      ? allInvoices.filter(inv => prefixList.includes((inv.name || '').split('/')[0].toUpperCase()))
      : allInvoices;
    console.log(`   Filtered: ${filtered.length} of ${allInvoices.length}`);

    const data = filtered.map(inv => ({
      invoiceNo: inv.name || '',
      invoiceDate: inv.invoice_date || '',
      partnerName: inv.partner_id?.[1] || '',
      amountUntaxed: inv.amount_untaxed || 0,
      amountTotal: inv.amount_total || 0,
      odooId: inv.id
    }));

    console.log(`✅ Invoice Sync: ${data.length} records`);
    res.json({ ok: true, count: data.length, data });
  } catch (e) {
    console.error('❌ Invoice sync:', e.message);
    res.status(400).json({ ok: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
//  ODOO - CREATE JOURNAL ENTRIES (TDS Receivable)
// ══════════════════════════════════════════════════════════════
app.post('/api/odoo/create-journal-entries', async (req, res) => {
  try {
    const { url, db: database, username, apiKey, entries, journalCode, tdsAccountCode, debtorAccountCode } = req.body;
    console.log(`📝 Creating ${entries?.length || 0} journal entries in Odoo...`);
    
    const session = await odooAuth(url, database, username, apiKey);
    
    // ALWAYS use Ginesys company
    const companies = await odooCall(session, 'res.company', 'search_read', 
      [[['name', 'ilike', 'Ginesys']]], 
      { fields: ['id', 'name'], limit: 1 }
    );
    
    if (!companies.length) throw new Error(`Company 'Ginesys' not found in Odoo`);
    
    const companyId = companies[0].id;
    const companyName = companies[0].name;
    console.log(`   Using company: ${companyName} (ID: ${companyId})`);
    
    // Find TDS account in Ginesys company - prefer 25-26 (current FY)
    let tdsAccounts = await odooCall(session, 'account.account', 'search_read', 
      [[['code', '=', tdsAccountCode || '231110'], ['company_id', '=', companyId], ['name', 'ilike', '25-26']]], 
      { fields: ['id', 'company_id', 'name'], limit: 1 }
    );
    
    // Fallback to any account with that code in Ginesys
    if (!tdsAccounts.length) {
      tdsAccounts = await odooCall(session, 'account.account', 'search_read', 
        [[['code', '=', tdsAccountCode || '231110'], ['company_id', '=', companyId]]], 
        { fields: ['id', 'company_id', 'name'], limit: 1 }
      );
    }
    
    if (!tdsAccounts.length) throw new Error(`TDS Account '${tdsAccountCode || '231110'}' not found in Ginesys company`);
    
    const tdsAccount = tdsAccounts[0];
    console.log(`   TDS Account: ${tdsAccount.id} - ${tdsAccount.name}`);
    
    // Find debtor account in Ginesys company
    const debtorAccounts = await odooCall(session, 'account.account', 'search_read', 
      [[['code', '=', debtorAccountCode || '251000'], ['company_id', '=', companyId]]], 
      { fields: ['id', 'name'], limit: 1 }
    );
    
    if (!debtorAccounts.length) throw new Error(`Debtor Account '${debtorAccountCode || '251000'}' not found in company ${companyName}`);
    
    const debtorAccount = debtorAccounts[0];
    console.log(`   Debtor Account: ${debtorAccount.id} - ${debtorAccount.name}`);
    
    // Find journal in the SAME company (TDS journal)
    let journalId = null;
    const tdsJournals = await odooCall(session, 'account.journal', 'search_read', 
      [[['name', 'ilike', 'TDS'], ['company_id', '=', companyId]]], 
      { fields: ['id', 'name', 'code'], limit: 5 }
    );
    console.log(`   Found TDS journals in ${companyName}:`, tdsJournals);
    
    if (tdsJournals.length) {
      journalId = tdsJournals[0].id;
    } else {
      // Fallback to any general journal in that company
      const generalJournals = await odooCall(session, 'account.journal', 'search_read', 
        [[['type', '=', 'general'], ['company_id', '=', companyId]]], 
        { fields: ['id', 'name'], limit: 1 }
      );
      if (generalJournals.length) journalId = generalJournals[0].id;
    }
    
    if (!journalId) throw new Error(`No suitable journal found in company ${companyName}. Please create a 'TDS Receivable' journal.`);
    console.log(`   Using journal ID: ${journalId}`);
    
    const results = [];
    
    for (const entry of entries) {
      try {
        // Find partner by external ID or name
        let partnerId = null;
        if (entry.partnerExternalId) {
          const partnerIds = await odooCall(session, 'ir.model.data', 'search_read', 
            [[['name', '=', entry.partnerExternalId], ['model', '=', 'res.partner']]], 
            { fields: ['res_id'], limit: 1 }
          );
          if (partnerIds.length) partnerId = partnerIds[0].res_id;
        }
        if (!partnerId && entry.partnerName) {
          const partnerIds = await odooCall(session, 'res.partner', 'search', [[['name', 'ilike', entry.partnerName]]], { limit: 1 });
          if (partnerIds.length) partnerId = partnerIds[0];
        }
        
        // Create journal entry with correct company_id
        const moveVals = {
          company_id: companyId,
          journal_id: journalId,
          date: entry.date,
          ref: entry.invoiceNo || `TDS Entry - ${entry.date}`,
          move_type: 'entry',
          line_ids: [
            [0, 0, {
              account_id: tdsAccount.id,
              partner_id: partnerId,
              name: entry.invoiceNo || 'TDS Receivable',
              debit: entry.amount,
              credit: 0,
              company_id: companyId
            }],
            [0, 0, {
              account_id: debtorAccount.id,
              partner_id: partnerId,
              name: entry.invoiceNo || 'TDS Receivable',
              debit: 0,
              credit: entry.amount,
              company_id: companyId
            }]
          ]
        };
        
        console.log(`   Creating entry for ${entry.invoiceNo} in ${companyName}...`);
        const moveId = await odooCall(session, 'account.move', 'create', [moveVals]);
        
        // Try to post the entry
        try {
          await odooCall(session, 'account.move', 'action_post', [[moveId]]);
          console.log(`   ✅ Created & posted: ${moveId}`);
        } catch (postErr) {
          console.log(`   ⚠ Created ${moveId} but could not post: ${postErr.message}`);
        }
        
        results.push({ invoiceNo: entry.invoiceNo, moveId, status: 'created' });
        
      } catch (entryErr) {
        results.push({ invoiceNo: entry.invoiceNo, status: 'error', error: entryErr.message });
        console.log(`   ❌ Failed for ${entry.invoiceNo}: ${entryErr.message}`);
      }
    }
    
    const created = results.filter(r => r.status === 'created').length;
    const failed = results.filter(r => r.status === 'error').length;
    
    console.log(`✅ Journal Entries: ${created} created, ${failed} failed`);
    res.json({ ok: true, created, failed, results, company: companyName });
    
  } catch (e) {
    console.error('❌ Create journal entries:', e.message);
    res.status(400).json({ ok: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
//  GMAIL OAUTH2
// ══════════════════════════════════════════════════════════════
const GMAIL_SCOPES = 'https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send';

app.post('/api/gmail/auth-url', (req, res) => {
  const { clientId, redirectUri } = req.body;
  const cb = redirectUri || `${getBaseUrl(req)}/api/gmail/callback`;
  console.log(`📧 auth-url redirect: ${cb}`);
  const params = new URLSearchParams({
    client_id: clientId, redirect_uri: cb, response_type: 'code',
    scope: GMAIL_SCOPES, access_type: 'offline', prompt: 'consent'
  });
  res.json({ ok: true, url: `https://accounts.google.com/o/oauth2/v2/auth?${params}` });
});

app.post('/api/gmail/exchange', async (req, res) => {
  try {
    const { code, clientId, clientSecret, redirectUri } = req.body;
    const cb = redirectUri || `${getBaseUrl(req)}/api/gmail/callback`;
    console.log(`📧 exchange redirect: ${cb}`);
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: clientId, client_secret: clientSecret, redirect_uri: cb, grant_type: 'authorization_code' })
    });
    const t = await r.json();
    console.log(`📧 token result:`, t.error || 'OK');
    if (t.error) return res.json({ ok: false, error: t.error_description || t.error });
    if (db && t.refresh_token) await db.collection('tds_config').doc('gmail_tokens').set({ refreshToken: t.refresh_token, updatedAt: new Date().toISOString() });
    res.json({ ok: true, accessToken: t.access_token, expiresIn: t.expires_in, refreshToken: t.refresh_token });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

app.post('/api/gmail/refresh', async (req, res) => {
  try {
    const { clientId, clientSecret, refreshToken } = req.body;
    let tok = refreshToken;
    if (!tok && db) { const d = await db.collection('tds_config').doc('gmail_tokens').get(); if (d.exists) tok = d.data().refreshToken; }
    if (!tok) return res.json({ ok: false, error: 'No refresh token' });
    const r = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: tok, grant_type: 'refresh_token' })
    });
    const d = await r.json();
    if (d.error) return res.json({ ok: false, error: d.error });
    res.json({ ok: true, accessToken: d.access_token, expiresIn: d.expires_in });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

app.post('/api/gmail/api', async (req, res) => {
  try {
    const { accessToken, endpoint, method, body } = req.body;
    const opts = { method: method || 'GET', headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' } };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/${endpoint}`, opts);
    res.json({ ok: true, data: await r.json() });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

app.get('/api/gmail/callback', (req, res) => {
  const { code, error } = req.query;
  res.send(`<!DOCTYPE html><html><body><script>
    window.opener && window.opener.postMessage(${JSON.stringify({ type: 'gmail-oauth', code, error })}, '*');
    setTimeout(() => window.close(), 1500);
  </script><p>${code ? '✅ Connected!' : '❌ ' + (error || 'unknown')}</p></body></html>`);
});

// ══════════════════════════════════════════════════════════════
app.get('/health', (req, res) => res.json({ ok: true, firebase: !!db, v: '3.0' }));

app.get('*', (req, res) => {
  const ip = path.join(distPath, 'index.html');
  const ip2 = path.join(__dirname, 'index.html');
  if (fs.existsSync(ip)) res.sendFile(ip);
  else if (fs.existsSync(ip2)) res.sendFile(ip2);
  else res.send('<h2 style="font-family:Segoe UI;padding:40px">Run: npm run build</h2>');
});

app.listen(PORT, () => {
  console.log(`\n  26AS RECON v3.0 → port ${PORT}`);
  console.log(`  Odoo: dual auth (password + API key)`);
  console.log(`  Firebase: ${db ? 'connected' : 'not configured'}\n`);
});
