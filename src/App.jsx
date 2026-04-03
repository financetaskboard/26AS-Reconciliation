import { useState, useCallback, useRef, useEffect } from "react";
import * as XLSX from "xlsx";
import Papa from "papaparse";


// ── ROBUST ZIP EXTRACTOR (reads Central Directory — handles data descriptors) ──
// TRACES ZIPs use bit-3 data descriptors, meaning local headers have compSize=0.
// We read the Central Directory at the END of the ZIP which always has correct sizes.
async function extractZip(arrayBuffer) {
  const u8 = new Uint8Array(arrayBuffer);
  const view = new DataView(arrayBuffer);
  const dec = new TextDecoder("utf-8");

  // ── Step 1: Find End of Central Directory record (EOCD) ──
  // Signature: PK\x05\x06 (0x06054b50)
  let eocdOffset = -1;
  for (let i = u8.length - 22; i >= 0; i--) {
    if (u8[i]===0x50&&u8[i+1]===0x4B&&u8[i+2]===0x05&&u8[i+3]===0x06) {
      eocdOffset = i; break;
    }
  }
  if (eocdOffset === -1) throw new Error("Not a valid ZIP file (no EOCD found)");

  const cdOffset = view.getUint32(eocdOffset + 16, true); // Central Directory offset
  const cdSize   = view.getUint32(eocdOffset + 12, true); // Central Directory size
  const totalEntries = view.getUint16(eocdOffset + 10, true);

  // ── Step 2: Read each Central Directory entry to get correct sizes ──
  const entries = [];
  let cdPos = cdOffset;
  for (let e = 0; e < totalEntries; e++) {
    if (u8[cdPos]!==0x50||u8[cdPos+1]!==0x4B||u8[cdPos+2]!==0x01||u8[cdPos+3]!==0x02) break;
    const compression  = view.getUint16(cdPos + 10, true);
    const compSize     = view.getUint32(cdPos + 20, true); // Correct even with data descriptors
    const uncompSize   = view.getUint32(cdPos + 24, true);
    const nameLen      = view.getUint16(cdPos + 28, true);
    const extraLen     = view.getUint16(cdPos + 30, true);
    const commentLen   = view.getUint16(cdPos + 32, true);
    const localOffset  = view.getUint32(cdPos + 42, true);
    const name         = dec.decode(u8.slice(cdPos + 46, cdPos + 46 + nameLen));
    entries.push({ name, compression, compSize, uncompSize, localOffset });
    cdPos += 46 + nameLen + extraLen + commentLen;
  }

  // ── Step 3: Extract each file using local header to find data start ──
  const files = [];
  for (const entry of entries) {
    if (entry.name.endsWith("/")) continue; // skip directories
    const lh = entry.localOffset;
    if (u8[lh]!==0x50||u8[lh+1]!==0x4B||u8[lh+2]!==0x03||u8[lh+3]!==0x04) continue;
    const lNameLen  = view.getUint16(lh + 26, true);
    const lExtraLen = view.getUint16(lh + 28, true);
    const dataStart = lh + 30 + lNameLen + lExtraLen;
    const compData  = u8.slice(dataStart, dataStart + entry.compSize);

    let text = "";
    if (entry.compression === 0) {
      // STORED — raw bytes
      text = dec.decode(compData);
    } else if (entry.compression === 8) {
      // DEFLATE — use DecompressionStream (deflate-raw)
      try {
        const ds = new DecompressionStream("deflate-raw");
        const writer = ds.writable.getWriter();
        const reader = ds.readable.getReader();
        // Write all compressed data then close
        await writer.write(compData);
        await writer.close();
        // Read ALL chunks until stream is fully done
        const chunks = [];
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          chunks.push(value);
        }
        const totalLen = chunks.reduce((s, c) => s + c.length, 0);
        const out = new Uint8Array(totalLen);
        let off = 0;
        for (const c of chunks) { out.set(c, off); off += c.length; }
        text = dec.decode(out);
      } catch(e) {
        throw new Error(`DEFLATE decompression failed for ${entry.name}: ${e.message}`);
      }
    }
    if (text) files.push({ name: entry.name.split("/").pop(), text });
  }
  return files;
}


const isElectron = typeof window !== 'undefined' && window.electronAPI?.isElectron;
const isWeb = !isElectron;
const SERVER_BASE = isWeb ? (window.location.protocol === 'file:' ? 'http://localhost:3003' : window.location.origin) : '';

// ── STORAGE HELPERS (web: Firebase ONLY via server API, no localStorage) ─────
async function saveToStore(key, value) {
  try {
    if (isElectron) {
      await window.electronAPI.storeSet(key, value);
    } else if (SERVER_BASE) {
      const res = await fetch(`${SERVER_BASE}/api/state/${encodeURIComponent(key)}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value })
      });
      if (!res.ok) console.warn('Firebase save failed:', key, res.status);
    }
  } catch (e) {
    console.warn('Store save failed:', key, e);
  }
}

async function loadFromStore(key) {
  try {
    if (isElectron) {
      return await window.electronAPI.storeGet(key);
    } else if (SERVER_BASE) {
      const res = await fetch(`${SERVER_BASE}/api/state/${encodeURIComponent(key)}`);
      const json = await res.json();
      if (json.ok && json.value !== undefined) return json.value;
    }
  } catch (e) {
    console.warn('Store load failed:', key, e);
  }
  return null;
}

async function clearStore() {
  try {
    if (isElectron) {
      await window.electronAPI.storeClear();
    } else if (SERVER_BASE) {
      await fetch(`${SERVER_BASE}/api/state`, { method: 'DELETE' });
    }
  } catch (e) {
    console.warn('Store clear failed:', e);
  }
}

// ── Web-only: Push/Pull server sync (Firebase direct) ────────
// Push is used by sidebar button — forces current in-memory state to Firebase
// (auto-save also does this, but Push is a manual guarantee)
// Pull reloads the page so loadFromStore fetches everything from Firebase fresh

// ── Web-only: Odoo sync via server proxy ─────────────────────
async function syncTDSViaServer(company, fyStart, fyEnd) {
  const res = await fetch(`${SERVER_BASE}/api/odoo/sync-tds`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      url: company.odooUrl,
      db: company.odooDatabase,
      username: company.odooUsername,
      apiKey: company.odooPassword,
      fyStart, fyEnd,
      tdsAccountCode: '231110',
      debtorAccountCode: '251000',
      prefixes: (company.prefixes || []).join(',')
    })
  });
  return res.json();
}

// ── PARSER: TRACES 26AS TXT (^ delimited format) ─────────────────────────────
function parse26ASTxt(text) {
  const rows = [];
  const lines = text.split(/\r?\n/).map(l => l.trim());
  let currentDeductor = "";
  let currentTAN = "";
  let rowId = 1;
  // Extract Financial Year from header line (e.g. "2025-26")
  let financialYear = "2025-26";
  for (let i = 0; i < Math.min(lines.length, 30); i++) {
    const cols = lines[i].split("^");
    // Header data row: 03-03-2026^AAACG9344A^ACTIVE^2025-26^...
    if (cols.length >= 4 && /^\d{2}-\d{2}-\d{4}$/.test(cols[0]?.trim()) && /^\d{4}-\d{2}$/.test(cols[3]?.trim())) {
      financialYear = cols[3].trim();
      break;
    }
  }
  const getQuarter = (dateStr) => {
    if (!dateStr) return "";
    const parts = dateStr.split("-");
    const month = parts[1]?.toLowerCase() || "";
    const mm = {jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12}[month] || parseInt(parts[1]);
    if (mm>=4&&mm<=6) return "Q1"; if (mm>=7&&mm<=9) return "Q2";
    if (mm>=10&&mm<=12) return "Q3"; return "Q4";
  };
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line || line.startsWith("^PART") || line.startsWith("^Annual") || line.startsWith("^Sr.") || line.startsWith("File Creation") || line.startsWith("Sr. No.")) continue;
    const cols = line.split("^");
    // Deductor header: starts with a number, TAN at col[2] matching TAN pattern
    if (cols.length >= 3 && /^\d+$/.test(cols[0]?.trim()) && cols[0].trim() && cols[2] && /^[A-Z]{4}\d{5}[A-Z]$/.test(cols[2].trim())) {
      currentDeductor = cols[1]?.trim() || "";
      currentTAN = cols[2]?.trim() || "";
      continue;
    }
    // Transaction row: empty col[0], number at col[1], section at col[2], date at col[3]
    if (cols.length >= 10 && cols[0] === "" && /^\d+$/.test(cols[1]?.trim()) && cols[2]?.trim() && cols[3]?.trim()) {
      const section = cols[2]?.trim() || "";
      const txnDate = cols[3]?.trim() || "";
      const bookingStatus = cols[4]?.trim() || "";
      const amountPaid = parseFloat((cols[7] || "").replace(/,/g, "")) || 0;
      const tdsDeducted = parseFloat((cols[8] || "").replace(/,/g, "")) || 0;
      const tdsDeposited = parseFloat((cols[9] || "").replace(/,/g, "")) || 0;
      if (amountPaid === 0 && tdsDeducted === 0) continue;
      if (!currentTAN) continue;
      rows.push({ id: rowId++, deductorName: currentDeductor, tan: currentTAN, pan: "", section, amountPaid, tdsDeducted, tdsDeposited, date: txnDate, quarter: getQuarter(txnDate), financialYear, bookingStatus, invoiceNo: "", source: "26AS", matchStatus: "Unmatched" });
    }
  }
  return rows;
}

// ── PARSER: CSV ───────────────────────────────────────────────────────────────
function parseCSVFile(text, sourceName) {
  const result = Papa.parse(text, { header: true, skipEmptyLines: true, dynamicTyping: false, transformHeader: h => h.trim() });
  const headers = result.meta.fields || [];
  const norm = h => h.toLowerCase().replace(/[^a-z0-9]/g, "");
  const find = (row, ...keys) => { for (const k of keys) { const hdr = headers.find(h => norm(h).includes(k)); if (hdr && row[hdr] !== undefined) return String(row[hdr]).trim(); } return ""; };
  const toNum = s => parseFloat(String(s).replace(/[,₹\s]/g,"")) || 0;
  const isAIS = sourceName.toLowerCase().includes("ais") || sourceName.toLowerCase().includes("tis");
  const src = isAIS ? "AIS" : sourceName.toLowerCase().includes("26as") ? "26AS" : "Books";
  const getQ = (ds) => { if(!ds) return ""; const d=new Date(ds); if(isNaN(d)) return ""; const m=d.getMonth()+1; if(m>=4&&m<=6)return"Q1";if(m>=7&&m<=9)return"Q2";if(m>=10&&m<=12)return"Q3";return"Q4"; };
  return result.data.map((row, i) => {
    const date = find(row,"date","invoicedate","transactiondate","billdate","voucherdate","docdate","bookingdate","tdsdate");
    const invoiceDate = find(row,"invoicedate","invdate","billdate","docdate") || date;
    const tdsDeducted = toNum(find(row,"tdsdeducted","taxdeducted","tdsamount")) || toNum(find(row,"tds","tax")) || toNum(find(row,"debit"));
    const amountPaid = toNum(find(row,"amountpaid","invoiceamount","grossamount","gross","billamt")) || toNum(find(row,"amount","total")) || toNum(find(row,"debit"));
    // Use invoice date for quarter calculation if available, otherwise use booking date
    const quarterDate = invoiceDate || date;
    return { id:i+1, deductorName:find(row,"deductor","partyname","vendorname","partner","party","payee","name"), tan:find(row,"tan","tanno"), pan:find(row,"pan","panno"), section:find(row,"section","tdssection","natureofpayment","nature"), amountPaid, tdsDeducted, invoiceNo:find(row,"label","invoiceno","invoicenumber","billno","voucherno","journalentry","reference","refno"), date, invoiceDate, quarter:find(row,"quarter","qtr")||getQ(quarterDate), financialYear:find(row,"financialyear","fy","year","ay")||"2025-26", source:src, matchStatus:"Unmatched" };
  }).filter(r => r.deductorName || r.tan || r.amountPaid);
}

function parseExcelFile(base64) { try { const wb=XLSX.read(base64,{type:"base64"}); const ws=wb.Sheets[wb.SheetNames[0]]; return XLSX.utils.sheet_to_csv(ws); } catch{return null;} }
function parse26ASXML(text) { try { const parser=new DOMParser(); const xml=parser.parseFromString(text,"text/xml"); const rows=[]; const get=(el,...tags)=>{for(const t of tags){const n=el.querySelector(t);if(n?.textContent?.trim())return n.textContent.trim();}return"";}; xml.querySelectorAll("TDSEntry,Entry,Row,row").forEach((e,i)=>{const amt=parseFloat(get(e,"AmountPaid","Amount").replace(/,/g,"")||0);const tds=parseFloat(get(e,"TaxDeducted","TDS").replace(/,/g,"")||0);if(!amt&&!tds)return;rows.push({id:i+1,deductorName:get(e,"DeductorName","Name"),tan:get(e,"TAN","tan"),section:get(e,"Section","section"),amountPaid:amt,tdsDeducted:tds,date:get(e,"Date","date"),quarter:get(e,"Quarter","quarter"),financialYear:"2025-26",source:"26AS",matchStatus:"Unmatched"});}); return rows; } catch{return[];} }

// ── MATCHING ENGINE ───────────────────────────────────────────────────────────
function runMatchingEngine(data26AS, dataBooks) {
  const normTAN = s => s?.toUpperCase().trim()||"";
  const map26 = {};
  data26AS.forEach(r => {
    const t = normTAN(r.tan); if (!t) return;
    if (!map26[t]) map26[t] = { tan:t, name:r.deductorName||"", tds:0, deposited:0, txns:0, sections:new Set() };
    map26[t].tds += r.tdsDeducted||0; map26[t].deposited += r.tdsDeposited||r.tdsDeducted||0; map26[t].txns++;
    if (r.section) map26[t].sections.add(r.section);
    if (!map26[t].name && r.deductorName) map26[t].name = r.deductorName;
  });
  const mapBk = {};
  // Collect Books rows with no TAN separately — group by party name
  const noTanBk = {};
  dataBooks.forEach(r => {
    const t = normTAN(r.tan);
    if (!t) {
      // No TAN — group by deductorName so each unique party appears as one row
      const key = (r.deductorName||"").trim() || "Unknown Party";
      if (!noTanBk[key]) noTanBk[key] = { name:key, tds:0, txns:0 };
      noTanBk[key].tds += r.tdsDeducted||0; noTanBk[key].txns++;
      return;
    }
    if (!mapBk[t]) mapBk[t] = { tan:t, name:r.deductorName||"", tds:0, txns:0 };
    mapBk[t].tds += r.tdsDeducted||0; mapBk[t].txns++;
    if (!mapBk[t].name && r.deductorName) mapBk[t].name = r.deductorName;
  });
  const allTANs = new Set([...Object.keys(map26), ...Object.keys(mapBk)]);
  const results = []; let id = 1;
  allTANs.forEach(tan => {
    const a = map26[tan], b = mapBk[tan];
    if (a && b) {
      const tdsDiff = a.tds - b.tds;
      const isTDSMatch = Math.abs(tdsDiff) < 1;
      const isNear = !isTDSMatch && Math.abs(tdsDiff) < 100;
      // Positive diff = 26AS has more TDS than Books = entry missing/short in Books
      const matchStatus = isTDSMatch ? "Matched" : isNear ? "Near Match" : tdsDiff > 0 ? "Missing in Books" : "Mismatch";
      const mismatchReason = isTDSMatch ? "" : tdsDiff > 0 ? `26AS TDS higher by ₹${Math.abs(tdsDiff).toLocaleString("en-IN",{maximumFractionDigits:2})} — check Books entries` : `Books TDS higher by ₹${Math.abs(tdsDiff).toLocaleString("en-IN",{maximumFractionDigits:2})} — possible duplicate in Books`;
      results.push({ id:id++, tan, as_name:a.name, as_tds:a.tds, as_deposited:a.deposited, as_txns:a.txns, as_sections:[...a.sections].join(", "), bk_name:b.name, bk_tds:b.tds, bk_txns:b.txns, tds_diff:tdsDiff, matchStatus, mismatchReason });
    } else if (a) {
      // Check if this deductor's name appears in Books under a different TAN or no TAN
      const normN = s => (s||"").toUpperCase().replace(/[^A-Z0-9]/g,"").trim();
      const an = normN(a.name);
      const nameMatchInBooks = an.length > 3 && dataBooks.some(r => normN(r.deductorName) === an);
      const mismatchReason = nameMatchInBooks
        ? `⚠ Party found in Books — TAN may be wrong or missing in Books entries`
        : "TAN not found in Books";
      results.push({ id:id++, tan, as_name:a.name, as_tds:a.tds, as_deposited:a.deposited, as_txns:a.txns, as_sections:[...a.sections].join(", "), bk_name:"", bk_tds:0, bk_txns:0, tds_diff:a.tds, matchStatus:"Missing in Books", mismatchReason, partyInBooks:nameMatchInBooks });
    } else {
      results.push({ id:id++, tan, as_name:"", as_tds:0, as_deposited:0, as_txns:0, as_sections:"", bk_name:b.name, bk_tds:b.tds, bk_txns:b.txns, tds_diff:-b.tds, matchStatus:"Missing in 26AS", mismatchReason:"TAN not found in 26AS" });
    }
  });
  // Append Books-only rows that have no TAN — shown as "Missing TAN" so user can act on them
  Object.values(noTanBk).forEach(g => {
    results.push({ id:id++, tan:"—", as_name:"", as_tds:0, as_deposited:0, as_txns:0, as_sections:"", bk_name:g.name, bk_tds:g.tds, bk_txns:g.txns, tds_diff:-g.tds, matchStatus:"Missing TAN", mismatchReason:`TAN not assigned in Books — assign TAN to reconcile (${g.txns} txn${g.txns!==1?"s":""})` });
  });
  const order = {"Missing TAN":-1,"Mismatch":0,"Missing in Books":1,"Near Match":2,"Missing in 26AS":3,"Matched":4};
  return results.sort((a,b)=>(order[a.matchStatus]??5)-(order[b.matchStatus]??5));
}

// ── SECTION-WISE MATCHING ENGINE ──────────────────────────────────────────────
function runSectionMatchingEngine(data26AS, dataBooks) {
  const normTAN = s => s?.toUpperCase().trim()||"";
  const normSec = s => s?.toUpperCase().trim()||"UNKNOWN";
  const map26 = {}, mapBk = {};
  data26AS.forEach(r => {
    const k = normTAN(r.tan)+"||"+normSec(r.section); if(!normTAN(r.tan)) return;
    if(!map26[k]) map26[k] = { tan:normTAN(r.tan), section:normSec(r.section), name:r.deductorName||"", tds:0, txns:0 };
    map26[k].tds += r.tdsDeducted||0; map26[k].txns++;
    if(!map26[k].name && r.deductorName) map26[k].name = r.deductorName;
  });
  const noTanSec = {};
  dataBooks.forEach(r => {
    const k = normTAN(r.tan)+"||"+normSec(r.section);
    if(!normTAN(r.tan)) {
      // Group by party name + section for section view
      const gk = (r.deductorName||"Unknown Party").trim()+"||"+normSec(r.section);
      if(!noTanSec[gk]) noTanSec[gk] = { name:(r.deductorName||"Unknown Party").trim(), section:normSec(r.section), tds:0, txns:0 };
      noTanSec[gk].tds += r.tdsDeducted||0; noTanSec[gk].txns++;
      return;
    }
    if(!mapBk[k]) mapBk[k] = { tan:normTAN(r.tan), section:normSec(r.section), name:r.deductorName||"", tds:0, txns:0 };
    mapBk[k].tds += r.tdsDeducted||0; mapBk[k].txns++;
    if(!mapBk[k].name && r.deductorName) mapBk[k].name = r.deductorName;
  });
  const allKeys = new Set([...Object.keys(map26), ...Object.keys(mapBk)]);
  const results = []; let id = 1;
  allKeys.forEach(k => {
    const a = map26[k], b = mapBk[k];
    const tan = a?.tan||b?.tan, section = a?.section||b?.section, name = a?.name||b?.name||"";
    if(a && b) {
      const diff = a.tds - b.tds;
      const isMatch = Math.abs(diff)<1, isNear = !isMatch && Math.abs(diff)<100;
      const matchStatus = isMatch?"Matched":isNear?"Near Match":diff>0?"Missing in Books":"Mismatch";
      const reason = isMatch?"":diff>0?`26AS ₹${Math.abs(diff).toLocaleString("en-IN",{maximumFractionDigits:2})} higher`:`Books ₹${Math.abs(diff).toLocaleString("en-IN",{maximumFractionDigits:2})} higher`;
      results.push({ id:id++, tan, section, name, as_tds:a.tds, as_txns:a.txns, bk_tds:b.tds, bk_txns:b.txns, tds_diff:diff, matchStatus, reason });
    } else if(a) {
      results.push({ id:id++, tan, section, name, as_tds:a.tds, as_txns:a.txns, bk_tds:0, bk_txns:0, tds_diff:a.tds, matchStatus:"Missing in Books", reason:"Section not in Books" });
    } else {
      results.push({ id:id++, tan, section, name, as_tds:0, as_txns:0, bk_tds:b.tds, bk_txns:b.txns, tds_diff:-b.tds, matchStatus:"Missing in 26AS", reason:"Section not in 26AS" });
    }
  });
  // Append no-TAN Books entries in section view too
  Object.values(noTanSec).forEach(g => {
    results.push({ id:id++, tan:"—", section:g.section, name:g.name, as_tds:0, as_txns:0, bk_tds:g.tds, bk_txns:g.txns, tds_diff:-g.tds, matchStatus:"Missing TAN", reason:`TAN not assigned — assign TAN to reconcile (${g.txns} txn${g.txns!==1?"s":""})` });
  });
  const order = {"Missing TAN":-1,"Mismatch":0,"Missing in Books":1,"Near Match":2,"Missing in 26AS":3,"Matched":4};
  return results.sort((a,b)=>a.tan.localeCompare(b.tan)||(order[a.matchStatus]??5)-(order[b.matchStatus]??5));
}

// ── ICONS ─────────────────────────────────────────────────────────────────────
const Ic = ({ d, s=16, c="currentColor", sw=1.8 }) => <svg width={s} height={s} viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth={sw} strokeLinecap="round" strokeLinejoin="round"><path d={d}/></svg>;
const I = { home:"M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2V9z", import:"M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3", grid:"M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z", recon:"M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18m0 0h10a2 2 0 002-2V9M9 21H5a2 2 0 01-2-2V9m0 0h18", chart:"M18 20V10M12 20V4M6 20v-6", settings:"M12 15a3 3 0 100-6 3 3 0 000 6zM19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z", play:"M5 3l14 9-14 9V3z", download:"M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3", check:"M20 6L9 17l-5-5", warn:"M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0zM12 9v4M12 17h.01", close:"M18 6L6 18M6 6l12 12", file:"M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8l-6-6zM14 2v6h6", trash:"M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6", refresh:"M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15", search:"M21 21l-4.35-4.35M17 11A6 6 0 105 11a6 6 0 0012 0z", filter:"M22 3H2l8 9.46V19l4 2v-8.54L22 3", excel:"M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8l-6-6zM8 13l2.5 4 1.5-2.5 1.5 2.5L16 13", report:"M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8l-6-6zM14 2v6h6M16 13H8M16 17H8M10 9H8", save:"M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2zM17 21v-8H7v8M7 3v5h8", mail:"M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2zM22 6l-10 7L2 6", tracker:"M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" };

const css = `
  @import url('https://fonts.googleapis.com/css2?family=Segoe+UI:wght@300;400;500;600;700&display=swap');
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  :root{--a:#0078d4;--a-dk:#005a9e;--a-lt:#e6f3fb;--sur:#f3f3f3;--wh:#ffffff;--bd:#d1d1d1;--tx:#201f1e;--tx2:#605e5c;--tx3:#a19f9d;--sb:#2b2b2b;--sbh:#383838;--red:#d13438;--grn:#107c10;--amb:#d59300;--pur:#5c2d91;--ora:#c7792a;--rh:#f0f6fc;--rs:#cce4f7;--hb:#f9f9f9;}
  @keyframes modalIn{from{opacity:0;transform:translateY(12px) scale(0.97)}to{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes spin{to{transform:rotate(360deg)}}
  body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--sur);overflow:hidden}
  .app{display:flex;height:100vh;overflow:hidden}
  .sb{width:200px;background:var(--sb);display:flex;flex-direction:column;flex-shrink:0;user-select:none}
  .sb-logo{padding:13px 15px 11px;border-bottom:1px solid #3a3a3a;display:flex;align-items:center;gap:9px}
  .sb-logo-ic{width:31px;height:31px;background:linear-gradient(135deg,#0078d4,#40a0ff);border-radius:5px;display:flex;align-items:center;justify-content:center;flex-shrink:0}
  .sb-logo-t{color:#fff;font-size:12.5px;font-weight:600} .sb-logo-s{color:#666;font-size:10px;margin-top:1px}
  .sb-nav{flex:1;padding:5px 0;overflow-y:auto}
  .sb-sec{padding:9px 13px 3px;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:#555}
  .sb-it{display:flex;align-items:center;gap:9px;padding:7.5px 13px;color:#aaa;cursor:pointer;font-size:12.5px;transition:all 0.1s}
  .sb-it:hover{background:var(--sbh);color:#fff} .sb-it.on{background:var(--a);color:#fff} .sb-it.dis{opacity:0.35;cursor:not-allowed}
  .sb-bdg{margin-left:auto;background:rgba(255,255,255,0.13);border-radius:10px;padding:1px 7px;font-size:10.5px;font-weight:600}
  .sb-it.on .sb-bdg{background:rgba(255,255,255,0.22)} .sb-soon{margin-left:auto;font-size:9px;color:#555;background:#3a3a3a;padding:1px 5px;border-radius:3px}
  .sb-ft{padding:9px 13px;border-top:1px solid #3a3a3a} .sb-ft-t{font-size:10px;color:#555}
  .main{flex:1;display:flex;flex-direction:column;overflow:hidden}
  .tbar{background:var(--a);height:37px;display:flex;align-items:center;padding:0 15px;gap:9px;flex-shrink:0}
  .tbar-t{color:rgba(255,255,255,0.9);font-size:12.5px} .tbar-s{color:rgba(255,255,255,0.35);font-size:11px}
  .cbar{background:var(--wh);border-bottom:1px solid var(--bd);padding:4px 15px;display:flex;align-items:center;gap:1px;flex-shrink:0}
  .cg{display:flex;align-items:center;gap:1px;padding-right:9px;margin-right:6px;border-right:1px solid var(--bd)} .cg:last-child{border-right:none}
  .cb2{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;padding:4px 8px;border:none;background:none;cursor:pointer;border-radius:3px;color:var(--tx);font-size:10.5px;font-family:inherit;min-width:48px;transition:background 0.1s;user-select:none}
  .cb2:hover:not(:disabled){background:var(--sur)} .cb2:active:not(:disabled){background:#e5e5e5}
  .cb2.bl{color:var(--a)} .cb2.gn{color:var(--grn)} .cb2.rd{color:var(--red)} .cb2:disabled{opacity:0.35;cursor:default}
  .cbar-r{margin-left:auto;font-size:11.5px;color:var(--tx2)}
  .content{flex:1;overflow:auto;display:flex;flex-direction:column}
  .home{padding:26px 34px;overflow-y:auto}
  .hero{background:linear-gradient(135deg,#0078d4,#005a9e);border-radius:7px;padding:32px 40px;color:#fff;margin-bottom:24px;display:flex;justify-content:space-between;align-items:center}
  .hero h1{font-size:24px;font-weight:300;margin-bottom:5px} .hero p{font-size:12.5px;opacity:0.85;line-height:1.6;max-width:460px}
  .hb-p{background:#fff;color:var(--a);border:none;padding:7px 20px;border-radius:3px;font-size:12.5px;font-weight:600;cursor:pointer;font-family:inherit}
  .hb-s{background:rgba(255,255,255,0.12);color:#fff;border:1px solid rgba(255,255,255,0.3);padding:7px 20px;border-radius:3px;font-size:12.5px;cursor:pointer;font-family:inherit;margin-left:8px}
  .sg{display:grid;grid-template-columns:repeat(6,1fr);gap:11px;margin-bottom:22px}
  .sc{background:var(--wh);border:1px solid var(--bd);border-radius:5px;padding:15px 16px}
  .sl{font-size:10.5px;color:var(--tx2);margin-bottom:3px;font-weight:500;text-transform:uppercase;letter-spacing:0.4px}
  .sv{font-size:24px;font-weight:300;color:var(--tx)} .sv.bl{color:var(--a)} .sv.gn{color:var(--grn)} .sv.am{color:var(--amb)} .sv.rd{color:var(--red)}
  .qg{display:grid;grid-template-columns:repeat(4,1fr);gap:11px}
  .qc{background:var(--wh);border:1px solid var(--bd);border-radius:5px;padding:16px;cursor:pointer;transition:all 0.12s}
  .qc:hover{border-color:var(--a);box-shadow:0 2px 8px rgba(0,120,212,0.1)}
  .qi{width:32px;height:32px;border-radius:4px;display:flex;align-items:center;justify-content:center;margin-bottom:9px}
  .qc h3{font-size:12px;font-weight:600;color:var(--tx);margin-bottom:2px} .qc p{font-size:11px;color:var(--tx2);line-height:1.4}
  .imp{padding:22px 26px;overflow-y:auto} .ih{font-size:15px;font-weight:600;color:var(--tx);margin-bottom:3px} .is{font-size:12.5px;color:var(--tx2);margin-bottom:16px}
  .drop{border:2px dashed var(--bd);border-radius:7px;background:var(--wh);padding:48px 36px;text-align:center;cursor:pointer;transition:all 0.18s}
  .drop.ov{border-color:var(--a);background:var(--a-lt)} .drop:hover{border-color:#999}
  .di{width:56px;height:56px;background:var(--a-lt);border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 12px}
  .drop h2{font-size:16px;font-weight:400;margin-bottom:5px} .drop p{font-size:12.5px;color:var(--tx2);margin-bottom:18px}
  .ib{background:var(--a);color:#fff;border:none;padding:8px 26px;border-radius:3px;font-size:12.5px;font-weight:500;cursor:pointer;font-family:inherit}
  .ib:hover{background:var(--a-dk)}
  .pw{background:var(--wh);border:1px solid var(--bd);border-radius:5px;padding:12px 16px;margin-top:12px}
  .ph{display:flex;justify-content:space-between;font-size:11.5px;margin-bottom:5px}
  .pt{height:3px;background:#e5e5e5;border-radius:2px} .pf{height:100%;background:var(--a);border-radius:2px;transition:width 0.25s}
  .lw{margin-top:12px} .ll{font-size:11px;font-weight:600;color:var(--tx2);margin-bottom:4px}
  .lg{background:#1e1e1e;border-radius:4px;padding:9px 12px;font-family:Consolas,monospace;font-size:11px;max-height:140px;overflow-y:auto}
  .li{color:#9cdcfe} .ls{color:#4ec9b0} .lw2{color:#dcdcaa} .le{color:#f48771}
  .fg{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:20px}
  .fc{background:var(--wh);border:1px solid var(--bd);border-radius:5px;padding:14px 16px}
  .fh{display:flex;align-items:center;gap:8px;margin-bottom:9px} .fe{width:28px;height:28px;border-radius:3px;display:flex;align-items:center;justify-content:center}
  .fc h3{font-size:12px;font-weight:600} .fc ul{list-style:none} .fc li{font-size:11px;color:var(--tx2);padding:2px 0;display:flex;align-items:flex-start;gap:5px} .fc li::before{content:"·";color:var(--a);flex-shrink:0}
  .fl{margin-top:18px} .fl h3{font-size:12.5px;font-weight:600;margin-bottom:9px}
  .fr{display:flex;align-items:center;gap:9px;background:var(--wh);border:1px solid var(--bd);border-radius:4px;padding:8px 11px;margin-bottom:5px}
  .fx{width:32px;height:32px;border-radius:3px;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:9px;font-weight:700;color:#fff}
  .fx-txt{background:#107c10} .fx-xml{background:#107c10} .fx-csv{background:var(--a)} .fx-xlsx{background:#217346}
  .fn{font-size:12px;font-weight:500} .fm{font-size:10.5px;color:var(--tx2);margin-top:1px}
  .fb2{padding:2px 8px;border-radius:9px;font-size:10.5px;font-weight:600}
  .fb-26as{background:#e6f3fb;color:var(--a)} .fb-ais{background:#e8f8e8;color:var(--grn)} .fb-books{background:#fff4e0;color:#a80000}
  .fvb{background:none;border:1px solid var(--bd);border-radius:3px;padding:3px 10px;cursor:pointer;font-size:11px;color:var(--a);font-family:inherit}
  .fdb{background:none;border:none;cursor:pointer;padding:4px;border-radius:3px;color:var(--tx3)} .fdb:hover{color:var(--red);background:#fde7e9}
  .dv{flex:1;display:flex;flex-direction:column;overflow:hidden}
  .dvtb{background:var(--wh);border-bottom:1px solid var(--bd);padding:6px 13px;display:flex;align-items:center;gap:7px;flex-shrink:0}
  .dstabs{display:flex}
  .dst{padding:4px 13px;font-size:11.5px;font-weight:500;border:1px solid var(--bd);background:var(--sur);cursor:pointer;color:var(--tx2);margin-right:-1px;font-family:inherit}
  .dst:first-child{border-radius:3px 0 0 3px} .dst:last-child{border-radius:0 3px 3px 0}
  .dst.on{background:var(--a);color:#fff;border-color:var(--a);z-index:1}
  .srch{display:flex;align-items:center;gap:5px;background:var(--sur);border:1px solid var(--bd);border-radius:3px;padding:4px 8px}
  .srch input{border:none;background:none;outline:none;font-size:11.5px;width:150px;font-family:inherit;color:var(--tx)}
  .rc{font-size:11.5px;color:var(--tx2);margin-left:auto}
  .gw{flex:1;overflow:auto}
  .dg{width:100%;border-collapse:collapse;font-size:11.5px}
  .dg thead{position:sticky;top:0;z-index:10}
  .dg th{background:var(--hb);border-bottom:2px solid var(--bd);border-right:1px solid var(--bd);padding:6px 8px;text-align:left;font-weight:600;color:var(--tx2);font-size:10.5px;text-transform:uppercase;letter-spacing:0.4px;white-space:nowrap;cursor:pointer;user-select:none}
  .dg th:hover{background:#efefef;color:var(--tx)} .dg th.srt{color:var(--a)}
  .dg td{padding:5.5px 8px;border-bottom:1px solid #f0f0f0;border-right:1px solid #f6f6f6;color:var(--tx);white-space:nowrap;max-width:190px;overflow:hidden;text-overflow:ellipsis}
  .dg tr:hover td{background:var(--rh)} .dg tr.sel td{background:var(--rs)!important;color:#003b73}
  .num{text-align:right;font-variant-numeric:tabular-nums;font-family:Consolas,monospace;font-size:11px}
  .tg{display:inline-block;padding:1px 7px;border-radius:9px;font-size:10px;font-weight:600}
  .tg-um{background:#fff4e0;color:#a80000} .tg-m{background:#e8f8e8;color:var(--grn)} .tg-nm{background:#e6f3fb;color:var(--a)}
  .tg-mm{background:#fde7e9;color:var(--red)} .tg-mib{background:#f0e8ff;color:var(--pur)} .tg-mia{background:#fff0e0;color:var(--ora)}
  .tg-mt{background:#fff0f0;color:#a80000;border:1px solid #ffc0c0} .row-mt td{background:#fff7f7}
  .row-resolved td{background:#d4f5dc!important} .row-resolved td:first-child{border-left:3px solid #28a745}
  .tg-sec{background:#f0e8ff;color:var(--pur)} .tg-q{background:#e8f8e8;color:var(--grn)} .tg-src{background:var(--sur);color:var(--tx2)}
  .cb3{width:13px;height:13px;cursor:pointer;accent-color:var(--a)}
  .smb{background:var(--wh);border-top:2px solid var(--bd);padding:6px 13px;display:flex;gap:24px;flex-shrink:0}
  .si{font-size:11.5px;color:var(--tx2)} .sv2{font-weight:600;color:var(--tx);font-family:Consolas,monospace}
  .emp{display:flex;flex-direction:column;align-items:center;justify-content:center;flex:1;padding:56px;color:var(--tx2);gap:9px;text-align:center}
  .emp p{font-size:13px} .emp .sub{font-size:12px}
  .rv{flex:1;display:flex;flex-direction:column;overflow:hidden}
  .rv-top{background:var(--wh);border-bottom:1px solid var(--bd);padding:10px 16px;display:flex;align-items:center;gap:12px;flex-shrink:0}
  .rv-title{font-size:14px;font-weight:600;color:var(--tx)} .rv-sub{font-size:12px;color:var(--tx2)}
  .run-btn{background:var(--grn);color:#fff;border:none;padding:7px 20px;border-radius:3px;font-size:12.5px;font-weight:600;cursor:pointer;font-family:inherit;display:flex;align-items:center;gap:6px}
  .run-btn:hover{background:#0a6a0a} .run-btn:disabled{background:#ccc;cursor:default}
  .rs-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;padding:12px 16px;flex-shrink:0;background:var(--sur);border-bottom:1px solid var(--bd)}
  .rs-card{background:var(--wh);border-radius:5px;padding:11px 13px;border-left:3px solid var(--bd)}
  .rs-card.grn{border-left-color:var(--grn)} .rs-card.red{border-left-color:var(--red)} .rs-card.amb{border-left-color:var(--amb)} .rs-card.blu{border-left-color:var(--a)} .rs-card.pur{border-left-color:var(--pur)}
  .rs-lbl{font-size:10.5px;color:var(--tx2);text-transform:uppercase;letter-spacing:0.4px;font-weight:500;margin-bottom:3px}
  .rs-val{font-size:22px;font-weight:300;color:var(--tx)} .rs-val.grn{color:var(--grn)} .rs-val.red{color:var(--red)} .rs-val.blu{color:var(--a)}
  .qtab{padding:3px 11px;font-size:11.5px;border:1px solid var(--bd);background:var(--sur);cursor:pointer;color:var(--tx2);border-radius:3px;font-family:inherit;transition:all 0.1s}
  .qtab.on{background:var(--a);color:#fff;border-color:var(--a)}
  .mm-only{display:flex;align-items:center;gap:5px;margin-left:auto;font-size:12px;color:var(--tx2);cursor:pointer;user-select:none}
  .rg-wrap{flex:1;overflow:auto}
  .rg{width:100%;border-collapse:collapse;font-size:11.5px}
  .rg thead{position:sticky;top:0;z-index:10}
  .rg th{background:#f0f4f8;border-bottom:2px solid var(--bd);border-right:1px solid var(--bd);padding:6px 8px;font-size:10.5px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:0.3px;white-space:nowrap}
  .rg th.ah{background:#e6f3fb;color:var(--a-dk)} .rg th.bh{background:#e8f8e8;color:#0a6a0a} .rg th.dh{background:#fff4e0;color:#7a4500}
  .rg td{padding:5.5px 8px;border-bottom:1px solid #f0f0f0;border-right:1px solid #f5f5f5;color:var(--tx);white-space:nowrap;max-width:180px;overflow:hidden;text-overflow:ellipsis}
  .rg tr:hover td{background:var(--rh)}
  .rg tr.row-m td{background:#f0fdf0} .rg tr.row-mm td{background:#fff8f8} .rg tr.row-nm td{background:#f0f8ff} .rg tr.row-mib td{background:#fdf8ff} .rg tr.row-mia td{background:#fff8f0}
  .divh{background:var(--bd);width:2px;min-width:2px;padding:0!important}
  .rep{flex:1;padding:20px 24px;overflow-y:auto}
  .rep-sec{background:var(--wh);border:1px solid var(--bd);border-radius:5px;margin-bottom:14px;overflow:hidden}
  .rep-sh{padding:9px 15px;background:var(--hb);border-bottom:1px solid var(--bd);font-size:12.5px;font-weight:600;display:flex;align-items:center;gap:7px}
  .rep-t{width:100%;border-collapse:collapse;font-size:11.5px}
  .rep-t th{padding:6px 11px;background:var(--hb);border-bottom:1px solid var(--bd);text-align:left;font-weight:600;font-size:10.5px;color:var(--tx2);text-transform:uppercase}
  .rep-t td{padding:5.5px 11px;border-bottom:1px solid #f5f5f5;color:var(--tx)}
  .rep-t tr:hover td{background:var(--rh)}
  .stb{background:var(--a);height:21px;display:flex;align-items:center;padding:0 13px;gap:18px;flex-shrink:0}
  .sti{font-size:11px;color:rgba(255,255,255,0.88);display:flex;align-items:center;gap:4px}
  .toast{position:fixed;bottom:26px;right:26px;padding:10px 16px;border-radius:4px;font-size:12.5px;font-weight:500;display:flex;align-items:center;gap:8px;box-shadow:0 4px 20px rgba(0,0,0,0.14);z-index:9999;animation:tIn 0.17s ease;max-width:360px}
  .ts{background:#dff6dd;color:var(--grn);border-left:4px solid var(--grn)} .te{background:#fde7e9;color:#a4262c;border-left:4px solid var(--red)} .tw{background:#fff4ce;color:#835b00;border-left:4px solid var(--amb)} .ti{background:var(--a-lt);color:var(--a-dk);border-left:4px solid var(--a)}
  @keyframes tIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
  ::-webkit-scrollbar{width:7px;height:7px} ::-webkit-scrollbar-track{background:#f1f1f1} ::-webkit-scrollbar-thumb{background:#c5c5c5;border-radius:4px} ::-webkit-scrollbar-thumb:hover{background:#999}
  .sec-t{font-size:13px;font-weight:600;color:var(--tx);margin-bottom:11px}
  .dp{color:var(--red);font-weight:600;font-family:Consolas,monospace;font-size:11px} .dn{color:var(--grn);font-weight:600;font-family:Consolas,monospace;font-size:11px} .dz{color:var(--tx3);font-family:Consolas,monospace;font-size:11px}
  .modal-bg{position:fixed;inset:0;background:rgba(0,0,0,0.45);z-index:1000;display:flex;align-items:center;justify-content:center}
  .modal{background:var(--wh);border-radius:7px;box-shadow:0 8px 40px rgba(0,0,0,0.22);width:92vw;max-width:1280px;height:86vh;display:flex;flex-direction:column;overflow:hidden}
  .modal-hd{padding:13px 18px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;flex-shrink:0;background:var(--hb)}
  .modal-title{font-size:14px;font-weight:600;color:var(--tx)} .modal-sub{font-size:11.5px;color:var(--tx2)}
  .modal-cls{margin-left:auto;background:none;border:none;cursor:pointer;padding:4px;border-radius:3px;color:var(--tx2)} .modal-cls:hover{background:var(--sur);color:var(--red)}
  .modal-body{flex:1;display:grid;grid-template-columns:1fr 2px 1.5fr;overflow:hidden}
  .modal-pane{display:flex;flex-direction:column;overflow:hidden}
  .modal-ph{padding:8px 13px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.6px;border-bottom:1px solid var(--bd);display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
  .modal-ph.as{background:#e6f3fb;color:var(--a-dk)} .modal-ph.bk{background:#e8f8e8;color:#0a6a0a}
  .modal-scroll{flex:1;overflow-y:auto}
  .modal-t{width:100%;border-collapse:collapse;font-size:11.5px}
  .modal-t th{position:sticky;top:0;background:var(--hb);padding:5px 9px;border-bottom:1px solid var(--bd);text-align:left;font-size:10.5px;font-weight:600;color:var(--tx2);text-transform:uppercase;white-space:nowrap}
  .modal-t td{padding:5px 9px;border-bottom:1px solid #f0f0f0;color:var(--tx);white-space:nowrap}
  .modal-t tr:hover td{background:var(--rh)}
  .modal-ft{padding:8px 13px;border-top:2px solid var(--bd);font-size:11.5px;display:flex;justify-content:space-between;align-items:center;flex-shrink:0;background:var(--sur)}
  .modal-divider{background:var(--bd);width:2px}
  .modal-empty{display:flex;align-items:center;justify-content:center;flex:1;color:var(--tx3);font-size:12.5px;flex-direction:column;gap:6px}
  .rf-bar{background:var(--wh);border-bottom:1px solid var(--bd);padding:7px 13px;display:flex;align-items:center;gap:6px;flex-shrink:0;flex-wrap:wrap}
  .rf-lbl{font-size:11px;font-weight:600;color:var(--tx2);margin-right:2px}
  .rf-sel{padding:3px 8px;font-size:11.5px;border:1px solid var(--bd);border-radius:3px;background:var(--sur);color:var(--tx);font-family:inherit;cursor:pointer}
  .rf-date{padding:3px 7px;font-size:11.5px;border:1px solid var(--bd);border-radius:3px;background:var(--sur);color:var(--tx);font-family:inherit}
  .rf-clr{font-size:10.5px;color:var(--a);background:none;border:none;cursor:pointer;font-family:inherit;padding:2px 5px;border-radius:3px} .rf-clr:hover{background:var(--a-lt)}
  .rg-row-click{cursor:pointer} .rg-row-click:hover td{background:#e6f3fb!important}
  .sv-banner{display:flex;align-items:center;gap:7px;padding:5px 13px;background:#dff6dd;border-bottom:1px solid #c3e6c3;font-size:11.5px;color:#107c10;flex-shrink:0}
  .sv-banner.saving{background:#e6f3fb;border-bottom-color:#b3d4f0;color:var(--a)}
  .sv-dot{width:7px;height:7px;border-radius:50%;background:currentColor;flex-shrink:0}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}
  @keyframes spin{to{transform:rotate(360deg)}}
  .sv-dot.saving{animation:pulse 1s infinite}
`;

// ── TAN DETAIL MODAL ──────────────────────────────────────────────────────────
function TanDetailModal({ tan, tanRow, txns26AS, txnsBooks, txnsInvoices, onClose, fmt, FmtDiff, odooUrl, odooConfig, tanMaster, odooRefs, setOdooRefs, setOdooLog }) {
  const as = txns26AS.filter(r => r.tan?.toUpperCase().trim() === tan);

  // Expand Books rows — Odoo may store combined invoice refs joined by '.'
  // e.g. "SHR/25-26/0500.SHR/25-26/0424" -> 2 separate rows with TDS split equally
  const bkRaw = txnsBooks.filter(r => r.tan?.toUpperCase().trim() === tan);
  const bk = bkRaw.flatMap(r => {
    const inv = (r.invoiceNo || '').trim();
    if (!inv || r.source !== 'Odoo ERP') return [r];
    const parts = inv.split('.').map(s => s.trim()).filter(Boolean);
    if (parts.length <= 1) return [r];
    const tdsEach = (r.tdsDeducted || 0) / parts.length;
    return parts.map((p, i) => ({ ...r, id: r.id + '_' + i, invoiceNo: p, tdsDeducted: tdsEach }));
  });

  const asTDS = as.reduce((s,r)=>s+(r.tdsDeducted||0),0);
  const bkTDS = bk.reduce((s,r)=>s+(r.tdsDeducted||0),0);
  const diff = asTDS - bkTDS;

  // ── Matching state ──────────────────────────────────────────────────────────
  // groups: [{ groupNo, asIds:Set, bkIds:Set }]
  const [groups, setGroups]   = useState([]);
  const [selBk,  setSelBk]    = useState(new Set()); // Books ids pending selection
  const [selAs,  setSelAs]    = useState(new Set()); // 26AS ids pending selection
  const [invoiceLinks, setInvoiceLinks] = useState({}); // { [asRowId]: "INV1,INV2,..." } supports multiple
  const [showTdsBookingModal, setShowTdsBookingModal] = useState(null);

  const PAIR_COLORS = ['#e8f8e8','#e6f3fb','#fff4e0','#f0e8ff','#fff0e0','#fdf0f8'];
  const PAIR_BORDER = ['#107c10','#0078d4','#d59300','#5c2d91','#c7792a','#c71585'];

  const getGroupForAs = id => groups.find(g => g.asIds.has(id));
  const getGroupForBk = id => groups.find(g => g.bkIds.has(id));

  // Click a Books row — toggle pending selection (skip if already in a group)
  const handleBkClick = id => {
    if (getGroupForBk(id)) { setGroups(g => g.filter(x => !x.bkIds.has(id) && !x.asIds.size)); removeGroup(getGroupForBk(id)); return; }
    setSelBk(prev => { const n = new Set(prev); n.has(id) ? n.delete(id) : n.add(id); return n; });
  };

  // Click a 26AS row — toggle pending selection (skip if already in a group)
  const handleAsClick = id => {
    if (getGroupForAs(id)) { removeGroup(getGroupForAs(id)); return; }
    setSelAs(prev => { const n = new Set(prev); n.has(id) ? n.delete(id) : n.add(id); return n; });
  };

  const removeGroup = grp => {
    if (!grp) return;
    setGroups(g => g.filter(x => x !== grp));
  };

  // Confirm pairing — create a new group from current selections
  const confirmMatch = () => {
    if (!selBk.size || !selAs.size) return;
    const groupNo = (groups.length > 0 ? Math.max(...groups.map(g=>g.groupNo)) : 0) + 1;
    setGroups(g => [...g, { groupNo, asIds: new Set(selAs), bkIds: new Set(selBk) }]);
    setSelBk(new Set()); setSelAs(new Set());
  };

  // Auto-match: same TDS (within ₹1), priority: same month → next month → any date
  const autoMatch = () => {
    const newGroups = [];
    const usedBk = new Set(); const usedAs = new Set();
    let groupNo = 1;
    const amtMatch = (a, b) => Math.abs((a.tdsDeducted||0)-(b.tdsDeducted||0)) < 1;
    const getYM = dateStr => { const d = new Date(dateStr||''); return isNaN(d) ? null : { y: d.getFullYear(), m: d.getMonth() }; };
    const isMonthOffset = (bkDate, asDate, offset) => {
      const bk = getYM(bkDate), as = getYM(asDate);
      if (!bk || !as) return false;
      const bkTotal = bk.y * 12 + bk.m + offset;
      return as.y * 12 + as.m === bkTotal;
    };
    bk.forEach(b => {
      if (usedBk.has(b.id)) return;
      const bkDate = b.invoiceDate || b.date || '';
      // Pass 1: same TDS + same month
      let match = as.find(a => !usedAs.has(a.id) && amtMatch(a,b) && isMonthOffset(bkDate, a.date, 0));
      // Pass 2: same TDS + 1 month after
      if (!match) match = as.find(a => !usedAs.has(a.id) && amtMatch(a,b) && isMonthOffset(bkDate, a.date, 1));
      // Pass 3: same TDS + 2 months after
      if (!match) match = as.find(a => !usedAs.has(a.id) && amtMatch(a,b) && isMonthOffset(bkDate, a.date, 2));
      // Pass 4: same TDS any date (fallback)
      if (!match) match = as.find(a => !usedAs.has(a.id) && amtMatch(a,b));
      if (match) { newGroups.push({ groupNo: groupNo++, asIds: new Set([match.id]), bkIds: new Set([b.id]) }); usedBk.add(b.id); usedAs.add(match.id); }
    });
    setGroups(newGroups); setSelBk(new Set()); setSelAs(new Set());
  };

  const clearAll = () => { setGroups([]); setSelBk(new Set()); setSelAs(new Set()); };

  // Open unbooked invoices list in a new browser tab
  const openInvoiceTab = () => {
    if (!txnsInvoices || txnsInvoices.length === 0) { alert('No invoice data. Sync invoices from Odoo first.'); return; }
    const norm = s => (s||'').toLowerCase().replace(/\s+/g,' ').trim();
    const partnerName = (tanRow?.bk_name || tanRow?.as_name || '').trim();
    const partnerInvoices = txnsInvoices.filter(inv => {
      const ip = norm(inv.partnerName), tp = norm(partnerName);
      if (!tp || tp.length < 4) return ip === tp; // require exact match for very short/empty names
      return ip === tp || ip.includes(tp.slice(0, 12)) || tp.includes(ip.slice(0, 12));
    });
    if (!partnerInvoices.length) { alert('No invoices found for this party in synced data.'); return; }
    const bookedInvNos = new Set(bk.map(r=>(r.invoiceNo||'').trim().toUpperCase()).filter(Boolean));
    const unbooked = partnerInvoices.filter(inv => !bookedInvNos.has((inv.invoiceNo||'').trim().toUpperCase()));
    if (!unbooked.length) { alert('All invoices for this party are already booked in Books.'); return; }

    const baseUrl = (odooUrl||'').replace(/\/$/,'');
    const fmtAmt = n => '₹' + Number(n||0).toLocaleString('en-IN',{minimumFractionDigits:2});

    // Build map: invoiceNo → total 26AS amount already linked to it
    const linkedAmtMap = {};
    Object.entries(invoiceLinks).forEach(([asId, invNo]) => {
      if (!invNo) return;
      const key = invNo.trim().toUpperCase();
      const asRow = as.find(r => String(r.id) === String(asId));
      if (asRow) linkedAmtMap[key] = (linkedAmtMap[key]||0) + (asRow.amountPaid||0);
    });

    const totalAmt = unbooked.reduce((s,r)=>s+(r.amountUntaxed||0),0);
    const totalWithTax = unbooked.reduce((s,r)=>s+(r.amountTotal||0),0);
    const totalDue = unbooked.reduce((s,r)=>s+(r.amountDue||0),0);
    // Total remaining = invoice amounts minus what's already linked
    const totalRemaining = unbooked.reduce((s,inv) => {
      const key = (inv.invoiceNo||'').trim().toUpperCase();
      const linked = linkedAmtMap[key]||0;
      return s + Math.max(0, (inv.amountUntaxed||0) - linked);
    }, 0);

    const rows = unbooked.map((inv,i) => {
      const invNo = inv.invoiceNo||'—';
      const key = invNo.trim().toUpperCase();
      const linkedAmt = linkedAmtMap[key]||0;
      const remaining = (inv.amountUntaxed||0) - linkedAmt;
      const isFullyLinked = Math.abs(remaining) < 1;
      const link = baseUrl && inv.odooId
        ? `<a href="${baseUrl}/web#id=${inv.odooId}&model=account.move&view_type=form" target="_blank" style="color:#0078d4;font-weight:700;text-decoration:none;font-family:Consolas,monospace">${invNo} ↗</a>`
        : `<span style="color:#0078d4;font-family:Consolas,monospace;font-weight:700">${invNo}</span>`;
      const remainBadge = linkedAmt > 0
        ? isFullyLinked
          ? `<span style="display:inline-block;background:#e8f8e8;color:#107c10;border:1px solid #107c10;border-radius:10px;padding:1px 8px;font-size:10px;font-weight:700;margin-left:6px">✓ Fully Linked</span>`
          : `<span style="display:inline-block;background:#fff8e1;color:#d59300;border:1px solid #ffd54f;border-radius:10px;padding:1px 8px;font-size:10px;font-weight:700;margin-left:6px">Rem: ${fmtAmt(remaining)}</span>`
        : '';
      return `<tr style="background:${isFullyLinked?'#f0faf0':i%2===0?'#fff':'#f9f9f9'}">
        <td style="padding:8px 12px;color:#666;width:36px">${i+1}</td>
        <td style="padding:8px 12px">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            ${link}
            <button onclick="copyInv('${invNo}',this)" style="border:1px solid #0078d4;background:#e6f3fb;color:#0078d4;border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;flex-shrink:0;font-family:Consolas,monospace">📋 Copy</button>
            ${remainBadge}
          </div>
        </td>
        <td style="padding:8px 12px;color:#333">${inv.invoiceDate||'—'}</td>
        <td style="padding:8px 12px;text-align:right;color:#107c10;font-weight:700;font-family:Consolas,monospace">${fmtAmt(inv.amountUntaxed)}</td>
        <td style="padding:8px 12px;text-align:right;color:#555;font-family:Consolas,monospace">${fmtAmt(inv.amountTotal)}</td>
        <td style="padding:8px 12px;text-align:right;font-family:Consolas,monospace;font-weight:700;color:${(inv.amountDue||0)>0?'#d59300':'#107c10'}">${(inv.amountDue||0)>0?fmtAmt(inv.amountDue):'Paid ✓'}</td>
        <td style="padding:8px 12px;color:#666;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${inv.partnerName||'—'}</td>
      </tr>`;
    }).join('');

    const html = `<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Unbooked Invoices — ${partnerName} (${tan})</title>
<style>
  body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:0;background:#f5f5f5;color:#222}
  .hd{background:#0078d4;color:#fff;padding:18px 28px;display:flex;justify-content:space-between;align-items:center}
  .hd h1{margin:0;font-size:17px;font-weight:700}
  .hd .sub{font-size:12px;opacity:0.85;margin-top:4px}
  .hd .amt{font-size:20px;font-weight:700;font-family:Consolas,monospace;text-align:right}
  .hd .amtsub{font-size:11px;opacity:0.8;text-align:right}
  .wrap{padding:20px 28px}
  .tip{background:#fff8e1;border:1px solid #ffd54f;border-radius:6px;padding:10px 16px;margin-bottom:16px;font-size:12px;color:#555;display:flex;align-items:center;gap:10px}
  table{width:100%;border-collapse:collapse;background:#fff;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px #0001}
  thead tr{background:#e6f3fb}
  th{padding:10px 12px;text-align:left;font-size:12px;font-weight:700;color:#0078d4;border-bottom:2px solid #0078d4}
  th.r{text-align:right}
  td{font-size:13px;border-bottom:1px solid #f0f0f0}
  tfoot tr{background:#e6f3fb;font-weight:700}
  tfoot td{padding:9px 12px;font-size:13px;border-top:2px solid #0078d4}
  .badge{display:inline-block;background:#fde7e9;color:#a80000;border-radius:10px;padding:2px 10px;font-size:11px;font-weight:700;margin-left:10px}
  .copied{background:#e8f8e8!important;border-color:#107c10!important;color:#107c10!important}
</style></head><body>
<div class="hd">
  <div>
    <div class="sub">TAN: ${tan}</div>
    <h1>${partnerName} <span class="badge">${unbooked.length} unbooked</span></h1>
    <div class="sub">Invoices not found in Books TDS entries · Generated ${new Date().toLocaleString('en-IN')}</div>
  </div>
  <div>
    <div class="amt">${fmtAmt(totalAmt)}</div>
    <div class="amtsub">excl. tax · total unbooked</div>
    <div class="amt" style="font-size:16px;margin-top:6px;color:${Math.abs(totalRemaining)<1?'#a8e6cf':'#ffe082'}">${fmtAmt(totalRemaining)}</div>
    <div class="amtsub">${Math.abs(totalRemaining)<1?'✓ fully linked — nothing pending':'remaining to link'}</div>
    <div class="amt" style="font-size:14px;margin-top:8px;color:${totalDue>0?'#ffab40':'#a8e6cf'}">${fmtAmt(totalDue)}</div>
    <div class="amtsub">${totalDue>0?'outstanding / unpaid':'✓ all paid'}</div>
  </div>
</div>
<div class="wrap">
  <div class="tip">💡 <span>Click <b>📋 Copy</b> next to an invoice → Alt+Tab back to the app → paste into the <b>Link invoice…</b> field on the matching 26AS row → Export will include it in the Label column</span></div>
<table>
  <thead><tr>
    <th style="width:36px">#</th><th>Invoice No</th><th>Invoice Date</th>
    <th class="r">Amount (excl. tax)</th><th class="r">Total Amount</th><th class="r">Amount Due</th><th>Party</th>
  </tr></thead>
  <tbody>${rows}</tbody>
  <tfoot><tr>
    <td colspan="3" style="color:#0078d4">Total (${unbooked.length} invoices)</td>
    <td style="text-align:right;color:#a80000;font-family:Consolas,monospace">${fmtAmt(totalAmt)}</td>
    <td style="text-align:right;color:#555;font-family:Consolas,monospace">${fmtAmt(totalWithTax)}</td>
    <td style="text-align:right;color:#d59300;font-family:Consolas,monospace;font-weight:700">${fmtAmt(unbooked.reduce((s,r)=>s+(r.amountDue||0),0))}</td>
    <td></td>
  </tr></tfoot>
</table>
</div>
<script>
function copyInv(invNo, btn) {
  navigator.clipboard.writeText(invNo).then(()=>{
    btn.textContent = '✅ Copied!';
    btn.classList.add('copied');
    setTimeout(()=>{ btn.textContent = '📋 Copy'; btn.classList.remove('copied'); }, 2000);
  }).catch(()=>{
    // Fallback
    const ta = document.createElement('textarea');
    ta.value = invNo; document.body.appendChild(ta); ta.select();
    document.execCommand('copy'); document.body.removeChild(ta);
    btn.textContent = '✅ Copied!'; btn.classList.add('copied');
    setTimeout(()=>{ btn.textContent = '📋 Copy'; btn.classList.remove('copied'); }, 2000);
  });
}
</script>
</body></html>`;

    const w = window.open('', '_blank');
    w.document.write(html);
    w.document.close();
  };

  // Export unmatched 26AS rows — supports multiple invoices per row with proportional TDS split
  const exportUnbooked = () => {
    const matchedAsIds = new Set(groups.flatMap(g=>[...g.asIds]));
    const allUnbooked = as.filter(r => !matchedAsIds.has(r.id));
    if (!allUnbooked.length) { alert('All 26AS entries are matched — nothing to export.'); return; }

    const unbooked = allUnbooked.filter(r => (invoiceLinks[r.id] || r.invoiceNo || '').trim());
    const unlinkedCount = allUnbooked.length - unbooked.length;

    if (!unbooked.length) {
      alert(`No invoice links found.\n\nPlease link invoices to the ${allUnbooked.length} unbooked 26AS row(s) using the "Link invoice…" field before exporting.`);
      return;
    }
    if (unlinkedCount > 0) {
      const ok = window.confirm(`${unlinkedCount} unbooked row(s) have no invoice linked and will be skipped.\n\nExport the ${unbooked.length} linked row(s) only?`);
      if (!ok) return;
    }

    const master = tanMaster || [];
    const partnerIdMap = {};
    master.forEach(r => { if (r.tan && r.odooPartnerId) partnerIdMap[r.tan] = r.odooPartnerId; });
    const partnerId = partnerIdMap[tan] || '';

    const today = (() => {
      const d = new Date();
      return `${String(d.getDate()).padStart(2,'0')}-${String(d.getMonth()+1).padStart(2,'0')}-${d.getFullYear()}`;
    })();

    const headers = ['Date', 'Journal', 'Journal Items/Partner/External ID', 'Journal Items/Account', 'Journal Items/Credit', 'Journal Items/Label', 'Journal Items/Debit'];
    const drRows = [];
    const crRows = [];

    let rowIdx = 0;
    unbooked.forEach((r) => {
      const tdsAmt = r.tdsDeposited || r.tdsDeducted || 0;
      const invoiceNoStr = (invoiceLinks[r.id] || r.invoiceNo || '').trim();
      const invNos = invoiceNoStr.split(',').map(s => s.trim()).filter(Boolean);
      
      if (invNos.length <= 1) {
        drRows.push([rowIdx === 0 ? today : '', rowIdx === 0 ? 'TDS Receivable' : '', partnerId, '231110 TDS Receivable 25-26', '', invoiceNoStr, tdsAmt]);
        crRows.push(['', '', partnerId, '251000 Debtors', tdsAmt, invoiceNoStr, '']);
        rowIdx++;
      } else {
        // Multiple invoices — split TDS proportionally by invoice amount
        const invAmts = invNos.map(invNo => {
          const inv = txnsInvoices?.find(i => (i.invoiceNo||'').trim().toUpperCase() === invNo.toUpperCase());
          return { invNo, amt: inv?.amountUntaxed || 0 };
        });
        const totalAmt = invAmts.reduce((s, i) => s + i.amt, 0);
        invAmts.forEach((inv) => {
          const proportion = totalAmt > 0 ? inv.amt / totalAmt : 1 / invNos.length;
          const splitTds = Math.round(tdsAmt * proportion * 100) / 100;
          drRows.push([rowIdx === 0 ? today : '', rowIdx === 0 ? 'TDS Receivable' : '', partnerId, '231110 TDS Receivable 25-26', '', inv.invNo, splitTds]);
          crRows.push(['', '', partnerId, '251000 Debtors', splitTds, inv.invNo, '']);
          rowIdx++;
        });
      }
    });

    const xlsRows = [headers, ...drRows, ...crRows];
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.aoa_to_sheet(xlsRows);
    ws['!cols'] = [{wch:14},{wch:18},{wch:42},{wch:32},{wch:14},{wch:22},{wch:14}];
    XLSX.utils.book_append_sheet(wb, ws, 'Journal Entries');
    const wbOut = XLSX.write(wb, {bookType:'xlsx', type:'base64'});
    const blob = new Blob([Uint8Array.from(atob(wbOut), c=>c.charCodeAt(0))], {type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `Unbooked_${tan}_${new Date().toISOString().slice(0,10)}.xlsx`;
    a.click();
  };

  // Push journal entries directly to Odoo
  const pushToOdoo = async () => {
    const matchedAsIds = new Set(groups.flatMap(g=>[...g.asIds]));
    const allUnbooked = as.filter(r => !matchedAsIds.has(r.id));
    const unbooked = allUnbooked.filter(r => (invoiceLinks[r.id] || r.invoiceNo || '').trim());
    
    if (!unbooked.length) {
      alert('No invoice links found. Please link invoices to the unbooked 26AS rows first.');
      return;
    }

    // Find Odoo credentials from tanMaster or use default
    const master = tanMaster || [];
    const partnerIdMap = {};
    master.forEach(r => { if (r.tan && r.odooPartnerId) partnerIdMap[r.tan] = Number(r.odooPartnerId); });
    const tanMasterPartnerId = partnerIdMap[tan] || null;
    console.log(`[pushToOdoo] TAN Master partnerId for ${tan}:`, tanMasterPartnerId);
    
    // Get deductor name from tanRow (the TAN detail)
    const deductorName = tanRow?.name || tanRow?.deductorName || tanRow?.bk_name || tanRow?.as_name || '';
    
    // Use odooConfig prop or fallback to window.__odooConfig
    const config = odooConfig || window.__odooConfig;
    if (!config || !config.url) {
      alert('Odoo not configured. Please configure Odoo in Client settings first, or sync from Odoo to establish connection.');
      return;
    }

    const today = new Date().toISOString().slice(0, 10);

    // ── Normalize 26AS date (DD-Mon-YYYY) → YYYY-MM-DD for Odoo ──
    // CRITICAL: Odoo requires exactly YYYY-MM-DD. 26AS dates come as DD-Mon-YYYY
    // (e.g. "30-Dec-2025"). Some 26AS files have truncated years (e.g. "30-Dec-202")
    // which the old regex (\d{2,4}) accepted, producing invalid dates like "202-12-30".
    const normalizeDateForOdoo = (dateStr) => {
      if (!dateStr) return today;
      const trimmed = dateStr.trim();
      // Already in YYYY-MM-DD format
      if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
      const MON = { jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12 };
      // DD-Mon-YYYY — require EXACTLY 4-digit year (e.g. 30-Dec-2025)
      const m4 = trimmed.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/);
      if (m4) {
        const month = MON[m4[2].toLowerCase()];
        if (month) return `${parseInt(m4[3])}-${String(month).padStart(2,'0')}-${String(m4[1]).padStart(2,'0')}`;
      }
      // DD-Mon-YY — exactly 2-digit year (e.g. 30-Dec-25 → 2025)
      const m2 = trimmed.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2})$/);
      if (m2) {
        const month = MON[m2[2].toLowerCase()];
        if (month) return `${parseInt(m2[3]) + 2000}-${String(month).padStart(2,'0')}-${String(m2[1]).padStart(2,'0')}`;
      }
      // DD/MM/YYYY or DD-MM-YYYY (numeric month)
      const mNum = trimmed.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
      if (mNum) return `${mNum[3]}-${String(mNum[2]).padStart(2,'0')}-${String(mNum[1]).padStart(2,'0')}`;
      // YYYY/MM/DD
      const mISO = trimmed.match(/^(\d{4})[\/](\d{1,2})[\/](\d{1,2})$/);
      if (mISO) return `${mISO[1]}-${String(mISO[2]).padStart(2,'0')}-${String(mISO[3]).padStart(2,'0')}`;
      // Fallback: JS Date constructor (reject results with year < 1900 or > 2100)
      try {
        const d = new Date(trimmed);
        if (!isNaN(d) && d.getFullYear() >= 1900 && d.getFullYear() <= 2100) return d.toISOString().slice(0, 10);
      } catch(e) {}
      // Final safety net — log warning and use today so Odoo never gets a malformed date
      console.warn('[normalizeDateForOdoo] Could not parse date, falling back to today:', trimmed);
      return today;
    };

    // Build entries array with partner info from invoice data
    const entries = [];
    unbooked.forEach((r) => {
      const tdsAmt = r.tdsDeposited || r.tdsDeducted || 0;
      const invoiceNoStr = (invoiceLinks[r.id] || r.invoiceNo || '').trim();
      const invNos = invoiceNoStr.split(',').map(s => s.trim()).filter(Boolean);
      // Use the 26AS transaction date (when TDS was deposited), normalized to YYYY-MM-DD for Odoo
      const entryDate = normalizeDateForOdoo(r.date);

      if (invNos.length <= 1) {
        // Get partner from invoice if available
        const inv = txnsInvoices?.find(i => (i.invoiceNo||'').trim().toUpperCase() === invoiceNoStr.toUpperCase());
        console.log(`[pushToOdoo] Invoice lookup for ${invoiceNoStr}:`, inv ? { partnerName: inv.partnerName, partnerId: inv.partnerId, odooPartnerId: inv.odooPartnerId } : 'NOT FOUND');
        const partnerFromInv = inv?.partnerName || '';
        const odooPartnerIdFromInv = inv?.odooPartnerId || inv?.partnerId || null;
        // Priority: 1. TAN Master partnerId, 2. Invoice partnerId, 3. name lookup
        const finalPartnerId = tanMasterPartnerId || odooPartnerIdFromInv || null;
        console.log(`[pushToOdoo] Using partner: name="${partnerFromInv || deductorName}", odooPartnerId=${finalPartnerId} (tanMaster: ${tanMasterPartnerId}, inv: ${odooPartnerIdFromInv}), date=${entryDate}`);
        entries.push({ 
          invoiceNo: invoiceNoStr, 
          amount: tdsAmt, 
          date: entryDate, 
          partnerName: partnerFromInv || deductorName,
          odooPartnerId: finalPartnerId,
          tan 
        });
      } else {
        // Multiple invoices — split proportionally
        const invAmts = invNos.map(invNo => {
          const inv = txnsInvoices?.find(i => (i.invoiceNo||'').trim().toUpperCase() === invNo.toUpperCase());
          return { invNo, amt: inv?.amountUntaxed || 0, partnerName: inv?.partnerName || '', odooPartnerId: inv?.odooPartnerId || inv?.partnerId || null };
        });
        const totalAmt = invAmts.reduce((s, i) => s + i.amt, 0);
        invAmts.forEach((inv) => {
          const proportion = totalAmt > 0 ? inv.amt / totalAmt : 1 / invNos.length;
          const splitTds = Math.round(tdsAmt * proportion * 100) / 100;
          // Priority: 1. TAN Master partnerId, 2. Invoice partnerId
          const finalPartnerId = tanMasterPartnerId || inv.odooPartnerId || null;
          entries.push({ 
            invoiceNo: inv.invNo, 
            amount: splitTds, 
            date: entryDate, 
            partnerName: inv.partnerName || deductorName,
            odooPartnerId: finalPartnerId,
            tan 
          });
        });
      }
    });

    if (!window.confirm(`Push ${entries.length} journal entries to Odoo?\n\nDeductor: ${deductorName || tan}\n\nEntries:\n${entries.slice(0,5).map(e => `• ${e.invoiceNo}: ₹${e.amount.toLocaleString('en-IN')}`).join('\n')}${entries.length > 5 ? `\n... and ${entries.length - 5} more` : ''}`)) {
      return;
    }

    // ── SAFETY: Validate ALL entry dates are strict YYYY-MM-DD before sending ──
    // This catches any edge case the normalizer missed (truncated years, weird formats)
    const VALID_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
    entries.forEach(e => {
      if (!e.date || !VALID_DATE_RE.test(e.date) || parseInt(e.date.slice(0,4)) < 1900) {
        console.warn(`[pushToOdoo] Invalid date "${e.date}" for ${e.invoiceNo} — replacing with ${today}`);
        e.date = today;
      }
    });

    try {
      const res = await fetch(`${window.location.origin}/api/odoo/create-journal-entries`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          url: config.url,
          db: config.database,
          username: config.username,
          apiKey: config.password,
          entries,
          tdsAccountCode: '231110',
          debtorAccountCode: '251000'
        })
      });
      const data = await res.json();
      
      if (data.ok) {
        const newRefs = { ...odooRefs };
        const createdList = data.results?.filter(r => r.status === 'created') || [];
        const failedList  = data.results?.filter(r => r.status === 'error')   || [];
        const now = new Date().toISOString();
        createdList.forEach(r => {
          if (r.invoiceNo && (r.odooRef || r.moveId)) {
            newRefs[r.invoiceNo.toUpperCase()] = {
              odooRef: r.odooRef || null, moveId: r.moveId,
              posted: r.posted || false, createdAt: now, tan, company: data.company || ''
            };
          }
        });
        setOdooRefs(newRefs);
        saveToStore('odooRefs', newRefs);
        const entryMap = {};
        entries.forEach(e => { entryMap[(e.invoiceNo||'').toUpperCase()] = e; });
        const logEntry = {
          id: now + '_' + tan, pushDate: now, tan,
          deductorName: deductorName || tan, company: data.company || '',
          totalCreated: data.created || 0, totalFailed: failedList.length,
          entries: [
            ...createdList.map(r => ({
              invoiceNo: r.invoiceNo||'', odooRef: r.odooRef||'', moveId: r.moveId||'',
              amount: entryMap[(r.invoiceNo||'').toUpperCase()]?.amount || 0,
              status: r.posted ? 'Posted' : 'Draft', posted: r.posted||false, error: ''
            })),
            ...failedList.map(r => ({
              invoiceNo: r.invoiceNo||'', odooRef:'', moveId:'',
              amount: entryMap[(r.invoiceNo||'').toUpperCase()]?.amount || 0,
              status:'Failed', posted:false, error: r.error||''
            }))
          ]
        };
        setOdooLog(prev => [logEntry, ...prev.slice(0, 999)]);
        const createdRefs   = createdList.map(r=>`• ${r.invoiceNo} → ${r.odooRef||'ID:'+r.moveId} ${r.posted?'[✓]':'(Draft)'}`).join('\n')||'';
        const failedDetails = failedList.map(r=>`• ${r.invoiceNo}: ${r.error}`).join('\n')||'';
        alert(
          `Success!${data.company?` (${data.company})`:''}

Created: ${data.created} entries
`+
          `${createdRefs?`
Odoo References:
${createdRefs}`:''}`+
          `${failedList.length>0?`

Failed: ${failedList.length}
${failedDetails}`:''}`+
          `

Log saved - check Push Log tab`
        );
      } else {
        alert('Error: ' + data.error);
      }
    } catch (err) {
      alert(`❌ Network error: ${err.message}`);
    }
  };

  const matchedAsIds  = new Set(groups.flatMap(g=>[...g.asIds]));
  const matchedBkIds  = new Set(groups.flatMap(g=>[...g.bkIds]));
  const unbookedCount = as.filter(r => !matchedAsIds.has(r.id)).length;
  const canConfirm    = selBk.size > 0 && selAs.size > 0;

  // TDS sum of pending selections for feedback
  const selBkTDS = [...selBk].reduce((s,id)=>s+((bk.find(r=>r.id===id)||{}).tdsDeducted||0),0);
  const selAsTDS = [...selAs].reduce((s,id)=>s+((as.find(r=>r.id===id)||{}).tdsDeducted||0),0);

  useEffect(() => {
    const fn = e => e.key==="Escape" && onClose();
    window.addEventListener("keydown", fn);
    return () => window.removeEventListener("keydown", fn);
  }, [onClose]);

  // Status hint shown in header
  const hint = selBk.size>0 && selAs.size===0 ? `${selBk.size} Books row${selBk.size>1?'s':''} selected — now select matching 26AS row(s)`
             : selBk.size>0 && selAs.size>0    ? `${selBk.size} Books + ${selAs.size} 26AS selected — click "Confirm Match"`
             : selBk.size===0 && selAs.size>0  ? `${selAs.size} 26AS row${selAs.size>1?'s':''} selected — also select Books row(s)`
             : null;

  return (
    <div className="modal-bg" onClick={e=>e.target===e.currentTarget&&onClose()}>
      <div className="modal">
        <div className="modal-hd">
          <div>
            <div className="modal-title"><span style={{fontFamily:"Consolas,monospace",color:"var(--a)"}}>{tan}</span>&nbsp;·&nbsp;{tanRow?.as_name||tanRow?.bk_name||""}</div>
            <div className="modal-sub">
              {hint
                ? <span style={{color:"var(--a)",fontWeight:600}}>{hint}</span>
                : `Transaction-wise breakdown · ${as.length} in 26AS · ${bk.length} in Books · Select Books rows first, then 26AS rows, then Confirm Match`}
            </div>
          </div>
          <span className={`tg ${tanRow?.matchStatus==="Matched"?"tg-m":tanRow?.matchStatus==="Mismatch"?"tg-mm":tanRow?.matchStatus==="Near Match"?"tg-nm":tanRow?.matchStatus==="Missing in Books"?"tg-mib":tanRow?.matchStatus==="Missing TAN"?"tg-mt":"tg-mia"}`} style={{marginLeft:12}}>{tanRow?.matchStatus}</span>
          {/* Toolbar */}
          <div style={{display:"flex",alignItems:"center",gap:6,marginLeft:12,flexShrink:0}}>
            {canConfirm && (
              <button onClick={confirmMatch} style={{padding:"4px 12px",fontSize:11.5,border:"2px solid #107c10",borderRadius:3,cursor:"pointer",background:"#107c10",color:"#fff",fontFamily:"inherit",fontWeight:700,display:"flex",alignItems:"center",gap:5}}>
                ✓ Confirm Match
                {selBkTDS>0&&selAsTDS>0&&<span style={{opacity:0.8,fontWeight:400,fontSize:10}}>Bk:₹{selBkTDS.toLocaleString("en-IN",{maximumFractionDigits:0})} / AS:₹{selAsTDS.toLocaleString("en-IN",{maximumFractionDigits:0})}</span>}
              </button>
            )}
            {(selBk.size>0||selAs.size>0) && (
              <button onClick={()=>{setSelBk(new Set());setSelAs(new Set());}} style={{padding:"4px 10px",fontSize:11,border:"1px solid var(--bd)",borderRadius:3,cursor:"pointer",background:"var(--sur)",color:"var(--tx2)",fontFamily:"inherit"}}>Deselect All</button>
            )}
            <button onClick={autoMatch} style={{padding:"4px 10px",fontSize:11,border:"1px solid var(--bd)",borderRadius:3,cursor:"pointer",background:"var(--sur)",color:"var(--tx)",fontFamily:"inherit",fontWeight:600}}>⚡ Auto-Match</button>
            {groups.length>0 && <button onClick={clearAll} style={{padding:"4px 10px",fontSize:11,border:"1px solid #fde7e9",borderRadius:3,cursor:"pointer",background:"#fff8f8",color:"var(--red)",fontFamily:"inherit"}}>✕ Clear All</button>}
            {unbookedCount>0 && (() => {
              const linkedCount = as.filter(r => !matchedAsIds.has(r.id) && (invoiceLinks[r.id]||r.invoiceNo||'').trim()).length;
              return <>
                <button onClick={exportUnbooked} style={{padding:"4px 10px",fontSize:11,border:"1px solid #107c10",borderRadius:3,cursor:"pointer",background:"#e8f8e8",color:"#107c10",fontFamily:"inherit",fontWeight:600}}>⬇ Export ({linkedCount}/{unbookedCount})</button>
                <button onClick={pushToOdoo} style={{padding:"4px 10px",fontSize:11,border:"1px solid #5c2d91",borderRadius:3,cursor:"pointer",background:"#f0e8ff",color:"#5c2d91",fontFamily:"inherit",fontWeight:600}}>🚀 Push to Odoo ({linkedCount})</button>
              </>;
            })()}
            {unbookedCount>0 && txnsInvoices && txnsInvoices.length>0 && <button onClick={openInvoiceTab} style={{padding:"4px 10px",fontSize:11,border:"1px solid #0078d4",borderRadius:3,cursor:"pointer",background:"#e6f3fb",color:"#0078d4",fontFamily:"inherit",fontWeight:600}}>↗ View Invoices</button>}
          </div>
          <button className="modal-cls" onClick={onClose}><Ic d={I.close} s={16}/></button>
        </div>

        <div className="modal-body">
          {/* Books Pane — LEFT (select first) */}
          <div className="modal-pane">
            <div className="modal-ph bk" style={{background:"#e8f8e8"}}>
              <span>📗 Books Transactions <span style={{fontWeight:400,fontSize:10,opacity:0.8}}>— select first</span></span>
              <span style={{fontWeight:400,display:"flex",alignItems:"center",gap:8}}>
                {(()=>{const dupSet=new Set();bk.forEach(r=>{const k=(r.invoiceNo||"").trim().toUpperCase();if(k&&bk.filter(x=>(x.invoiceNo||"").trim().toUpperCase()===k).length>1)dupSet.add(k);});return dupSet.size>0?<span style={{fontSize:10,fontWeight:700,padding:"1px 6px",borderRadius:3,background:"#d59300",color:"#fff"}}>⚠ {dupSet.size} dup inv</span>:null;})()}
                {bk.length} entries · TDS: <b style={{color:"#0a6a0a"}}>₹{bkTDS.toLocaleString("en-IN",{minimumFractionDigits:2})}</b>
              </span>
            </div>
            <div className="modal-scroll">
              {bk.length===0
                ? <div className="modal-empty"><Ic d={I.file} s={32} c="#d1d1d1" sw={1}/><span>No Books entries for this TAN</span></div>
                : (
                  <table className="modal-t">
                    <thead><tr><th style={{width:24}}></th><th>#</th><th>Date</th><th>Invoice / Label</th><th>Section</th><th>Qtr</th><th style={{textAlign:"right"}}>TDS</th></tr></thead>
                    <tbody>{bk.map((r,i)=>{
                      const grp = getGroupForBk(r.id);
                      const isPendingSel = selBk.has(r.id);
                      const ci = grp ? (grp.groupNo-1) % PAIR_COLORS.length : -1;
                      // Duplicate detection within this TAN's Books rows
                      const invKey = (r.invoiceNo||"").trim().toUpperCase();
                      const isDupInv = invKey && bk.filter(x=>(x.invoiceNo||"").trim().toUpperCase()===invKey).length > 1;
                      const rowBg     = grp ? PAIR_COLORS[ci] : isPendingSel ? "#fffbe6" : isDupInv ? "#fff8e8" : "transparent";
                      const rowOutline = grp ? `2px solid ${PAIR_BORDER[ci]}` : isPendingSel ? "2px solid #d59300" : isDupInv ? "2px solid #f0c040" : "2px solid transparent";
                      return (
                        <tr key={r.id} onClick={()=>handleBkClick(r.id)}
                          style={{cursor:"pointer",background:rowBg,outline:rowOutline,outlineOffset:"-2px"}}
                          title={grp?"Click to remove from group":isPendingSel?"Click to deselect":isDupInv?"Duplicate invoice number — click to select":"Click to select"}>
                          <td style={{width:24,textAlign:"center"}}>
                            {grp
                              ? <span style={{display:"inline-block",width:18,height:18,borderRadius:9,background:PAIR_BORDER[ci],color:"#fff",fontSize:9,fontWeight:700,lineHeight:"18px",textAlign:"center"}}>✓{grp.groupNo}</span>
                              : isPendingSel
                              ? <span style={{display:"inline-block",width:18,height:18,borderRadius:9,background:"#d59300",color:"#fff",fontSize:12,lineHeight:"18px",textAlign:"center"}}>✔</span>
                              : <span style={{display:"inline-block",width:18,height:18,borderRadius:9,border:"1.5px solid var(--bd)",background:"#fff"}}/>}
                          </td>
                          <td style={{color:"#aaa"}}>{i+1}</td>
                          <td style={{fontFamily:"Consolas,monospace",fontSize:11}} title={r.date?`Txn: ${r.date}`:""}>{r.invoiceDate||r.date||"—"}</td>
                          <td style={{fontSize:11,maxWidth:150,overflow:"hidden",textOverflow:"ellipsis"}} title={r.invoiceNo}>
                            <span style={{display:"flex",alignItems:"center",gap:4}}>
                              <span style={{color:"var(--a)",overflow:"hidden",textOverflow:"ellipsis"}}>{r.invoiceNo||"—"}</span>
                              {isDupInv&&<span style={{flexShrink:0,fontSize:8,fontWeight:700,padding:"1px 4px",borderRadius:2,background:"#d59300",color:"#fff",letterSpacing:0.3}}>DUP</span>}
                            </span>
                          </td>
                          <td>{r.section?<span className="tg tg-sec">{r.section}</span>:"—"}</td>
                          <td>{r.quarter?<span className="tg tg-q">{r.quarter}</span>:"—"}</td>
                          <td style={{textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--grn)",fontWeight:600}}>{fmt(r.tdsDeducted)}</td>
                        </tr>
                      );
                    })}</tbody>
                  </table>
                )}
            </div>
            <div className="modal-ft">
              <span style={{color:"var(--tx2)"}}>Total TDS (Books)</span>
              <div style={{display:"flex",alignItems:"center",gap:10}}>
                {groups.length>0 && <span style={{fontSize:11,color:"var(--grn)",fontWeight:600}}>✓ {[...matchedBkIds].length} matched</span>}
                {selBk.size>0 && <span style={{fontSize:11,color:"#d59300",fontWeight:600}}>{selBk.size} selected</span>}
                <span style={{fontFamily:"Consolas,monospace",fontWeight:700,color:"var(--grn)",fontSize:13}}>₹{bkTDS.toLocaleString("en-IN",{minimumFractionDigits:2})}</span>
              </div>
            </div>
          </div>

          <div className="modal-divider"/>

          {/* 26AS Pane — RIGHT (select second) */}
          <div className="modal-pane">
            <div className="modal-ph as" style={{background:"#e6f3fb"}}>
              <span>📘 26AS Transactions <span style={{fontWeight:400,fontSize:10,opacity:0.8}}>— select second</span></span>
              <span style={{fontWeight:400}}>{as.length} entries · TDS: <b style={{color:"#a80000"}}>₹{asTDS.toLocaleString("en-IN",{minimumFractionDigits:2})}</b></span>
            </div>
            <div className="modal-scroll">
              {as.length===0
                ? <div className="modal-empty"><Ic d={I.file} s={32} c="#d1d1d1" sw={1}/><span>No 26AS entries for this TAN</span></div>
                : (
                  <table className="modal-t">
                    <thead><tr><th style={{width:24}}></th><th>#</th><th>Date</th><th>Section</th><th>Qtr</th><th style={{textAlign:"right"}}>Amount</th><th style={{textAlign:"right"}}>TDS Deducted</th><th style={{textAlign:"right"}}>TDS Deposited</th><th>B.Status</th><th style={{width:160,color:"#c7792a"}}>Invoice No <span style={{fontWeight:400,fontSize:9,opacity:0.8}}>(link)</span></th></tr></thead>
                    <tbody>{as.map((r,i)=>{
                      const grp = getGroupForAs(r.id);
                      const isPendingSel = selAs.has(r.id);
                      const ci = grp ? (grp.groupNo-1) % PAIR_COLORS.length : -1;
                      const rowBg      = grp ? PAIR_COLORS[ci] : isPendingSel ? "#e6f3fb" : selBk.size>0 ? "#fafeff" : "transparent";
                      const rowOutline  = grp ? `2px solid ${PAIR_BORDER[ci]}` : isPendingSel ? "2px solid var(--a)" : "2px solid transparent";
                      return (
                        <tr key={r.id} onClick={()=>handleAsClick(r.id)}
                          style={{cursor: selBk.size>0||grp||isPendingSel ? "pointer" : "default", background:rowBg, outline:rowOutline, outlineOffset:"-2px"}}
                          title={grp?"Click to remove from group":selBk.size>0?"Click to select this 26AS row":isPendingSel?"Click to deselect":""}>
                          <td style={{width:24,textAlign:"center"}}>
                            {grp
                              ? <span style={{display:"inline-block",width:18,height:18,borderRadius:9,background:PAIR_BORDER[ci],color:"#fff",fontSize:9,fontWeight:700,lineHeight:"18px",textAlign:"center"}}>✓{grp.groupNo}</span>
                              : isPendingSel
                              ? <span style={{display:"inline-block",width:18,height:18,borderRadius:9,background:"var(--a)",color:"#fff",fontSize:12,lineHeight:"18px",textAlign:"center"}}>✔</span>
                              : selBk.size>0
                              ? <span style={{display:"inline-block",width:18,height:18,borderRadius:9,border:"1.5px dashed var(--a)",background:"#fff"}}/>
                              : <span style={{display:"inline-block",width:18,height:18,borderRadius:9,border:"1.5px solid var(--bd)",background:"#fff"}}/>}
                          </td>
                          <td style={{color:"#aaa"}}>{i+1}</td>
                          <td style={{fontFamily:"Consolas,monospace",fontSize:11}}>{r.date||"—"}</td>
                          <td>{r.section?<span className="tg tg-sec">{r.section}</span>:"—"}</td>
                          <td>{r.quarter?<span className="tg tg-q">{r.quarter}</span>:"—"}</td>
                          <td style={{textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11}}>{fmt(r.amountPaid)}</td>
                          <td style={{textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11,color:"#a80000",fontWeight:600}}>{fmt(r.tdsDeducted)}</td>
                          <td style={{textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--grn)"}}>{fmt(r.tdsDeposited||r.tdsDeducted)}</td>
                          <td><span style={{fontFamily:"Consolas,monospace",fontSize:10,color:r.bookingStatus==="F"?"var(--grn)":"var(--amb)"}}>{r.bookingStatus||"—"}</span></td>
                          <td onClick={e=>e.stopPropagation()} style={{minWidth:140,maxWidth:200}}>
                            {!grp
                              ? (() => {
                                  const linkedInvNoStr = invoiceLinks[r.id]||"";
                                  const linkedInvNos = linkedInvNoStr.split(',').map(s => s.trim()).filter(Boolean);
                                  const hasMultiple = linkedInvNos.length > 1;
                                  const firstInvNo = linkedInvNos[0] || "";
                                  const invData = firstInvNo && txnsInvoices ? txnsInvoices.find(inv => (inv.invoiceNo||'').trim().toUpperCase() === firstInvNo.toUpperCase()) : null;
                                  const totalInvAmt = linkedInvNos.reduce((sum, invNo) => {
                                    const inv = txnsInvoices?.find(i => (i.invoiceNo||'').trim().toUpperCase() === invNo.toUpperCase());
                                    return sum + (inv?.amountUntaxed || 0);
                                  }, 0);
                                  const totalLinkedAmt = linkedInvNos.length > 0
                                    ? as.filter(a => { const aInvs = (invoiceLinks[a.id]||'').split(',').map(s=>s.trim().toUpperCase()).filter(Boolean); return linkedInvNos.some(inv => aInvs.includes(inv.toUpperCase())); }).reduce((s,a) => s + (a.amountPaid||0), 0)
                                    : (r.amountPaid||0);
                                  const diff = totalInvAmt > 0 ? totalInvAmt - totalLinkedAmt : null;
                                  const getTdsBooked = (invNo) => bk.filter(b => (b.invoiceNo||'').trim().toUpperCase() === invNo.toUpperCase()).reduce((s, b) => s + (b.tdsDeducted || 0), 0);
                                  return (
                                    <div style={{padding:"2px 4px"}}>
                                      <div style={{display:"flex",alignItems:"center",gap:3}}>
                                        <input value={linkedInvNoStr} onChange={e=>setInvoiceLinks(prev=>({...prev,[r.id]:e.target.value}))} placeholder="INV1, INV2…" title="Comma-separated for multiple" style={{flex:1,minWidth:0,border:`1px solid ${linkedInvNoStr?"#c7792a":"#ddd"}`,borderRadius:3,padding:"2px 5px",fontSize:10,fontFamily:"Consolas,monospace",color:"#c7792a",background:linkedInvNoStr?"#fff8f0":"transparent",outline:"none"}} onFocus={e=>{e.target.style.border="1px solid #c7792a";e.target.style.background="#fff8f0";}} onBlur={e=>{e.target.style.border=`1px solid ${linkedInvNoStr?"#c7792a":"#ddd"}`;e.target.style.background=linkedInvNoStr?"#fff8f0":"transparent";}}/>
                                        {linkedInvNoStr && <span title={hasMultiple?`${linkedInvNos.length} invoices linked`:"Linked"} style={{color:"#c7792a",fontSize:11}}>🔗{hasMultiple&&<sup style={{fontSize:8}}>{linkedInvNos.length}</sup>}</span>}
                                      </div>
                                      {totalInvAmt > 0 && (
                                        <div style={{marginTop:2,fontSize:9.5,display:"flex",gap:6,flexWrap:"wrap"}}>
                                          <span style={{color:"#107c10",fontFamily:"Consolas,monospace",fontWeight:600}}>Inv: ₹{totalInvAmt.toLocaleString("en-IN",{maximumFractionDigits:0})}</span>
                                          {(() => {
                                            const totalDueAmt = linkedInvNos.reduce((sum, invNo) => {
                                              const inv = txnsInvoices?.find(i => (i.invoiceNo||'').trim().toUpperCase() === invNo.toUpperCase());
                                              return sum + (inv?.amountDue || 0);
                                            }, 0);
                                            return totalDueAmt > 0 
                                              ? <span style={{color:"#d59300",fontFamily:"Consolas,monospace",fontWeight:600}}>Due: ₹{totalDueAmt.toLocaleString("en-IN",{maximumFractionDigits:0})}</span>
                                              : <span style={{color:"#107c10",fontFamily:"Consolas,monospace",fontWeight:600}}>Paid ✓</span>;
                                          })()}
                                          {hasMultiple && <span style={{color:"#5c2d91",fontSize:9}}>({linkedInvNos.length} inv)</span>}
                                          {diff !== null && <span style={{color: Math.abs(diff)<1 ? "#107c10" : diff>0 ? "#0078d4" : "#a80000", fontFamily:"Consolas,monospace", fontWeight:600}}>{Math.abs(diff)<1 ? "✓" : diff>0 ? `+₹${diff.toLocaleString("en-IN",{maximumFractionDigits:0})}` : `-₹${Math.abs(diff).toLocaleString("en-IN",{maximumFractionDigits:0})}`}</span>}
                                        </div>
                                      )}
                                      {linkedInvNos.length > 0 && linkedInvNos.some(inv => txnsInvoices?.find(i => (i.invoiceNo||'').trim().toUpperCase() === inv.toUpperCase())) && (
                                        <div style={{marginTop:3,display:"flex",flexWrap:"wrap",gap:2}}>
                                          {linkedInvNos.slice(0,2).map(invNo => {
                                            const inv = txnsInvoices?.find(i => (i.invoiceNo||'').trim().toUpperCase() === invNo.toUpperCase());
                                            if (!inv) return null;
                                            const tdsBooked = getTdsBooked(invNo);
                                            const taxableVal = inv.amountUntaxed || 0;
                                            const tdsPercent = taxableVal > 0 ? ((tdsBooked / taxableVal) * 100).toFixed(1) : 0;
                                            const isExcess = tdsBooked > taxableVal * 0.105;
                                            return (
                                              <button key={invNo} onClick={(e) => { e.stopPropagation(); setShowTdsBookingModal({ invoiceNo: invNo, invData: inv, tdsBooked, tdsPercent, linkedRows: bk.filter(b => (b.invoiceNo||'').trim().toUpperCase() === invNo.toUpperCase()), taxableVal }); }} style={{display:"flex",alignItems:"center",gap:3,background:isExcess?"#fff0f0":tdsBooked>0?"#e8f8e8":"#f5f5f5",border:`1px solid ${isExcess?"#f0a0a0":tdsBooked>0?"#90d090":"#ddd"}`,borderRadius:3,padding:"1px 5px",cursor:"pointer",fontSize:9,fontFamily:"inherit",color:isExcess?"#a80000":tdsBooked>0?"#107c10":"#666"}} title={`TDS: ₹${tdsBooked.toLocaleString("en-IN")} (${tdsPercent}%)`}>
                                                <span style={{fontWeight:600}}>{hasMultiple ? invNo.slice(-6) : 'TDS'}</span>
                                                <span style={{fontWeight:700}}>₹{tdsBooked.toLocaleString("en-IN",{maximumFractionDigits:0})}</span>
                                                <span style={{opacity:0.7}}>({tdsPercent}%)</span>
                                                {isExcess && <span>⚠</span>}
                                              </button>
                                            );
                                          })}
                                          {linkedInvNos.length > 2 && <span style={{fontSize:9,color:"#999"}}>+{linkedInvNos.length-2}</span>}
                                        </div>
                                      )}
                                      {linkedInvNoStr && !invData && linkedInvNos.length === 1 && <div style={{marginTop:2,fontSize:9.5,color:"#d59300"}}>⚠ not in synced invoices</div>}
                                    </div>
                                  );
                                })()
                              : <span style={{fontSize:10,color:"var(--tx3)",fontStyle:"italic",padding:"2px 6px"}}>matched</span>
                            }
                          </td>
                        </tr>
                      );
                    })}</tbody>
                  </table>
                )}
            </div>
            <div className="modal-ft">
              <span style={{color:"var(--tx2)"}}>Total TDS (26AS)</span>
              <div style={{display:"flex",alignItems:"center",gap:10}}>
                {groups.length>0 && <span style={{fontSize:11,color:"var(--grn)",fontWeight:600}}>✓ {[...matchedAsIds].length} matched · {unbookedCount} unbooked</span>}
                {selAs.size>0 && <span style={{fontSize:11,color:"var(--a)",fontWeight:600}}>{selAs.size} selected</span>}
                <div style={{display:"flex",alignItems:"center",gap:8}}>
                  <span style={{fontFamily:"Consolas,monospace",fontWeight:700,color:"#a80000",fontSize:13}}>₹{asTDS.toLocaleString("en-IN",{minimumFractionDigits:2})}</span>
                  <span style={{fontSize:11,color:"var(--tx2)"}}>Diff:</span>
                  <FmtDiff n={diff} status={tanRow?.matchStatus}/>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      {/* TDS Booking Details Modal */}
      {showTdsBookingModal && (
        <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.5)",zIndex:2000,display:"flex",alignItems:"center",justifyContent:"center"}} onClick={e=>e.target===e.currentTarget&&setShowTdsBookingModal(null)}>
          <div style={{background:"#fff",borderRadius:8,width:560,maxHeight:"80vh",overflow:"hidden",boxShadow:"0 8px 32px rgba(0,0,0,0.25)"}}>
            <div style={{background:"linear-gradient(135deg, #5c2d91, #0078d4)",padding:"16px 20px",color:"#fff"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
                <div>
                  <div style={{fontSize:10,opacity:0.8,textTransform:"uppercase",letterSpacing:1,marginBottom:4}}>TDS Booking Details</div>
                  <div style={{fontSize:16,fontWeight:700,fontFamily:"Consolas,monospace"}}>{showTdsBookingModal.invoiceNo}</div>
                </div>
                <button onClick={()=>setShowTdsBookingModal(null)} style={{background:"rgba(255,255,255,0.2)",border:"none",borderRadius:4,padding:"4px 8px",cursor:"pointer",color:"#fff",fontSize:14}}>✕</button>
              </div>
            </div>
            <div style={{padding:"14px 20px",background:"#f8f9fa",borderBottom:"1px solid #e0e0e0",display:"grid",gridTemplateColumns:"1fr 1fr 1fr 1fr",gap:12}}>
              <div style={{background:"#fff",padding:"10px",borderRadius:6,border:"1px solid #e0e0e0"}}>
                <div style={{fontSize:10,color:"#666",textTransform:"uppercase",marginBottom:3}}>Taxable Value</div>
                <div style={{fontSize:16,fontWeight:700,color:"#107c10",fontFamily:"Consolas,monospace"}}>₹{(showTdsBookingModal.taxableVal||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</div>
              </div>
              <div style={{background:"#fff",padding:"10px",borderRadius:6,border:"1px solid #e0e0e0"}}>
                <div style={{fontSize:10,color:"#666",textTransform:"uppercase",marginBottom:3}}>TDS Booked</div>
                <div style={{fontSize:16,fontWeight:700,color:"#0078d4",fontFamily:"Consolas,monospace"}}>₹{(showTdsBookingModal.tdsBooked||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</div>
              </div>
              <div style={{background:"#fff",padding:"10px",borderRadius:6,border:`1px solid ${(()=>{const inv=txnsInvoices?.find(i=>(i.invoiceNo||'').trim().toUpperCase()===(showTdsBookingModal.invoiceNo||'').toUpperCase());return(inv?.amountDue||0)>0?"#ffd54f":"#c8e6c9";})()}`}}>
                  <div style={{fontSize:10,color:"#666",textTransform:"uppercase",marginBottom:3}}>Amount Due</div>
                  <div style={{fontSize:16,fontWeight:700,fontFamily:"Consolas,monospace",color:(()=>{const inv=txnsInvoices?.find(i=>(i.invoiceNo||'').trim().toUpperCase()===(showTdsBookingModal.invoiceNo||'').toUpperCase());return(inv?.amountDue||0)>0?"#d59300":"#107c10";})()}}>
                    {(()=>{const inv=txnsInvoices?.find(i=>(i.invoiceNo||'').trim().toUpperCase()===(showTdsBookingModal.invoiceNo||'').toUpperCase());const due=inv?.amountDue||0;return due>0?`₹${due.toLocaleString("en-IN",{minimumFractionDigits:2})}`:"Paid ✓";})()}
                  </div>
                </div>
              <div style={{background:"#fff",padding:"10px",borderRadius:6,border:`1px solid ${parseFloat(showTdsBookingModal.tdsPercent)>10.5?"#f0a0a0":"#e0e0e0"}`}}>
                <div style={{fontSize:10,color:"#666",textTransform:"uppercase",marginBottom:3}}>Tax Rate</div>
                <div style={{fontSize:16,fontWeight:700,color:parseFloat(showTdsBookingModal.tdsPercent)>10.5?"#a80000":"#5c2d91",fontFamily:"Consolas,monospace"}}>{showTdsBookingModal.tdsPercent}%{parseFloat(showTdsBookingModal.tdsPercent)>10.5&&<span style={{fontSize:11,marginLeft:4}}>⚠</span>}</div>
              </div>
            </div>
            <div style={{padding:"10px 20px",background:"#fff",borderBottom:"1px solid #e0e0e0",fontSize:12}}>
              <span style={{color:"#666"}}>Expected @10%: </span>
              <span style={{fontWeight:600,color:"#107c10",fontFamily:"Consolas,monospace"}}>₹{((showTdsBookingModal.taxableVal||0)*0.10).toLocaleString("en-IN",{minimumFractionDigits:2})}</span>
              <span style={{color:"#666",marginLeft:12}}>Diff: </span>
              {(()=>{const exp=(showTdsBookingModal.taxableVal||0)*0.10;const d=(showTdsBookingModal.tdsBooked||0)-exp;return<span style={{fontWeight:600,fontFamily:"Consolas,monospace",color:d>1?"#a80000":d<-1?"#d59300":"#107c10"}}>{d>0?"+":""}{d.toLocaleString("en-IN",{minimumFractionDigits:2})}{d>1?" (Excess)":d<-1?" (Short)":" ✓"}</span>;})()}
            </div>
            <div style={{padding:"14px 20px",maxHeight:220,overflowY:"auto"}}>
              <div style={{fontSize:12,fontWeight:600,color:"#333",marginBottom:8}}>📘 Books Entries ({showTdsBookingModal.linkedRows?.length||0})</div>
              {(!showTdsBookingModal.linkedRows||showTdsBookingModal.linkedRows.length===0)?(
                <div style={{padding:"20px",textAlign:"center",color:"#999",fontSize:12,background:"#f8f8f8",borderRadius:6}}>No TDS booked</div>
              ):(
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <thead><tr style={{background:"#f0f4f8"}}><th style={{padding:"6px 8px",textAlign:"left",fontWeight:600,borderBottom:"2px solid #0078d4"}}>S.No.</th><th style={{padding:"6px 8px",textAlign:"left",fontWeight:600,borderBottom:"2px solid #0078d4"}}>Date</th><th style={{padding:"6px 8px",textAlign:"left",fontWeight:600,borderBottom:"2px solid #0078d4"}}>Qtr</th><th style={{padding:"6px 8px",textAlign:"left",fontWeight:600,borderBottom:"2px solid #0078d4"}}>Section</th><th style={{padding:"6px 8px",textAlign:"right",fontWeight:600,borderBottom:"2px solid #0078d4"}}>TDS Amt</th><th style={{padding:"6px 8px",textAlign:"right",fontWeight:600,borderBottom:"2px solid #0078d4"}}>Rate</th></tr></thead>
                  <tbody>
                    {showTdsBookingModal.linkedRows.map((row,idx)=>{const rate=showTdsBookingModal.taxableVal>0?((row.tdsDeducted||0)/showTdsBookingModal.taxableVal*100).toFixed(2):"—";return(
                      <tr key={idx} style={{borderBottom:"1px solid #f0f0f0",background:idx%2===0?"#fff":"#fafafa"}}>
                        <td style={{padding:"6px 8px",color:"#999"}}>{idx+1}</td>
                        <td style={{padding:"6px 8px",fontFamily:"Consolas,monospace",fontSize:10}}>{row.date||row.invoiceDate||"—"}</td>
                        <td style={{padding:"6px 8px"}}><span style={{background:"#e8f8e8",color:"#107c10",padding:"1px 6px",borderRadius:8,fontSize:9,fontWeight:600}}>{row.quarter||"—"}</span></td>
                        <td style={{padding:"6px 8px"}}><span style={{background:"#f0e8ff",color:"#5c2d91",padding:"1px 6px",borderRadius:8,fontSize:9,fontWeight:600}}>{row.section||"—"}</span></td>
                        <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:600,color:"#0078d4"}}>₹{(row.tdsDeducted||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</td>
                        <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"Consolas,monospace",color:"#666"}}>{rate}%</td>
                      </tr>
                    );})}
                  </tbody>
                  <tfoot><tr style={{background:"#e6f3fb"}}><td colSpan={4} style={{padding:"8px",fontWeight:700,color:"#0078d4"}}>Total</td><td style={{padding:"8px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:700,color:"#0078d4"}}>₹{(showTdsBookingModal.tdsBooked||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</td><td style={{padding:"8px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:600,color:"#5c2d91"}}>{showTdsBookingModal.tdsPercent}%</td></tr></tfoot>
                </table>
              )}
            </div>
            <div style={{padding:"10px 20px",background:"#f0f0f0",borderTop:"1px solid #e0e0e0",display:"flex",justifyContent:"flex-end"}}><button onClick={()=>setShowTdsBookingModal(null)} style={{background:"#0078d4",color:"#fff",border:"none",borderRadius:4,padding:"7px 18px",cursor:"pointer",fontSize:12,fontWeight:600}}>Close</button></div>
          </div>
        </div>
      )}
    </div>
  );
}

const FY_LIST = ["2025-26","2024-25","2023-24","2022-23","2021-22","2020-21"];
const mkCompany = (name, meta={}) => ({ 
  id: Date.now().toString(36)+Math.random().toString(36).slice(2), 
  name, 
  pan:"", 
  gstin:"", 
  contactPerson:"", 
  phone:"", 
  email:"", 
  clientType:"Corporate", 
  status:"active", 
  group:"", 
  notes:"", 
  // TRACES Portal Credentials
  tracesTaxpayerPAN:"", 
  tracesTaxpayerPass:"",
  tracesDeductorTAN:"",
  tracesDeductorPass:"",
  // IT Portal Credentials  
  itPortalPAN:"",
  itPortalPass:"",
  itPortalDOB:"",
  // ZIP Password (DOB for TRACES ZIP decryption)
  zipPassword:"",
  // Odoo ERP Integration
  odooUrl:"",
  odooDatabase:"",
  odooUsername:"",
  odooPassword:"",
  odooEnabled:false,
  addedOn:new Date().toISOString().slice(0,10), 
  ...meta, 
  years: {} 
});
const mkYear = () => ({ datasets:{"26AS":[],"AIS":[],"Books":[]}, files:[], reconResults:[], reconDone:false, tanMaster:[], prevMissingInBooks:[] });

export default function App() {
  const [view, setView] = useState("home");

  // ── MULTI-COMPANY STATE ──────────────────────────────────────────────────────
  const [companies, setCompanies] = useState(() => {
    const c = mkCompany("Company 1"); c.years["2025-26"] = mkYear(); return [c];
  });
  const [selCompanyId, setSelCompanyId] = useState(() => {
    const c = mkCompany("Company 1"); c.years["2025-26"] = mkYear(); return companies?.[0]?.id ?? c.id;
  });
  const [selYear, setSelYear] = useState("2025-26");
  const [showCompanyModal, setShowCompanyModal] = useState(false);
  const [editingCompany, setEditingCompany] = useState(null);
  const [newCompanyName, setNewCompanyName] = useState("");
  // ── CLIENT DASHBOARD STATE ───────────────────────────────────────────────────
  const [clientSearch, setClientSearch] = useState("");
  const [clientFilterStatus, setClientFilterStatus] = useState("All");
  const [clientFilterType, setClientFilterType] = useState("All");
  const [clientFilterGroup, setClientFilterGroup] = useState("All");
  const [clientSortBy, setClientSortBy] = useState("name");
  const [clientViewMode, setClientViewMode] = useState("table");
  const [showAddClientModal, setShowAddClientModal] = useState(false);
  const [editClientId, setEditClientId] = useState(null);
  const [clientDraft, setClientDraft] = useState({
    name:"",
    pan:"",
    gstin:"",
    contactPerson:"",
    phone:"",
    email:"",
    clientType:"Corporate",
    group:"",
    notes:"",
    tracesTaxpayerPAN:"",
    tracesTaxpayerPass:"",
    tracesDeductorTAN:"",
    tracesDeductorPass:"",
    itPortalPAN:"",
    itPortalPass:"",
    itPortalDOB:"",
    zipPassword:"",
    odooUrl:"",
    odooDatabase:"",
    odooUsername:"",
    odooPassword:"",
    odooEnabled:false
  });
  const [dashFY, setDashFY] = useState("2025-26");
  const [dashSelClientId, setDashSelClientId] = useState(null);
  
  // ── ODOO SYNC STATE ──────────────────────────────────────────────────────────
  const [showOdooSyncModal, setShowOdooSyncModal] = useState(false);
  const [odooSyncStarted, setOdooSyncStarted] = useState(false);
  const [odooSyncType, setOdooSyncType] = useState('tds');
  const [odooDateRange, setOdooDateRange] = useState({ from: '', to: '' });
  const [odooSyncProgress, setOdooSyncProgress] = useState({
    step: '',
    message: '',
    count: 0
  });
  const [odooSyncComplete, setOdooSyncComplete] = useState(false);


  // ── HELPERS TO GET/SET CURRENT COMPANY+YEAR DATA ────────────────────────────
  const curCompany = companies.find(c=>c.id===selCompanyId) || companies[0];
  const curYearData = curCompany?.years?.[selYear] || mkYear();
  const datasets = curYearData.datasets;
  const files = curYearData.files;
  const reconResults = curYearData.reconResults;
  const reconDone = curYearData.reconDone;
  const tanMaster = curYearData.tanMaster;
  const prevMissingInBooks = curYearData.prevMissingInBooks || [];

  // Migrate old TAN Master format to new format (contactPerson/contactPhone -> ccEmail/csmName)
  useEffect(() => {
    if (tanMaster && tanMaster.length > 0) {
      const needsMigration = tanMaster.some(r => r.hasOwnProperty('contactPerson') || r.hasOwnProperty('contactPhone'));
      if (needsMigration) {
        const migrated = tanMaster.map(r => ({
          ...r,
          ccEmail: r.ccEmail ?? "",
          csmName: r.csmName ?? "",
          contactPerson: undefined,
          contactPhone: undefined
        }));
        updateCurYear(yd => ({ ...yd, tanMaster: migrated }));
        console.log("Migrated TAN Master to new format (removed contactPerson/contactPhone)");
      }
    }
  }, [selCompanyId, selYear]); // Only run when company/year changes

  // Debug: Log whenever TAN Master changes
  useEffect(() => {
    console.log(`[TAN Master Changed] Count: ${tanMaster.length}, With Emails: ${tanMaster.filter(r=>r.contactEmail?.includes("@")).length}, With CC: ${tanMaster.filter(r=>r.ccEmail?.includes("@")).length}`);
  }, [tanMaster]);

  const updateCurYear = useCallback((updater) => {
    setCompanies(prev => prev.map(c => {
      if (c.id !== selCompanyId) return c;
      const yd = c.years[selYear] || mkYear();
      return { ...c, years: { ...c.years, [selYear]: { ...yd, ...updater(yd) } } };
    }));
  }, [selCompanyId, selYear]);

  const setDatasets = useCallback(val => updateCurYear(yd => ({ datasets: typeof val==="function"?val(yd.datasets):val })), [updateCurYear]);
  const setFiles = useCallback(val => updateCurYear(yd => ({ files: typeof val==="function"?val(yd.files):val })), [updateCurYear]);
  const setReconResults = useCallback(val => updateCurYear(yd => ({ reconResults: typeof val==="function"?val(yd.reconResults):val })), [updateCurYear]);
  const setReconDone = useCallback(val => updateCurYear(yd => ({ reconDone: typeof val==="function"?val(yd.reconDone):val })), [updateCurYear]);
  const setTanMaster = useCallback(val => updateCurYear(yd => ({ tanMaster: typeof val==="function"?val(yd.tanMaster):val })), [updateCurYear]);
  const setPrevMissingInBooks = useCallback(val => updateCurYear(yd => ({ prevMissingInBooks: typeof val==="function"?val(yd.prevMissingInBooks):val })), [updateCurYear]);

  // ── COMPANY MANAGEMENT ───────────────────────────────────────────────────────
  const addCompany = () => {
    const name = newCompanyName.trim() || `Company ${companies.length+1}`;
    const c = mkCompany(name); c.years[selYear] = mkYear();
    setCompanies(prev=>[...prev,c]); setSelCompanyId(c.id); setNewCompanyName(""); setShowCompanyModal(false);
    showToast(`Company "${name}" added`);
  };
  const saveClientDraft = () => {
    const name = clientDraft.name.trim();
    if(!name){ showToast("Client name is required","w"); return; }
    if(editClientId){
      setCompanies(prev=>prev.map(c=>c.id===editClientId?{...c,...clientDraft,name}:c));
      showToast(`"${name}" updated`);
    } else {
      const c = mkCompany(name, clientDraft); c.years[dashFY] = mkYear();
      setCompanies(prev=>[...prev,c]);
      showToast(`"${name}" added`);
    }
    setShowAddClientModal(false); setEditClientId(null);
  };
  const renameCompany = (id, name) => {
    setCompanies(prev=>prev.map(c=>c.id===id?{...c,name:name.trim()||c.name}:c));
    setEditingCompany(null);
  };
  const deleteCompany = (id) => {
    if (companies.length===1) { showToast("Cannot delete last company","w"); return; }
    setCompanies(prev=>{ const n=prev.filter(c=>c.id!==id); if(selCompanyId===id) setSelCompanyId(n[0].id); return n; });
    showToast("Company deleted");
  };
  const ensureYear = (compId, yr) => {
    setCompanies(prev=>prev.map(c=>{
      if(c.id!==compId) return c;
      if(c.years[yr]) return c;
      return {...c, years:{...c.years,[yr]:mkYear()}};
    }));
  };

  const [importing, setImporting] = useState(false);
  const [progress, setProgress] = useState(0);
  const [log, setLog] = useState([]);
  const [isDragging, setIsDragging] = useState(false);
  const [selDS, setSelDS] = useState("26AS");
  const [searchQ, setSearchQ] = useState("");
  const [invStatusFilter, setInvStatusFilter] = useState("all"); // all, ok, excess, notds
  const [showDupOnly, setShowDupOnly] = useState(false);
  const [invTdsDetailPopup, setInvTdsDetailPopup] = useState(null); // { invoiceNo, entries, total }
  const [odooRefs, setOdooRefs] = useState({}); // { [invoiceNo]: { odooRef, moveId, createdAt } }
  const [sortCol, setSortCol] = useState("id");
  const [sortDir, setSortDir] = useState("asc");
  const [selRows, setSelRows] = useState(new Set());
  const [toast, setToast] = useState(null);
  const [selQ, setSelQ] = useState("All");
  const [selStatus, setSelStatus] = useState("All");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [mmOnly, setMmOnly] = useState(false);
  const [reconSearch, setReconSearch] = useState("");
  const [reconMode, setReconMode] = useState("tan");   // "tan" | "section"
  const [selSection, setSelSection] = useState("All"); // section filter in section mode
  const [sectionSearch, setSectionSearch] = useState("");
  const [detailTAN, setDetailTAN] = useState(null);
  const [storageStatus, setStorageStatus] = useState("idle");
  const [lastSaved, setLastSaved] = useState(null);
  const [tanSearch, setTanSearch] = useState("");
  const [importTab, setImportTab] = useState("file"); // "file" | "traces" | "itportal"
  const [issuesOpen, setIssuesOpen] = useState(false);

  // ── TRACES PORTAL STATE ──────────────────────────────────────────────────────
  const [tracesNewFiles, setTracesNewFiles] = useState([]);
  const [tracesPortalOpen, setTracesPortalOpen] = useState(false);
  const [tracesStatus, setTracesStatus] = useState("idle");
  const [tracesClosing, setTracesClosing] = useState(false);
  const [tracesDismissed, setTracesDismissed] = useState(new Set());
  // Per-client TRACES credentials: { [companyId]: { taxpayerPAN, taxpayerPass, deductorTAN, deductorPass, zipDate, savedAt } }
  const [tracesCredsMap, setTracesCredsMap] = useState({});
  const [tracesCredsOpen, setTracesCredsOpen] = useState(false);
  // Derived: always shows credentials for the currently selected client
  const tracesCreds = tracesCredsMap[selCompanyId] || {taxpayerPAN:"",taxpayerPass:"",deductorTAN:"",deductorPass:"",zipDate:"",savedAt:null};
  const setTracesCreds = (valOrFn) => setTracesCredsMap(prev => {
    const cur = prev[selCompanyId] || {taxpayerPAN:"",taxpayerPass:"",deductorTAN:"",deductorPass:"",zipDate:"",savedAt:null};
    const next = typeof valOrFn === "function" ? valOrFn(cur) : valOrFn;
    return {...prev, [selCompanyId]: next};
  });
  const [tracesDraft, setTracesDraft]         = useState({});
  const [tracesSaving, setTracesSaving]       = useState(false);

  // ── IT PORTAL STATE (per-client credentials) ─────────────────────────────────
  // Stored as { [companyId]: { pan, password, dob, savedAt } }
  const [itPortalCredsMap, setItPortalCredsMap] = useState({});
  const [itPortalCredsOpen, setItPortalCredsOpen] = useState(false);
  const [itPortalDraft, setItPortalDraft] = useState({pan:"",password:"",dob:""});
  const [itPortalSaving, setItPortalSaving] = useState(false);
  const [itPortalOpen, setItPortalOpen] = useState(false);
  const [itPortalClosing, setItPortalClosing] = useState(false);
  const [itPortalStatus, setItPortalStatus] = useState("idle");

  // Derived: current client's IT Portal creds
  const itPortalCreds = itPortalCredsMap[selCompanyId] || {pan:"",password:"",dob:"",savedAt:null};

  // ── EMAIL NOTICE STATE ───────────────────────────────────────────────────────
  const [emailSelTANs, setEmailSelTANs] = useState(new Set());
  const [emailConfig, setEmailConfig] = useState({ ourName:"", ourDesignation:"Account Receivable Team", ourFirm:"", ourPhone:"", ourEmail:"", subject:"TDS Pending-FY 2025-26", dueDate:"", refNo:"", extraNote:"" });

  // Auto-update Firm and Subject in email settings when company or year changes
  useEffect(() => {
    if (!curCompany?.name) return;
    const due = new Date(); due.setDate(due.getDate() + 10);
    const dueDateStr = due.toISOString().slice(0, 10); // yyyy-mm-dd for date input
    setEmailConfig(prev => ({
      ...prev,
      ourFirm: curCompany.name,
      subject: `${curCompany.name}-TDS Pending-FY ${selYear}`,
      dueDate: prev.dueDate || dueDateStr, // only set if not already manually set
    }));
  }, [selCompanyId, selYear]); // eslint-disable-line
  const [emailPreviewTAN, setEmailPreviewTAN] = useState(null);
  const [emailSearch, setEmailSearch] = useState("");
  const [emailPeriodFilter, setEmailPeriodFilter] = useState(new Set()); // empty = All quarters
  const [emailTopN, setEmailTopN] = useState("All");
  const [emailMinAmt, setEmailMinAmt] = useState("");
  const [emailMaxAmt, setEmailMaxAmt] = useState("");
  const [emailPendingType, setEmailPendingType] = useState("books_gt_26as"); // books_gt_26as | all_pending
  const [emailSettingsOpen, setEmailSettingsOpen] = useState(false);
  const [emailFiltersOpen, setEmailFiltersOpen] = useState(false);

  // TAN → email address map, persisted in electron-store
  const [tanEmails, setTanEmails] = useState({});
  const updateTanEmail = (tan, email) => setTanEmails(prev => ({...prev, [tan]: email}));
  const [tanCCs, setTanCCs] = useState({});
  const updateTanCC = (tan, cc) => setTanCCs(prev => ({...prev, [tan]: cc}));

  // ── EMAIL TRACKER STATE ──────────────────────────────────────────────────────
  const [emailLog, setEmailLog] = useState([]);          // [{id,tan,name,to,subject,sentAt,status,threadId,messageId,pendingAmt,fy,company,openedAt,repliedAt,lastChecked}]
  const [odooLog, setOdooLog] = useState([]);
  const [odooLogFilter, setOdooLogFilter] = useState('All');
  const [odooLogSearch, setOdooLogSearch] = useState('');
  const [odooLogPeriod, setOdooLogPeriod] = useState('All');
  const [trackerSearch, setTrackerSearch] = useState("");
  const [trackerFilter, setTrackerFilter] = useState("All"); // All | Sent | Opened | Replied | Failed
  const [checkingStatus, setCheckingStatus] = useState(false);
  const addEmailLog = (entry) => setEmailLog(prev => [entry, ...prev.slice(0,999)]);

  // ── GMAIL OAUTH STATE ────────────────────────────────────────────────────────
  const [gmailClientId, setGmailClientId] = useState("");
  const [gmailClientSecret, setGmailClientSecret] = useState("");
  const [gmailClientSecretDraft, setGmailClientSecretDraft] = useState("");
  const [gmailToken, setGmailToken] = useState(null);          // { access_token, expires_at }
  const [gmailUser, setGmailUser] = useState(null);            // { email, name }
  const [gmailConnecting, setGmailConnecting] = useState(false); // spinner in modal
  const [gmailAuthError, setGmailAuthError]   = useState("");    // error shown inside modal

  // ── Load Gmail credentials + token from store on startup ────────────────────
  useEffect(() => {
    (async () => {
      // Restore credentials from Firebase
      const storedId = await loadFromStore('gmail_client_id');
      const storedSecret = await loadFromStore('gmail_client_secret');
      if (storedId) setGmailClientId(storedId);
      if (storedSecret) setGmailClientSecret(storedSecret);
      // Restore access token if still valid
      const storedToken   = await loadFromStore('gmail_access_token');
      const storedExpiry  = await loadFromStore('gmail_token_expiry');
      const storedEmail   = await loadFromStore('gmail_user_email');
      if (storedToken && storedExpiry && Number(storedExpiry) > Date.now() + 60_000) {
        setGmailToken({ access_token: storedToken, expires_at: Number(storedExpiry) });
        if (storedEmail) setGmailUser({ email: storedEmail, name: "" });
      }
    })();
  // eslint-disable-next-line
  }, []);
  const [showGmailSetup, setShowGmailSetup] = useState(false);
  const [gmailClientIdDraft, setGmailClientIdDraft] = useState("");
  const [gmailSending, setGmailSending] = useState(false);     // bulk sending in progress
  const [gmailSendProgress, setGmailSendProgress] = useState({done:0,total:0,errors:[]});
  const tokenClient = useRef(null);
  const pendingSendQueue = useRef(null);  // holds rows waiting after sign-in

  const isGmailConnected = gmailToken && gmailToken.expires_at > Date.now();

  // ── GOOGLE DRIVE AUTO-BACKUP STATE ──────────────────────────────────────────
  const [driveToken, setDriveToken]               = useState(null);
  const [driveUser, setDriveUser]                 = useState(null);
  const [driveBackupStatus, setDriveBackupStatus] = useState("idle"); // idle|running|done|error
  const [driveLastBackup, setDriveLastBackup]     = useState(() => localStorage.getItem("drive_last_backup") || null);
  const [driveEnabled, setDriveEnabled]           = useState(() => localStorage.getItem("drive_enabled") === "true");
  const [localBackupFolder, setLocalBackupFolder] = useState(""); // path to local auto-backup folder
  const [localBackupLog, setLocalBackupLog]       = useState([]);
  const [driveBackupLog, setDriveBackupLog]       = useState([]);
  const driveTokenClient          = useRef(null);
  const driveBackupRanThisSession = useRef(false);
  const isDriveConnected          = driveToken && driveToken.expires_at > Date.now();
  const driveBackupRef            = useRef(null); // always points to latest runDriveBackup
  const companiesRef              = useRef([]);   // always points to latest companies state
  const driveTokenRef             = useRef(null); // always points to latest driveToken

  // ── DRIVE SYNC STATE ─────────────────────────────────────────────────────────
  const [driveSyncStatus, setDriveSyncStatus] = useState("idle"); // idle|checking|synced|conflict|error|no_backup
  const [driveSyncModal, setDriveSyncModal]   = useState(null);   // null | { driveData, driveTs, localTs, recordCount }
  const [driveLastSync, setDriveLastSync]     = useState(() => localStorage.getItem("drive_last_sync") || null);
  const driveSyncRanThisSession               = useRef(false);
  // Drive index stored in state+ref so reads are always instant and never stale
  // (localStorage in Electron can be cleared on restart — electron-store is reliable)
  const [driveBackupIndex, setDriveBackupIndex] = useState([]);
  const [driveFolderId,    setDriveFolderId]    = useState(() => localStorage.getItem("drive_folder_id") || "");
  const driveBackupIndexRef = useRef([]);
  const driveFolderIdRef    = useRef(localStorage.getItem("drive_folder_id") || "");

  // ── Gmail OAuth via real browser (works on all PCs, no GSI popup issues) ──
  const connectGmail = async () => {
    if(!gmailClientId){ setShowGmailSetup(true); return; }
    setGmailAuthError("");
    setGmailConnecting(true);
    // Auto-save draft secret if user typed it but didn't explicitly save
    if (gmailClientSecretDraft.trim() && gmailClientSecretDraft.trim() !== gmailClientSecret) {
      const s = gmailClientSecretDraft.trim();
      setGmailClientSecret(s);
      saveToStore('gmail_client_secret', s);
    }
    showToast("Opening Google sign-in…","s");
    try {
      let resp;
      if (isElectron) {
        resp = await window.electronAPI.googleOAuthStart({
          clientId: gmailClientId,
          clientSecret: gmailClientSecretDraft.trim() || gmailClientSecret || undefined,
          scope: "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/userinfo.email",
        });
      } else {
        // Web: OAuth popup flow via server
        const secret = gmailClientSecretDraft.trim() || gmailClientSecret;
        const redirectUri = `${SERVER_BASE}/api/gmail/callback`;
        const params = new URLSearchParams({
          client_id: gmailClientId,
          redirect_uri: redirectUri,
          response_type: 'code',
          scope: 'https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/userinfo.email',
          access_type: 'offline',
          prompt: 'consent'
        });
        const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + params.toString();

        // Open popup and wait for callback
        const oauthResult = await new Promise((resolve) => {
          const popup = window.open(authUrl, 'Gmail Sign In', 'width=500,height=650,left=200,top=100');
          const handler = (event) => {
            if (event.data?.type === 'gmail-oauth') {
              window.removeEventListener('message', handler);
              resolve(event.data);
            }
          };
          window.addEventListener('message', handler);
          setTimeout(() => { window.removeEventListener('message', handler); resolve({ error: 'timeout' }); }, 180000);
        });

        if (oauthResult.error) {
          resp = { error: oauthResult.error };
        } else if (oauthResult.code) {
          // Exchange code for tokens via server
          const exchangeRes = await fetch(`${SERVER_BASE}/api/gmail/exchange`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              code: oauthResult.code,
              clientId: gmailClientId,
              clientSecret: secret,
              redirectUri: redirectUri
            })
          });
          const exchangeData = await exchangeRes.json();
          if (exchangeData.ok) {
            resp = { access_token: exchangeData.accessToken, expires_in: exchangeData.expiresIn, refresh_token: exchangeData.refreshToken };
          } else {
            resp = { error: exchangeData.error };
          }
        } else {
          resp = { error: 'No code received from OAuth popup' };
        }
      }

      console.log("[Gmail OAuth] Response:", JSON.stringify(resp));

      if(!resp || resp.error){
        const msg = resp?.error || "Unknown error";
        setGmailAuthError("❌ " + msg);
        setShowGmailSetup(true);
        showToast("Gmail auth failed: "+msg,"e",8000);
        setGmailConnecting(false);
        return;
      }
      if(!resp.access_token){
        const msg = "No access_token in response";
        setGmailAuthError("❌ " + msg);
        setShowGmailSetup(true);
        setGmailConnecting(false);
        return;
      }

      const token = { access_token: resp.access_token, expires_at: Date.now() + (resp.expires_in||3599)*1000 };

      // Persist token to store
      await saveToStore('gmail_access_token', resp.access_token);
      await saveToStore('gmail_token_expiry', String(token.expires_at));
      if(resp.refresh_token) await saveToStore('gmail_refresh_token', resp.refresh_token);

      try {
        const ui = await fetch("https://www.googleapis.com/oauth2/v3/userinfo",{headers:{Authorization:"Bearer "+resp.access_token}});
        const ud = await ui.json();
        setGmailUser({email:ud.email||"",name:ud.name||""});
        if(ud.email) await saveToStore('gmail_user_email', ud.email);
      } catch(e){ setGmailUser({email:"",name:""}); }

      setGmailToken(token);
      setGmailAuthError("");
      setGmailConnecting(false);
      showToast("Gmail connected ✓","s");
      setShowGmailSetup(false);
      if(pendingSendQueue.current){
        const rows = pendingSendQueue.current;
        pendingSendQueue.current = null;
        sendViaGmail(rows, token);
      }
    } catch(e){
      const msg = e.message || String(e);
      setGmailAuthError("❌ " + msg);
      setShowGmailSetup(true);
      showToast("Gmail auth error: "+msg,"e",8000);
      setGmailConnecting(false);
    }
  };

  const disconnectGmail = () => {
    if(gmailToken) { fetch(`https://oauth2.googleapis.com/revoke?token=${gmailToken.access_token}`,{method:'POST'}).catch(()=>{}); }
    setGmailToken(null); setGmailUser(null);
    showToast("Gmail disconnected");
  };

  const saveGmailClientId = (id) => {
    const trimmed = id.trim();
    setGmailClientId(trimmed);
    saveToStore('gmail_client_id', trimmed);
    if(gmailClientSecretDraft.trim()){
      const secret = gmailClientSecretDraft.trim();
      setGmailClientSecret(secret);
      setGmailClientSecretDraft("");
      saveToStore('gmail_client_secret', secret);
    }
    showToast("Client ID saved","s");
    setShowGmailSetup(false);
    // Auto-connect after saving
    setTimeout(()=>{ connectGmail(); }, 300);
  };

  // ── GOOGLE DRIVE FUNCTIONS ───────────────────────────────────────────────────

  // ── Drive OAuth via real browser ──
  const connectDrive = async () => {
    const clientId = gmailClientId;
    if (!clientId) { showToast("Set your Google OAuth Client ID in Gmail settings first", "w"); return; }
    showToast("Opening Google sign-in…","s");
    try {
      let resp;
      if (isElectron) {
        resp = await window.electronAPI.googleOAuthStart({
          clientId,
          clientSecret: gmailClientSecretDraft.trim() || gmailClientSecret || undefined,
          scope: "https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/userinfo.email",
        });
      } else {
        // Web: popup OAuth
        const secret = gmailClientSecretDraft.trim() || gmailClientSecret;
        const redirectUri = `${SERVER_BASE}/api/gmail/callback`;
        const params = new URLSearchParams({
          client_id: clientId, redirect_uri: redirectUri, response_type: 'code',
          scope: 'https://www.googleapis.com/auth/drive https://www.googleapis.com/auth/userinfo.email',
          access_type: 'offline', prompt: 'consent'
        });
        const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + params.toString();
        const oauthResult = await new Promise((resolve) => {
          const popup = window.open(authUrl, 'Drive Sign In', 'width=500,height=650');
          const handler = (event) => { if (event.data?.type === 'gmail-oauth') { window.removeEventListener('message', handler); resolve(event.data); } };
          window.addEventListener('message', handler);
          setTimeout(() => { window.removeEventListener('message', handler); resolve({ error: 'timeout' }); }, 180000);
        });
        if (oauthResult.error) { resp = { error: oauthResult.error }; }
        else if (oauthResult.code) {
          const exRes = await fetch(`${SERVER_BASE}/api/gmail/exchange`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code: oauthResult.code, clientId, clientSecret: secret, redirectUri })
          });
          const exData = await exRes.json();
          resp = exData.ok ? { access_token: exData.accessToken, expires_in: exData.expiresIn, refresh_token: exData.refreshToken } : { error: exData.error };
        } else { resp = { error: 'No code' }; }
      }
      if (resp.error) { showToast("Drive auth failed: " + resp.error, "e"); return; }
      if (isElectron && resp.refresh_token) {
        await window.electronAPI.driveSaveRefreshToken(resp.refresh_token);
      }
      try {
        const ui = await fetch("https://www.googleapis.com/oauth2/v3/userinfo", { headers: { Authorization: "Bearer " + resp.access_token } });
        const ud = await ui.json();
        setDriveUser({ email: ud.email || "" });
      } catch(e) {}
      const token = { access_token: resp.access_token, expires_at: Date.now() + (resp.expires_in || 3599) * 1000 };
      setDriveToken(token);
      driveTokenRef.current = token;
      setDriveEnabled(true);
      showToast("Google Drive connected ✓ — auto-backup enabled", "s");
      runDriveBackup(token);
    } catch(e) { showToast("Drive auth error: " + e.message, "e"); }
  };

  const disconnectDrive = async () => {
    if (driveToken) { fetch(`https://oauth2.googleapis.com/revoke?token=${driveToken.access_token}`,{method:'POST'}).catch(()=>{}); }
    if (isElectron) await window.electronAPI.driveClearRefreshToken?.();
    setDriveToken(null); setDriveUser(null);
    setDriveEnabled(false);
    showToast("Google Drive disconnected");
  };

  // Step 3: Silent token refresh via Electron (no popup) — uses stored refresh_token
  const silentlyRefreshDriveToken = async () => {
    if (!gmailClientId) return null;
    if (isElectron) {
      const result = await window.electronAPI.driveRefreshAccessToken?.(gmailClientId);
      if (!result || result.error) return null;
      const token = { access_token: result.access_token, expires_at: Date.now() + (result.expires_in || 3599) * 1000 };
      setDriveToken(token);
      driveTokenRef.current = token;
      if (!driveUser?.email) {
        fetch("https://www.googleapis.com/oauth2/v3/userinfo", { headers: { Authorization: "Bearer " + result.access_token } })
          .then(r => r.json()).then(ud => { if (ud.email) setDriveUser({ email: ud.email }); }).catch(()=>{});
      }
      return token;
    }
    // Web: try server-side refresh
    try {
      const secret = gmailClientSecret || localStorage.getItem('gmail_client_secret') || '';
      const res = await fetch(`${SERVER_BASE}/api/gmail/refresh`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clientId: gmailClientId, clientSecret: secret })
      });
      const data = await res.json();
      if (!data.ok) return null;
      const token = { access_token: data.accessToken, expires_at: Date.now() + (data.expiresIn || 3599) * 1000 };
      setDriveToken(token); driveTokenRef.current = token;
      return token;
    } catch(e) { return null; }
  };

  // ── DRIVE BACKUP ─────────────────────────────────────────────────────────────
  // Uploads a full JSON backup. Stores the file ID directly in electron-store
  // and in-memory ref — never relies on files.list which can fail with GIS tokens.
  const runDriveBackup = async (overrideToken) => {
    const token = overrideToken || driveTokenRef.current || driveToken;
    if (!token || token.expires_at <= Date.now()) {
      if (gmailClientId) {
        const refreshed = await silentlyRefreshDriveToken();
        if (refreshed) { runDriveBackup(refreshed); return; }
      }
      setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "error", msg: "✗ Token expired — please reconnect Google Drive" }, ...p.slice(0, 19)]);
      showToast("Drive token expired — please reconnect Google Drive", "w");
      return;
    }
    if (driveBackupStatus === "running") return;

    const currentCompanies = companiesRef.current || [];
    const totalRecordsNow = currentCompanies.reduce((a, c) =>
      a + Object.values(c.years || {}).reduce((b, y) =>
        b + Object.values(y.datasets || {}).reduce((s, d) => s + d.length, 0), 0), 0);
    if (totalRecordsNow === 0) {
      setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "done", msg: "⏭ Skipped — no data to backup" }, ...p.slice(0, 19)]);
      return;
    }

    setDriveBackupStatus("running");
    setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "running", msg: "Backup in progress…" }, ...p.slice(0, 19)]);
    try {
      const auth = "Bearer " + token.access_token;
      // Log token validity for debugging
      const tokenAge = Math.round((token.expires_at - Date.now()) / 1000);
      console.log("[Drive Backup] Token expires in:", tokenAge, "s | access_token prefix:", token.access_token?.slice(0,20));
      const payload = { version: 2, scope: "all", companies: currentCompanies, exportedAt: new Date().toISOString() };
      const fileContent = JSON.stringify(payload, null, 2);
      const ts = new Date().toISOString().slice(0, 16).replace("T", "_").replace(/:/g, "-");
      const fileName = `26AS_Recon_Backup_${ts}.json`;

      // ── Get or create backup folder ───────────────────────────────────────────
      // Priority: 1) in-memory ref (fastest), 2) localStorage, 3) create new
      // We deliberately skip Drive folder search — it requires drive.metadata scope
      // which GIS implicit tokens may not provide. Creating a folder is idempotent enough.
      let folderId = driveFolderIdRef.current || localStorage.getItem("drive_folder_id") || null;

      if (!folderId) {
        setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "running",
          msg: "Creating backup folder on Drive…" }, ...p.slice(0, 19)]);
        const cf = await fetch("https://www.googleapis.com/drive/v3/files?fields=id", {
          method: "POST",
          headers: { Authorization: auth, "Content-Type": "application/json" },
          body: JSON.stringify({ name: "26AS Recon Backups", mimeType: "application/vnd.google-apps.folder" })
        });
        const cfData = await cf.json();
        if (!cf.ok) throw new Error("Folder create: " + (cfData?.error?.message || `HTTP ${cf.status}`));
        folderId = cfData.id || null;
        if (!folderId) throw new Error("Folder create returned no ID — check Drive API permissions");
        // Persist immediately
        setDriveFolderId(folderId); driveFolderIdRef.current = folderId;
        localStorage.setItem("drive_folder_id", folderId); saveToStore('driveFolderId', folderId);
        setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "done",
          msg: `Folder created: ${folderId}` }, ...p.slice(0, 19)]);
      }

      // ── Upload the backup file ────────────────────────────────────────────────
      const boundary = "26as_backup_boundary";
      const body = [
        `--${boundary}\r\nContent-Type: application/json\r\n\r\n`,
        JSON.stringify({ name: fileName, parents: [folderId] }),
        `\r\n--${boundary}\r\nContent-Type: application/json\r\n\r\n`,
        fileContent,
        `\r\n--${boundary}--`
      ].join("");
      const uploadRes = await fetch("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id", {
        method: "POST",
        headers: { Authorization: auth, "Content-Type": `multipart/related; boundary=${boundary}` },
        body
      });
      if (!uploadRes.ok) {
        // Log the full Google error so we can diagnose exactly what's wrong
        let errDetail = `HTTP ${uploadRes.status}`;
        try { const errBody = await uploadRes.json(); errDetail = errBody?.error?.message || errBody?.error?.status || errDetail; } catch(e) {}
        throw new Error(`Upload failed: ${errDetail}`);
      }
      const uploadedFileId = (await uploadRes.json()).id;
      if (!uploadedFileId) throw new Error("Upload succeeded but got no file ID back");

      // ── Save file ID to index IMMEDIATELY — this is the only thing sync needs ─
      // We prepend to the existing index and cap at 10 entries.
      // We do NOT use files.list here — it can return empty with GIS-scoped tokens.
      const prevIndex = driveBackupIndexRef.current || [];
      const newIndex = [
        { id: uploadedFileId, name: fileName, ts: Date.now(), records: totalRecordsNow },
        ...prevIndex.filter(e => e.id !== uploadedFileId)  // deduplicate
      ].slice(0, 10);

      setDriveBackupIndex(newIndex);
      driveBackupIndexRef.current = newIndex;
      localStorage.setItem("drive_backup_index", JSON.stringify(newIndex));
      saveToStore('driveBackupIndex', newIndex);  // primary persistent store

      // ── Best-effort: delete old backups beyond 7 using IDs we already know ────
      // Uses only file IDs we track — no files.list needed.
      const toDelete = prevIndex.slice(6); // entries beyond 7 (index 0-6 = 7 entries)
      if (toDelete.length) {
        Promise.all(toDelete.map(f =>
          fetch(`https://www.googleapis.com/drive/v3/files/${f.id}`, { method: "DELETE", headers: { Authorization: auth } })
            .catch(() => {}) // ignore delete errors
        ));
      }

      const now = new Date().toLocaleString("en-IN");
      localStorage.setItem("drive_last_backup", now);
      setDriveLastBackup(now);
      setDriveBackupStatus("done");
      setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "done", msg: `✓ Backed up — ${fileName}` }, ...p.slice(1)]);
    } catch(e) {
      setDriveBackupStatus("error");
      const errMsg = e.message || "Unknown error";
      // Truncate long Google API error messages in toast, show full in log
      const toastMsg = errMsg.length > 80 ? errMsg.slice(0, 77) + "…" : errMsg;
      setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "error", msg: `✗ Failed: ${errMsg}` }, ...p.slice(1)]);
      showToast("Drive backup failed: " + toastMsg, "e");
    }
  };

  // ── PULL & SYNC FROM DRIVE ───────────────────────────────────────────────────
  // Called on startup (auto) or manually. Fetches the latest NON-EMPTY backup
  // from Drive, and either auto-applies (if local is empty) or shows conflict modal.
  const pullDriveSync = async (overrideToken) => {
    // Guard: don't run two syncs at once
    if (driveSyncStatus === "checking") return;

    // ── Helper: fetch with per-request timeout ────────────────────────────────
    const fetchWithTimeout = (url, opts = {}, ms = 12000) => {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), ms);
      return fetch(url, { ...opts, signal: ctrl.signal }).finally(() => clearTimeout(t));
    };

    // ── Overall 30s hard guard ────────────────────────────────────────────────
    let syncDone = false;
    const overallTimer = setTimeout(() => {
      if (syncDone) return;
      syncDone = true;
      setDriveSyncStatus("error");
      setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "error",
        msg: "\u2717 Sync timed out — check your connection and try again" }, ...p.slice(0, 19)]);
      showToast("Drive sync timed out — please try again", "w");
    }, 30000);

    setDriveSyncStatus("checking");

    try {
      // ── Step 1: Get a valid token — use existing if still fresh ──────────────
      // If backup just ran successfully, driveTokenRef is already fresh — no need
      // to call GIS again. Only refresh if the token is actually expired.
      let token = overrideToken || driveTokenRef.current || driveToken;
      if (!token || token.expires_at <= Date.now()) {
        setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "running",
          msg: "\u2b07 Refreshing Drive token\u2026" }, ...p.slice(0, 19)]);
        token = await silentlyRefreshDriveToken(); // 6s timeout built-in
        if (!token) {
          setDriveSyncStatus("error");
          showToast("Drive token expired — please reconnect Google Drive", "e");
          return;
        }
      }
      const auth = "Bearer " + token.access_token;

      // ── Step 2: Get backup index from ref/store — NO files.list needed ─────────
      // The index is built purely from file IDs returned at upload time.
      // files.list is intentionally avoided — it fails with GIS-scoped tokens.
      let backupIndex = driveBackupIndexRef.current?.length
        ? driveBackupIndexRef.current
        : JSON.parse(localStorage.getItem("drive_backup_index") || "[]");

      if (!backupIndex.length) {
        setDriveSyncStatus("no_backup");
        setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "error",
          msg: "✗ No backup index found — click 'Backup Now' to create first backup" }, ...p.slice(0, 19)]);
        showToast("Click 'Backup Now' first — then sync will work automatically", "w", 7000);
        return;
      }

      setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "running",
        msg: `\u2b07 Scanning ${backupIndex.length} backup(s)\u2026` }, ...p.slice(0, 19)]);

      // ── Step 3: Download newest non-empty backup ──────────────────────────────
      let goodPayload = null, goodName = "";
      const nonEmptyEntries = backupIndex.filter(e => !e.records || e.records > 0);

      for (const entry of nonEmptyEntries) {
        if (syncDone) return;
        try {
          let dlRes = await fetchWithTimeout(
            `https://www.googleapis.com/drive/v3/files/${entry.id}?alt=media`,
            { headers: { Authorization: auth } },
            20000 // 20s for potentially large backup files
          );
          // 401/403 → refresh token once and retry
          if (dlRes.status === 401 || dlRes.status === 403) {
            const retryToken = await silentlyRefreshDriveToken();
            if (retryToken) {
              dlRes = await fetchWithTimeout(
                `https://www.googleapis.com/drive/v3/files/${entry.id}?alt=media`,
                { headers: { Authorization: "Bearer " + retryToken.access_token } },
                20000
              );
            }
          }
          if (!dlRes.ok) {
            setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "error",
              msg: `\u2b07 ${entry.name}: HTTP ${dlRes.status}` }, ...p.slice(0, 19)]);
            continue;
          }
          const data = await dlRes.json();
          const recs = (data?.companies || []).reduce((a, c) =>
            a + Object.values(c.years || {}).reduce((b, y) =>
              b + Object.values(y.datasets || {}).reduce((s, d) => s + d.length, 0), 0), 0);
          setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: recs > 0 ? "done" : "error",
            msg: `\u2b07 ${entry.name}: ${recs} records` }, ...p.slice(0, 19)]);
          if (recs > 0) { goodPayload = data; goodName = entry.name; break; }
        } catch(e) {
          setDriveBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "error",
            msg: `\u2b07 ${entry.name}: ${e.name === "AbortError" ? "timed out" : e.message}` }, ...p.slice(0, 19)]);
          continue;
        }
      }

      if (!goodPayload) {
        setDriveSyncStatus("no_backup");
        showToast("All Drive backups are empty — please import data and backup first", "w");
        return;
      }

      // ── Step 4: Apply or show conflict modal ──────────────────────────────────
      const driveRecords = goodPayload.companies.reduce((a, c) =>
        a + Object.values(c.years || {}).reduce((b, y) =>
          b + Object.values(y.datasets || {}).reduce((s, d) => s + d.length, 0), 0), 0);
      const localRecords = (companiesRef.current || []).reduce((a, c) =>
        a + Object.values(c.years || {}).reduce((b, y) =>
          b + Object.values(y.datasets || {}).reduce((s, d) => s + d.length, 0), 0), 0);

      if (localRecords === 0) {
        applyDriveData(goodPayload.companies, driveRecords);
      } else {
        setDriveSyncModal({ driveData: goodPayload.companies, driveFileName: goodName, driveRecords, localRecords });
        setDriveSyncStatus("conflict");
      }

    } catch(e) {
      if (syncDone) return;
      setDriveSyncStatus("error");
      showToast("Drive sync failed: " + e.message, "e");
      console.warn("Drive sync failed:", e);
    } finally {
      syncDone = true;
      clearTimeout(overallTimer);
    }
  };


  // ── LOCAL FOLDER AUTO-BACKUP ─────────────────────────────────────────────────
  // Writes backup JSON to a user-chosen local folder (e.g. Google Drive desktop,
  // Dropbox, OneDrive). 100% reliable — no OAuth, no 403, no scope issues.
  const runLocalBackup = async (folderPath) => {
    const path = folderPath || localBackupFolder;
    if (!path || !isElectron) return;
    const currentCompanies = companiesRef.current || [];
    const totalRecords = currentCompanies.reduce((a, c) =>
      a + Object.values(c.years || {}).reduce((b, y) =>
        b + Object.values(y.datasets || {}).reduce((s, d) => s + d.length, 0), 0), 0);
    if (totalRecords === 0) return;
    try {
      const payload = { version: 2, scope: "all", companies: currentCompanies, exportedAt: new Date().toISOString() };
      const content = JSON.stringify(payload, null, 2);
      const ts = new Date().toISOString().slice(0, 16).replace("T", "_").replace(/:/g, "-");
      const fileName = `26AS_Recon_Backup_${ts}.json`;
      // Use saveFile with a targetPath if supported, otherwise fall back to dialog
      const res = await window.electronAPI.saveFile({ defaultName: fileName, content, targetPath: path + "/" + fileName });
      if (res?.success) {
        const now = new Date().toLocaleString("en-IN");
        setLocalBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "done", msg: `✓ Saved — ${fileName}` }, ...p.slice(0, 9)]);
        showToast("Local backup saved ✓", "s");
      }
    } catch(e) {
      setLocalBackupLog(p => [{ time: new Date().toLocaleTimeString(), status: "error", msg: `✗ ${e.message}` }, ...p.slice(0, 9)]);
    }
  };

  // Apply Drive data to state (replace companies)
  const applyDriveData = (driveCompanies, recordCount) => {
    if (!driveCompanies?.length) { showToast("Drive backup has no data", "e"); return; }
    setCompanies(driveCompanies);
    setSelCompanyId(driveCompanies[0].id);
    setSelYear(Object.keys(driveCompanies[0].years || {}).sort().reverse()[0] || "2025-26");
    const now = new Date().toLocaleString("en-IN");
    localStorage.setItem("drive_last_sync", now);
    localStorage.setItem("drive_last_backup", now); // treat sync as a fresh baseline
    setDriveLastSync(now);
    setDriveLastBackup(now);
    setDriveSyncStatus("synced");
    setDriveSyncModal(null);
    showToast(`☁ Synced ${recordCount.toLocaleString()} records from Google Drive ✓`, "s", 5000);
  };

  // Merge Drive data with local (union of companies by id, newer wins per company)
  const mergeDriveData = (driveCompanies, recordCount) => {
    setCompanies(prev => {
      const merged = [...prev];
      driveCompanies.forEach(dc => {
        const idx = merged.findIndex(lc => lc.id === dc.id);
        if (idx === -1) {
          merged.push(dc); // New company from Drive → add
        } else {
          // Merge years: union, Drive wins per year if it has more records
          const lc = merged[idx];
          const mergedYears = { ...lc.years };
          Object.entries(dc.years || {}).forEach(([yr, dyData]) => {
            const lyData = lc.years?.[yr];
            if (!lyData) {
              mergedYears[yr] = dyData;
            } else {
              const dRecs = Object.values(dyData.datasets || {}).reduce((s, d) => s + d.length, 0);
              const lRecs = Object.values(lyData.datasets || {}).reduce((s, d) => s + d.length, 0);
              if (dRecs > lRecs) mergedYears[yr] = dyData; // Drive has more → use Drive
            }
          });
          merged[idx] = { ...lc, ...dc, years: mergedYears };
        }
      });
      return merged;
    });
    const now = new Date().toLocaleString("en-IN");
    localStorage.setItem("drive_last_sync", now);
    setDriveLastSync(now);
    setDriveSyncStatus("synced");
    setDriveSyncModal(null);
    showToast(`☁ Merged Drive data — ${recordCount} records synced ✓`, "s", 5000);
  };

  // Build multipart MIME email with optional base64 attachment
  const buildRawEmail = (to, subject, body, fromEmail, attachment, cc) => {
    const boundary = "----TDSMailer" + Date.now();
    // Base64-encode HTML body so Gmail renders it correctly (not as plain text)
    const bodyB64 = btoa(unescape(encodeURIComponent(body))).replace(/.{1,76}/g, m => m + "\r\n").trim();
    const lines = [
      `From: ${fromEmail}`,
      `To: ${to}`,
      ...(cc ? [`Cc: ${cc}`] : []),
      `Subject: =?UTF-8?B?${btoa(unescape(encodeURIComponent(to.split("@")[0] ? subject : subject)))}?=`,
      `MIME-Version: 1.0`,
      `Content-Type: multipart/mixed; boundary="${boundary}"`,
      ``,
      `--${boundary}`,
      `Content-Type: text/html; charset=UTF-8`,
      `Content-Transfer-Encoding: base64`,
      ``,
      bodyB64,
    ];
    if (attachment) {
      lines.push(
        `--${boundary}`,
        `Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`,
        `Content-Transfer-Encoding: base64`,
        `Content-Disposition: attachment; filename="${attachment.filename}"`,
        ``,
        attachment.base64.match(/.{1,76}/g).join("\r\n"),
      );
    }
    lines.push(`--${boundary}--`);
    const raw = lines.join("\r\n");
    return btoa(unescape(encodeURIComponent(raw))).replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/,"");
  };

  // Build Excel attachment base64 for a row
  const buildAttachmentForRow = (row, periodLabel, today, activeQtrs, datasets, curCompany, selYear, emailConfig) => {
    try {
      const name = row.as_name||row.bk_name||"Deductor";
      const txns26 = datasets["26AS"].filter(r=>r.tan===row.tan);
      const txnsBk = datasets["Books"].filter(r=>r.tan===row.tan);
      const rel26 = activeQtrs.size===0 ? txns26 : txns26.filter(r=>activeQtrs.has(r.quarter));
      const relBk  = activeQtrs.size===0 ? txnsBk : txnsBk.filter(r=>activeQtrs.has(r.quarter));
      const pendingAmt = Math.max(0,(row.bk_tds||0)-(row.as_tds||0));
      const fmtAmt = (v) => v==null||v===""?"":Number(v).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});

      // Style definitions matching target format (navy blue header: 002060, white bold text)
      const NAVY = "FF002060";
      const WHITE = "FFFFFFFF";
      const navyHdrStyle = {
        font: { bold: true, sz: 12, color: { rgb: WHITE } },
        fill: { patternType: "solid", fgColor: { rgb: NAVY } },
        alignment: { horizontal: "left", vertical: "center" }
      };
      const boldStyle = { font: { bold: true, sz: 12 } };
      const normalStyle = { font: { sz: 12 } };

      // Helper: apply style to a range of cells in a worksheet (row/col are 0-indexed)
      const styleCell = (ws, r, c, style) => {
        const addr = XLSX.utils.encode_cell({r, c});
        if (!ws[addr]) ws[addr] = { t: "s", v: "" };
        ws[addr].s = style;
      };
      const styleCellRange = (ws, r, cStart, cEnd, style) => {
        for (let c = cStart; c <= cEnd; c++) styleCell(ws, r, c, style);
      };

      const wb = XLSX.utils.book_new();

      // ── SHEET 1: SUMMARY ──────────────────────────────────────────────────────
      // Rows 1-2: navy bg + bold + white text; rest: normal
      const titleText = "TDS RECONCILIATION STATEMENT — "+periodLabel;
      const genText   = "Generated on: "+today+(emailConfig.ourName?"   |   Prepared by: "+emailConfig.ourName:"");
      const wsSummary = XLSX.utils.aoa_to_sheet([
        [titleText, ""],           // row 0 — navy header
        [genText,   ""],           // row 1 — navy header
        [""],                      // row 2 — blank
        ["── DEDUCTOR DETAILS ─────────────────────────────────────"],  // row 3
        ["Company Name",  curCompany?.name||"—"],   // row 4
        ["Deductor Name", name],                    // row 5
        ["TAN",           row.tan],                 // row 6
        ["Period",        periodLabel],             // row 7
        ["Match Status",  row.matchStatus],         // row 8
        [""],                                       // row 9 — blank
        ["── RECONCILIATION SUMMARY ───────────────────────────────"],  // row 10
        ["TDS as per Books", fmtAmt(row.bk_tds)],  // row 11
        ["TDS as per 26AS",  fmtAmt(row.as_tds)],  // row 12
        ["Pending Amount",   fmtAmt(pendingAmt)],   // row 13
        [""],                                       // row 14 — blank
        ["Note: See '26AS Entries' and 'Books Entries' sheets for transaction-level details."], // row 15
      ]);
      wsSummary["!cols"] = [{wch:30},{wch:42}];
      wsSummary["!merges"] = [{s:{r:0,c:0},e:{r:0,c:1}},{s:{r:1,c:0},e:{r:1,c:1}}];
      // Apply navy style to rows 0 and 1 (cols 0-1)
      styleCellRange(wsSummary, 0, 0, 1, navyHdrStyle);
      styleCellRange(wsSummary, 1, 0, 1, navyHdrStyle);
      XLSX.utils.book_append_sheet(wb, wsSummary, "Summary");

      // ── SHEET 2: 26AS ENTRIES ─────────────────────────────────────────────────
      const total26Ded = rel26.reduce((s,r)=>s+(r.tdsDeducted||0),0);
      const total26Dep = rel26.reduce((s,r)=>s+(r.tdsDeposited||r.tdsDeducted||0),0);
      const hdr26 = ["S No","Date","Quarter","Section","Amount Paid","TDS Deducted","TDS Deposited","Booking Status"];
      const data26Rows = rel26.map((r,i)=>[i+1, r.date||"", r.quarter||"", r.section||"", r.amountPaid||0, r.tdsDeducted||0, r.tdsDeposited||r.tdsDeducted||0, r.bookingStatus||""]);
      const ws26 = XLSX.utils.aoa_to_sheet([
        ["26AS ENTRIES", ...Array(7).fill("")],              // row 0 — bold title
        ["Deductor: "+name+"   |   TAN: "+row.tan+"   |   Period: "+periodLabel, ...Array(7).fill("")], // row 1 — bold subtitle
        [""],                                                // row 2 — blank
        hdr26,                                              // row 3 — navy header
        ...data26Rows,                                      // row 4+ — data
        ["","","","TOTAL (₹)","",fmtAmt(total26Ded),fmtAmt(total26Dep),""], // total row
      ]);
      ws26["!cols"]=[{wch:6},{wch:14},{wch:9},{wch:12},{wch:16},{wch:16},{wch:16},{wch:14}];
      ws26["!merges"]=[{s:{r:0,c:0},e:{r:0,c:7}},{s:{r:1,c:0},e:{r:1,c:7}}];
      // Bold title rows
      styleCellRange(ws26, 0, 0, 7, boldStyle);
      styleCellRange(ws26, 1, 0, 7, boldStyle);
      // Navy header row (row 3)
      hdr26.forEach((_,ci) => styleCell(ws26, 3, ci, navyHdrStyle));
      XLSX.utils.book_append_sheet(wb, ws26, "26AS Entries");

      // ── SHEET 3: BOOKS ENTRIES ────────────────────────────────────────────────
      // Target: S No, Date, Invoice Date, Quarter, Invoice No, TDS Amount
      const totalBkTDS = relBk.reduce((s,r)=>s+(r.tdsDeducted||0),0);
      const hdrBk = ["S No","Date","Invoice Date","Quarter","Invoice No","TDS Amount"];
      const dataBkRows = relBk.map((r,i)=>[i+1, r.date||"", r.invoiceDate||"", r.quarter||"", r.invoiceNo||"", r.tdsDeducted||0]);
      const wsBk = XLSX.utils.aoa_to_sheet([
        ["BOOKS ENTRIES", ...Array(5).fill("")],             // row 0 — bold title
        ["Deductor: "+name+"   |   TAN: "+row.tan+"   |   Period: "+periodLabel, ...Array(5).fill("")], // row 1 — bold subtitle
        [""],                                                // row 2 — blank
        hdrBk,                                              // row 3 — navy header
        ...dataBkRows,                                      // row 4+ — data
        ["","","","","TOTAL (₹)", totalBkTDS],                 // total row — numeric value
      ]);
      wsBk["!cols"]=[{wch:6},{wch:14},{wch:14},{wch:9},{wch:24},{wch:16}];
      wsBk["!merges"]=[{s:{r:0,c:0},e:{r:0,c:5}},{s:{r:1,c:0},e:{r:1,c:5}}];
      // Bold title rows
      styleCellRange(wsBk, 0, 0, 5, boldStyle);
      styleCellRange(wsBk, 1, 0, 5, boldStyle);
      // Navy header row (row 3)
      hdrBk.forEach((_,ci) => styleCell(wsBk, 3, ci, navyHdrStyle));
      XLSX.utils.book_append_sheet(wb, wsBk, "Books Entries");

      const buf = XLSX.write(wb, {bookType:"xlsx", type:"base64", cellStyles:true});
      const safeName = (name||row.tan).replace(/[^a-zA-Z0-9_]/g,"_").slice(0,30);
      return { base64: buf, filename: `TDS_Attachment_${safeName}_${row.tan}.xlsx` };
    } catch(e) { return null; }
  };

  const sendSingleEmail = async (to, subject, body, token, attachment, cc) => {
    const fromEmail = gmailUser?.email || emailConfig.ourEmail || "";
    const raw = buildRawEmail(to, subject, body, fromEmail, attachment, cc||"");
    const res = await fetch("https://www.googleapis.com/gmail/v1/users/me/messages/send",{
      method:"POST",
      headers:{ Authorization:"Bearer "+token.access_token, "Content-Type":"application/json" },
      body: JSON.stringify({raw})
    });
    if(!res.ok){
      const err = await res.json();
      throw new Error(err?.error?.message || res.statusText);
    }
    const data = await res.json();
    // Return both threadId and id (messageId) for tracking
    return { messageId: data.id, threadId: data.threadId };
  };

  const sendViaGmail = async (rows, overrideToken) => {
    const token = overrideToken || gmailToken;
    if(!token || token.expires_at <= Date.now()){
      pendingSendQueue.current = rows;
      connectGmail(); return;
    }
    // Always refresh dueDate to today+10 at send time
    const due = new Date(); due.setDate(due.getDate() + 10);
    setEmailConfig(prev => ({...prev, dueDate: due.toISOString().slice(0,10)}));
    // Build merged CC map from TAN Master
    const tanMasterCCMap = {};
    tanMaster.forEach(r=>{ if(r.ccEmail?.includes("@")) tanMasterCCMap[r.tan]=r.ccEmail; });
    const mergedCCs = {...tanMasterCCMap, ...tanCCs};
    
    setGmailSending(true);
    setGmailSendProgress({done:0,total:rows.length,errors:[]});
    const errors = [];
    const newLogEntries = [];
    for(let i=0;i<rows.length;i++){
      const row = rows[i];
      const to = tanEmails[row.tan];
      const sentAt = new Date().toISOString();
      if(!to?.includes("@")){
        errors.push({tan:row.tan,msg:"No email address"});
        setGmailSendProgress(p=>({...p,done:p.done+1,errors:[...p.errors,{tan:row.tan,msg:"No email"}]}));
        newLogEntries.push({ id:Date.now().toString(36)+i, tan:row.tan, name:row.as_name||row.bk_name||"—", to:"—", subject:"—", sentAt, status:"Failed", failReason:"No email address", threadId:null, messageId:null, pendingAmt:Math.abs(row.tds_diff||0), fy:selYear, company:curCompany?.name||"" });
        continue;
      }
      const subject = (emailConfig.subject||"TDS Pending-FY "+selYear+"")+(emailConfig.refNo?" (Ref: "+emailConfig.refNo+")":"");
      const body = generateEmailBodyRef.current(row);
      const attachment = attachmentBuilderRef.current ? attachmentBuilderRef.current(row) : null;
      const rowCC = mergedCCs[row.tan]?.trim() || "";
      try {
        const {messageId,threadId} = await sendSingleEmail(to, subject, body, token, attachment, rowCC);
        setGmailSendProgress(p=>({...p,done:p.done+1}));
        newLogEntries.push({ id:Date.now().toString(36)+i, tan:row.tan, name:row.as_name||row.bk_name||"—", to, subject, sentAt, status:"Sent", threadId, messageId, pendingAmt:Math.abs(row.tds_diff||0), fy:selYear, company:curCompany?.name||"", openedAt:null, repliedAt:null, lastChecked:null });
      } catch(e){
        errors.push({tan:row.tan,msg:e.message});
        setGmailSendProgress(p=>({...p,done:p.done+1,errors:[...p.errors,{tan:row.tan,msg:e.message}]}));
        newLogEntries.push({ id:Date.now().toString(36)+i, tan:row.tan, name:row.as_name||row.bk_name||"—", to, subject, sentAt, status:"Failed", failReason:e.message, threadId:null, messageId:null, pendingAmt:Math.abs(row.tds_diff||0), fy:selYear, company:curCompany?.name||"" });
      }
      if(i<rows.length-1) await new Promise(r=>setTimeout(r,400));
    }
    // Prepend new entries to log (newest first)
    if(newLogEntries.length>0) setEmailLog(prev=>[...newLogEntries,...prev].slice(0,1000));
    setGmailSending(false);
    if(errors.length===0) showToast(`✅ ${rows.length} email(s) sent via Gmail`,"s");
    else showToast(`⚠️ ${rows.length-errors.length} sent, ${errors.length} failed`,"w");
  };

  // ref so generateEmailBody & attachmentBuilder are accessible inside sendViaGmail
  const generateEmailBodyRef = useRef(null);
  const attachmentBuilderRef = useRef(null);

  const fileInputRef = useRef();
  const searchRef = useRef();
  const saveTimerRef = useRef(null);
  const initialLoadDone = useRef(false);
  const [loadDone, setLoadDone] = useState(false);

  const addLog = (msg, type="i") => setLog(p => [...p.slice(-100), { msg, type, t: new Date().toLocaleTimeString() }]);
  const showToast = (msg, type="s", dur=3500) => { setToast({msg,type,id:Date.now()}); setTimeout(()=>setToast(null),dur); };
  const showToastRef = useRef(showToast);
  const addLogRef    = useRef(addLog);
  const tanMasterRef = useRef([]);
  tanMasterRef.current = tanMaster; // ← assign synchronously during render (never stale)
  useEffect(() => { showToastRef.current = showToast; });
  useEffect(() => { addLogRef.current    = addLog; });
  const totalRecords = Object.values(datasets).reduce((a,v)=>a+v.length,0);

  // ── LOAD FROM STORE ON MOUNT ────────────────────────────────────────────────
  useEffect(() => {
    async function loadSaved() {
      // loadFromStore goes directly to Firebase on web (no localStorage)
      try {
        const [savedCompanies, savedSelCompanyId, savedSelYear, savedTanEmails] = await Promise.all([
          loadFromStore('companies'),
          loadFromStore('selCompanyId'),
          loadFromStore('selYear'),
          loadFromStore('tanEmails'),
          loadFromStore('emailLog'),
        ]);
        if (savedCompanies && savedCompanies.length > 0) {
          setCompanies(savedCompanies);
          const total = savedCompanies.reduce((a,c)=>a+Object.values(c.years||{}).reduce((b,y)=>b+Object.values(y.datasets||{}).reduce((s,d)=>s+d.length,0),0),0);
          if (total > 0) { addLog(`💾 Restored ${total} records from previous session`, "s"); showToast(`Session restored — ${total} records`, "i", 4000); }
        }
        if (savedSelCompanyId) setSelCompanyId(savedSelCompanyId);
        if (savedSelYear) setSelYear(savedSelYear);
        if (savedCompanies) { setLastSaved("Previous session"); setStorageStatus("saved"); }
        if (savedTanEmails) setTanEmails(savedTanEmails);
        const savedCreds = await loadFromStore('tracesCredsMap');
        if (savedCreds) setTracesCredsMap(savedCreds);
        const savedEmailLog = await loadFromStore('emailLog');
        if (savedEmailLog) setEmailLog(savedEmailLog);
        // Load local backup folder path
        const savedLocalBackupFolder = await loadFromStore('localBackupFolder');
        if (savedLocalBackupFolder) setLocalBackupFolder(savedLocalBackupFolder);
        // Load Drive index from electron-store (reliable across restarts, unlike localStorage)
        const savedDriveIndex = await loadFromStore('driveBackupIndex');
        if (savedDriveIndex?.length) { setDriveBackupIndex(savedDriveIndex); driveBackupIndexRef.current = savedDriveIndex; }
        const savedDriveFolderId = await loadFromStore('driveFolderId');
        if (savedDriveFolderId) { setDriveFolderId(savedDriveFolderId); driveFolderIdRef.current = savedDriveFolderId; }
        // Load Odoo references
        const savedOdooRefs = await loadFromStore('odooRefs');
        if (savedOdooRefs) setOdooRefs(savedOdooRefs);
        const savedOdooLog = await loadFromStore('odooLog');
        if (savedOdooLog) setOdooLog(savedOdooLog);
      } catch (e) { console.warn('Failed to load saved data:', e); }
      initialLoadDone.current = true;
      setLoadDone(true);
    }
    loadSaved();
  }, []);

  // ── AUTO DRIVE SYNC ON STARTUP ──────────────────────────────────────────────
  // Runs once per session, 4s after mount. pullDriveSync handles its own token.
  useEffect(() => {
    if (!driveEnabled || driveSyncRanThisSession.current) return;
    const timer = setTimeout(() => {
      if (driveSyncRanThisSession.current) return;
      driveSyncRanThisSession.current = true;
      // Pass fresh token if available — pullDriveSync will refresh if expired
      pullDriveSync(isDriveConnected ? driveToken : undefined);
    }, 4000);
    return () => clearTimeout(timer);
  // eslint-disable-next-line
  }, [driveEnabled]);

  // ── AUTO DRIVE BACKUP ON STARTUP ────────────────────────────────────────────
  // Fires once per session, 3s after data loads.
  // If token is fresh → backup immediately.
  // If token expired → silently get a new one via refresh token (no popup), then backup.
  // If never connected → does nothing.
  useEffect(() => {
    if (!driveEnabled || driveBackupRanThisSession.current) return;
    const timer = setTimeout(async () => {
      if (driveBackupRanThisSession.current) return;
      driveBackupRanThisSession.current = true;
      if (isDriveConnected) {
        runDriveBackup();
      } else if (gmailClientId) { // FIX: removed isElectron gate
        // Token expired — try silent refresh
        const freshToken = await silentlyRefreshDriveToken();
        if (freshToken) runDriveBackup(freshToken);
      }
    }, 3000);
    return () => clearTimeout(timer);
  // eslint-disable-next-line
  }, [driveEnabled]);

  // Keep refs current so async functions always read latest state (avoids stale closures)
  useEffect(() => { driveBackupRef.current = runDriveBackup; });
  useEffect(() => { companiesRef.current = companies; }, [companies]);
  useEffect(() => { driveTokenRef.current = driveToken; }, [driveToken]);
  useEffect(() => { driveBackupIndexRef.current = driveBackupIndex; }, [driveBackupIndex]);
  useEffect(() => { driveFolderIdRef.current = driveFolderId; }, [driveFolderId]);

  // ── 30-MINUTE INTERVAL BACKUP ───────────────────────────────────────────────
  useEffect(() => {
    if (!driveEnabled) return;
    const interval = setInterval(() => {
      if (driveBackupRef.current) driveBackupRef.current();
    }, 30 * 60 * 1000); // every 30 minutes
    return () => clearInterval(interval);
  }, [driveEnabled]);

  // ── AUTO-SAVE ON EVERY DATA CHANGE ─────────────────────────────────────────
  useEffect(() => {
    if (!loadDone) return;
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    setStorageStatus("saving");
    saveTimerRef.current = setTimeout(async () => {
      try {
        await Promise.all([
          saveToStore('companies', companies),
          saveToStore('selCompanyId', selCompanyId),
          saveToStore('selYear', selYear),
          saveToStore('tanEmails', tanEmails),
          saveToStore('emailLog', emailLog),
          saveToStore('odooLog', odooLog),
        ]);
        setStorageStatus("saved"); setLastSaved(new Date().toLocaleTimeString());
      } catch (e) { setStorageStatus("idle"); console.warn('Auto-save failed:', e); }
    }, 800);
    return () => clearTimeout(saveTimerRef.current);
  }, [companies, selCompanyId, selYear, emailLog, odooLog, loadDone]);

  const processFileDataRef = useRef(null);

  const processFileData = useCallback(async (name, ext, content, isBase64, size, fromTraces=false) => {
    const toast = (...a) => showToastRef.current(...a);
    const log   = (...a) => addLogRef.current(...a);
    setImporting(true); setProgress(10);
    log(`📂 Reading: ${name}`);
    await new Promise(r => setTimeout(r, 200));
    setProgress(40);
    try {
      let parsed = []; let category = "";
      if (ext === "txt") { parsed = parse26ASTxt(content); category = "26AS"; log(`✅ 26AS TRACES text parsed — ${parsed.length} transactions`, "s"); }
      else if (ext === "xml") { parsed = parse26ASXML(content); category = "26AS"; log(`✅ XML parsed — ${parsed.length} entries`, "s"); }
      else if (ext === "csv") {
        parsed = parseCSVFile(content, name);
        const isAIS = name.toLowerCase().includes("ais") || name.toLowerCase().includes("tis");
        category = isAIS ? "AIS" : name.toLowerCase().includes("26as") ? "26AS" : "Books";
        log(`✅ CSV parsed — ${parsed.length} records → ${category}`, "s");
      } else if (ext === "xlsx" || ext === "xls") {
        log("📗 Excel converting...", "i");
        const csvText = parseExcelFile(content);
        if (csvText) { parsed = parseCSVFile(csvText, name); category = name.toLowerCase().includes("ais")?"AIS":name.toLowerCase().includes("26as")?"26AS":"Books"; log(`✅ Excel → ${parsed.length} records → ${category}`, "s"); }
        else { log("⚠️ Excel failed — save as CSV", "w"); toast("Save Excel as CSV for best results","w"); setImporting(false); setProgress(0); return; }
      } else if (ext === "zip") {
        log("📦 ZIP detected — extracting...", "i");
        try {
          // Convert to Uint8Array safely — never use atob() on Electron binary strings
          // because Electron fs.readFile returns a Buffer that may have chars > 255 when
          // stringified. We use charCodeAt & 0xFF to clamp safely in all cases.
          let u8;
          if (isBase64 && typeof content === "string") {
            // Genuine base64 from FileReader (drag-drop in browser)
            try {
              const bin = atob(content);
              u8 = new Uint8Array(bin.length);
              for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i);
            } catch(e) {
              // atob failed — fall through to binary string path
              u8 = new Uint8Array(content.length);
              for (let i = 0; i < content.length; i++) u8[i] = content.charCodeAt(i) & 0xFF;
            }
          } else if (content instanceof Uint8Array) {
            u8 = content;
          } else if (content instanceof ArrayBuffer) {
            u8 = new Uint8Array(content);
          } else {
            // Binary string from Electron — clamp each char to 0-255
            u8 = new Uint8Array(content.length);
            for (let i = 0; i < content.length; i++) u8[i] = content.charCodeAt(i) & 0xFF;
          }
          const files = await extractZip(u8.buffer);
          const target = files.find(f => f.name.toLowerCase().endsWith(".txt")) ||
                         files.find(f => f.name.toLowerCase().endsWith(".csv"));
          if (!target) {
            log("❌ No .txt or .csv file found inside ZIP", "e");
            toast("ZIP has no 26AS .txt or .csv file inside", "e");
            setImporting(false); setProgress(0); return;
          }
          log(`📄 Extracted from ZIP: ${target.name}`, "i");
          const innerExt = target.name.split(".").pop().toLowerCase();
          setImporting(false); setProgress(0);
          await processFileData(target.name, innerExt, target.text, false, target.text.length, fromTraces);
          return;
        } catch(zipErr) {
          log(`❌ ZIP extraction failed: ${zipErr.message}`, "e");
          toast("ZIP extraction failed: " + zipErr.message, "e");
          setImporting(false); setProgress(0); return;
        }
      } else if (ext === "pdf") { log("⚠️ PDF not supported. Use 26AS .txt from TRACES.","w"); toast("Use 26AS .txt file from TRACES portal","w"); setImporting(false); setProgress(0); return; }
      else { log(`❌ Unsupported: .${ext}`,"e"); setImporting(false); setProgress(0); return; }
      if (!parsed.length) { log("⚠️ No records found — check file format","w"); toast("No records found — check file","w"); setImporting(false); setProgress(0); return; }
      setProgress(85); await new Promise(r=>setTimeout(r,150));

      if (category === "Books") {
        const master = tanMasterRef.current || [];
        if (master.length > 0) {
          const normName = s => (s||"").toUpperCase().replace(/[^A-Z0-9]/g,"").trim();
          const nameToTan = {};
          master.forEach(r => {
            // Include all known name variants: finalName, name26AS, nameBooks, plus bookNames array
            const allNames = [r.finalName, r.name26AS, r.nameBooks, ...(r.bookNames||[])].filter(Boolean);
            allNames.forEach(n => { const k=normName(n); if(k&&!nameToTan[k]) nameToTan[k]=r.tan; });
          });
          let enriched = 0;
          parsed = parsed.map(r => { if(r.tan) return r; const match=nameToTan[normName(r.deductorName)]; if(match){enriched++;return{...r,tan:match};} return r; });
          if(enriched>0) log(`🔗 Auto-filled TAN for ${enriched} Books row(s) from TAN Master`,"s");
          const stillMissing = parsed.filter(r=>!r.tan&&r.deductorName).length;
          if(stillMissing>0) log(`⚠️ ${stillMissing} Books row(s) still have no TAN`,"w");
        }
      }
      // 26AS and AIS always replace (they are full-year statements — re-importing = fresh data)
      // Books appends (multiple tally/SAP exports may be imported together)
      const shouldReplace = category === "26AS" || category === "AIS";
      setDatasets(prev => {
        const ex = shouldReplace ? [] : (prev[category]||[]);
        return {...prev, [category]: [...ex, ...parsed.map((r,i) => ({...r, id: ex.length+i+1}))]};
      });
      setFiles(prev => {
        const filtered = shouldReplace ? prev.filter(f => f.category !== category) : prev;
        return [...filtered, {id:Date.now(),name,ext,rows:parsed.length,size:size?(size/1024).toFixed(1)+" KB":"—",category,time:new Date().toLocaleTimeString(),source:fromTraces?"traces":"manual"}];
      });
      setProgress(100); log(`✅ Done: ${parsed.length} records → ${category}`,"s");
      toast(`Imported ${parsed.length} records from ${name}`);
      setSelDS(category); setView("data"); setReconDone(false);
      // Auto-backup after import — 2s delay so state settles
      setTimeout(() => driveBackupRef.current?.(), 2000);
    } catch(e) { addLogRef.current(`❌ Error: ${e.message}`,"e"); showToastRef.current(`Import failed: ${e.message}`,"e"); }
    await new Promise(r=>setTimeout(r,300));
    setImporting(false); setProgress(0);
  }, [setDatasets, setFiles, setReconDone]);

  // Keep ref always up-to-date so IPC listener can call latest version
  useEffect(() => { processFileDataRef.current = processFileData; }, [processFileData]);

  // ── ELECTRON IPC LISTENERS ──────────────────────────────────────────────────
  useEffect(() => {
    if (!isElectron) return;
    const c1 = window.electronAPI.onFileOpened(async (data) => {
      const isZip = (data.ext||"").toLowerCase() === "zip";
      processFileDataRef.current?.(data.name, data.ext, data.content, isZip ? true : data.isBase64, data.size);
    });
    const c2 = window.electronAPI.onMenuCommand(async (cmd) => {
      if (cmd==="export") exportCSV();
      if (cmd==="export-report") exportReconReport();
      if (cmd==="clear") handleClearAll();
      if (cmd==="reconcile") runRecon();
      if (cmd==="focus-search") searchRef.current?.focus();
    });
    const c3 = window.electronAPI.onMenuView((ds) => { setSelDS(ds); setView("data"); });
    const c4 = window.electronAPI.onTracesFileDetected?.((fileInfo) => {
      setTracesNewFiles(prev => {
        if (prev.some(f => f.path === fileInfo.path)) return prev;
        return [...prev, { ...fileInfo, detectedAt: Date.now(), id: Date.now().toString(36) }];
      });
      setTracesStatus("detected");
      setView("import");
      setImportTab("traces");
      // ── AUTO-IMPORT immediately — no button click needed ──────────────────
      if (isElectron && fileInfo.path) {
        showToast(`📥 Detected: ${fileInfo.name} — importing…`, "i", 4000);
        const isZip = (fileInfo.ext || "").toLowerCase() === "zip";
        // Retry up to 3× with 1s gap — file may still be writing when detected
        const tryRead = async (attempts = 3) => {
          try {
            // ZIP password from Client Master (ddmmyyyy format)
            const zipPwd = curCompany?.zipPassword || null;
            const result = await window.electronAPI.readDetectedFile(fileInfo.path, zipPwd);
            if (result?.content) {
              // ZIP returns extractedName + correct ext (e.g. .txt inside the ZIP)
              const useName = result.extractedName || fileInfo.name;
              const useExt  = result.ext || fileInfo.ext;
              processFileDataRef.current?.(useName, useExt, result.content, result.isBase64 || false, fileInfo.size, true);
            } else if (result?.error && result.error.includes('password')) {
              showToast(`🔐 ZIP password wrong — check ZIP Password in Client Master`, "e", 7000);
            } else if (attempts > 1) {
              setTimeout(() => tryRead(attempts - 1), 1200);
            } else {
              showToast(`❌ Could not read "${fileInfo.name}" — use "Import Now" button`, "e", 7000);
            }
          } catch(e) {
            if (attempts > 1) setTimeout(() => tryRead(attempts - 1), 1200);
            else showToast("Auto-import failed: " + e.message, "e");
          }
        };
        tryRead();
      } else {
        showToast(`📥 TRACES file detected: ${fileInfo.name}`, "i", 6000);
      }
    });
    return () => { c1?.(); c2?.(); c3?.(); c4?.(); };
  }, []); // ← empty deps: register once, always use processFileDataRef.current for latest fn

  // ── TRACES CLOSE HANDLER (shared, always works) ─────────────────────────────
  const closeTracesPortalFn = useCallback(async () => {
    if (tracesClosing) return;
    setTracesClosing(true);
    const fallback = setTimeout(() => {
      setTracesPortalOpen(false);
      setTracesStatus("watching");
      setTracesClosing(false);
    }, 2500);
    try {
      if (isElectron) await window.electronAPI.closeTracesPortal();
    } catch(e) {
      console.warn("closeTracesPortal error:", e);
    } finally {
      clearTimeout(fallback);
      setTracesPortalOpen(false);
      setTracesStatus("watching");
      setTracesClosing(false);
    }
  }, [tracesClosing]);

  // ── IT PORTAL CLOSE HANDLER ───────────────────────────────────────────────────
  const closeITPortalFn = useCallback(async () => {
    if (itPortalClosing) return;
    setItPortalClosing(true);
    const fallback = setTimeout(() => {
      setItPortalOpen(false);
      setItPortalStatus("idle");
      setItPortalClosing(false);
    }, 2500);
    try {
      if (isElectron) await window.electronAPI.closeTracesPortal(); // same BrowserView
    } catch(e) {
      console.warn("closeITPortal error:", e);
    } finally {
      clearTimeout(fallback);
      setItPortalOpen(false);
      setItPortalStatus("idle");
      setItPortalClosing(false);
    }
  }, [itPortalClosing]);

  // Escape key closes the portal from anywhere
  useEffect(() => {
    if (!tracesPortalOpen) return;
    const handler = (e) => { if (e.key === "Escape") closeTracesPortalFn(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [tracesPortalOpen, closeTracesPortalFn]);

  // Escape key closes IT Portal too
  useEffect(() => {
    if (!itPortalOpen) return;
    const handler = (e) => { if (e.key === "Escape") closeITPortalFn(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [itPortalOpen, closeITPortalFn]);

  const processFile = useCallback(async (file) => {
    const ext = file.name.split(".").pop().toLowerCase();
    if (ext === "zip") {
      const r = new FileReader();
      r.onload = e => processFileData(file.name, "zip", e.target.result.split(",")[1], true, file.size);
      r.readAsDataURL(file);
    } else if (ext==="xlsx"||ext==="xls") { const r=new FileReader(); r.onload=e=>processFileData(file.name,ext,e.target.result.split(",")[1],true,file.size); r.readAsDataURL(file); }
    else { const text=await file.text(); processFileData(file.name,ext,text,false,file.size); }
  }, [processFileData]);

  const openImport = async (exts) => { if (isElectron) await window.electronAPI.openFileDialog(exts); else fileInputRef.current?.click(); };

  // ── ODOO SYNC FUNCTION ────────────────────────────────────────────────────────
  // Open the Odoo sync modal with date picker
  const openOdooSyncModal = () => {
    // Find Odoo credentials
    let creds = null;
    if (curCompany.odooEnabled && curCompany.odooUrl) {
      creds = { url: curCompany.odooUrl, database: curCompany.odooDatabase, username: curCompany.odooUsername, password: curCompany.odooPassword };
    } else {
      const oc = companies.find(c => c.odooEnabled && c.odooUrl);
      if (oc) creds = { url: oc.odooUrl, database: oc.odooDatabase, username: oc.odooUsername, password: oc.odooPassword };
    }
    if (!creds) {
      showToast("Odoo integration not enabled. Configure Odoo credentials in any company's settings.", "w");
      return;
    }
    // Set default date range to full FY
    const [sy] = selYear.split('-');
    const ey = parseInt(sy) + 1;
    setOdooDateRange({ from: `${sy}-04-01`, to: `${ey}-03-31` });
    setOdooSyncStarted(false);
    setOdooSyncType('tds');
    setOdooSyncComplete(false);
    setOdooSyncProgress({ step: 'start', message: 'Initializing...', count: 0 });
    setShowOdooSyncModal(true);
  };

  const syncFromOdooERP = async () => {
    // Find Odoo credentials
    let odooCredentials = null;
    if (curCompany.odooEnabled && curCompany.odooUrl) {
      odooCredentials = { url: curCompany.odooUrl, database: curCompany.odooDatabase, username: curCompany.odooUsername, password: curCompany.odooPassword };
    } else {
      const odooCompany = companies.find(c => c.odooEnabled && c.odooUrl);
      if (odooCompany) {
        odooCredentials = { url: odooCompany.odooUrl, database: odooCompany.odooDatabase, username: odooCompany.odooUsername, password: odooCompany.odooPassword };
      }
    }
    
    if (!odooCredentials) {
      showToast("Odoo integration not enabled.", "w");
      return;
    }
    
    // Store config globally for push-to-odoo feature
    window.__odooConfig = {
      url: odooCredentials.url,
      database: odooCredentials.database,
      username: odooCredentials.username,
      password: odooCredentials.password
    };
    
    // Start sync
    setOdooSyncStarted(true);
    setOdooSyncComplete(false);
    setOdooSyncProgress({ step: 'start', message: 'Initializing...', count: 0 });
    
    try {
      // Use the selected date range
      const fyStart = odooDateRange.from;
      const fyEnd = odooDateRange.to;
      
      // Get company prefixes
      const normalized = curCompany.name.toLowerCase();
      let companyPrefixes = [];
      if (normalized.includes('ginni') || normalized.includes('gsl')) {
        companyPrefixes = ['SMH', 'SWB', 'STN', 'SHR', 'SKN', 'SOH'];
      } else if (normalized.includes('easemy') || normalized.includes('emg')) {
        companyPrefixes = ['SEM'];
      } else if (normalized.includes('browntape') || normalized.includes('bt')) {
        companyPrefixes = ['SBTE', 'SBTM', 'SBT'];
      }
      
      if (companyPrefixes.length === 0) {
        throw new Error(`No invoice prefixes configured for company: ${curCompany.name}`);
      }
      
      setOdooSyncProgress({ step: 'auth', message: 'Connecting to Odoo...', count: 0 });
      
      // Call Odoo sync (Electron: IPC, Web: server proxy)
      let result;
      if (isElectron) {
        result = await window.electronAPI.syncFromOdoo({
          url: odooCredentials.url,
          database: odooCredentials.database,
          username: odooCredentials.username,
          password: odooCredentials.password,
          fyStart,
          fyEnd,
          companyPrefixes,
          syncType: odooSyncType
        });
      } else {
        // Web: use server proxy — invoice sync uses its own endpoint
        setOdooSyncProgress({ step: 'auth', message: 'Connecting via server proxy...', count: 0 });
        const endpoint = odooSyncType === 'invoices' ? '/api/odoo/sync-invoices' : '/api/odoo/sync-tds';
        const proxyResult = await fetch(`${SERVER_BASE}${endpoint}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            url: odooCredentials.url,
            db: odooCredentials.database,
            username: odooCredentials.username,
            apiKey: odooCredentials.password,
            fyStart,
            fyEnd,
            tdsAccountCode: '231110',
            debtorAccountCode: '251000',
            prefixes: companyPrefixes.join(',')
          })
        }).then(r => r.json());
        if (proxyResult.ok) {
          result = { success: true, records: proxyResult.data };
        } else {
          result = { success: false, error: proxyResult.error };
        }
      }
      
      // Show debug log from main process
      if (result.debugLog) {
        console.log('[Odoo Debug Log]:');
        result.debugLog.forEach(msg => console.log('  [Odoo]', msg));
      }
      
      if (!result.success) {
        throw new Error(result.error || 'Odoo sync failed');
      }
      
      // Store Odoo config for push-to-odoo feature
      window.__odooConfig = {
        url: odooCredentials.url,
        database: odooCredentials.database,
        username: odooCredentials.username,
        password: odooCredentials.password
      };
      
      const odooRecords = result.records || [];
      
      // ── INVOICE SYNC: Update existing Books entries ──
      if (odooSyncType === 'invoices') {
        setOdooSyncProgress({ step: 'search_complete', message: `Found ${odooRecords.length} invoices`, count: odooRecords.length });
        
        if (odooRecords.length === 0) {
          showToast("No posted invoices found for this company and period", "w");
          setOdooSyncComplete(true);
          return;
        }
        
        if (!datasets["Books"] || datasets["Books"].length === 0) {
          showToast("Import Books (TDS) data first, then sync invoices to update dates", "w");
          setOdooSyncComplete(true);
          return;
        }
        
        setOdooSyncProgress({ step: 'transform', message: 'Matching invoices to Books...', count: odooRecords.length });
        
        // Build invoice lookup map (by invoice number)
        const normalizeInv = (s) => String(s||'').trim().toUpperCase().replace(/\s+/g, '');
        const invoiceMap = {};
        odooRecords.forEach(inv => {
          const key = normalizeInv(inv.invoiceNo);
          if (key) invoiceMap[key] = inv;
        });
        
        const getQ = (ds) => { if(!ds) return ''; const d=new Date(ds); if(isNaN(d)) return ''; const m=d.getMonth()+1; if(m>=4&&m<=6)return'Q1';if(m>=7&&m<=9)return'Q2';if(m>=10&&m<=12)return'Q3';return'Q4'; };
        
        let updated = 0, notFound = 0;
        const updatedBooks = datasets["Books"].map(row => {
          if (row.invoiceNo) {
            const key = normalizeInv(row.invoiceNo);
            const inv = invoiceMap[key];
            if (inv) {
              updated++;
              const newQuarter = getQ(inv.invoiceDate) || row.quarter;
              return {
                ...row,
                invoiceDate: inv.invoiceDate,
                quarter: newQuarter,
                amount: inv.amountUntaxed || row.amount,
                deductorName: row.deductorName || inv.partnerName
              };
            } else {
              notFound++;
            }
          }
          return row;
        });
        
        // Save updated Books AND store raw invoice list for unbooked lookup
        setDatasets(prev => ({...prev, Books: updatedBooks, Invoices: odooRecords}));
        
        setOdooSyncProgress({ step: 'complete', message: `Updated ${updated} entries, ${notFound} not matched`, count: updated });
        setOdooSyncComplete(true);
        
        if (updated > 0) {
          showToast(`✅ ${updated} Books entries updated with invoice dates & amounts from Odoo`, "s");
          setSelDS("Books");
          setView("data");
        } else {
          showToast(`⚠️ No invoice matches found. Check that Books has matching invoice numbers.`, "w");
        }
        
        console.log(`[Odoo Invoice] Updated: ${updated}, Not matched: ${notFound}, Total invoices from Odoo: ${odooRecords.length}`);
        return;
      }
      
      setOdooSyncProgress({ step: 'search_complete', message: `Found ${odooRecords.length} records`, count: odooRecords.length });
      
      if (odooRecords.length === 0) {
        showToast("No TDS records found for this company and FY", "w");
        setOdooSyncComplete(true);
        return;
      }
      
      setOdooSyncProgress({ step: 'transform', message: 'Transforming data...', count: odooRecords.length });
      
      // Transform Odoo records to Books format
      const calculateQuarter = (dateStr) => {
        const date = new Date(dateStr);
        const month = date.getMonth() + 1;
        if (month >= 4 && month <= 6) return 'Q1';
        if (month >= 7 && month <= 9) return 'Q2';
        if (month >= 10 && month <= 12) return 'Q3';
        return 'Q4';
      };
      
      const booksData = odooRecords.map(record => ({
        deductorName: record.deductorName || '',
        tan: record.tan || '',
        amount: record.amount || 0,
        tdsDeducted: record.tdsDeducted || 0,
        section: record.section || '',
        date: record.date || '',
        invoiceNo: record.invoiceNo || '',
        quarter: record.quarter || calculateQuarter(record.date),
        source: 'Odoo ERP',
        journalEntry: record.journalEntry || '',
        odooCompany: record.odooCompany || ''
      }));
      
      // Apply TAN Master name→TAN enrichment (same as CSV import path)
      const master = tanMasterRef.current || [];
      let enrichedData = booksData;
      if (master.length > 0) {
        const normName = s => (s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').trim();
        const nameToTan = {};
        master.forEach(r => {
          const allNames = [r.finalName, r.name26AS, r.nameBooks, ...(r.bookNames || [])].filter(Boolean);
          allNames.forEach(n => { const k = normName(n); if (k && !nameToTan[k]) nameToTan[k] = r.tan; });
        });
        let enrichedCount = 0;
        enrichedData = booksData.map(row => {
          if (row.tan) return row;
          const match = nameToTan[normName(row.deductorName)];
          if (match) { enrichedCount++; return { ...row, tan: match }; }
          return row;
        });
        if (enrichedCount > 0) addLog(`🔗 Auto-filled TAN for ${enrichedCount} Odoo Books row(s) from TAN Master`, 's');
        const stillMissing = enrichedData.filter(r => !r.tan && r.deductorName).length;
        if (stillMissing > 0) addLog(`⚠️ ${stillMissing} Odoo Books row(s) still have no TAN — use TAN Master to map`, 'w');
      }

      // Process data same as CSV import
      const processed = enrichedData.map((row, idx) => ({
        id: Date.now() + idx,
        ...row
      }));
      
      // Update datasets
      setDatasets(prev => ({...prev, Books: processed}));
      
      // Add to files list
      const newFile = {
        id: Date.now(),
        name: `Odoo_Sync_${new Date().toISOString().slice(0,10)}.json`,
        category: "Books",
        uploadedAt: new Date().toLocaleString(),
        records: processed.length,
        ext: "odoo"
      };
      setFiles(f => [...f, newFile]);
      
      setOdooSyncProgress({ step: 'complete', message: `✅ Synced ${booksData.length} records`, count: booksData.length });
      setOdooSyncComplete(true);
      showToast(`✅ Synced ${booksData.length} Books records from Odoo!`, "s");
      
    } catch (error) {
      console.error('Odoo sync error:', error);
      showToast(`Odoo sync failed: ${error.message}`, "e");
      setShowOdooSyncModal(false);
      setOdooSyncStarted(false);
    }
  };


  // Duplicate TAN/Name detection
  const getDuplicates = (master) => {
    const issues = [];
    // 1. Name mismatch: same TAN, different names in 26AS vs Books
    master.forEach(r => {
      if(r.name26AS && r.nameBooks) {
        const n1 = r.name26AS.toUpperCase().replace(/[^A-Z0-9]/g,"");
        const n2 = r.nameBooks.toUpperCase().replace(/[^A-Z0-9]/g,"");
        if(n1 && n2 && n1!==n2) {
          issues.push({ type:"name_mismatch", tan:r.tan, label:"Name Mismatch", detail:`26AS: "${r.name26AS}" vs Books: "${r.nameBooks}"`, severity:"warn" });
        }
      }
    });
    // 2. Duplicate TAN: same TAN appearing more than once
    const tanCount = {};
    master.forEach(r => { tanCount[r.tan]=(tanCount[r.tan]||0)+1; });
    Object.entries(tanCount).forEach(([tan,count]) => {
      if(count>1) issues.push({ type:"dup_tan", tan, label:"Duplicate TAN", detail:`TAN appears ${count} times`, severity:"error" });
    });
    // 3. Same deductor name, different TANs (possible wrong TAN entry)
    const nameMap = {};
    master.forEach(r => {
      const norm = (r.finalName||r.name26AS||r.nameBooks||"").toUpperCase().replace(/[^A-Z0-9 ]/g,"").trim();
      if(!norm) return;
      if(!nameMap[norm]) nameMap[norm]=[];
      nameMap[norm].push(r.tan);
    });
    Object.entries(nameMap).forEach(([name, tans]) => {
      if(tans.length>1) issues.push({ type:"dup_name", tan:tans.join(", "), label:"Multiple TANs — Same Name", detail:`"${name}" has TANs: ${tans.join(", ")}`, severity:"warn" });
    });
    return issues;
  };

  const buildTanMaster = () => {
    const norm = s => s?.toUpperCase().trim() || "";
    const map26 = {}, mapBk = {};
    datasets["26AS"].forEach(r => { const t = norm(r.tan); if (t && !map26[t]) map26[t] = r.deductorName?.trim() || ""; });
    datasets["Books"].forEach(r => { const t = norm(r.tan); if (t && !mapBk[t]) mapBk[t] = r.deductorName?.trim() || ""; });
    const newTANs = new Set([...Object.keys(map26), ...Object.keys(mapBk)]);
    
    console.log(`=== Building TAN Master ===`);
    console.log(`Existing TANs in TAN Master: ${tanMaster.length}`);
    console.log(`New TANs found in data: ${newTANs.size}`);
    
    setTanMaster(prev => {
      const prevMap = {}; 
      prev.forEach(r=>{prevMap[r.tan]=r;});
      
      // CRITICAL: Keep ALL existing TANs with ALL their data unchanged
      // Only add NEW TANs that don't exist yet
      const allTANs = new Set([...Object.keys(prevMap),...newTANs]);
      console.log(`Total TANs after merge: ${allTANs.size}`);
      
      const merged = [...allTANs].map(tan => {
        const existing = prevMap[tan];
        
        // If TAN already exists, keep it EXACTLY as is (preserve all manual edits)
        if (existing) {
          return existing;
        }
        
        // Only for NEW TANs, create entry from data
        const name26AS = map26[tan] || "";
        const nameBooks = mapBk[tan] || "";
        return {
          tan,
          name26AS,
          nameBooks,
          finalName: nameBooks || name26AS,
          contactEmail: "",
          ccEmail: "",
          csmName: ""
        };
      }).sort((a,b)=>a.tan.localeCompare(b.tan)).map((r,i)=>({...r,sno:i+1}));
      
      const newAdded = merged.length - prev.length;
      console.log(`Preserved existing TANs: ${prev.length}`);
      console.log(`New TANs added: ${newAdded}`);
      
      return merged;
    });
    
    showToast(`TAN Master rebuilt — ${tanMaster.length} existing TANs preserved, new TANs added from data`,"s");
    setView("tanmaster");
  };

  const updateFinalName = (tan, val) => {
    setTanMaster(prev => prev.map(r => r.tan === tan ? { ...r, finalName: val } : r));
  };

  const deleteTanRow = (tan) => {
    setTanMaster(prev => prev.filter(r => r.tan !== tan));
    showToast(`TAN ${tan} removed from TAN Master`, "s");
  };

  const updateTanContact = (tan, field, val) => {
    setTanMaster(prev => prev.map(r => r.tan === tan ? { ...r, [field]: val } : r));
    // If email updated, sync to tanEmails map so TDS Notice picks it up immediately
    if (field === "contactEmail") {
      setTanEmails(prev => ({ ...prev, [tan]: val }));
    }
    // If CC email updated, sync to tanCCs map
    if (field === "ccEmail") {
      setTanCCs(prev => ({ ...prev, [tan]: val }));
    }
  };

  const importTanEmailsFromExcel = (file) => {
    const ext = file.name.split(".").pop().toLowerCase();
    const process = (text) => {
      const result = Papa.parse(text, { header:true, skipEmptyLines:true, dynamicTyping:false, transformHeader:h=>h.trim() });
      const headers = result.meta.fields||[];
      const norm = h => h.toLowerCase().replace(/[^a-z0-9]/g,"");
      const findCol = (...keys) => headers.find(h => keys.some(k=>norm(h).includes(k)));
      const tanCol      = findCol("tan","tanno","tannum");
      const emailCol    = findCol("email","mail","contactemail","customeremail");
      const ccCol       = findCol("ccemail","ccmail","cc");
      const csmCol      = findCol("csm","csmname","customersuccess","csmnam");
      const nameCol     = findCol("finalname","name","deductor","party","vendor");
      const partnerCol  = findCol("odooid","odoopartner","partnerid","externalid","partnerexternalid","odoopartnerid");

      // Only TAN is required — all other columns are optional
      if(!tanCol){
        showToast(`TAN column not found. File has: ${headers.slice(0,6).join(", ")}${headers.length>6?" …":""}`, "e", 7000);
        return;
      }
      // Must have at least one data column to update
      if(!emailCol && !ccCol && !csmCol && !nameCol && !partnerCol){
        showToast(`No updatable columns found. Add at least one of: Email, CC Email, CSM Name, Final Name, Odoo Partner ID`, "e", 7000);
        return;
      }

      // Helper: split multi-email strings (comma or semicolon separated), clean invisible chars
      const splitEmails = raw => {
        return String(raw||"")
          .replace(/[\u2060\u200b\u00a0\n\r]/g," ") // strip invisible/control chars
          .split(/[,;]/)
          .map(e=>e.trim())
          .filter(e=>e.includes("@") && e.includes("."));
      };

      let imported=0, skipped=0, multiEmail=0, badTan=0;
      const emailUpdates = {};
      const ccUpdates = {};
      const tanMasterPatches = {};

      result.data.forEach(row => {
        const tanRaw = String(row[tanCol]||"").trim().toUpperCase()
          .replace(/[^A-Z0-9]/g,""); // strip spaces/special chars from TAN
        if(!tanRaw) { skipped++; return; }
        if(!/^[A-Z]{4}\d{5}[A-Z]$/.test(tanRaw)) { badTan++; skipped++; return; }
        const tan = tanRaw;

        const patch = {};

        if(emailCol) {
          const rawEmail = String(row[emailCol]||"");
          const emails = splitEmails(rawEmail);
          if(emails.length > 0) {
            const primary = emails[0];          // first email → contactEmail
            patch.contactEmail = primary;
            emailUpdates[tan] = primary;
            // extra emails from this cell → append to ccEmail (if no dedicated CC col)
            if(emails.length > 1 && !ccCol) {
              const extras = emails.slice(1).join(", ");
              patch.ccEmail = extras;
              ccUpdates[tan] = extras;
              multiEmail++;
            }
          }
        }

        if(ccCol) {
          const ccEmails = splitEmails(row[ccCol]);
          if(ccEmails.length > 0) {
            const ccVal = ccEmails.join(", ");
            patch.ccEmail = ccVal;
            ccUpdates[tan] = ccVal;
          }
        }

        if(csmCol) {
          const csm = String(row[csmCol]||"").replace(/[\u2060\u200b]/g,"").trim();
          if(csm) patch.csmName = csm;
        }
        if(nameCol) {
          const nm = String(row[nameCol]||"").trim();
          if(nm) patch.finalName = nm;
        }
        if(partnerCol) {
          const pid = String(row[partnerCol]||"").trim();
          if(pid) patch.odooPartnerId = pid;
        }

        if(Object.keys(patch).length === 0) { skipped++; return; }
        tanMasterPatches[tan] = patch;
        imported++;
      });

      if(!imported){ showToast(`No valid rows found${skipped?` · ${skipped} skipped (${badTan} invalid TAN)`:""}`, "w"); return; }

      // Apply all patches in one setTanMaster call
      setTanMaster(prev => prev.map(r => {
        const patch = tanMasterPatches[r.tan];
        return patch ? {...r, ...patch} : r;
      }));
      if(Object.keys(emailUpdates).length) setTanEmails(prev => ({...prev,...emailUpdates}));
      if(Object.keys(ccUpdates).length)    setTanCCs(prev => ({...prev,...ccUpdates}));

      const parts = [];
      if(emailCol)   parts.push("emails");
      if(ccCol)      parts.push("CC emails");
      if(csmCol)     parts.push("CSM names");
      if(nameCol)    parts.push("final names");
      if(partnerCol) parts.push("Odoo Partner IDs");
      const notes = [];
      if(multiEmail>0) notes.push(`${multiEmail} had multiple emails — extras saved to CC`);
      if(badTan>0)     notes.push(`${badTan} skipped (invalid TAN)`);
      showToast(`✅ ${imported} TANs updated (${parts.join(", ")})${skipped?" · "+skipped+" skipped":""}${notes.length?" — "+notes.join(", "):""}`, "s", 6000);
      addLog(`📧 Bulk import: ${imported} TANs updated (${parts.join(", ")}) from ${file.name}${notes.length?" — "+notes.join("; "):""}`, "s");
    };
    // Detect actual file type by magic bytes — user may save .xlsx as .csv
    const sniff = new FileReader();
    sniff.onload = e => {
      const arr = new Uint8Array(e.target.result);
      const isExcel = (arr[0]===0x50&&arr[1]===0x4B&&arr[2]===0x03&&arr[3]===0x04)||ext==="xlsx"||ext==="xls";
      if(isExcel) {
        const r2 = new FileReader();
        r2.onload = e2 => { const csv = parseExcelFile(e2.target.result.split(",")[1]); if(csv) process(csv); else showToast("Could not parse Excel file","e"); };
        r2.readAsDataURL(file);
      } else {
        file.text().then(process);
      }
    };
    sniff.readAsArrayBuffer(file.slice(0,4));
  };

  const importInvoiceDetails = (file) => {
    if(!datasets["Books"] || datasets["Books"].length === 0) {
      showToast("Please import Books data first before importing invoice details", "w");
      return;
    }
    
    const ext = file.name.split(".").pop().toLowerCase();
    const getQ = (ds) => { if(!ds) return ""; const d=new Date(ds); if(isNaN(d)) return ""; const m=d.getMonth()+1; if(m>=4&&m<=6)return"Q1";if(m>=7&&m<=9)return"Q2";if(m>=10&&m<=12)return"Q3";return"Q4"; };
    
    // Normalize invoice number - remove spaces, convert to uppercase, handle special chars
    const normalizeInvoiceNo = (invNo) => {
      return String(invNo||"").trim().toUpperCase().replace(/\s+/g,"");
    };
    
    const process = (text) => {
      const result = Papa.parse(text, { header:true, skipEmptyLines:true, dynamicTyping:false, transformHeader:h=>h.trim() });
      const headers = result.meta.fields||[];
      const norm = h => h.toLowerCase().replace(/[^a-z0-9]/g,"");
      const findCol = (...keys) => headers.find(h => keys.some(k=>norm(h).includes(k)));
      // IMPORTANT: Search for "number" BEFORE generic terms to avoid matching date columns
      const invoiceNoCol = findCol("number") || findCol("invoiceno","invoicenumber","invoice","billno","voucherno","refno","billnumber");
      const invoiceDateCol = findCol("invoicebilldate","invoicedate","invdate","billdate","docdate") || findCol("date");
      
      console.log("=== Invoice Import Debug ===");
      console.log("Headers found:", headers);
      console.log("Invoice No Column:", invoiceNoCol);
      console.log("Invoice Date Column:", invoiceDateCol);
      
      if(!invoiceNoCol||!invoiceDateCol){ 
        showToast(`Could not find Invoice No/Invoice Date columns. Found: ${headers.join(", ")}`,"e"); 
        return; 
      }
      
      let updated=0, notFound=0, totalInvoices=0;
      const invoiceMap = {};
      const invoiceMapNormalized = {}; // For normalized matching
      
      result.data.forEach(row => {
        const invoiceNo = String(row[invoiceNoCol]||"").trim();
        const invoiceDate = String(row[invoiceDateCol]||"").trim();
        if(invoiceNo && invoiceDate) {
          const normalized = normalizeInvoiceNo(invoiceNo);
          invoiceMap[invoiceNo] = invoiceDate; // Keep original
          invoiceMapNormalized[normalized] = invoiceDate; // Also store normalized
          totalInvoices++;
        }
      });
      
      console.log(`Loaded ${totalInvoices} invoices from file`);
      console.log("Sample invoice numbers (original):", Object.keys(invoiceMap).slice(0, 5));
      console.log("Sample invoice numbers (normalized):", Object.keys(invoiceMapNormalized).slice(0, 5));
      
      const booksInvoices = datasets["Books"].filter(r=>r.invoiceNo).map(r=>r.invoiceNo);
      console.log(`Books has ${booksInvoices.length} entries with invoice numbers`);
      console.log("Sample Books invoice numbers:", booksInvoices.slice(0, 5));
      console.log("Sample Books invoice numbers (normalized):", booksInvoices.slice(0, 5).map(normalizeInvoiceNo));
      
      // NEW: Show exact comparison for first invoice
      if(booksInvoices.length > 0 && Object.keys(invoiceMap).length > 0) {
        const firstBooksInv = booksInvoices[0];
        const firstSalesInv = Object.keys(invoiceMap)[0];
        console.log("=== DETAILED COMPARISON ===");
        console.log(`First Books invoice: "${firstBooksInv}" (length: ${firstBooksInv ? firstBooksInv.length : 0}, type: ${typeof firstBooksInv})`);
        console.log(`First Sales invoice: "${firstSalesInv}" (length: ${firstSalesInv ? firstSalesInv.length : 0}, type: ${typeof firstSalesInv})`);
        console.log(`Books normalized: "${normalizeInvoiceNo(firstBooksInv)}"`);
        console.log(`Sales normalized: "${normalizeInvoiceNo(firstSalesInv)}"`);
        console.log(`Exact match exists? ${invoiceMap[firstBooksInv] ? 'YES ✅' : 'NO ❌'}`);
        console.log(`Normalized match exists? ${invoiceMapNormalized[normalizeInvoiceNo(firstBooksInv)] ? 'YES ✅' : 'NO ❌'}`);
        
        // Check for SHR/25-26/0600 specifically
        const testInv = "SHR/25-26/0600";
        const hasExact = booksInvoices.includes(testInv);
        console.log(`\nTest invoice "${testInv}" in Books? ${hasExact ? 'YES ✅' : 'NO ❌'}`);
        if(hasExact) {
          console.log(`  Has exact match in Sales? ${invoiceMap[testInv] ? 'YES ✅ -> ' + invoiceMap[testInv] : 'NO ❌'}`);
          console.log(`  Has normalized match in Sales? ${invoiceMapNormalized[normalizeInvoiceNo(testInv)] ? 'YES ✅ -> ' + invoiceMapNormalized[normalizeInvoiceNo(testInv)] : 'NO ❌'}`);
        }
      }
      
      const updatedBooks = datasets["Books"].map(r => {
        if(r.invoiceNo) {
          // Try exact match first, then normalized match
          let invDate = invoiceMap[r.invoiceNo];
          if(!invDate) {
            const normalized = normalizeInvoiceNo(r.invoiceNo);
            invDate = invoiceMapNormalized[normalized];
          }
          
          if(invDate) {
            const newQuarter = getQ(invDate);
            updated++;
            if(updated <= 5) { // Log first 5 matches
              console.log(`✅ Matched: ${r.invoiceNo} -> ${invDate} -> ${newQuarter}`);
            }
            return { ...r, invoiceDate: invDate, quarter: newQuarter || r.quarter };
          } else {
            notFound++;
          }
        }
        return r;
      });
      
      console.log(`=== Final Result: ${updated} updated, ${notFound} not matched ===`);
      
      // Update the datasets with new Books data
      updateCurYear(yd => ({
        ...yd,
        datasets: { ...yd.datasets, Books: updatedBooks }
      }));
      
      // Force view refresh to Books data viewer to see changes
      if(updated > 0) {
        setSelDS("Books");
        setView("data");
        // Force a small delay to ensure state is updated
        setTimeout(() => {
          showToast(`✅ ${updated} invoice dates updated! Check the Invoice Date column.`, "s");
        }, 100);
      } else {
        showToast(`⚠️ No invoices matched. Check console (F12) for details.`, "w");
      }
      
      addLog(`📋 Invoice import: ${updated}/${totalInvoices} matched and updated. Quarter recalculated from invoice dates.`, updated > 0 ? "s" : "w");
    };
    if(ext==="xlsx"||ext==="xls") {
      const reader = new FileReader();
      reader.onload = e => { const csv = parseExcelFile(e.target.result.split(",")[1]); if(csv) process(csv); };
      reader.readAsDataURL(file);
    } else {
      file.text().then(process);
    }
  };

  const exportTanMaster = () => {
    if (!tanMaster.length) { showToast("Build TAN Master first", "w"); return; }
    const cols    = ["sno","tan","name26AS","nameBooks","finalName","contactEmail","ccEmail","csmName","odooPartnerId"];
    const headers = ["S.No","TAN","Name as per 26AS","Name as per Books","Final Name","Email","CC Email","CSM Name","Odoo Partner ID"];
    const wb = XLSX.utils.book_new();
    const rows = [headers, ...tanMaster.map(r => cols.map(c => r[c] ?? ""))];
    const ws = XLSX.utils.aoa_to_sheet(rows);
    ws["!cols"] = [6, 16, 30, 30, 30, 28, 28, 22, 42].map(w => ({ wch: w }));
    XLSX.utils.book_append_sheet(wb, ws, "TAN Master");
    const buf = XLSX.write(wb, { bookType: "xlsx", type: "array" });
    const blob = new Blob([buf], { type: "application/octet-stream" });
    if (isElectron) {
      const reader = new FileReader(); reader.readAsDataURL(blob);
      reader.onload = async () => { const b64 = reader.result.split(",")[1]; const res = await window.electronAPI.saveFile({ defaultName: "TAN_Master.xlsx", content: b64, isBase64: true }); if (res.success) showToast(`TAN Master exported: ${res.path}`); };
    } else {
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = "TAN_Master.xlsx"; a.click();
      showToast(`TAN Master exported — ${tanMaster.length} TANs`);
    }
  };

  const runRecon = () => {
    if (!datasets["26AS"].length) { showToast("Import 26AS data first","w"); return; }
    if (!datasets["Books"].length) { showToast("Import Books/Tally data first","w"); return; }
    // Snapshot TANs that were "Missing in Books" before this run — used to highlight newly-resolved entries
    const currentMib = reconResults.filter(r=>r.matchStatus==="Missing in Books").map(r=>r.tan);
    if (currentMib.length > 0) setPrevMissingInBooks(currentMib);
    const results = runMatchingEngine(datasets["26AS"], datasets["Books"]);
    setReconResults(results); setReconDone(true); setView("recon");
    const matched = results.filter(r=>r.matchStatus==="Matched").length;
    const mismatch = results.filter(r=>r.matchStatus==="Mismatch").length;
    showToast(`Reconciliation done: ${matched} matched, ${mismatch} mismatches`);
    // Auto-backup after reconciliation — results are now saved
    setTimeout(() => driveBackupRef.current?.(), 1500);
  };

  const handleClearAll = async () => {
    const has26 = (datasets["26AS"]||[]).length;
    const hasBooks = (datasets["Books"]||[]).length;
    const hasAIS = (datasets["AIS"]||[]).length;
    if (!has26 && !hasBooks && !hasAIS) return;

    // Build options based on what data exists
    const opts = [];
    if (hasBooks) opts.push("Books");
    if (hasAIS)   opts.push("AIS");
    if (has26)    opts.push("26AS");

    // Show custom modal instead of window.confirm
    setShowClearModal(true);
  };
  const [showClearModal, setShowClearModal] = useState(false);
  const [clearSelections, setClearSelections] = useState({books:true, ais:true, as26:false});
  const [showMissingTanModal, setShowMissingTanModal] = useState(false);
  const [missingTanEdits, setMissingTanEdits] = useState({});
  const [missingTanSearch, setMissingTanSearch] = useState("");

  const addManualTan = (partyName, tan) => {
    const normTAN = tan.trim().toUpperCase();
    if(!/^[A-Z]{4}\d{5}[A-Z]$/.test(normTAN)){ showToast("Invalid TAN format (e.g. ABCD12345E)","e"); return; }
    setTanMaster(prev => {
      const existing=prev.find(r=>r.tan===normTAN);
      if(existing) return prev.map(r=>{
        if(r.tan!==normTAN) return r;
        const bookNames=[...new Set([...(r.bookNames||[]),partyName])];
        return {...r,nameBooks:r.nameBooks||partyName,finalName:r.finalName||partyName,bookNames};
      }).map((r,i)=>({...r,sno:i+1}));
      return [...prev,{tan:normTAN,name26AS:"",nameBooks:partyName,finalName:partyName,bookNames:[partyName],contactEmail:"",ccEmail:"",csmName:"",odooPartnerId:""}].sort((a,b)=>a.tan.localeCompare(b.tan)).map((r,i)=>({...r,sno:i+1}));
    });
    setDatasets(prev=>({...prev,Books:(prev["Books"]||[]).map(r=>(!r.tan&&r.deductorName===partyName)?{...r,tan:normTAN}:r)}));
    setMissingTanEdits(prev=>({...prev,[partyName]:""}));
    setReconDone(false);
    showToast(`✅ TAN ${normTAN} mapped to "${partyName}" and SAVED`,"s");
  };

  const importMissingTanFromExcel = (file) => {
    const ext = file.name.split(".").pop().toLowerCase();
    const process = (text) => {
      const result = Papa.parse(text,{header:true,skipEmptyLines:true,dynamicTyping:false,transformHeader:h=>h.trim()});
      const headers = result.meta.fields||[];
      const norm = h=>h.toLowerCase().replace(/[^a-z0-9]/g,"");
      const findCol = (...keys)=>headers.find(h=>keys.some(k=>norm(h).includes(k)));
      const nameCol = findCol("party","deductor","name","partyname","deductorname");
      const tanCol  = findCol("tan","tanno","tannum","tanfill","fillhere");
      if(!nameCol||!tanCol){ showToast("Need 'Party Name' and 'TAN' columns in the file","e"); return; }
      let mapped=0, skipped=0, invalid=0;
      const booksPatch={}, tanMasterAdditions=[];
      result.data.forEach(row=>{
        const partyName=String(row[nameCol]||"").trim();
        const tan=String(row[tanCol]||"").trim().toUpperCase();
        if(!partyName) return;
        if(!tan){skipped++;return;}
        if(!/^[A-Z]{4}\d{5}[A-Z]$/.test(tan)){invalid++;return;}
        booksPatch[partyName]=tan; tanMasterAdditions.push({partyName,tan}); mapped++;
      });
      if(!mapped){ showToast(`No valid TAN mappings found${invalid?` · ${invalid} invalid format`:""}${skipped?` · ${skipped} blank`:""}`, "w"); return; }
      setDatasets(prev=>({...prev,Books:(prev["Books"]||[]).map(r=>(!r.tan&&r.deductorName&&booksPatch[r.deductorName])?{...r,tan:booksPatch[r.deductorName]}:r)}));
      setTanMaster(prev=>{
        let updated=[...prev];
        tanMasterAdditions.forEach(({partyName,tan})=>{
          const ex=updated.find(r=>r.tan===tan);
          if(ex) updated=updated.map(r=>{
            if(r.tan!==tan) return r;
            const bookNames=[...new Set([...(r.bookNames||[]),partyName])];
            return {...r,nameBooks:r.nameBooks||partyName,finalName:r.finalName||partyName,bookNames};
          });
          else updated=[...updated,{tan,name26AS:"",nameBooks:partyName,finalName:partyName,bookNames:[partyName],contactEmail:"",ccEmail:"",csmName:"",odooPartnerId:""}];
        });
        return updated.sort((a,b)=>a.tan.localeCompare(b.tan)).map((r,i)=>({...r,sno:i+1}));
      });
      setReconDone(false);
      showToast(`✅ ${mapped} TAN${mapped!==1?"s":""} mapped and SAVED to TAN Master${invalid?` · ${invalid} invalid skipped`:""}${skipped?` · ${skipped} blank skipped`:""}`, "s");
      addLog(`📥 Missing TAN import: ${mapped} parties mapped and saved to TAN Master from ${file.name}`,"s");
    };
    if(ext==="xlsx"||ext==="xls"){const r=new FileReader();r.onload=e=>{const csv=parseExcelFile(e.target.result.split(",")[1]);if(csv)process(csv);};r.readAsDataURL(file);}
    else{file.text().then(process);}
  };

  // ── BACKUP & RESTORE ────────────────────────────────────────────────────────
  const exportBackup = async (scope="year") => {
    // scope: "year" = current company+year only, "company" = all years of current company, "all" = everything
    let payload, defaultName;
    const ts = new Date().toISOString().slice(0,10);
    if (scope==="year") {
      payload = { version:2, scope:"year", company:curCompany.name, year:selYear, data: curYearData, exportedAt: new Date().toISOString() };
      defaultName = `Backup_${curCompany.name.replace(/\s+/g,"_")}_FY${selYear}_${ts}.json`;
    } else if (scope==="company") {
      payload = { version:2, scope:"company", company:curCompany.name, years: curCompany.years, exportedAt: new Date().toISOString() };
      defaultName = `Backup_${curCompany.name.replace(/\s+/g,"_")}_AllYears_${ts}.json`;
    } else {
      payload = { version:2, scope:"all", companies, exportedAt: new Date().toISOString() };
      defaultName = `Backup_AllCompanies_${ts}.json`;
    }
    const content = JSON.stringify(payload, null, 2);
    if (isElectron) {
      const res = await window.electronAPI.saveFile({ defaultName, content });
      if (res?.success) showToast(`Backup saved: ${res.path}`);
    } else {
      const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([content],{type:"application/json"})); a.download = defaultName; a.click();
      showToast(`Backup downloaded: ${defaultName}`);
    }
  };

  const importBackup = (file) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const payload = JSON.parse(e.target.result);
        if (!payload.version || payload.version < 2) { showToast("Old backup format not supported","e"); return; }
        if (payload.scope==="year") {
          // Restore single year into current company (or matching company)
          setCompanies(prev => prev.map(c => {
            if (c.name !== payload.company && c.id !== selCompanyId) return c;
            return {...c, years:{...c.years, [payload.year]: payload.data}};
          }));
          setSelYear(payload.year);
          showToast(`Restored ${payload.company} — FY ${payload.year}`);
        } else if (payload.scope==="company") {
          // Restore all years of a company
          setCompanies(prev => {
            const existing = prev.find(c=>c.name===payload.company);
            if (existing) return prev.map(c=>c.name===payload.company?{...c,years:{...c.years,...payload.years}}:c);
            const nc = mkCompany(payload.company); nc.years = payload.years;
            return [...prev, nc];
          });
          showToast(`Restored company: ${payload.company}`);
        } else if (payload.scope==="all") {
          if (!window.confirm("This will MERGE all companies from the backup into your current data. Continue?")) return;
          setCompanies(prev => {
            let updated = [...prev];
            payload.companies.forEach(bc => {
              const existing = updated.find(c=>c.name===bc.name);
              if (existing) updated = updated.map(c=>c.name===bc.name?{...c,years:{...c.years,...bc.years}}:c);
              else updated.push(bc);
            });
            return updated;
          });
          showToast(`Restored ${payload.companies.length} companies from backup`);
        }
      } catch(err) { showToast("Invalid backup file","e"); }
    };
    reader.readAsText(file);
  };

  const exportCSV = async () => {
    const data = datasets[selDS]; if (!data.length) { showToast("No data","w"); return; }
    const cols = ["id","deductorName","tan","section","amountPaid","tdsDeducted","tdsDeposited","date","quarter","financialYear","bookingStatus","matchStatus"];
    const csv = [cols.join(","), ...data.map(r=>cols.map(c=>`"${r[c]??""}"`)  .join(","))].join("\n");
    if (isElectron) { const res=await window.electronAPI.saveFile({defaultName:`26AS_${selDS}_Export.csv`,content:csv}); if(res.success) showToast(`Exported: ${res.path}`); }
    else { const a=document.createElement("a"); a.href=URL.createObjectURL(new Blob([csv],{type:"text/csv"})); a.download=`26AS_${selDS}_Export.csv`; a.click(); showToast(`Exported ${data.length} records`); }
  };

  const downloadTabExcel = async (tabName) => {
    const wb = XLSX.utils.book_new();
    const companyName = curCompany?.name || 'Export';
    const fy = selYear || '';

    if (tabName === '26AS' || tabName === 'AIS') {
      const data = datasets[tabName] || [];
      if (!data.length) { showToast(`No ${tabName} data to download`, 'w'); return; }
      const headers = ['#', 'Deductor Name', 'TAN', 'Section', 'Amount Paid', 'TDS Deducted', 'TDS Deposited', 'Trans. Date', 'Quarter', 'Financial Year', 'Booking Status', 'Match Status'];
      const rows = data.map((r, i) => [
        i + 1, r.deductorName || '', r.tan || '', r.section || '',
        r.amountPaid || 0, r.tdsDeducted || 0, r.tdsDeposited || 0,
        r.date || '', r.quarter || '', r.financialYear || '',
        r.bookingStatus || '', r.matchStatus || ''
      ]);
      const ws = XLSX.utils.aoa_to_sheet([headers, ...rows]);
      ws['!cols'] = [6, 28, 14, 10, 14, 14, 14, 12, 8, 12, 10, 14].map(w => ({ wch: w }));
      XLSX.utils.book_append_sheet(wb, ws, tabName);

    } else if (tabName === 'Books') {
      const data = datasets['Books'] || [];
      if (!data.length) { showToast('No Books data to download', 'w'); return; }
      const invLookup = {};
      (datasets['Invoices'] || []).forEach(inv => { const k = (inv.invoiceNo || '').trim().toUpperCase(); if (k) invLookup[k] = inv; });
      const headers = ['#', 'Party Name', 'TAN', 'Section', 'Amount Paid', 'TDS Deducted', 'Invoice No.', 'Trans. Date', 'Invoice Date', 'Taxable Value', 'Amt Due', 'TDS %', 'Odoo Ref', 'Quarter', 'Match Status'];
      const rows = data.map((r, i) => {
        const invKey = (r.invoiceNo || '').trim().toUpperCase();
        const inv = invLookup[invKey];
        const taxableVal = inv?.amountUntaxed || 0;
        const amtDue = inv?.amountDue ?? '';
        const tdsRate = taxableVal > 0 ? ((r.tdsDeducted || 0) / taxableVal * 100) : '';
        const refData = invKey ? (odooRefs[invKey] || null) : null;
        const odooRef = refData ? (refData.odooRef || `ID:${refData.moveId}`) : (r.journalEntry || '');
        return [
          i + 1, r.deductorName || '', r.tan || '', r.section || '',
          r.amountPaid || 0, r.tdsDeducted || 0, r.invoiceNo || '',
          r.date || '', r.invoiceDate || '', taxableVal || '', amtDue,
          tdsRate !== '' ? Math.round(tdsRate * 10) / 10 : '', odooRef,
          r.quarter || '', r.matchStatus || ''
        ];
      });
      const ws = XLSX.utils.aoa_to_sheet([headers, ...rows]);
      ws['!cols'] = [6, 28, 14, 10, 14, 14, 16, 12, 12, 14, 12, 8, 18, 8, 14].map(w => ({ wch: w }));
      XLSX.utils.book_append_sheet(wb, ws, 'Books');

    } else if (tabName === 'Invoices') {
      const data = datasets['Invoices'] || [];
      if (!data.length) { showToast('No Invoice data to download', 'w'); return; }
      const booksData = datasets['Books'] || [];
      const headers = ['S.No.', 'Name of Client', 'Invoice No.', 'Invoice Date', 'Taxable Value', 'Amount Due', 'Booked TDS', 'TDS %', 'Status', 'Odoo Ref'];
      const rows = data.map((inv, idx) => {
        const invNo = (inv.invoiceNo || '').trim().toUpperCase();
        const tdsBooked = booksData.filter(b => (b.invoiceNo || '').trim().toUpperCase() === invNo).reduce((s, b) => s + (b.tdsDeducted || 0), 0);
        const taxableVal = inv.amountUntaxed || 0;
        const tdsPercent = taxableVal > 0 ? ((tdsBooked / taxableVal) * 100) : 0;
        const isExcess = tdsBooked > taxableVal * 0.105;
        const hasNoTds = tdsBooked === 0;
        const status = hasNoTds ? 'No TDS' : isExcess ? 'Excess' : 'OK';
        const refData = odooRefs[invNo] || null;
        const odooRef = refData ? (refData.odooRef || `ID:${refData.moveId}`) : '';
        return [
          idx + 1, inv.partnerName || '', inv.invoiceNo || '', inv.invoiceDate || '',
          taxableVal, inv.amountDue || 0, tdsBooked,
          tdsBooked > 0 ? Math.round(tdsPercent * 10) / 10 : '',
          status, odooRef
        ];
      });
      const ws = XLSX.utils.aoa_to_sheet([headers, ...rows]);
      ws['!cols'] = [6, 28, 16, 12, 14, 14, 14, 8, 10, 18].map(w => ({ wch: w }));
      XLSX.utils.book_append_sheet(wb, ws, 'Invoices');
    }

    const buf = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([buf], { type: 'application/octet-stream' });
    const fileName = `${companyName.replace(/[^a-zA-Z0-9]/g, '_')}_${tabName}_FY${fy}.xlsx`;
    if (isElectron) {
      const reader = new FileReader(); reader.readAsDataURL(blob);
      reader.onload = async () => { const b64 = reader.result.split(',')[1]; const res = await window.electronAPI.saveFile({ defaultName: fileName, content: b64, isBase64: true }); if (res.success) showToast(`Exported: ${res.path}`); };
    } else {
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = fileName; a.click();
      showToast(`✅ ${tabName} exported — ${tabName === 'Invoices' ? (datasets['Invoices'] || []).length : (datasets[tabName] || []).length} records`, 's');
    }
  };

  const exportReconReport = async () => {
    if (!reconResults.length) { showToast("Run reconciliation first","w"); return; }
    const cols = ["id","tan","as_name","as_tds","as_deposited","as_txns","as_sections","bk_name","bk_tds","bk_txns","tds_diff","matchStatus","mismatchReason"];
    const headers = ["#","TAN","Deductor (26AS)","TDS (26AS)","TDS Deposited","Txns (26AS)","Sections","Party (Books)","TDS (Books)","Txns (Books)","TDS Diff","Status","Remark"];
    const wb = XLSX.utils.book_new();
    // If a specific status is selected, export only that filtered view as the first/primary sheet
    const activeFilter = selStatus !== "All" ? selStatus : null;
    const sheetsToExport = activeFilter
      ? [activeFilter, "All", ...["Matched","Near Match","Mismatch","Missing in Books","Missing in 26AS"].filter(s=>s!==activeFilter)]
      : ["All","Missing TAN","Matched","Near Match","Mismatch","Missing in Books","Missing in 26AS"];
    sheetsToExport.forEach(status => {
      const data = status==="All" ? liveResults : liveResults.filter(r=>r.matchStatus===status);
      if (!data.length && status!=="All" && status!==activeFilter) return;
      const rows = [headers, ...data.map(r=>cols.map(c=>r[c]??""))];
      const ws = XLSX.utils.aoa_to_sheet(rows);
      ws["!cols"] = [6,14,22,14,14,8,14,22,14,8,14,16,26].map(w=>({wch:w}));
      const sheetName = status==="All"?"All Results":status==="Missing in Books"?"Missing-Books":status==="Missing in 26AS"?"Missing-26AS":status;
      XLSX.utils.book_append_sheet(wb, ws, sheetName);
    });
    const buf = XLSX.write(wb, {bookType:"xlsx", type:"array"});
    const blob = new Blob([buf], {type:"application/octet-stream"});
    if (isElectron) {
      const reader = new FileReader(); reader.readAsDataURL(blob);
      reader.onload = async () => { const b64=reader.result.split(",")[1]; const res=await window.electronAPI.saveFile({defaultName:"26AS_Reconciliation_Report.xlsx",content:b64,isBase64:true}); if(res.success) showToast(`Report exported: ${res.path}`); };
    } else {
      const a=document.createElement("a"); a.href=URL.createObjectURL(blob); a.download="26AS_Reconciliation_Report.xlsx"; a.click();
      showToast(`Excel exported — ${reconResults.length} TANs`);
    }
  };

  const activeData = datasets[selDS]||[];
  // Compute duplicate invoice numbers for Books tab
  const dupInvoiceNos = (() => {
    if (selDS !== "Books") return new Set();
    const counts = {};
    (datasets["Books"]||[]).forEach(r => {
      const inv = (r.invoiceNo||"").trim().toUpperCase();
      if (!inv) return;
      counts[inv] = (counts[inv]||0) + 1;
    });
    return new Set(Object.keys(counts).filter(k => counts[k] > 1));
  })();
  // Invoice lookup map for Books tab extra columns
  const invMap = (() => {
    const m = {};
    (datasets["Invoices"]||[]).forEach(inv => {
      const k = (inv.invoiceNo||"").trim().toUpperCase();
      if (k) m[k] = inv;
    });
    return m;
  })();
  const filtered = activeData.filter(r=>{
    if (showDupOnly && selDS === "Books") {
      const inv = (r.invoiceNo||"").trim().toUpperCase();
      if (!inv || !dupInvoiceNos.has(inv)) return false;
    }
    return !searchQ||Object.values(r).some(v=>String(v).toLowerCase().includes(searchQ.toLowerCase()));
  });
  const sortedData = [...filtered].sort((a,b)=>{const va=a[sortCol]??"";const vb=b[sortCol]??"";const c=typeof va==="number"?va-vb:String(va).localeCompare(String(vb));return sortDir==="asc"?c:-c;});
  const toggleSort = col=>{if(sortCol===col)setSortDir(d=>d==="asc"?"desc":"asc");else{setSortCol(col);setSortDir("asc");}};
  const toggleRow = id=>setSelRows(p=>{const s=new Set(p);s.has(id)?s.delete(id):s.add(id);return s;});
  const toggleAll = ()=>setSelRows(p=>p.size===sortedData.length&&sortedData.length>0?new Set():new Set(sortedData.map(r=>r.id)));
  const deleteSelected = ()=>{setDatasets(prev=>({...prev,[selDS]:prev[selDS].filter(r=>!selRows.has(r.id))}));showToast(`${selRows.size} records deleted`);setSelRows(new Set());};

  const parseDate = s => { if (!s) return null; const d=new Date(s); return isNaN(d)?null:d; };
  const filterTxns = (arr) => arr.filter(r => {
    if (selQ !== "All" && r.quarter !== selQ) return false;
    if (dateFrom || dateTo) { const d = parseDate(r.date); if (d) { if (dateFrom && d < new Date(dateFrom)) return false; if (dateTo && d > new Date(dateTo + "T23:59:59")) return false; } }
    return true;
  });
  const liveResults = reconDone ? runMatchingEngine(filterTxns(datasets["26AS"]), filterTxns(datasets["Books"])) : [];
  // TANs that were previously Missing in Books but now have entries — for green highlight
  const resolvedTANs = new Set(
    prevMissingInBooks.filter(tan => liveResults.find(r => r.tan===tan && r.matchStatus!=="Missing in Books"))
  );
  const liveSectionResults = reconDone ? runSectionMatchingEngine(filterTxns(datasets["26AS"]), filterTxns(datasets["Books"])) : [];
  // All unique sections across both datasets
  const allSections = [...new Set([...datasets["26AS"],...datasets["Books"]].map(r=>r.section?.toUpperCase()?.trim()).filter(Boolean))].sort();
  const sectionFiltered = liveSectionResults.filter(r => {
    const ms = selStatus==="All"||r.matchStatus===selStatus;
    const mm = !mmOnly||r.matchStatus!=="Matched";
    const ss = selSection==="All"||r.section===selSection;
    const sm = !sectionSearch||[r.tan,r.name,r.section].some(v=>String(v||"").toLowerCase().includes(sectionSearch.toLowerCase()));
    return ms&&mm&&ss&&sm;
  });
  const reconFiltered = liveResults.filter(r => {
    const stm = selStatus==="All" || r.matchStatus===selStatus;
    const mm = !mmOnly || r.matchStatus!=="Matched";
    const sm = !reconSearch || [r.as_name, r.bk_name, r.tan].some(v => String(v||"").toLowerCase().includes(reconSearch.toLowerCase()));
    return stm && mm && sm;
  });
  const hasActiveFilter = selQ !== "All" || dateFrom || dateTo;
  const rs = { total:liveResults.length, matched:liveResults.filter(r=>r.matchStatus==="Matched").length, mismatch:liveResults.filter(r=>r.matchStatus==="Mismatch").length, mib:liveResults.filter(r=>r.matchStatus==="Missing in Books").length, mia:liveResults.filter(r=>r.matchStatus==="Missing in 26AS").length, mt:liveResults.filter(r=>r.matchStatus==="Missing TAN").length, tdsDiff: (() => {
    // Net difference: Books TDS - 26AS TDS (same formula as Summary Dashboard)
    // Positive = Books higher (over-booked), Negative = 26AS higher (short in Books)
    const as26  = liveResults.reduce((s,r) => s + (r.as_tds||0), 0);
    const books = liveResults.reduce((s,r) => s + (r.bk_tds||0), 0);
    return as26 - books; // 26AS - Books: positive = short in Books
  })() };
  const totalTDS = activeData.reduce((s,r)=>s+(r.tdsDeducted||0),0);
  const totalAmt = activeData.reduce((s,r)=>s+(r.amountPaid||0),0);
  const fmt = n => n?`₹${Number(n).toLocaleString("en-IN",{minimumFractionDigits:2})}`:"—";
  const FmtDiff = ({n}) => { if(!n||Math.abs(n)<0.01) return <span className="dz">—</span>; return n>0?<span className="dp">+₹{Math.abs(n).toLocaleString("en-IN",{minimumFractionDigits:2})}</span>:<span className="dn">-₹{Math.abs(n).toLocaleString("en-IN",{minimumFractionDigits:2})}</span>; };
  const getRC = s=>s==="Matched"?"row-m":s==="Mismatch"?"row-mm":s==="Near Match"?"row-nm":s==="Missing in Books"?"row-mib":s==="Missing TAN"?"row-mt":"row-mia";
  const getTag = s=>s==="Matched"?"tg-m":s==="Mismatch"?"tg-mm":s==="Near Match"?"tg-nm":s==="Missing in Books"?"tg-mib":s==="Missing TAN"?"tg-mt":"tg-mia";
  const navs = [{id:"dashboard",icon:I.chart,label:"Summary"},{id:"home",icon:I.home,label:"Client Master"},{id:"import",icon:I.import,label:"Import / TRACES",badge:tracesNewFiles.filter(f=>!tracesDismissed.has(f.id)).length||null},{id:"data",icon:I.grid,label:"Data Viewer",badge:totalRecords||null},{id:"recon",icon:I.recon,label:"Reconciliation",badge:reconDone?reconResults.length:null},{id:"tanmaster",icon:I.save,label:"TAN Master",badge:tanMaster.length||null},{id:"backup",icon:I.download,label:"Backup & Restore"},{id:"email",icon:I.mail,label:"TDS Notice",badge:reconDone?(rs.mismatch+rs.mib)||null:null},{id:"reports",icon:I.report,label:"Mismatch Report",badge:reconDone?rs.mismatch+rs.mib:null,soon:!reconDone},{id:"tracker",icon:I.tracker,label:"Email Tracker",badge:emailLog.length||null},{id:"odoolog",icon:I.save,label:"Push Log",badge:odooLog.length||null},{id:"settings",icon:I.settings,label:"Settings",soon:true}];

  return (
    <>
      <style>{css}</style>
      <div className="app">
        <div className="sb">
          <div className="sb-logo">
            <div className="sb-logo-ic"><Ic d={I.recon} s={15} c="#fff" sw={2}/></div>
            <div><div className="sb-logo-t">26AS Recon Suite</div><div className="sb-logo-s">Professional Edition</div></div>
          </div>
          <div className="sb-nav">
            <div className="sb-sec">Workspace</div>
            {navs.slice(0,7).map(n=>(
              <div key={n.id} className={`sb-it${view===n.id?" on":""}${n.soon?" dis":""}`} onClick={()=>!n.soon&&setView(n.id)}>
                <Ic d={n.icon} s={14} c={view===n.id?"#fff":"#999"}/><span>{n.label}</span>
                {n.badge!=null&&<span className="sb-bdg">{n.badge}</span>}
              </div>
            ))}
            <div className="sb-sec" style={{marginTop:7}}>Reports</div>
            {navs.slice(6).map(n=>(
              <div key={n.id} className={`sb-it${view===n.id?" on":""}${n.soon?" dis":""}`} onClick={()=>!n.soon&&setView(n.id)}>
                <Ic d={n.icon} s={14} c={view===n.id?"#fff":"#999"}/><span>{n.label}</span>
                {n.badge!=null&&<span className="sb-bdg" style={n.id==="reports"&&n.badge>0?{background:"var(--red)"}:{}}>{n.badge}</span>}
                {n.soon&&!reconDone&&<span className="sb-soon">SOON</span>}
              </div>
            ))}
          </div>
          <div className="sb-ft">
            {isWeb && (
              <div style={{display:"flex",gap:4,marginBottom:4}}>
                <button onClick={async()=>{
                  showToast("Saving to Firebase…","s");
                  try {
                    await Promise.all([
                      saveToStore('companies', companies),
                      saveToStore('selCompanyId', selCompanyId),
                      saveToStore('selYear', selYear),
                      saveToStore('tanEmails', tanEmails),
                      saveToStore('emailLog', emailLog),
                    ]);
                    showToast("✅ All data saved to Firebase","s");
                  } catch(e) { showToast("Save failed: "+e.message,"e"); }
                }} style={{flex:1,padding:"4px 0",fontSize:10,background:"#3a3a3a",color:"#aaa",border:"1px solid #555",borderRadius:3,cursor:"pointer",fontFamily:"inherit"}}>🔼 Push</button>
                <button onClick={()=>{showToast("Reloading from Firebase…","s");setTimeout(()=>location.reload(),500);}} style={{flex:1,padding:"4px 0",fontSize:10,background:"#3a3a3a",color:"#aaa",border:"1px solid #555",borderRadius:3,cursor:"pointer",fontFamily:"inherit"}}>🔽 Pull</button>
              </div>
            )}
            <div className="sb-ft-t" style={{color:"#3a8a3a",fontSize:9,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{storageStatus==="saved"&&totalRecords>0?`💾 ${totalRecords} records saved`:storageStatus==="saving"?"⏳ Saving…":"Ready"}</div>
          </div>
        </div>

        <div className="main">
          {/* COMPANY + YEAR DROPDOWNS IN TBAR */}
          <div className="tbar">
            <Ic d={I.recon} s={13} c="rgba(255,255,255,0.75)"/>
            <span className="tbar-t">26AS Recon Suite</span>
            <span className="tbar-s">›</span>
            {/* Company Dropdown */}
            <div style={{position:"relative",display:"flex",alignItems:"center",gap:5,background:"rgba(255,255,255,0.13)",borderRadius:3,padding:"2px 8px",cursor:"pointer"}}>
              <Ic d={I.home} s={11} c="rgba(255,255,255,0.7)"/>
              <select value={selCompanyId} onChange={e=>setSelCompanyId(e.target.value)} style={{background:"transparent",border:"none",color:"#fff",fontSize:12,fontFamily:"inherit",outline:"none",cursor:"pointer",appearance:"none",paddingRight:14,maxWidth:140}}>
                {companies.map(c=><option key={c.id} value={c.id} style={{background:"#2b2b2b",color:"#fff"}}>{c.name}</option>)}
              </select>
              <span style={{position:"absolute",right:5,color:"rgba(255,255,255,0.6)",fontSize:9,pointerEvents:"none"}}>▼</span>
            </div>
            {/* Year Dropdown */}
            <div style={{position:"relative",display:"flex",alignItems:"center",gap:5,background:"rgba(255,255,255,0.13)",borderRadius:3,padding:"2px 8px",cursor:"pointer"}}>
              <Ic d={I.chart} s={11} c="rgba(255,255,255,0.7)"/>
              <select value={selYear} onChange={e=>{setSelYear(e.target.value);ensureYear(selCompanyId,e.target.value);}} style={{background:"transparent",border:"none",color:"#fff",fontSize:12,fontFamily:"inherit",outline:"none",cursor:"pointer",appearance:"none",paddingRight:14}}>
                {FY_LIST.map(fy=>{
                  const hasData=curCompany?.years?.[fy]&&Object.values(curCompany.years[fy].datasets||{}).some(d=>d.length>0);
                  return <option key={fy} value={fy} style={{background:"#2b2b2b",color:"#fff"}}>{hasData?"● ":""} FY {fy}</option>;
                })}
              </select>
              <span style={{position:"absolute",right:5,color:"rgba(255,255,255,0.6)",fontSize:9,pointerEvents:"none"}}>▼</span>
            </div>
            {/* Add Company Button */}
            <button onClick={()=>{setNewCompanyName(`Company ${companies.length+1}`);setShowCompanyModal(true);}} style={{background:"rgba(255,255,255,0.1)",border:"1px solid rgba(255,255,255,0.2)",color:"rgba(255,255,255,0.8)",borderRadius:3,padding:"2px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit"}}>+ Company</button>
            <span className="tbar-s">›</span>
            <span className="tbar-t" style={{opacity:0.7}}>{{dashboard:"Summary Dashboard",home:"Client Master",import:"Import / TRACES",data:"Data Viewer",recon:"Reconciliation",tanmaster:"TAN Master",backup:"Backup & Restore",email:"TDS Pending Details",reports:"Mismatch Report",tracker:"Email Tracker",settings:"Settings"}[view]}</span>
          </div>
          <div className="cbar" style={{flexDirection:"column",alignItems:"stretch",padding:0,gap:0}}>
            {/* ── TRACES PORTAL CLOSE BAR — renders in cbar so always above Electron BrowserView ── */}
            {tracesPortalOpen&&(
              <div style={{display:"flex",alignItems:"center",gap:10,background:"#a80000",padding:"4px 10px"}}>
                <span style={{fontSize:13,color:"#fff",fontWeight:700,letterSpacing:0.2}}>🌐 TRACES Portal is open</span>
                <span style={{fontSize:11,color:"rgba(255,255,255,0.75)"}}>Press <kbd style={{background:"rgba(255,255,255,0.2)",borderRadius:3,padding:"1px 6px",fontFamily:"inherit",fontSize:10}}>Esc</kbd> or click to close</span>
                <button
                  onClick={closeTracesPortalFn}
                  disabled={tracesClosing}
                  style={{marginLeft:"auto",background:"#fff",color:"#a80000",border:"none",borderRadius:4,padding:"5px 18px",cursor:tracesClosing?"not-allowed":"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:700,display:"flex",alignItems:"center",gap:6,opacity:tracesClosing?0.7:1}}>
                  {tracesClosing
                    ? <><span style={{width:10,height:10,border:"2px solid rgba(168,0,0,0.3)",borderTopColor:"#a80000",borderRadius:"50%",display:"inline-block",animation:"spin 0.7s linear infinite"}}/> Closing…</>
                    : <>✕ Close TRACES Portal</>}
                </button>
              </div>
            )}
            {/* ── IT PORTAL CLOSE BAR — also in cbar, always above Electron BrowserView ── */}
            {itPortalOpen&&(
              <div style={{display:"flex",alignItems:"center",gap:10,background:"#0a5c0a",padding:"4px 10px"}}>
                <span style={{fontSize:13,color:"#fff",fontWeight:700,letterSpacing:0.2}}>🏛️ IT Portal is open</span>
                <span style={{fontSize:11,color:"rgba(255,255,255,0.75)"}}>incometax.gov.in · e-File → View AIS → Annual Tax Statement → Download</span>
                <button
                  onClick={closeITPortalFn}
                  disabled={itPortalClosing}
                  style={{marginLeft:"auto",background:"#fff",color:"#0a5c0a",border:"none",borderRadius:4,padding:"5px 18px",cursor:itPortalClosing?"not-allowed":"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:700,display:"flex",alignItems:"center",gap:6,opacity:itPortalClosing?0.7:1}}>
                  {itPortalClosing
                    ? <><span style={{width:10,height:10,border:"2px solid rgba(10,92,10,0.3)",borderTopColor:"#0a5c0a",borderRadius:"50%",display:"inline-block",animation:"spin 0.7s linear infinite"}}/> Closing…</>
                    : <>✕ Close IT Portal</>}
                </button>
              </div>
            )}
            {/* ── NEW FILE DETECTED BANNER — always in cbar so it's above BrowserView ── */}
            {tracesNewFiles.filter(f=>!tracesDismissed.has(f.id)).length>0&&(
              <div style={{background:"#dff6dd",borderBottom:"1px solid #b3dab3",padding:"5px 12px",display:"flex",alignItems:"center",gap:8,flexWrap:"wrap"}}>
                <span style={{fontSize:16,flexShrink:0}}>📥</span>
                <span style={{fontSize:12,fontWeight:700,color:"#107c10",flexShrink:0}}>{tracesNewFiles.filter(f=>!tracesDismissed.has(f.id)).length} new file(s) detected</span>
                <div style={{display:"flex",gap:6,flexWrap:"wrap",flex:1}}>
                  {tracesNewFiles.filter(f=>!tracesDismissed.has(f.id)).map(f=>(
                    <div key={f.id} style={{display:"flex",alignItems:"center",gap:6,background:"#fff",border:"1px solid #b3dab3",borderRadius:4,padding:"3px 8px",fontSize:11.5}}>
                      <span style={{background:"#107c10",color:"#fff",borderRadius:2,padding:"1px 5px",fontSize:9,fontWeight:700,fontFamily:"Consolas,monospace"}}>.{(f.ext||"").toUpperCase()}</span>
                      <span style={{maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",color:"#333"}}>{f.name}</span>
                      <button onClick={async()=>{
                        if(!isElectron)return;
                        try{
                          const zipPwd = curCompany?.zipPassword || null;
                          const result=await window.electronAPI.readDetectedFile(f.path,zipPwd);
                          if(result?.content){
                            const useName=result.extractedName||f.name;
                            const useExt=result.ext||f.ext;
                            await processFileDataRef.current?.(useName,useExt,result.content,result.isBase64||false,f.size,true);
                            setTracesDismissed(p=>new Set([...p,f.id]));
                          } else if(result?.error?.includes('password')){
                            showToast("🔐 Wrong ZIP password — check ZIP Password in Client Master","e",6000);
                          } else {
                            showToast("Could not read file: "+(result?.error||"unknown error"),"e");
                          }
                        }catch(e){showToast("Import failed: "+e.message,"e");}
                      }} style={{background:"#0078d4",color:"#fff",border:"none",borderRadius:3,padding:"3px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600,flexShrink:0}}>Import Now</button>
                      <button onClick={()=>setTracesDismissed(p=>new Set([...p,f.id]))} style={{background:"none",border:"none",cursor:"pointer",color:"#999",fontSize:13,padding:"0 2px",lineHeight:1}}>✕</button>
                    </div>
                  ))}
                </div>
              </div>
            )}
            {!tracesPortalOpen&&<>
            <div style={{display:"flex",alignItems:"center",gap:1,padding:"4px 15px"}}>
            <div className="cg">
              <button className="cb2 bl" onClick={()=>openImport(['txt','xml'])}><Ic d={I.file} s={17} c="#0078d4"/>Import 26AS</button>
              <button className="cb2 bl" onClick={()=>openImport(['csv'])}><Ic d={I.file} s={17} c="#0078d4"/>Import Books</button>
              {(
                <button 
                  className="cb2" 
                  onClick={openOdooSyncModal}
                  style={{background:"linear-gradient(135deg,#e8590c,#d14500)",color:"#fff",border:"none",borderRadius:4,padding:"5px 10px",boxShadow:"0 1px 3px rgba(209,69,0,0.3)"}}
                >
                  <Ic d={I.refresh} s={17} c="#fff"/>
                  Sync from Odoo
                </button>
              )}
              <button className="cb2 bl" onClick={()=>document.getElementById("invoice-import-input").click()} disabled={!datasets["Books"].length}><Ic d={I.file} s={17} c={datasets["Books"].length?"#0078d4":"#ccc"}/>Import Invoice</button>
              <button className="cb2 bl" onClick={()=>openImport(['xlsx','xls'])}><Ic d={I.excel} s={17} c="#0078d4"/>Excel</button>
            </div>
            <div className="cg">
              <button className="cb2 gn" onClick={runRecon} disabled={!datasets["26AS"].length||!datasets["Books"].length}><Ic d={I.play} s={17} c={datasets["26AS"].length&&datasets["Books"].length?"#107c10":"#ccc"}/>Reconcile</button>
              <button className="cb2" onClick={buildTanMaster} disabled={!datasets["26AS"].length&&!datasets["Books"].length}><Ic d={I.save} s={17} c={datasets["26AS"].length||datasets["Books"].length?"#201f1e":"#ccc"}/>TAN Master</button>
            </div>
            <div className="cg">
              <button className="cb2" onClick={exportCSV} disabled={!activeData.length}><Ic d={I.download} s={17} c={activeData.length?"#201f1e":"#ccc"}/>Export</button>
              <button className="cb2" onClick={exportReconReport} disabled={!reconDone}><Ic d={I.report} s={17} c={reconDone?"#201f1e":"#ccc"}/>Recon Report</button>
            </div>
            <div className="cg">
              <button className="cb2 rd" onClick={deleteSelected} disabled={!selRows.size}><Ic d={I.trash} s={17} c={selRows.size?"#d13438":"#ccc"}/>Delete</button>
              <button className="cb2" onClick={handleClearAll} disabled={!totalRecords}><Ic d={I.refresh} s={17} c={totalRecords?"#201f1e":"#ccc"}/>Clear All</button>
            </div>
            <div className="cbar-r">{selRows.size>0&&`${selRows.size} sel · `}{totalRecords} records</div>
            </div>
            </>}
          </div>

          {/* SAVE STATUS BANNER */}
          {isElectron && (storageStatus==="saving" || (storageStatus==="saved" && lastSaved)) && (
            <div className={`sv-banner${storageStatus==="saving"?" saving":""}`}>
              <div className={`sv-dot${storageStatus==="saving"?" saving":""}`}/>
              {storageStatus==="saving" ? "Saving data…" : `💾 Auto-saved · ${lastSaved}`}
            </div>
          )}

          <input ref={fileInputRef} type="file" multiple accept=".txt,.xml,.csv,.xlsx,.xls,.zip,.pdf" style={{display:"none"}} onChange={e=>[...e.target.files].forEach(processFile)}/>
          <input id="backup-file-input" type="file" accept=".json" style={{display:"none"}} onChange={e=>{if(e.target.files[0])importBackup(e.target.files[0]);e.target.value="";}}/>
          <input id="tan-email-import-input" type="file" accept=".xlsx,.xls,.csv" style={{display:"none"}} onChange={e=>{if(e.target.files[0])importTanEmailsFromExcel(e.target.files[0]);e.target.value="";}} />
          <input id="invoice-import-input" type="file" accept=".xlsx,.xls,.csv" style={{display:"none"}} onChange={e=>{if(e.target.files[0])importInvoiceDetails(e.target.files[0]);e.target.value="";}} />

          <div className="content">
            {view==="backup"&&(
              <div className="imp">
                <div className="ih">Backup & Restore</div>
                <div className="is">Auto-backup to Google Drive every time the app opens, plus manual exports anytime.</div>

                {/* ── GOOGLE DRIVE AUTO-BACKUP ── */}
                <div style={{background:"var(--wh)",border:`2px solid ${isDriveConnected&&driveBackupStatus!=="error"?"#34a853":driveBackupStatus==="error"?"#d32f2f":driveEnabled?"#fbbc04":"var(--bd)"}`,borderRadius:7,padding:20,marginBottom:16}}>
                  <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:12}}>
                    <div style={{width:40,height:40,borderRadius:7,background:isDriveConnected?"#e8f5e9":"#f5f5f5",display:"flex",alignItems:"center",justifyContent:"center",fontSize:22,flexShrink:0}}>☁️</div>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{fontSize:13,fontWeight:700,color:"var(--tx)",display:"flex",alignItems:"center",gap:8,flexWrap:"wrap"}}>
                        Google Drive Auto-Backup
                        {isDriveConnected && driveBackupStatus!=="error" && <span style={{fontSize:9.5,background:"#e8f8e8",color:"#2e7d32",borderRadius:9,padding:"2px 9px",fontWeight:700}}>● ACTIVE</span>}
                        {isDriveConnected && driveBackupStatus==="error" && <span style={{fontSize:9.5,background:"#fdecea",color:"#c62828",borderRadius:9,padding:"2px 9px",fontWeight:700}}>✗ UPLOAD ERROR</span>}
                        {!isDriveConnected && driveEnabled && <span style={{fontSize:9.5,background:"#fff8e1",color:"#f57c00",borderRadius:9,padding:"2px 9px",fontWeight:700}}>⏳ RECONNECTING</span>}
                        {!driveEnabled && <span style={{fontSize:9.5,background:"#f5f5f5",color:"#888",borderRadius:9,padding:"2px 9px",fontWeight:700}}>NOT CONNECTED</span>}
                      </div>
                      <div style={{fontSize:11,color:"var(--tx2)",marginTop:3}}>
                        {isDriveConnected ? `Connected as ${driveUser?.email||"—"} · Backs up silently every time the app opens` : driveEnabled ? "Reconnecting using saved credentials…" : "Connect once — backups happen automatically in background"}
                      </div>
                    </div>
                    <div style={{display:"flex",gap:8,flexShrink:0}}>
                      {isDriveConnected
                        ? <>
                            <button onClick={()=>runDriveBackup()} disabled={driveBackupStatus==="running"}
                              style={{background:"#34a853",color:"#fff",border:"none",borderRadius:4,padding:"7px 16px",cursor:driveBackupStatus==="running"?"default":"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,opacity:driveBackupStatus==="running"?0.6:1}}>
                              {driveBackupStatus==="running"?"⏳ Backing up…":"☁ Backup Now"}
                            </button>
                            <button onClick={()=>pullDriveSync()} disabled={driveSyncStatus==="checking"}
                              style={{background:"#0078d4",color:"#fff",border:"none",borderRadius:4,padding:"7px 16px",cursor:driveSyncStatus==="checking"?"default":"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,opacity:driveSyncStatus==="checking"?0.6:1}}>
                              {driveSyncStatus==="checking"?"⏳ Checking…":"⬇ Sync from Drive"}
                            </button>
                            <button onClick={disconnectDrive} style={{background:"none",border:"1px solid var(--bd)",borderRadius:4,padding:"7px 14px",cursor:"pointer",fontSize:12,fontFamily:"inherit",color:"var(--tx2)"}}>Disconnect</button>
                          </>
                        : <button onClick={connectDrive} style={{background:"#34a853",color:"#fff",border:"none",borderRadius:4,padding:"8px 22px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:700,boxShadow:"0 2px 8px rgba(52,168,83,0.3)"}}>
                            Connect Google Drive
                          </button>
                      }
                    </div>
                  </div>
                  {/* Drive API not enabled — show actionable fix */}
                  {driveBackupStatus==="error" && driveBackupLog[0]?.msg?.toLowerCase().includes("not been used") && (
                    <div style={{background:"#fff8e1",border:"1px solid #ffe082",borderRadius:5,padding:"12px 14px",marginBottom:8,fontSize:11.5,color:"#5d4037",lineHeight:1.9}}>
                      <b>⚠ Google Drive API is not enabled for your project.</b> Fix in 1 click:<br/>
                      <b>Step 1:</b> <a href="https://console.developers.google.com/apis/api/drive.googleapis.com/overview" target="_blank" rel="noreferrer" style={{color:"#0078d4",fontWeight:600}}>Click here to enable Google Drive API</a> — sign in, select your project, click <b>Enable</b>.<br/>
                      <b>Step 2:</b> Wait 2 minutes, then click <b>Backup Now</b> again. Done.
                    </div>
                  )}

                  {/* Status + last backup */}
                  {driveEnabled && (
                    <div style={{display:"flex",alignItems:"center",gap:16,background:"var(--sur)",borderRadius:5,padding:"8px 13px",fontSize:11,color:"var(--tx2)",marginBottom:driveBackupLog.length>0?8:0,flexWrap:"wrap"}}>
                      <span>Last backup: <b style={{color:driveBackupStatus==="error"?"var(--red)":"var(--tx)"}}>{driveLastBackup||"Never"}</b></span>
                      <span style={{color:"var(--tx3)"}}>·</span>
                      <span>Folder: <b style={{color:"var(--a)"}}>26AS Recon Backups</b> (Drive)</span>
                      {driveLastSync && <><span style={{color:"var(--tx3)"}}>·</span><span>Last sync: <b style={{color:"var(--grn)"}}>{driveLastSync}</b></span></>}
                      <span style={{marginLeft:"auto",display:"flex",gap:10,alignItems:"center"}}>
                        {driveSyncStatus==="checking" && <span style={{color:"var(--amb)"}}>⏳ Checking Drive…</span>}
                        {driveSyncStatus==="synced"   && <span style={{color:"var(--grn)"}}>☁ In sync</span>}
                        {driveSyncStatus==="no_backup"&& <span style={{color:"var(--tx3)"}}>No Drive backup yet</span>}
                        {driveSyncStatus==="error"    && <span style={{color:"var(--red)"}}>⚠ Sync check failed</span>}
                        {driveBackupStatus==="running" && <span style={{color:"var(--amb)"}}>⏳ Backing up…</span>}
                        {driveBackupStatus==="done"    && <span style={{color:"var(--grn)"}}>✓ Backed up</span>}
                        {driveBackupStatus==="error"   && <span style={{color:"var(--red)"}}>✗ Backup failed</span>}
                      </span>
                    </div>
                  )}

                  {/* Live log */}
                  {driveBackupLog.length > 0 && (
                    <div style={{background:"#1e1e1e",borderRadius:4,padding:"8px 12px",maxHeight:80,overflowY:"auto"}}>
                      {driveBackupLog.map((l,i)=>(
                        <div key={i} style={{fontSize:10.5,fontFamily:"Consolas,monospace",color:l.status==="done"?"#4ec9b0":l.status==="error"?"#f48771":"#dcdcaa",lineHeight:1.6}}>
                          [{l.time}] {l.msg}
                        </div>
                      ))}
                    </div>
                  )}

                  {/* First-time info / prerequisite warning */}
                  {!driveEnabled && (
                    <div style={{marginTop:12,display:"flex",flexDirection:"column",gap:8}}>
                      <div style={{background:"#e8f5e9",border:"1px solid #c8e6c9",borderRadius:5,padding:"10px 14px",fontSize:11.5,color:"#1b5e20",lineHeight:1.8}}>
                        <b>How it works:</b> Uses the same Google OAuth Client ID you set up for Gmail sending.
                        Click <b>Connect Google Drive</b> and authorize once — select <b>Full Drive access</b> when prompted.
                        After that, every time you open the app, a full backup uploads silently to your Drive —
                        no clicks needed. Keeps the <b>last 7 backups</b> automatically.
                      </div>
                      {!gmailClientId && (
                        <div style={{background:"#fff3e0",border:"1px solid #ffe0b2",borderRadius:5,padding:"9px 13px",fontSize:11.5,color:"#bf360c"}}>
                          ⚠️ You need a Google OAuth Client ID first.
                          Go to <b>TDS Notice → Gmail Settings</b>, enter your Client ID there, then come back here to connect Drive.
                        </div>
                      )}
                    </div>
                  )}
                </div>
                {/* LOCAL FOLDER BACKUP */}
                {isElectron && (
                  <div style={{background:"var(--wh)",border:"2px solid #0078d4",borderRadius:7,padding:20,marginBottom:16}}>
                    <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:10}}>
                      <div style={{width:40,height:40,borderRadius:7,background:"#e6f3fb",display:"flex",alignItems:"center",justifyContent:"center",fontSize:22,flexShrink:0}}>📁</div>
                      <div style={{flex:1}}>
                        <div style={{fontSize:13,fontWeight:700,color:"var(--tx)",display:"flex",alignItems:"center",gap:8}}>Local Folder Backup<span style={{fontSize:9.5,background:"#e6f3fb",color:"#0078d4",borderRadius:9,padding:"2px 9px",fontWeight:700}}>RECOMMENDED ALTERNATIVE</span></div>
                        <div style={{fontSize:11,color:"var(--tx2)",marginTop:3}}>Works without any Google setup. Save to your Google Drive desktop folder, Dropbox, or OneDrive for automatic cloud sync.</div>
                      </div>
                    </div>
                    <div style={{background:"#e6f3fb",border:"1px solid #b3d4f0",borderRadius:5,padding:"10px 14px",marginBottom:12,fontSize:11.5,color:"#004a82",lineHeight:1.7}}>
                      <b>Tip:</b> When prompted, navigate to your <b>Google Drive</b> or <b>OneDrive</b> desktop sync folder and save there. The file will automatically sync to cloud — no OAuth or 403 errors ever.
                    </div>
                    <div style={{display:"flex",gap:10,alignItems:"center",flexWrap:"wrap"}}>
                      <button onClick={async()=>{
                        const cc=companiesRef.current||[];
                        const total=cc.reduce((a,c)=>a+Object.values(c.years||{}).reduce((b,y)=>b+Object.values(y.datasets||{}).reduce((s,d)=>s+d.length,0),0),0);
                        if(total===0){showToast("No data to backup","w");return;}
                        const payload={version:2,scope:"all",companies:cc,exportedAt:new Date().toISOString()};
                        const fileContent=JSON.stringify(payload,null,2);
                        const ts=new Date().toISOString().slice(0,16).replace("T","_").replace(/:/g,"-");
                        const dn=`26AS_Recon_Backup_${ts}.json`;
                        const res=await window.electronAPI.saveFile({defaultName:dn,content:fileContent});
                        if(res?.success)showToast(`Backup saved: ${res.path}`,"s",5000);
                      }} style={{background:"#0078d4",color:"#fff",border:"none",borderRadius:4,padding:"8px 20px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:700}}>💾 Save Backup Now</button>
                      <span style={{fontSize:11,color:"var(--tx3)"}}>Choose your Google Drive / OneDrive folder when the save dialog opens</span>
                    </div>
                  </div>
                )}

                <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:5,padding:20,marginBottom:16}}>
                  <div style={{fontSize:13,fontWeight:600,marginBottom:4,display:"flex",alignItems:"center",gap:7}}><Ic d={I.download} s={14} c="var(--a)"/>Export Backup</div>
                  <div style={{fontSize:12,color:"var(--tx2)",marginBottom:14}}>Choose what to include in the backup file. The file is saved as <b>.json</b> and can be restored later.</div>
                  <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12}}>
                    {[
                      {scope:"year", title:`Current Year Only`, sub:`${curCompany?.name} · FY ${selYear}`, col:"var(--a)", bg:"#e6f3fb",
                        stat:`26AS: ${datasets["26AS"].length} · Books: ${datasets["Books"].length} records`},
                      {scope:"company", title:`All Years — This Company`, sub:curCompany?.name, col:"var(--grn)", bg:"#e8f8e8",
                        stat:`${Object.keys(curCompany?.years||{}).length} years of data`},
                      {scope:"all", title:`All Companies & Years`, sub:"Complete backup", col:"var(--pur)", bg:"#f0e8ff",
                        stat:`${companies.length} companies`},
                    ].map(opt=>(
                      <div key={opt.scope} style={{border:`1px solid var(--bd)`,borderRadius:5,padding:16,cursor:"pointer",transition:"all 0.12s"}}
                        onMouseEnter={e=>e.currentTarget.style.borderColor=opt.col}
                        onMouseLeave={e=>e.currentTarget.style.borderColor="var(--bd)"}
                        onClick={()=>exportBackup(opt.scope)}>
                        <div style={{width:36,height:36,borderRadius:4,background:opt.bg,display:"flex",alignItems:"center",justifyContent:"center",marginBottom:10}}>
                          <Ic d={I.download} s={18} c={opt.col}/>
                        </div>
                        <div style={{fontSize:12.5,fontWeight:600,marginBottom:2}}>{opt.title}</div>
                        <div style={{fontSize:11,color:"var(--tx2)",marginBottom:6}}>{opt.sub}</div>
                        <div style={{fontSize:10.5,color:opt.col,fontWeight:500,background:opt.bg,padding:"2px 8px",borderRadius:9,display:"inline-block"}}>{opt.stat}</div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* RESTORE SECTION */}
                <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:5,padding:20,marginBottom:16}}>
                  <div style={{fontSize:13,fontWeight:600,marginBottom:4,display:"flex",alignItems:"center",gap:7}}><Ic d={I.import} s={14} c="var(--grn)"/>Restore from Backup</div>
                  <div style={{fontSize:12,color:"var(--tx2)",marginBottom:14}}>Select a <b>.json</b> backup file. Data will be <b>merged</b> — existing data is not deleted, restored data is added on top.</div>
                  <div className="drop" style={{padding:"32px 24px"}} onClick={()=>document.getElementById("backup-file-input").click()}
                    onDragOver={e=>e.preventDefault()} onDrop={e=>{e.preventDefault();const f=e.dataTransfer.files[0];if(f)importBackup(f);}}>
                    <div className="di" style={{width:48,height:48}}><Ic d={I.import} s={22} c="var(--grn)"/></div>
                    <h2 style={{fontSize:14}}>Drop backup file here</h2>
                    <p>or click to browse — accepts .json backup files only</p>
                    <button className="ib" style={{background:"var(--grn)",marginTop:8}} onClick={e=>{e.stopPropagation();document.getElementById("backup-file-input").click();}}>Browse Backup File</button>
                  </div>
                </div>

                {/* BACKUP HISTORY — all years for current company */}
                <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:5,padding:20}}>
                  <div style={{fontSize:13,fontWeight:600,marginBottom:12,display:"flex",alignItems:"center",gap:7}}><Ic d={I.grid} s={14} c="var(--tx2)"/>Data Summary — {curCompany?.name}</div>
                  <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                    <thead><tr style={{background:"var(--hb)"}}>
                      {["Financial Year","26AS Records","Books Records","AIS Records","Recon Done","TAN Master","Quick Backup"].map(h=>(
                        <th key={h} style={{padding:"7px 10px",textAlign:"left",fontWeight:600,fontSize:11,color:"var(--tx2)",borderBottom:"1px solid var(--bd)"}}>{h}</th>
                      ))}
                    </tr></thead>
                    <tbody>
                      {FY_LIST.map(fy=>{
                        const yd = curCompany?.years?.[fy] || mkYear();
                        const has = Object.values(yd.datasets||{}).some(d=>d.length>0);
                        return (
                          <tr key={fy} style={{borderBottom:"1px solid #f5f5f5"}}>
                            <td style={{padding:"7px 10px",fontWeight:600,color:fy===selYear?"var(--a)":"var(--tx)"}}>{fy}{fy===selYear&&<span style={{fontSize:9,background:"var(--a-lt)",color:"var(--a)",borderRadius:3,padding:"1px 5px",marginLeft:5}}>current</span>}</td>
                            <td style={{padding:"7px 10px",fontFamily:"Consolas,monospace",color:yd.datasets?.["26AS"]?.length?"var(--tx)":"var(--tx3)"}}>{yd.datasets?.["26AS"]?.length||"—"}</td>
                            <td style={{padding:"7px 10px",fontFamily:"Consolas,monospace",color:yd.datasets?.["Books"]?.length?"var(--tx)":"var(--tx3)"}}>{yd.datasets?.["Books"]?.length||"—"}</td>
                            <td style={{padding:"7px 10px",fontFamily:"Consolas,monospace",color:yd.datasets?.["AIS"]?.length?"var(--tx)":"var(--tx3)"}}>{yd.datasets?.["AIS"]?.length||"—"}</td>
                            <td style={{padding:"7px 10px"}}>{yd.reconDone?<span style={{color:"var(--grn)",fontWeight:600}}>✓ Yes</span>:<span style={{color:"var(--tx3)"}}>—</span>}</td>
                            <td style={{padding:"7px 10px"}}>{yd.tanMaster?.length?<span style={{color:"var(--a)"}}>{yd.tanMaster.length} TANs</span>:<span style={{color:"var(--tx3)"}}>—</span>}</td>
                            <td style={{padding:"7px 10px"}}>
                              {has
                                ? <button onClick={()=>{
                                    const yd2 = curCompany?.years?.[fy] || mkYear();
                                    const payload = {version:2,scope:"year",company:curCompany.name,year:fy,data:yd2,exportedAt:new Date().toISOString()};
                                    const content = JSON.stringify(payload,null,2);
                                    const ts = new Date().toISOString().slice(0,10);
                                    const defaultName = `Backup_${curCompany.name.replace(/\s+/g,"_")}_FY${fy}_${ts}.json`;
                                    if(isElectron){window.electronAPI.saveFile({defaultName,content}).then(r=>r?.success&&showToast(`Backup saved: ${r.path}`));}
                                    else{const a=document.createElement("a");a.href=URL.createObjectURL(new Blob([content],{type:"application/json"}));a.download=defaultName;a.click();showToast(`Backup downloaded`);}
                                  }} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"3px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit"}}>💾 Backup FY {fy}</button>
                                : <span style={{color:"var(--tx3)",fontSize:11}}>No data</span>
                              }
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {view==="dashboard"&&(
              <div style={{flex:1,display:"flex",flexDirection:"column",overflow:"hidden",background:"var(--sur)"}}>
                {/* ── HEADER ── */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"12px 20px",display:"flex",alignItems:"center",gap:12,flexShrink:0}}>
                  <div style={{fontSize:15,fontWeight:700,color:"var(--tx)"}}>Summary Dashboard</div>
                  <div style={{marginLeft:"auto",display:"flex",alignItems:"center",gap:8}}>
                    <span style={{fontSize:11,color:"var(--tx2)"}}>Financial Year</span>
                    <select value={dashFY} onChange={e=>setDashFY(e.target.value)} style={{border:"1px solid var(--bd)",borderRadius:3,padding:"5px 10px",fontSize:12,fontFamily:"inherit",background:"var(--wh)",color:"var(--tx)",outline:"none",fontWeight:600}}>
                      {FY_LIST.map(fy=><option key={fy} value={fy}>{fy}</option>)}
                    </select>
                  </div>
                </div>
                {/* ── CONTENT ── */}
                <div style={{flex:1,overflow:"auto",padding:20}}>
                  {(()=>{
                    const allActive = companies.filter(c=>c.status!=="archived");
                    const withFY = allActive.map(c=>({...c, yd: c.years?.[dashFY] || null}));
                    const has26 = withFY.filter(c=>c.yd?.datasets?.["26AS"]?.length>0);
                    const hasBk = withFY.filter(c=>c.yd?.datasets?.["Books"]?.length>0);
                    const reconDoneList = withFY.filter(c=>c.yd?.reconDone);
                    const cleanList = reconDoneList.filter(c=>!(c.yd.reconResults||[]).some(r=>r.matchStatus!=="Matched"));
                    const issueList = reconDoneList.filter(c=>(c.yd.reconResults||[]).some(r=>r.matchStatus!=="Matched"));
                    const pendingList = withFY.filter(c=>!c.yd?.reconDone && c.yd?.datasets?.["26AS"]?.length>0 && c.yd?.datasets?.["Books"]?.length>0);
                    const notStartedList = allActive.filter(c=>!c.years?.[dashFY]?.datasets?.["26AS"]?.length);
                    // Aggregate TDS numbers
                    const total26AsTDS = has26.reduce((s,c)=>(c.yd?.datasets?.["26AS"]||[]).reduce((ss,r)=>ss+(r.tdsDeducted||0),s),0);
                    const totalBooksTDS = hasBk.reduce((s,c)=>(c.yd?.datasets?.["Books"]||[]).reduce((ss,r)=>ss+(r.tdsDeducted||0),s),0);
                    // Compute TDS difference from stored reconResults (same method as Reconciliation screen:
                    // sum as_tds - bk_tds per TAN row) so both screens always show the same figure.
                    // Falls back to raw dataset diff if reconciliation hasn't been run yet.
                    const totalDiff = (() => {
                      const allRR = issueList.concat(cleanList).flatMap(c=>c.yd?.reconResults||[]);
                      if (allRR.length > 0) {
                        const as26rr  = allRR.reduce((s,r)=>s+(r.as_tds||0),0);
                        const bkrr    = allRR.reduce((s,r)=>s+(r.bk_tds||0),0);
                        return as26rr - bkrr;
                      }
                      return total26AsTDS - totalBooksTDS;
                    })();
                    const totalMismatches = issueList.reduce((s,c)=>{
                      return s + (c.yd?.reconResults||[]).filter(r=>r.matchStatus!=="Matched").length;
                    }, 0);
                    const fmt2 = n => n>=10000000?`₹${(n/10000000).toFixed(2)}Cr`:n>=100000?`₹${(n/100000).toFixed(1)}L`:`₹${n.toLocaleString("en-IN",{maximumFractionDigits:0})}`;
                    const selC = (dashSelClientId ? allActive.find(c=>c.id===dashSelClientId) : null) || allActive[0];
                    const selYd = selC?.years?.[dashFY];
                    const selRR = selYd?.reconResults||[];
                    const sel26TDS = (selYd?.datasets?.["26AS"]||[]).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                    const selBkTDS = (selYd?.datasets?.["Books"]||[]).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                    // Use reconResults for diff (matches Reconciliation screen method); fallback to raw diff
                    const selDiff = selRR.length > 0
                      ? selRR.reduce((s,r)=>s+(r.as_tds||0),0) - selRR.reduce((s,r)=>s+(r.bk_tds||0),0)
                      : sel26TDS - selBkTDS; // 26AS - Books: positive = 26AS higher (short in Books)
                    const fmtT = n => n ? `₹${Math.abs(n).toLocaleString("en-IN",{maximumFractionDigits:0})}` : "—";
                    // Build breakdown from reconResults — each TAN row has as_tds and bk_tds
                    // Use actual as_tds/bk_tds per status (bk_tds is non-zero even for "Missing in Books"
                    // when TAN exists in Books but with lower amount)
                    const byStatus = (st, field) => selRR.filter(r=>r.matchStatus===st).reduce((s,r)=>s+(r[field]||0),0);
                    const statuses = ["Matched","Near Match","Mismatch","Missing in Books","Missing in 26AS"];
                    const statusMeta = {
                      "Matched":          {col:"var(--grn)", sno:1},
                      "Near Match":       {col:"var(--amb)", sno:2},
                      "Mismatch":         {col:"var(--red)", sno:3},
                      "Missing in Books": {col:"var(--pur)", sno:4},
                      "Missing in 26AS":  {col:"var(--ora)", sno:5},
                    };
                    const detailRows = statuses
                      .map(st => ({
                        sno:  statusMeta[st].sno,
                        type: st,
                        col:  statusMeta[st].col,
                        record: selRR.filter(r=>r.matchStatus===st).length,
                        tds26:  byStatus(st, "as_tds"),
                        tdsBk:  byStatus(st, "bk_tds"),
                      }))
                      .filter(r => r.record > 0);
                    return (
                      <div style={{display:"grid",gridTemplateColumns:"55% 45%",gap:14,alignItems:"stretch"}}>

                        {/* ══ LEFT COLUMN ══ */}
                        <div style={{display:"flex",flexDirection:"column",gap:10,height:"100%"}}>

                          {/* KPI cards: 3x2 */}
                          <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:8}}>
                            {[
                              {label:"Total Clients",  val:allActive.length,      sub:"Active",                       col:"var(--a)",   bg:"#e6f3fb",icon:"👥"},
                              {label:"Recon Complete", val:reconDoneList.length,  sub:`of ${allActive.length}`,        col:"var(--grn)", bg:"#e8f8e8",icon:"✅"},
                              {label:"Clean",          val:cleanList.length,      sub:"No issues",                    col:"#107c10",    bg:"#d4edda",icon:"🟢"},
                              {label:"Issues Found",   val:issueList.length,      sub:`${totalMismatches} mismatches`,col:"var(--red)", bg:"#fde7e9",icon:"⚠️"},
                              {label:"Pending",        val:pendingList.length,    sub:"Ready to recon",               col:"var(--amb)", bg:"#fff8e1",icon:"⏳"},
                              {label:"Not Started",    val:notStartedList.length, sub:"No data",                      col:"#888",       bg:"#f5f5f5",icon:"📭"},
                            ].map(k=>(
                              <div key={k.label} style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"9px 11px",display:"flex",alignItems:"center",gap:9}}>
                                <div style={{width:30,height:30,borderRadius:5,background:k.bg,display:"flex",alignItems:"center",justifyContent:"center",fontSize:14,flexShrink:0}}>{k.icon}</div>
                                <div>
                                  <div style={{fontSize:17,fontWeight:700,color:k.col,lineHeight:1}}>{k.val}</div>
                                  <div style={{fontSize:10.5,fontWeight:600,color:"var(--tx)",marginTop:2}}>{k.label}</div>
                                  <div style={{fontSize:9.5,color:"var(--tx2)"}}>{k.sub}</div>
                                </div>
                              </div>
                            ))}
                          </div>

                          {/* TDS summary: 3-col */}
                          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:8}}>
                            {[
                              {label:"Total 26AS TDS", val:fmt2(total26AsTDS),  sub:`${has26.length} clients`, col:"var(--a)",   barBg:"#e6f3fb",barFill:"var(--a)",   barW:"100%"},
                              {label:"Total Books TDS", val:fmt2(totalBooksTDS), sub:`${hasBk.length} clients`, col:"var(--grn)", barBg:"#e8f8e8",barFill:"var(--grn)", barW:total26AsTDS>0?`${Math.min(100,(totalBooksTDS/total26AsTDS)*100).toFixed(0)}%`:"0%"},
                              {label:"TDS Difference",  val:Math.abs(totalDiff)>1?((totalDiff>0?"+":"-")+fmt2(Math.abs(totalDiff))):"✓ Nil", sub:`${totalMismatches} mismatches`, col:Math.abs(totalDiff)>1?"var(--red)":"var(--grn)", barBg:"#fde7e9",barFill:"var(--red)", barW:total26AsTDS>0?`${Math.min(100,(Math.abs(totalDiff)/total26AsTDS)*100).toFixed(0)}%`:"0%"},
                            ].map(t=>(
                              <div key={t.label} style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"10px 13px"}}>
                                <div style={{fontSize:9.5,fontWeight:600,color:"var(--tx2)",textTransform:"uppercase",letterSpacing:0.4,marginBottom:3}}>{t.label}</div>
                                <div style={{fontSize:19,fontWeight:600,color:t.col,marginBottom:2}}>{t.val}</div>
                                <div style={{fontSize:9.5,color:"var(--tx2)",marginBottom:6}}>{t.sub}</div>
                                <div style={{height:3,borderRadius:2,background:t.barBg}}>
                                  <div style={{height:"100%",borderRadius:2,background:t.barFill,width:t.barW}}/>
                                </div>
                              </div>
                            ))}
                          </div>

                          {/* All Clients Table */}
                          <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:7,overflow:"hidden",flex:1,display:"flex",flexDirection:"column"}}>
                            <div style={{padding:"9px 13px",borderBottom:"1px solid var(--bd)",display:"flex",alignItems:"center",gap:8}}>
                              <div style={{fontSize:12,fontWeight:700,color:"var(--tx)"}}>All Clients</div>
                              <span style={{fontSize:10.5,background:"var(--a-lt)",color:"var(--a)",borderRadius:9,padding:"1px 7px",fontWeight:600}}>FY {dashFY}</span>
                              <span style={{fontSize:10,color:"var(--tx2)",marginLeft:"auto"}}>{allActive.length} clients · click row to select</span>
                            </div>
                            <div style={{overflowX:"hidden",overflowY:"auto",flex:1}}>
                              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                                <thead style={{position:"sticky",top:0,zIndex:2}}>
                                  <tr style={{background:"var(--hb)"}}>
                                    {["#","Client Name","26AS","Books","Matched","Mismatch","MiB","MiA","26AS TDS","Books TDS","Diff","Status"].map(h=>(
                                      <th key={h} style={{padding:"6px 7px",textAlign:h==="#"||h==="Client Name"||h==="Type"?"left":"right",fontSize:9.5,fontWeight:700,color:"var(--tx2)",borderBottom:"2px solid var(--bd)",whiteSpace:"nowrap",background:"var(--hb)"}}>{h}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {(()=>{
                                    if(allActive.length===0) return <tr><td colSpan={13} style={{padding:"24px",textAlign:"center",color:"var(--tx2)",fontSize:12}}>No clients found</td></tr>;
                                    let tot26=0,totBk=0,totM=0,totMM=0,totMib=0,totMia=0,tot26T=0,totBkT=0,totD=0;
                                    const trows = allActive.map((c,idx)=>{
                                      const yd=c.years?.[dashFY]; const rr=yd?.reconResults||[];
                                      const r26=yd?.datasets?.["26AS"]?.length||0, rBk=yd?.datasets?.["Books"]?.length||0;
                                      const matched=rr.filter(r=>r.matchStatus==="Matched").length, mismatch=rr.filter(r=>r.matchStatus==="Mismatch").length;
                                      const mib=rr.filter(r=>r.matchStatus==="Missing in Books").length, mia=rr.filter(r=>r.matchStatus==="Missing in 26AS").length;
                                      const tds26=(yd?.datasets?.["26AS"]||[]).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                                      const tdsBk=(yd?.datasets?.["Books"]||[]).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                                      // Use reconResults for diff to match Reconciliation screen; fallback to raw
                                      const diff = rr.length > 0
                                        ? rr.reduce((s,r)=>s+(r.bk_tds||0),0) - rr.reduce((s,r)=>s+(r.as_tds||0),0)
                                        : tdsBk - tds26; // Books - 26AS: positive = Books higher
                                      tot26+=r26; totBk+=rBk; totM+=matched; totMM+=mismatch; totMib+=mib; totMia+=mia; tot26T+=tds26; totBkT+=tdsBk; totD+=diff;
                                      let sLabel="Not Started",sCol="#999",sBg="#f5f5f5";
                                      if(yd?.reconDone){const hi=rr.some(r=>r.matchStatus!=="Matched");sLabel=hi?"Issues":"Clean ✓";sCol=hi?"var(--red)":"var(--grn)";sBg=hi?"#fde7e9":"#e8f8e8";}
                                      else if(r26>0&&rBk>0){sLabel="Pending";sCol="var(--amb)";sBg="#fff8e1";}
                                      else if(r26>0||rBk>0){sLabel="Partial";sCol="var(--a)";sBg="#e6f3fb";}
                                      const isSelected = selC?.id === c.id;
                                      const fmtN = n=>n?`₹${Math.abs(n).toLocaleString("en-IN",{maximumFractionDigits:0})}`:"—";
                                      return (
                                        <tr key={c.id} onClick={()=>setDashSelClientId(c.id)}
                                          style={{borderBottom:"1px solid #f0f0f0",cursor:"pointer",background:isSelected?"#e8f2fd":idx%2===0?"#fff":"#fafafa",outline:isSelected?"2px solid var(--a)":"none",outlineOffset:"-2px",transition:"background 0.1s"}}
                                          onMouseEnter={e=>{if(!isSelected)e.currentTarget.style.background="#f5f9ff";}}
                                          onMouseLeave={e=>{e.currentTarget.style.background=isSelected?"#e8f2fd":idx%2===0?"#fff":"#fafafa";}}>
                                          <td style={{padding:"6px 7px",color:"var(--tx3)",fontSize:9.5}}>{idx+1}</td>
                                          <td style={{padding:"6px 7px",minWidth:110}}>
                                            <div style={{display:"flex",alignItems:"center",gap:5}}>
                                              <div style={{width:18,height:18,borderRadius:3,background:isSelected?"var(--a)":"var(--a-lt)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:9,fontWeight:700,color:isSelected?"#fff":"var(--a)",flexShrink:0}}>{(c.name||"?")[0].toUpperCase()}</div>
                                              <span style={{fontWeight:600,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:120,fontSize:11}}>{c.name}</span>
                                            </div>
                                          </td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:r26?"var(--a)":"var(--tx3)",fontWeight:r26?600:400}}>{r26||"—"}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:rBk?"var(--grn)":"var(--tx3)",fontWeight:rBk?600:400}}>{rBk||"—"}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:matched?"var(--grn)":"var(--tx3)"}}>{yd?.reconDone?matched:"—"}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:mismatch>0?"var(--red)":"var(--tx3)",fontWeight:mismatch>0?700:400}}>{yd?.reconDone?mismatch:"—"}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:mib>0?"var(--pur)":"var(--tx3)"}}>{yd?.reconDone?mib:"—"}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:mia>0?"var(--ora)":"var(--tx3)"}}>{yd?.reconDone?mia:"—"}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:9.5,color:tds26?"var(--tx)":"var(--tx3)"}}>{fmtN(tds26)}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:9.5,color:tdsBk?"var(--tx)":"var(--tx3)"}}>{fmtN(tdsBk)}</td>
                                          <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:9.5,fontWeight:600,color:Math.abs(diff)>0?(diff>0?"var(--red)":"var(--grn)"):"var(--tx3)"}}>
                                            {yd?.reconDone?(Math.abs(diff)<1?"✓":(diff>0?"+":"-")+fmtN(diff)):"—"}
                                          </td>
                                          <td style={{padding:"6px 7px",textAlign:"right"}}>
                                            <span style={{background:sBg,color:sCol,borderRadius:9,padding:"1px 6px",fontSize:9,fontWeight:600,whiteSpace:"nowrap"}}>{sLabel}</span>
                                          </td>
                                        </tr>
                                      );
                                    });
                                    return (<>
                                      {trows}
                                      <tr style={{background:"#f0f4f8",borderTop:"2px solid var(--bd)",fontWeight:700}}>
                                        <td colSpan={2} style={{padding:"6px 7px",fontSize:10.5,color:"var(--tx)"}}>TOTAL ({allActive.length})</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:"var(--a)"}}>{tot26||"—"}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:"var(--grn)"}}>{totBk||"—"}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:"var(--grn)"}}>{totM||"—"}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:totMM>0?"var(--red)":"var(--tx3)",fontWeight:700}}>{totMM||"—"}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:totMib>0?"var(--pur)":"var(--tx3)"}}>{totMib||"—"}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10,color:totMia>0?"var(--ora)":"var(--tx3)"}}>{totMia||"—"}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:9.5}}>₹{tot26T.toLocaleString("en-IN",{maximumFractionDigits:0})}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:9.5}}>₹{totBkT.toLocaleString("en-IN",{maximumFractionDigits:0})}</td>
                                        <td style={{padding:"6px 7px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:9.5,color:Math.abs(totD)>1?(totD>0?"var(--red)":"var(--grn)"):"var(--grn)",fontWeight:700}}>
                                          {Math.abs(totD)<1?"✓":(totD>0?"+":"-")+"₹"+Math.abs(totD).toLocaleString("en-IN",{maximumFractionDigits:0})}
                                        </td>
                                        <td/>
                                      </tr>
                                    </>);
                                  })()}
                                </tbody>
                              </table>
                            </div>
                          </div>

                        </div>{/* end left col */}

                        {/* ══ RIGHT COLUMN: Client Detail ══ */}
                        <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:7,overflow:"hidden",display:"flex",flexDirection:"column",minHeight:"100%"}}>

                          {/* Header */}
                          <div style={{padding:"10px 14px",borderBottom:"1px solid var(--bd)",display:"flex",alignItems:"center",gap:8,background:"var(--wh)"}}>
                            <div style={{fontSize:12.5,fontWeight:700,color:"var(--tx)"}}>Client Detail</div>
                            <span style={{fontSize:10.5,background:"var(--a-lt)",color:"var(--a)",borderRadius:9,padding:"1px 7px",fontWeight:600}}>FY {dashFY}</span>
                          </div>

                          {/* Client identity strip */}
                          {selC && <div style={{padding:"10px 14px",background:"var(--sur)",borderBottom:"1px solid var(--bd)",display:"flex",alignItems:"center",gap:10}}>
                            <div style={{width:32,height:32,borderRadius:6,background:"var(--a)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:14,fontWeight:700,color:"#fff",flexShrink:0}}>{(selC.name||"?")[0].toUpperCase()}</div>
                            <div style={{flex:1,minWidth:0}}>
                              <div style={{fontWeight:700,fontSize:13,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{selC.name}</div>
                              <div style={{fontSize:10,color:"var(--tx2)",display:"flex",gap:6,marginTop:2,flexWrap:"wrap"}}>
                                {selC.pan&&<span style={{fontFamily:"Consolas,monospace"}}>{selC.pan}</span>}
                                {selC.clientType&&<span style={{background:"#f0f0f0",borderRadius:2,padding:"0 4px"}}>{selC.clientType}</span>}
                              </div>
                            </div>
                            {selYd?.reconDone
                              ? <span style={{fontSize:9.5,fontWeight:700,background:selRR.some(r=>r.matchStatus!=="Matched")?"#fde7e9":"#e8f8e8",color:selRR.some(r=>r.matchStatus!=="Matched")?"var(--red)":"var(--grn)",borderRadius:9,padding:"3px 9px",whiteSpace:"nowrap",flexShrink:0}}>{selRR.some(r=>r.matchStatus!=="Matched")?"Issues Found":"Clean ✓"}</span>
                              : <span style={{fontSize:9.5,fontWeight:700,background:"#f5f5f5",color:"#888",borderRadius:9,padding:"3px 9px",flexShrink:0}}>Not Reconciled</span>
                            }
                          </div>}

                          {/* Summary table — compact, no scroll */}
                          <div style={{flex:1}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:11.5}}>
                            <thead>
                              <tr style={{background:"var(--hb)"}}>
                                <th style={{padding:"7px 10px",textAlign:"left",fontSize:10,fontWeight:700,color:"var(--tx2)",borderBottom:"2px solid var(--bd)",width:30}}>#</th>
                                <th style={{padding:"7px 10px",textAlign:"left",fontSize:10,fontWeight:700,color:"var(--tx2)",borderBottom:"2px solid var(--bd)"}}>Type</th>
                                <th style={{padding:"7px 10px",textAlign:"right",fontSize:10,fontWeight:700,color:"var(--tx2)",borderBottom:"2px solid var(--bd)",width:50}}>Rec</th>
                                <th style={{padding:"7px 10px",textAlign:"right",fontSize:10,fontWeight:700,color:"var(--tx2)",borderBottom:"2px solid var(--bd)"}}>26AS</th>
                                <th style={{padding:"7px 10px",textAlign:"right",fontSize:10,fontWeight:700,color:"var(--tx2)",borderBottom:"2px solid var(--bd)"}}>Books</th>
                                <th style={{padding:"7px 10px",textAlign:"right",fontSize:10,fontWeight:700,color:"var(--tx2)",borderBottom:"2px solid var(--bd)"}}>Diff</th>
                              </tr>
                            </thead>
                            <tbody>
                              {detailRows.map((r,i)=>{
                                // Diff per category = 26AS amount - Books amount for that group
                                // Positive = 26AS higher (short in Books, needs entry)
                                // Negative = Books higher (excess booking)
                                const diff = r.tds26 - r.tdsBk;
                                return (
                                  <tr key={r.sno} style={{borderBottom:"1px solid #f0f0f0",background:i%2===0?"#fff":"#fafafa"}}>
                                    <td style={{padding:"8px 10px",color:"var(--tx3)",fontSize:10}}>{r.sno}</td>
                                    <td style={{padding:"8px 10px",fontWeight:600,color:r.col,whiteSpace:"nowrap",fontSize:12}}>{r.type}</td>
                                    <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:700,color:r.record>0?r.col:"var(--tx3)",fontSize:14}}>{r.record||"—"}</td>
                                    <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10.5,color:r.tds26?"var(--a)":"var(--tx3)"}}>{fmtT(r.tds26)}</td>
                                    <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10.5,color:r.tdsBk?"var(--grn)":"var(--tx3)"}}>{fmtT(r.tdsBk)}</td>
                                    <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10.5,fontWeight:600,
                                      color:Math.abs(diff)>1?(diff>0?"var(--red)":"var(--grn)"):"var(--tx3)"}}>
                                      {r.record>0 ? (Math.abs(diff)<1 ? "✓" : (diff>0?"+":"")+fmtT(diff)) : "—"}
                                    </td>
                                  </tr>
                                );
                              })}
                              {/* Total row */}
                              <tr style={{background:"#f0f4f8",borderTop:"2px solid var(--bd)"}}>
                                <td colSpan={2} style={{padding:"8px 10px",fontSize:11.5,fontWeight:700,color:"var(--tx)"}}>Total</td>
                                <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:15,fontWeight:700,color:"var(--tx)"}}>{selRR.length||"—"}</td>
                                <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10.5,fontWeight:600,color:"var(--a)"}}>{fmtT(sel26TDS)}</td>
                                <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10.5,fontWeight:600,color:"var(--grn)"}}>{fmtT(selBkTDS)}</td>
                                <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:10.5,fontWeight:700,color:Math.abs(selDiff)>1?(selDiff>0?"var(--red)":"var(--grn)"):"var(--grn)"}}>
                                  {sel26TDS||selBkTDS?(Math.abs(selDiff)<1?"✓ Nil":(selDiff>0?"+":"")+fmtT(selDiff)):"—"}
                                </td>
                              </tr>
                            </tbody>
                          </table>

                          </div>{/* end table wrapper */}
                          {/* Footer: record counts + action */}
                          <div style={{padding:"9px 14px",background:"var(--sur)",borderTop:"1px solid var(--bd)",display:"flex",alignItems:"center",gap:14,flexWrap:"wrap"}}>
                            <span style={{fontSize:10.5,color:"var(--tx2)"}}>26AS: <b style={{color:"var(--a)"}}>{selYd?.datasets?.["26AS"]?.length||0}</b></span>
                            <span style={{fontSize:10.5,color:"var(--tx2)"}}>Books: <b style={{color:"var(--grn)"}}>{selYd?.datasets?.["Books"]?.length||0}</b></span>
                            <div style={{marginLeft:"auto"}}>
                              <button onClick={()=>{setSelCompanyId(selC.id);setSelYear(dashFY);setView(selYd?.reconDone?"recon":"import");}}
                                style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:4,padding:"5px 14px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600,whiteSpace:"nowrap"}}>
                                {selYd?.reconDone?"View Reconciliation →":"Open & Import →"}
                              </button>
                            </div>
                          </div>

                        </div>{/* end right col */}

                      </div>
                    );

                  })()}
                </div>
              </div>
            )}

            {view==="home"&&(
              <div style={{flex:1,display:"flex",flexDirection:"column",overflow:"hidden",background:"var(--sur)"}}>
                {/* ── TOP STATS BAR ── */}
                {(()=>{
                  const activeClients = companies.filter(c=>c.status!=="archived");
                  const fyClients = activeClients.filter(c=>c.years?.[dashFY]);
                  const reconDoneCount = fyClients.filter(c=>c.years?.[dashFY]?.reconDone).length;
                  const issueCount = fyClients.filter(c=>{
                    const yd=c.years?.[dashFY]; if(!yd?.reconDone) return false;
                    return (yd.reconResults||[]).some(r=>r.matchStatus!=="Matched");
                  }).length;
                  const pendingCount = fyClients.filter(c=>!c.years?.[dashFY]?.reconDone && (c.years?.[dashFY]?.datasets?.["26AS"]?.length||0)>0).length;
                  const notStartedCount = activeClients.filter(c=>!c.years?.[dashFY]?.datasets?.["26AS"]?.length).length;
                  return (
                    <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"10px 18px",display:"flex",alignItems:"center",gap:0,flexShrink:0}}>
                      <div style={{fontSize:14,fontWeight:700,color:"var(--tx)",marginRight:20}}>Client Dashboard</div>
                      {[
                        {label:"Total Clients",val:activeClients.length,col:"var(--a)",bg:"#e6f3fb"},
                        {label:"Recon Done",val:reconDoneCount,col:"var(--grn)",bg:"#e8f8e8"},
                        {label:"Issues Found",val:issueCount,col:"var(--red)",bg:"#fde7e9"},
                        {label:"Pending Recon",val:pendingCount,col:"var(--amb)",bg:"#fff8e1"},
                        {label:"Not Started",val:notStartedCount,col:"#999",bg:"var(--sur)"},
                      ].map(s=>(
                        <div key={s.label} style={{display:"flex",alignItems:"center",gap:10,padding:"6px 18px",borderRight:"1px solid var(--bd)"}}>
                          <div style={{width:34,height:34,borderRadius:5,background:s.bg,display:"flex",alignItems:"center",justifyContent:"center",fontSize:17,fontWeight:700,color:s.col}}>{s.val}</div>
                          <div style={{fontSize:11,color:"var(--tx2)",lineHeight:1.4}}>{s.label}</div>
                        </div>
                      ))}
                      <div style={{marginLeft:"auto",display:"flex",alignItems:"center",gap:8}}>
                        <span style={{fontSize:11,color:"var(--tx2)"}}>FY</span>
                        <select value={dashFY} onChange={e=>setDashFY(e.target.value)} style={{border:"1px solid var(--bd)",borderRadius:3,padding:"4px 8px",fontSize:12,fontFamily:"inherit",background:"var(--wh)",color:"var(--tx)",outline:"none"}}>
                          {FY_LIST.map(fy=><option key={fy} value={fy}>{fy}</option>)}
                        </select>
                      </div>
                    </div>
                  );
                })()}

                {/* ── TOOLBAR ── */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"8px 18px",display:"flex",alignItems:"center",gap:8,flexShrink:0}}>
                  <div style={{position:"relative",flex:1,maxWidth:280}}>
                    <Ic d={I.search} s={12} c="#999" style={{position:"absolute",left:8,top:"50%",transform:"translateY(-50%)"}}/>
                    <input value={clientSearch} onChange={e=>setClientSearch(e.target.value)} placeholder="Search client, PAN, contact…" style={{width:"100%",padding:"6px 8px 6px 28px",border:"1px solid var(--bd)",borderRadius:4,fontSize:12,fontFamily:"inherit",outline:"none",background:"var(--sur)"}}/>
                  </div>
                  {/* Status filter */}
                  {["All","Active","Archived"].map(s=>(
                    <button key={s} onClick={()=>setClientFilterStatus(s)} style={{background:clientFilterStatus===s?"var(--a)":"var(--sur)",color:clientFilterStatus===s?"#fff":"var(--tx2)",border:"1px solid var(--bd)",borderRadius:3,padding:"5px 12px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit",fontWeight:clientFilterStatus===s?600:400}}>{s}</button>
                  ))}
                  <div style={{width:1,height:20,background:"var(--bd)",margin:"0 4px"}}/>
                  {/* Type filter */}
                  <select value={clientFilterType} onChange={e=>setClientFilterType(e.target.value)} style={{border:"1px solid var(--bd)",borderRadius:3,padding:"5px 8px",fontSize:11.5,fontFamily:"inherit",background:"var(--sur)",color:"var(--tx)",outline:"none"}}>
                    {["All Types","Corporate","Individual","Partnership","Trust","LLP","HUF"].map(t=><option key={t} value={t==="All Types"?"All":t}>{t}</option>)}
                  </select>
                  {/* Group filter */}
                  {(()=>{
                    const groups = [...new Set(companies.map(c=>c.group||"").filter(Boolean))];
                    return groups.length>0 ? (
                      <select value={clientFilterGroup} onChange={e=>setClientFilterGroup(e.target.value)} style={{border:"1px solid var(--bd)",borderRadius:3,padding:"5px 8px",fontSize:11.5,fontFamily:"inherit",background:"var(--sur)",color:"var(--tx)",outline:"none"}}>
                        <option value="All">All Groups</option>
                        {groups.map(g=><option key={g} value={g}>{g}</option>)}
                      </select>
                    ) : null;
                  })()}
                  <div style={{marginLeft:"auto",display:"flex",gap:6}}>
                    {/* Sort */}
                    <select value={clientSortBy} onChange={e=>setClientSortBy(e.target.value)} style={{border:"1px solid var(--bd)",borderRadius:3,padding:"5px 8px",fontSize:11.5,fontFamily:"inherit",background:"var(--sur)",color:"var(--tx)",outline:"none"}}>
                      <option value="name">Sort: Name</option>
                      <option value="addedOn">Sort: Date Added</option>
                      <option value="reconStatus">Sort: Recon Status</option>
                    </select>
                    {/* View toggle */}
                    {["table","grid"].map(m=>(
                      <button key={m} onClick={()=>setClientViewMode(m)} title={m==="table"?"Table View":"Grid View"} style={{background:clientViewMode===m?"var(--a)":"var(--sur)",color:clientViewMode===m?"#fff":"var(--tx2)",border:"1px solid var(--bd)",borderRadius:3,padding:"5px 9px",cursor:"pointer",fontSize:13}}>
                        {m==="table"?"☰":"⊞"}
                      </button>
                    ))}
                    <button onClick={()=>{setClientDraft({name:"",pan:"",gstin:"",contactPerson:"",phone:"",email:"",clientType:"Corporate",group:"",notes:""});setEditClientId(null);setShowAddClientModal(true);}} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:4,padding:"6px 14px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5}}>
                      + Add Client
                    </button>
                  </div>
                </div>

                {/* ── CLIENT LIST ── */}
                {(()=>{
                  let list = companies.filter(c=>{
                    if(clientFilterStatus==="Active" && c.status==="archived") return false;
                    if(clientFilterStatus==="Archived" && c.status!=="archived") return false;
                    if(clientFilterType!=="All" && c.clientType!==clientFilterType) return false;
                    if(clientFilterGroup!=="All" && (c.group||"")!==clientFilterGroup) return false;
                    if(clientSearch){
                      const q=clientSearch.toLowerCase();
                      if(!(c.name||"").toLowerCase().includes(q) && !(c.pan||"").toLowerCase().includes(q) && !(c.contactPerson||"").toLowerCase().includes(q) && !(c.gstin||"").toLowerCase().includes(q)) return false;
                    }
                    return true;
                  });
                  list = [...list].sort((a,b)=>{
                    if(clientSortBy==="name") return (a.name||"").localeCompare(b.name||"");
                    if(clientSortBy==="addedOn") return (b.addedOn||"").localeCompare(a.addedOn||"");
                    if(clientSortBy==="reconStatus"){
                      const score = c => c.years?.[dashFY]?.reconDone ? 0 : (c.years?.[dashFY]?.datasets?.["26AS"]?.length ? 1 : 2);
                      return score(a)-score(b);
                    }
                    return 0;
                  });
                  const getReconStatus = (c) => {
                    const yd = c.years?.[dashFY];
                    if(!yd) return {label:"No Data",col:"#bbb",bg:"#f5f5f5",dot:"#ccc"};
                    if(yd.reconDone){
                      const issues = (yd.reconResults||[]).filter(r=>r.matchStatus!=="Matched").length;
                      return issues>0
                        ? {label:`${issues} Issues`,col:"var(--red)",bg:"#fde7e9",dot:"var(--red)"}
                        : {label:"Clean ✓",col:"var(--grn)",bg:"#e8f8e8",dot:"var(--grn)"};
                    }
                    const has26 = yd.datasets?.["26AS"]?.length>0;
                    const hasBk = yd.datasets?.["Books"]?.length>0;
                    if(has26&&hasBk) return {label:"Ready to Recon",col:"var(--amb)",bg:"#fff8e1",dot:"var(--amb)"};
                    if(has26||hasBk) return {label:"Partial Data",col:"var(--a)",bg:"#e6f3fb",dot:"var(--a)"};
                    return {label:"Not Started",col:"#999",bg:"#f5f5f5",dot:"#ccc"};
                  };
                  const openClient = (c) => {
                    setSelCompanyId(c.id);
                    ensureYear(c.id, dashFY);
                    setSelYear(dashFY);
                    setView("import");
                  };
                  if(list.length===0) return (
                    <div style={{flex:1,display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",color:"var(--tx2)",gap:12}}>
                      <div style={{fontSize:40,opacity:0.3}}>👤</div>
                      <div style={{fontSize:14,fontWeight:500}}>No clients found</div>
                      <div style={{fontSize:12}}>Add your first client to get started</div>
                      <button onClick={()=>{setClientDraft({name:"",pan:"",gstin:"",contactPerson:"",phone:"",email:"",clientType:"Corporate",group:"",notes:""});setEditClientId(null);setShowAddClientModal(true);}} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:4,padding:"8px 18px",cursor:"pointer",fontSize:13,fontFamily:"inherit",fontWeight:600,marginTop:8}}>+ Add First Client</button>
                    </div>
                  );
                  if(clientViewMode==="grid") return (
                    <div style={{flex:1,overflow:"auto",padding:18,display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(260px,1fr))",gap:14,alignContent:"start"}}>
                      {list.map(c=>{
                        const rs = getReconStatus(c);
                        const yd = c.years?.[dashFY];
                        const r26 = yd?.datasets?.["26AS"]?.length||0;
                        const rBk = yd?.datasets?.["Books"]?.length||0;
                        return (
                          <div key={c.id} style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:7,padding:16,cursor:"pointer",transition:"all 0.12s",position:"relative"}}
                            onMouseEnter={e=>{e.currentTarget.style.borderColor="var(--a)";e.currentTarget.style.boxShadow="0 3px 16px rgba(0,120,212,0.10)";}}
                            onMouseLeave={e=>{e.currentTarget.style.borderColor="var(--bd)";e.currentTarget.style.boxShadow="none";}}>
                            <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",marginBottom:10}}>
                              <div style={{width:38,height:38,borderRadius:6,background:"var(--a-lt)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:16,fontWeight:700,color:"var(--a)",flexShrink:0}}>
                                {(c.name||"?")[0].toUpperCase()}
                              </div>
                              <span style={{background:rs.bg,color:rs.col,borderRadius:9,padding:"3px 9px",fontSize:10,fontWeight:600}}>{rs.label}</span>
                            </div>
                            <div style={{fontSize:13,fontWeight:600,marginBottom:2,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{c.name}</div>
                            <div style={{fontSize:11,color:"var(--tx2)",marginBottom:8}}>
                              {c.clientType && <span style={{background:"#f0f0f0",borderRadius:2,padding:"1px 5px",marginRight:4,fontSize:10}}>{c.clientType}</span>}
                              {c.pan && <span style={{fontFamily:"Consolas,monospace",fontSize:10}}>{c.pan}</span>}
                            </div>
                            {c.contactPerson && <div style={{fontSize:11,color:"var(--tx2)",marginBottom:2}}>👤 {c.contactPerson}</div>}
                            <div style={{display:"flex",gap:8,marginTop:8,paddingTop:8,borderTop:"1px solid var(--sur)",fontSize:10.5,color:"var(--tx2)"}}>
                              <span>26AS: <b style={{color:"var(--tx)"}}>{r26||"—"}</b></span>
                              <span>Books: <b style={{color:"var(--tx)"}}>{rBk||"—"}</b></span>
                            </div>
                            <div style={{display:"flex",gap:6,marginTop:10}}>
                              <button onClick={e=>{e.stopPropagation();openClient(c);}} style={{flex:1,background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"5px 0",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600}}>Open</button>
                              <button onClick={e=>{e.stopPropagation();setClientDraft({name:c.name,pan:c.pan||"",gstin:c.gstin||"",contactPerson:c.contactPerson||"",phone:c.phone||"",email:c.email||"",clientType:c.clientType||"Corporate",group:c.group||"",notes:c.notes||"",tracesTaxpayerPAN:c.tracesTaxpayerPAN||"",tracesTaxpayerPass:c.tracesTaxpayerPass||"",tracesDeductorTAN:c.tracesDeductorTAN||"",tracesDeductorPass:c.tracesDeductorPass||"",itPortalPAN:c.itPortalPAN||"",itPortalPass:c.itPortalPass||"",itPortalDOB:c.itPortalDOB||"",zipPassword:c.zipPassword||"",odooUrl:c.odooUrl||"",odooDatabase:c.odooDatabase||"",odooUsername:c.odooUsername||"",odooPassword:c.odooPassword||"",odooEnabled:c.odooEnabled||false});setEditClientId(c.id);setShowAddClientModal(true);}} style={{background:"var(--sur)",color:"var(--tx)",border:"1px solid var(--bd)",borderRadius:3,padding:"5px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit"}}>✏️</button>
                              <button onClick={e=>{e.stopPropagation();setCompanies(p=>p.map(x=>x.id===c.id?{...x,status:x.status==="archived"?"active":"archived"}:x));showToast(c.status==="archived"?"Client restored":"Client archived");}} style={{background:"var(--sur)",color:"var(--tx2)",border:"1px solid var(--bd)",borderRadius:3,padding:"5px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit"}}>{c.status==="archived"?"↩":"📦"}</button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  );
                  // TABLE VIEW
                  return (
                    <div style={{flex:1,overflow:"auto"}}>
                      <table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5}}>
                        <thead style={{position:"sticky",top:0,zIndex:2}}>
                          <tr style={{background:"var(--hb)"}}>
                            {["#","Client Name","Type","PAN / GSTIN","Contact","TRACES","26AS","Books","Recon Status","Actions"].map(h=>(
                              <th key={h} style={{padding:"9px 12px",textAlign:"left",fontSize:11,fontWeight:600,color:"var(--tx2)",borderBottom:"1px solid var(--bd)",whiteSpace:"nowrap"}}>{h}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {list.map((c,idx)=>{
                            const rs = getReconStatus(c);
                            const yd = c.years?.[dashFY];
                            const r26 = yd?.datasets?.["26AS"]?.length||0;
                            const rBk = yd?.datasets?.["Books"]?.length||0;
                            return (
                              <tr key={c.id} style={{borderBottom:"1px solid #f5f5f5",background:idx%2===0?"var(--wh)":"#fafafa",cursor:"pointer",transition:"background 0.1s"}}
                                onMouseEnter={e=>e.currentTarget.style.background="#f0f7ff"}
                                onMouseLeave={e=>e.currentTarget.style.background=idx%2===0?"var(--wh)":"#fafafa"}>
                                <td style={{padding:"9px 12px",color:"var(--tx3)",fontSize:11}}>{idx+1}</td>
                                <td style={{padding:"9px 12px"}}>
                                  <div style={{display:"flex",alignItems:"center",gap:9}}>
                                    <div style={{width:28,height:28,borderRadius:4,background:"var(--a-lt)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:12,fontWeight:700,color:"var(--a)",flexShrink:0}}>{(c.name||"?")[0].toUpperCase()}</div>
                                    <div>
                                      <div style={{fontWeight:600,color:"var(--tx)"}}>{c.name}</div>
                                      {c.group&&<div style={{fontSize:10,color:"var(--tx3)"}}>{c.group}</div>}
                                    </div>
                                  </div>
                                </td>
                                <td style={{padding:"9px 12px"}}>
                                  <span style={{background:"#f0f0f0",color:"var(--tx2)",borderRadius:2,padding:"2px 7px",fontSize:10.5,fontWeight:500}}>{c.clientType||"—"}</span>
                                </td>
                                <td style={{padding:"9px 12px",fontFamily:"Consolas,monospace",fontSize:11}}>
                                  {c.pan&&<div style={{color:"var(--tx)"}}>{c.pan}</div>}
                                  {c.gstin&&<div style={{color:"var(--tx2)",fontSize:10}}>{c.gstin}</div>}
                                  {!c.pan&&!c.gstin&&<span style={{color:"var(--tx3)"}}>—</span>}
                                </td>
                                <td style={{padding:"9px 12px"}}>
                                  {c.contactPerson&&<div style={{fontSize:12,color:"var(--tx)"}}>{c.contactPerson}</div>}
                                  {c.phone&&<div style={{fontSize:10.5,color:"var(--tx2)"}}>{c.phone}</div>}
                                  {!c.contactPerson&&!c.phone&&<span style={{color:"var(--tx3)"}}>—</span>}
                                </td>
                                <td style={{padding:"9px 12px",textAlign:"center"}}>
                                  {c.tracesPAN
                                    ? <span title={`PAN: ${c.tracesPAN} · Password saved`} style={{background:"#e6f3fb",color:"var(--a)",borderRadius:9,padding:"2px 8px",fontSize:10,fontWeight:600,cursor:"default"}}>🔐 Saved</span>
                                    : <span style={{color:"var(--tx3)",fontSize:11}}>—</span>
                                  }
                                </td>
                                <td style={{padding:"9px 12px",fontFamily:"Consolas,monospace",color:r26?"var(--a)":"var(--tx3)",fontWeight:r26?600:400}}>{r26||"—"}</td>
                                <td style={{padding:"9px 12px",fontFamily:"Consolas,monospace",color:rBk?"var(--grn)":"var(--tx3)",fontWeight:rBk?600:400}}>{rBk||"—"}</td>
                                <td style={{padding:"9px 12px"}}>
                                  <div style={{display:"flex",alignItems:"center",gap:5}}>
                                    <div style={{width:7,height:7,borderRadius:"50%",background:rs.dot,flexShrink:0}}/>
                                    <span style={{background:rs.bg,color:rs.col,borderRadius:9,padding:"2px 8px",fontSize:10.5,fontWeight:600,whiteSpace:"nowrap"}}>{rs.label}</span>
                                  </div>
                                </td>
                                <td style={{padding:"9px 12px"}}>
                                  <div style={{display:"flex",gap:5}}>
                                    <button onClick={()=>openClient(c)} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"4px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600}}>Open</button>
                                    <button onClick={e=>{e.stopPropagation();setClientDraft({name:c.name,pan:c.pan||"",gstin:c.gstin||"",contactPerson:c.contactPerson||"",phone:c.phone||"",email:c.email||"",clientType:c.clientType||"Corporate",group:c.group||"",notes:c.notes||"",tracesTaxpayerPAN:c.tracesTaxpayerPAN||"",tracesTaxpayerPass:c.tracesTaxpayerPass||"",tracesDeductorTAN:c.tracesDeductorTAN||"",tracesDeductorPass:c.tracesDeductorPass||"",itPortalPAN:c.itPortalPAN||"",itPortalPass:c.itPortalPass||"",itPortalDOB:c.itPortalDOB||"",zipPassword:c.zipPassword||"",odooUrl:c.odooUrl||"",odooDatabase:c.odooDatabase||"",odooUsername:c.odooUsername||"",odooPassword:c.odooPassword||"",odooEnabled:c.odooEnabled||false});setEditClientId(c.id);setShowAddClientModal(true);}} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"4px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",color:"var(--tx2)"}}>✏️</button>
                                    <button onClick={()=>{setCompanies(p=>p.map(x=>x.id===c.id?{...x,status:x.status==="archived"?"active":"archived"}:x));showToast(c.status==="archived"?"Client restored":"Client archived");}} title={c.status==="archived"?"Restore":"Archive"} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"4px 8px",cursor:"pointer",fontSize:11,color:"var(--tx2)"}}>
                                      {c.status==="archived"?"↩":"📦"}
                                    </button>
                                    {companies.length>1&&<button onClick={()=>deleteCompany(c.id)} title="Delete" style={{background:"none",border:"1px solid #fcd0d3",borderRadius:3,padding:"4px 8px",cursor:"pointer",fontSize:11,color:"var(--red)"}}>🗑</button>}
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  );
                })()}
              </div>
            )}

            {view==="import"&&(
              <div style={{flex:1,display:"flex",flexDirection:"column",overflow:"hidden"}}>
                {/* ── Tab bar ── */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"0 16px",display:"flex",alignItems:"center",flexShrink:0}}>
                  {[{id:"file",label:"📂 File Import"},{id:"traces",label:"🌐 TRACES Portal"},{id:"itportal",label:"🏛️ IT Portal (26AS)"}].map(t=>(
                    <div key={t.id} onClick={()=>setImportTab(t.id)}
                      style={{padding:"10px 20px",fontSize:12.5,fontWeight:500,cursor:"pointer",borderBottom:`2px solid ${importTab===t.id?"var(--a)":"transparent"}`,color:importTab===t.id?"var(--a)":"var(--tx2)",marginBottom:-1,transition:"all 0.1s",userSelect:"none",display:"flex",alignItems:"center",gap:6}}>
                      {t.label}
                      {t.id==="traces"&&tracesNewFiles.filter(f=>!tracesDismissed.has(f.id)).length>0&&(
                        <span style={{background:"var(--red)",color:"#fff",borderRadius:8,padding:"1px 6px",fontSize:10,fontWeight:700}}>{tracesNewFiles.filter(f=>!tracesDismissed.has(f.id)).length}</span>
                      )}
                    </div>
                  ))}
                </div>
                {/* ── File Import Panel ── */}
                {importTab==="file"&&(
                  <div className="imp">
                    <div className="ih">Import Data Files</div>
                    <div className="is">Supports 26AS .txt (TRACES ^ format), .xml, .csv, .xlsx, .zip. Drag & drop or click browse.</div>
                    <div className={`drop${isDragging?" ov":""}`} onDragOver={e=>{e.preventDefault();setIsDragging(true);}} onDragLeave={()=>setIsDragging(false)} onDrop={e=>{e.preventDefault();setIsDragging(false);[...e.dataTransfer.files].forEach(processFile);}} onClick={()=>fileInputRef.current?.click()}>
                      <div className="di"><Ic d={I.import} s={24} c="#0078d4"/></div>
                      <h2>Drop files here</h2><p>26AS .txt · XML · CSV · Excel (XLSX) · ZIP</p>
                      <button className="ib" onClick={e=>{e.stopPropagation();fileInputRef.current?.click();}}>Browse Files</button>
                    </div>
                    {importing&&<div className="pw"><div className="ph"><span style={{fontWeight:500}}>Processing...</span><span style={{color:"var(--a)"}}>{progress}%</span></div><div className="pt"><div className="pf" style={{width:progress+"%"}}/></div></div>}
                    {log.length>0&&<div className="lw"><div className="ll">Import Log</div><div className="lg">{log.map((l,i)=><div key={i} className={l.type==="s"?"ls":l.type==="w"?"lw2":l.type==="e"?"le":"li"} style={{marginBottom:1}}><span style={{color:"#555"}}>[{l.t}]</span> {l.msg}</div>)}</div></div>}
                    <div className="fg">
                      {[{title:"26AS — TRACES Text (.txt)",col:"#107c10",items:["Login TRACES → tdscpc.gov.in","View 26AS → Download as Text (.txt)","File uses ^ (caret) as separator","Parser reads all sections & auto-assigns quarters"]},{title:"Books — CSV/Excel",col:"#0078d4",items:["Tally: Display → TDS Reports → Export CSV","SAP: FBL1N → List → Export","Busy/Marg: Reports → TDS → Export","Required: Party Name, TAN, Amount, TDS, Date"]},{title:"Invoice Details — CSV/Excel",col:"#d59300",items:["Import invoice dates separately","Required: Invoice No + Invoice Date","Updates Books entries & recalculates quarters","Click 'Import Invoice' button after Books import"]},{title:"AIS / TIS — CSV",col:"#5c2d91",items:["Income Tax portal → AIS section","Download Annual Information Statement","Name file starting with \'AIS_\'","Phase 3: 3-way reconciliation"]}].map(f=>(
                        <div className="fc" key={f.title}><div className="fh"><div className="fe" style={{background:f.col}}><Ic d={I.file} s={14} c="#fff"/></div><h3>{f.title}</h3></div><ul>{f.items.map(it=><li key={it}>{it}</li>)}</ul></div>
                      ))}
                    </div>
                    {files.length>0&&(
                      <div className="fl"><h3>Imported Files ({files.length})</h3>
                        {files.map(f=>(
                          <div className="fr" key={f.id}>
                            <div className={`fx fx-${f.ext}`}>.{f.ext.toUpperCase()}</div>
                            <div style={{flex:1,overflow:"hidden"}}><div className="fn">{f.name}</div><div className="fm">{f.rows} records · {f.size} · {f.time}</div></div>
                            <span className={`fb2 fb-${f.category.toLowerCase().replace("/","")}`}>{f.category}</span>
                            <button className="fvb" onClick={()=>{setSelDS(f.category);setView("data");}}>View</button>
                            <button className="fdb" onClick={()=>{setFiles(p=>p.filter(x=>x.id!==f.id));showToast("Removed");}}><Ic d={I.trash} s={13} c="currentColor"/></button>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
                {/* ── TRACES Portal Panel ── */}
                {importTab==="traces"&&(
                  <div style={{flex:1,display:"flex",flexDirection:"column",background:"var(--sur)",overflow:"hidden"}}>
                    <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"10px 16px",display:"flex",alignItems:"center",gap:10,flexShrink:0}}>
                      <div style={{width:32,height:32,borderRadius:5,background:"#e6f3fb",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0}}>
                        <Ic d={I.download} s={16} c="var(--a)" sw={2}/>
                      </div>
                      <div style={{flex:1}}>
                        <div style={{fontSize:14,fontWeight:600,color:"var(--tx)"}}>TRACES Portal</div>
                        <div style={{fontSize:11,color:"var(--tx2)"}}>Login below · App auto-detects downloads · One-click import</div>
                      </div>
                      <div style={{display:"flex",alignItems:"center",gap:8}}>
                        {tracesPortalOpen&&<div style={{fontSize:11,color:"var(--grn)",background:"#e8f8e8",border:"1px solid #c3e6c3",borderRadius:3,padding:"3px 8px",display:"flex",alignItems:"center",gap:4}}><div style={{width:6,height:6,borderRadius:"50%",background:"var(--grn)"}}/>Watching Downloads</div>}
                        {!tracesPortalOpen
                          ? <button onClick={async()=>{
                              if(!isElectron){showToast("TRACES Portal requires the desktop app","w");return;}
                              if(itPortalOpen){showToast("Close IT Portal first before opening TRACES Portal","w");return;}
                              // Read credentials from current company
                              const hasCreds = curCompany?.tracesTaxpayerPAN || curCompany?.tracesDeductorTAN;
                              try {
                                await window.electronAPI.openTracesPortal(hasCreds?{
                                  taxpayerPAN: curCompany.tracesTaxpayerPAN||"",
                                  taxpayerPass: curCompany.tracesTaxpayerPass||"",
                                  deductorTAN: curCompany.tracesDeductorTAN||"",
                                  deductorPass: curCompany.tracesDeductorPass||"",
                                  portalType:"TRACES"
                                }:{portalType:"TRACES"});
                                setTracesPortalOpen(true);setTracesStatus("open");
                              } catch(e) {
                                showToast("Failed to open TRACES portal","e");
                              }
                            }} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:4,padding:"7px 16px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5}}>
                              <Ic d={I.play} s={12} c="#fff"/> Open TRACES Portal
                            </button>
                          : <button onClick={closeTracesPortalFn} disabled={tracesClosing}
                              style={{background:tracesClosing?"#c0392b":"var(--red)",color:"#fff",border:"none",borderRadius:4,padding:"7px 16px",cursor:tracesClosing?"not-allowed":"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5,opacity:tracesClosing?0.75:1}}>
                              {tracesClosing
                                ? <><span style={{width:11,height:11,border:"2px solid rgba(255,255,255,0.35)",borderTopColor:"#fff",borderRadius:"50%",display:"inline-block",animation:"spin 0.7s linear infinite"}}/> Closing…</>
                                : <><Ic d={I.close} s={12} c="#fff"/> Close Portal</>}
                            </button>
                        }
                      </div>
                    </div>
                    {tracesPortalOpen&&(
                      <div id="traces-browser-container" style={{flex:1,background:"#fff",minHeight:0,display:"flex",alignItems:"center",justifyContent:"center",color:"var(--tx3)",fontSize:12.5,flexDirection:"column",gap:8}}>
                        <Ic d={I.download} s={28} c="#ccc" sw={1}/>
                        <span>TRACES is loading in the panel above…</span>
                        <span style={{fontSize:11}}>If you don\'t see it, try closing and reopening the portal.</span>
                      </div>
                    )}
                    {!tracesPortalOpen&&(
                      <div style={{flex:1,overflow:"auto",padding:"16px"}}>
                        {/* ── Current 26AS Data Status ── */}
                        {(()=>{
                          const d26 = datasets["26AS"] || [];
                          const lastFile = files.filter(f=>f.category==="26AS").sort((a,b)=>b.id-a.id)[0];
                          if(!d26.length) return (
                            <div style={{background:"#fff4e0",border:"1px solid #ffd591",borderRadius:5,padding:"10px 14px",marginBottom:14,display:"flex",alignItems:"center",gap:10,fontSize:12}}>
                              <span style={{fontSize:18}}>📂</span>
                              <div style={{flex:1,color:"#7a4500"}}>
                                <b>No 26AS data yet for {selYear}.</b> Open TRACES Portal, download 26AS and it will import automatically.
                              </div>
                            </div>
                          );
                          return (
                            <div style={{background:"#e6f3fb",border:"1px solid #b3d4f5",borderRadius:5,padding:"10px 14px",marginBottom:14,fontSize:12}}>
                              <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
                                <span style={{fontSize:16}}>📊</span>
                                <b style={{color:"#003d7a",fontSize:13}}>26AS Data — {selYear}</b>
                                <span style={{marginLeft:"auto",background:"#0078d4",color:"#fff",borderRadius:10,padding:"2px 10px",fontSize:11,fontWeight:700}}>{d26.length.toLocaleString()} records</span>
                              </div>
                              <div style={{display:"flex",gap:20,flexWrap:"wrap",fontSize:11,color:"#1a3a5c"}}>
                                <span>💰 Total TDS: <b>₹{d26.reduce((s,r)=>s+(r.tdsDeducted||0),0).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2})}</b></span>
                                <span>🏢 Deductors: <b>{new Set(d26.map(r=>r.tan).filter(Boolean)).size}</b></span>
                                {lastFile&&<span>📁 File: <b>{lastFile.name}</b></span>}
                                {lastFile&&<span>🕐 Imported: <b>{lastFile.time}</b></span>}
                              </div>
                              <div style={{marginTop:6,fontSize:10.5,color:"#555",background:"rgba(255,255,255,0.6)",borderRadius:3,padding:"4px 8px"}}>
                                ✅ This data is saved and will remain until you download a fresh 26AS from TRACES.
                              </div>
                            </div>
                          );
                        })()}
                        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14,maxWidth:900}}>
                          <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"14px 16px",gridColumn:"1/-1"}}>
                            <div style={{fontWeight:600,fontSize:12.5,marginBottom:10}}>How It Works</div>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10}}>
                              {[{n:"1",t:"Save Credentials",d:"Click Credentials above, enter PAN/TAN + password. Stored encrypted.",c:"var(--a)"},{n:"2",t:"Login Normally",d:"Enter PAN, password and OTP. Nothing is stored.",c:"#107c10"},{n:"3",t:"Download File",d:"Navigate to 26AS/AIS and download as usual.",c:"var(--pur)"},{n:"4",t:"Auto Import",d:"App detects the file instantly. Click Import Now.",c:"var(--amb)"}].map(s=>(
                                <div key={s.n} style={{display:"flex",gap:8}}>
                                  <div style={{width:22,height:22,borderRadius:"50%",background:s.c,color:"#fff",display:"flex",alignItems:"center",justifyContent:"center",fontSize:11,fontWeight:700,flexShrink:0}}>{s.n}</div>
                                  <div><div style={{fontSize:12,fontWeight:600,marginBottom:2}}>{s.t}</div><div style={{fontSize:11,color:"var(--tx2)",lineHeight:1.5}}>{s.d}</div></div>
                                </div>
                              ))}
                            </div>
                          </div>
                          <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"14px 16px"}}>
                            <div style={{fontWeight:600,fontSize:12.5,marginBottom:10}}>TRACES Download Paths</div>
                            {[{label:"Form 26AS",path:"Taxpayer Login → Form 26AS → View/Download → FY → Download TXT",tag:"26AS"},{label:"AIS",path:"IT Portal → e-File → View AIS → Download CSV",tag:"AIS"},{label:"TIS",path:"Inside AIS → TIS tab → Download CSV",tag:"TIS"},{label:"Form 16A",path:"TRACES → Deductee → Request → Form 16A",tag:"16A"}].map(l=>(
                              <div key={l.label} style={{display:"flex",gap:7,marginBottom:8,paddingBottom:8,borderBottom:"1px solid var(--sur)"}}>
                                <span style={{background:"var(--a-lt)",color:"var(--a)",borderRadius:2,padding:"1px 5px",fontSize:9,fontWeight:700,fontFamily:"Consolas,monospace",flexShrink:0,marginTop:2}}>{l.tag}</span>
                                <div><div style={{fontSize:12,fontWeight:500}}>{l.label}</div><div style={{fontSize:10.5,color:"var(--tx2)",lineHeight:1.5}}>{l.path}</div></div>
                              </div>
                            ))}
                          </div>
                          <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"14px 16px"}}>
                            <div style={{fontWeight:600,fontSize:12.5,marginBottom:10}}>Download Watcher</div>
                            <div style={{display:"flex",alignItems:"center",gap:8,padding:"8px 12px",background:"var(--sur)",borderRadius:4,marginBottom:10}}>
                              <div style={{width:8,height:8,borderRadius:"50%",background:tracesStatus==="detected"?"var(--grn)":tracesStatus==="open"||tracesStatus==="watching"?"var(--amb)":"#ccc"}}/>
                              <div style={{fontSize:12}}>
                                {tracesStatus==="idle"&&"Inactive · Open portal to start"}
                                {tracesStatus==="open"&&"Watching ~/Downloads…"}
                                {tracesStatus==="watching"&&"Watching (portal closed)"}
                                {tracesStatus==="detected"&&"File detected! See banner above."}
                              </div>
                            </div>
                            {["26AS_*.txt","26AS_*.zip","AIS_*.csv","TIS_*.csv","*.xlsx / *.csv"].map(t=>(
                              <div key={t} style={{display:"flex",gap:5,fontSize:11,color:"var(--tx2)",lineHeight:1.9}}><span style={{color:"var(--grn)",fontWeight:700}}>✓</span>{t}</div>
                            ))}
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}
                {/* ── IT Portal Panel ── */}
                {importTab==="itportal"&&(
              <div style={{flex:1,display:"flex",flexDirection:"column",background:"var(--sur)",overflow:"hidden"}}>
                {/* Header */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"10px 16px",display:"flex",alignItems:"center",gap:10,flexShrink:0}}>
                  <div style={{width:32,height:32,borderRadius:5,background:"#e8f8e8",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,fontSize:17}}>🏛️</div>
                  <div style={{flex:1}}>
                    <div style={{fontSize:14,fontWeight:600,color:"var(--tx)"}}>Income Tax Portal — 26AS Download</div>
                    <div style={{fontSize:11,color:"var(--tx2)"}}>Login at incometax.gov.in · App auto-detects downloads · One-click import</div>
                  </div>
                  <div style={{display:"flex",alignItems:"center",gap:8}}>
                    {itPortalOpen&&<div style={{fontSize:11,color:"var(--grn)",background:"#e8f8e8",border:"1px solid #c3e6c3",borderRadius:3,padding:"3px 8px",display:"flex",alignItems:"center",gap:4}}><div style={{width:6,height:6,borderRadius:"50%",background:"var(--grn)"}}/>Watching Downloads</div>}
                    {!itPortalOpen
                      ? <button onClick={async()=>{
                          if(!isElectron){showToast("IT Portal requires the desktop app","w");return;}
                          if(tracesPortalOpen){showToast("Close TRACES Portal first before opening IT Portal","w");return;}
                          try {
                            // Prepare credentials from current company
                            const credentials = {
                              url: "https://eportal.incometax.gov.in/iec/foservices/#/login",
                              portalType: "IT",
                              pan: curCompany?.itPortalPAN || "",
                              password: curCompany?.itPortalPass || ""
                            };
                            
                            // Use openITPortal if available, otherwise fall back to openTracesPortal with credentials
                            if (window.electronAPI.openITPortal) {
                              await window.electronAPI.openITPortal(credentials);
                            } else {
                              await window.electronAPI.openTracesPortal(credentials);
                            }
                            setItPortalOpen(true); setItPortalStatus("open");
                          } catch(e) { showToast("Failed to open IT Portal: "+e.message,"e"); }
                        }} style={{background:"#107c10",color:"#fff",border:"none",borderRadius:4,padding:"7px 16px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5}}>
                          <Ic d={I.play} s={12} c="#fff"/> Open IT Portal
                        </button>
                      : <button onClick={closeITPortalFn} disabled={itPortalClosing}
                          style={{background:itPortalClosing?"#c0392b":"var(--red)",color:"#fff",border:"none",borderRadius:4,padding:"7px 16px",cursor:itPortalClosing?"not-allowed":"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5,opacity:itPortalClosing?0.75:1}}>
                          {itPortalClosing?<><span style={{width:11,height:11,border:"2px solid rgba(255,255,255,0.35)",borderTopColor:"#fff",borderRadius:"50%",display:"inline-block",animation:"spin 0.7s linear infinite"}}/> Closing…</>:<><Ic d={I.close} s={12} c="#fff"/> Close Portal</>}
                        </button>
                    }
                  </div>
                </div>

                {/* ── Embedded browser container (Electron mounts WebView here) ── */}
                {itPortalOpen&&(
                  <div id="it-portal-browser-container" style={{flex:1,background:"#fff",minHeight:0,display:"flex",alignItems:"center",justifyContent:"center",color:"var(--tx3)",fontSize:12.5,flexDirection:"column",gap:8}}>
                    <Ic d={I.download} s={28} c="#ccc" sw={1}/>
                    <span>IT Portal is loading in the panel above…</span>
                    <span style={{fontSize:11}}>If you don't see it, try closing and reopening.</span>
                  </div>
                )}

                {/* ── Info / guide — shown only when portal is closed ── */}
                {!itPortalOpen&&(
                <div style={{flex:1,overflow:"auto",padding:"16px"}}>
                  {/* Current 26AS data status */}
                  {(()=>{
                    const d26=datasets["26AS"]||[];
                    const lastFile=files.filter(f=>f.category==="26AS").sort((a,b)=>b.id-a.id)[0];
                    if(!d26.length) return(
                      <div style={{background:"#fff4e0",border:"1px solid #ffd591",borderRadius:5,padding:"10px 14px",marginBottom:14,display:"flex",alignItems:"center",gap:10,fontSize:12}}>
                        <span style={{fontSize:18}}>📂</span>
                        <div style={{flex:1,color:"#7a4500"}}><b>No 26AS data yet for {selYear}.</b> Open IT Portal → download Annual Tax Statement (26AS) → it will import automatically.</div>
                      </div>
                    );
                    return(
                      <div style={{background:"#e6f3fb",border:"1px solid #b3d4f5",borderRadius:5,padding:"10px 14px",marginBottom:14,fontSize:12}}>
                        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
                          <span style={{fontSize:16}}>📊</span>
                          <b style={{color:"#003d7a",fontSize:13}}>26AS Data — {selYear}</b>
                          <span style={{marginLeft:"auto",background:"#0078d4",color:"#fff",borderRadius:10,padding:"2px 10px",fontSize:11,fontWeight:700}}>{d26.length.toLocaleString()} records</span>
                        </div>
                        <div style={{display:"flex",gap:20,flexWrap:"wrap",fontSize:11,color:"#1a3a5c"}}>
                          <span>💰 Total TDS: <b>₹{d26.reduce((s,r)=>s+(r.tdsDeducted||0),0).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2})}</b></span>
                          <span>🏢 Deductors: <b>{new Set(d26.map(r=>r.tan).filter(Boolean)).size}</b></span>
                          {lastFile&&<span>📁 File: <b>{lastFile.name}</b></span>}
                        </div>
                      </div>
                    );
                  })()}

                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14,maxWidth:900}}>
                    {/* How It Works */}
                    <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"14px 16px",gridColumn:"1/-1"}}>
                      <div style={{fontWeight:600,fontSize:12.5,marginBottom:10}}>How It Works</div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10}}>
                        {[{n:"1",t:"Save Credentials",d:"Click Credentials above. Each client has its own PAN + password stored separately.",c:"var(--a)"},{n:"2",t:"Open IT Portal",d:"Portal opens at incometax.gov.in. Enter OTP when prompted — nothing is stored.",c:"#107c10"},{n:"3",t:"Download 26AS",d:"Go to e-File → View AIS/26AS → Annual Tax Statement → Download TXT.",c:"var(--pur)"},{n:"4",t:"Auto Import",d:"App detects the file instantly and imports it for this client automatically.",c:"var(--amb)"}].map(s=>(
                          <div key={s.n} style={{display:"flex",gap:8}}>
                            <div style={{width:22,height:22,borderRadius:"50%",background:s.c,color:"#fff",display:"flex",alignItems:"center",justifyContent:"center",fontSize:11,fontWeight:700,flexShrink:0}}>{s.n}</div>
                            <div><div style={{fontSize:12,fontWeight:600,marginBottom:2}}>{s.t}</div><div style={{fontSize:11,color:"var(--tx2)",lineHeight:1.5}}>{s.d}</div></div>
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Download Paths */}
                    <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"14px 16px"}}>
                      <div style={{fontWeight:600,fontSize:12.5,marginBottom:10}}>IT Portal Download Paths</div>
                      {[
                        {label:"26AS / Annual Tax Statement",path:"Login → e-File → Income Tax Returns → View Filed Returns OR e-File → View AIS → Annual Tax Statement → Download",tag:"26AS"},
                        {label:"AIS",path:"Login → e-File → View AIS → Download CSV",tag:"AIS"},
                        {label:"TIS",path:"Inside AIS → TIS tab → Download CSV",tag:"TIS"},
                      ].map(l=>(
                        <div key={l.label} style={{display:"flex",gap:7,marginBottom:8,paddingBottom:8,borderBottom:"1px solid var(--sur)"}}>
                          <span style={{background:"#e8f8e8",color:"#107c10",borderRadius:2,padding:"1px 5px",fontSize:9,fontWeight:700,fontFamily:"Consolas,monospace",flexShrink:0,marginTop:2}}>{l.tag}</span>
                          <div><div style={{fontSize:12,fontWeight:500}}>{l.label}</div><div style={{fontSize:10.5,color:"var(--tx2)",lineHeight:1.5}}>{l.path}</div></div>
                        </div>
                      ))}
                    </div>

                    {/* Download Watcher */}
                    <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:6,padding:"14px 16px"}}>
                      <div style={{fontWeight:600,fontSize:12.5,marginBottom:10}}>Download Watcher</div>
                      <div style={{display:"flex",alignItems:"center",gap:8,padding:"8px 12px",background:"var(--sur)",borderRadius:4,marginBottom:10}}>
                        <div style={{width:8,height:8,borderRadius:"50%",background:itPortalStatus==="open"?"var(--amb)":"#ccc"}}/>
                        <div style={{fontSize:12}}>
                          {itPortalStatus==="idle"&&"Inactive · Open portal to start"}
                          {itPortalStatus==="open"&&"Watching ~/Downloads…"}
                        </div>
                      </div>
                      {["26AS_*.txt","26AS_*.zip","AIS_*.csv","TIS_*.csv","*.xlsx / *.csv"].map(t=>(
                        <div key={t} style={{display:"flex",gap:5,fontSize:11,color:"var(--tx2)",lineHeight:1.9}}><span style={{color:"var(--grn)",fontWeight:700}}>✓</span>{t}</div>
                      ))}
                      <div style={{marginTop:10,padding:"8px 10px",background:"#f0f8ff",borderRadius:4,fontSize:11,color:"#1a3a5c",lineHeight:1.6}}>
                        💡 <b>Tip:</b> Credentials are saved per-client. Switching companies will use that company's IT Portal login automatically.
                      </div>
                    </div>
                  </div>
                </div>
                )}
              </div>
              )}
            </div>
            )}

            {/* ── IT PORTAL CREDENTIALS MODAL ── */}
            {itPortalCredsOpen&&(
              <div className="modal-bg" onClick={e=>e.target===e.currentTarget&&setItPortalCredsOpen(false)}>
                <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:7,padding:28,width:440,boxShadow:"0 8px 40px rgba(0,0,0,0.22)"}}>
                  <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:4}}>
                    <div style={{width:36,height:36,borderRadius:5,background:"#e8f8e8",display:"flex",alignItems:"center",justifyContent:"center",fontSize:20}}>🏛️</div>
                    <div>
                      <div style={{fontSize:15,fontWeight:600}}>IT Portal Credentials</div>
                      <div style={{fontSize:11.5,color:"var(--tx2)"}}>Client: <b>{curCompany?.name}</b> · Stored per-client, encrypted</div>
                    </div>
                  </div>
                  <div style={{background:"#e8f8e8",border:"1px solid #c3e6c3",borderRadius:4,padding:"8px 12px",margin:"12px 0",fontSize:11.5,color:"#107c10",lineHeight:1.6}}>
                    🔒 Each client's credentials are stored separately and never shared across companies.
                  </div>
                  <div style={{marginBottom:14}}>
                    <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>PAN (Taxpayer)</div>
                    <input value={itPortalDraft.pan||""} onChange={e=>setItPortalDraft(p=>({...p,pan:e.target.value.toUpperCase()}))} placeholder="ABCDE1234F" maxLength={10}
                      style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,fontFamily:"Consolas,monospace",outline:"none",boxSizing:"border-box"}}
                      onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                  </div>
                  <div style={{marginBottom:14}}>
                    <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>Password</div>
                    <input type="password" value={itPortalDraft.password||""} onChange={e=>setItPortalDraft(p=>({...p,password:e.target.value}))} placeholder="IT Portal password"
                      style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,outline:"none",boxSizing:"border-box"}}
                      onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                  </div>
                  <div style={{marginBottom:20}}>
                    <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>Date of Birth / Incorporation (for ZIP password)</div>
                    <div style={{display:"flex",alignItems:"center",gap:10}}>
                      <input type="date" value={itPortalDraft.dob||""} onChange={e=>setItPortalDraft(p=>({...p,dob:e.target.value}))}
                        style={{flex:1,border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,fontFamily:"inherit",outline:"none",boxSizing:"border-box"}}
                        onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                      {itPortalDraft.dob&&(
                        <div style={{flexShrink:0,textAlign:"center"}}>
                          <div style={{fontSize:10,color:"var(--tx2)",marginBottom:2}}>ZIP Password</div>
                          <div style={{fontFamily:"Consolas,monospace",fontSize:14,fontWeight:700,color:"#107c10",background:"#e8f8e8",borderRadius:4,padding:"5px 10px",letterSpacing:1}}>
                            {itPortalDraft.dob.replace(/-/g,"").replace(/^(\d{4})(\d{2})(\d{2})$/,"$3$2$1")}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                    <button onClick={()=>{
                      if(window.confirm(`Clear IT Portal credentials for ${curCompany?.name}?`)){
                        setItPortalCredsMap(prev=>({...prev,[selCompanyId]:{pan:"",password:"",dob:"",savedAt:null}}));
                        setItPortalCredsOpen(false);showToast("IT Portal credentials cleared");
                      }
                    }} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 14px",cursor:"pointer",fontSize:12,fontFamily:"inherit",color:"var(--red)"}}>Clear</button>
                    <div style={{display:"flex",gap:8}}>
                      <button onClick={()=>setItPortalCredsOpen(false)} style={{border:"1px solid var(--bd)",background:"none",borderRadius:3,padding:"7px 16px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit"}}>Cancel</button>
                      <button onClick={async()=>{
                        setItPortalSaving(true);
                        try{
                          const saved={pan:(itPortalDraft.pan||"").toUpperCase().trim(),dob:(itPortalDraft.dob||""),savedAt:Date.now()};
                          if(isElectron&&itPortalDraft.password){
                            saved.password = await window.electronAPI.tracesEncrypt(itPortalDraft.password);
                          }
                          setItPortalCredsMap(prev=>({...prev,[selCompanyId]:saved}));
                          setItPortalCredsOpen(false);
                          showToast(`✅ IT Portal credentials saved for ${curCompany?.name}`,"s");
                        }catch(e){showToast("Save failed: "+e.message,"e");}
                        setItPortalSaving(false);
                      }} disabled={itPortalSaving} style={{background:"#107c10",color:"#fff",border:"none",borderRadius:3,padding:"7px 18px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:600,opacity:itPortalSaving?0.6:1}}>
                        {itPortalSaving?"Saving…":"🔐 Save Securely"}
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* ── TRACES CREDENTIALS MODAL ── */}
            {tracesCredsOpen&&(
              <div className="modal-bg" onClick={e=>e.target===e.currentTarget&&setTracesCredsOpen(false)}>
                <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:7,padding:28,width:440,boxShadow:"0 8px 40px rgba(0,0,0,0.22)"}}>
                  <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:4}}>
                    <div style={{width:36,height:36,borderRadius:5,background:"#e6f3fb",display:"flex",alignItems:"center",justifyContent:"center",fontSize:20}}>🔐</div>
                    <div><div style={{fontSize:15,fontWeight:600}}>TRACES Credentials</div><div style={{fontSize:11.5,color:"var(--tx2)"}}>Client: <b>{curCompany?.name}</b> · Stored per-client, encrypted</div></div>
                  </div>
                  <div style={{background:"#fff8e1",border:"1px solid #ffe082",borderRadius:4,padding:"8px 12px",margin:"12px 0",fontSize:11.5,color:"#795548",lineHeight:1.6}}>
                    🔒 Passwords are encrypted with AES-256 before saving. Only captcha + OTP need manual entry.
                  </div>
                  {/* ── ZIP Password Date ── */}
                  <div style={{background:"#f0f8ff",border:"1px solid #b3d4f5",borderRadius:4,padding:"10px 14px",marginBottom:14}}>
                    <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:6}}>
                      <span style={{fontSize:15}}>📦</span>
                      <div style={{fontSize:12,fontWeight:700,color:"#0050a0"}}>26AS ZIP Password</div>
                    </div>
                    <div style={{fontSize:11,color:"#555",marginBottom:8,lineHeight:1.5}}>
                      TRACES encrypts the downloaded ZIP with your Date of Birth / Date of Incorporation in <b>ddmmyyyy</b> format (e.g. 01011990).
                    </div>
                    <div style={{display:"flex",alignItems:"center",gap:10}}>
                      <div style={{flex:1}}>
                        <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>Date of Birth / Incorporation</div>
                        <input type="date" value={tracesDraft.zipDate||""} onChange={e=>setTracesDraft(p=>({...p,zipDate:e.target.value}))}
                          style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,fontFamily:"inherit",outline:"none",boxSizing:"border-box"}}
                          onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                      </div>
                      {tracesDraft.zipDate&&(
                        <div style={{flexShrink:0,textAlign:"center"}}>
                          <div style={{fontSize:10,color:"var(--tx2)",marginBottom:2}}>ZIP Password</div>
                          <div style={{fontFamily:"Consolas,monospace",fontSize:14,fontWeight:700,color:"#0050a0",background:"#e6f3fb",borderRadius:4,padding:"5px 10px",letterSpacing:1}}>
                            {tracesDraft.zipDate.replace(/-/g,"").replace(/^(\d{4})(\d{2})(\d{2})$/,"$3$2$1")}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                  <div style={{fontSize:11.5,fontWeight:700,color:"var(--a)",marginBottom:8,textTransform:"uppercase",letterSpacing:0.5}}>Taxpayer Login (PAN)</div>
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:14}}>
                    <div>
                      <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>PAN</div>
                      <input value={tracesDraft.taxpayerPAN||""} onChange={e=>setTracesDraft(p=>({...p,taxpayerPAN:e.target.value.toUpperCase()}))} placeholder="ABCDE1234F" maxLength={10} style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,fontFamily:"Consolas,monospace",outline:"none",boxSizing:"border-box"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                    </div>
                    <div>
                      <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>Password</div>
                      <input type="password" value={tracesDraft.taxpayerPass||""} onChange={e=>setTracesDraft(p=>({...p,taxpayerPass:e.target.value}))} placeholder="••••••••" style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,outline:"none",boxSizing:"border-box"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                    </div>
                  </div>
                  <div style={{fontSize:11.5,fontWeight:700,color:"var(--pur)",marginBottom:8,textTransform:"uppercase",letterSpacing:0.5}}>Deductor Login (TAN)</div>
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:20}}>
                    <div>
                      <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>TAN</div>
                      <input value={tracesDraft.deductorTAN||""} onChange={e=>setTracesDraft(p=>({...p,deductorTAN:e.target.value.toUpperCase()}))} placeholder="ABCD12345E" maxLength={10} style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,fontFamily:"Consolas,monospace",outline:"none",boxSizing:"border-box"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                    </div>
                    <div>
                      <div style={{fontSize:11,color:"var(--tx2)",marginBottom:3}}>Password</div>
                      <input type="password" value={tracesDraft.deductorPass||""} onChange={e=>setTracesDraft(p=>({...p,deductorPass:e.target.value}))} placeholder="••••••••" style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 9px",fontSize:12.5,outline:"none",boxSizing:"border-box"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                    </div>
                  </div>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                    <button onClick={async()=>{
                      if(window.confirm(`Clear TRACES credentials for ${curCompany?.name}?`)){
                        const empty={taxpayerPAN:"",taxpayerPass:"",deductorTAN:"",deductorPass:"",zipDate:"",savedAt:null};
                        setTracesCreds(empty);setTracesDraft(empty);
                        const updatedMap = {...tracesCredsMap, [selCompanyId]: empty};
                        await saveToStore('tracesCredsMap', updatedMap);
                        setTracesCredsOpen(false);showToast("Credentials cleared for "+curCompany?.name);
                      }
                    }} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"7px 14px",cursor:"pointer",fontSize:12,fontFamily:"inherit",color:"var(--red)"}}>Clear</button>
                    <div style={{display:"flex",gap:8}}>
                      <button onClick={()=>setTracesCredsOpen(false)} style={{border:"1px solid var(--bd)",background:"none",borderRadius:3,padding:"7px 16px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit"}}>Cancel</button>
                      <button onClick={async()=>{
                        setTracesSaving(true);
                        try{
                          const encTP = tracesDraft.taxpayerPass ? await window.electronAPI.tracesEncrypt(tracesDraft.taxpayerPass) : '';
                          const encDP = tracesDraft.deductorPass  ? await window.electronAPI.tracesEncrypt(tracesDraft.deductorPass)  : '';
                          const saved={taxpayerPAN:(tracesDraft.taxpayerPAN||'').toUpperCase().trim(),taxpayerPass:encTP,deductorTAN:(tracesDraft.deductorTAN||'').toUpperCase().trim(),deductorPass:encDP,savedAt:Date.now(),zipDate:(tracesDraft.zipDate||"")};
                          const updatedMap = {...tracesCredsMap, [selCompanyId]: saved};
                          await saveToStore('tracesCredsMap', updatedMap);
                          setTracesCreds(saved);setTracesCredsOpen(false);
                          showToast("✅ Credentials saved for "+curCompany?.name,"s");
                        }catch(e){showToast("Save failed: "+e.message,"e");}
                        setTracesSaving(false);
                      }} disabled={tracesSaving} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"7px 18px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:600,opacity:tracesSaving?0.6:1}}>
                        {tracesSaving?"Saving…":"🔐 Save Securely"}
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {view==="data"&&(
              <div className="dv">
                <div className="dvtb">
                  <div className="dstabs">
                    {["26AS","AIS","Books","Invoices"].map(ds=>(
                      <div key={ds} className={`dst${selDS===ds?" on":""}`} onClick={()=>{setSelDS(ds);setSelRows(new Set());setSearchQ("");setShowDupOnly(false);}}>
                        {ds} ({ds==="Invoices"?(datasets["Invoices"]||[]).length:datasets[ds].length})
                      </div>
                    ))}
                  </div>
                  <div className="srch" style={{marginLeft:10}}>
                    <Ic d={I.search} s={12} c="#888"/>
                    <input ref={searchRef} placeholder="Search..." value={searchQ} onChange={e=>setSearchQ(e.target.value)}/>
                    {searchQ&&<span onClick={()=>setSearchQ("")} style={{cursor:"pointer",color:"#999",fontSize:11}}>✕</span>}
                  </div>
                  <div className="rc">{selDS==="Invoices"?`${(datasets["Invoices"]||[]).length} invoices`:(filtered.length!==activeData.length?`${filtered.length} of ${activeData.length}`:`${activeData.length} records`)}{selRows.size>0&&` · ${selRows.size} selected`}</div>
                  <button
                    onClick={()=>downloadTabExcel(selDS)}
                    disabled={selDS==="Invoices"?(datasets["Invoices"]||[]).length===0:!activeData.length}
                    title={`Download ${selDS} as Excel`}
                    style={{
                      display:"flex",alignItems:"center",gap:4,
                      padding:"3px 10px",marginLeft:4,
                      borderRadius:3,
                      border:"1px solid var(--bd)",
                      background:(selDS==="Invoices"?(datasets["Invoices"]||[]).length>0:activeData.length>0)?"#e8f8e8":"var(--sur)",
                      color:(selDS==="Invoices"?(datasets["Invoices"]||[]).length>0:activeData.length>0)?"#107c10":"var(--tx3)",
                      cursor:(selDS==="Invoices"?(datasets["Invoices"]||[]).length>0:activeData.length>0)?"pointer":"not-allowed",
                      fontSize:11.5,fontFamily:"inherit",fontWeight:600,
                      transition:"all 0.15s"
                    }}
                  >
                    <Ic d={I.download} s={12} c={(selDS==="Invoices"?(datasets["Invoices"]||[]).length>0:activeData.length>0)?"#107c10":"#ccc"}/>
                    Excel
                  </button>
                  {selDS==="Books"&&dupInvoiceNos.size>0&&(
                    <button
                      onClick={()=>setShowDupOnly(p=>!p)}
                      style={{
                        marginLeft:6,
                        display:"flex",alignItems:"center",gap:5,
                        padding:"3px 10px",
                        borderRadius:3,
                        border:`1px solid ${showDupOnly?"#d59300":"var(--bd)"}`,
                        background:showDupOnly?"#fff4e0":"var(--sur)",
                        color:showDupOnly?"#835b00":"var(--tx2)",
                        cursor:"pointer",fontSize:11.5,fontFamily:"inherit",fontWeight:600,
                        transition:"all 0.15s"
                      }}
                    >
                      <span style={{fontSize:12}}>⚠</span>
                      Duplicates
                      <span style={{
                        background:showDupOnly?"#d59300":"#aaa",
                        color:"#fff",borderRadius:9,
                        padding:"1px 6px",fontSize:10,fontWeight:700
                      }}>{dupInvoiceNos.size}</span>
                    </button>
                  )}
                </div>
                {selDS==="Invoices"?(
                  (datasets["Invoices"]||[]).length===0?(
                    <div className="emp"><Ic d={I.file} s={44} c="#d1d1d1" sw={1}/><p>No Invoice data loaded</p><p className="sub">Sync invoices from Odoo</p><button className="ib" style={{marginTop:8}} onClick={()=>setView("import")}>Go to Import</button></div>
                  ):(
                    <>
                      <div style={{display:"flex",gap:8,marginBottom:8,alignItems:"center"}}>
                        <input type="text" placeholder="Search by client name or invoice no..." value={searchQ} onChange={e=>setSearchQ(e.target.value)} style={{flex:1,padding:"6px 10px",border:"1px solid #ddd",borderRadius:4,fontSize:12}}/>
                        <select value={invStatusFilter} onChange={e=>setInvStatusFilter(e.target.value)} style={{padding:"6px 10px",border:"1px solid #ddd",borderRadius:4,fontSize:12,background:"#fff",cursor:"pointer"}}>
                          <option value="all">All Status</option>
                          <option value="ok">✓ OK</option>
                          <option value="excess">⚠ Excess TDS</option>
                          <option value="notds">○ No TDS</option>
                        </select>
                      </div>
                      <div className="gw">
                        <table className="dg">
                          <thead><tr>
                            <th style={{width:50}}>S.No.</th>
                            <th style={{width:180}}>Name of Client</th>
                            <th style={{width:120}}>Invoice No.</th>
                            <th style={{width:85}}>Invoice Date</th>
                            <th style={{width:110,textAlign:"right"}}>Taxable Value</th>
                            <th style={{width:100,textAlign:"right"}}>Amount Due</th>
                            <th style={{width:100,textAlign:"right"}}>Booked TDS</th>
                            <th style={{width:65,textAlign:"right"}}>Rate</th>
                            <th style={{width:60}}>Status</th>
                            <th style={{width:110}}>Odoo Ref</th>
                          </tr></thead>
                          <tbody>
                            {(datasets["Invoices"]||[]).filter(inv=>{
                              // Text search
                              const matchSearch = !searchQ||(inv.partnerName||'').toLowerCase().includes(searchQ.toLowerCase())||(inv.invoiceNo||'').toLowerCase().includes(searchQ.toLowerCase());
                              if (!matchSearch) return false;
                              // Status filter
                              if (invStatusFilter === "all") return true;
                              const invNo=(inv.invoiceNo||'').trim().toUpperCase();
                              const tdsBooked=(datasets["Books"]||[]).filter(b=>(b.invoiceNo||'').trim().toUpperCase()===invNo).reduce((s,b)=>s+(b.tdsDeducted||0),0);
                              const taxableVal=inv.amountUntaxed||0;
                              const isExcess=tdsBooked>taxableVal*0.105;
                              const hasNoTds=tdsBooked===0;
                              if (invStatusFilter === "excess") return isExcess;
                              if (invStatusFilter === "notds") return hasNoTds;
                              if (invStatusFilter === "ok") return !isExcess && !hasNoTds;
                              return true;
                            }).map((inv,idx)=>{
                              const invNo=(inv.invoiceNo||'').trim().toUpperCase();
                              const booksEntries=(datasets["Books"]||[]).filter(b=>(b.invoiceNo||'').trim().toUpperCase()===invNo);
                              const tdsBooked=booksEntries.reduce((s,b)=>s+(b.tdsDeducted||0),0);
                              const taxableVal=inv.amountUntaxed||0;
                              const tdsPercent=taxableVal>0?((tdsBooked/taxableVal)*100):0;
                              const isExcess=tdsBooked>taxableVal*0.105;
                              const hasNoTds=tdsBooked===0;
                              const odooRefData = odooRefs[invNo] || null;
                              return(
                                <tr key={inv.id||idx} style={{background:isExcess?"#fff8f8":hasNoTds?"#fffaf0":idx%2===0?"#fff":"#fafafa"}}>
                                  <td style={{color:"#aaa"}}>{idx+1}</td>
                                  <td title={inv.partnerName} style={{fontWeight:500,maxWidth:180,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{inv.partnerName||"—"}</td>
                                  <td><span style={{fontFamily:"Consolas,monospace",color:"var(--a)",fontSize:11,fontWeight:600}}>{inv.invoiceNo||"—"}</span></td>
                                  <td style={{fontFamily:"Consolas,monospace",fontSize:11}}>{inv.invoiceDate||"—"}</td>
                                  <td className="num" style={{fontWeight:600,color:"#107c10"}}>₹{taxableVal.toLocaleString("en-IN",{minimumFractionDigits:2})}</td>
                                  <td className="num" style={{fontWeight:600,color:(inv.amountDue||0)>0?"#d59300":"#107c10"}}>{(inv.amountDue||0)>0?`₹${(inv.amountDue||0).toLocaleString("en-IN",{minimumFractionDigits:2})}`:<span style={{color:"#107c10",fontWeight:600}}>Paid ✓</span>}</td>
                                  <td className="num" style={{fontWeight:600,color:isExcess?"#a80000":hasNoTds?"#d59300":"#0078d4",cursor:tdsBooked>0?"pointer":"default",textDecoration:tdsBooked>0?"underline":"none"}} onClick={()=>{ if(tdsBooked>0) setInvTdsDetailPopup({ invoiceNo: inv.invoiceNo, entries: booksEntries, total: tdsBooked, taxableVal, tdsPercent, isExcess }); }} title={tdsBooked>0?`Click to see ${booksEntries.length} TDS entries`:""}>
                                    {tdsBooked>0?`₹${tdsBooked.toLocaleString("en-IN",{minimumFractionDigits:2})}`:"—"}
                                    {booksEntries.length>1&&<sup style={{fontSize:8,marginLeft:2,color:"#666"}}>{booksEntries.length}</sup>}
                                  </td>
                                  <td className="num" style={{fontWeight:600,color:isExcess?"#a80000":"#5c2d91"}}>{tdsBooked>0?`${tdsPercent.toFixed(1)}%`:"—"}{isExcess&&<span style={{marginLeft:2}}>⚠</span>}</td>
                                  <td><span style={{display:"inline-block",padding:"2px 6px",borderRadius:10,fontSize:9,fontWeight:600,background:hasNoTds?"#fff4e0":isExcess?"#fde7e9":"#e8f8e8",color:hasNoTds?"#996600":isExcess?"#a80000":"#107c10"}}>{hasNoTds?"No TDS":isExcess?"Excess":"OK"}</span></td>
                                  <td style={{fontSize:10,fontFamily:"Consolas,monospace"}}>{odooRefData ? <span style={{color:"#5c2d91",fontWeight:600}} title={`Created: ${odooRefData.createdAt?.slice(0,10)||'?'}`}>{odooRefData.odooRef || `ID:${odooRefData.moveId}`}</span> : <span style={{color:"#ccc"}}>—</span>}</td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                      <div className="smb">
                        <div className="si">Total Invoices: <span className="sv2">{(datasets["Invoices"]||[]).length}</span></div>
                        <div className="si">Total Taxable: <span className="sv2" style={{color:"#107c10"}}>₹{(datasets["Invoices"]||[]).reduce((s,r)=>s+(r.amountUntaxed||0),0).toLocaleString("en-IN",{minimumFractionDigits:2})}</span></div>
                        <div className="si">Amount Due: <span className="sv2" style={{color:"#d59300"}}>₹{(datasets["Invoices"]||[]).reduce((s,r)=>s+(r.amountDue||0),0).toLocaleString("en-IN",{minimumFractionDigits:2})}</span></div>
                        <div className="si">No TDS: <span className="sv2" style={{color:"#d59300"}}>{(datasets["Invoices"]||[]).filter(inv=>{const invNo=(inv.invoiceNo||'').trim().toUpperCase();return(datasets["Books"]||[]).filter(b=>(b.invoiceNo||'').trim().toUpperCase()===invNo).reduce((s,b)=>s+(b.tdsDeducted||0),0)===0;}).length}</span></div>
                        <div className="si">Excess: <span className="sv2" style={{color:"#a80000"}}>{(datasets["Invoices"]||[]).filter(inv=>{const invNo=(inv.invoiceNo||'').trim().toUpperCase();const tds=(datasets["Books"]||[]).filter(b=>(b.invoiceNo||'').trim().toUpperCase()===invNo).reduce((s,b)=>s+(b.tdsDeducted||0),0);return tds>(inv.amountUntaxed||0)*0.105;}).length}</span></div>
                      </div>
                    </>
                  )
                ):activeData.length===0?(
                  <div className="emp"><Ic d={I.grid} s={44} c="#d1d1d1" sw={1}/><p>No {selDS} data loaded</p><p className="sub">Go to Import Data</p><button className="ib" style={{marginTop:8}} onClick={()=>setView("import")}>Import Files</button></div>
                ):(
                  <>
                    <div className="gw">
                      <table className="dg">
                        <thead><tr>
                          <th style={{width:34}}><input type="checkbox" className="cb3" checked={selRows.size===sortedData.length&&sortedData.length>0} onChange={toggleAll}/></th>
                          {[{k:"id",l:"#",w:42},{k:"deductorName",l:selDS==="Books"?"Party Name":"Deductor Name",w:200},{k:"tan",l:"TAN",w:110},{k:"section",l:"Section",w:80},{k:"amountPaid",l:"Amount Paid",w:120},{k:"tdsDeducted",l:"TDS Deducted",w:118},{k:"tdsDeposited",l:"TDS Deposited",w:118,skip:selDS==="Books"},{k:"invoiceNo",l:"Invoice No.",w:112,skip:selDS!=="Books"},{k:"date",l:"Trans. Date",w:96},{k:"invoiceDate",l:"Invoice Date",w:96,skip:selDS!=="Books"},{k:"_taxable",l:"Taxable Val",w:108,skip:selDS!=="Books"},{k:"_amtDue",l:"Amt Due",w:96,skip:selDS!=="Books"},{k:"_tdsRate",l:"TDS %",w:60,skip:selDS!=="Books"},{k:"_odooRef",l:"Odoo Ref",w:120,skip:selDS!=="Books"},{k:"quarter",l:"Qtr",w:55},{k:"financialYear",l:"F.Y.",w:76,skip:selDS==="Books"},{k:"bookingStatus",l:"B.Status",w:65,skip:selDS==="Books"},{k:"matchStatus",l:"Match",w:95}].filter(c=>!c.skip).map(c=><th key={c.k} style={{width:c.w,minWidth:c.w}} className={sortCol===c.k?"srt":""} onClick={()=>toggleSort(c.k)}>{c.l}{sortCol===c.k?(sortDir==="asc"?" ↑":" ↓"):""}</th>)}
                        </tr></thead>
                        <tbody>
                          {sortedData.map(row=>{
                            const isDup = selDS==="Books" && !!((row.invoiceNo||"").trim()) && dupInvoiceNos.has((row.invoiceNo||"").trim().toUpperCase());
                            const invKey = selDS==="Books" ? (row.invoiceNo||"").trim().toUpperCase() : null;
                            const inv = invKey ? invMap[invKey] : null;
                            const taxableVal = inv?.amountUntaxed || 0;
                            const amtDue = inv?.amountDue ?? null;
                            const tdsRate = taxableVal > 0 ? ((row.tdsDeducted||0) / taxableVal * 100) : null;
                            const odooRefData = invKey ? (odooRefs[invKey] || null) : null;
                            return (
                            <tr key={row.id} className={selRows.has(row.id)?"sel":""} onClick={()=>toggleRow(row.id)}
                              style={isDup&&!selRows.has(row.id)?{background:"#fff8e8"}:{}}>
                              <td><input type="checkbox" className="cb3" checked={selRows.has(row.id)} onChange={()=>toggleRow(row.id)} onClick={e=>e.stopPropagation()}/></td>
                              <td style={{color:"#aaa"}}>{row.id}</td>
                              <td title={row.deductorName} style={{fontWeight:500}}>{row.deductorName||"—"}</td>
                              <td><span style={{fontFamily:"Consolas,monospace",color:"var(--a)",fontSize:11}}>{row.tan||"—"}</span></td>
                              <td>{row.section?<span className="tg tg-sec">{row.section}</span>:"—"}</td>
                              <td className="num">{fmt(row.amountPaid)}</td>
                              <td className="num" style={{color:"#a80000"}}>{fmt(row.tdsDeducted)}</td>
                              {selDS!=="Books"&&<td className="num" style={{color:"var(--grn)"}}>{fmt(row.tdsDeposited)}</td>}
                              {selDS==="Books"&&<td style={{color:"var(--tx2)"}}>
                                <span style={{display:"flex",alignItems:"center",gap:4}}>
                                  <span>{row.invoiceNo||"—"}</span>
                                  {isDup&&<span title="Duplicate invoice number" style={{fontSize:9,fontWeight:700,padding:"1px 5px",borderRadius:3,background:"#d59300",color:"#fff",flexShrink:0,letterSpacing:0.3}}>DUP</span>}
                                </span>
                              </td>}
                              <td style={{color:"var(--tx2)"}}>
                                {row.date||"—"}
                                {selDS==="Books"&&(odooRefData||row.journalEntry)&&<div style={{fontSize:9.5,fontFamily:"Consolas,monospace",color:"#5c2d91",fontWeight:600,marginTop:1,whiteSpace:"nowrap"}}>{odooRefData?(odooRefData.odooRef||`ID:${odooRefData.moveId}`):row.journalEntry}</div>}
                              </td>
                              {selDS==="Books"&&<td style={{color:"var(--grn)",fontWeight:600}}>{row.invoiceDate||"—"}</td>}
                              {selDS==="Books"&&<td className="num" style={{color:taxableVal>0?"#107c10":"var(--tx3)",fontSize:11}}>{taxableVal>0?`₹${taxableVal.toLocaleString("en-IN",{minimumFractionDigits:2})}`:"—"}</td>}
                              {selDS==="Books"&&<td className="num" style={{fontSize:11,color:amtDue===null?"var(--tx3)":amtDue>0?"#d59300":"#107c10",fontWeight:amtDue!=null?600:400}}>{amtDue===null?"—":amtDue>0?`₹${amtDue.toLocaleString("en-IN",{minimumFractionDigits:2})}`:<span style={{color:"#107c10"}}>Paid ✓</span>}</td>}
                              {selDS==="Books"&&<td style={{textAlign:"right",fontSize:11,fontWeight:600,color:tdsRate===null?"var(--tx3)":tdsRate>10.5?"#a80000":"#5c2d91"}}>{tdsRate===null?"—":`${tdsRate.toFixed(1)}%`}</td>}
                              {selDS==="Books"&&<td style={{fontSize:10,fontFamily:"Consolas,monospace"}}>{odooRefData?<span style={{color:"#5c2d91",fontWeight:600}} title={`Created: ${odooRefData.createdAt?.slice(0,10)||"?"}`}>{odooRefData.odooRef||`ID:${odooRefData.moveId}`}</span>:row.journalEntry?<span style={{color:"#5c2d91",fontWeight:500}} title="Synced from Odoo">{row.journalEntry}</span>:<span style={{color:"#ccc"}}>—</span>}</td>}
                              <td>{row.quarter?<span className="tg tg-q">{row.quarter}</span>:"—"}</td>
                              {selDS!=="Books"&&<><td>{row.financialYear||"—"}</td><td><span style={{fontFamily:"Consolas,monospace",fontSize:10,color:row.bookingStatus==="F"?"var(--grn)":"var(--amb)"}}>{row.bookingStatus||"—"}</span></td></>}
                              <td><span className={`tg ${row.matchStatus==="Matched"?"tg-m":row.matchStatus==="Mismatch"?"tg-mm":"tg-um"}`}>{row.matchStatus}</span></td>
                            </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <div className="smb">
                      <div className="si">Records: <span className="sv2">{activeData.length}</span></div>
                      <div className="si">Total Amount: <span className="sv2">₹{totalAmt.toLocaleString("en-IN",{minimumFractionDigits:2})}</span></div>
                      <div className="si">Total TDS: <span className="sv2" style={{color:"#a80000"}}>₹{totalTDS.toLocaleString("en-IN",{minimumFractionDigits:2})}</span></div>
                      <div className="si">Unmatched: <span className="sv2" style={{color:"#a80000"}}>{activeData.filter(r=>r.matchStatus==="Unmatched").length}</span></div>
                      {selDS==="Books"&&dupInvoiceNos.size>0&&(
                        <div className="si" style={{cursor:"pointer",borderLeft:"2px solid #d59300",paddingLeft:8}} onClick={()=>setShowDupOnly(p=>!p)} title="Click to toggle duplicate filter">
                          ⚠ Dup. Invoices: <span className="sv2" style={{color:"#d59300",fontWeight:700}}>
                            {dupInvoiceNos.size} inv · {activeData.filter(r=>dupInvoiceNos.has((r.invoiceNo||"").trim().toUpperCase())).length} rows
                          </span>
                        </div>
                      )}
                    </div>
                  </>
                )}
              </div>
            )}

            {view==="recon"&&(
              <div className="rv">
                <div className="rv-top">
                  <div><div className="rv-title">26AS vs Books Reconciliation</div><div className="rv-sub">{curCompany?.name} · FY {selYear} · {reconMode==="tan"?"TAN-wise":"Section-wise"} · {reconMode==="tan"?"Click any row for transaction details":"Drill down by TDS section"}</div></div>
                  <div style={{display:"flex",alignItems:"center",gap:4,background:"var(--sur)",borderRadius:4,padding:3,border:"1px solid var(--bd)"}}>
                    {[["tan","TAN-wise"],["section","Section-wise"]].map(([m,l])=>(
                      <button key={m} onClick={()=>setReconMode(m)} style={{background:reconMode===m?"var(--a)":"none",color:reconMode===m?"#fff":"var(--tx2)",border:"none",borderRadius:3,padding:"4px 12px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,transition:"all 0.15s"}}>{l}</button>
                    ))}
                  </div>
                  <button className="run-btn" onClick={runRecon} disabled={!datasets["26AS"].length||!datasets["Books"].length}><Ic d={I.play} s={13} c="#fff"/>Re-run</button>
                  <button style={{marginLeft:8,background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"6px 14px",cursor:"pointer",fontSize:12,color:"var(--a)",fontFamily:"inherit"}} onClick={exportReconReport}>📊 Export Excel</button>
                </div>
                {reconDone&&(
                  <div className="rs-grid">
                    {[{l:"Total TANs",v:rs.total,c:"blu"},{l:"Matched",v:rs.matched,c:"grn"},{l:"Mismatches",v:rs.mismatch,c:"red"},{l:"Missing in Books",v:rs.mib,c:"pur"},{l:"Missing TAN",v:rs.mt,c:rs.mt>0?"red":"grn"},{l:"TDS Difference",v:(rs.tdsDiff>0?"+":rs.tdsDiff<0?"-":"")+"₹"+Math.abs(rs.tdsDiff).toLocaleString("en-IN",{maximumFractionDigits:0}),c:Math.abs(rs.tdsDiff)>1?"red":"grn"}].map((s,i)=>(
                      <div className={`rs-card ${s.c}`} key={i}><div className="rs-lbl">{s.l}</div><div className={`rs-val ${s.c}`}>{s.v}</div></div>
                    ))}
                  </div>
                )}
                <div className="rf-bar">
                  <span className="rf-lbl">Quarter:</span>
                  {["All","Q1","Q2","Q3","Q4"].map(q=><button key={q} className={`qtab${selQ===q?" on":""}`} onClick={()=>setSelQ(q)}>{q}</button>)}
                  <div style={{width:1,height:20,background:"var(--bd)",margin:"0 6px"}}/>
                  <span className="rf-lbl">From:</span>
                  <input type="date" className="rf-date" value={dateFrom} onChange={e=>setDateFrom(e.target.value)}/>
                  <span className="rf-lbl">To:</span>
                  <input type="date" className="rf-date" value={dateTo} onChange={e=>setDateTo(e.target.value)}/>
                  {hasActiveFilter&&<button className="rf-clr" onClick={()=>{setSelQ("All");setDateFrom("");setDateTo("");}}>✕ Clear filters</button>}
                  <div style={{width:1,height:20,background:"var(--bd)",margin:"0 6px"}}/>
                  <span className="rf-lbl">Status:</span>
                  <select className="rf-sel" value={selStatus} onChange={e=>setSelStatus(e.target.value)}>
                    {["All","Missing TAN","Matched","Near Match","Mismatch","Missing in Books","Missing in 26AS"].map(s=><option key={s} value={s}>{s}{s!=="All"?" ("+(reconMode==="section"?liveSectionResults:liveResults).filter(r=>r.matchStatus===s).length+")":""}</option>)}
                  </select>
                  {reconMode==="section"&&(
                    <>
                      <div style={{width:1,height:20,background:"var(--bd)",margin:"0 6px"}}/>
                      <span className="rf-lbl">Section:</span>
                      <select className="rf-sel" value={selSection} onChange={e=>setSelSection(e.target.value)}>
                        <option value="All">All ({allSections.length})</option>
                        {allSections.map(s=><option key={s} value={s}>{s}</option>)}
                      </select>
                    </>
                  )}
                  <div className="srch" style={{marginLeft:6}}>
                    <Ic d={I.search} s={11} c="#888"/>
                    {reconMode==="section"
                      ? <input placeholder="Search TAN, name or section..." value={sectionSearch} onChange={e=>setSectionSearch(e.target.value)} style={{width:185,fontSize:11.5,border:"none",background:"none",outline:"none",fontFamily:"inherit",color:"var(--tx)"}}/>
                      : <input placeholder="Search TAN or name..." value={reconSearch} onChange={e=>setReconSearch(e.target.value)} style={{width:155,fontSize:11.5,border:"none",background:"none",outline:"none",fontFamily:"inherit",color:"var(--tx)"}}/>
                    }
                    {(reconSearch||sectionSearch)&&<span onClick={()=>{setReconSearch("");setSectionSearch("");}} style={{cursor:"pointer",color:"#999",fontSize:11}}>✕</span>}
                  </div>
                  <label className="mm-only"><input type="checkbox" checked={mmOnly} onChange={e=>setMmOnly(e.target.checked)} style={{accentColor:"var(--a)"}}/>Mismatches only</label>
                  <span style={{marginLeft:"auto",fontSize:11.5,color:"var(--tx2)",fontWeight:500}}>{reconMode==="section"?sectionFiltered.length+" rows":reconFiltered.length+" TANs"}</span>
                </div>
                {!reconDone?(
                  <div className="emp"><Ic d={I.recon} s={44} c="#d1d1d1" sw={1}/><p>No reconciliation run yet</p><p className="sub">Import 26AS and Books data, then click Reconcile</p><button className="run-btn" style={{marginTop:10}} onClick={runRecon} disabled={!datasets["26AS"].length||!datasets["Books"].length}>▶ Run Reconciliation</button></div>
                ):reconMode==="tan"?(
                  <>

                  <div className="rg-wrap">
                    <table className="rg">
                      <thead><tr>
                        <th style={{width:38}}>#</th>
                        <th className="ah" style={{width:118}}>TAN</th>
                        <th className="ah" style={{width:185}}>Deductor (26AS)</th>
                        <th className="ah" style={{width:48,textAlign:"right"}}>Txns</th>
                        <th className="ah" style={{width:120,textAlign:"right"}}>TDS (26AS)</th>
                        <th className="divh"></th>
                        <th className="bh" style={{width:185}}>Party (Books)</th>
                        <th className="bh" style={{width:48,textAlign:"right"}}>Txns</th>
                        <th className="bh" style={{width:120,textAlign:"right"}}>TDS (Books)</th>
                        <th className="divh"></th>
                        <th className="dh" style={{width:115,textAlign:"right"}}>TDS Diff</th>
                        <th className="dh" style={{width:125}}>Status</th>
                        <th className="dh" style={{width:200}}>Remark</th>
                      </tr></thead>
                      <tbody>
                        {reconFiltered.map((row,i)=>{
                          const wasResolved = resolvedTANs.has(row.tan);
                          return (
                          <tr key={row.id} className={`${wasResolved?"row-resolved":getRC(row.matchStatus)} rg-row-click`}
                            onClick={()=>row.matchStatus==="Missing TAN"?setShowMissingTanModal(true):setDetailTAN(row.tan)}>
                            <td style={{color:"#aaa"}}>{i+1}</td>
                            <td><span style={{fontFamily:"Consolas,monospace",fontSize:11,color:row.matchStatus==="Missing TAN"?"#a80000":"var(--a)",fontWeight:600}}>{row.tan||"—"}</span></td>
                            <td title={row.as_name} style={{fontWeight:500}}>
                              <div style={{display:"flex",alignItems:"center",gap:5}}>
                                <span>{row.as_name||<span style={{color:"var(--tx3)"}}>—</span>}</span>
                                {(()=>{
                                  const lastSent = emailLog.filter(e=>e.tan===row.tan && e.status!=="Failed").sort((a,b)=>new Date(b.sentAt)-new Date(a.sentAt))[0];
                                  if(!lastSent) return null;
                                  const sentDate = new Date(lastSent.sentAt).toLocaleDateString("en-IN",{day:"2-digit",month:"short",year:"2-digit"});
                                  return <span title={`Email sent on ${sentDate}`} style={{fontSize:10,background:"#e8f8e8",color:"#107c10",border:"1px solid #b3dab3",borderRadius:3,padding:"1px 5px",whiteSpace:"nowrap",flexShrink:0,cursor:"default"}}>✉</span>;
                                })()}
                              </div>
                            </td>
                            <td className="num" style={{color:"var(--tx2)",fontSize:11}}>{row.as_txns||"—"}</td>
                            <td className="num" style={{color:"#a80000",fontWeight:500}}>{row.as_tds?fmt(row.as_tds):<span style={{color:"var(--tx3)"}}>—</span>}</td>
                            <td className="divh"></td>
                            <td title={row.bk_name}>{row.bk_name||<span style={{color:"var(--red)",fontStyle:"italic",fontSize:11}}>Not in Books</span>}</td>
                            <td className="num" style={{color:"var(--tx2)",fontSize:11}}>{row.bk_txns||"—"}</td>
                            <td className="num" style={{color:"var(--grn)",fontWeight:500}}>{row.bk_tds?fmt(row.bk_tds):<span style={{color:"var(--tx3)"}}>—</span>}</td>
                            <td className="divh"></td>
                            <td className="num"><FmtDiff n={row.tds_diff}/></td>
                            <td><span className={`tg ${getTag(row.matchStatus)}`}>{row.matchStatus}</span></td>
                            <td style={{fontSize:11,color:"var(--tx2)"}}>
                              {row.matchStatus==="Missing TAN"
                                ? <span style={{display:"flex",alignItems:"center",gap:6}}>
                                    <span style={{color:"#a80000"}}>{row.mismatchReason}</span>
                                    <button onClick={e=>{e.stopPropagation();setShowMissingTanModal(true);}}
                                      style={{background:"#a80000",color:"#fff",border:"none",borderRadius:3,padding:"2px 8px",cursor:"pointer",fontSize:10,fontFamily:"inherit",fontWeight:600,whiteSpace:"nowrap"}}>
                                      Assign TAN →
                                    </button>
                                  </span>
                                : row.mismatchReason||"—"}
                            </td>
                          </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                  </>
                ):(
                  /* ── SECTION-WISE TABLE ── */
                  <div className="rg-wrap">
                    <table className="rg">
                      <thead><tr>
                        <th style={{width:38}}>#</th>
                        <th className="ah" style={{width:110}}>TAN</th>
                        <th className="ah" style={{width:90}}>Section</th>
                        <th className="ah" style={{width:180}}>Deductor / Party</th>
                        <th className="ah" style={{width:44,textAlign:"right"}}>Txns</th>
                        <th className="ah" style={{width:115,textAlign:"right"}}>TDS (26AS)</th>
                        <th className="divh"></th>
                        <th className="bh" style={{width:44,textAlign:"right"}}>Txns</th>
                        <th className="bh" style={{width:115,textAlign:"right"}}>TDS (Books)</th>
                        <th className="divh"></th>
                        <th className="dh" style={{width:110,textAlign:"right"}}>TDS Diff</th>
                        <th className="dh" style={{width:125}}>Status</th>
                        <th className="dh" style={{width:190}}>Reason</th>
                      </tr></thead>
                      <tbody>
                        {sectionFiltered.length===0?(
                          <tr><td colSpan={13} style={{textAlign:"center",padding:"40px",color:"var(--tx3)"}}>No section-wise data matches current filters</td></tr>
                        ):sectionFiltered.map((row,i)=>(
                          <tr key={row.id} className={`${getRC(row.matchStatus)} rg-row-click`} onClick={()=>setDetailTAN(row.tan)}>
                            <td style={{color:"#aaa"}}>{i+1}</td>
                            <td><span style={{fontFamily:"Consolas,monospace",fontSize:11,color:"var(--a)",fontWeight:600}}>{row.tan||"—"}</span></td>
                            <td><span style={{background:"#e6f3fb",color:"#0078d4",fontWeight:700,padding:"2px 7px",borderRadius:3,fontSize:11,fontFamily:"Consolas,monospace"}}>{row.section||"—"}</span></td>
                            <td style={{fontWeight:500,maxWidth:180,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={row.name}>{row.name||"—"}</td>
                            <td className="num" style={{color:"var(--tx2)",fontSize:11}}>{row.as_txns||"—"}</td>
                            <td className="num" style={{color:"#a80000",fontWeight:500}}>{row.as_tds?fmt(row.as_tds):<span style={{color:"var(--tx3)"}}>—</span>}</td>
                            <td className="divh"></td>
                            <td className="num" style={{color:"var(--tx2)",fontSize:11}}>{row.bk_txns||"—"}</td>
                            <td className="num" style={{color:"var(--grn)",fontWeight:500}}>{row.bk_tds?fmt(row.bk_tds):<span style={{color:"var(--tx3)"}}>—</span>}</td>
                            <td className="divh"></td>
                            <td className="num"><FmtDiff n={row.tds_diff}/></td>
                            <td><span className={`tg ${getTag(row.matchStatus)}`}>{row.matchStatus}</span></td>
                            <td style={{fontSize:11,color:"var(--tx2)"}}>{row.reason||"—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {detailTAN&&(
              <TanDetailModal tan={detailTAN} tanRow={liveResults.find(r=>r.tan===detailTAN)} txns26AS={datasets["26AS"]} txnsBooks={datasets["Books"]} txnsInvoices={datasets["Invoices"]||[]} onClose={()=>setDetailTAN(null)} fmt={fmt} FmtDiff={FmtDiff} odooUrl={curCompany.odooUrl || companies.find(c=>c.odooEnabled&&c.odooUrl)?.odooUrl || ''} odooConfig={curCompany.odooEnabled ? {url:curCompany.odooUrl,database:curCompany.odooDatabase,username:curCompany.odooUsername,password:curCompany.odooPassword} : (()=>{const oc=companies.find(c=>c.odooEnabled&&c.odooUrl);return oc?{url:oc.odooUrl,database:oc.odooDatabase,username:oc.odooUsername,password:oc.odooPassword}:null;})() } tanMaster={tanMaster} odooRefs={odooRefs} setOdooRefs={setOdooRefs} setOdooLog={setOdooLog}/>
            )}

            {/* TDS Details Popup for Invoices Tab */}
            {invTdsDetailPopup && (
              <div style={{position:"fixed",top:0,left:0,right:0,bottom:0,background:"rgba(0,0,0,0.5)",zIndex:9999,display:"flex",alignItems:"center",justifyContent:"center"}} onClick={()=>setInvTdsDetailPopup(null)}>
                <div style={{background:"#fff",borderRadius:10,width:650,maxHeight:"80vh",overflow:"hidden",boxShadow:"0 8px 32px rgba(0,0,0,0.25)"}} onClick={e=>e.stopPropagation()}>
                  <div style={{background:"linear-gradient(135deg,#0078d4,#5c2d91)",padding:"16px 20px",color:"#fff"}}>
                    <div style={{fontSize:11,textTransform:"uppercase",letterSpacing:1,opacity:0.8,marginBottom:4}}>TDS Booking Details</div>
                    <div style={{fontSize:16,fontWeight:700,fontFamily:"Consolas,monospace"}}>{invTdsDetailPopup.invoiceNo}</div>
                  </div>
                  <div style={{padding:"16px 20px"}}>
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:12,marginBottom:16}}>
                      <div style={{background:"#f0fff0",padding:"10px",borderRadius:6,border:"1px solid #c0e0c0"}}>
                        <div style={{fontSize:10,color:"#666",textTransform:"uppercase",marginBottom:3}}>Taxable Value</div>
                        <div style={{fontSize:15,fontWeight:700,color:"#107c10",fontFamily:"Consolas,monospace"}}>₹{(invTdsDetailPopup.taxableVal||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</div>
                      </div>
                      <div style={{background:"#f0f8ff",padding:"10px",borderRadius:6,border:"1px solid #c0d8f0"}}>
                        <div style={{fontSize:10,color:"#666",textTransform:"uppercase",marginBottom:3}}>TDS Booked</div>
                        <div style={{fontSize:15,fontWeight:700,color:"#0078d4",fontFamily:"Consolas,monospace"}}>₹{(invTdsDetailPopup.total||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</div>
                      </div>
                      <div style={{background:invTdsDetailPopup.isExcess?"#fff0f0":"#f8f0ff",padding:"10px",borderRadius:6,border:`1px solid ${invTdsDetailPopup.isExcess?"#f0c0c0":"#d8c0f0"}`}}>
                        <div style={{fontSize:10,color:"#666",textTransform:"uppercase",marginBottom:3}}>Tax Rate</div>
                        <div style={{fontSize:15,fontWeight:700,color:invTdsDetailPopup.isExcess?"#a80000":"#5c2d91",fontFamily:"Consolas,monospace"}}>{invTdsDetailPopup.tdsPercent?.toFixed(2)||0}%{invTdsDetailPopup.isExcess&&<span style={{fontSize:11,marginLeft:4}}>⚠</span>}</div>
                      </div>
                    </div>
                    {(()=>{const odooRefData = odooRefs[(invTdsDetailPopup.invoiceNo||'').toUpperCase()]; return odooRefData ? (
                      <div style={{background:"#f8f0ff",border:"1px solid #d8c0f0",borderRadius:6,padding:"10px 14px",marginBottom:16,display:"flex",alignItems:"center",gap:10}}>
                        <span style={{fontSize:11,color:"#666"}}>Odoo Reference:</span>
                        <span style={{fontFamily:"Consolas,monospace",fontWeight:700,color:"#5c2d91",fontSize:13}}>{odooRefData.odooRef || `ID:${odooRefData.moveId}`}</span>
                        <span style={{fontSize:10,color:"#999",marginLeft:"auto"}}>Created: {odooRefData.createdAt?.slice(0,10)||'?'}</span>
                      </div>
                    ) : null;})()}
                    <div style={{fontSize:12,fontWeight:600,color:"#333",marginBottom:8}}>📘 Books Entries ({invTdsDetailPopup.entries?.length||0})</div>
                    {(!invTdsDetailPopup.entries||invTdsDetailPopup.entries.length===0)?(
                      <div style={{padding:"20px",textAlign:"center",color:"#999",fontSize:12,background:"#f8f8f8",borderRadius:6}}>No TDS entries found</div>
                    ):(
                      <div style={{maxHeight:280,overflow:"auto",border:"1px solid #e0e0e0",borderRadius:6}}>
                        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                          <thead><tr style={{background:"#f5f5f5"}}>
                            <th style={{padding:"8px",textAlign:"left",borderBottom:"1px solid #e0e0e0"}}>S.No.</th>
                            <th style={{padding:"8px",textAlign:"left",borderBottom:"1px solid #e0e0e0"}}>Date</th>
                            <th style={{padding:"8px",textAlign:"left",borderBottom:"1px solid #e0e0e0"}}>Quarter</th>
                            <th style={{padding:"8px",textAlign:"left",borderBottom:"1px solid #e0e0e0"}}>Section</th>
                            <th style={{padding:"8px",textAlign:"left",borderBottom:"1px solid #e0e0e0"}}>Deductor</th>
                            <th style={{padding:"8px",textAlign:"right",borderBottom:"1px solid #e0e0e0"}}>TDS Amount</th>
                            <th style={{padding:"8px",textAlign:"right",borderBottom:"1px solid #e0e0e0"}}>Rate %</th>
                          </tr></thead>
                          <tbody>
                            {invTdsDetailPopup.entries.map((row,idx)=>{const rate=invTdsDetailPopup.taxableVal>0?((row.tdsDeducted||0)/invTdsDetailPopup.taxableVal*100).toFixed(2):"—";return(
                              <tr key={idx} style={{background:idx%2===0?"#fff":"#fafafa"}}>
                                <td style={{padding:"8px",color:"#aaa"}}>{idx+1}</td>
                                <td style={{padding:"8px",fontFamily:"Consolas,monospace"}}>{row.date||row.invoiceDate||"—"}</td>
                                <td style={{padding:"8px"}}>{row.quarter||"—"}</td>
                                <td style={{padding:"8px"}}>{row.section||"—"}</td>
                                <td style={{padding:"8px",maxWidth:150,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={row.deductorName}>{row.deductorName||"—"}</td>
                                <td style={{padding:"8px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:600,color:"#0078d4"}}>₹{(row.tdsDeducted||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</td>
                                <td style={{padding:"8px",textAlign:"right",fontFamily:"Consolas,monospace",color:"#5c2d91"}}>{rate}%</td>
                              </tr>
                            );})}
                          </tbody>
                          <tfoot><tr style={{background:"#e6f3fb"}}>
                            <td colSpan={5} style={{padding:"8px",fontWeight:700,color:"#0078d4"}}>Total</td>
                            <td style={{padding:"8px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:700,color:"#0078d4"}}>₹{(invTdsDetailPopup.total||0).toLocaleString("en-IN",{minimumFractionDigits:2})}</td>
                            <td style={{padding:"8px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:600,color:"#5c2d91"}}>{invTdsDetailPopup.tdsPercent?.toFixed(2)||0}%</td>
                          </tr></tfoot>
                        </table>
                      </div>
                    )}
                  </div>
                  <div style={{padding:"12px 20px",borderTop:"1px solid #eee",display:"flex",justifyContent:"flex-end"}}>
                    <button onClick={()=>setInvTdsDetailPopup(null)} style={{padding:"8px 20px",background:"#0078d4",color:"#fff",border:"none",borderRadius:4,cursor:"pointer",fontWeight:600,fontSize:12}}>Close</button>
                  </div>
                </div>
              </div>
            )}

            {view==="tanmaster"&&(
              <div className="dv">
                <div className="dvtb">
                  <div style={{fontSize:14,fontWeight:600,color:"var(--tx)"}}>TAN Master</div>
                  <div style={{fontSize:12,color:"var(--tx2)",marginLeft:8}}>✅ Auto-saves all changes · Once saved, data persists permanently · "Rebuild" only adds NEW TANs from data</div>
                  <div className="srch" style={{marginLeft:12}}>
                    <Ic d={I.search} s={12} c="#888"/>
                    <input placeholder="Search TAN or name..." value={tanSearch} onChange={e=>setTanSearch(e.target.value)} style={{width:180}}/>
                    {tanSearch&&<span onClick={()=>setTanSearch("")} style={{cursor:"pointer",color:"#999",fontSize:11}}>✕</span>}
                  </div>
                  <div className="rc">{tanMaster.filter(r=>!tanSearch||[r.tan,r.name26AS,r.nameBooks,r.finalName].some(v=>String(v||"").toLowerCase().includes(tanSearch.toLowerCase()))).length} TANs</div>
                  {(()=>{ const mc=datasets["Books"].filter(r=>!r.tan?.trim()).length; const pc=new Set(datasets["Books"].filter(r=>!r.tan?.trim()&&r.deductorName).map(r=>r.deductorName)).size; return mc>0?(<button onClick={()=>setShowMissingTanModal(true)} style={{marginLeft:8,background:"#a4262c",color:"#fff",border:"none",padding:"4px 12px",borderRadius:3,cursor:"pointer",fontSize:12,fontFamily:"inherit",display:"flex",alignItems:"center",gap:6,fontWeight:600}}>⚠️ {pc} TAN{pc!==1?"s":""} Missing <span style={{background:"rgba(255,255,255,0.25)",borderRadius:2,padding:"1px 5px",fontSize:11}}>{mc} rows</span></button>):null; })()}
                  <button style={{marginLeft:8,background:"var(--a)",color:"#fff",border:"none",padding:"4px 14px",borderRadius:3,cursor:"pointer",fontSize:12,fontFamily:"inherit"}} onClick={buildTanMaster} title="Adds new TANs from 26AS/Books. Existing TANs remain unchanged.">↺ Rebuild</button>
                  <button style={{marginLeft:6,background:"#217346",color:"#fff",border:"none",padding:"4px 14px",borderRadius:3,cursor:"pointer",fontSize:12,fontFamily:"inherit"}} onClick={exportTanMaster}>📥 Export Excel</button>
                  <button onClick={()=>document.getElementById("tan-email-import-input").click()} style={{marginLeft:6,background:"#5c2d91",color:"#fff",border:"none",padding:"4px 14px",borderRadius:3,cursor:"pointer",fontSize:12,fontFamily:"inherit",display:"flex",alignItems:"center",gap:5}} title="Import TAN emails from Excel/CSV">
                    <Ic d={I.import} s={12} c="#fff"/>Import Emails
                  </button>
                </div>
                {(()=>{
                  const dups = getDuplicates(tanMaster);
                  return dups.length>0?(
                    <div style={{margin:"8px 16px 0",background:"#fff8e1",border:"1px solid #ffe082",borderRadius:5,flexShrink:0,overflow:"hidden"}}>
                      <div onClick={()=>setIssuesOpen(o=>!o)} style={{padding:"8px 14px",fontSize:11.5,fontWeight:700,color:"#795548",display:"flex",alignItems:"center",gap:6,cursor:"pointer",userSelect:"none"}}>
                        <Ic d={I.warn} s={13} c="#f59e0b"/>
                        ⚠️ {dups.length} issue{dups.length!==1?"s":""} found
                        <span style={{marginLeft:"auto",fontSize:11,fontWeight:400,color:"#a08060"}}>{issuesOpen?"▲ Hide":"▼ Show"}</span>
                      </div>
                      {issuesOpen&&(
                        <div style={{padding:"0 14px 10px",maxHeight:120,overflowY:"auto",display:"flex",flexWrap:"wrap",gap:6}}>
                          {dups.map((d,i)=>(
                            <div key={i} style={{background:d.severity==="error"?"#fde8e8":"#fff3e0",border:`1px solid ${d.severity==="error"?"#f0c4b4":"#ffe0b2"}`,borderRadius:4,padding:"5px 10px",fontSize:11}}>
                              <span style={{fontWeight:700,color:d.severity==="error"?"#a80000":"#c7792a"}}>{d.label}:</span>{" "}
                              <span style={{fontFamily:"Consolas,monospace",color:"#0078d4"}}>{d.tan}</span>{" — "}{d.detail}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ):null;
                })()}
                <div style={{margin:"6px 16px 2px",padding:"5px 12px",background:"#f0e8ff",border:"1px solid #d0b8f0",borderRadius:4,fontSize:11,color:"#5c2d91",display:"flex",alignItems:"center",gap:6,flexShrink:0}}>
                  <Ic d={I.import} s={11} c="#5c2d91"/><strong>Import Emails / CSM:</strong>&nbsp;Only&nbsp;<code style={{background:"#e8d8ff",padding:"1px 4px",borderRadius:2}}>TAN</code>&nbsp;is required. Add any columns you want to update:&nbsp;<code style={{background:"#e8d8ff",padding:"1px 4px",borderRadius:2}}>Email</code>&nbsp;<code style={{background:"#e8d8ff",padding:"1px 4px",borderRadius:2}}>CC Email</code>&nbsp;<code style={{background:"#e8d8ff",padding:"1px 4px",borderRadius:2}}>CSM Name</code>&nbsp;<code style={{background:"#e8d8ff",padding:"1px 4px",borderRadius:2}}>Final Name</code>
                </div>
                {tanMaster.length===0?(
                  <div className="emp">
                    <Ic d={I.save} s={44} c="#d1d1d1" sw={1}/>
                    <p>No TAN Master built yet</p>
                    <p className="sub">Import 26AS and/or Books data, then click "TAN Master" in the toolbar</p>
                    <button className="ib" style={{marginTop:8}} onClick={buildTanMaster} disabled={!datasets["26AS"].length&&!datasets["Books"].length}>Build TAN Master</button>
                  </div>
                ):(
                  <>
                    <div className="gw" style={{flex:1,overflowY:"auto",overflowX:"auto",minHeight:0}}>
                      <table className="dg">
                        <thead><tr>
                          <th style={{width:44}}>S.No</th>
                          <th style={{width:120}}>TAN</th>
                          <th style={{width:200}}>Name as per 26AS</th>
                          <th style={{width:200}}>Name as per Books</th>
                          <th style={{width:180}}>Final Name <span style={{fontWeight:400,fontSize:10,color:"var(--a)"}}>(editable)</span></th>
                          <th style={{width:200}}>Customer Email <span style={{fontWeight:400,fontSize:10,color:"var(--grn)"}}>(for TDS Notice)</span></th>
                          <th style={{width:200}}>CC Email <span style={{fontWeight:400,fontSize:10,color:"var(--a)"}}>(optional)</span></th>
                          <th style={{width:180}}>CSM Name <span style={{fontWeight:400,fontSize:10,color:"var(--a)"}}>(editable)</span></th>
                          <th style={{width:260}}>Odoo Partner ID <span style={{fontWeight:400,fontSize:10,color:"#c7792a"}}>(for journal export)</span></th>
                          <th style={{width:50,textAlign:"center"}}>Del</th>
                        </tr></thead>
                        <tbody>
                          {tanMaster.filter(r=>!tanSearch||[r.tan,r.name26AS,r.nameBooks,r.finalName,r.contactEmail,r.ccEmail,r.csmName].some(v=>String(v||"").toLowerCase().includes(tanSearch.toLowerCase()))).map((row,i)=>{
                            const inlineInput = (field, placeholder, isEmail) => (
                              <input
                                value={row[field]||""}
                                onChange={e=>updateTanContact(row.tan, field, e.target.value)}
                                placeholder={placeholder}
                                style={{width:"100%",border:`1px solid ${isEmail&&row[field]?.includes("@")?"var(--grn)":"transparent"}`,borderRadius:3,padding:"3px 7px",fontSize:11.5,fontFamily:"inherit",color:"var(--tx)",background:"transparent",outline:"none"}}
                                onFocus={e=>{e.target.style.border="1px solid var(--a)";e.target.style.background="var(--wh)";}}
                                onBlur={e=>{e.target.style.border=`1px solid ${isEmail&&row[field]?.includes("@")?"var(--grn)":"transparent"}`;e.target.style.background="transparent";}}
                              />
                            );
                            return (
                            <tr key={row.tan}>
                              <td style={{color:"#aaa",textAlign:"center"}}>{row.sno}</td>
                              <td><span style={{fontFamily:"Consolas,monospace",fontSize:11,color:"var(--a)",fontWeight:600}}>{row.tan}</span></td>
                              <td style={{color:"var(--tx2)"}}>{row.name26AS||<span style={{color:"var(--tx3)",fontStyle:"italic"}}>—</span>}</td>
                              <td style={{color:"var(--tx2)"}}>{row.nameBooks||<span style={{color:"var(--tx3)",fontStyle:"italic"}}>Not in Books</span>}</td>
                              <td>
                                <input
                                  value={row.finalName||""}
                                  onChange={e=>updateFinalName(row.tan,e.target.value)}
                                  style={{width:"100%",border:"1px solid transparent",borderRadius:3,padding:"3px 7px",fontSize:11.5,fontFamily:"inherit",color:"var(--tx)",background:"transparent",outline:"none"}}
                                  onFocus={e=>{e.target.style.border="1px solid var(--a)";e.target.style.background="var(--wh)";}}
                                  onBlur={e=>{e.target.style.border="1px solid transparent";e.target.style.background="transparent";}}
                                />
                              </td>
                              <td>
                                <div style={{display:"flex",alignItems:"center",gap:4}}>
                                  {inlineInput("contactEmail","email@company.com",true)}
                                  {row.contactEmail?.includes("@")&&<span style={{color:"var(--grn)",fontSize:11,flexShrink:0}}>✓</span>}
                                </div>
                              </td>
                              <td>
                                <div style={{display:"flex",alignItems:"center",gap:4}}>
                                  {inlineInput("ccEmail","cc@company.com",true)}
                                  {row.ccEmail?.includes("@")&&<span style={{color:"var(--grn)",fontSize:11,flexShrink:0}}>✓</span>}
                                </div>
                              </td>
                              <td>{inlineInput("csmName","CSM Name",false)}</td>
                              <td>
                                <input value={row.odooPartnerId||""} onChange={e=>updateTanContact(row.tan,"odooPartnerId",e.target.value)} placeholder="__export__.res_partner_..." style={{width:"100%",border:`1px solid ${row.odooPartnerId?"#c7792a":"transparent"}`,borderRadius:3,padding:"3px 7px",fontSize:10.5,fontFamily:"Consolas,monospace",color:"#c7792a",background:"transparent",outline:"none"}} onFocus={e=>{e.target.style.border="1px solid var(--a)";e.target.style.background="var(--wh)";}} onBlur={e=>{e.target.style.border=`1px solid ${row.odooPartnerId?"#c7792a":"transparent"}`;e.target.style.background="transparent";}}/>
                              </td>
                              <td style={{textAlign:"center"}}>
                                <button onClick={()=>deleteTanRow(row.tan)} title={`Delete ${row.tan}`} className="fdb" style={{padding:"3px 6px",borderRadius:3}}><Ic d={I.close} s={11} c="var(--red)"/></button>
                              </td>
                            </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <div className="smb">
                      <div className="si">Total TANs: <span className="sv2">{tanMaster.length}</span></div>
                      <div className="si">In 26AS: <span className="sv2">{tanMaster.filter(r=>r.name26AS).length}</span></div>
                      <div className="si">In Books: <span className="sv2">{tanMaster.filter(r=>r.nameBooks).length}</span></div>
                      <div className="si">Both sources: <span className="sv2">{tanMaster.filter(r=>r.name26AS&&r.nameBooks).length}</span></div>
                      {(()=>{ const mc=datasets["Books"].filter(r=>!r.tan?.trim()).length; return mc>0?(<div className="si" style={{color:"var(--red)",cursor:"pointer"}} onClick={()=>setShowMissingTanModal(true)}>⚠️ TAN Missing: <span className="sv2" style={{color:"var(--red)",textDecoration:"underline"}}>{mc} rows</span></div>):null; })()}
                      <div className="si" style={{marginLeft:"auto",color:"var(--grn)"}}><Ic d={I.mail} s={11} c="var(--grn)"/>&nbsp;Emails filled: <span className="sv2" style={{color:"var(--grn)"}}>{tanMaster.filter(r=>r.contactEmail?.includes("@")).length}</span> / {tanMaster.length}</div>
                    </div>
                  </>
                )}
              </div>
            )}

            {view==="email"&&(()=>{
              // Auto-sync emails from TAN Master contact details
              const tanMasterEmailMap = {};
              const tanMasterCCMap = {};
              tanMaster.forEach(r=>{ 
                if(r.contactEmail?.includes("@")) tanMasterEmailMap[r.tan]=r.contactEmail; 
                if(r.ccEmail?.includes("@")) tanMasterCCMap[r.tan]=r.ccEmail;
              });
              // Merge: tanEmails (manually typed) takes priority, TAN Master fills blanks
              const mergedEmails = {...tanMasterEmailMap, ...tanEmails};
              const mergedCCs = {...tanMasterCCMap, ...tanCCs};
              // "TDS Pending" = Books shows MORE TDS than 26AS → deductor deducted but didn't deposit → we need to claim
              const allMismatch = liveResults.filter(r=>["Mismatch","Missing in Books","Missing in 26AS"].includes(r.matchStatus));
              const pendingRows = emailPendingType==="books_gt_26as"
                ? allMismatch.filter(r=>(r.bk_tds||0)>(r.as_tds||0))   // Books TDS > 26AS TDS = actual TDS pending
                : emailPendingType==="untraced_26as"
                ? allMismatch.filter(r=>(r.as_tds||0)>0 && (r.bk_tds||0)<1)  // 26AS has TDS but Books has nothing = untraced deposit
                : emailPendingType==="excess_26as"
                ? allMismatch.filter(r=>(r.as_tds||0)>(r.bk_tds||0) && (r.bk_tds||0)>=1)  // 26AS > Books (both non-zero) = excess deposit
                : allMismatch;
              const activeQtrs = emailPeriodFilter; // Set — empty = All quarters
              const periodFiltered = activeQtrs.size===0 ? pendingRows : pendingRows.filter(r=>{
                return datasets["26AS"].some(d=>d.tan===r.tan && activeQtrs.has(d.quarter));
              });
              // Value-wise filter
              const minAmt = emailMinAmt ? parseFloat(emailMinAmt) : null;
              const maxAmt = emailMaxAmt ? parseFloat(emailMaxAmt) : null;
              const valueFiltered = periodFiltered.filter(r=>{
                const diff = Math.abs((r.bk_tds||0)-(r.as_tds||0));
                if(minAmt!==null && diff < minAmt) return false;
                if(maxAmt!==null && diff > maxAmt) return false;
                return true;
              });
              const sortedByAmt = [...valueFiltered].sort((a,b)=>Math.abs((b.bk_tds||0)-(b.as_tds||0))-Math.abs((a.bk_tds||0)-(a.as_tds||0)));
              const topNFiltered = emailTopN==="All" ? sortedByAmt : sortedByAmt.slice(0, parseInt(emailTopN));
              const filteredPending = topNFiltered.filter(r=>!emailSearch||[r.as_name,r.bk_name,r.tan].some(v=>String(v||"").toLowerCase().includes(emailSearch.toLowerCase())));
              const previewRow = emailPreviewTAN ? pendingRows.find(r=>r.tan===emailPreviewTAN) : (emailSelTANs.size===1 ? pendingRows.find(r=>emailSelTANs.has(r.tan)) : null);
              const fmtINR = n => n?`\u20b9${Number(n).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2})}`:"₹0.00";
              const today = new Date().toLocaleDateString("en-IN",{day:"2-digit",month:"long",year:"numeric"});
              const activeSorted = [...activeQtrs].sort();
              const periodLabel = activeQtrs.size===0 ? `FY ${selYear}` : activeQtrs.size===4 ? `FY ${selYear}` : `${activeSorted.join(", ")} of FY ${selYear}`;

              const toggleQtr = (q) => setEmailPeriodFilter(prev=>{ const s=new Set(prev); s.has(q)?s.delete(q):s.add(q); return s; });

              const generateEmailBody = (row) => {
                const name = row.as_name||row.bk_name||"Sir/Madam";
                const txns26 = datasets["26AS"].filter(r=>r.tan===row.tan);
                const txnsBkRow = datasets["Books"].filter(r=>r.tan===row.tan);
                const relevantTxns26 = activeQtrs.size===0 ? txns26 : txns26.filter(r=>activeQtrs.has(r.quarter));
                const relevantTxnsBk = activeQtrs.size===0 ? txnsBkRow : txnsBkRow.filter(r=>activeQtrs.has(r.quarter));
                const period26TDS = relevantTxns26.reduce((s,r)=>s+(r.tdsDeducted||0),0);
                const periodBkTDS = relevantTxnsBk.reduce((s,r)=>s+(r.tdsDeducted||0),0);
                const qtrsToShow = activeQtrs.size===0 ? ["Q1","Q2","Q3","Q4"] : activeSorted;
                const statusColor = row.matchStatus==="Matched"?"#107c10":row.matchStatus==="Near Match"?"#c7792a":"#a80000";
                const sig = [
                  emailConfig.ourName,
                  emailConfig.ourDesignation,
                  emailConfig.ourFirm,
                  emailConfig.ourPhone ? "Mob: "+emailConfig.ourPhone : "",
                  emailConfig.ourEmail ? "Email: "+emailConfig.ourEmail : "",
                ].filter(Boolean).join("<br>");

                // ── SCENARIO: UNTRACED TDS (26AS deposit, no Books match) ─────────────────
                if (emailPendingType === "untraced_26as") {
                  const untracedAmt = fmtINR(activeQtrs.size===0 ? (row.as_tds||0) : period26TDS);
                  const qtrRows26 = qtrsToShow.map(q=>{
                    const q26=txns26.filter(r=>r.quarter===q).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                    if(q26===0) return null;
                    return {q,q26};
                  }).filter(Boolean);
                  const qtrTableRows = qtrRows26.map(r=>`<tr style="background:#f4f9fe"><td style="padding:9px 16px;font-weight:700;color:#0078d4;border-bottom:1px solid #e8e8e8">${r.q}</td><td style="padding:9px 16px;text-align:right;font-family:Consolas,monospace;font-size:13px;border-bottom:1px solid #e8e8e8">${fmtINR(r.q26)}</td><td style="padding:9px 16px;text-align:right;font-family:Consolas,monospace;font-size:13px;font-weight:700;color:#a80000;border-bottom:1px solid #e8e8e8">Not traced</td></tr>`).join("");
                  return `<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,Helvetica,sans-serif">
<div style="max-width:640px;margin:20px auto;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.12)">
  <div style="background:#0050a0;padding:22px 30px">
    <div style="color:#fff;font-size:18px;font-weight:700;letter-spacing:0.2px">TDS Credit — Invoice Details Required</div>
    <div style="color:rgba(255,255,255,0.8);font-size:12px;margin-top:4px">${periodLabel} &nbsp;&middot;&nbsp; ${today}</div>
  </div>
  <div style="padding:26px 30px">
    <table style="margin:0 0 20px;font-size:14px;color:#201f1e;border-collapse:collapse">
      <tr><td style="padding:0 0 2px"><strong>To,</strong></td></tr>
      <tr><td style="padding:0 0 2px;font-weight:600">${name}</td></tr>
      <tr><td style="padding:0 0 2px;font-family:Consolas,monospace;font-size:12.5px;color:#0078d4">TAN: ${row.tan}</td></tr>
      ${row.as_sections?`<tr><td style="padding:0;font-size:12px;color:#605e5c">Section(s): ${row.as_sections}</td></tr>`:""}
    </table>
    <p style="margin:0 0 18px;font-size:14px;color:#201f1e">Dear Sir/Madam,</p>
    <p style="margin:0 0 22px;font-size:13.5px;color:#444;line-height:1.7">We have noted a TDS credit of <strong>${untracedAmt}</strong> in our Form 26AS for <strong>${periodLabel}</strong> against your TAN. However, we are <strong>unable to trace the corresponding invoice(s)</strong> in our books of accounts against which this TDS was deducted.</p>
    <div style="background:#f4f9fe;border:1px solid #b3d4f0;border-radius:6px;padding:18px 22px;margin-bottom:22px">
      <div style="font-size:11px;font-weight:700;color:#0078d4;text-transform:uppercase;letter-spacing:1px;padding-bottom:10px;margin-bottom:14px;border-bottom:2px solid #0078d4">TDS Credit Details (as per 26AS)</div>
      <table style="width:100%;border-collapse:collapse;font-size:13.5px">
        <tr><td style="padding:5px 0;color:#605e5c;width:180px">Deductor Name</td><td style="padding:5px 0;font-weight:600;color:#201f1e">${name}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TAN</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#0078d4">${row.tan}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">Period</td><td style="padding:5px 0;color:#201f1e">${periodLabel}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TDS in 26AS</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#107c10">${untracedAmt}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TDS in Books</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#a80000">NIL / Not Found</td></tr>
        <tr><td colspan="2" style="padding-top:12px">
          <div style="background:#fff8e1;border:1px solid #ffe082;border-radius:5px;padding:10px 14px">
            <table style="width:100%;border-collapse:collapse"><tr>
              <td style="color:#5d4037;font-size:13px;font-weight:600">Untraced TDS Amount</td>
              <td style="text-align:right;font-family:Consolas,monospace;font-size:18px;font-weight:700;color:#e65100">${untracedAmt}</td>
            </tr></table>
          </div>
        </td></tr>
      </table>
    </div>
    ${qtrTableRows?`<div style="margin-bottom:22px"><table style="width:100%;border-collapse:collapse;font-size:13px"><thead><tr style="background:#e8f0fe"><th style="padding:9px 16px;text-align:left;font-weight:700;color:#0078d4;border-bottom:2px solid #b3d4f0">Quarter</th><th style="padding:9px 16px;text-align:right;font-weight:700;color:#0078d4;border-bottom:2px solid #b3d4f0">TDS in 26AS</th><th style="padding:9px 16px;text-align:right;font-weight:700;color:#0078d4;border-bottom:2px solid #b3d4f0">Status in Books</th></tr>${qtrTableRows}</table></div>`:""}
    <div style="border-left:3px solid #0078d4;background:#f8f9fa;padding:14px 18px;border-radius:0 5px 5px 0;margin-bottom:20px">
      <div style="font-size:13px;font-weight:700;color:#201f1e;margin-bottom:8px">We request you to kindly provide:</div>
      <ol style="margin:0;padding-left:18px;color:#444;font-size:13px;line-height:1.9">
        <li>Invoice number(s) against which the above TDS was deducted</li>
        <li>TDS Certificate (Form 16A) for the said period</li>
        <li>Details of the transaction / nature of payment</li>
        ${emailConfig.dueDate?`<li><strong>Please respond by: ${emailConfig.dueDate}</strong></li>`:""}
      </ol>
    </div>
    ${emailConfig.extraNote?`<p style="margin:0 0 16px;font-size:13px;color:#444;line-height:1.7;padding:12px 14px;background:#fffde7;border-radius:4px;border:1px solid #ffe082">${emailConfig.extraNote}</p>`:""}
    <p style="margin:0 0 16px;font-size:13px;color:#444;line-height:1.7">If this TDS pertains to an invoice that has already been raised, please share the invoice reference so we can update our records accordingly. Failure to provide the details may result in the TDS credit being <strong>held unreconciled in our books</strong>.</p>
    <p style="margin:0 0 26px;font-size:13px;color:#444">Your early response will be highly appreciated. Kindly <strong>reply to this email</strong> with the required details.</p>
    <p style="margin:0;font-size:13px;color:#605e5c;line-height:1.8">Thanking you,<br><br>${sig}</p>
  </div>
  <div style="background:#f3f3f3;border-top:1px solid #e0e0e0;padding:10px 30px;font-size:11px;color:#a19f9d;text-align:center">
    Generated by 26AS Recon Suite &nbsp;&middot;&nbsp; ${today}
  </div>
</div>
</body></html>`;
                }

                // ── SCENARIO: EXCESS TDS (26AS > Books, both have values) ────────────────
                if (emailPendingType === "excess_26as") {
                  const booksTDS   = fmtINR(activeQtrs.size===0 ? (row.bk_tds||0) : periodBkTDS);
                  const as26TDS    = fmtINR(activeQtrs.size===0 ? (row.as_tds||0) : period26TDS);
                  const excessAmt  = activeQtrs.size===0 ? Math.max(0,(row.as_tds||0)-(row.bk_tds||0)) : Math.max(0,period26TDS-periodBkTDS);
                  const excessFmt  = fmtINR(excessAmt);
                  return `<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,Helvetica,sans-serif">
<div style="max-width:640px;margin:20px auto;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.12)">
  <div style="background:#c25c00;padding:22px 30px">
    <div style="color:#fff;font-size:18px;font-weight:700;letter-spacing:0.2px">Excess TDS Deposited — Clarification Required</div>
    <div style="color:rgba(255,255,255,0.8);font-size:12px;margin-top:4px">${periodLabel} &nbsp;&middot;&nbsp; ${today}</div>
  </div>
  <div style="padding:26px 30px">
    <table style="margin:0 0 20px;font-size:14px;color:#201f1e;border-collapse:collapse">
      <tr><td style="padding:0 0 2px"><strong>To,</strong></td></tr>
      <tr><td style="padding:0 0 2px;font-weight:600">${name}</td></tr>
      <tr><td style="padding:0 0 2px;font-family:Consolas,monospace;font-size:12.5px;color:#0078d4">TAN: ${row.tan}</td></tr>
      ${row.as_sections?`<tr><td style="padding:0;font-size:12px;color:#605e5c">Section(s): ${row.as_sections}</td></tr>`:""}
    </table>
    <p style="margin:0 0 18px;font-size:14px;color:#201f1e">Dear Sir/Madam,</p>
    <p style="margin:0 0 22px;font-size:13.5px;color:#444;line-height:1.7">We have observed a discrepancy while reconciling the TDS credits for <strong>${periodLabel}</strong>. As per Form 26AS, the TDS deposited by you (<strong>${as26TDS}</strong>) is <strong>higher</strong> than the TDS recorded in our books of accounts (<strong>${booksTDS}</strong>). This results in an excess TDS deposit of <strong>${excessFmt}</strong> which requires your clarification.</p>
    <div style="background:#fff8f0;border:1px solid #ffd0a0;border-radius:6px;padding:18px 22px;margin-bottom:22px">
      <div style="font-size:11px;font-weight:700;color:#c25c00;text-transform:uppercase;letter-spacing:1px;padding-bottom:10px;margin-bottom:14px;border-bottom:2px solid #c25c00">Excess TDS Details</div>
      <table style="width:100%;border-collapse:collapse;font-size:13.5px">
        <tr><td style="padding:5px 0;color:#605e5c;width:180px">Deductor Name</td><td style="padding:5px 0;font-weight:600;color:#201f1e">${name}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TAN</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#0078d4">${row.tan}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">Period</td><td style="padding:5px 0;color:#201f1e">${periodLabel}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TDS as per Books</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#107c10">${booksTDS}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TDS as per 26AS</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#c25c00">${as26TDS}</td></tr>
        <tr><td colspan="2" style="padding-top:12px">
          <div style="background:#fff3e0;border:1px solid #ffa040;border-radius:5px;padding:10px 14px">
            <table style="width:100%;border-collapse:collapse"><tr>
              <td style="color:#4e2a00;font-size:13px;font-weight:600">Excess Amount</td>
              <td style="text-align:right;font-family:Consolas,monospace;font-size:18px;font-weight:700;color:#c25c00">${excessFmt}</td>
            </tr></table>
          </div>
        </td></tr>
      </table>
    </div>
    <div style="border-left:3px solid #c25c00;background:#f8f9fa;padding:14px 18px;border-radius:0 5px 5px 0;margin-bottom:20px">
      <div style="font-size:13px;font-weight:700;color:#201f1e;margin-bottom:8px">We request you to kindly:</div>
      <ol style="margin:0;padding-left:18px;color:#444;font-size:13px;line-height:1.9">
        <li>Verify the excess TDS amount of <strong>${excessFmt}</strong> in your records</li>
        <li>Clarify whether this excess pertains to any additional invoice not yet raised by us</li>
        <li>If deposited in error, kindly revise the TDS Return at your end</li>
        <li>Share the TDS Certificate (Form 16A) covering the entire deposited amount</li>
        ${emailConfig.dueDate?`<li><strong>Please respond by: ${emailConfig.dueDate}</strong></li>`:""}
      </ol>
    </div>
    ${emailConfig.extraNote?`<p style="margin:0 0 16px;font-size:13px;color:#444;line-height:1.7;padding:12px 14px;background:#fffde7;border-radius:4px;border:1px solid #ffe082">${emailConfig.extraNote}</p>`:""}
    <p style="margin:0 0 16px;font-size:13px;color:#444;line-height:1.7">Please note that until this excess is clarified, we will be unable to fully reconcile the TDS credit in our books. If the excess pertains to a future invoice or credit note, kindly provide relevant documentation so we may process the same.</p>
    <p style="margin:0 0 26px;font-size:13px;color:#444">Your prompt response will help us close this matter at the earliest. Kindly <strong>reply to this email</strong> with the required details.</p>
    <p style="margin:0;font-size:13px;color:#605e5c;line-height:1.8">Thanking you,<br><br>${sig}</p>
  </div>
  <div style="background:#f3f3f3;border-top:1px solid #e0e0e0;padding:10px 30px;font-size:11px;color:#a19f9d;text-align:center">
    Generated by 26AS Recon Suite &nbsp;&middot;&nbsp; ${today}
  </div>
</div>
</body></html>`;
                }

                // ── SCENARIO: TDS PENDING (original — Books > 26AS) ───────────────────────
                const pendingDiff = activeQtrs.size===0 ? Math.max(0,(row.bk_tds||0)-(row.as_tds||0)) : Math.max(0,periodBkTDS-period26TDS);
                const tdsAmt = fmtINR(pendingDiff||Math.abs(row.tds_diff||row.as_tds||0));
                const qtrRows = qtrsToShow.map(q=>{
                  const q26=txns26.filter(r=>r.quarter===q).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                  const qBk=txnsBkRow.filter(r=>r.quarter===q).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                  if(q26===0&&qBk===0) return null;
                  return {q,q26,qBk,pending:qBk>q26?qBk-q26:null};
                }).filter(Boolean);
                const qtrTableRows = qtrRows.map(r=>`<tr style="background:${r.pending?"#fff8f8":"#f9f9f9"}"><td style="padding:9px 16px;font-weight:700;color:#0078d4;border-bottom:1px solid #e8e8e8">${r.q}</td><td style="padding:9px 16px;text-align:right;font-family:Consolas,monospace;font-size:13px;border-bottom:1px solid #e8e8e8">${fmtINR(r.q26)}</td><td style="padding:9px 16px;text-align:right;font-family:Consolas,monospace;font-size:13px;border-bottom:1px solid #e8e8e8">${fmtINR(r.qBk)}</td><td style="padding:9px 16px;text-align:right;font-family:Consolas,monospace;font-size:13px;font-weight:700;color:${r.pending?"#a80000":"#107c10"};border-bottom:1px solid #e8e8e8">${r.pending?fmtINR(r.pending):"NIL"}</td></tr>`).join("");
                return `<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,Helvetica,sans-serif">
<div style="max-width:640px;margin:20px auto;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.12)">

  <div style="background:#0078d4;padding:22px 30px">
    <div style="color:#fff;font-size:18px;font-weight:700;letter-spacing:0.2px">TDS Pending Details</div>
    <div style="color:rgba(255,255,255,0.8);font-size:12px;margin-top:4px">${periodLabel} &nbsp;&middot;&nbsp; ${today}</div>
  </div>

  <div style="padding:26px 30px">
    <table style="margin:0 0 20px;font-size:14px;color:#201f1e;border-collapse:collapse">
      <tr><td style="padding:0 0 2px"><strong>To,</strong></td></tr>
      <tr><td style="padding:0 0 2px;font-weight:600">${name}</td></tr>
      <tr><td style="padding:0 0 2px;font-family:Consolas,monospace;font-size:12.5px;color:#0078d4">TAN: ${row.tan}</td></tr>
      ${row.as_sections?`<tr><td style="padding:0;font-size:12px;color:#605e5c">Section(s): ${row.as_sections}</td></tr>`:""}
    </table>

    <p style="margin:0 0 18px;font-size:14px;color:#201f1e">Dear Sir/Madam,</p>
    <p style="margin:0 0 22px;font-size:13.5px;color:#444;line-height:1.7">This is to bring to your kind attention that as per our books of accounts, Tax Deducted at Source (TDS) has been recorded against your TAN for <strong>${periodLabel}</strong>. However, upon verification with Form 26AS, we observe that the TDS deposited / reflected in Form 26AS is lower than the TDS deducted as per our records. This creates a pending TDS demand which requires your <strong>immediate attention</strong>.</p>

    <div style="background:#f4f9fe;border:1px solid #b3d4f0;border-radius:6px;padding:18px 22px;margin-bottom:22px">
      <div style="font-size:11px;font-weight:700;color:#0078d4;text-transform:uppercase;letter-spacing:1px;padding-bottom:10px;margin-bottom:14px;border-bottom:2px solid #0078d4">Pending TDS Details</div>
      <table style="width:100%;border-collapse:collapse;font-size:13.5px">
        <tr><td style="padding:5px 0;color:#605e5c;width:180px">Deductor Name</td><td style="padding:5px 0;font-weight:600;color:#201f1e">${name}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TAN</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#0078d4">${row.tan}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">Period</td><td style="padding:5px 0;color:#201f1e">${periodLabel}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">Status</td><td style="padding:5px 0"><span style="background:${statusColor}22;color:${statusColor};font-weight:700;padding:2px 10px;border-radius:4px;font-size:12px">${row.matchStatus}</span></td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TDS as per Books</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#107c10">${activeQtrs.size===0?fmtINR(row.bk_tds):fmtINR(periodBkTDS)}</td></tr>
        <tr><td style="padding:5px 0;color:#605e5c">TDS as per 26AS</td><td style="padding:5px 0;font-family:Consolas,monospace;font-weight:600;color:#a80000">${activeQtrs.size===0?fmtINR(row.as_tds):fmtINR(period26TDS)}</td></tr>
        <tr><td colspan="2" style="padding-top:12px">
          <div style="background:#fff3f0;border:1px solid #f0c4b4;border-radius:5px;padding:10px 14px">
            <table style="width:100%;border-collapse:collapse"><tr>
              <td style="color:#605e5c;font-size:13px;font-weight:600">Pending Amount</td>
              <td style="text-align:right;font-family:Consolas,monospace;font-size:18px;font-weight:700;color:#a80000">${tdsAmt}</td>
            </tr></table>
          </div>
        </td></tr>
      </table>
    </div>

    ${qtrTableRows?`<div style="margin-bottom:22px"><table style="width:100%;border-collapse:collapse;font-size:13px"><thead><tr style="background:#f4f9fe"><th style="padding:9px 16px;text-align:left;font-weight:700;color:#0078d4;border-bottom:2px solid #b3d4f0">Quarter</th><th style="padding:9px 16px;text-align:right;font-weight:700;color:#0078d4;border-bottom:2px solid #b3d4f0">TDS — 26AS</th><th style="padding:9px 16px;text-align:right;font-weight:700;color:#0078d4;border-bottom:2px solid #b3d4f0">TDS — Books</th><th style="padding:9px 16px;text-align:right;font-weight:700;color:#0078d4;border-bottom:2px solid #b3d4f0">Pending</th></tr>${qtrTableRows}</table></div>`:""}

    <div style="border-left:3px solid #0078d4;background:#f8f9fa;padding:14px 18px;border-radius:0 5px 5px 0;margin-bottom:20px">
      <div style="font-size:13px;font-weight:700;color:#201f1e;margin-bottom:8px">You are requested to kindly:</div>
      <ol style="margin:0;padding-left:18px;color:#444;font-size:13px;line-height:1.9">
        <li>Verify the above TDS amount in your records</li>
        <li>Issue the TDS Certificate (Form 16A) at the earliest</li>
        <li>Confirm compliance or provide necessary clarification</li>
        ${emailConfig.dueDate?`<li><strong>Respond by: ${emailConfig.dueDate}</strong></li>`:""}
      </ol>
    </div>

    ${emailConfig.extraNote?`<p style="margin:0 0 16px;font-size:13px;color:#444;line-height:1.7;padding:12px 14px;background:#fffde7;border-radius:4px;border:1px solid #ffe082">${emailConfig.extraNote}</p>`:""}

    <p style="margin:0 0 16px;font-size:13px;color:#444;line-height:1.7">Please note that if the TDS has not been deposited, we may be required to <strong>reverse the TDS amount in our books</strong>. Consequently, the same will be reflected as <strong>receivable from your end</strong>.</p>
    <p style="margin:0 0 26px;font-size:13px;color:#444">Please treat this as <strong>urgent</strong> and kindly <strong>reply to this email</strong> at the earliest.</p>
    <p style="margin:0;font-size:13px;color:#605e5c;line-height:1.8">Thanking you,<br><br>${sig}</p>
  </div>

  <div style="background:#f3f3f3;border-top:1px solid #e0e0e0;padding:10px 30px;font-size:11px;color:#a19f9d;text-align:center">
    Generated by 26AS Recon Suite &nbsp;&middot;&nbsp; ${today}
  </div>
</div>
</body></html>`;
              };

              // Wire ref so sendViaGmail can call generateEmailBody
              generateEmailBodyRef.current = generateEmailBody;

              // Wire attachment builder ref
              attachmentBuilderRef.current = (row) => buildAttachmentForRow(row, periodLabel, today, activeQtrs, datasets, curCompany, selYear, emailConfig);

              // Build quarter table rows for visual preview
              const getQtrTableRows = (row) => {
                const txns26r = datasets["26AS"].filter(r=>r.tan===row.tan);
                const txnsBkr = datasets["Books"].filter(r=>r.tan===row.tan);
                return ["Q1","Q2","Q3","Q4"].map(q=>{
                  const q26=txns26r.filter(r=>r.quarter===q).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                  const qBk=txnsBkr.filter(r=>r.quarter===q).reduce((s,r)=>s+(r.tdsDeducted||0),0);
                  if(q26===0&&qBk===0) return null;
                  return { q, q26, qBk, pending: qBk>q26 ? qBk-q26 : null };
                }).filter(Boolean);
              };

              const downloadAttachment = (row) => {
                const name = row.as_name||row.bk_name||"Deductor";
                const txns26 = datasets["26AS"].filter(r=>r.tan===row.tan);
                const txnsBk = datasets["Books"].filter(r=>r.tan===row.tan);
                const rel26 = activeQtrs.size===0 ? txns26 : txns26.filter(r=>activeQtrs.has(r.quarter));
                const relBk = activeQtrs.size===0 ? txnsBk : txnsBk.filter(r=>activeQtrs.has(r.quarter));
                const wb = XLSX.utils.book_new();
                const summaryRows = [["TDS RECONCILIATION ATTACHMENT"],[""],["Company",curCompany?.name||""],["Deductor",name],["TAN",row.tan],["Period",periodLabel],["Status",row.matchStatus],[""],["TDS as per 26AS",row.as_tds||0],["TDS as per Books",row.bk_tds||0],["Difference",row.tds_diff||0],[""],["Generated on",today],["Prepared by",emailConfig.ourName||""]];
                const wsSummary = XLSX.utils.aoa_to_sheet(summaryRows);
                wsSummary["!cols"]=[{wch:22},{wch:36}];
                XLSX.utils.book_append_sheet(wb,wsSummary,"Summary");
                const ws26 = XLSX.utils.aoa_to_sheet([["#","Date","Quarter","Section","Amount Paid","TDS Deducted","TDS Deposited","Booking Status"],...rel26.map((r,i)=>[i+1,r.date||"",r.quarter||"",r.section||"",r.amountPaid||0,r.tdsDeducted||0,r.tdsDeposited||r.tdsDeducted||0,r.bookingStatus||""]),["","","","TOTAL","",rel26.reduce((s,r)=>s+(r.tdsDeducted||0),0),rel26.reduce((s,r)=>s+(r.tdsDeposited||r.tdsDeducted||0),0),""]]);
                ws26["!cols"]=[4,13,7,10,14,14,14,12].map(w=>({wch:w}));
                XLSX.utils.book_append_sheet(wb,ws26,"26AS Entries");
                const wsBk = XLSX.utils.aoa_to_sheet([["#","Date","Quarter","Invoice No","Section","TDS Amount"],...relBk.map((r,i)=>[i+1,r.date||"",r.quarter||"",r.invoiceNo||"",r.section||"",r.tdsDeducted||0]),["","","","","TOTAL",relBk.reduce((s,r)=>s+(r.tdsDeducted||0),0)]]);
                wsBk["!cols"]=[4,13,7,20,10,13].map(w=>({wch:w}));
                XLSX.utils.book_append_sheet(wb,wsBk,"Books Entries");
                const buf=XLSX.write(wb,{bookType:"xlsx",type:"array"});
                const blob=new Blob([buf],{type:"application/octet-stream"});
                const safeName=(name||row.tan).replace(/[^a-zA-Z0-9_]/g,"_").slice(0,30);
                const fname=`TDS_Attachment_${safeName}_${row.tan}_${selYear}.xlsx`;
                if(isElectron){const reader=new FileReader();reader.onload=()=>window.electronAPI.saveFile({defaultName:fname,content:reader.result.split(",")[1],isBase64:true}).then(r=>r?.success&&showToast("Saved: "+r.path));reader.readAsDataURL(blob);}
                else{const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=fname;a.click();showToast(`Downloaded: ${fname}`,"s");}
              };

              const downloadAllAttachments = () => {
                const selected = pendingRows.filter(r=>emailSelTANs.has(r.tan));
                if(!selected.length) return;
                const wb = XLSX.utils.book_new();
                selected.forEach(row=>{
                  const shName=(row.as_name||row.bk_name||row.tan).replace(/[^a-zA-Z0-9 ]/g," ").slice(0,28).trim();
                  const t26=(activeQtrs.size===0?datasets["26AS"]:datasets["26AS"].filter(r=>activeQtrs.has(r.quarter))).filter(r=>r.tan===row.tan);
                  const tBk=(activeQtrs.size===0?datasets["Books"]:datasets["Books"].filter(r=>activeQtrs.has(r.quarter))).filter(r=>r.tan===row.tan);
                  const shRows=[
                    [`Deductor: ${shName}  |  TAN: ${row.tan}  |  Period: ${periodLabel}  |  Status: ${row.matchStatus}`],[],
                    ["── 26AS ENTRIES ──","","","","",""],
                    ["Date","Quarter","Section","Amt Paid","TDS Deducted","TDS Deposited"],
                    ...t26.map(r=>[r.date||"",r.quarter||"",r.section||"",r.amountPaid||0,r.tdsDeducted||0,r.tdsDeposited||r.tdsDeducted||0]),
                    ["","","TOTAL 26AS","",t26.reduce((s,r)=>s+(r.tdsDeducted||0),0),""],
                    [],
                    ["── BOOKS ENTRIES ──","","","","",""],
                    ["Date","Quarter","Invoice No","Section","TDS",""],
                    ...tBk.map(r=>[r.date||"",r.quarter||"",r.invoiceNo||"",r.section||"",r.tdsDeducted||0,""]),
                    ["","","TOTAL BOOKS","",tBk.reduce((s,r)=>s+(r.tdsDeducted||0),0),""],
                    [],
                    ["26AS TDS",row.as_tds||0,"Books TDS",row.bk_tds||0,"Diff",row.tds_diff||0],
                  ];
                  const ws=XLSX.utils.aoa_to_sheet(shRows);
                  ws["!cols"]=[13,7,18,13,13,13].map(w=>({wch:w}));
                  XLSX.utils.book_append_sheet(wb,ws,shName.slice(0,31));
                });
                const buf=XLSX.write(wb,{bookType:"xlsx",type:"array"});
                const blob=new Blob([buf],{type:"application/octet-stream"});
                const ts=new Date().toISOString().slice(0,10);
                const fname=`TDS_Attachments_${curCompany?.name?.replace(/\s+/g,"_")}_${selYear}_${ts}.xlsx`;
                if(isElectron){const reader=new FileReader();reader.onload=()=>window.electronAPI.saveFile({defaultName:fname,content:reader.result.split(",")[1],isBase64:true}).then(r=>r?.success&&showToast("Saved: "+r.path));reader.readAsDataURL(blob);}
                else{const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=fname;a.click();showToast(`Downloaded ${selected.length} attachment(s)`,"s");}
              };

              return (
                <div style={{display:"flex",flex:1,overflow:"hidden"}}>
                  {/* LEFT PANEL */}
                  <div style={{width:330,flexShrink:0,borderRight:"1px solid var(--bd)",display:"flex",flexDirection:"column",background:"var(--wh)"}}>
                    <div style={{padding:"10px 12px",borderBottom:"1px solid var(--bd)",background:"var(--hb)"}}>
                      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:7}}>
                        <div style={{fontSize:13,fontWeight:600}}>Pending Deductors</div>
                        {/* Gmail quick-connect badge always visible */}
                        {isGmailConnected?(
                          <span style={{fontSize:10.5,color:"var(--grn)",fontWeight:600,background:"#e8f8e8",border:"1px solid #b3dab3",borderRadius:3,padding:"2px 8px"}}>✅ Gmail: {gmailUser?.email?.split("@")[0]}</span>
                        ):(
                          <button onClick={()=>{setGmailClientIdDraft("");setShowGmailSetup(true);}} style={{fontSize:10.5,fontWeight:600,background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"3px 9px",cursor:"pointer",fontFamily:"inherit"}}>⚙️ Setup Gmail</button>
                        )}
                      </div>

                      {/* ── Filters Collapsible Panel ── */}
                      <div style={{border:"1px solid var(--bd)",borderRadius:4,marginBottom:7,overflow:"hidden"}}>
                        <div onClick={()=>setEmailFiltersOpen(o=>!o)} style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"6px 10px",background:"var(--sur)",cursor:"pointer",userSelect:"none"}} >
                          <div style={{display:"flex",alignItems:"center",gap:7}}>
                            <span style={{fontSize:11.5,fontWeight:600,color:"var(--tx)"}}>🔽 Filters</span>
                            {/* Active filter badges */}
                            {emailPendingType!=="books_gt_26as"&&<span style={{fontSize:9.5,background:"var(--a)",color:"#fff",borderRadius:9,padding:"1px 6px",fontWeight:600}}>{emailPendingType==="untraced_26as"?"Untraced":emailPendingType==="excess_26as"?"Excess":"All Mismatches"}</span>}
                            {(emailMinAmt||emailMaxAmt)&&<span style={{fontSize:9.5,background:"var(--amb)",color:"#fff",borderRadius:9,padding:"1px 6px",fontWeight:600}}>₹ Range</span>}
                            {activeQtrs.size>0&&<span style={{fontSize:9.5,background:"var(--a)",color:"#fff",borderRadius:9,padding:"1px 6px",fontWeight:600}}>{activeSorted.join("+")}</span>}
                            {emailTopN!=="All"&&<span style={{fontSize:9.5,background:"var(--pur)",color:"#fff",borderRadius:9,padding:"1px 6px",fontWeight:600}}>Top {emailTopN}</span>}
                          </div>
                          <span style={{fontSize:11,color:"var(--tx3)",transform:emailFiltersOpen?"rotate(180deg)":"rotate(0deg)",transition:"transform 0.2s"}}>▼</span>
                        </div>
                        {emailFiltersOpen&&(
                          <div style={{padding:"10px 12px",background:"var(--wh)"}}>

                      {/* ── Pending Type Filter ── */}
                      <div style={{marginBottom:7}}>
                        <div style={{fontSize:10,color:"var(--tx3)",marginBottom:3,textTransform:"uppercase",letterSpacing:"0.4px",fontWeight:600}}>Show Customers</div>
                        <div style={{display:"flex",gap:0,border:"1px solid var(--bd)",borderRadius:3,overflow:"hidden"}}>
                          {[
                            {v:"books_gt_26as",  l:"📥 TDS Not Deposited",  title:"Books TDS > 26AS — customer deducted but didn't deposit"},
                            {v:"untraced_26as",  l:"🔍 Untraced in Books",   title:"26AS shows TDS deposit but no matching invoice in Books"},
                            {v:"excess_26as",    l:"⬆️ Excess Deposited",    title:"26AS TDS > Books TDS — customer deposited more than expected"},
                            {v:"all_pending",    l:"⚠️ All Mismatches",      title:"All mismatch types combined"},
                          ].map((opt,i)=>(
                            <button key={opt.v} title={opt.title} onClick={()=>setEmailPendingType(opt.v)} style={{flex:1,padding:"4px 6px",fontSize:10.5,border:"none",borderLeft:i>0?"1px solid var(--bd)":"none",background:emailPendingType===opt.v?"var(--a)":"var(--wh)",color:emailPendingType===opt.v?"#fff":"var(--tx2)",cursor:"pointer",fontFamily:"inherit",fontWeight:emailPendingType===opt.v?600:400,lineHeight:1.3}}>
                              {opt.l}
                            </button>
                          ))}
                        </div>
                        {emailPendingType==="books_gt_26as"&&<div style={{fontSize:10,color:"var(--grn)",marginTop:3}}>✅ Customers where TDS was deducted in Books but not yet reflected in 26AS</div>}
                        {emailPendingType==="untraced_26as"&&<div style={{fontSize:10,color:"#0078d4",marginTop:3}}>🔍 TDS deposited in 26AS with no matching invoice in your Books — request details from customer</div>}
                        {emailPendingType==="excess_26as"&&<div style={{fontSize:10,color:"var(--amb)",marginTop:3}}>⬆️ 26AS deposit exceeds Books amount — excess TDS found, request clarification or credit note</div>}
                      </div>

                      {/* ── Value Range Filter ── */}
                      <div style={{marginBottom:7}}>
                        <div style={{fontSize:10,color:"var(--tx3)",marginBottom:3,textTransform:"uppercase",letterSpacing:"0.4px",fontWeight:600}}>Pending Amount Range (₹)</div>
                        <div style={{display:"flex",gap:5,alignItems:"center"}}>
                          <input type="number" value={emailMinAmt} onChange={e=>setEmailMinAmt(e.target.value)} placeholder="Min" style={{flex:1,border:"1px solid var(--bd)",borderRadius:3,padding:"4px 7px",fontSize:11.5,fontFamily:"inherit",outline:"none",color:"var(--tx)"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                          <span style={{fontSize:11,color:"var(--tx3)"}}>—</span>
                          <input type="number" value={emailMaxAmt} onChange={e=>setEmailMaxAmt(e.target.value)} placeholder="Max" style={{flex:1,border:"1px solid var(--bd)",borderRadius:3,padding:"4px 7px",fontSize:11.5,fontFamily:"inherit",outline:"none",color:"var(--tx)"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                          {(emailMinAmt||emailMaxAmt)&&<button onClick={()=>{setEmailMinAmt("");setEmailMaxAmt("");}} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"3px 7px",cursor:"pointer",fontSize:10,color:"var(--tx3)",fontFamily:"inherit"}}>✕</button>}
                        </div>
                        {(emailMinAmt||emailMaxAmt)&&<div style={{fontSize:10,color:"var(--amb)",marginTop:3}}>💰 Filtering: {emailMinAmt?`₹${Number(emailMinAmt).toLocaleString("en-IN")}+`:"any"} to {emailMaxAmt?`₹${Number(emailMaxAmt).toLocaleString("en-IN")}`:"unlimited"}</div>}
                      </div>

                      {/* Multi-Quarter Filter */}
                      <div style={{marginBottom:7}}>
                        <div style={{fontSize:10,color:"var(--tx3)",marginBottom:3,textTransform:"uppercase",letterSpacing:"0.4px",fontWeight:600}}>Filter by Period <span style={{color:"var(--a)",fontWeight:400,textTransform:"none",letterSpacing:0}}>(multi-select)</span></div>
                        <div style={{display:"flex",gap:4}}>
                          <button onClick={()=>setEmailPeriodFilter(new Set())} style={{padding:"3px 9px",fontSize:11,border:`1px solid ${activeQtrs.size===0?"var(--a)":"var(--bd)"}`,borderRadius:3,background:activeQtrs.size===0?"var(--a)":"var(--wh)",color:activeQtrs.size===0?"#fff":"var(--tx2)",cursor:"pointer",fontFamily:"inherit",fontWeight:activeQtrs.size===0?600:400}}>All</button>
                          {["Q1","Q2","Q3","Q4"].map(q=>(
                            <button key={q} onClick={()=>toggleQtr(q)} style={{padding:"3px 9px",fontSize:11,border:`1px solid ${activeQtrs.has(q)?"var(--a)":"var(--bd)"}`,borderRadius:3,background:activeQtrs.has(q)?"var(--a)":"var(--wh)",color:activeQtrs.has(q)?"#fff":"var(--tx2)",cursor:"pointer",fontFamily:"inherit",fontWeight:activeQtrs.has(q)?600:400,position:"relative"}}>
                              {q}{activeQtrs.has(q)&&<span style={{position:"absolute",top:-4,right:-4,width:7,height:7,background:"var(--grn)",borderRadius:"50%",border:"1px solid #fff"}}/>}
                            </button>
                          ))}
                        </div>
                        {activeQtrs.size>0&&<div style={{fontSize:10,color:"var(--a)",marginTop:3}}>📅 {activeSorted.join(" + ")} selected — notice references selected period TDS</div>}
                      </div>

                      {/* Top N Filter */}
                      <div style={{marginBottom:0}}>
                        <div style={{fontSize:10,color:"var(--tx3)",marginBottom:3,textTransform:"uppercase",letterSpacing:"0.4px",fontWeight:600}}>Top Customers by Amount</div>
                        <div style={{display:"flex",gap:4,flexWrap:"wrap"}}>
                          {["All","5","10","20","50"].map(n=>(
                            <button key={n} onClick={()=>setEmailTopN(n)} style={{padding:"3px 9px",fontSize:11,border:`1px solid ${emailTopN===n?"var(--pur)":"var(--bd)"}`,borderRadius:3,background:emailTopN===n?"var(--pur)":"var(--wh)",color:emailTopN===n?"#fff":"var(--tx2)",cursor:"pointer",fontFamily:"inherit",fontWeight:emailTopN===n?600:400}}>
                              {n==="All"?"All":n==="5"?"Top 5":n==="10"?"Top 10":n==="20"?"Top 20":"Top 50"}
                            </button>
                          ))}
                        </div>
                        {emailTopN!=="All"&&<div style={{fontSize:10,color:"var(--pur)",marginTop:3}}>🏆 Sorted by highest pending TDS</div>}
                      </div>

                          </div>
                        )}
                      </div>

                      {/* Search */}
                      <div style={{display:"flex",gap:6,marginBottom:6}}>
                        <div className="srch" style={{flex:1}}>
                          <Ic d={I.search} s={11} c="#888"/>
                          <input placeholder="Search deductor / TAN..." value={emailSearch} onChange={e=>setEmailSearch(e.target.value)} style={{width:"100%",fontSize:11,border:"none",background:"none",outline:"none",fontFamily:"inherit",color:"var(--tx)"}}/>
                          {emailSearch&&<span onClick={()=>setEmailSearch("")} style={{cursor:"pointer",color:"#999",fontSize:10}}>✕</span>}
                        </div>
                      </div>
                      <div style={{display:"flex",gap:6,alignItems:"center"}}>
                        <label style={{fontSize:11,color:"var(--tx2)",display:"flex",alignItems:"center",gap:4,cursor:"pointer"}}>
                          <input type="checkbox" checked={emailSelTANs.size===filteredPending.length&&filteredPending.length>0} onChange={e=>setEmailSelTANs(e.target.checked?new Set(filteredPending.map(r=>r.tan)):new Set())} style={{accentColor:"var(--a)"}}/>
                          Select all ({filteredPending.length})
                        </label>
                        {emailSelTANs.size>0&&<span style={{marginLeft:"auto",fontSize:10.5,color:"var(--a)",fontWeight:600}}>{emailSelTANs.size} selected</span>}
                      </div>
                    </div>

                    {!reconDone?(
                      <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",flex:1,padding:24,textAlign:"center",gap:8,color:"var(--tx2)"}}>
                        <Ic d={I.mail} s={36} c="#d1d1d1" sw={1}/>
                        <div style={{fontSize:12.5}}>Run Reconciliation first</div>
                        <button className="ib" style={{fontSize:11,padding:"5px 14px",marginTop:4}} onClick={()=>setView("recon")}>Go to Recon</button>
                      </div>
                    ):(
                      <div style={{flex:1,overflowY:"auto"}}>
                        {filteredPending.length===0&&<div style={{padding:20,textAlign:"center",color:"var(--tx3)",fontSize:12}}>No pending deductors for selected filter</div>}
                        {filteredPending.map(row=>{
                          const rowEmail = mergedEmails[row.tan]||"";
                          const rowCC = mergedCCs[row.tan]||"";
                          const hasEmail = rowEmail.includes("@");
                          const hasCC = rowCC.includes("@");
                          const openMail = (e) => {
                            e.stopPropagation();
                            if(!hasEmail){showToast("Enter email address for "+row.tan,"w");return;}
                            if(isGmailConnected){
                              sendViaGmail([row]);
                            } else {
                              const subj = encodeURIComponent((emailConfig.subject||"TDS Pending-FY "+selYear+"")+( emailConfig.refNo?" (Ref: "+emailConfig.refNo+")":""));
                              const body = encodeURIComponent(generateEmailBody(row));
                              const ccPart = rowCC.trim() ? `&cc=${encodeURIComponent(rowCC.trim())}` : "";
                              window.open(`mailto:${rowEmail}?subject=${subj}${ccPart}&body=${body}`,"_self");
                              showToast("Opening email client for "+row.tan);
                            }
                          };
                          return (
                          <div key={row.tan} onClick={()=>setEmailPreviewTAN(row.tan)} style={{display:"flex",flexDirection:"column",gap:0,borderBottom:"1px solid #f0f0f0",background:emailPreviewTAN===row.tan?"var(--a-lt)":emailSelTANs.has(row.tan)?"#f0fff0":"var(--wh)",transition:"background 0.1s"}}>
                            {/* Top row */}
                            <div style={{display:"flex",alignItems:"flex-start",gap:8,padding:"9px 12px 4px",cursor:"pointer"}}>
                              <input type="checkbox" checked={emailSelTANs.has(row.tan)} onChange={e=>{e.stopPropagation();setEmailSelTANs(prev=>{const s=new Set(prev);e.target.checked?s.add(row.tan):s.delete(row.tan);return s;});}} onClick={e=>e.stopPropagation()} style={{accentColor:"var(--a)",marginTop:3,flexShrink:0}}/>
                              <div style={{flex:1,minWidth:0}}>
                                <div style={{display:"flex",alignItems:"center",gap:6,flexWrap:"wrap"}}>
                                  <span style={{fontSize:12,fontWeight:600,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{row.as_name||row.bk_name||"—"}</span>
                                  {(()=>{
                                    const lastSent = emailLog.filter(e=>e.tan===row.tan && e.status!=="Failed").sort((a,b)=>new Date(b.sentAt)-new Date(a.sentAt))[0];
                                    if(!lastSent) return null;
                                    const sentDate = new Date(lastSent.sentAt).toLocaleDateString("en-IN",{day:"2-digit",month:"short",year:"2-digit"});
                                    return <span title={`Last emailed on ${sentDate}`} style={{fontSize:9,fontWeight:700,background:"#e8f8e8",color:"#107c10",border:"1px solid #b3dab3",borderRadius:3,padding:"1px 6px",whiteSpace:"nowrap",flexShrink:0}}>✉ Sent {sentDate}</span>;
                                  })()}
                                </div>
                                <div style={{fontSize:10.5,color:"var(--a)",fontFamily:"Consolas,monospace"}}>{row.tan}</div>
                                <div style={{display:"flex",gap:5,marginTop:2,alignItems:"center",flexWrap:"wrap"}}>
                                  <span className={`tg ${getTag(row.matchStatus)}`} style={{fontSize:9}}>{row.matchStatus}</span>
                                  {emailPendingType==="books_gt_26as"&&<span style={{fontSize:10,color:"var(--red)",fontWeight:600,fontFamily:"Consolas,monospace"}}>Pending: ₹{Math.max(0,(row.bk_tds||0)-(row.as_tds||0)).toLocaleString("en-IN",{maximumFractionDigits:0})}</span>}
                                  {emailPendingType==="untraced_26as"&&<span style={{fontSize:10,color:"#0078d4",fontWeight:600,fontFamily:"Consolas,monospace"}}>Untraced: ₹{(row.as_tds||0).toLocaleString("en-IN",{maximumFractionDigits:0})}</span>}
                                  {emailPendingType==="excess_26as"&&<span style={{fontSize:10,color:"var(--amb)",fontWeight:600,fontFamily:"Consolas,monospace"}}>Excess: ₹{Math.max(0,(row.as_tds||0)-(row.bk_tds||0)).toLocaleString("en-IN",{maximumFractionDigits:0})}</span>}
                                  {emailPendingType==="all_pending"&&<span style={{fontSize:10,color:"var(--red)",fontWeight:600,fontFamily:"Consolas,monospace"}}>Diff: ₹{Math.abs((row.bk_tds||0)-(row.as_tds||0)).toLocaleString("en-IN",{maximumFractionDigits:0})}</span>}
                                </div>
                                <div style={{display:"flex",gap:8,marginTop:1}}>
                                  <span style={{fontSize:9.5,color:"var(--tx3)"}}>Bks: <span style={{color:"var(--amb)",fontWeight:600}}>₹{(row.bk_tds||0).toLocaleString("en-IN",{maximumFractionDigits:0})}</span></span>
                                  <span style={{fontSize:9.5,color:"var(--tx3)"}}>26AS: <span style={{color:"var(--grn)",fontWeight:600}}>₹{(row.as_tds||0).toLocaleString("en-IN",{maximumFractionDigits:0})}</span></span>
                                </div>
                              </div>
                              <button title="Download Excel attachment" onClick={e=>{e.stopPropagation();downloadAttachment(row);}} style={{background:"none",border:"1px solid #d1d1d1",borderRadius:3,padding:"3px 6px",cursor:"pointer",fontSize:11,color:"#217346",flexShrink:0}} onMouseEnter={e=>e.currentTarget.style.background="var(--sur)"} onMouseLeave={e=>e.currentTarget.style.background="none"}>📎</button>
                            </div>
                            {/* Email row */}
                            <div style={{display:"flex",flexDirection:"column",gap:3,padding:"4px 12px 8px 32px"}} onClick={e=>e.stopPropagation()}>
                              {/* TO */}
                              <div style={{display:"flex",alignItems:"center",gap:5}}>
                                <span style={{fontSize:9.5,color:"var(--tx3)",width:18,flexShrink:0}}>To</span>
                                <div style={{position:"relative",flex:1}}>
                                  <input
                                    type="email"
                                    value={rowEmail}
                                    onChange={e=>updateTanEmail(row.tan, e.target.value)}
                                    placeholder="recipient@email.com"
                                    style={{width:"100%",border:`1px solid ${hasEmail?"var(--grn)":"var(--bd)"}`,borderRadius:3,padding:"3px 22px 3px 7px",fontSize:11,fontFamily:"inherit",outline:"none",color:"var(--tx)",background:"var(--wh)",boxSizing:"border-box"}}
                                    onFocus={e=>e.target.style.borderColor="var(--a)"}
                                    onBlur={e=>e.target.style.borderColor=hasEmail?"var(--grn)":"var(--bd)"}
                                  />
                                  {hasEmail&&<span style={{position:"absolute",right:6,top:"50%",transform:"translateY(-50%)",fontSize:10,color:"var(--grn)"}}>✓</span>}
                                </div>
                                <button
                                  onClick={openMail}
                                  title={hasEmail?`Send to ${rowEmail}`:"Add email first"}
                                  style={{flexShrink:0,display:"flex",alignItems:"center",gap:4,background:hasEmail?"var(--a)":"var(--bd)",color:hasEmail?"#fff":"var(--tx3)",border:"none",borderRadius:3,padding:"4px 10px",cursor:hasEmail?"pointer":"not-allowed",fontSize:11,fontFamily:"inherit",fontWeight:600,whiteSpace:"nowrap"}}
                                >
                                  ✉️ Send
                                </button>
                              </div>
                              {/* CC */}
                              <div style={{display:"flex",alignItems:"center",gap:5}}>
                                <span style={{fontSize:9.5,color:"var(--tx3)",width:18,flexShrink:0}}>CC</span>
                                <div style={{position:"relative",flex:1}}>
                                  <input
                                    type="email"
                                    value={rowCC}
                                    onChange={e=>updateTanCC(row.tan, e.target.value)}
                                    placeholder="CC (optional)"
                                    style={{width:"100%",border:`1px solid ${hasCC?"var(--grn)":"var(--bd)"}`,borderRadius:3,padding:"3px 22px 3px 7px",fontSize:11,fontFamily:"inherit",outline:"none",color:"var(--tx)",background:"var(--wh)",boxSizing:"border-box"}}
                                    onFocus={e=>e.target.style.borderColor="var(--a)"}
                                    onBlur={e=>e.target.style.borderColor=hasCC?"var(--grn)":"var(--bd)"}
                                  />
                                  {hasCC&&<span style={{position:"absolute",right:6,top:"50%",transform:"translateY(-50%)",fontSize:10,color:"var(--grn)"}}>✓</span>}
                                </div>
                                <div style={{width:60,flexShrink:0}}/>
                              </div>
                            </div>
                          </div>
                          );
                        })}
                      </div>
                    )}

                    {emailSelTANs.size>0&&(()=>{
                      const selRows = pendingRows.filter(r=>emailSelTANs.has(r.tan));
                      const withEmail = selRows.filter(r=>mergedEmails[r.tan]&&mergedEmails[r.tan].includes("@"));
                      const withoutEmail = selRows.filter(r=>!mergedEmails[r.tan]||!mergedEmails[r.tan].includes("@"));
                      const openAllMail = () => {
                        if(!withEmail.length){showToast("No email addresses set for selected TANs","w");return;}
                        if(isGmailConnected){
                          sendViaGmail(withEmail);
                        } else {
                          withEmail.forEach((row,i)=>{
                            setTimeout(()=>{
                              const subj=encodeURIComponent((emailConfig.subject||"TDS Pending-FY "+selYear+"")+(emailConfig.refNo?" (Ref: "+emailConfig.refNo+")":""));
                              const body=encodeURIComponent(generateEmailBody(row));
                              const ccPart = mergedCCs[row.tan]?.trim() ? `&cc=${encodeURIComponent(mergedCCs[row.tan].trim())}` : "";
                              window.open(`mailto:${mergedEmails[row.tan]}?subject=${subj}${ccPart}&body=${body}`,"_blank");
                            }, i*400);
                          });
                          showToast(`Opening ${withEmail.length} email(s)…`,"s");
                        }
                      };
                      return (
                      <div style={{padding:"10px 12px",borderTop:"1px solid var(--bd)",background:"var(--sur)",display:"flex",flexDirection:"column",gap:5}}>
                        {/* Summary line */}
                        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between"}}>
                          <span style={{fontSize:11,color:"var(--tx2)",fontWeight:600}}>{selRows.length} selected</span>
                          <span style={{fontSize:10.5}}>
                            <span style={{color:"var(--grn)",fontWeight:600}}>{withEmail.length} ✉️ ready</span>
                            {withoutEmail.length>0&&<span style={{color:"var(--red)",marginLeft:6}}>{withoutEmail.length} ⚠️ no email</span>}
                          </span>
                        </div>
                        {/* Primary — open mail client */}
                        <button
                          onClick={openAllMail}
                          disabled={withEmail.length===0}
                          style={{background:withEmail.length?"var(--a)":"#ccc",color:"#fff",border:"none",borderRadius:3,padding:"7px 12px",cursor:withEmail.length?"pointer":"not-allowed",fontSize:12,fontFamily:"inherit",fontWeight:700,display:"flex",alignItems:"center",justifyContent:"center",gap:6}}
                        >
                          ✉️ {isGmailConnected?"Send":"Open"} {withEmail.length} Email{withEmail.length!==1?"s":""} {isGmailConnected?"via Gmail":"in Mail Client"}
                        </button>
                        {withoutEmail.length>0&&(
                          <div style={{fontSize:10,color:"var(--red)",background:"#fff4f4",border:"1px solid #ffd0d0",borderRadius:3,padding:"4px 8px"}}>
                            ⚠️ {withoutEmail.length} TAN(s) missing email: {withoutEmail.map(r=>r.tan).join(", ")}
                          </div>
                        )}
                        <div style={{display:"flex",gap:5}}>
                          <button onClick={()=>{const txt=selRows.map(r=>generateEmailBody(r)).join("\n\n"+"═".repeat(60)+"\n\n");navigator.clipboard.writeText(txt).then(()=>showToast(`${selRows.length} notice(s) copied`,"s"));}} style={{flex:1,background:"var(--wh)",color:"var(--a)",border:"1px solid var(--a)",borderRadius:3,padding:"5px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600}}>📋 Copy All</button>
                          <button onClick={()=>{const txt=selRows.map(r=>generateEmailBody(r)).join("\n\n"+"═".repeat(60)+"\n\n");const ts=new Date().toISOString().slice(0,10);const fname=`TDS_Notice_${curCompany?.name?.replace(/\s+/g,"_")}_FY${selYear}_${ts}.html`;if(isElectron){window.electronAPI.saveFile({defaultName:fname,content:txt}).then(r=>r?.success&&showToast("Saved: "+r.path));}else{const a=document.createElement("a");a.href=URL.createObjectURL(new Blob([txt],{type:"text/html"}));a.download=fname;a.click();showToast("Downloaded");}}} style={{flex:1,background:"var(--wh)",color:"var(--grn)",border:"1px solid var(--grn)",borderRadius:3,padding:"5px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600}}>💾 Save .txt</button>
                          <button onClick={downloadAllAttachments} style={{flex:1,background:"var(--wh)",color:"#217346",border:"1px solid #217346",borderRadius:3,padding:"5px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600}}>📎 Excel</button>
                        </div>
                      </div>
                      );
                    })()}
                  </div>

                  {/* RIGHT PANEL */}
                  <div style={{flex:1,display:"flex",flexDirection:"column",overflow:"hidden"}}>
                    <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"10px 16px",flexShrink:0}}>
                      {/* Gmail Connection Status Bar */}
                      <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:8,padding:"6px 10px",borderRadius:4,background:isGmailConnected?"#e8f8e8":"#f3f3f3",border:`1px solid ${isGmailConnected?"#b3dab3":"var(--bd)"}`}}>
                        <span style={{fontSize:13}}>{isGmailConnected?"✅":"📧"}</span>
                        {isGmailConnected?(
                          <>
                            <div style={{flex:1}}>
                              <span style={{fontSize:11.5,fontWeight:600,color:"var(--grn)"}}>Gmail Connected</span>
                              <span style={{fontSize:11,color:"var(--tx2)",marginLeft:7}}>{gmailUser?.email}</span>
                            </div>
                            <button onClick={disconnectGmail} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"2px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit",color:"var(--tx2)"}}>Disconnect</button>
                          </>
                        ):(
                          <>
                            <div style={{flex:1}}>
                              <span style={{fontSize:11.5,fontWeight:600,color:"var(--tx2)"}}>Gmail not connected</span>
                              <span style={{fontSize:11,color:"var(--tx3)",marginLeft:7}}>Connect to send without mail client</span>
                            </div>
                            <button onClick={()=>{setGmailClientIdDraft("");setShowGmailSetup(true);}} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"3px 11px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600}}>⚙️ Setup Gmail</button>
                            {gmailClientId&&<button onClick={connectGmail} style={{background:"var(--grn)",color:"#fff",border:"none",borderRadius:3,padding:"3px 11px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600}}>🔗 Connect</button>}
                          </>
                        )}
                      </div>

                      {/* Collapsible Email Settings */}
                      <div style={{border:"1px solid var(--bd)",borderRadius:4,overflow:"hidden"}}>
                        <div onClick={()=>setEmailSettingsOpen(o=>!o)} style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"6px 11px",background:"var(--hb)",cursor:"pointer",userSelect:"none"}} onMouseEnter={e=>e.currentTarget.style.background="#ebebeb"} onMouseLeave={e=>e.currentTarget.style.background="var(--hb)"}>
                          <div style={{display:"flex",alignItems:"center",gap:7}}>
                            <span style={{fontSize:12}}>⚙️</span>
                            <span style={{fontSize:12,fontWeight:600,color:"var(--tx)"}}>Email Settings</span>
                            {/* show filled badge if any config is set */}
                            {(emailConfig.ourName||emailConfig.subject)&&<span style={{fontSize:9.5,background:"var(--a)",color:"#fff",borderRadius:9,padding:"1px 6px",fontWeight:600}}>{emailConfig.ourName?emailConfig.ourName.split(" ")[0]:"Set"}</span>}
                          </div>
                          <span style={{fontSize:11,color:"var(--tx3)",transform:emailSettingsOpen?"rotate(180deg)":"rotate(0deg)",transition:"transform 0.2s"}}>▼</span>
                        </div>
                        {emailSettingsOpen&&(
                          <div style={{padding:"10px 12px",background:"var(--wh)"}}>
                            <div style={{fontSize:10.5,fontWeight:600,color:"var(--tx2)",marginBottom:7,textTransform:"uppercase",letterSpacing:"0.4px"}}>Sender Details (appears in signature)</div>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:7,marginBottom:7}}>
                              {[{k:"ourName",ph:"Your Name",l:"Name"},{k:"ourDesignation",ph:"CA / Manager",l:"Designation"},{k:"ourFirm",ph:"Firm / Company Name",l:"Firm"},{k:"ourPhone",ph:"+91 98xxx xxxxx",l:"Phone"},{k:"ourEmail",ph:"your@email.com",l:"Email"},{k:"dueDate",ph:"",l:"Reply by Date",type:"date"}].map(f=>(
                                <div key={f.k}>
                                  <div style={{fontSize:9.5,color:"var(--tx3)",marginBottom:2}}>{f.l}</div>
                                  <input type={f.type||"text"} value={emailConfig[f.k]} onChange={e=>setEmailConfig(p=>({...p,[f.k]:e.target.value}))} placeholder={f.ph} style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"3px 7px",fontSize:11,fontFamily:"inherit",outline:"none",color:"var(--tx)"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                                </div>
                              ))}
                            </div>
                            <div style={{display:"grid",gridTemplateColumns:"2fr 1fr",gap:7,marginBottom:7}}>
                              <div>
                                <div style={{fontSize:9.5,color:"var(--tx3)",marginBottom:2}}>Email Subject</div>
                                <input value={emailConfig.subject} onChange={e=>setEmailConfig(p=>({...p,subject:e.target.value}))} style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"3px 7px",fontSize:11,fontFamily:"inherit",outline:"none"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                              </div>
                              <div>
                                <div style={{fontSize:9.5,color:"var(--tx3)",marginBottom:2}}>Reference No.</div>
                                <input value={emailConfig.refNo} onChange={e=>setEmailConfig(p=>({...p,refNo:e.target.value}))} placeholder="e.g. TDS/2025-26/001" style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"3px 7px",fontSize:11,fontFamily:"inherit",outline:"none"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                              </div>
                            </div>
                            <div>
                              <div style={{fontSize:9.5,color:"var(--tx3)",marginBottom:2}}>Additional Note (optional)</div>
                              <input value={emailConfig.extraNote} onChange={e=>setEmailConfig(p=>({...p,extraNote:e.target.value}))} placeholder="e.g. Please also provide Form 16A for the pending quarters." style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"3px 7px",fontSize:11,fontFamily:"inherit",outline:"none"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>

                    <div style={{flex:1,overflowY:"auto",padding:"16px 20px",background:"var(--sur)"}}>
                      {previewRow ? (
                        <div style={{background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:5,overflow:"hidden"}}>
                          <div style={{background:"var(--hb)",padding:"10px 16px",borderBottom:"1px solid var(--bd)",display:"flex",alignItems:"center",justifyContent:"space-between"}}>
                            <div style={{display:"flex",alignItems:"center",gap:8,flexWrap:"wrap"}}>
                              <span style={{fontSize:13,fontWeight:600}}>{previewRow.as_name||previewRow.bk_name}</span>
                              <span style={{fontSize:11,color:"var(--a)",fontFamily:"Consolas,monospace"}}>{previewRow.tan}</span>
                              {activeQtrs.size>0&&<span style={{fontSize:10,background:"var(--a-lt)",color:"var(--a)",padding:"1px 6px",borderRadius:3,fontWeight:600}}>{activeSorted.join(" + ")}</span>}
                            </div>
                            <div style={{display:"flex",gap:7,alignItems:"center"}}>
                              {/* To + CC inputs - HORIZONTAL LAYOUT */}
                              <div style={{display:"flex",alignItems:"center",gap:5}}>
                                <span style={{fontSize:10,color:"var(--tx3)",width:20,flexShrink:0}}>To</span>
                                <div style={{position:"relative"}}>
                                  <input
                                    type="email"
                                    value={mergedEmails[previewRow.tan]||""}
                                    onChange={e=>updateTanEmail(previewRow.tan,e.target.value)}
                                    placeholder="recipient@email.com"
                                    style={{border:`1px solid ${mergedEmails[previewRow.tan]?.includes("@")?"var(--grn)":"var(--bd)"}`,borderRadius:3,padding:"4px 24px 4px 8px",fontSize:11.5,fontFamily:"inherit",outline:"none",width:190,color:"var(--tx)"}}
                                    onFocus={e=>e.target.style.borderColor="var(--a)"}
                                    onBlur={e=>e.target.style.borderColor=mergedEmails[previewRow.tan]?.includes("@")?"var(--grn)":"var(--bd)"}
                                  />
                                  {mergedEmails[previewRow.tan]?.includes("@")&&<span style={{position:"absolute",right:7,top:"50%",transform:"translateY(-50%)",fontSize:11,color:"var(--grn)"}}>✓</span>}
                                </div>
                              </div>
                              <div style={{display:"flex",alignItems:"center",gap:5}}>
                                <span style={{fontSize:10,color:"var(--tx3)",width:20,flexShrink:0}}>CC</span>
                                <div style={{position:"relative"}}>
                                  <input
                                    type="email"
                                    value={mergedCCs[previewRow.tan]||""}
                                    onChange={e=>updateTanCC(previewRow.tan,e.target.value)}
                                    placeholder="CC (optional)"
                                    style={{border:`1px solid ${mergedCCs[previewRow.tan]?.includes("@")?"var(--grn)":"var(--bd)"}`,borderRadius:3,padding:"4px 24px 4px 8px",fontSize:11.5,fontFamily:"inherit",outline:"none",width:190,color:"var(--tx)"}}
                                    onFocus={e=>e.target.style.borderColor="var(--a)"}
                                    onBlur={e=>e.target.style.borderColor=mergedCCs[previewRow.tan]?.includes("@")?"var(--grn)":"var(--bd)"}
                                  />
                                  {mergedCCs[previewRow.tan]?.includes("@")&&<span style={{position:"absolute",right:7,top:"50%",transform:"translateY(-50%)",fontSize:11,color:"var(--grn)"}}>✓</span>}
                                </div>
                              </div>
                              <button
                                onClick={()=>{
                                  const em=mergedEmails[previewRow.tan];
                                  if(!em?.includes("@")){showToast("Enter recipient email address","w");return;}
                                  if(isGmailConnected){
                                    sendViaGmail([previewRow]);
                                  } else {
                                    const subj=encodeURIComponent((emailConfig.subject||"TDS Pending-FY "+selYear+"")+(emailConfig.refNo?" (Ref: "+emailConfig.refNo+")":""));
                                    const body=encodeURIComponent(generateEmailBody(previewRow));
                                    const ccPart = mergedCCs[previewRow.tan]?.trim() ? `&cc=${encodeURIComponent(mergedCCs[previewRow.tan].trim())}` : "";
                                    window.open(`mailto:${em}?subject=${subj}${ccPart}&body=${body}`,"_self");
                                    showToast("Opening email client…");
                                  }
                                }}
                                style={{background:isGmailConnected?"var(--grn)":"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"5px 13px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit",fontWeight:700}}
                              >{isGmailConnected?"📤 Send via Gmail":"✉️ Send"}</button>
                              <button onClick={()=>downloadAttachment(previewRow)} style={{background:"#217346",color:"#fff",border:"none",borderRadius:3,padding:"5px 10px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit",fontWeight:600}}>📎</button>
                              <button onClick={()=>navigator.clipboard.writeText(generateEmailBody(previewRow)).then(()=>showToast("Copied to clipboard"))} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"5px 10px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit"}}>📋</button>
                              <button onClick={()=>{const w=window.open("","_blank","width=700,height=800");w.document.write(generateEmailBody(previewRow).replace(/<\/body>/, "<script>window.print();<\/script></body>"));w.document.close();}} style={{background:"none",border:"1px solid var(--bd)",borderRadius:3,padding:"5px 10px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit"}}>🖨️</button>
                            </div>
                          </div>
                          <iframe
                            srcDoc={generateEmailBody(previewRow)}
                            style={{width:"100%",flex:1,border:"none",minHeight:540,display:"block"}}
                            sandbox="allow-same-origin"
                            title="Email Preview"
                          />
                        </div>
                      ):(
                        <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",height:"100%",gap:10,color:"var(--tx2)",textAlign:"center"}}>
                          <Ic d={I.mail} s={48} c="#d1d1d1" sw={1}/>
                          <div style={{fontSize:13}}>Click any deductor on the left to preview their notice</div>
                          <div style={{fontSize:11.5,color:"var(--tx3)"}}>Fill in your details above, then copy or print the notice</div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              );
            })()}

            {view==="tracker"&&(()=>{
              const fmtDate = iso => iso ? new Date(iso).toLocaleString("en-IN",{day:"2-digit",month:"short",year:"numeric",hour:"2-digit",minute:"2-digit"}) : "—";
              const fmtAmt = n => n ? `₹${Number(n).toLocaleString("en-IN",{minimumFractionDigits:2})}` : "—";
              const statusColor = s => s==="Sent"?"#0078d4":s==="Opened"?"#107c10":s==="Replied"?"#5c2d91":s==="Failed"?"#a80000":"#605e5c";
              const statusBg   = s => s==="Sent"?"#e6f3fb":s==="Opened"?"#e8f8e8":s==="Replied"?"#f0e8ff":s==="Failed"?"#fde8e8":"#f3f3f3";
              const filtered = emailLog.filter(r=>{
                const matchStatus = trackerFilter==="All" || r.status===trackerFilter;
                const matchSearch = !trackerSearch || [r.tan,r.name,r.to,r.subject,r.company].some(v=>String(v||"").toLowerCase().includes(trackerSearch.toLowerCase()));
                return matchStatus && matchSearch;
              });
              const stats = {
                total: emailLog.length,
                sent: emailLog.filter(r=>r.status==="Sent").length,
                opened: emailLog.filter(r=>r.status==="Opened").length,
                replied: emailLog.filter(r=>r.status==="Replied").length,
                failed: emailLog.filter(r=>r.status==="Failed").length,
              };

              // Check reply status via Gmail API (checks if thread has >1 message = someone replied)
              const checkReplyStatus = async (entry) => {
                if(!gmailToken||gmailToken.expires_at<=Date.now()){showToast("Gmail not connected","w");return;}
                if(!entry.threadId){showToast("No thread ID — cannot check","w");return;}
                try {
                  const res = await fetch(`https://www.googleapis.com/gmail/v1/users/me/threads/${entry.threadId}?fields=messages(id,labelIds)`,{
                    headers:{Authorization:"Bearer "+gmailToken.access_token}
                  });
                  if(!res.ok) {
                    const errData = await res.json().catch(()=>({}));
                    const msg = errData?.error?.message || res.statusText;
                    if(res.status===403) throw new Error("Permission denied — reconnect Gmail to grant read access");
                    throw new Error("API error "+res.status+": "+msg);
                  }
                  const data = await res.json();
                  const msgs = data.messages||[];
                  const now = new Date().toISOString();
                  let newStatus = entry.status;
                  if(msgs.length>1) newStatus="Replied";
                  // Check if original message was read (no UNREAD label on thread)
                  const firstMsg = msgs[0];
                  const isRead = firstMsg && !firstMsg.labelIds?.includes("UNREAD");
                  if(isRead && newStatus==="Sent") newStatus="Opened";
                  setEmailLog(prev=>prev.map(r=>r.id===entry.id?{...r,status:newStatus,lastChecked:now,repliedAt:newStatus==="Replied"&&!r.repliedAt?now:r.repliedAt,openedAt:newStatus!=="Sent"&&newStatus!=="Failed"&&!r.openedAt?now:r.openedAt}:r));
                  showToast(`Status: ${newStatus}`,"s");
                } catch(e){showToast("Check failed: "+e.message,"e");}
              };

              const checkAllPending = async () => {
                if(!gmailToken||gmailToken.expires_at<=Date.now()){showToast("Gmail not connected","w");return;}
                const pending = emailLog.filter(r=>r.status==="Sent"&&r.threadId);
                if(!pending.length){showToast("No pending emails to check","i");return;}
                setCheckingStatus(true);
                let updated=0;
                for(const entry of pending){
                  try {
                    const res = await fetch(`https://www.googleapis.com/gmail/v1/users/me/threads/${entry.threadId}?fields=messages(id,labelIds)`,{
                      headers:{Authorization:"Bearer "+gmailToken.access_token}
                    });
                    if(!res.ok) {
                      if(res.status===403){ setCheckingStatus(false); showToast("Permission denied — disconnect & reconnect Gmail to grant read access","e"); return; }
                      continue;
                    }
                    const data = await res.json();
                    const msgs = data.messages||[];
                    const now = new Date().toISOString();
                    let newStatus = entry.status;
                    if(msgs.length>1) newStatus="Replied";
                    else if(msgs[0]&&!msgs[0].labelIds?.includes("UNREAD")) newStatus="Opened";
                    if(newStatus!==entry.status){
                      setEmailLog(prev=>prev.map(r=>r.id===entry.id?{...r,status:newStatus,lastChecked:now,repliedAt:newStatus==="Replied"&&!r.repliedAt?now:r.repliedAt,openedAt:newStatus!=="Sent"&&!r.openedAt?now:r.openedAt}:r));
                      updated++;
                    }
                  } catch(e){}
                  await new Promise(r=>setTimeout(r,200));
                }
                setCheckingStatus(false);
                showToast(`✅ Checked ${pending.length} emails — ${updated} status update(s)`,"s");
              };

              return (
              <div style={{flex:1,overflow:"auto",padding:"0 0 20px"}}>
                {/* Stats Bar */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"14px 24px",display:"flex",gap:0,flexShrink:0}}>
                  {[
                    {l:"Total Sent",v:stats.total,c:"#0078d4",bg:"#e6f3fb"},
                    {l:"Sent (Unread)",v:stats.sent,c:"#605e5c",bg:"#f3f3f3"},
                    {l:"Opened",v:stats.opened,c:"#107c10",bg:"#e8f8e8"},
                    {l:"Replied",v:stats.replied,c:"#5c2d91",bg:"#f0e8ff"},
                    {l:"Failed",v:stats.failed,c:"#a80000",bg:"#fde8e8"},
                  ].map((s,i)=>(
                    <div key={i} onClick={()=>setTrackerFilter(s.l==="Total Sent"?"All":s.l==="Sent (Unread)"?"Sent":s.l)} style={{flex:1,padding:"10px 18px",cursor:"pointer",borderRight:i<4?"1px solid var(--bd)":"none",borderBottom:`3px solid ${trackerFilter===(s.l==="Total Sent"?"All":s.l==="Sent (Unread)"?"Sent":s.l)?s.c:"transparent"}`,transition:"border-color 0.15s"}}>
                      <div style={{fontSize:10.5,color:"var(--tx2)",textTransform:"uppercase",letterSpacing:"0.5px",marginBottom:3}}>{s.l}</div>
                      <div style={{fontSize:26,fontWeight:300,color:s.c}}>{s.v}</div>
                    </div>
                  ))}
                  <div style={{display:"flex",alignItems:"center",gap:8,padding:"0 16px",marginLeft:"auto"}}>
                    <button onClick={checkAllPending} disabled={checkingStatus||!isGmailConnected} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"7px 14px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,opacity:checkingStatus||!isGmailConnected?0.5:1,display:"flex",alignItems:"center",gap:6}}>
                      <Ic d={I.refresh} s={13} c="#fff"/>{checkingStatus?"Checking…":"Refresh All Status"}
                    </button>
                    <button onClick={()=>{if(window.confirm(`Clear all ${emailLog.length} log entries?`))setEmailLog([]);}} disabled={!emailLog.length} style={{background:"none",border:"1px solid var(--red)",borderRadius:3,padding:"7px 12px",cursor:"pointer",fontSize:12,fontFamily:"inherit",color:"var(--red)",opacity:emailLog.length?1:0.4}}>
                      <Ic d={I.trash} s={13} c="var(--red)"/> Clear Log
                    </button>
                  </div>
                </div>

                {/* Filter + Search bar */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"8px 24px",display:"flex",alignItems:"center",gap:10}}>
                  {["All","Sent","Opened","Replied","Failed"].map(f=>(
                    <button key={f} onClick={()=>setTrackerFilter(f)} style={{background:trackerFilter===f?"var(--a)":"none",color:trackerFilter===f?"#fff":"var(--tx2)",border:`1px solid ${trackerFilter===f?"var(--a)":"var(--bd)"}`,borderRadius:3,padding:"3px 12px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit"}}>{f}</button>
                  ))}
                  <div style={{marginLeft:"auto",display:"flex",alignItems:"center",gap:8}}>
                    <div style={{display:"flex",alignItems:"center",gap:6,background:"var(--sur)",borderRadius:3,padding:"4px 10px",border:"1px solid var(--bd)"}}>
                      <Ic d={I.search} s={12} c="#888"/>
                      <input placeholder="Search TAN, name, email…" value={trackerSearch} onChange={e=>setTrackerSearch(e.target.value)} style={{border:"none",background:"none",outline:"none",fontSize:12,fontFamily:"inherit",width:200,color:"var(--tx)"}}/>
                      {trackerSearch&&<span onClick={()=>setTrackerSearch("")} style={{cursor:"pointer",color:"#aaa",fontSize:11}}>✕</span>}
                    </div>
                    <span style={{fontSize:11.5,color:"var(--tx2)"}}>{filtered.length} of {emailLog.length}</span>
                  </div>
                </div>

                {/* Gmail connection notice */}
                {!isGmailConnected&&(
                  <div style={{margin:"12px 24px",padding:"10px 14px",background:"#fff8e1",border:"1px solid #ffe082",borderRadius:4,fontSize:12.5,color:"#795548",display:"flex",alignItems:"center",gap:8}}>
                    <Ic d={I.warn} s={15} c="#f59e0b"/>
                    Gmail not connected — connect Gmail to auto-refresh reply/open status.
                    <button onClick={connectGmail} style={{marginLeft:"auto",background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"4px 12px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit"}}>Connect</button>
                  </div>
                )}

                {/* Table */}
                {filtered.length===0?(
                  <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",height:300,gap:12,color:"var(--tx2)"}}>
                    <Ic d={I.tracker} s={48} c="#d1d1d1" sw={1}/>
                    <div style={{fontSize:13}}>No email records yet</div>
                    <div style={{fontSize:11.5,color:"var(--tx3)"}}>Emails sent from TDS Notice will appear here</div>
                  </div>
                ):(
                  <div style={{overflowX:"auto",margin:"16px 24px 0"}}>
                    <table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5,background:"var(--wh)",borderRadius:5,overflow:"hidden",border:"1px solid var(--bd)"}}>
                      <thead>
                        <tr style={{background:"var(--hb)"}}>
                          {["#","TAN","Deductor","To (Email)","Subject","Pending Amt","Sent At","Status","Last Checked","Actions"].map(h=>(
                            <th key={h} style={{padding:"8px 12px",textAlign:"left",fontWeight:600,fontSize:11,color:"var(--tx2)",borderBottom:"1px solid var(--bd)",whiteSpace:"nowrap"}}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {filtered.map((entry,i)=>(
                          <tr key={entry.id} style={{borderBottom:"1px solid #f0f0f0",background:i%2===0?"var(--wh)":"#fafafa"}}>
                            <td style={{padding:"9px 12px",color:"#aaa",fontSize:11}}>{i+1}</td>
                            <td style={{padding:"9px 12px",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--a)",fontWeight:600}}>{entry.tan||"—"}</td>
                            <td style={{padding:"9px 12px",fontWeight:500,maxWidth:170,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={entry.name}>{entry.name||"—"}</td>
                            <td style={{padding:"9px 12px",color:"var(--tx2)",maxWidth:160,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={entry.to}>{entry.to||"—"}</td>
                            <td style={{padding:"9px 12px",color:"var(--tx2)",maxWidth:180,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={entry.subject}>{entry.subject||"—"}</td>
                            <td style={{padding:"9px 12px",fontFamily:"Consolas,monospace",color:"#a80000",fontWeight:600,whiteSpace:"nowrap"}}>{fmtAmt(entry.pendingAmt)}</td>
                            <td style={{padding:"9px 12px",color:"var(--tx2)",whiteSpace:"nowrap",fontSize:11}}>{fmtDate(entry.sentAt)}</td>
                            <td style={{padding:"9px 12px"}}>
                              <div style={{display:"flex",flexDirection:"column",gap:2}}>
                                <span style={{background:statusBg(entry.status),color:statusColor(entry.status),fontWeight:700,padding:"2px 9px",borderRadius:4,fontSize:11,display:"inline-block",whiteSpace:"nowrap"}}>{entry.status}</span>
                                {entry.repliedAt&&<span style={{fontSize:10,color:"var(--tx3)"}}>Replied: {fmtDate(entry.repliedAt)}</span>}
                                {entry.openedAt&&!entry.repliedAt&&<span style={{fontSize:10,color:"var(--tx3)"}}>Opened: {fmtDate(entry.openedAt)}</span>}
                                {entry.failReason&&<span style={{fontSize:10,color:"var(--red)"}}>{entry.failReason}</span>}
                              </div>
                            </td>
                            <td style={{padding:"9px 12px",fontSize:11,color:"var(--tx3)",whiteSpace:"nowrap"}}>{entry.lastChecked?fmtDate(entry.lastChecked):"Never"}</td>
                            <td style={{padding:"9px 12px"}}>
                              <div style={{display:"flex",gap:5}}>
                                {entry.threadId&&isGmailConnected&&(
                                  <button onClick={()=>checkReplyStatus(entry)} title="Check reply/open status" style={{background:"var(--a-lt)",color:"var(--a)",border:"1px solid var(--a)",borderRadius:3,padding:"3px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600,whiteSpace:"nowrap"}}>↻ Check</button>
                                )}
                                {entry.threadId&&(
                                  <button onClick={()=>window.open(`https://mail.google.com/mail/u/0/#sent/${entry.messageId}`,"_blank")} title="Open in Gmail" style={{background:"none",color:"var(--tx2)",border:"1px solid var(--bd)",borderRadius:3,padding:"3px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",whiteSpace:"nowrap"}}>📧 View</button>
                                )}
                                <button onClick={()=>{if(window.confirm("Remove this entry?"))setEmailLog(prev=>prev.filter(r=>r.id!==entry.id));}} title="Delete entry" style={{background:"none",color:"var(--red)",border:"1px solid var(--red)",borderRadius:3,padding:"3px 6px",cursor:"pointer",fontSize:11}}>✕</button>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
              );
            })()}

            {view==="reports"&&reconDone&&(
              <div className="rep">
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:16}}>
                  <div style={{fontSize:15,fontWeight:600}}>Mismatch Report — {curCompany?.name} · FY {selYear}</div>
                  <button style={{background:"var(--a)",color:"#fff",border:"none",padding:"7px 18px",borderRadius:3,cursor:"pointer",fontSize:12.5,fontFamily:"inherit"}} onClick={exportReconReport}>Export Excel</button>
                </div>
                <div className="rep-sec">
                  <div className="rep-sh"><Ic d={I.chart} s={13} c="var(--a)"/>Summary</div>
                  <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)"}}>
                    {[{l:"Total",v:rs.total,c:"var(--tx)"},{l:"Matched",v:rs.matched,c:"var(--grn)"},{l:"Mismatches",v:rs.mismatch,c:"var(--red)"},{l:"Missing in Books",v:rs.mib,c:"var(--pur)"},{l:"TDS Difference",v:(rs.tdsDiff>0?"+":rs.tdsDiff<0?"-":"")+"₹"+Math.abs(rs.tdsDiff).toLocaleString("en-IN",{maximumFractionDigits:0}),c:Math.abs(rs.tdsDiff)>1?"var(--red)":"var(--grn)"}].map((s,i)=>(
                      <div key={i} style={{padding:"13px 18px",borderRight:i<4?"1px solid var(--bd)":"none"}}>
                        <div style={{fontSize:10.5,color:"var(--tx2)",textTransform:"uppercase",letterSpacing:"0.4px",marginBottom:3}}>{s.l}</div>
                        <div style={{fontSize:24,fontWeight:300,color:s.c}}>{s.v}</div>
                      </div>
                    ))}
                  </div>
                </div>
                <div className="rep-sec">
                  <div className="rep-sh"><Ic d={I.warn} s={13} c="var(--red)"/>All Mismatches & Missing Entries ({rs.mismatch+rs.mib} issues)</div>
                  <table className="rep-t">
                    <thead><tr><th>#</th><th>Deductor (26AS)</th><th>TAN</th><th style={{textAlign:"right"}}>26AS TDS</th><th style={{textAlign:"right"}}>Books TDS</th><th style={{textAlign:"right"}}>TDS Diff</th><th>Status</th><th>Reason</th></tr></thead>
                    <tbody>
                      {reconResults.filter(r=>r.matchStatus!=="Matched"&&r.matchStatus!=="Near Match").map((r,i)=>(
                        <tr key={r.id}>
                          <td style={{color:"#aaa"}}>{i+1}</td>
                          <td style={{fontWeight:500}}>{r.as_name||r.bk_name||"—"}</td>
                          <td style={{fontFamily:"Consolas,monospace",fontSize:11,color:"var(--a)"}}>{r.tan||"—"}</td>
                          <td style={{textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11,color:"#a80000"}}>{fmt(r.as_tds)}</td>
                          <td style={{textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--grn)"}}>{fmt(r.bk_tds)||"—"}</td>
                          <td style={{textAlign:"right"}}><FmtDiff n={r.tds_diff}/></td>
                          <td><span className={`tg ${getTag(r.matchStatus)}`}>{r.matchStatus}</span></td>
                          <td style={{fontSize:11,color:"var(--tx2)"}}>{r.mismatchReason||"—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>

          {/* ODOO PUSH LOG VIEW */}
          {view==="odoolog"&&(()=>{
            const fmtDT = iso => iso ? new Date(iso).toLocaleString("en-IN",{day:"2-digit",month:"short",year:"numeric",hour:"2-digit",minute:"2-digit"}) : "—";
            const fmtD  = iso => iso ? new Date(iso).toLocaleDateString("en-IN",{day:"2-digit",month:"short",year:"numeric"}) : "—";
            const fmtAmt = n => "₹" + Number(n||0).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});

            // Build period options from log data
            const periodOptions = ["All"];
            const seen = new Set();
            [...odooLog].sort((a,b)=>b.pushDate.localeCompare(a.pushDate)).forEach(p=>{
              const d = new Date(p.pushDate);
              const key = d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0");
              const label = d.toLocaleDateString("en-IN",{month:"long",year:"numeric"});
              if(!seen.has(key)){seen.add(key);periodOptions.push({key,label});}
            });

            const filtered = odooLog.filter(push => {
              const d = new Date(push.pushDate);
              const key = d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0");
              const matchPeriod = odooLogPeriod==="All" || odooLogPeriod===key;
              const matchStatus = odooLogFilter==="All" || (odooLogFilter==="HasFailed"?push.totalFailed>0:push.totalFailed===0);
              const matchSearch = !odooLogSearch||[push.tan,push.deductorName,push.company].some(v=>String(v||"").toLowerCase().includes(odooLogSearch.toLowerCase()));
              return matchPeriod && matchStatus && matchSearch;
            });

            const totalEntries = filtered.reduce((s,p)=>s+p.totalCreated,0);
            const totalFailed  = filtered.reduce((s,p)=>s+p.totalFailed,0);
            const totalAmt     = filtered.reduce((s,p)=>s+p.entries.filter(e=>e.status!=="Failed").reduce((ss,e)=>ss+(e.amount||0),0),0);

            const downloadCSV = (pushes, label) => {
              const rows = [["Push Date","TAN","Deductor","Company","Invoice No","Odoo TDS Entry","Move ID","TDS Amount","Status","Error"].join(",")];
              pushes.forEach(push => push.entries.forEach(e => {
                rows.push([
                  fmtD(push.pushDate),
                  '"'+push.tan+'"',
                  '"'+(push.deductorName||"").replace(/"/g,"'")+'"',
                  '"'+(push.company||"")+'"',
                  '"'+(e.invoiceNo||"")+'"',
                  '"'+(e.odooRef||"")+'"',
                  e.moveId||"",
                  (e.amount||0).toFixed(2),
                  e.status||"",
                  '"'+(e.error||"").replace(/"/g,"'")+'"'
                ].join(","));
              }));
              const a = document.createElement("a");
              a.href = URL.createObjectURL(new Blob([rows.join("\n")],{type:"text/csv"}));
              a.download = "OdooTDS_PushLog_"+label+".csv"; a.click();
            };

            const byDate = {};
            filtered.forEach(p => { const d=(p.pushDate||"").slice(0,10); (byDate[d]=byDate[d]||[]).push(p); });
            const sortedDates = Object.keys(byDate).sort((a,b)=>b.localeCompare(a));

            return (
              <div style={{flex:"1 1 0%",overflow:"hidden",display:"flex",flexDirection:"column",height:"100%"}}>

                {/* Compact stats + actions bar (single row) */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"7px 16px",display:"flex",alignItems:"center",gap:0,flexShrink:0}}>
                  {[
                    {l:"Pushes",   v:filtered.length, c:"#5c2d91"},
                    {l:"Created",  v:totalEntries,    c:"#107c10"},
                    {l:"Failed",   v:totalFailed,     c:"#a80000"},
                    {l:"TDS Pushed",v:fmtAmt(totalAmt),c:"#0078d4",wide:true},
                  ].map((s,i)=>(
                    <div key={i} style={{padding:"4px 16px",borderRight:i<3?"1px solid var(--bd)":"none",display:"flex",alignItems:"center",gap:6}}>
                      <span style={{fontSize:10,color:"var(--tx2)",textTransform:"uppercase",letterSpacing:"0.4px",whiteSpace:"nowrap"}}>{s.l}</span>
                      <span style={{fontSize:s.wide?13:16,fontWeight:s.wide?600:700,color:s.c,fontFamily:s.wide?"Consolas,monospace":"inherit"}}>{s.v}</span>
                    </div>
                  ))}
                  <div style={{display:"flex",alignItems:"center",gap:6,marginLeft:"auto"}}>
                    <button
                      onClick={()=>downloadCSV(filtered, (odooLogPeriod==="All"?"All":odooLogPeriod)+"_"+new Date().toISOString().slice(0,10))}
                      disabled={!filtered.length}
                      style={{background:"#107c10",color:"#fff",border:"none",borderRadius:3,padding:"5px 12px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit",fontWeight:600,opacity:filtered.length?1:0.4}}>
                      ⬇ CSV ({filtered.length})
                    </button>
                    <button
                      onClick={()=>{if(window.confirm("Clear all "+odooLog.length+" log entries? This cannot be undone."))setOdooLog([]);}}
                      disabled={!odooLog.length}
                      style={{background:"none",border:"1px solid #a80000",borderRadius:3,padding:"5px 10px",cursor:"pointer",fontSize:11.5,fontFamily:"inherit",color:"#a80000",opacity:odooLog.length?1:0.4}}>
                      Clear
                    </button>
                  </div>
                </div>

                {/* Filter + Period + Search bar */}
                <div style={{background:"var(--wh)",borderBottom:"1px solid var(--bd)",padding:"5px 16px",display:"flex",alignItems:"center",gap:8,flexWrap:"wrap",flexShrink:0}}>
                  <span style={{fontSize:11,fontWeight:600,color:"var(--tx2)"}}>Period</span>
                  <select value={odooLogPeriod} onChange={e=>setOdooLogPeriod(e.target.value)}
                    style={{padding:"3px 8px",border:"1px solid var(--bd)",borderRadius:3,fontSize:11.5,fontFamily:"inherit",background:"var(--wh)",color:"var(--tx)",cursor:"pointer",outline:"none"}}>
                    {periodOptions.map(p=>
                      typeof p==="string"
                        ? <option key="All" value="All">All Months</option>
                        : <option key={p.key} value={p.key}>{p.label}</option>
                    )}
                  </select>
                  <div style={{width:1,height:18,background:"var(--bd)"}}/>
                  {[["All","All"],["AllPosted","No Failures"],["HasFailed","Has Failures"]].map(([k,l])=>(
                    <button key={k} onClick={()=>setOdooLogFilter(k)}
                      style={{background:odooLogFilter===k?"var(--a)":"none",color:odooLogFilter===k?"#fff":"var(--tx2)",
                        border:"1px solid "+(odooLogFilter===k?"var(--a)":"var(--bd)"),borderRadius:3,
                        padding:"3px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit"}}>
                      {l}
                    </button>
                  ))}
                  <div style={{marginLeft:"auto",display:"flex",alignItems:"center",gap:6}}>
                    <div style={{display:"flex",alignItems:"center",gap:5,border:"1px solid var(--bd)",borderRadius:3,padding:"3px 8px",background:"var(--sur)"}}>
                      <Ic d={I.search} s={11} c="#aaa"/>
                      <input placeholder="Search TAN, deductor, company..." value={odooLogSearch}
                        onChange={e=>setOdooLogSearch(e.target.value)}
                        style={{border:"none",outline:"none",background:"transparent",fontSize:11.5,fontFamily:"inherit",width:200,color:"var(--tx)"}}/>
                      {odooLogSearch&&<span onClick={()=>setOdooLogSearch("")} style={{cursor:"pointer",color:"#aaa",fontSize:11}}>✕</span>}
                    </div>
                    <span style={{fontSize:11,color:"var(--tx2)",whiteSpace:"nowrap"}}>{filtered.length} pushes</span>
                  </div>
                </div>

                {/* Table — fills remaining space */}
                <div style={{flex:"1 1 0%",overflow:"auto",minHeight:0}}>
                  {!odooLog.length ? (
                    <div style={{margin:"80px auto",textAlign:"center",color:"var(--tx2)"}}>
                      <div style={{fontSize:40,marginBottom:12}}>📤</div>
                      <div style={{fontSize:14,fontWeight:600,marginBottom:6,color:"var(--tx)"}}>No push log entries yet</div>
                      <div style={{fontSize:12}}>Push journal entries to Odoo from the Reconciliation tab to see logs here.</div>
                    </div>
                  ) : filtered.length===0 ? (
                    <div style={{margin:"80px auto",textAlign:"center",color:"var(--tx2)"}}>
                      <div style={{fontSize:32,marginBottom:10}}>🔍</div>
                      <div style={{fontSize:13,fontWeight:600,marginBottom:4,color:"var(--tx)"}}>No results</div>
                      <div style={{fontSize:11.5}}>Try changing the period or filters.</div>
                    </div>
                  ) : (
                    <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                      <thead>
                        <tr style={{background:"var(--hb)",position:"sticky",top:0,zIndex:2}}>
                          {["Push Date","TAN","Deductor","Company","Invoice No","Odoo TDS Entry","Move ID","TDS Amount","Status"].map(h=>(
                            <th key={h} style={{padding:"8px 12px",textAlign:h==="TDS Amount"?"right":"left",fontWeight:600,fontSize:11,color:"var(--tx2)",borderBottom:"2px solid var(--bd)",whiteSpace:"nowrap",userSelect:"none",letterSpacing:"0.3px"}}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortedDates.map(date=>
                          byDate[date].map(push=>
                            push.entries.map((e,i)=>{
                              const isFirst = i===0;
                              const rowspan = push.entries.length;
                              return (
                                <tr key={push.id+"-"+i} style={{borderBottom:"1px solid #f0f0f0",background:e.status==="Failed"?"#fff5f5":i%2===0?"var(--wh)":"var(--sur)"}}>
                                  {isFirst&&<td rowSpan={rowspan} style={{padding:"7px 12px",verticalAlign:"top",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--a)",whiteSpace:"nowrap",fontWeight:600,borderRight:"1px solid var(--bd)",background:"var(--sur)"}}>{fmtD(date)}</td>}
                                  {isFirst&&<td rowSpan={rowspan} style={{padding:"7px 12px",verticalAlign:"top",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--a)",whiteSpace:"nowrap",borderRight:"1px solid var(--bd)",background:"var(--sur)"}}>{push.tan}</td>}
                                  {isFirst&&<td rowSpan={rowspan} style={{padding:"7px 12px",verticalAlign:"top",fontWeight:600,fontSize:12,color:"var(--tx)",borderRight:"1px solid var(--bd)",background:"var(--sur)",minWidth:140}}>{push.deductorName||"—"}</td>}
                                  {isFirst&&<td rowSpan={rowspan} style={{padding:"7px 12px",verticalAlign:"top",fontSize:11,color:"var(--tx2)",borderRight:"1px solid var(--bd)",background:"var(--sur)",whiteSpace:"nowrap"}}>{push.company||"—"}</td>}
                                  <td style={{padding:"7px 12px",fontFamily:"Consolas,monospace",fontWeight:600,color:"var(--a)",whiteSpace:"nowrap"}}>{e.invoiceNo||"—"}</td>
                                  <td style={{padding:"7px 12px",fontFamily:"Consolas,monospace",color:"#5c2d91",fontWeight:600,whiteSpace:"nowrap"}}>{e.odooRef||"—"}</td>
                                  <td style={{padding:"7px 12px",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--tx2)",whiteSpace:"nowrap"}}>{e.moveId||"—"}</td>
                                  <td style={{padding:"7px 12px",textAlign:"right",fontFamily:"Consolas,monospace",fontWeight:600,color:"#0078d4",whiteSpace:"nowrap"}}>{fmtAmt(e.amount)}</td>
                                  <td style={{padding:"7px 12px"}}>
                                    <span style={{
                                      background:e.status==="Posted"?"#e8f8e8":e.status==="Draft"?"#fff8dc":"#fde8e8",
                                      color:e.status==="Posted"?"#107c10":e.status==="Draft"?"#7a6000":"#a80000",
                                      padding:"2px 8px",borderRadius:10,fontSize:10,fontWeight:700,whiteSpace:"nowrap"
                                    }}>
                                      {e.status}{e.error?" — "+e.error.slice(0,40):""}
                                    </span>
                                  </td>
                                </tr>
                              );
                            })
                          )
                        )}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            );
          })()}

          <div className="stb">
            <div className="sti"><Ic d={I.check} s={10} c="rgba(255,255,255,0.75)"/>Ready</div>
            <div className="sti" style={{background:"rgba(255,255,255,0.1)",padding:"0 7px",borderRadius:3}}>{curCompany?.name} · FY {selYear}</div>
            <div className="sti">26AS: {datasets["26AS"].length} · Books: {datasets["Books"].length}</div>
            {reconDone&&<div className="sti">Matched: {rs.matched} · Mismatches: {rs.mismatch}</div>}
            <div className="sti" style={{marginLeft:"auto"}}>
              {storageStatus==="saved"&&<span style={{opacity:0.8}}>💾 Saved</span>}
              {storageStatus==="saving"&&<span style={{opacity:0.7}}>⏳ Saving…</span>}
              &nbsp;·&nbsp;{new Date().toLocaleDateString("en-IN",{day:"2-digit",month:"short",year:"numeric"})}
            </div>
          </div>
        </div>

        {/* ── ADD / EDIT CLIENT MODAL ── */}
        {showAddClientModal&&(
          <div className="modal-bg" onClick={e=>e.target===e.currentTarget&&setShowAddClientModal(false)}>
            <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:8,padding:28,width:520,boxShadow:"0 8px 40px rgba(0,0,0,0.18)",maxHeight:"90vh",overflow:"auto"}}>
              <div style={{fontSize:16,fontWeight:700,marginBottom:18}}>{editClientId?"Edit Client":"Add New Client"}</div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
                {[
                  {label:"Client / Company Name *",key:"name",placeholder:"e.g. Tata Consultancy Services",span:2},
                  {label:"PAN",key:"pan",placeholder:"ABCDE1234F"},
                  {label:"GSTIN",key:"gstin",placeholder:"27ABCDE1234F1Z5"},
                  {label:"Contact Person",key:"contactPerson",placeholder:"e.g. Ramesh Kumar"},
                  {label:"Phone",key:"phone",placeholder:"+91 9876543210"},
                  {label:"Email",key:"email",placeholder:"accounts@company.com"},
                  {label:"Group / Category",key:"group",placeholder:"e.g. Manufacturing, IT, Banking"},
                ].map(f=>(
                  <div key={f.key} style={{gridColumn:f.span===2?"1/-1":"auto"}}>
                    <div style={{fontSize:11,fontWeight:600,color:"var(--tx2)",marginBottom:4,textTransform:"uppercase",letterSpacing:0.4}}>{f.label}</div>
                    <input value={clientDraft[f.key]||""} onChange={e=>setClientDraft(p=>({...p,[f.key]:e.target.value}))} placeholder={f.placeholder}
                      style={{width:"100%",padding:"7px 10px",border:"1px solid var(--bd)",borderRadius:4,fontSize:12.5,fontFamily:"inherit",outline:"none",boxSizing:"border-box"}}
                      onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                  </div>
                ))}
                <div>
                  <div style={{fontSize:11,fontWeight:600,color:"var(--tx2)",marginBottom:4,textTransform:"uppercase",letterSpacing:0.4}}>Client Type</div>
                  <select value={clientDraft.clientType||"Corporate"} onChange={e=>setClientDraft(p=>({...p,clientType:e.target.value}))} style={{width:"100%",padding:"7px 10px",border:"1px solid var(--bd)",borderRadius:4,fontSize:12.5,fontFamily:"inherit",outline:"none",background:"var(--wh)"}}>
                    {["Corporate","Individual","Partnership","Trust","LLP","HUF"].map(t=><option key={t}>{t}</option>)}
                  </select>
                </div>
                <div style={{gridColumn:"1/-1"}}>
                  <div style={{fontSize:11,fontWeight:600,color:"var(--tx2)",marginBottom:4,textTransform:"uppercase",letterSpacing:0.4}}>Notes</div>
                  <textarea value={clientDraft.notes||""} onChange={e=>setClientDraft(p=>({...p,notes:e.target.value}))} placeholder="Any additional notes…" rows={2}
                    style={{width:"100%",padding:"7px 10px",border:"1px solid var(--bd)",borderRadius:4,fontSize:12,fontFamily:"inherit",outline:"none",resize:"vertical",boxSizing:"border-box"}}/>
                </div>
              </div>
              
              {/* ── PORTAL CREDENTIALS SECTION ── */}
              <div style={{marginTop:20,border:"2px solid #e3f2fd",borderRadius:8,overflow:"hidden"}}>
                {/* Header */}
                <div style={{background:"linear-gradient(135deg, #0078d4 0%, #005a9e 100%)",padding:"12px 16px",display:"flex",alignItems:"center",gap:8}}>
                  <span style={{fontSize:18}}>🔐</span>
                  <div style={{fontSize:13.5,fontWeight:700,color:"#fff"}}>Portal Login Credentials</div>
                  <span style={{fontSize:10,background:"rgba(255,255,255,0.25)",color:"#fff",borderRadius:3,padding:"2px 7px",marginLeft:"auto"}}>Stored Encrypted</span>
                </div>
                
                {/* TRACES Portal Credentials */}
                <div style={{padding:"16px",background:"#f0f7ff",borderBottom:"1px solid #c5dffa"}}>
                  <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:12}}>
                    <span style={{fontSize:14}}>🏛️</span>
                    <div style={{fontSize:12.5,fontWeight:700,color:"#005a9e"}}>TRACES Portal</div>
                    <span style={{fontSize:9,background:"#005a9e",color:"#fff",borderRadius:2,padding:"2px 5px"}}>TDS Filing</span>
                  </div>
                  <div style={{fontSize:10.5,color:"#1a5276",marginBottom:10,lineHeight:1.5}}>
                    For downloading 26AS from TRACES. Auto-fills when you click "Open TRACES Portal".
                  </div>
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
                    {[
                      {label:"Taxpayer PAN",key:"tracesTaxpayerPAN",placeholder:"e.g. ABCDE1234F",type:"text"},
                      {label:"Taxpayer Password",key:"tracesTaxpayerPass",placeholder:"••••••••",type:"password"},
                      {label:"Deductor TAN",key:"tracesDeductorTAN",placeholder:"e.g. ABCD12345E",type:"text"},
                      {label:"Deductor Password",key:"tracesDeductorPass",placeholder:"••••••••",type:"password"},
                    ].map(f=>(
                      <div key={f.key}>
                        <div style={{fontSize:10,fontWeight:600,color:"#1a5276",marginBottom:4,textTransform:"uppercase",letterSpacing:0.3}}>{f.label}</div>
                        <input type={f.type} value={clientDraft[f.key]||""} onChange={e=>setClientDraft(p=>({...p,[f.key]:e.target.value}))} placeholder={f.placeholder}
                          style={{width:"100%",padding:"6px 9px",border:"1px solid #c5dffa",borderRadius:4,fontSize:11.5,fontFamily:"Consolas,monospace",outline:"none",boxSizing:"border-box",background:"#fff"}}
                          onFocus={e=>e.target.style.borderColor="#005a9e"} onBlur={e=>e.target.style.borderColor="#c5dffa"}/>
                      </div>
                    ))}
                  </div>
                </div>
                
                {/* IT Portal Credentials */}
                <div style={{padding:"16px",background:"#f0fdf4",borderBottom:"1px solid #bbf7d0"}}>
                  <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:12}}>
                    <span style={{fontSize:14}}>🇮🇳</span>
                    <div style={{fontSize:12.5,fontWeight:700,color:"#15803d"}}>Income Tax Portal</div>
                    <span style={{fontSize:9,background:"#15803d",color:"#fff",borderRadius:2,padding:"2px 5px"}}>26AS Download</span>
                  </div>
                  <div style={{fontSize:10.5,color:"#166534",marginBottom:10,lineHeight:1.5}}>
                    For downloading 26AS from incometax.gov.in. OTP required on login.
                  </div>
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:10}}>
                    {[
                      {label:"PAN",key:"itPortalPAN",placeholder:"ABCDE1234F",type:"text"},
                      {label:"Password",key:"itPortalPass",placeholder:"••••••••",type:"password"},
                      {label:"Date of Birth",key:"itPortalDOB",placeholder:"DD/MM/YYYY",type:"text"},
                    ].map(f=>(
                      <div key={f.key}>
                        <div style={{fontSize:10,fontWeight:600,color:"#166534",marginBottom:4,textTransform:"uppercase",letterSpacing:0.3}}>{f.label}</div>
                        <input type={f.type} value={clientDraft[f.key]||""} onChange={e=>setClientDraft(p=>({...p,[f.key]:e.target.value}))} placeholder={f.placeholder}
                          style={{width:"100%",padding:"6px 9px",border:"1px solid #bbf7d0",borderRadius:4,fontSize:11.5,fontFamily:f.key==="itPortalDOB"?"inherit":"Consolas,monospace",outline:"none",boxSizing:"border-box",background:"#fff"}}
                          onFocus={e=>e.target.style.borderColor="#15803d"} onBlur={e=>e.target.style.borderColor="#bbf7d0"}/>
                      </div>
                    ))}
                  </div>
                </div>
                
                {/* ZIP Password */}
                <div style={{padding:"16px",background:"#fefce8"}}>
                  <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:12}}>
                    <span style={{fontSize:14}}>🔓</span>
                    <div style={{fontSize:12.5,fontWeight:700,color:"#854d0e"}}>ZIP File Password</div>
                    <span style={{fontSize:9,background:"#854d0e",color:"#fff",borderRadius:2,padding:"2px 5px"}}>For Encrypted ZIPs</span>
                  </div>
                  <div style={{fontSize:10.5,color:"#713f12",marginBottom:10,lineHeight:1.5}}>
                    TRACES 26AS ZIPs are password-protected with Date of Birth (ddmmyyyy) or Date of Incorporation.
                  </div>
                  <div style={{maxWidth:"280px"}}>
                    <div style={{fontSize:10,fontWeight:600,color:"#854d0e",marginBottom:4,textTransform:"uppercase",letterSpacing:0.3}}>ZIP Password (ddmmyyyy)</div>
                    <input type="text" value={clientDraft.zipPassword||""} onChange={e=>setClientDraft(p=>({...p,zipPassword:e.target.value}))} placeholder="e.g. 15031990"
                      style={{width:"100%",padding:"6px 9px",border:"1px solid #fde047",borderRadius:4,fontSize:11.5,fontFamily:"Consolas,monospace",outline:"none",boxSizing:"border-box",background:"#fff"}}
                      onFocus={e=>e.target.style.borderColor="#854d0e"} onBlur={e=>e.target.style.borderColor="#fde047"}/>
                    <div style={{fontSize:9.5,color:"#a16207",marginTop:5}}>💡 Format: ddmmyyyy (e.g., 15031990 for 15-Mar-1990)</div>
                  </div>
                </div>
                
                {/* Odoo ERP Integration */}
                <div style={{padding:"16px",background:"#fef3c7",borderBottom:"1px solid #fde047"}}>
                  <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:12}}>
                    <span style={{fontSize:14}}>📊</span>
                    <div style={{fontSize:12.5,fontWeight:700,color:"#92400e"}}>Odoo ERP Integration</div>
                    <span style={{fontSize:9,background:"#92400e",color:"#fff",borderRadius:2,padding:"2px 5px"}}>Auto Sync Books</span>
                  </div>
                  <div style={{fontSize:10.5,color:"#78350f",marginBottom:10,lineHeight:1.5}}>
                    Automatically fetch TDS Books data from Odoo ERP. One-click sync instead of manual CSV upload.
                  </div>
                  
                  {/* Enable Toggle */}
                  <div style={{marginBottom:12}}>
                    <label style={{display:"flex",alignItems:"center",gap:8,cursor:"pointer"}}>
                      <input 
                        type="checkbox" 
                        checked={clientDraft.odooEnabled||false} 
                        onChange={e=>setClientDraft(p=>({...p,odooEnabled:e.target.checked}))}
                        style={{width:16,height:16,accentColor:"#92400e",cursor:"pointer"}}
                      />
                      <span style={{fontSize:11,fontWeight:600,color:"#92400e"}}>Enable Odoo Integration</span>
                    </label>
                  </div>
                  
                  {/* Connection Fields - only show if enabled */}
                  {clientDraft.odooEnabled&&(
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
                      {[
                        {label:"Odoo URL",key:"odooUrl",placeholder:"https://yourcompany.odoo.com",type:"text",span:2},
                        {label:"Database",key:"odooDatabase",placeholder:"Auto-detected or enter manually",type:"text"},
                        {label:"Username (Email)",key:"odooUsername",placeholder:"user@company.com",type:"text"},
                        {label:"API Key",key:"odooPassword",placeholder:"Paste API key from Odoo settings",type:"password",span:2},
                      ].map(f=>(
                        <div key={f.key} style={{gridColumn:f.span===2?"1/-1":"auto"}}>
                          <div style={{fontSize:10,fontWeight:600,color:"#78350f",marginBottom:4,textTransform:"uppercase",letterSpacing:0.3}}>{f.label}</div>
                          <input 
                            type={f.type} 
                            value={clientDraft[f.key]||""} 
                            onChange={e=>setClientDraft(p=>({...p,[f.key]:e.target.value}))} 
                            placeholder={f.placeholder}
                            style={{width:"100%",padding:"6px 9px",border:"1px solid #fde047",borderRadius:4,fontSize:11.5,fontFamily:f.key==="odooPassword"?"monospace":"inherit",outline:"none",boxSizing:"border-box",background:"#fff"}}
                            onFocus={e=>e.target.style.borderColor="#92400e"} 
                            onBlur={e=>e.target.style.borderColor="#fde047"}
                          />
                        </div>
                      ))}
                    </div>
                  )}
                  
                  {clientDraft.odooEnabled&&(
                    <div style={{fontSize:9.5,color:"#a16207",marginTop:8,display:"flex",alignItems:"center",gap:5}}>
                      💡 Get API Key: Odoo → Settings → Users & Companies → Your User → Manage API Keys
                    </div>
                  )}
                </div>
                
                {/* Security Notice */}
                <div style={{padding:"10px 16px",background:"#f8fafc",borderTop:"1px solid #e2e8f0",fontSize:9.5,color:"#64748b",display:"flex",alignItems:"center",gap:5}}>
                  <span>🔒</span>
                  <span>All passwords encrypted with AES-256 · Stored locally only · Never sent to any server</span>
                </div>
              </div>
              
              <div style={{display:"flex",gap:8,justifyContent:"flex-end",marginTop:20}}>
                <button onClick={()=>setShowAddClientModal(false)} style={{background:"none",border:"1px solid var(--bd)",borderRadius:4,padding:"8px 18px",cursor:"pointer",fontSize:13,fontFamily:"inherit"}}>Cancel</button>
                <button onClick={saveClientDraft} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:4,padding:"8px 22px",cursor:"pointer",fontSize:13,fontFamily:"inherit",fontWeight:600}}>{editClientId?"Save Changes":"Add Client"}</button>
              </div>
            </div>
          </div>
        )}

        {showCompanyModal&&(
          <div className="modal-bg" onClick={()=>setShowCompanyModal(false)}>
            <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:7,padding:28,width:400,boxShadow:"0 8px 40px rgba(0,0,0,0.22)"}}>
              <div style={{fontSize:15,fontWeight:600,marginBottom:16}}>Manage Companies</div>
              {/* Existing companies */}
              <div style={{marginBottom:16}}>
                {companies.map(c=>(
                  <div key={c.id} style={{display:"flex",alignItems:"center",gap:8,padding:"6px 8px",borderRadius:3,background:"var(--sur)",marginBottom:4}}>
                    {editingCompany===c.id
                      ? <input autoFocus value={newCompanyName} onChange={e=>setNewCompanyName(e.target.value)} onBlur={()=>renameCompany(c.id,newCompanyName)} onKeyDown={e=>{if(e.key==="Enter")renameCompany(c.id,newCompanyName);if(e.key==="Escape")setEditingCompany(null);}} style={{flex:1,border:"1px solid var(--a)",borderRadius:2,padding:"3px 7px",fontSize:12,fontFamily:"inherit",outline:"none"}}/>
                      : <span style={{flex:1,fontSize:12.5,fontWeight:500}}>{c.name}</span>
                    }
                    <button onClick={()=>{setEditingCompany(c.id);setNewCompanyName(c.name);}} style={{background:"none",border:"1px solid var(--bd)",borderRadius:2,padding:"2px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",color:"var(--a)"}}>Rename</button>
                    {companies.length>1&&<button onClick={()=>{if(window.confirm(`Delete "${c.name}"?`))deleteCompany(c.id);}} style={{background:"none",border:"1px solid var(--bd)",borderRadius:2,padding:"2px 8px",cursor:"pointer",fontSize:11,fontFamily:"inherit",color:"var(--red)"}}>Delete</button>}
                  </div>
                ))}
              </div>
              <div style={{borderTop:"1px solid var(--bd)",paddingTop:14,marginBottom:4,fontSize:12,fontWeight:600,color:"var(--tx2)"}}>Add New Company</div>
              <div style={{display:"flex",gap:8,marginBottom:14}}>
                <input value={newCompanyName} onChange={e=>setNewCompanyName(e.target.value)} onKeyDown={e=>e.key==="Enter"&&addCompany()} placeholder="Company name..." style={{flex:1,border:"1px solid var(--bd)",borderRadius:3,padding:"7px 10px",fontSize:12.5,fontFamily:"inherit",outline:"none"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                <button onClick={addCompany} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"7px 16px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:600,whiteSpace:"nowrap"}}>Add</button>
              </div>
              <div style={{display:"flex",justifyContent:"flex-end"}}>
                <button onClick={()=>setShowCompanyModal(false)} style={{border:"1px solid var(--bd)",background:"none",borderRadius:3,padding:"7px 18px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit"}}>Close</button>
              </div>
            </div>
          </div>
        )}

        {/* ── GMAIL SETUP MODAL ── */}
        {showGmailSetup&&(
          <div className="modal-bg" onClick={()=>{if(!gmailConnecting){setShowGmailSetup(false);setGmailAuthError("");}}}>
            <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:7,padding:28,width:500,boxShadow:"0 8px 40px rgba(0,0,0,0.22)"}}>
              <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:4}}>
                <div style={{width:36,height:36,borderRadius:5,background:"#e6f3fb",display:"flex",alignItems:"center",justifyContent:"center",fontSize:20}}>📧</div>
                <div><div style={{fontSize:15,fontWeight:600}}>Connect Gmail</div><div style={{fontSize:11.5,color:"var(--tx2)"}}>Send notices directly via your Gmail account</div></div>
              </div>

              {/* ── Persistent error box — never auto-dismisses ── */}
              {gmailAuthError&&(
                <div style={{background:"#fde7e9",border:"2px solid #f4a8b0",borderRadius:5,padding:"11px 14px",margin:"12px 0",fontSize:12,color:"#a4262c",lineHeight:1.7,wordBreak:"break-word"}}>
                  <b style={{fontSize:13}}>⛔ Authentication failed</b><br/>
                  <span style={{fontFamily:"Consolas,monospace",fontSize:11.5}}>{gmailAuthError}</span>
                  <div style={{marginTop:7,fontSize:11,color:"#7a1c22",background:"#fff4f4",borderRadius:3,padding:"6px 9px"}}>
                    💡 <b>To debug:</b> Open DevTools → Ctrl+Shift+I → Console tab → look for <i>[Gmail OAuth]</i> line.<br/>
                    Common fixes: re-paste Client Secret · check redirect URIs · ensure Gmail API is enabled.
                  </div>
                </div>
              )}

              <div style={{background:"#fff8e1",border:"1px solid #ffe082",borderRadius:4,padding:"10px 13px",marginBottom:14,marginTop:gmailAuthError?0:10,fontSize:11.5,color:"#795548",lineHeight:1.7}}>
                ℹ️ You need a <b>Google OAuth Client ID &amp; Secret</b> (Desktop App type).<br/>
                <b>⚠️ Required — Register all 4 redirect URIs in Google Cloud Console:</b>
                <div style={{fontFamily:"Consolas,monospace",fontSize:11,background:"#fffde7",border:"1px solid #ffe082",borderRadius:3,padding:"5px 8px",marginTop:5,lineHeight:1.9,color:"#5d4037",userSelect:"all"}}>
                  http://localhost:9173<br/>http://localhost:9174<br/>http://localhost:9175<br/>http://localhost:9176
                </div>
                <span style={{fontSize:10.5,color:"#8d6e63",display:"block",marginTop:4}}>All 4 must be added — the app picks whichever port is free. Missing any causes a <i>redirect_uri_mismatch</i> error.</span>
              </div>
              <div style={{marginBottom:14}}>
                <div style={{fontSize:11.5,fontWeight:600,marginBottom:5,color:"var(--tx2)"}}>OAuth Client ID</div>
                <input autoFocus value={gmailClientIdDraft||gmailClientId} onChange={e=>setGmailClientIdDraft(e.target.value)} disabled={gmailConnecting} placeholder="xxxxx.apps.googleusercontent.com" style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"8px 10px",fontSize:12,fontFamily:"Consolas,monospace",outline:"none",color:"var(--tx)",boxSizing:"border-box"}} onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"} />
                {gmailClientId&&!gmailClientIdDraft&&<div style={{fontSize:10.5,color:"var(--grn)",marginTop:3}}>✓ Client ID saved — click Connect to sign in</div>}
              </div>
              <div style={{marginBottom:14}}>
                <div style={{fontSize:11.5,fontWeight:600,marginBottom:5,color:"var(--tx2)"}}>OAuth Client Secret <span style={{fontWeight:400,color:"var(--tx3)"}}>(required — from Google Cloud Console)</span></div>
                <input value={gmailClientSecretDraft||(gmailClientSecret?"••••••••":"")} onChange={e=>setGmailClientSecretDraft(e.target.value)} disabled={gmailConnecting} placeholder="Paste your Client Secret here" style={{width:"100%",border:"1px solid var(--bd)",borderRadius:3,padding:"8px 10px",fontSize:12,fontFamily:"Consolas,monospace",outline:"none",color:"var(--tx)",boxSizing:"border-box"}} onFocus={e=>{e.target.style.borderColor="var(--a)";if(!gmailClientSecretDraft)setGmailClientSecretDraft("");}} onBlur={e=>e.target.style.borderColor="var(--bd)"} />
                {gmailClientSecret&&!gmailClientSecretDraft&&<div style={{fontSize:10.5,color:"var(--grn)",marginTop:3}}>✓ Client Secret saved</div>}
                {!gmailClientSecret&&!gmailClientSecretDraft&&<div style={{fontSize:10.5,color:"#c67a00",marginTop:3}}>⚠ Without the Client Secret, token exchange will fail</div>}
              </div>
              <div style={{display:"flex",gap:8,justifyContent:"flex-end",alignItems:"center"}}>
                {gmailConnecting&&<span style={{fontSize:12,color:"var(--tx2)",marginRight:"auto",display:"flex",alignItems:"center",gap:6}}>⏳ Waiting for Google sign-in…</span>}
                <button onClick={()=>{if(!gmailConnecting){setShowGmailSetup(false);setGmailAuthError("");}}} disabled={gmailConnecting} style={{border:"1px solid var(--bd)",background:"none",borderRadius:3,padding:"7px 18px",cursor:gmailConnecting?"not-allowed":"pointer",fontSize:12.5,fontFamily:"inherit",opacity:gmailConnecting?0.4:1}}>Cancel</button>
                {gmailClientId&&!gmailClientIdDraft&&<button onClick={()=>{setShowGmailSetup(false);connectGmail();}} disabled={gmailConnecting} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"7px 18px",cursor:gmailConnecting?"not-allowed":"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:600,opacity:gmailConnecting?0.5:1}}>🔗 {gmailAuthError?"Retry Connect":"Connect Gmail"}</button>}
                {(gmailClientIdDraft||!gmailClientId)&&<button onClick={()=>saveGmailClientId(gmailClientIdDraft||gmailClientId)} disabled={!(gmailClientIdDraft||gmailClientId).trim()||gmailConnecting} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"7px 18px",cursor:"pointer",fontSize:12.5,fontFamily:"inherit",fontWeight:600,opacity:(gmailClientIdDraft||gmailClientId).trim()&&!gmailConnecting?1:0.4}}>💾 Save & Connect</button>}
              </div>
            </div>
          </div>
        )}

        {/* ── GMAIL SEND PROGRESS ── */}
        {gmailSending&&(
          <div className="modal-bg">
            <div style={{background:"var(--wh)",borderRadius:7,padding:28,width:400,boxShadow:"0 8px 40px rgba(0,0,0,0.22)",textAlign:"center"}}>
              <div style={{fontSize:32,marginBottom:8}}>📤</div>
              <div style={{fontSize:15,fontWeight:600,marginBottom:4}}>Sending via Gmail…</div>
              <div style={{fontSize:12.5,color:"var(--tx2)",marginBottom:16}}>{gmailSendProgress.done} of {gmailSendProgress.total} emails sent</div>
              <div style={{height:6,background:"#e5e5e5",borderRadius:3,overflow:"hidden",marginBottom:12}}><div style={{height:"100%",background:"var(--a)",borderRadius:3,width:`${gmailSendProgress.total?Math.round(gmailSendProgress.done/gmailSendProgress.total*100):0}%`,transition:"width 0.3s"}}/></div>
              {gmailSendProgress.errors.length>0&&<div style={{fontSize:11,color:"var(--red)",textAlign:"left",background:"#fff4f4",border:"1px solid #ffd0d0",borderRadius:3,padding:"6px 9px"}}>{gmailSendProgress.errors.map((e,i)=><div key={i}>❌ {e.tan}: {e.msg}</div>)}</div>}
            </div>
          </div>
        )}

        {/* ── DRIVE SYNC CONFLICT MODAL ── */}
        {driveSyncModal && (
          <div className="modal-bg">
            <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:8,padding:28,width:480,boxShadow:"0 8px 40px rgba(0,0,0,0.24)"}}>
              <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:14}}>
                <div style={{width:40,height:40,borderRadius:7,background:"#fff3e0",display:"flex",alignItems:"center",justifyContent:"center",fontSize:22,flexShrink:0}}>☁️</div>
                <div>
                  <div style={{fontSize:15,fontWeight:700,color:"var(--tx)"}}>Newer Data on Google Drive</div>
                  <div style={{fontSize:11.5,color:"var(--tx2)",marginTop:2}}>A newer backup was found. How should we handle it?</div>
                </div>
              </div>
              <div style={{background:"var(--sur)",border:"1px solid var(--bd)",borderRadius:5,padding:"10px 14px",marginBottom:16,fontSize:11.5,color:"var(--tx2)"}}>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6}}>
                  <div><span style={{fontWeight:600,color:"var(--tx)"}}>☁ Drive backup:</span> {driveSyncModal.driveRecords.toLocaleString()} records</div>
                  <div><span style={{fontWeight:600,color:"var(--tx)"}}>💻 Your local data:</span> {driveSyncModal.localRecords.toLocaleString()} records</div>
                  <div style={{gridColumn:"1/-1",fontSize:11,color:"var(--tx3)",marginTop:2}}>File: {driveSyncModal.driveFileName}</div>
                </div>
              </div>
              <div style={{display:"flex",flexDirection:"column",gap:10,marginBottom:4}}>
                {[
                  { label:"☁ Use Drive Data", sub:"Replace your local data with the Drive backup (best choice for syncing from a teammate's machine)", col:"#0078d4", bg:"#e6f3fb", action:"drive" },
                  { label:"🔀 Merge Both", sub:"Keep your local changes AND add Drive data. Drive wins where both have the same year.", col:"#34a853", bg:"#e8f8e8", action:"merge" },
                  { label:"💻 Keep My Local Data", sub:"Ignore the Drive backup. Your local data stays as-is.", col:"var(--tx2)", bg:"var(--sur)", action:"local" },
                ].map(opt=>(
                  <button key={opt.action} onClick={()=>{
                    if(opt.action==="drive") applyDriveData(driveSyncModal.driveData, driveSyncModal.driveRecords);
                    else if(opt.action==="merge") mergeDriveData(driveSyncModal.driveData, driveSyncModal.driveRecords);
                    else { setDriveSyncModal(null); setDriveSyncStatus("idle"); showToast("Kept local data","i"); }
                  }} style={{background:opt.bg,border:`1px solid ${opt.col}22`,borderRadius:5,padding:"11px 14px",cursor:"pointer",textAlign:"left",fontFamily:"inherit",width:"100%"}}>
                    <div style={{fontSize:13,fontWeight:700,color:opt.col,marginBottom:2}}>{opt.label}</div>
                    <div style={{fontSize:11,color:"var(--tx2)",lineHeight:1.5}}>{opt.sub}</div>
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* ── MISSING TAN MODAL ── */}
        {showMissingTanModal&&(()=>{
          const allMissingRows=datasets["Books"].filter(r=>!r.tan?.trim()&&r.deductorName);
          const partyMap={};
          allMissingRows.forEach(r=>{if(!partyMap[r.deductorName])partyMap[r.deductorName]={name:r.deductorName,rows:0,tds:0,sections:new Set()};partyMap[r.deductorName].rows++;partyMap[r.deductorName].tds+=(r.tdsDeducted||0);if(r.section)partyMap[r.deductorName].sections.add(r.section);});
          const allParties=Object.values(partyMap).sort((a,b)=>b.tds-a.tds);
          const q=missingTanSearch.toLowerCase().trim();
          const parties=q?allParties.filter(p=>p.name.toLowerCase().includes(q)):allParties;
          const fmt=n=>n?`₹${n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2})}`:"—";
          const totalTDS=allMissingRows.reduce((s,r)=>s+(r.tdsDeducted||0),0);
          const remainingCount=datasets["Books"].filter(r=>!r.tan?.trim()).length;
          const downloadMissingTan=()=>{
            const headers=["Party / Deductor Name","Rows","TDS Amount","Section(s)","TAN (fill here)"];
            const rows=allParties.map(p=>[p.name,p.rows,p.tds,[...p.sections].join(", "),""]);
            const wb=XLSX.utils.book_new();const ws=XLSX.utils.aoa_to_sheet([headers,...rows]);
            ws["!cols"]=[{wch:42},{wch:8},{wch:14},{wch:22},{wch:18}];
            XLSX.utils.book_append_sheet(wb,ws,"Missing TAN");
            const buf=XLSX.write(wb,{bookType:"xlsx",type:"array"});const blob=new Blob([buf],{type:"application/octet-stream"});
            if(isElectron){const reader=new FileReader();reader.readAsDataURL(blob);reader.onload=async()=>{const b64=reader.result.split(",")[1];const res=await window.electronAPI.saveFile({defaultName:"Missing_TAN_List.xlsx",content:b64,isBase64:true});if(res.success)showToast(`Downloaded: ${res.path}`,"s");};}
            else{const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download="Missing_TAN_List.xlsx";a.click();showToast("Missing TAN list downloaded","s");}
          };
          return(
            <div className="modal-bg" onClick={()=>{setShowMissingTanModal(false);setMissingTanSearch("");}}>
              <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:8,width:860,maxHeight:"88vh",display:"flex",flexDirection:"column",boxShadow:"0 8px 40px rgba(0,0,0,0.24)",overflow:"hidden"}}>
                {/* Header */}
                <div style={{padding:"14px 20px",borderBottom:"1px solid var(--bd)",background:"#fff4f4",display:"flex",alignItems:"center",gap:12,flexShrink:0}}>
                  <div style={{width:38,height:38,borderRadius:7,background:"#fde7e9",display:"flex",alignItems:"center",justifyContent:"center",fontSize:20,flexShrink:0}}>⚠️</div>
                  <div style={{flex:1}}>
                    <div style={{fontSize:15,fontWeight:700,color:"#a4262c"}}>Missing TAN in Books</div>
                    <div style={{fontSize:12,color:"#c0392b",marginTop:2}}>{allParties.length} unique {allParties.length===1?"party":"parties"} · {allMissingRows.length} rows · Total TDS: <strong style={{fontFamily:"Consolas,monospace"}}>{fmt(totalTDS)}</strong></div>
                  </div>
                  <button onClick={downloadMissingTan} style={{background:"#217346",color:"#fff",border:"none",borderRadius:4,padding:"7px 13px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5,flexShrink:0}}>📥 Download Excel</button>
                  <button onClick={()=>document.getElementById("missing-tan-import-input").click()} style={{background:"#0078d4",color:"#fff",border:"none",borderRadius:4,padding:"7px 13px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5,flexShrink:0}}>
                    <Ic d={I.import} s={12} c="#fff"/>Import Filled Excel
                  </button>
                  <input id="missing-tan-import-input" type="file" accept=".xlsx,.xls,.csv" style={{display:"none"}} onChange={e=>{if(e.target.files[0]){importMissingTanFromExcel(e.target.files[0]);e.target.value="";}}}/>
                  <button onClick={()=>{setShowMissingTanModal(false);setMissingTanSearch("");}} style={{background:"none",border:"none",cursor:"pointer",padding:6,borderRadius:4,color:"var(--tx3)",fontSize:20,lineHeight:1,marginLeft:2}}>✕</button>
                </div>
                {/* Workflow strip */}
                <div style={{padding:"8px 20px",background:"#f0f7ff",borderBottom:"1px solid #c8dff5",display:"flex",alignItems:"center",gap:20,flexShrink:0,flexWrap:"wrap"}}>
                  {[{n:"1",c:"#217346",t:"Download Excel",d:"Get party list with blank TAN column"},{n:"2",c:"#0078d4",t:"Fill TANs in Excel",d:"Type correct TAN next to each party"},{n:"3",c:"#5c2d91",t:"Import Filled Excel",d:"Upload back — all rows patched instantly"},{n:"OR",c:"#a4262c",t:"Type TAN directly",d:"Use input box per row below"}].map(s=>(
                    <div key={s.n} style={{display:"flex",alignItems:"flex-start",gap:7,minWidth:160}}>
                      <div style={{width:20,height:20,borderRadius:"50%",background:s.c,color:"#fff",fontSize:10,fontWeight:700,display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,marginTop:1}}>{s.n}</div>
                      <div><div style={{fontSize:11.5,fontWeight:600,color:"var(--tx)"}}>{s.t}</div><div style={{fontSize:10.5,color:"var(--tx2)",lineHeight:1.4}}>{s.d}</div></div>
                    </div>
                  ))}
                </div>
                {/* Search bar */}
                <div style={{padding:"7px 16px",background:"var(--sur)",borderBottom:"1px solid var(--bd)",display:"flex",alignItems:"center",gap:10,flexShrink:0}}>
                  <div style={{display:"flex",alignItems:"center",gap:6,background:"var(--wh)",border:"1px solid var(--bd)",borderRadius:4,padding:"4px 9px",flex:"0 0 280px"}}>
                    <Ic d={I.search} s={12} c="#aaa"/>
                    <input autoFocus value={missingTanSearch} onChange={e=>setMissingTanSearch(e.target.value)} placeholder="Filter parties by name..." style={{border:"none",outline:"none",fontSize:12,fontFamily:"inherit",color:"var(--tx)",background:"transparent",width:"100%"}}/>
                    {missingTanSearch&&<span onClick={()=>setMissingTanSearch("")} style={{cursor:"pointer",color:"#aaa",fontSize:12}}>✕</span>}
                  </div>
                  {q&&<span style={{fontSize:11.5,color:"var(--tx2)"}}>{parties.length} of {allParties.length} parties</span>}
                  <span style={{marginLeft:"auto",fontSize:11,color:"var(--tx3)"}}>Sorted by TDS amount · highest first</span>
                </div>
                {/* Table */}
                <div style={{flex:1,overflowY:"auto"}}>
                  {parties.length===0?(
                    <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",padding:48,color:"var(--tx3)",gap:8}}>
                      <Ic d={I.search} s={36} c="#ddd" sw={1}/><div style={{fontSize:13}}>No parties match "<strong>{missingTanSearch}</strong>"</div>
                    </div>
                  ):(
                    <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                      <thead style={{position:"sticky",top:0,zIndex:2}}>
                        <tr style={{background:"#f7f0f0"}}>
                          <th style={{padding:"8px 12px",textAlign:"left",fontWeight:600,fontSize:11,color:"#a4262c",borderBottom:"2px solid #f0c4b4",width:34}}>#</th>
                          <th style={{padding:"8px 12px",textAlign:"left",fontWeight:600,fontSize:11,color:"#a4262c",borderBottom:"2px solid #f0c4b4"}}>Party / Deductor Name</th>
                          <th style={{padding:"8px 12px",textAlign:"right",fontWeight:600,fontSize:11,color:"#a4262c",borderBottom:"2px solid #f0c4b4",width:58}}>Rows</th>
                          <th style={{padding:"8px 12px",textAlign:"right",fontWeight:600,fontSize:11,color:"#a4262c",borderBottom:"2px solid #f0c4b4",width:130}}>TDS Amount</th>
                          <th style={{padding:"8px 12px",textAlign:"left",fontWeight:600,fontSize:11,color:"#a4262c",borderBottom:"2px solid #f0c4b4",width:110}}>Section(s)</th>
                          <th style={{padding:"8px 12px",textAlign:"left",fontWeight:600,fontSize:11,color:"#a4262c",borderBottom:"2px solid #f0c4b4",width:240}}>Enter TAN manually</th>
                        </tr>
                      </thead>
                      <tbody>
                        {parties.map((p,i)=>{
                          const alreadyMapped=!datasets["Books"].some(r=>!r.tan?.trim()&&r.deductorName===p.name);
                          return(
                            <tr key={p.name} style={{borderBottom:"1px solid #f5f5f5",background:alreadyMapped?"#f0fdf0":i%2===0?"var(--wh)":"#fdf8f8"}}>
                              <td style={{padding:"8px 12px",color:"#bbb",fontSize:11}}>{i+1}</td>
                              <td style={{padding:"8px 12px",fontWeight:500,color:"var(--tx)"}}>{p.name}{alreadyMapped&&<span style={{marginLeft:7,background:"#e8f8e8",color:"var(--grn)",borderRadius:3,padding:"1px 7px",fontSize:10,fontWeight:700}}>✓ Mapped</span>}</td>
                              <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11,color:"var(--tx2)"}}>{p.rows}</td>
                              <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"Consolas,monospace",fontSize:11.5,fontWeight:600,color:"#a80000"}}>{fmt(p.tds)}</td>
                              <td style={{padding:"8px 12px"}}>{[...p.sections].slice(0,3).map(s=><span key={s} style={{background:"#e6f3fb",color:"#0078d4",borderRadius:3,padding:"1px 6px",fontSize:10,fontWeight:600,marginRight:3,fontFamily:"Consolas,monospace"}}>{s}</span>)}{p.sections.size>3&&<span style={{fontSize:10,color:"var(--tx3)"}}>+{p.sections.size-3}</span>}</td>
                              <td style={{padding:"6px 10px"}}>
                                {alreadyMapped?<span style={{fontSize:11,color:"var(--grn)"}}>✓ TAN assigned</span>:(
                                  <div style={{display:"flex",gap:6,alignItems:"center"}}>
                                    <input value={missingTanEdits[p.name]||""} onChange={e=>setMissingTanEdits(prev=>({...prev,[p.name]:e.target.value.toUpperCase()}))} onKeyDown={e=>{if(e.key==="Enter"&&missingTanEdits[p.name])addManualTan(p.name,missingTanEdits[p.name]);}} placeholder="e.g. ABCD12345E" maxLength={10}
                                      style={{width:118,border:"1px solid var(--bd)",borderRadius:3,padding:"4px 8px",fontSize:11.5,fontFamily:"Consolas,monospace",outline:"none",color:"var(--tx)",letterSpacing:"0.5px"}}
                                      onFocus={e=>e.target.style.borderColor="var(--a)"} onBlur={e=>e.target.style.borderColor="var(--bd)"}/>
                                    <button onClick={()=>{if(missingTanEdits[p.name])addManualTan(p.name,missingTanEdits[p.name]);}} disabled={!missingTanEdits[p.name]}
                                      style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:3,padding:"4px 10px",cursor:"pointer",fontSize:11,fontFamily:"inherit",fontWeight:600,opacity:missingTanEdits[p.name]?1:0.4,whiteSpace:"nowrap"}}>Map TAN</button>
                                  </div>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  )}
                </div>
                {/* Footer */}
                <div style={{padding:"10px 20px",borderTop:"1px solid var(--bd)",background:"var(--sur)",display:"flex",alignItems:"center",justifyContent:"space-between",flexShrink:0}}>
                  <div style={{fontSize:11.5,color:"var(--tx2)"}}>
                    {remainingCount===0?<span style={{color:"var(--grn)",fontWeight:600}}>✅ All TANs mapped! Re-run Reconciliation to see updated results.</span>
                      :<><strong style={{color:"#a80000"}}>{remainingCount} rows</strong> still missing TAN across <strong style={{color:"#a80000"}}>{allParties.filter(p=>datasets["Books"].some(r=>!r.tan?.trim()&&r.deductorName===p.name)).length} parties</strong></>}
                  </div>
                  <div style={{display:"flex",gap:8}}>
                    <button onClick={downloadMissingTan} style={{background:"#217346",color:"#fff",border:"none",borderRadius:4,padding:"7px 14px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600}}>📥 Download Excel</button>
                    <button onClick={()=>document.getElementById("missing-tan-import-input").click()} style={{background:"#0078d4",color:"#fff",border:"none",borderRadius:4,padding:"7px 14px",cursor:"pointer",fontSize:12,fontFamily:"inherit",fontWeight:600,display:"flex",alignItems:"center",gap:5}}><Ic d={I.import} s={12} c="#fff"/>Import Filled Excel</button>
                    <button onClick={()=>{setShowMissingTanModal(false);setMissingTanSearch("");}} style={{background:"var(--a)",color:"#fff",border:"none",borderRadius:4,padding:"7px 22px",cursor:"pointer",fontSize:13,fontFamily:"inherit",fontWeight:600}}>Close</button>
                  </div>
                </div>
              </div>
            </div>
          );
        })()}

        {/* ── SMART CLEAR MODAL ── */}
        {showClearModal&&(
          <div className="modal-bg" onClick={()=>setShowClearModal(false)}>
            <div onClick={e=>e.stopPropagation()} style={{background:"var(--wh)",borderRadius:8,padding:28,width:420,boxShadow:"0 8px 40px rgba(0,0,0,0.22)"}}>
              <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:16}}>
                <div style={{width:38,height:38,borderRadius:7,background:"#fde7e9",display:"flex",alignItems:"center",justifyContent:"center",fontSize:20,flexShrink:0}}>🗑️</div>
                <div>
                  <div style={{fontSize:15,fontWeight:700}}>Clear Data</div>
                  <div style={{fontSize:11.5,color:"var(--tx2)"}}>Select what to clear for {curCompany?.name} · FY {selYear}</div>
                </div>
              </div>
              <div style={{display:"flex",flexDirection:"column",gap:8,marginBottom:18}}>
                {(datasets["Books"]||[]).length>0&&(
                  <label style={{display:"flex",alignItems:"center",gap:10,background:"#fff4e0",border:"1px solid #ffd591",borderRadius:5,padding:"10px 14px",cursor:"pointer"}}>
                    <input type="checkbox" checked={clearSelections.books} onChange={e=>setClearSelections(p=>({...p,books:e.target.checked}))} style={{width:15,height:15,accentColor:"var(--a)",cursor:"pointer"}}/>
                    <div style={{flex:1}}>
                      <div style={{fontSize:12.5,fontWeight:600}}>Books Data</div>
                      <div style={{fontSize:11,color:"var(--tx2)"}}>{(datasets["Books"]||[]).length.toLocaleString()} records · Can re-import from Tally/SAP</div>
                    </div>
                  </label>
                )}
                {(datasets["AIS"]||[]).length>0&&(
                  <label style={{display:"flex",alignItems:"center",gap:10,background:"#f0e8ff",border:"1px solid #d4b8ff",borderRadius:5,padding:"10px 14px",cursor:"pointer"}}>
                    <input type="checkbox" checked={clearSelections.ais} onChange={e=>setClearSelections(p=>({...p,ais:e.target.checked}))} style={{width:15,height:15,accentColor:"var(--a)",cursor:"pointer"}}/>
                    <div style={{flex:1}}>
                      <div style={{fontSize:12.5,fontWeight:600}}>AIS Data</div>
                      <div style={{fontSize:11,color:"var(--tx2)"}}>{(datasets["AIS"]||[]).length.toLocaleString()} records · Can re-download from IT Portal</div>
                    </div>
                  </label>
                )}
                {(datasets["26AS"]||[]).length>0&&(
                  <label style={{display:"flex",alignItems:"center",gap:10,background:clearSelections.as26?"#fde7e9":"#e6f3fb",border:`1px solid ${clearSelections.as26?"#ffc0c0":"#b3d4f5"}`,borderRadius:5,padding:"10px 14px",cursor:"pointer",transition:"all 0.15s"}}>
                    <input type="checkbox" checked={clearSelections.as26} onChange={e=>setClearSelections(p=>({...p,as26:e.target.checked}))} style={{width:15,height:15,accentColor:"var(--red)",cursor:"pointer"}}/>
                    <div style={{flex:1}}>
                      <div style={{fontSize:12.5,fontWeight:600,color:clearSelections.as26?"var(--red)":"var(--a)"}}>26AS Data {clearSelections.as26?"⚠️":""}</div>
                      <div style={{fontSize:11,color:"var(--tx2)"}}>{(datasets["26AS"]||[]).length.toLocaleString()} records · Requires TRACES login to re-download</div>
                    </div>
                    {!clearSelections.as26&&<span style={{fontSize:10,background:"#0078d4",color:"#fff",borderRadius:3,padding:"2px 6px",flexShrink:0}}>Protected</span>}
                  </label>
                )}
              </div>
              {clearSelections.as26&&(
                <div style={{background:"#fff4f4",border:"1px solid #ffd0d0",borderRadius:4,padding:"8px 12px",marginBottom:14,fontSize:11.5,color:"#a4262c"}}>
                  ⚠️ Clearing 26AS requires you to login to TRACES again and re-download. Are you sure?
                </div>
              )}
              <div style={{display:"flex",justifyContent:"flex-end",gap:8}}>
                <button onClick={()=>setShowClearModal(false)} style={{border:"1px solid var(--bd)",background:"none",borderRadius:4,padding:"8px 18px",cursor:"pointer",fontSize:13,fontFamily:"inherit"}}>Cancel</button>
                <button onClick={()=>{
                  const newDs = {...datasets};
                  if(clearSelections.books) newDs["Books"]=[];
                  if(clearSelections.ais)   newDs["AIS"]=[];
                  if(clearSelections.as26)  newDs["26AS"]=[];
                  updateCurYear(yd=>({...yd, datasets:newDs, files: yd.files.filter(f=>
                    !(clearSelections.books&&f.category==="Books")&&
                    !(clearSelections.ais&&f.category==="AIS")&&
                    !(clearSelections.as26&&f.category==="26AS")
                  ), reconResults:[], reconDone:false}));
                  const cleared=[];
                  if(clearSelections.books&&datasets["Books"]?.length) cleared.push("Books");
                  if(clearSelections.ais&&datasets["AIS"]?.length)     cleared.push("AIS");
                  if(clearSelections.as26&&datasets["26AS"]?.length)   cleared.push("26AS");
                  showToast(cleared.length?`Cleared: ${cleared.join(", ")}`:"Nothing to clear");
                  setShowClearModal(false);
                  setClearSelections({books:true,ais:true,as26:false});
                }} disabled={!clearSelections.books&&!clearSelections.ais&&!clearSelections.as26}
                style={{background:"var(--red)",color:"#fff",border:"none",borderRadius:4,padding:"8px 22px",cursor:"pointer",fontSize:13,fontFamily:"inherit",fontWeight:600,opacity:(!clearSelections.books&&!clearSelections.ais&&!clearSelections.as26)?0.4:1}}>
                  Clear Selected
                </button>
              </div>
            </div>
          </div>
        )}
        {/* Odoo Sync Progress Modal */}
        {showOdooSyncModal && (
          <div className="modal-bg" onClick={e => { if (e.target === e.currentTarget && !odooSyncStarted) { setShowOdooSyncModal(false); setOdooSyncStarted(false); }}}>
            <div style={{
              background:"var(--wh)",
              borderRadius:8,
              width:480,
              maxHeight:"85vh",
              overflowY:"auto",
              boxShadow:"0 25px 70px rgba(0,0,0,0.25), 0 0 0 1px rgba(0,0,0,0.08)",
              animation:"modalIn 0.18s ease-out"
            }}>
              {/* Header */}
              <div style={{
                padding:"18px 24px 16px",
                borderBottom:"1px solid var(--bd)",
                display:"flex",
                alignItems:"center",
                gap:12,
                background:"var(--hb)"
              }}>
                <div style={{
                  width:36,height:36,borderRadius:6,
                  background:"linear-gradient(135deg,#e8590c,#d14500)",
                  display:"flex",alignItems:"center",justifyContent:"center",
                  boxShadow:"0 2px 8px rgba(209,69,0,0.3)"
                }}>
                  <Ic d={I.refresh} s={18} c="#fff"/>
                </div>
                <div style={{flex:1}}>
                  <div style={{fontSize:15,fontWeight:600,color:"var(--tx)"}}>{odooSyncStarted ? (odooSyncType === 'invoices' ? "Syncing Invoices" : "Syncing TDS") : "Sync from Odoo ERP"}</div>
                  <div style={{fontSize:11,color:"var(--tx2)",marginTop:1}}>{curCompany.name} · FY {selYear}</div>
                </div>
                {!odooSyncStarted && (
                  <button onClick={() => { setShowOdooSyncModal(false); setOdooSyncStarted(false); }}
                    style={{background:"none",border:"none",cursor:"pointer",padding:6,borderRadius:4,color:"var(--tx2)",fontSize:16,lineHeight:1,display:"flex"}}
                    onMouseOver={e => e.currentTarget.style.background="var(--sur)"}
                    onMouseOut={e => e.currentTarget.style.background="none"}
                  >✕</button>
                )}
              </div>

              {/* Date Range Picker - Before Sync */}
              {!odooSyncStarted && (
                <div style={{padding:"20px 24px"}}>
                  {/* Quick Period Selectors */}
                  <div style={{display:"flex",gap:0,marginBottom:18,borderRadius:5,overflow:"hidden",border:"1px solid var(--bd)"}}>
                    <button onClick={() => setOdooSyncType('tds')} style={{
                      flex:1,padding:"9px 0",fontSize:12.5,fontWeight:odooSyncType==='tds'?600:400,
                      fontFamily:"inherit",border:"none",cursor:"pointer",
                      background:odooSyncType==='tds'?"var(--a)":"var(--wh)",
                      color:odooSyncType==='tds'?"#fff":"var(--tx)",
                      transition:"all 0.15s"
                    }}>📋 TDS Entries</button>
                    <button onClick={() => setOdooSyncType('invoices')} style={{
                      flex:1,padding:"9px 0",fontSize:12.5,fontWeight:odooSyncType==='invoices'?600:400,
                      fontFamily:"inherit",border:"none",cursor:"pointer",borderLeft:"1px solid var(--bd)",
                      background:odooSyncType==='invoices'?"var(--a)":"var(--wh)",
                      color:odooSyncType==='invoices'?"#fff":"var(--tx)",
                      transition:"all 0.15s"
                    }}>🧾 Sales Invoices</button>
                  </div>
                  
                  {odooSyncType === 'invoices' && (
                    <div style={{padding:"8px 12px",marginBottom:14,background:"#fff8e6",borderRadius:4,border:"1px solid #ffe0a0",fontSize:11.5,color:"#6d5200",lineHeight:1.5}}>
                      Fetches posted sales invoices and updates existing Books entries with <strong>invoice dates</strong>, <strong>quarters</strong>, and <strong>tax-excluded amounts</strong>.
                    </div>
                  )}
                  
                  <div style={{fontSize:12,fontWeight:600,color:"var(--tx2)",marginBottom:10,textTransform:"uppercase",letterSpacing:"0.5px"}}>Period</div>
                  <div style={{display:"flex",gap:6,marginBottom:18,flexWrap:"wrap"}}>
                    {(() => {
                      const [sy] = selYear.split('-');
                      const ey = parseInt(sy) + 1;
                      return [
                        {label: 'Full Year', from: sy+'-04-01', to: ey+'-03-31'},
                        {label: 'Q1 · Apr–Jun', from: sy+'-04-01', to: sy+'-06-30'},
                        {label: 'Q2 · Jul–Sep', from: sy+'-07-01', to: sy+'-09-30'},
                        {label: 'Q3 · Oct–Dec', from: sy+'-10-01', to: sy+'-12-31'},
                        {label: 'Q4 · Jan–Mar', from: ey+'-01-01', to: ey+'-03-31'},
                      ].map(q => {
                        const active = odooDateRange.from===q.from && odooDateRange.to===q.to;
                        return (
                          <button 
                            key={q.label}
                            onClick={() => setOdooDateRange({from: q.from, to: q.to})}
                            style={{
                              padding:"6px 14px",
                              fontSize:11.5,
                              fontFamily:"inherit",
                              fontWeight: active ? 600 : 400,
                              border: active ? "1.5px solid var(--a)" : "1px solid var(--bd)",
                              borderRadius:4,
                              cursor:"pointer",
                              background: active ? "var(--a-lt)" : "var(--wh)",
                              color: active ? "var(--a-dk)" : "var(--tx)",
                              transition:"all 0.12s",
                              outline:"none"
                            }}
                          >{q.label}</button>
                        );
                      });
                    })()}
                  </div>

                  {/* Custom Date Range */}
                  <div style={{fontSize:12,fontWeight:600,color:"var(--tx2)",marginBottom:10,textTransform:"uppercase",letterSpacing:"0.5px"}}>Custom Range</div>
                  <div style={{display:"flex",gap:12,alignItems:"flex-end",marginBottom:20}}>
                    <div style={{flex:1}}>
                      <label style={{fontSize:11,color:"var(--tx2)",display:"block",marginBottom:4,fontWeight:500}}>From</label>
                      <input 
                        type="date" 
                        value={odooDateRange.from} 
                        onChange={e => setOdooDateRange(p => ({...p, from: e.target.value}))}
                        style={{
                          width:"100%",padding:"7px 10px",
                          border:"1px solid var(--bd)",borderRadius:4,
                          fontSize:12.5,fontFamily:"inherit",
                          background:"var(--wh)",color:"var(--tx)",
                          outline:"none",transition:"border-color 0.15s"
                        }}
                        onFocus={e => e.target.style.borderColor="var(--a)"}
                        onBlur={e => e.target.style.borderColor="var(--bd)"}
                      />
                    </div>
                    <div style={{color:"var(--tx3)",fontSize:13,paddingBottom:8}}>→</div>
                    <div style={{flex:1}}>
                      <label style={{fontSize:11,color:"var(--tx2)",display:"block",marginBottom:4,fontWeight:500}}>To</label>
                      <input 
                        type="date" 
                        value={odooDateRange.to} 
                        onChange={e => setOdooDateRange(p => ({...p, to: e.target.value}))}
                        style={{
                          width:"100%",padding:"7px 10px",
                          border:"1px solid var(--bd)",borderRadius:4,
                          fontSize:12.5,fontFamily:"inherit",
                          background:"var(--wh)",color:"var(--tx)",
                          outline:"none",transition:"border-color 0.15s"
                        }}
                        onFocus={e => e.target.style.borderColor="var(--a)"}
                        onBlur={e => e.target.style.borderColor="var(--bd)"}
                      />
                    </div>
                  </div>

                  {/* Info Bar */}
                  <div style={{
                    display:"flex",alignItems:"center",gap:10,
                    padding:"10px 14px",marginBottom:20,
                    background:"var(--hb)",borderRadius:5,
                    border:"1px solid var(--bd)",fontSize:11.5,color:"var(--tx2)"
                  }}>
                    <span style={{fontSize:14}}>🏢</span>
                    <span>{curCompany.name}</span>
                    <span style={{color:"var(--bd)"}}>|</span>
                    <span style={{fontWeight:600,color:"var(--tx)"}}>
                      {(() => {
                        const n = curCompany.name.toLowerCase();
                        if (n.includes('ginni') || n.includes('gsl')) return 'SMH, SWB, STN, SHR, SKN, SOH';
                        if (n.includes('easemy') || n.includes('emg')) return 'SEM';
                        if (n.includes('browntape') || n.includes('bt')) return 'SBTE, SBTM, SBT';
                        return '—';
                      })()}
                    </span>
                  </div>

                  {/* Actions */}
                  <div style={{display:"flex",gap:10}}>
                    <button 
                      onClick={syncFromOdooERP}
                      disabled={!odooDateRange.from || !odooDateRange.to}
                      style={{
                        flex:1,
                        background: (!odooDateRange.from || !odooDateRange.to) ? "#ccc" : "var(--a)",
                        color:"#fff",
                        border:"none",
                        borderRadius:4,
                        padding:"10px 16px",
                        cursor: (!odooDateRange.from || !odooDateRange.to) ? "not-allowed" : "pointer",
                        fontSize:13,
                        fontWeight:600,
                        fontFamily:"inherit",
                        transition:"background 0.15s",
                        boxShadow: (!odooDateRange.from || !odooDateRange.to) ? "none" : "0 1px 4px rgba(0,120,212,0.3)"
                      }}
                    >
                      Start Sync
                    </button>
                    <button 
                      onClick={() => { setShowOdooSyncModal(false); setOdooSyncStarted(false); }}
                      style={{
                        padding:"10px 20px",
                        border:"1px solid var(--bd)",
                        borderRadius:4,
                        cursor:"pointer",
                        fontSize:13,
                        background:"var(--wh)",
                        color:"var(--tx)",
                        fontFamily:"inherit",
                        transition:"background 0.12s"
                      }}
                      onMouseOver={e => e.currentTarget.style.background="var(--sur)"}
                      onMouseOut={e => e.currentTarget.style.background="var(--wh)"}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}

              {/* Progress Steps */}
              {odooSyncStarted && (
                <div style={{padding:"20px 24px"}}>
                  <div style={{display:"flex",flexDirection:"column",gap:2,marginBottom:20}}>
                    {[
                      {id:'auth',label:'Connecting to Odoo',icon:'🔗'},
                      {id:'auth_success',label:'Authentication',icon:'🔐'},
                      {id:'search',label:'Searching TDS records',icon:'🔍'},
                      {id:'search_complete',label:'TDS records found',icon:'📋'},
                      {id:'read',label:'Fetching details',icon:'📥'},
                      {id:'filter_complete',label:'Filtered by company',icon:'🏢'},
                      {id:'amounts',label:'Getting invoice amounts',icon:'💰'},
                      {id:'transform',label:'Transforming data',icon:'⚙️'},
                      {id:'complete',label:'Sync complete',icon:'✨'}
                    ].map((s, idx) => {
                      const steps = ['auth','auth_success','search','search_complete','read','filter_complete','amounts','transform','complete'];
                      const currentIdx = steps.indexOf(odooSyncProgress.step);
                      const stepIdx = steps.indexOf(s.id);
                      const isDone = stepIdx < currentIdx || odooSyncComplete;
                      const isCurrent = s.id === odooSyncProgress.step && !odooSyncComplete;
                      const isPending = stepIdx > currentIdx && !odooSyncComplete;
                      
                      return (
                        <div key={s.id} style={{
                          display:"flex",alignItems:"center",gap:12,
                          padding:"8px 12px",
                          borderRadius:5,
                          background: isCurrent ? "var(--a-lt)" : "transparent",
                          transition:"all 0.2s"
                        }}>
                          <div style={{
                            width:24,height:24,borderRadius:12,
                            display:"flex",alignItems:"center",justifyContent:"center",
                            fontSize:11,fontWeight:700,flexShrink:0,
                            background: isDone ? "var(--grn)" : isCurrent ? "var(--a)" : "var(--sur)",
                            color: isDone || isCurrent ? "#fff" : "var(--tx3)",
                            border: isPending ? "1.5px solid var(--bd)" : "none",
                            transition:"all 0.2s"
                          }}>
                            {isDone ? "✓" : isCurrent ? "⋯" : idx + 1}
                          </div>
                          <div style={{flex:1}}>
                            <div style={{
                              fontSize:12.5,
                              fontWeight: isCurrent ? 600 : isDone ? 500 : 400,
                              color: isPending ? "var(--tx3)" : "var(--tx)"
                            }}>{s.label}</div>
                            {isCurrent && odooSyncProgress.message && (
                              <div style={{fontSize:10.5,color:"var(--tx2)",marginTop:1}}>{odooSyncProgress.message}</div>
                            )}
                            {s.id === 'search_complete' && isDone && odooSyncProgress.count > 0 && (
                              <div style={{fontSize:10.5,color:"var(--tx2)",marginTop:1}}>Found {odooSyncProgress.count} records</div>
                            )}
                          </div>
                          {isCurrent && odooSyncProgress.count > 0 && (
                            <span style={{
                              background:"var(--a)",color:"#fff",
                              borderRadius:10,padding:"2px 10px",
                              fontSize:10.5,fontWeight:700
                            }}>{odooSyncProgress.count}</span>
                          )}
                        </div>
                      );
                    })}
                  </div>
                  
                  {/* Done / Waiting */}
                  {odooSyncComplete ? (
                    <button 
                      onClick={() => { setShowOdooSyncModal(false); setOdooSyncStarted(false); }}
                      style={{
                        width:"100%",
                        background:"var(--grn)",
                        color:"#fff",
                        border:"none",
                        borderRadius:4,
                        padding:"11px",
                        cursor:"pointer",
                        fontSize:13,
                        fontWeight:600,
                        fontFamily:"inherit",
                        boxShadow:"0 1px 4px rgba(16,124,16,0.3)"
                      }}
                    >Done</button>
                  ) : (
                    <div style={{
                      textAlign:"center",fontSize:11,color:"var(--tx3)",
                      padding:"8px 0",display:"flex",alignItems:"center",justifyContent:"center",gap:8
                    }}>
                      <span style={{display:"inline-block",width:14,height:14,border:"2px solid var(--a)",borderTopColor:"transparent",borderRadius:"50%",animation:"spin 0.8s linear infinite"}}/>
                      Processing...
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )}

        {toast&&<div className={`toast t${toast.type[0]}`}><Ic d={toast.type==="s"?I.check:toast.type==="e"?I.close:I.warn} s={13} c={toast.type==="s"?"#107c10":toast.type==="e"?"#a4262c":"#835b00"}/>{toast.msg}</div>}
      </div>
    </>
  );
}
