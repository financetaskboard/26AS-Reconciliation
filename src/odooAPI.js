/**
 * Odoo ERP Integration - XML-RPC API Helper Functions
 * 
 * CORRECTED VERSION - Fixed account search to match Odoo's behavior
 */

// ============================================================================
// XML-RPC SERIALIZATION / DESERIALIZATION
// ============================================================================

function escapeXML(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function serializeValue(value) {
  if (value === null || value === undefined) {
    return '<value><nil/></value>';
  }
  
  if (typeof value === 'string') {
    return `<value><string>${escapeXML(value)}</string></value>`;
  }
  
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return `<value><int>${value}</int></value>`;
    } else {
      return `<value><double>${value}</double></value>`;
    }
  }
  
  if (typeof value === 'boolean') {
    return `<value><boolean>${value ? '1' : '0'}</boolean></value>`;
  }
  
  if (Array.isArray(value)) {
    const items = value.map(v => serializeValue(v)).join('');
    return `<value><array><data>${items}</data></array></value>`;
  }
  
  if (typeof value === 'object') {
    const members = Object.entries(value).map(([k, v]) => 
      `<member><n>${escapeXML(k)}</n>${serializeValue(v)}</member>`
    ).join('');
    return `<value><struct>${members}</struct></value>`;
  }
  
  return '<value><nil/></value>';
}

function buildXMLRPCRequest(method, params) {
  const paramXML = params.map(p => `<param>${serializeValue(p)}</param>`).join('');
  
  return `<?xml version="1.0"?>
<methodCall>
  <methodName>${method}</methodName>
  <params>${paramXML}</params>
</methodCall>`;
}

function parseXMLRPCResponse(xmlText) {
  const parser = new DOMParser();
  const doc = parser.parseFromString(xmlText, 'text/xml');
  
  const fault = doc.querySelector('fault');
  if (fault) {
    const faultValue = deserializeValue(fault.querySelector('value'));
    throw new Error(`Odoo Error: ${faultValue.faultString || 'Unknown error'}`);
  }
  
  const valueNode = doc.querySelector('methodResponse > params > param > value');
  if (!valueNode) {
    throw new Error('Invalid XML-RPC response: no value found');
  }
  
  return deserializeValue(valueNode);
}

function deserializeValue(node) {
  if (!node) return null;
  
  const childNode = node.firstElementChild;
  
  if (!childNode) {
    return node.textContent || '';
  }
  
  switch (childNode.tagName.toLowerCase()) {
    case 'int':
    case 'i4':
      return parseInt(childNode.textContent) || 0;
      
    case 'double':
      return parseFloat(childNode.textContent) || 0;
      
    case 'boolean':
      return childNode.textContent === '1' || childNode.textContent === 'true';
      
    case 'string':
      return childNode.textContent || '';
      
    case 'datetime.iso8601':
      return childNode.textContent || '';
      
    case 'array': {
      const dataNode = childNode.querySelector('data');
      if (!dataNode) return [];
      const values = Array.from(dataNode.querySelectorAll(':scope > value'));
      return values.map(v => deserializeValue(v));
    }
      
    case 'struct': {
      const obj = {};
      const members = childNode.querySelectorAll(':scope > member');
      members.forEach(member => {
        const nameNode = member.querySelector('name');
        const valueNode = member.querySelector('value');
        if (nameNode && valueNode) {
          obj[nameNode.textContent] = deserializeValue(valueNode);
        }
      });
      return obj;
    }
      
    case 'nil':
      return null;
      
    default:
      return childNode.textContent || '';
  }
}

// ============================================================================
// ODOO API FUNCTIONS
// ============================================================================

export async function authenticateOdoo(url, database, username, apiKey) {
  const endpoint = `${url}/xmlrpc/2/common`;
  
  const requestBody = buildXMLRPCRequest('authenticate', [
    database,
    username,
    apiKey,
    {}
  ]);
  
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml',
    },
    body: requestBody,
  });
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  
  const responseText = await response.text();
  const uid = parseXMLRPCResponse(responseText);
  
  if (!uid || uid === false) {
    throw new Error('Authentication failed - check username and API key');
  }
  
  return uid;
}

export async function executeOdooMethod(url, database, uid, apiKey, model, method, args = [], kwargs = {}) {
  const endpoint = `${url}/xmlrpc/2/object`;
  
  const requestBody = buildXMLRPCRequest('execute_kw', [
    database,
    uid,
    apiKey,
    model,
    method,
    args,
    kwargs
  ]);
  
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/xml',
    },
    body: requestBody,
  });
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  
  const responseText = await response.text();
  return parseXMLRPCResponse(responseText);
}

export async function searchOdoo(url, database, uid, apiKey, model, domain) {
  return await executeOdooMethod(url, database, uid, apiKey, model, 'search', [domain]);
}

export async function readOdoo(url, database, uid, apiKey, model, ids, fields) {
  return await executeOdooMethod(url, database, uid, apiKey, model, 'read', [ids], { fields });
}

export async function searchReadOdoo(url, database, uid, apiKey, model, domain, fields, limit = null) {
  const kwargs = { fields };
  if (limit) kwargs.limit = limit;
  
  return await executeOdooMethod(url, database, uid, apiKey, model, 'search_read', [domain], kwargs);
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

export function calculateQuarter(dateStr) {
  if (!dateStr) return 'Q1';
  
  try {
    const date = new Date(dateStr);
    const month = date.getMonth() + 1;
    
    if (month >= 4 && month <= 6) return 'Q1';
    if (month >= 7 && month <= 9) return 'Q2';
    if (month >= 10 && month <= 12) return 'Q3';
    return 'Q4';
  } catch (e) {
    console.warn('Error calculating quarter:', e);
    return 'Q1';
  }
}

export function identifyCompanyFromInvoice(invoiceRef) {
  if (!invoiceRef) return null;
  
  const prefix = invoiceRef.split('/')[0].toUpperCase();
  
  if (['SMH', 'SWB', 'STN', 'SHR', 'SKN', 'SOH'].includes(prefix)) {
    return 'Ginni Systems Limited';
  }
  
  if (prefix === 'SEM') {
    return 'EASEMY BUSINESS PRIVATE LIMITED';
  }
  
  if (['SBTE', 'SBTM', 'SBT'].includes(prefix)) {
    return 'Browntape Technologies Pvt Ltd';
  }
  
  return null;
}

export function getCompanyPrefixes(companyName) {
  const normalized = companyName.toLowerCase();
  
  if (normalized.includes('ginni') || normalized.includes('gsl')) {
    return ['SMH', 'SWB', 'STN', 'SHR', 'SKN', 'SOH'];
  }
  
  if (normalized.includes('easemy') || normalized.includes('emg')) {
    return ['SEM'];
  }
  
  if (normalized.includes('browntape') || normalized.includes('bt')) {
    return ['SBTE', 'SBTM', 'SBT'];
  }
  
  return [];
}

/**
 * Keywords to match an Odoo `account.move.line.company_id` against our local
 * company. Used as a FALLBACK when a TDS line has no recognised invoice
 * prefix (typical for manual JVs like "Excess TDS" adjustments that aren't
 * tied to a customer invoice).
 *
 * Add a case here when onboarding a new company.
 */
export function getOdooCompanyKeywords(companyName) {
  const normalized = companyName.toLowerCase();

  if (normalized.includes('ginni') || normalized.includes('gsl')) {
    return ['ginni', 'gsl'];
  }

  if (normalized.includes('easemy') || normalized.includes('emg')) {
    return ['easemy', 'emg'];
  }

  if (normalized.includes('browntape') || normalized.includes('bt')) {
    return ['browntape'];
  }

  return [];
}

export function getFYDates(fyYear) {
  const [startYear] = fyYear.split('-');
  const start = `${startYear}-04-01`;
  const endYear = parseInt(startYear) + 1;
  const end = `${endYear}-03-31`;
  
  return { start, end };
}

// ============================================================================
// MAIN SYNC FUNCTION - CORRECTED VERSION
// ============================================================================

/**
 * Get account ID by code
 * This is needed because Odoo searches work on account_id (integer), not account_id.code
 */
async function getAccountIdByCode(url, database, uid, apiKey, accountCode) {
  console.log(`[Odoo] Looking up account with code: ${accountCode}`);
  
  // Search for account by code
  const accountIds = await searchOdoo(
    url,
    database,
    uid,
    apiKey,
    'account.account',
    [['code', '=', accountCode]]
  );
  
  if (accountIds.length === 0) {
    throw new Error(`Account with code ${accountCode} not found in chart of accounts`);
  }
  
  console.log(`[Odoo] Found account ID ${accountIds[0]} for code ${accountCode}`);
  return accountIds[0];
}

/**
 * Detect which moves in our result set have been reversed in Odoo.
 *
 * Returns a map keyed by ORIGINAL MOVE NAME (e.g. "TDS/2024/2601") whose
 * value is the REVERSAL MOVE NAME (e.g. "TDS/2024/3493"). Only counts
 * reversals that are themselves posted; ignores draft and cancelled reversals.
 *
 * Handles "reversal-of-reversal" — if B reverses A and C reverses B (all
 * posted), A is back in play and is NOT included in the result.
 *
 * Detects reversals created via Odoo's "Reverse Entry" wizard, which sets
 * `reversed_entry_id` on the new move. Manual contra-entries (where the user
 * passes their own JV without using the wizard) won't be detected here —
 * those still need the frontend's manual "↶ Mark Rev" button.
 *
 * Safe to fail: if reversed_entry_id isn't queryable (very old Odoo, custom
 * builds), returns {} and the sync continues without reversal detection.
 */
async function detectReversedMoves(url, database, uid, apiKey, moveIds) {
  if (!moveIds || moveIds.length === 0) return {};

  try {
    // First-level: find every POSTED move that reverses any move in our set.
    // reversed_entry_id is a many2one, returned by Odoo as [id, name].
    const reversals = await searchReadOdoo(
      url, database, uid, apiKey,
      'account.move',
      [
        ['reversed_entry_id', 'in', moveIds],
        ['state', '=', 'posted']
      ],
      ['id', 'name', 'reversed_entry_id']
    );

    if (reversals.length === 0) return {};

    // Build the candidate map: original-name -> reversal-name
    const reversalMap = {};
    reversals.forEach(rev => {
      if (Array.isArray(rev.reversed_entry_id) && rev.reversed_entry_id.length >= 2) {
        const origName = rev.reversed_entry_id[1];
        if (origName) reversalMap[origName] = rev.name;
      }
    });

    // Second pass: handle reversal-of-reversal (un-reversal). If reversal B
    // has itself been reversed by C, original A is back in play.
    const reversalIds = reversals.map(r => r.id).filter(Boolean);
    if (reversalIds.length > 0) {
      try {
        const counterReversals = await searchReadOdoo(
          url, database, uid, apiKey,
          'account.move',
          [
            ['reversed_entry_id', 'in', reversalIds],
            ['state', '=', 'posted']
          ],
          ['name', 'reversed_entry_id']
        );

        counterReversals.forEach(cr => {
          if (Array.isArray(cr.reversed_entry_id) && cr.reversed_entry_id.length >= 2) {
            const reversedReversalName = cr.reversed_entry_id[1];
            // Walk the map and drop any original whose reversal was itself reversed
            for (const origName of Object.keys(reversalMap)) {
              if (reversalMap[origName] === reversedReversalName) {
                console.log(`[Odoo Sync] ↺ Reversal-of-reversal: ${origName} → ${reversedReversalName} (now un-reversed by ${cr.name})`);
                delete reversalMap[origName];
              }
            }
          }
        });
      } catch (e) {
        console.warn('[Odoo Sync] Counter-reversal pass failed (non-fatal):', e.message);
      }
    }

    return reversalMap;
  } catch (e) {
    console.warn('[Odoo Sync] Reversal detection failed (non-fatal — continuing without it):', e.message);
    return {};
  }
}

/**
 * Sync TDS Books data from Odoo ERP
 * 
 * CORRECTED: Now searches by account_id (integer) instead of account_id.code
 */
export async function syncTDSFromOdoo(company, fyYear, onProgress = () => {}) {
  try {
    if (!company.odooEnabled) {
      throw new Error('Odoo integration is not enabled for this company');
    }
    
    if (!company.odooUrl || !company.odooDatabase || !company.odooUsername || !company.odooPassword) {
      throw new Error('Odoo credentials are incomplete. Please configure in Client Master.');
    }
    
    const { start: fyStart, end: fyEnd } = getFYDates(fyYear);
    console.log(`[Odoo Sync] FY Period: ${fyStart} to ${fyEnd}`);
    
    const companyPrefixes = getCompanyPrefixes(company.name);
    if (companyPrefixes.length === 0) {
      throw new Error(`No invoice prefixes configured for company: ${company.name}`);
    }
    console.log(`[Odoo Sync] Company prefixes:`, companyPrefixes);
    
    onProgress('auth', 'Connecting to Odoo...', 0);
    
    // Step 1: Authenticate
    const uid = await authenticateOdoo(
      company.odooUrl,
      company.odooDatabase,
      company.odooUsername,
      company.odooPassword
    );
    
    console.log(`[Odoo Sync] Authenticated as UID: ${uid}`);
    onProgress('auth_success', `✅ Connected as UID: ${uid}`, 0);
    
    // Step 1.5: Get Account IDs (IMPORTANT FIX!)
    onProgress('accounts', 'Looking up account codes...', 0);
    
    const tdsAccountId = await getAccountIdByCode(
      company.odooUrl,
      company.odooDatabase,
      uid,
      company.odooPassword,
      '231110'  // TDS Receivable account code
    );
    
    const debtorAccountId = await getAccountIdByCode(
      company.odooUrl,
      company.odooDatabase,
      uid,
      company.odooPassword,
      '251000'  // Debtors account code
    );
    
    console.log(`[Odoo Sync] TDS Account ID: ${tdsAccountId}, Debtor Account ID: ${debtorAccountId}`);
    
    // Step 2: Search TDS Receivable Lines - NOW USING account_id INSTEAD OF account_id.code
    onProgress('search', 'Searching TDS records...', 0);
    
    const domain = [
      ['account_id', '=', tdsAccountId],  // ← FIXED: Use account_id integer, not code
      ['date', '>=', fyStart],
      ['date', '<=', fyEnd],
      ['debit', '>', 0]
    ];
    
    console.log(`[Odoo Sync] Search domain:`, domain);
    
    const tdsLineIds = await searchOdoo(
      company.odooUrl,
      company.odooDatabase,
      uid,
      company.odooPassword,
      'account.move.line',
      domain
    );
    
    console.log(`[Odoo Sync] Found ${tdsLineIds.length} TDS line IDs:`, tdsLineIds.slice(0, 10));
    onProgress('search_complete', `Found ${tdsLineIds.length} TDS records`, tdsLineIds.length);
    
    if (tdsLineIds.length === 0) {
      console.warn('[Odoo Sync] No TDS records found');
      return [];
    }
    
    // Step 3: Read TDS Line Details
    onProgress('read', 'Fetching record details...', tdsLineIds.length);
    
    const fields = [
      'date',
      'move_id',
      'partner_id',
      'company_id',
      'name',
      'account_id',
      'debit',
      'credit',
      'balance'
    ];
    
    const tdsLines = await readOdoo(
      company.odooUrl,
      company.odooDatabase,
      uid,
      company.odooPassword,
      'account.move.line',
      tdsLineIds,
      fields
    );
    
    console.log(`[Odoo Sync] Read ${tdsLines.length} TDS lines (first 2):`, tdsLines.slice(0, 2));
    onProgress('read_complete', 'Processing records...', tdsLines.length);
    
    // Step 4: Filter — primary match by invoice prefix; fall back to
    //                  company_id for manual JVs (e.g. Excess TDS adjustments
    //                  that have no SWB/SHR/etc. invoice reference).
    const companyKeywords = getOdooCompanyKeywords(company.name);
    let manualJvCount = 0;
    
    const filteredLines = tdsLines.filter(line => {
      const invoiceRef = line.name || '';
      const prefix = invoiceRef.split('/')[0].toUpperCase();
      
      // Pass 1: known invoice prefix → regular Books entry.
      if (companyPrefixes.includes(prefix)) {
        line._isManualJV = false;
        return true;
      }
      
      // Pass 2: no recognised prefix → may still belong to us if the line's
      // company_id matches. Used for manual JVs ("Excess TDS", adjustments).
      if (companyKeywords.length > 0) {
        const odooCompanyName = (Array.isArray(line.company_id) ? line.company_id[1] : '') || '';
        const odooCompanyLower = odooCompanyName.toLowerCase();
        const companyMatches = companyKeywords.some(kw => odooCompanyLower.includes(kw));
        
        if (companyMatches) {
          line._isManualJV = true;
          manualJvCount++;
          const partnerName = (Array.isArray(line.partner_id) ? line.partner_id[1] : '') || '(no partner)';
          console.log(`[Odoo Sync] ✓ Included manual JV: "${invoiceRef || '(unlabelled)'}" · ${partnerName} · debit ₹${line.debit}`);
          return true;
        }
      }
      
      console.log(`[Odoo Sync] Filtering out: ${invoiceRef} (prefix: ${prefix})`);
      return false;
    });
    
    console.log(`[Odoo Sync] Filtered to ${filteredLines.length} records for ${company.name}${manualJvCount > 0 ? ` (incl. ${manualJvCount} manual JV)` : ''}`);
    if (manualJvCount > 0) {
      onProgress('manual_jv', `Included ${manualJvCount} manual JV line(s) (excess TDS / adjustments)`, manualJvCount);
    }
    onProgress('filter_complete', `Filtered to ${filteredLines.length} records for ${company.name}`, filteredLines.length);
    
    if (filteredLines.length === 0) {
      console.warn(`[Odoo Sync] No records matched company prefixes:`, companyPrefixes);
      return [];
    }
    
    // Step 5: Get Invoice Amounts from Debtors Lines
    onProgress('amounts', 'Fetching invoice amounts...', filteredLines.length);
    
    const enrichedData = [];
    
    for (let i = 0; i < filteredLines.length; i++) {
      const line = filteredLines[i];
      
      // Manual JVs may have no partner set. Skip the debtor lookup — there's
      // nothing to match in that case, invoiceAmount stays 0.
      const hasPartner = Array.isArray(line.partner_id) && line.partner_id.length >= 1;
      if (!hasPartner) {
        enrichedData.push({ ...line, invoiceAmount: 0 });
        if ((i + 1) % 10 === 0 || i === filteredLines.length - 1) {
          onProgress('amounts_progress', `Processing ${i + 1} of ${filteredLines.length}...`, i + 1);
        }
        continue;
      }
      
      // FIXED: Use debtorAccountId instead of code
      const debtorDomain = [
        ['move_id', '=', line.move_id[0]],
        ['account_id', '=', debtorAccountId],  // ← FIXED: Use account_id integer
        ['partner_id', '=', line.partner_id[0]]
      ];
      
      try {
        const debtorIds = await searchOdoo(
          company.odooUrl,
          company.odooDatabase,
          uid,
          company.odooPassword,
          'account.move.line',
          debtorDomain
        );
        
        let invoiceAmount = 0;
        
        if (debtorIds.length > 0) {
          const debtorLines = await readOdoo(
            company.odooUrl,
            company.odooDatabase,
            uid,
            company.odooPassword,
            'account.move.line',
            [debtorIds[0]],
            ['credit']
          );
          
          invoiceAmount = debtorLines[0]?.credit || 0;
          console.log(`[Odoo Sync] Invoice ${line.name}: TDS=${line.debit}, Amount=${invoiceAmount}`);
        } else {
          console.warn(`[Odoo Sync] No debtor line found for ${line.name}`);
        }
        
        enrichedData.push({
          ...line,
          invoiceAmount
        });
        
        if ((i + 1) % 10 === 0 || i === filteredLines.length - 1) {
          onProgress('amounts_progress', `Processing ${i + 1} of ${filteredLines.length}...`, i + 1);
        }
        
      } catch (error) {
        console.error(`[Odoo Sync] Failed to get invoice amount for line ${line.id}:`, error);
        enrichedData.push({
          ...line,
          invoiceAmount: 0
        });
      }
    }
    
    // Step 6: Transform to Books Format
    onProgress('transform', 'Transforming data...', enrichedData.length);

    console.log(`[Odoo Sync] Transforming ${enrichedData.length} records...`);

    // Step 6a: Detect reversed entries before final transformation, so each
    // record can carry the reversed flag. Single-pass, two XML-RPC calls max
    // (first-level + counter-reversal). Falls back safely on any error.
    onProgress('reversals', 'Checking for reversed entries...', 0);
    const uniqueMoveIds = [...new Set(
      enrichedData
        .map(l => Array.isArray(l.move_id) ? l.move_id[0] : null)
        .filter(Boolean)
    )];
    const reversalMap = await detectReversedMoves(
      company.odooUrl,
      company.odooDatabase,
      uid,
      company.odooPassword,
      uniqueMoveIds
    );
    const reversedCount = Object.keys(reversalMap).length;
    if (reversedCount > 0) {
      console.log(`[Odoo Sync] ↶ Detected ${reversedCount} reversed move(s):`, reversalMap);
      onProgress('reversals_complete', `Detected ${reversedCount} reversed entr${reversedCount===1?'y':'ies'}`, reversedCount);
    } else {
      console.log('[Odoo Sync] No reversed entries detected');
    }

    const booksData = enrichedData.map((line, idx) => {
      try {
        const journalEntry = (Array.isArray(line.move_id) ? line.move_id[1] : '') || '';
        const reversalRef = journalEntry ? reversalMap[journalEntry] : undefined;
        const partnerName = (Array.isArray(line.partner_id) ? line.partner_id[1] : '') || '';
        const odooCompany = (Array.isArray(line.company_id) ? line.company_id[1] : '') || '';

        const record = {
          deductorName: partnerName,
          tan: '',
          amount: line.invoiceAmount,
          tdsDeducted: line.debit || 0,
          section: '',
          date: line.date || '',
          invoiceNo: line.name || '',
          quarter: calculateQuarter(line.date),
          source: 'Odoo ERP',
          journalEntry,
          odooCompany,
          // NEW — Excess TDS / manual JVs (no recognised invoice prefix,
          // included via company_id fallback). Frontend renders an "EXCESS
          // TDS" badge so these are not mistaken for ordinary invoice rows.
          ...(line._isManualJV
            ? {
                manualJV: true,
                manualJvLabel: (line.name && line.name.trim()) || 'Manual JV',
              }
            : {}),
          // NEW — set only when Odoo's "Reverse Entry" wizard has been used
          // and the reversal is posted. Frontend uses these to auto-populate
          // its reversedEntries map (see reversal-aware reconciliation).
          ...(reversalRef ? { reversed: true, reversal_ref: reversalRef } : {})
        };

        if (idx === 0) {
          console.log('[Odoo Sync] Sample transformed record:', record);
        }

        return record;
      } catch (error) {
        console.error(`[Odoo Sync] Error transforming record:`, error, line);
        throw error;
      }
    });
    
    const reversedInData = booksData.filter(r => r.reversed).length;
    const manualJvInData = booksData.filter(r => r.manualJV).length;
    const extras = [];
    if (reversedInData > 0) extras.push(`${reversedInData} reversed`);
    if (manualJvInData > 0) extras.push(`${manualJvInData} manual JV`);
    const extrasStr = extras.length > 0 ? ` (${extras.join(', ')})` : '';
    console.log(`[Odoo Sync] ✅ Successfully synced ${booksData.length} records${extrasStr}`);
    console.log('[Odoo Sync] First 3 records:', booksData.slice(0, 3));

    onProgress('complete', `✅ Synced ${booksData.length} records${extrasStr}`, booksData.length);

    return booksData;
    
  } catch (error) {
    console.error('[Odoo Sync] ❌ Fatal error:', error);
    onProgress('error', `❌ Error: ${error.message}`, 0);
    throw error;
  }
}

export default {
  authenticateOdoo,
  executeOdooMethod,
  searchOdoo,
  readOdoo,
  searchReadOdoo,
  syncTDSFromOdoo,
  identifyCompanyFromInvoice,
  getCompanyPrefixes,
  getOdooCompanyKeywords,
  getFYDates,
  calculateQuarter
};
