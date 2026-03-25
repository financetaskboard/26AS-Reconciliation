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
    
    // Step 4: Filter by company prefixes
    const filteredLines = tdsLines.filter(line => {
      const invoiceRef = line.name || '';
      const prefix = invoiceRef.split('/')[0].toUpperCase();
      const matches = companyPrefixes.includes(prefix);
      if (!matches) {
        console.log(`[Odoo Sync] Filtering out: ${invoiceRef} (prefix: ${prefix})`);
      }
      return matches;
    });
    
    console.log(`[Odoo Sync] Filtered to ${filteredLines.length} records for ${company.name}`);
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
    
    const booksData = enrichedData.map((line, idx) => {
      try {
        const record = {
          deductorName: line.partner_id[1] || '',
          tan: '',
          amount: line.invoiceAmount,
          tdsDeducted: line.debit || 0,
          section: '',
          date: line.date || '',
          invoiceNo: line.name || '',
          quarter: calculateQuarter(line.date),
          source: 'Odoo ERP',
          journalEntry: line.move_id[1] || '',
          odooCompany: line.company_id[1] || ''
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
    
    console.log(`[Odoo Sync] ✅ Successfully synced ${booksData.length} records`);
    console.log('[Odoo Sync] First 3 records:', booksData.slice(0, 3));
    
    onProgress('complete', `✅ Synced ${booksData.length} records`, booksData.length);
    
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
  getFYDates,
  calculateQuarter
};
