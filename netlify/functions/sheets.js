const { google } = require('googleapis');

const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const SHEET_TAB = 'US_Opportunities';

// Authenticate using service account credentials from environment variable
async function getSheets() {
  const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const authClient = await auth.getClient();
  return google.sheets({ version: 'v4', auth: authClient });
}

// Column headers — must match CRM data structure exactly
const HEADERS = [
  'ID', 'Company', 'Country', 'Owner', 'Stage',
  'Contact Name', 'Contact Email', 'Contact Phone', 'Products',
  'Customer Total Volume', 'Palletech Capture %', 'Palletech Volume',
  'Unit Price USD', 'Deal Value USD', 'Industry', 'Competitor Displaced',
  'Reason for Change', 'Next Step', 'Next Step Date', 'Expected Close Date',
  'Loss Reason', 'Loss Notes', 'Re-Engage Date', 'Notes',
  'Margin %', 'Stage Changed At', 'Created At', 'Updated At', 'Created By'
];

// Map deal object to row array
function dealToRow(deal) {
  return [
    deal.id || '',
    deal.company || '',
    deal.country || '',
    deal.owner || '',
    deal.stage || '',
    deal.contactName || '',
    deal.contactEmail || '',
    deal.contactPhone || '',
    deal.products || '',
    deal.customerTotalVolume || '',
    deal.palletchCapturePct || '',
    deal.palletchVolume || '',
    deal.unitPriceUSD || '',
    deal.dealValueUSD || '',
    deal.industry || '',
    deal.competitorDisplaced || '',
    deal.reasonForChange || '',
    deal.nextStep || '',
    deal.nextStepDate || '',
    deal.expectedCloseDate || '',
    deal.lossReason || '',
    deal.lossNotes || '',
    deal.reEngageDate || '',
    deal.notes || '',
    deal.marginPct || '',
    deal.stageChangedAt || '',
    deal.createdAt || '',
    deal.updatedAt || '',
    deal.createdBy || ''
  ];
}

// Map row array back to deal object
function rowToDeal(row) {
  return {
    id: row[0] || '',
    company: row[1] || '',
    country: row[2] || '',
    owner: row[3] || '',
    stage: row[4] || '',
    contactName: row[5] || '',
    contactEmail: row[6] || '',
    contactPhone: row[7] || '',
    products: row[8] || '',
    customerTotalVolume: row[9] || '',
    palletchCapturePct: row[10] || '',
    palletchVolume: row[11] || '',
    unitPriceUSD: row[12] || '',
    dealValueUSD: row[13] || '',
    industry: row[14] || '',
    competitorDisplaced: row[15] || '',
    reasonForChange: row[16] || '',
    nextStep: row[17] || '',
    nextStepDate: row[18] || '',
    expectedCloseDate: row[19] || '',
    lossReason: row[20] || '',
    lossNotes: row[21] || '',
    reEngageDate: row[22] || '',
    notes: row[23] || '',
    marginPct: row[24] || '',
    stageChangedAt: row[25] || '',
    createdAt: row[26] || '',
    updatedAt: row[27] || '',
    createdBy: row[28] || ''
  };
}

// Ensure the sheet tab exists with headers
async function ensureSheet(sheets) {
  try {
    // Try to read the sheet
    await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_TAB}!A1:A1`,
    });
  } catch (e) {
    // Sheet tab doesn't exist — create it
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title: SHEET_TAB } } }]
      }
    });
    // Add headers
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_TAB}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: [HEADERS] }
    });
  }
}

exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  // Handle preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const sheets = await getSheets();
    await ensureSheet(sheets);

    // GET — load all deals from Sheet
    if (event.httpMethod === 'GET') {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_TAB}!A2:AC`,
      });
      const rows = response.data.values || [];
      const deals = rows
        .filter(row => row[0] && row[1]) // must have ID and Company
        .map(rowToDeal);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, deals })
      };
    }

    // POST — save all deals to Sheet (full overwrite)
    if (event.httpMethod === 'POST') {
      const { deals } = JSON.parse(event.body);
      if (!Array.isArray(deals)) {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ success: false, error: 'Invalid deals array' })
        };
      }

      // Clear existing data (keep header row)
      await sheets.spreadsheets.values.clear({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_TAB}!A2:AC`,
      });

      // Write all deals if any exist
      if (deals.length > 0) {
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID,
          range: `${SHEET_TAB}!A2`,
          valueInputOption: 'RAW',
          requestBody: { values: deals.map(dealToRow) }
        });
      }

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, count: deals.length })
      };
    }

    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' })
    };

  } catch (err) {
    console.error('Sheets function error:', err);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ success: false, error: err.message })
    };
  }
};
