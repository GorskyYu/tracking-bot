// ─── Configuration ───────────────────────────────────────────────────────────
// REPLACE THIS with your actual Heroku app URL
const WEBHOOK_URL = "https://YOUR-HEROKU-APP.herokuapp.com/ace-trigger";

// REPLACE THIS with your shared secret (must match ACE_TRIGGER_SECRET in Heroku)
const TRIGGER_SECRET = "your-secret-here"; 

/**
 * Triggered on file open. Adds a custom menu to the spreadsheet.
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('ACE 出貨')
    .addItem('檢查紅色分隔線並推送 (Check Red Row)', 'checkRedRowAndTrigger')
    .addItem('直接推送今日出貨 (Force Push)', 'triggerWebhook')
    .addToUi();
}

/**
 * Main logic to check for the red row.
 * THIS FUNCTION MUST BE SET UP AS AN INSTALLABLE "ON CHANGE" TRIGGER.
 * 
 * Apps Script Triggers Setup:
 * 1. Click the Clock icon (Triggers) in the left sidebar.
 * 2. Click "Add Trigger".
 * 3. Choose function: `checkRedRowAndTrigger`.
 * 4. Select event source: `From spreadsheet`.
 * 5. Select event type: `On change`.
 * 
 * Why "On change"? 
 * Because "On edit" does NOT fire when you only change the background color (format).
 * "On change" detects format changes.
 */
function checkRedRowAndTrigger(e) {
  // Optional: Check if the change type is relevant (e.g., FORMAT or OTHER)
  // if (e && e.changeType !== 'FORMAT' && e.changeType !== 'OTHER') return;

  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  // Get all data to find the empty row (assumes data starts row 2, headers row 1)
  const lastRow = sheet.getLastRow();
  // If sheet is empty or only header, nothing to do
  if (lastRow < 1) return;

  // We'll scan from row 2 downwards. 
  // Note: getDataRange() might include empty rows if they have formatting.
  // Ideally, rely on a robust way to find the "first empty row" used for separation.
  // Here we scan row by row until we find a completely empty one.
  
  const range = sheet.getDataRange();
  const values = range.getValues();
  
  // 1-based index for the first empty row
  let firstEmptyRowIndex = -1;
  
  // values is 0-indexed. Row 1 is values[0].
  for (let i = 1; i < values.length; i++) {
    const rowData = values[i];
    // Check if the row is entirely empty
    const isEmpty = rowData.every(cell => cell === "" || cell === null);
    if (isEmpty) {
      firstEmptyRowIndex = i + 1; // Convert 0-based array index to 1-based row index
      break;
    }
  }

  // If we couldn't find an empty row in the used range, maybe the next one after lastRow is the target?
  // But usually users leave an empty row as a separator between days. 
  // If no empty row found in data range, we stop to avoid false positives.
  if (firstEmptyRowIndex === -1) {
    console.log("No empty separator row found.");
    return;
  }

  // Check the background color of this row
  // We only check the first cell or the whole row. Let's check the first 10 columns to be safe.
  const bgColors = sheet.getRange(firstEmptyRowIndex, 1, 1, 10).getBackgrounds()[0];
  
  // Define "Red". Google Sheets might return hex or color names.
  // Common reds: #ff0000 (red), #ea4335 (google red), #e06666 (light red)
  const redSet = new Set(["#ff0000", "#ea4335", "#cc0000", "#e06666", "#f4cccc", "red"]);
  
  const isRed = bgColors.some(color => redSet.has(color.toLowerCase()));

  if (isRed) {
    console.log(`Row ${firstEmptyRowIndex} is RED. Triggering Webhook...`);
    triggerWebhook();
    
    // OPTIONAL: Reset color to white/transparent to prevent re-triggering?
    // sheet.getRange(firstEmptyRowIndex, 1, 1, sheet.getLastColumn()).setBackground(null);
  } else {
    console.log(`Row ${firstEmptyRowIndex} found but is NOT red.`);
  }
}

/**
 * Sends the webhook request to the Python bot.
 */
function triggerWebhook() {
  const payload = {
    secret: TRIGGER_SECRET,
    timestamp: new Date().toISOString()
  };
  
  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };
  
  try {
    const response = UrlFetchApp.fetch(WEBHOOK_URL, options);
    const code = response.getResponseCode();
    const text = response.getContentText();
    console.log(`Webhook sent. Code: ${code}, Response: ${text}`);
    
    if (code !== 200) {
      SpreadsheetApp.getUi().alert(`Push Failed: ${code}\n${text}`);
    } else {
      // Optional: Show a toast notification
      // SpreadsheetApp.getActiveSpreadsheet().toast("推播成功", "Success");
    }
  } catch (error) {
    console.error("Webhook Error: " + error.toString());
    SpreadsheetApp.getUi().alert("Webhook Error:\n" + error.toString());
  }
}
