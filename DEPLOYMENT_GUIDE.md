# Dynamic Monday Board Sender Mapping - Deployment Guide

## 📋 Overview

This update replaces hard-coded sender name mappings with dynamic Monday board group lookup for improved flexibility and maintenance.

### Changes Made

**🐍 Python tracking-bot:**
- ✅ Added `utils/sender_mapping.py` - New sender mapping service
- ✅ Enhanced `utils/dynamic_names.py` - Added sender group mapping 
- ✅ Updated `handlers/handlers.py` - New dynamic mapping functions

**📱 Google Apps Script:**
- ✅ Enhanced `3_Monday_Integration.js` - Added dynamic mapping functions
- ✅ Replaced hard-coded sender check with `shouldRouteToAbbBoard()`

## 🎯 Mapping Logic

### Before (Hard-coded):
```javascript
// OLD: Hard-coded sender list
if (['MM','KT','AD','Yves Lai'].includes(sender)) {
  pushToABB(sheet, row, tracking, sizeCm, weightKg, boxId, prevStatus);
}
```

### After (Dynamic):
```javascript  
// NEW: Dynamic Monday board group lookup
if (shouldRouteToAbbBoard(sender)) {
  pushToABB(sheet, row, tracking, sizeCm, weightKg, boxId, prevStatus);
}
```

### Mapping Rules:
- **MM** → Group title contains "(MM)"
- **AD** → Group title contains "(AD)"  
- **KT** → Group title contains "(KT)"
- **Yves Lai** → Group title contains "Ace" or "SQ"
- **"Yves MM Lai"** → Detects "MM" → Maps to "(MM)" group

## 🚀 Deployment Steps

### Step 1: Deploy Python Changes

```bash
# 1. Commit changes to git
cd /path/to/tracking-bot
git add .
git commit -m "feat: implement dynamic Monday board sender mapping

- Add sender mapping service with group pattern matching
- Replace hard-coded sender lists with dynamic lookup  
- Support MM/AD/KT abbreviations and Yves variations
- Add caching and fallback mechanisms"

# 2. Deploy to Heroku
git push heroku main

# 3. Monitor deployment
heroku logs --tail
```

### Step 2: Deploy Google Apps Script Changes

```bash
# 1. Navigate to Google Apps Script project
cd "/path/to/google sheet/打包資料表更新"

# 2. Use clasp to deploy (if configured)
clasp push
clasp deploy

# Or manually copy changes to Apps Script editor
```

### Step 3: Validate Monday Board Structure

**Required Board:** `7745917861 - 報關人頭和收件人資料`

**Required Groups:**
```
✅ Group containing "(MM)" - for MM senders
✅ Group containing "(AD)" - for AD senders  
✅ Group containing "(KT)" - for KT senders
✅ Group containing "Ace" or "SQ" - for Yves senders
```

**Verification Script:**
```python
# Run test script to validate mapping
python test_sender_mapping.py
```

### Step 4: Test Functionality

**Test Cases:**
1. **Direct abbreviations:** `MM`, `AD`, `KT` → Should map to respective groups
2. **Name with abbreviations:** `"Yves MM Lai"` → Should map to `(MM)` group
3. **Yves variations:** `"Yves Lai"`, `"Yves"` → Should map to Ace/SQ group
4. **No mapping:** `"Random Name"` → Should return null/fallback

**Google Apps Script Test:**
```javascript
function testSenderMapping() {
  const testCases = [
    ['MM', 'Should map to (MM) group'],
    ['Yves MM Lai', 'Should detect MM and map to (MM) group'],
    ['Yves Lai', 'Should map to Ace/SQ group']
  ];
  
  for (const [senderName, expected] of testCases) {
    const result = mapSenderToMondayGroup(senderName);
    const groupTitle = result ? result.title : 'null';
    Logger.log(`'${senderName}' → ${groupTitle} (${expected})`);
  }
}
```

## 🔄 Rollback Plan

If issues occur, you can quickly rollback:

### Python Rollback:
```bash
# Revert to previous commit
git revert HEAD
git push heroku main
```

### Google Apps Script Rollback:
```javascript
// Restore old hard-coded logic
function processSyncWithMonday(sheet, row, e, prevStatus) {
  // ...
  // Restore this line:
  else if (['MM','KT','AD','Yves Lai'].includes(sender)) {
    pushToABB(sheet, row, tracking, sizeCm, weightKg, boxId, prevStatus);
  }
  // ...
}
```

## 📊 Monitoring

### Key Metrics:
- **Mapping Success Rate:** Track successful sender → group mappings
- **Fallback Usage:** Monitor when hard-coded fallbacks are used
- **API Performance:** Monday board query response times

### Log Monitoring:
```bash
# Monitor Heroku logs for mapping activity
heroku logs --tail | grep "SenderMapping"

# Look for these log patterns:
# ✅ "[SenderMapping] Found group 'Group(MM)' containing pattern '(MM)'"
# ✅ "[SenderMapping] Retrieved 5 names from SQ/Ace group"
# ⚠️  "[SenderMapping] No group mapping found for sender: 'Unknown Name'"
```

## 🐛 Troubleshooting

### Common Issues:

**1. "No Monday API token found"**
```bash
# Check Heroku config vars
heroku config:get MONDAY_TOKEN
heroku config:get MONDAY_API_TOKEN

# Set if missing
heroku config:set MONDAY_TOKEN=your_token_here
```

**2. "No group mapping found"**
- Verify board 7745917861 has groups with expected patterns
- Check group titles contain exact patterns: "(MM)", "(AD)", "(KT)"
- Ensure Ace/SQ group exists with "Ace" or "SQ" in title

**3. "Dynamic names manager not available"**
- Ensure `init_dynamic_names_manager()` is called during app startup
- Check if Monday service is properly initialized

**4. Google Apps Script errors****
- Verify `MONDAY_TOKEN` is set in Apps Script
- Check Apps Script execution transcript for detailed errors
- Ensure proper authorization for Monday API calls

## ✅ Success Criteria

- [ ] All test cases pass in both Python and Google Apps Script
- [ ] Sender mapping logs show successful group lookups
- [ ] No impact on existing shipment processing workflow
- [ ] Fallback mechanisms work when Monday API unavailable
- [ ] Performance remains acceptable (< 2s for mapping operations)

## 📞 Support Contacts

- **Primary:** Yves Lai - System owner
- **Technical:** Development team
- **Escalation:** Monday.com support (if API issues)

---

**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Version:** 1.0.0
**Status:** Ready for deployment