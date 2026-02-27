"""
Debug script: Test REF extraction from the sample FedEx PDF
"""
import os
import re
import json
import base64
import fitz
from PIL import Image
from io import BytesIO
import openai

pdf_path = r"c:\Users\yves.lai\Downloads\F11000049241.pdf"
pdf_bytes = open(pdf_path, "rb").read()

# Setup OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY")
client = openai.Client(api_key=os.getenv("OPENAI_API_KEY"))
model = os.getenv("OPENAI_MODEL", "gpt-4o")

# Convert PDF to image (same logic as ocr_engine.py)
doc = fitz.open(stream=pdf_bytes, filetype="pdf")
page = doc[0]
page.set_rotation(0)
pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

# Save for inspection
img.save("debug_fedex_sample.jpg")
print("✅ PDF rendered and saved as debug_fedex_sample.jpg")

# Send to GPT-4o for extraction
buf = BytesIO()
img.save(buf, format="JPEG")
b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

FEDEX_SHIPPING_PROMPT = """
Task: Extract sender and receiver info from this FedEx shipping label.
IMPORTANT: Only extract text that is ACTUALLY PRINTED on the label.
If a field is not clearly visible or does not exist, return an empty string "".
Do NOT guess or invent any data.

Look for:
- "FROM" section: sender name, phone number, and any client/account code
- "TO" section: receiver name, full street address, and postal/ZIP code
- "REF", "INV", or "PO" fields: Extract ALL reference numbers you find. 
  If multiple fields exist (e.g., both REF and INV), return ONLY the value from the "REF:" field if it exists.
  Otherwise return from "INV:" or "PO:" in that priority order.

Response JSON (every value must be a string):
{
  "sender": {
    "name": "exact name from the FROM section",
    "phone": "phone number near FROM, or empty",
    "client_id": "account code or alias printed below/near the sender name, or empty"
  },
  "receiver": {
    "name": "exact name from the TO section",
    "address": "full street address from TO section",
    "postal_code": "ZIP or postal code e.g. V6X 1Z7"
  },
  "reference_number": "THE VALUE AFTER 'REF:' if it exists, else INV:, else PO:. Include the complete number/text only, no label prefix.",
  "ref_field": "The complete 'REF: xxxxx' as printed, if visible",
  "inv_field": "The complete 'INV: xxxxx' as printed, if visible",
  "po_field": "The complete 'PO: xxxxx' as printed, if visible"
}
"""

messages = [{"role": "user", "content": [
    {"type": "text", "text": FEDEX_SHIPPING_PROMPT},
    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
]}]

response = client.chat.completions.create(
    model=model, messages=messages, temperature=0
)
content = re.sub(r"```json|```", "", response.choices[0].message.content.strip()).strip()
extracted = json.loads(content)

print("\n" + "=" * 60)
print("RAW EXTRACTED DATA FROM GPT-4o:")
print("=" * 60)
print(json.dumps(extracted, indent=2, ensure_ascii=False))

# Show all reference fields
print("\n" + "=" * 60)
print("ALL REFERENCE FIELDS FOUND:")
print("=" * 60)
if extracted.get("ref_field"):
    print(f"  REF:  {extracted['ref_field']}")
if extracted.get("inv_field"):
    print(f"  INV:  {extracted['inv_field']}")
if extracted.get("po_field"):
    print(f"  PO:   {extracted['po_field']}")
print(f"  Selected: {extracted.get('reference_number')}")

# Now apply our cleaning logic
raw_ref = (extracted.get("reference_number") or "").strip()
print("\n" + "=" * 60)
print("STEP 1: Raw Reference Number (already prioritized by GPT-4o):")
print(f"  {raw_ref}")

if raw_ref:
    # Only remove trailing -digit suffix
    clean_ref = re.sub(r'-\d+$', '', raw_ref).strip()
    print("\nSTEP 2: After removing suffix (-1, -2, etc.):")
    print(f"  {clean_ref}")
    
    print("\n" + "=" * 60)
    print("FINAL RESULT:")
    print(f"  📌 Reference Number to search in sheet: {clean_ref}")
    print("=" * 60)
else:
    print("\n⚠️ No reference number found!")
