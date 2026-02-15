"""Final verify: set_rotation(0) + GPT-4o"""
import fitz, openai, os, json, re, base64
from PIL import Image
from io import BytesIO

pdf_bytes = open(r"c:\Users\yves.lai\Downloads\F11000029722.pdf", "rb").read()
doc = fitz.open(stream=pdf_bytes, filetype="pdf")
page = doc[0]
page.set_rotation(0)
pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
print(f"Image: {img.size}, landscape={img.width > img.height}")

PROMPT = """
Task: Extract sender and receiver info from this FedEx shipping label.
IMPORTANT: Only extract text that is ACTUALLY PRINTED on the label.
If a field is not clearly visible or does not exist, return an empty string "".
Do NOT guess or invent any data.

Look for:
- "FROM" section: sender name, phone number, and any client/account code
- "TO" section: receiver name, full street address, and postal/ZIP code
- "REF", "INV", or "PO" fields: reference number

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
  "reference_number": "REF, INV, or PO number if present"
}
"""

buf = BytesIO()
img.save(buf, format="JPEG")
b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

client = openai.Client(api_key=os.getenv("OPENAI_API_KEY"))
resp = client.chat.completions.create(
    model="gpt-4o", temperature=0,
    messages=[{"role": "user", "content": [
        {"type": "text", "text": PROMPT},
        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
    ]}]
)
raw = resp.choices[0].message.content.strip()
content = re.sub(r"```json|```", "", raw).strip()
result = json.loads(content)
print(json.dumps(result, indent=2, ensure_ascii=False))
