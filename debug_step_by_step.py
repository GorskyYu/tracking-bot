"""
Step-by-step debug: simulate EXACT production pipeline for F11000029722.pdf
Each step saves an image so we can visually verify.
"""
import fitz
from PIL import Image
from pyzbar.pyzbar import decode, ZBarSymbol
import base64, json, os, re
from io import BytesIO
import openai

PDF_PATH = r"c:\Users\yves.lai\Downloads\F11000029722.pdf"
pdf_bytes = open(PDF_PATH, "rb").read()

print("=" * 60)
print("STEP 1: Open PDF and inspect metadata")
print("=" * 60)
doc = fitz.open(stream=pdf_bytes, filetype="pdf")
page = doc[0]
print(f"  MediaBox: {page.rect}")
print(f"  /Rotate : {page.rotation}")
print(f"  CropBox : {page.cropbox}")
print()

print("=" * 60)
print("STEP 2: Render pixmap (PyMuPDF native, no manual rotation)")
print("=" * 60)
pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
print(f"  Rendered size: {img.size} (W x H)")
img.save("step2_native_render.jpg")
print(f"  Saved: step2_native_render.jpg")
print(f"  Is portrait? {img.height > img.width}")
print()

print("=" * 60)
print("STEP 3: Barcode detection on this image")
print("=" * 60)
decoded = decode(img, symbols=[ZBarSymbol.CODE128, ZBarSymbol.CODE39, ZBarSymbol.I25])
print(f"  Barcodes found: {len(decoded)}")
for i, obj in enumerate(decoded):
    r = obj.rect
    print(f"  [{i}] Data={obj.data.decode('utf-8')!r}  Rect(x={r.left}, y={r.top}, w={r.width}, h={r.height})  Aspect={'VERTICAL' if r.height > r.width else 'HORIZONTAL'}")

if decoded:
    main_bc = max(decoded, key=lambda o: o.rect.width * o.rect.height)
    w, h = main_bc.rect.width, main_bc.rect.height
    print(f"\n  Main barcode: W={w}, H={h}")
    if h > w:
        print(f"  -> VERTICAL barcode detected, would ROTATE -90")
        img_rotated = img.rotate(-90, expand=True)
    else:
        print(f"  -> HORIZONTAL barcode, keeping as-is")
        img_rotated = img
else:
    print(f"  -> No barcode found, keeping as-is")
    img_rotated = img

img_rotated.save("step3_after_barcode_rotation.jpg")
print(f"  Saved: step3_after_barcode_rotation.jpg  Size={img_rotated.size}")
print()

print("=" * 60)
print("STEP 4: What the GPT API would receive (this is the final image)")
print("=" * 60)
print(f"  Final image size: {img_rotated.size}")
print(f"  Is portrait? {img_rotated.height > img_rotated.width}")
img_rotated.save("step4_sent_to_gpt.jpg")
print(f"  Saved: step4_sent_to_gpt.jpg")
print()

print("=" * 60)
print("STEP 5: Actually call GPT-4o with this image")
print("=" * 60)

FEDEX_SHIPPING_PROMPT = """
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

client = openai.Client(api_key=os.getenv("OPENAI_API_KEY"))

buf = BytesIO()
img_rotated.save(buf, format="JPEG")
b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

messages = [{"role": "user", "content": [
    {"type": "text", "text": FEDEX_SHIPPING_PROMPT},
    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
]}]

response = client.chat.completions.create(model="gpt-4o", messages=messages, temperature=0)
raw = response.choices[0].message.content.strip()
print(f"  Raw GPT response:\n{raw}")
print()

content = re.sub(r"```json|```", "", raw).strip()
try:
    result = json.loads(content)
    print(f"  Parsed JSON:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
except:
    print(f"  FAILED to parse JSON!")
