import os
import io
import json
import base64
import re
import logging
import fitz  # PyMuPDF: Converts PDF pages to images
import openai
from PIL import Image # Pillow: For rotation and precise cropping
from pyzbar.pyzbar import decode, ZBarSymbol
from io import BytesIO

log = logging.getLogger(__name__)

# --- SECTION 1: THE PROMPTS (THE BRAIN) ---
# We use separate, strict instructions for each carrier to prevent data mix-ups.

OCR_SHIPPING_PROMPT = """
Task: Extract sender (name, phone, client ID, address), 
receiver (name, address, postal code), 
and Reference No.1 from this shipping ticket.

Response Format: 
{
  "sender": {
    "name": "", 
    "phone": "", 
    "client_id": "", 
    "address": ""
  }, 
  "receiver": {
    "name": "",
    "address": "",
    "postal_code": ""
  }, 
  "reference_number": ""
}
"""

# STRICT FEDEX PROMPT: Expanded to capture Receiver Address for routing.
FEDEX_SHIPPING_PROMPT = """
Task: Extract sender info and receiver info from this FedEx label.
Focus on the "TO" section for receiver address, and "FROM" for sender.

Response JSON:
{
  "sender": {
    "name": "string (From name)",
    "client_id": "string (lines under name)"
  },
  "receiver": {
    "name": "string (To name)",
    "address": "string (Full address line)",
    "postal_code": "string (ZIP/Postal code e.g. V6X 1Z7)"
  },
  "reference_number": "string (Ref #, PO #, Invoice #)"
}
"""

class OCRAgent:
    def __init__(self):
        """Initializes the worker with your OpenAI API keys."""
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.client = openai.Client(api_key=self.api_key)

    # --- SECTION 2: IMAGE PRE-PROCESSING (THE EYES) ---
    # This section handles visual cleanup like rotation and physical barcode scanning.

    def pdf_to_images(self, pdf_bytes):
        images = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for page in doc:
            # Let PyMuPDF respect the page's native /Rotate attribute
            # (do NOT override with set_rotation — invalid values like -45
            #  corrupt the rendering and produce unreadable images for GPT)
            pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

            # Ensure portrait orientation for shipping labels
            if img.width > img.height:
                img = img.rotate(90, expand=True)

            images.append(img)
        return images

    def get_barcode(self, image):
        """Reads physical barcodes using traditional pyzbar scanning."""
        objs = decode(image, symbols=[ZBarSymbol.CODE128, ZBarSymbol.CODE39, ZBarSymbol.I25])
        for obj in objs:
            data = obj.data.decode("utf-8").replace(" ", "")
            # If we find a '1Z', we know it's a UPS label.
            if data.startswith("1Z"): return data
        return objs[0].data.decode("utf-8") if objs else None

    # --- SECTION 3: AI INTERFACE (THE MAGNIFYING GLASS) ---
    # This section sends specific snippets (crops) to the AI to prevent confusion.

    def extract_from_image(self, image, prompt, crop_area=None, debug_name=None):
        """Sends a specific crop to OpenAI and optionally saves it for debugging."""
        target_img = image
        if crop_area:
            width, height = image.size
            left, top, right, bottom = crop_area
            target_img = image.crop((left * width, top * height, right * width, bottom * height))

        # DEBUG: Save the crop so you can see what the AI sees.
        if debug_name:
            target_img.save(f"debug_{debug_name}.jpg")

        buf = BytesIO()
        target_img.save(buf, format="JPEG")
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        
        messages = [{"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
        ]}]
        
        response = self.client.chat.completions.create(model=self.model, messages=messages)
        content = re.sub(r"```json|```", "", response.choices[0].message.content.strip()).strip()
        try:
            return json.loads(content)
        except:
            return {"_raw": content}

    # --- SECTION 4: THE MASTER WORKFLOW (THE DECISION MAKER) ---
    # This logic coordinates between carrier detection and multi-pass extraction.

    def process_shipment_pdf(self, pdf_bytes):
        images = self.pdf_to_images(pdf_bytes)
        if not images: return None
        img = images[0]

        # 1. UPS PROTECTION
        bc_data = self.get_barcode(img)
        is_ups = bc_data and bc_data.startswith("1Z")
        
        if is_ups:
            data = self.extract_from_image(img, OCR_SHIPPING_PROMPT)
            data["carrier"] = "UPS"
        else:
            # 2. FEDEX SINGLE-ZONE SCAN:
            # We capture the Sender, Client ID, and Reference in ONE box.
            log.info("[OCR] FedEx detected. Running Single-Zone extraction.")
            
            # Expanded Area to capture 'TO' address (Top 60%)
            # (Left: 0.0, Top: 0.0, Right: 1.0, Bottom: 0.60)
            res = self.extract_from_image(img, FEDEX_SHIPPING_PROMPT, 
                                          crop_area=(0.0, 0.0, 1.0, 0.60), 
                                          debug_name="fedex_single_area_scan")

            data = {
                "sender": res.get("sender", {}),
                "receiver": res.get("receiver", {}), # Use extracted receiver info (Postal Code!)
                "carrier": "FedEx",
                "reference_number": res.get("reference_number", "")
            }

            # --- Fedex Reference Number清洗邏輯 ---
            raw_ref = data.get("reference_number", "")
            if raw_ref:
                # 使用 Regex 正則表達式移除結尾的 -1, -2 等後綴
                # r'-\d+$' 表示匹配字串結尾的「橫槓+數字」
                clean_ref = re.sub(r'-\d+$', '', raw_ref).strip()
                data["reference_number"] = clean_ref
                log.info(f"[OCR CLEAN] Original: {raw_ref} -> Cleaned: {clean_ref}")

        # 3. TRACKING CONSOLIDATION
        all_tracking = []
        for p_img in images:
            bc = self.get_barcode(p_img)
            if bc: 
                clean_bc = bc.replace(" ", "")
                if not clean_bc.startswith("1Z") and len(clean_bc) > 12:
                    clean_bc = clean_bc[-12:]
                all_tracking.append(clean_bc)
        
        data["all_tracking_numbers"] = sorted(list(set(all_tracking)))
        return data