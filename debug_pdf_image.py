"""Debug script: render PDF → image to see what GPT actually receives."""
import fitz
from PIL import Image

pdf_path = r"c:\Users\yves.lai\Downloads\F11000029722.pdf"
pdf_bytes = open(pdf_path, "rb").read()

doc = fitz.open(stream=pdf_bytes, filetype="pdf")
page = doc[0]

print(f"Page MediaBox: {page.rect}")
print(f"Page /Rotate : {page.rotation}")

# --- Render like ocr_engine.py (after fix) ---
pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
print(f"Rendered size: {img.size}  (w x h)")

if img.width > img.height:
    img = img.rotate(90, expand=True)
    print(f"Auto-rotated to portrait: {img.size}")

img.save("debug_full_page.jpg")
print("Saved: debug_full_page.jpg")

# --- Crop (top 60%) like FedEx code path ---
w, h = img.size
crop = img.crop((0, 0, w, int(h * 0.60)))
crop.save("debug_fedex_crop_60.jpg")
print(f"Saved crop (top 60%): {crop.size}")
