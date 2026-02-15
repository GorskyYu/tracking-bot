"""Deep debug script: exact rotation visualization"""
import fitz
from PIL import Image

pdf_path = "c:/Users/yves.lai/Downloads/F11000029722.pdf"
doc = fitz.open(pdf_path)
page = doc[0]

# --- Simulate PRODUCTION Code Logic ---
print(f"Original Page rotation: {page.rotation}")

# 1. Reset rotation instructions to 0 (so we get raw unrotated visual)
original_rotation = page.rotation
page.set_rotation(0)

# 2. Render content 
pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
print(f"Raw rendered size (unrotated): {img.size}")
img.save("debug_step1_raw_unrotated.jpg")

# 3. Apply MANUAL PIL rotation (Simulating the code pushed to Heroku)
# Note: page.rotation was 90. So -original_rotation = -90.
if original_rotation != 0:
    # PIL rotate: positive is CCW.
    # PDF /Rotate: postive is CW.
    # So to MATCH PDF /Rotate 90 (CW), we need PIL rotate -90 (CW).
    rotated_img = img.rotate(-original_rotation, expand=True)
    print(f"After manual PIL rotate({-original_rotation}): {rotated_img.size}")
    rotated_img.save("debug_step2_pil_rotated.jpg")
else:
    print("No rotation needed")

# --- COMPARISON: What if we let PyMuPDF handle it natively? ---
doc2 = fitz.open(pdf_path)
page2 = doc2[0]
print(f"\nComparing with PyMuPDF native rendering (Rotate={page2.rotation})")
# Do NOT call set_rotation(0)
pix2 = page2.get_pixmap(matrix=fitz.Matrix(3, 3))
img2 = Image.frombytes("RGB", [pix2.width, pix2.height], pix2.samples)
print(f"Native PyMuPDF size: {img2.size}")
img2.save("debug_step3_native_pymupdf.jpg")
