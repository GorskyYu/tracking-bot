"""Step-by-step debug: show EVERY rotation option to find the correct one."""
import fitz
from PIL import Image

pdf_path = r"c:\Users\yves.lai\Downloads\F11000029722.pdf"
doc = fitz.open(pdf_path)
page = doc[0]

print(f"MediaBox: {page.rect}")
print(f"/Rotate: {page.rotation}")
print()

# ======= Option A: PyMuPDF native (applies /Rotate 90 CW) =======
pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
img_native = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
img_native.save("opt_A_native.jpg")
print(f"[A] Native render: {img_native.size}")

# ======= Option B: Native + rotate 90 CCW (PIL rotate(90)) =======
img_b = img_native.rotate(90, expand=True)
img_b.save("opt_B_native_ccw90.jpg")
print(f"[B] Native + 90 CCW: {img_b.size}")

# ======= Option C: Native + rotate 90 CW (PIL rotate(-90)) =======
img_c = img_native.rotate(-90, expand=True)
img_c.save("opt_C_native_cw90.jpg")
print(f"[C] Native + 90 CW: {img_c.size}")

# ======= Option D: Ignore /Rotate, render raw content =======
doc2 = fitz.open(pdf_path)
page2 = doc2[0]
page2.set_rotation(0)  # Strip /Rotate
pix2 = page2.get_pixmap(matrix=fitz.Matrix(3, 3))
img_raw = Image.frombytes("RGB", [pix2.width, pix2.height], pix2.samples)
img_raw.save("opt_D_raw_no_rotate.jpg")
print(f"[D] Raw (no /Rotate): {img_raw.size}")

print()
print("Saved: opt_A_native.jpg, opt_B_native_ccw90.jpg, opt_C_native_cw90.jpg, opt_D_raw_no_rotate.jpg")
print("Please open all 4 and tell me which one has the text in the correct reading direction.")
