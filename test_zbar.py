from pyzbar.pyzbar import decode
from PIL import Image
img = Image.open("sample_barcode.png")
result = decode(img)
print(result)
