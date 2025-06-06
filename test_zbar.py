from pyzbar.pyzbar import decode
from PIL import Image
img = Image.open("messageImage_1749233126808.jpg")
result = decode(img)
print(result)
