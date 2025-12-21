import fitz  # PyMuPDF
from PIL import Image, ImageDraw

def generate_grid():
    input_file = "test_label.pdf"
    output_file = "debug_coordinate_map.jpg"
    
    try:
        doc = fitz.open(input_file)
        page = doc[0]
        
        # 180 degrees flips the bottom-right text to the TOP-LEFT
        page.set_rotation(-45) 

        # High resolution Matrix 3 for clear text
        pix = page.get_pixmap(matrix=fitz.Matrix(3, 3))
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        draw = ImageDraw.Draw(img)
        w, h = img.size
        
        # Grid lines every 5%
        for i in range(1, 20):
            val = round(i / 20, 2)
            pos_x, pos_y = val * w, val * h
            draw.line([(pos_x, 0), (pos_x, h)], fill="red", width=2)
            draw.text((pos_x + 5, 20), str(val), fill="red")
            draw.line([(0, pos_y), (w, pos_y)], fill="red", width=2)
            draw.text((20, pos_y + 5), str(val), fill="red")

        img.save(output_file, quality=95)
        print("SUCCESS! Check 'debug_coordinate_map.jpg' for the top-left orientation.")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    generate_grid()