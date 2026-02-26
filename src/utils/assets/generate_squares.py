import urllib.request
from PIL import Image

def create_square(color_hex, filename):
    img = Image.new('RGB', (128, 128), color=color_hex)
    img.save(filename)

# Colors based on a smooth health gradient
colors = {
    "dark_green": "#2E7D32",   # >85%
    "light_green": "#81C784",  # >65%
    "yellow": "#FFD54F",       # >50%
    "light_orange": "#FFB74D", # >30%
    "dark_orange": "#F57C00",  # >15%
    "light_red": "#E57373",    # >1%
    "dark_red": "#C62828"      # 0%
}

for name, hex_val in colors.items():
    create_square(hex_val, f"src/utils/assets/{name}.png")
    print(f"Generated {name}.png with color {hex_val}")
