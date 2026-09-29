"""Static reference data for the simulated marketplace."""

from __future__ import annotations

CITIES: tuple[str, ...] = (
    "Jakarta",
    "Surabaya",
    "Bandung",
    "Medan",
    "Semarang",
    "Makassar",
    "Denpasar",
    "Yogyakarta",
)

COUNTRIES: tuple[str, ...] = ("ID",)

DEVICES: tuple[str, ...] = ("android", "ios", "web")

PAYMENT_METHODS: tuple[tuple[str, float], ...] = (
    ("ewallet", 0.35),
    ("va", 0.25),
    ("card", 0.20),
    ("cod", 0.12),
    ("bank_transfer", 0.08),
)

# (name, slug, parent_slug). Parents are listed before their children.
CATEGORY_TREE: tuple[tuple[str, str, str | None], ...] = (
    ("Electronics", "electronics", None),
    ("Mobile Phones", "mobile-phones", "electronics"),
    ("Laptops", "laptops", "electronics"),
    ("Audio", "audio", "electronics"),
    ("Fashion", "fashion", None),
    ("Men's Clothing", "mens-clothing", "fashion"),
    ("Women's Clothing", "womens-clothing", "fashion"),
    ("Shoes", "shoes", "fashion"),
    ("Home & Living", "home-living", None),
    ("Furniture", "furniture", "home-living"),
    ("Kitchen", "kitchen", "home-living"),
    ("Beauty", "beauty", None),
    ("Skincare", "skincare", "beauty"),
    ("Makeup", "makeup", "beauty"),
    ("Sports", "sports", None),
    ("Fitness", "fitness", "sports"),
    ("Outdoor", "outdoor", "sports"),
    ("Groceries", "groceries", None),
)

# Leaf categories that actually hold products, with plausible product nouns and
# a rough price band in IDR (thousands). Keeps generated catalogues believable.
LEAF_CATALOG: dict[str, dict] = {
    "mobile-phones": {
        "nouns": ["Smartphone", "Phone Case", "Screen Protector", "Power Bank", "Charger"],
        "price": (150_000, 12_000_000),
    },
    "laptops": {
        "nouns": ["Laptop", "Laptop Sleeve", "Docking Station", "External SSD", "Keyboard"],
        "price": (250_000, 25_000_000),
    },
    "audio": {
        "nouns": ["Wireless Earbuds", "Bluetooth Speaker", "Headphones", "Soundbar", "Microphone"],
        "price": (90_000, 6_000_000),
    },
    "mens-clothing": {
        "nouns": ["T-Shirt", "Hoodie", "Jeans", "Jacket", "Formal Shirt"],
        "price": (60_000, 900_000),
    },
    "womens-clothing": {
        "nouns": ["Dress", "Blouse", "Cardigan", "Skirt", "Hijab"],
        "price": (60_000, 1_200_000),
    },
    "shoes": {
        "nouns": ["Sneakers", "Running Shoes", "Sandals", "Boots", "Loafers"],
        "price": (120_000, 3_500_000),
    },
    "furniture": {
        "nouns": ["Desk Chair", "Coffee Table", "Bookshelf", "Sofa Bed", "Wardrobe"],
        "price": (300_000, 9_000_000),
    },
    "kitchen": {
        "nouns": ["Air Fryer", "Cookware Set", "Blender", "Rice Cooker", "Knife Set"],
        "price": (80_000, 4_000_000),
    },
    "skincare": {
        "nouns": ["Sunscreen", "Serum", "Moisturizer", "Face Wash", "Toner"],
        "price": (35_000, 750_000),
    },
    "makeup": {
        "nouns": ["Lipstick", "Cushion", "Mascara", "Eyeshadow Palette", "Setting Spray"],
        "price": (45_000, 900_000),
    },
    "fitness": {
        "nouns": ["Yoga Mat", "Dumbbell Set", "Resistance Band", "Treadmill", "Jump Rope"],
        "price": (50_000, 8_000_000),
    },
    "outdoor": {
        "nouns": [
            "Camping Tent",
            "Hiking Backpack",
            "Water Bottle",
            "Trekking Pole",
            "Sleeping Bag",
        ],
        "price": (100_000, 4_500_000),
    },
    "groceries": {
        "nouns": ["Coffee Beans", "Instant Noodles", "Olive Oil", "Rice 5kg", "Snack Box"],
        "price": (15_000, 400_000),
    },
}

ADJECTIVES: tuple[str, ...] = (
    "Premium",
    "Original",
    "Ultra",
    "Classic",
    "Pro",
    "Mini",
    "Eco",
    "Smart",
    "Everyday",
    "Deluxe",
)
