import os
import pandas as pd
from datasets import load_dataset

# --- CONFIGURATION ---
CATEGORY = "Cell_Phones_and_Accessories"
MAX_PRODUCTS = 50  # Limit to 50 to stay safe on free tiers
OUTPUT_DIR = "data"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "product_reviews.csv")

# Direct URLs to the raw JSONL files on Hugging Face (Bypassing the script)
BASE_URL = "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main"
META_URL = f"{BASE_URL}/raw/meta_categories/meta_{CATEGORY}.jsonl"
REVIEW_URL = f"{BASE_URL}/raw/review_categories/{CATEGORY}.jsonl"

def stream_amazon_data():
    print(f"📡 Connecting to Hugging Face (Streaming Mode)...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Stream Metadata (Get Product Details first)
    # We use 'json' builder to load the file directly, bypassing the 'trust_remote_code' issue
    print(f"   └── Streaming Metadata from: {META_URL[:60]}...")
    try:
        ds_meta = load_dataset(
            "json", 
            data_files=META_URL, 
            split="train", 
            streaming=True
        )
    except Exception as e:
        print(f"❌ Error loading metadata: {e}")
        return

    products = {}
    
    # Iterate through the stream and pick the first MAX_PRODUCTS valid items
    for item in ds_meta:
        if len(products) >= MAX_PRODUCTS:
            break
            
        p_id = item.get("parent_asin")
        title = item.get("title")
        
        # specific check to ensure we get meaningful products
        if p_id and title and len(title) > 5:
            products[p_id] = {
                "product_id": p_id,
                "product_title": title,
                "price": item.get("price", "N/A"),
                "rating": item.get("average_rating", 0.0),
                "total_reviews": item.get("rating_number", 0),
                "reviews_list": [] # Placeholder for reviews
            }

    print(f"✅ Selected {len(products)} products to process.")

    # 2. Stream Reviews (Filter for our selected products)
    print(f"   └── Streaming Reviews from: {REVIEW_URL[:60]}...")
    try:
        ds_reviews = load_dataset(
            "json", 
            data_files=REVIEW_URL, 
            split="train", 
            streaming=True
        )
    except Exception as e:
        print(f"❌ Error loading reviews: {e}")
        return

    # We need to scan the review stream. 
    # Since we can't scan forever, we'll set a safety counter.
    reviews_found_count = 0
    max_scan_limit = 50000 # Scan first 50k reviews to find matches for our 50 products
    
    for i, review in enumerate(ds_reviews):
        if i >= max_scan_limit:
            break
            
        p_id = review.get("parent_asin")
        
        # If this review belongs to one of our selected 50 products
        if p_id in products:
            # Add review text to that product
            review_text = review.get("text", "")
            rating = review.get("rating", 5.0)
            
            if review_text:
                products[p_id]["reviews_list"].append(f"{rating}⭐ {review_text[:200]}...")
                reviews_found_count += 1

    print(f"✅ Found {reviews_found_count} reviews for selected products.")

    # 3. Format & Save
    final_rows = []
    for p_id, data in products.items():
        # Combine top 5 reviews into one string
        top_reviews = " || ".join(data["reviews_list"][:5])
        
        if not top_reviews:
            top_reviews = "No detailed reviews available."

        final_rows.append({
            "product_id": data["product_id"],
            "product_title": data["product_title"],
            "rating": data["rating"],
            "total_reviews": data["total_reviews"],
            "price": data["price"],
            "top_reviews": top_reviews
        })

    df = pd.DataFrame(final_rows)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n💾 SUCCESS! Saved {len(df)} products to: {OUTPUT_CSV}")

if __name__ == "__main__":
    stream_amazon_data()