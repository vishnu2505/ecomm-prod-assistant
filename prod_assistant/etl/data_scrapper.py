import csv
import re
import os
import time
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

class FlipkartScraper:
    def __init__(self, output_dir="data"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _get_text_safe(self, item, selectors):
        """Helper to try multiple selectors until one works."""
        for selector in selectors:
            try:
                el = item.query_selector(selector)
                if el:
                    return el.inner_text().strip()
            except:
                continue
        return "N/A"

    def get_top_reviews(self, product_url, count=2):
        if not product_url.startswith("http"):
            return "No reviews found"

        print(f"    └── 📄 Visiting Product Page (Original Logic): {product_url[:50]}...")
        reviews = []
        
        try:
            with sync_playwright() as p:
                # headless=False matches your original "undetected" approach better by looking real
                browser = p.chromium.launch(headless=False)
                context = browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                
                # 1. ORIGINAL LOGIC: Go to product page
                page.goto(product_url, timeout=60000, wait_until="domcontentloaded")
                
                # 2. ORIGINAL LOGIC: Close Popup (Try/Except)
                try:
                    # Wait briefly for popup
                    close_btn = page.wait_for_selector("//button[contains(text(), '✕')]", timeout=4000)
                    if close_btn:
                        close_btn.click()
                        print("    ❎ Closed Popup")
                except:
                    pass # No popup, move on

                # 3. ORIGINAL LOGIC: Aggressive "End" Key Scrolling
                # This triggers lazy loading better than mouse scrolling
                print("    ⬇️ Scrolling to load reviews...")
                for _ in range(5):
                    page.keyboard.press("End")
                    time.sleep(1.5) # Matches original sleep(1.5)

                soup = BeautifulSoup(page.content(), "html.parser")
                
                # 4. ORIGINAL LOGIC: Use the EXACT selectors from your original file
                #    plus the new 't-ZTKy' as a fallback.
                #    _27M-vq, col.EPCmJX, _6K-7Co came from YOUR original file.
                review_blocks = soup.select("div._27M-vq, div.col.EPCmJX, div._6K-7Co, div.t-ZTKy")
                
                seen = set()
                for block in review_blocks:
                    text = block.get_text(separator=" ", strip=True)
                    # Cleanup "READ MORE"
                    text = text.replace("READ MORE", "").strip()
                    
                    if text and text not in seen and len(text) > 20:
                        reviews.append(text)
                        seen.add(text)
                    if len(reviews) >= count:
                        break
                
                browser.close()
                
        except Exception as e:
            print(f"    ⚠️ Error fetching reviews: {e}")
            return "Error fetching reviews"

        print(f"    ✅ Captured {len(reviews)} reviews.")
        return " || ".join(reviews) if reviews else "No reviews found"
    
    def scrape_flipkart_products(self, query, max_products=2, review_count=2):
        print(f"🔍 Searching Flipkart for: '{query}'")
        products = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                
                search_url = f"https://www.flipkart.com/search?q={query.replace(' ', '+')}&sort=relevance"
                page.goto(search_url, timeout=60000, wait_until="domcontentloaded")

                # Wait for products
                try:
                    page.wait_for_selector("div[data-id]", timeout=15000)
                except:
                    print("⚠️ Timeout waiting for products.")
                    browser.close()
                    return []

                items = page.query_selector_all("div[data-id]")[:max_products]
                print(f"✅ Found {len(items)} product cards.")

                extracted_data = []
                for i, item in enumerate(items):
                    try:
                        # 1. TITLE (Using expanded list including generic fallbacks)
                        title = self._get_text_safe(item, ["div.KzDlHZ", "a.wjcEIp", "div._4rR01T", "a.s1Q9rs"])
                        if title == "N/A":
                            img = item.query_selector("img")
                            if img: title = img.get_attribute("alt") or "N/A"
                        
                        # 2. PRICE
                        price = self._get_text_safe(item, ["div.Nx9bqj", "div._30jeq3"])
                        
                        # 3. LINK
                        link_el = item.query_selector("a[href*='/p/']")
                        if not link_el: link_el = item.query_selector("a")
                        
                        href = link_el.get_attribute("href") if link_el else ""
                        product_link = href if href.startswith("http") else "https://www.flipkart.com" + href
                        
                        # 4. RATING
                        rating = self._get_text_safe(item, ["div.XQDdHH", "div._3LWZlK"])
                        
                        # 5. REVIEW COUNT
                        reviews_text = self._get_text_safe(item, ["span.Wphh3N", "span._2_R_DZ"])
                        match = re.search(r"\d+(,\d+)?(?=\s+Reviews)", reviews_text)
                        total_reviews = match.group(0) if match else "0"

                        if (title != "N/A" or price != "N/A") and "flipkart" in product_link:
                            id_match = re.findall(r"/p/(itm[0-9A-Za-z]+)", href)
                            product_id = id_match[0] if id_match else f"item_{i}"

                            extracted_data.append({
                                "id": product_id,
                                "title": title,
                                "rating": rating,
                                "reviews_count": total_reviews,
                                "price": price,
                                "link": product_link
                            })
                    except Exception as e:
                        print(f"Skipping item: {e}")
                        continue
                
                browser.close()

            # Iterate found items to get reviews
            for data in extracted_data:
                top_reviews = self.get_top_reviews(data["link"], count=review_count)
                products.append([
                    data["id"], 
                    data["title"], 
                    data["rating"], 
                    data["reviews_count"], 
                    data["price"], 
                    top_reviews
                ])

        except Exception as e:
            print(f"Global scraping error: {e}")

        return products
    
    def save_to_csv(self, data, filename="product_reviews.csv"):
        if os.path.isabs(filename):
            path = filename
        elif os.path.dirname(filename):
            path = filename
            os.makedirs(os.path.dirname(path), exist_ok=True)
        else:
            path = os.path.join(self.output_dir, filename)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["product_id", "product_title", "rating", "total_reviews", "price", "top_reviews"])
            writer.writerows(data)