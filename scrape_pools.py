import os
import time
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

# Base URL identified from your requirement
BASE_URL = "https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg="

def scrape_leads(max_pages=3):
    leads = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        context = browser.new_context(viewport={'width': 1400, 'height': 900})
        page = context.new_page()
        
        # 1. Login
        print("🚀 Logging in...")
        page.goto('https://data.cofoundersgroup.com/login')
        page.fill('input[type="email"]', EMAIL)
        page.fill('input[type="password"]', PASSWORD)
        page.click('button[type="submit"]')
        page.wait_for_load_state('networkidle')

        # 2. Page Loop
        for pg in range(1, max_pages + 1):
            print(f"\n--- Processing Page {pg} ---")
            page.goto(f"{BASE_URL}{pg}")
            page.wait_for_selector('table tbody tr')
            
            # Find all rows
            rows = page.locator('table tbody tr').all()
            
            for index, row in enumerate(rows):
                # Check if the name cell is empty (skips the empty leads in your screenshot)
                name_text = row.locator('td').first.inner_text().strip()
                if not name_text or name_text == ",":
                    continue
                
                print(f"  Scraping lead: {name_text}")
                
                try:
                    # Click View Lead to populate the right-side panel
                    view_btn = row.get_by_role("button", name="View Lead")
                    view_btn.click()
                    
                    # Wait for the right panel to update (using the ID or a specific class)
                    # Based on your image, we look for the "Contact ID" or "Address" in the right panel
                    page.wait_for_selector('.details-container, [class*="Contact ID"]', timeout=5000)
                    
                    # Extract data from the right-hand details panel
                    lead_data = {
                        'name': name_text,
                        'contact_id': page.get_by_text("Contact ID").locator("xpath=following-sibling::*").first.inner_text().strip(),
                        'address': page.locator('i.fa-map-marker').locator("xpath=parent::*").inner_text().replace('Address', '').strip(),
                        'phone': page.locator('i.fa-mobile-phone').locator("xpath=parent::*").inner_text().strip(),
                        'email': page.get_by_text("@").first.inner_text().strip(),
                        'page': pg
                    }
                    leads.append(lead_data)
                    
                except Exception as e:
                    print(f"  ⚠️ Error on row {index}: {e}")
            
            # Small rest between pages to avoid rate limiting
            time.sleep(2)

        browser.close()

    # 3. Export
    if leads:
        df = pd.DataFrame(leads)
        df.to_csv('wa_pool_leads_detailed.csv', index=False)
        print(f"\n✅ Successfully scraped {len(leads)} leads.")
    else:
        print("No leads found.")

if __name__ == "__main__":
    # Start with 1 or 2 pages to test, then increase
    scrape_leads(max_pages=2)