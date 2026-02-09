import os
import csv
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

OUTPUT_FILE = 'wa_property_leads.csv'
BASE_URL = "https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg="

def init_csv():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Name', 'Location', 'Phone', 'Email', 'Address', 'MLS ID', 'Contact ID'])

def append_to_csv(data):
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([data['name'], data['location'], data['phone_link'], data['email'], data['address'], data['mls_id'], data['contact_id']])

def scrape_wa_pool(start_page=1, end_page=1):
    init_csv()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=1000)
        context = browser.new_context(viewport={'width': 1400, 'height': 900})
        page = context.new_page()
        
        # Login
        page.goto('https://data.cofoundersgroup.com/login')
        page.fill('input[type="email"]', EMAIL)
        page.fill('input[type="password"]', PASSWORD)
        page.click('button[type="submit"]')
        page.wait_for_load_state('networkidle')

        for pg in range(start_page, end_page + 1):
            print(f"📄 Processing Page {pg}...")
            page.goto(f"{BASE_URL}{pg}")
            page.wait_for_selector('table tbody tr', timeout=15000)
            
            row_count = page.locator('table tbody tr').count()
            
            for i in range(row_count):
                row = page.locator('table tbody tr').nth(i)
                name_text = row.locator('td').first.inner_text().strip()
                
                if not name_text or name_text == ",":
                    continue
                
                try:
                    # Capture the current Contact ID to detect refresh
                    current_panel_id = page.locator('b:has-text("Contact ID") + span')
                    old_id = current_panel_id.inner_text().strip() if current_panel_id.is_visible() else ""

                    row.get_by_role("button", name="View Lead").click()
                    
                    # Wait for panel to change (timeout after 5s if it doesn't)
                    try:
                        page.wait_for_function(
                            f"id => document.querySelector('b:has-text(\"Contact ID\") + span')?.innerText.trim() !== '{old_id}'",
                            old_id, timeout=5000
                        )
                    except:
                        pass # Continue anyway if it's slow to refresh

                    # Helper function to get text safely without timing out
                    def get_text_safe(selector):
                        loc = page.locator(selector).first
                        return loc.inner_text().strip() if loc.is_visible() else "N/A"

                    # Extract MLS ID from the specific Client Import Data table
                    mls_id = "N/A"
                    mls_label = page.locator('tr:has-text("mls_id") td').nth(1)
                    if mls_label.is_visible():
                        mls_id = mls_label.inner_text().strip()

                    raw_phone = get_text_safe('i.fa-mobile-phone + a')

                    lead_data = {
                        'name': name_text,
                        'location': row.locator('td').nth(1).inner_text().strip(),
                        'phone_link': f'=HYPERLINK("tel:{raw_phone}", "{raw_phone}")' if raw_phone != "N/A" else "N/A",
                        'email': get_text_safe('i.fa-envelope + a'),
                        'address': get_text_safe('i.fa-map-marker + span'),
                        'mls_id': mls_id,
                        'contact_id': get_text_safe('b:has-text("Contact ID") + span')
                    }
                    
                    append_to_csv(lead_data)
                    print(f"  ✅ Saved: {name_text}")
                    
                except Exception as e:
                    print(f"  ⚠️ Skipping row {i+1} due to error: {e}")

        browser.close()

if __name__ == "__main__":
    scrape_wa_pool(start_page=1, end_page=1)