import os
import csv
import time
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# Load credentials
load_dotenv()
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

# Output file path
OUTPUT_FILE = 'wa_leads_full_details.csv'
BASE_URL = "https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg="

def init_csv():
    """Initializes the CSV with headers for all relevant real estate fields."""
    headers = [
        'Name', 'Phone', 'Email', 'Address', 'City', 'State', 'Zip', 
        'Lead Source', 'Folder', 'Remarks', 'Bedrooms', 'Bathrooms', 
        'SqFt', 'Year Built', 'Parcel/Tax ID', 'Contact ID', 'Internal ID'
    ]
    print(f"📂 Initializing {OUTPUT_FILE} (Overwriting existing file)...")
    # Using 'w' here ensures the file is fresh and doesn't contain old data
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

def scrape_wa_pool():
    init_csv()
    leads_saved_count = 0
    
    with sync_playwright() as p:
        print("🚀 Launching browser...")
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context(viewport={'width': 1280, 'height': 800})
        page = context.new_page()
        
        try:
            # 1. Login
            print("🔑 Logging in...")
            page.goto('https://data.cofoundersgroup.com/login')
            page.fill('input[type="email"]', EMAIL)
            page.fill('input[type="password"]', PASSWORD)
            page.click('button[type="submit"]')
            page.wait_for_load_state('networkidle')

            # 2. Navigate to Page 1
            print("📄 Loading Page 1...")
            page.goto(f"{BASE_URL}1")
            page.wait_for_selector('table', timeout=30000)
            time.sleep(2) 

            # 3. Index Leads (FIXED: Added deduplication logic here)
            leads_to_process = page.evaluate("""() => {
                const results = [];
                const seenIds = new Set();
                document.querySelectorAll('.leads').forEach(btn => {
                    const cid = btn.getAttribute('data-contact-id');
                    const tr = btn.closest('tr');
                    const name = tr?.querySelector('td')?.innerText.trim();
                    
                    // Only add if we haven't seen this ID yet and it has a valid name
                    if (cid && name && name !== "," && !seenIds.has(cid)) {
                        seenIds.add(cid);
                        results.push({ id: cid, name: name });
                    }
                });
                return results;
            }""")

            print(f"📊 Found {len(leads_to_process)} unique leads to process.")

            # 4. Deep Extraction
            for lead in leads_to_process:
                print(f"  ➡️ Scraping Details: {lead['name']} (ID: {lead['id']})")
                try:
                    temp_page = context.new_page()
                    temp_page.goto(f"https://data.cofoundersgroup.com/view_lead/{lead['id']}", timeout=60000)
                    temp_page.wait_for_selector('.ibox-content', timeout=20000)

                    # Dynamic mapping of the "Client Import Data" table
                    attr = {}
                    rows = temp_page.locator('table.table-striped tr').all()
                    for r in rows:
                        cells = r.locator('td').all()
                        if len(cells) >= 2:
                            k = cells[0].inner_text().strip().lower().replace(":", "").replace(" ", "_")
                            v = cells[1].inner_text().strip()
                            attr[k] = v

                    def get_sb(label):
                        try: return temp_page.locator(f'b:has-text("{label}") + span').first.inner_text().strip()
                        except: return "N/A"

                    phone = attr.get('phone') or get_sb('Phone Number/s')
                    
                    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            lead['name'],
                            f'=HYPERLINK("tel:{phone}", "{phone}")' if phone != "N/A" else "N/A",
                            attr.get('email') or get_sb('Email/s'),
                            attr.get('address1') or get_sb('Address'),
                            attr.get('city', 'N/A'),
                            attr.get('state', 'WA'),
                            attr.get('zip', 'N/A'),
                            attr.get('lead_source', 'N/A'),
                            attr.get('folder', 'N/A'),
                            attr.get('remarks', 'N/A'),
                            attr.get('bedrooms', 'N/A'),
                            attr.get('bathrooms', 'N/A'),
                            attr.get('square_footage', 'N/A'),
                            attr.get('year_built', 'N/A'),
                            attr.get('parcel_number') or attr.get('tax_id', 'N/A'), # Essential for NWMLS
                            attr.get('contact_id', 'N/A'),
                            lead['id']
                        ])
                    
                    print(f"    ✅ Success: Saved {lead['name']}")
                    leads_saved_count += 1
                    temp_page.close()

                except Exception as inner_e:
                    print(f"    ⚠️ Error on lead {lead['name']}: {inner_e}")
                    if 'temp_page' in locals(): temp_page.close()

        except Exception as e:
            print(f"🛑 Critical Error: {e}")
        finally:
            browser.close()
            print(f"\n🏁 Finished. Total Unique Leads Saved: {leads_saved_count}")

if __name__ == "__main__":
    scrape_wa_pool()