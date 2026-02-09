import os
import csv
import asyncio
import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# 1. Global Configuration
load_dotenv()
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

OUTPUT_FILE = 'wa_leads_full_pool.csv'
BASE_URL = "https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg="

# CONFIGURATION: How many pages to scrape (25 leads per page)
TOTAL_PAGES = 10 

def get_existing_ids():
    """Loads IDs from the CSV so we never scrape them again."""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE)
        return set(df['Internal ID'].astype(str).unique())
    except:
        return set()

async def fetch_lead_details(context, lead, semaphore):
    """Fetches every attribute for a lead concurrently."""
    async with semaphore:
        # Final console log so you can see the name in real-time
        print(f"  ⚡ Deep-Scraping: {lead['name']}")
        
        page = await context.new_page()
        try:
            await page.goto(f"https://data.cofoundersgroup.com/view_lead/{lead['id']}", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector('table.table-striped', timeout=15000)

            attr = {}
            rows = await page.locator('table.table-striped tr').all()
            for r in rows:
                cells = await r.locator('td').all()
                if len(cells) >= 2:
                    k = (await cells[0].inner_text()).strip().lower().replace(":", "").replace(" ", "_")
                    v = (await cells[1].inner_text()).strip()
                    attr[k] = v

            async def get_sb(label):
                try: return (await page.locator(f'b:has-text("{label}") + span').first.inner_text()).strip()
                except: return "N/A"

            phone = attr.get('phone') or await get_sb('Phone Number/s')
            
            data_row = [
                lead['name'], f'=HYPERLINK("tel:{phone}", "{phone}")',
                attr.get('email') or await get_sb('Email/s'),
                attr.get('address1') or await get_sb('Address'),
                attr.get('city', 'N/A'), attr.get('state', 'WA'), attr.get('zip', 'N/A'),
                attr.get('lead_source', 'N/A'), attr.get('folder', 'N/A'),
                attr.get('remarks', 'N/A'), attr.get('bedrooms', 'N/A'),
                attr.get('bathrooms', 'N/A'), attr.get('square_footage', 'N/A'),
                attr.get('year_built', 'N/A'), 
                attr.get('parcel_number') or attr.get('tax_id', 'N/A'),
                attr.get('contact_id', 'N/A'), lead['id']
            ]
            await page.close()
            return data_row
        except Exception:
            await page.close()
            return None

async def main():
    processed_ids = get_existing_ids()
    print(f"📊 Session started. {len(processed_ids)} leads already in CSV.")

    async with async_playwright() as p:
        # Headless for max speed on your MacBook Air
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        print("🔑 Logging in...")
        await page.goto('https://data.cofoundersgroup.com/login')
        await page.fill('input[type="email"]', EMAIL)
        await page.fill('input[type="password"]', PASSWORD)
        await page.click('button[type="submit"]')
        await page.wait_for_load_state('networkidle')

        if not os.path.exists(OUTPUT_FILE):
            headers = ['Name', 'Phone', 'Email', 'Address', 'City', 'State', 'Zip', 'Lead Source', 'Folder', 'Remarks', 'Bedrooms', 'Bathrooms', 'SqFt', 'Year Built', 'Parcel/Tax ID', 'Contact ID', 'Internal ID']
            with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)

        # 2. Main Loop using 25-record offset
        for page_idx in range(TOTAL_PAGES):
            offset = (page_idx * 25) + 1
            print(f"\n🚀 STEPPING TO OFFSET {offset} (Logical Page {page_idx + 1})")
            await page.goto(f"{BASE_URL}{offset}", wait_until="networkidle")
            
            # Extract raw leads from current offset
            raw_leads = await page.evaluate("""() => {
                const results = [];
                const seenOnPage = new Set();
                document.querySelectorAll('.leads').forEach(btn => {
                    const cid = btn.getAttribute('data-contact-id');
                    const tr = btn.closest('tr');
                    const name = tr?.querySelector('td')?.innerText.trim();
                    if (cid && name && name !== "," && !seenOnPage.has(cid)) {
                        seenOnPage.add(cid);
                        results.push({ id: cid, name: name });
                    }
                });
                return results;
            }""")

            if not raw_leads:
                print("🛑 No more leads found. End of pool reached.")
                break

            # Deduplicate against current CSV and print for review
            new_leads = []
            for lead in raw_leads:
                if str(lead['id']) in processed_ids:
                    print(f"  🚫 [DUPE SKIPPED]: {lead['name']} ({lead['id']})")
                else:
                    new_leads.append(lead)

            if not new_leads:
                print(f"⏭️ Page {page_idx + 1} has no new data. Skipping to next offset.")
                continue

            # Concurrent Deep-Scrape (10 leads at once)
            semaphore = asyncio.Semaphore(10)
            tasks = [fetch_lead_details(context, lead, semaphore) for lead in new_leads]
            results = await asyncio.gather(*tasks)

            # Save and update processed list
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in results:
                    if row: 
                        writer.writerow(row)
                        processed_ids.add(str(row[-1]))
            
            print(f"✅ Page {page_idx + 1} finalized.")

        await browser.close()
        print(f"\n🏁 Finished! Total unique leads: {len(processed_ids)}")

if __name__ == "__main__":
    asyncio.run(main())