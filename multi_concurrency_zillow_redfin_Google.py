import os
import csv
import asyncio
import pandas as pd
import urllib.parse
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# 1. Global Configuration
load_dotenv()
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

OUTPUT_FILE = 'wa_leads_matrix_ready.csv'
BASE_URL = "https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg="

# CONFIGURATION: Set the volume for this session
TOTAL_PAGES = 10

def get_existing_ids():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE)
        return set(df['Internal ID'].astype(str).unique())
    except Exception:
        return set()

async def fetch_lead_details(context, lead, semaphore):
    async with semaphore:
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
                try:
                    return (await page.locator(f'b:has-text("{label}") + span').first.inner_text()).strip()
                except Exception:
                    return "N/A"

            # Core Data
            phone = attr.get('phone') or await get_sb('Phone Number/s')
            addr = attr.get('address1') or await get_sb('Address')
            city = attr.get('city', 'N/A')
            zip_code = attr.get('zip', 'N/A')
            full_addr_query = urllib.parse.quote(f"{addr} {city} WA {zip_code}")

            # 🔗 LINK GENERATION
            zillow_link = f"https://www.zillow.com/homes/{full_addr_query}_rb/"
            redfin_link = f"https://www.redfin.com/search?q={full_addr_query}"
            google_link = f"https://www.google.com/search?q={full_addr_query}"

            data_row = [
                lead['name'],
                f'=HYPERLINK("tel:{phone}", "{phone}")' if phone != "N/A" else "N/A",
                attr.get('email') or await get_sb('Email/s'),
                addr, city, 'WA', zip_code,
                attr.get('date_added', 'N/A'),
                attr.get('remarks', 'N/A'),
                attr.get('parcel_number') or attr.get('tax_id', 'N/A'),
                # Matrix/NWMLS Deep Links
                f'=HYPERLINK("{zillow_link}", "Zillow")',
                f'=HYPERLINK("{redfin_link}", "Redfin")',
                f'=HYPERLINK("{google_link}", "Google")',
                attr.get('contact_id', 'N/A'), lead['id']
            ]
            await page.close()
            return data_row
        except Exception:
            await page.close()
            return None

async def main():
    processed_ids = get_existing_ids()
    async with async_playwright() as p:
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
            headers = [
                'Full Name', 'Phone', 'Email', 'Street Address', 'City', 'State', 'Zip',
                'Date Added', 'Property Remarks', 'Parcel ID',
                'Zillow Link', 'Redfin Link', 'Google Search',
                'Contact ID', 'Internal ID'
            ]
            with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)

        for page_idx in range(TOTAL_PAGES):
            offset = (page_idx * 25) + 1
            print(f"\n🚀 OFFSET {offset} (Page {page_idx + 1})")
            await page.goto(f"{BASE_URL}{offset}", wait_until="networkidle")

            raw_leads = await page.evaluate("""() => {
                const results = [];
                const seen = new Set();
                document.querySelectorAll('.leads').forEach(btn => {
                    const cid = btn.getAttribute('data-contact-id');
                    const tr = btn.closest('tr');
                    const name = tr?.querySelector('td')?.innerText.trim();
                    if (cid && name && name !== "," && !seen.has(cid)) {
                        seen.add(cid);
                        results.push({ id: cid, name: name });
                    }
                });
                return results;
            }""")

            new_leads = [l for l in raw_leads if str(l['id']) not in processed_ids]
            if not new_leads:
                continue

            semaphore = asyncio.Semaphore(10)
            tasks = [fetch_lead_details(context, lead, semaphore) for lead in new_leads]
            results = await asyncio.gather(*tasks)

            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for row in results:
                    if row:
                        writer.writerow(row)
                        processed_ids.add(str(row[-1]))

            print(f"✅ Page {page_idx + 1} synced.")

        await browser.close()
        print(f"\n🏁 Complete! File: {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())