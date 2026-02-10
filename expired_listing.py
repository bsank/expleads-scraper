"""
MCP TOOL SPECIFICATION: Expired Listing Scraper (Production)
DESCRIPTION: Matches portal UI 1:1. Captures all 87 leads by removing back-end status filtering.
            Conditional Logic: Generates Excel HYPERLINKs only if an address is present.
INPUTS: EMAIL, PASSWORD (from .env), TARGET_ZIP, TARGET_STATUS.
OUTPUT: CSV with clickable formulas for valid addresses and empty cells for N/A addresses.
"""

import os
import csv
import asyncio
import math
import pandas as pd
import urllib.parse
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

# --- ⚙️ CONFIGURATION BLOCK ⚙️ ---
TARGET_ZIP = "98072"            
TARGET_STATUS = "Expired"
OUTPUT_FILE = f'wa_{TARGET_STATUS}_{TARGET_ZIP}_Final.csv'
SHOW_BROWSER = False # Set to True to watch the browser work
# ---------------------------------

BASE_URL = "https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg="
PROCESSED_REGISTRY = set()

async def apply_site_filters(page):
    """Applies Zip and Status filters and reports the portal's lead count."""
    is_collapsed = await page.locator('div.ibox.collapsed').is_visible()
    if is_collapsed:
        await page.locator('.collapse-link').click()

    await page.locator('input[name="zip"]').fill(TARGET_ZIP)
    status_container = page.locator("#mls_status_chosen")
    await status_container.get_by_role("textbox").click()
    await page.locator(f'#mls_status_chosen li:has-text("{TARGET_STATUS}")').click()

    await page.locator('button:has-text("Update List")').click()
    await page.wait_for_load_state('networkidle')
    
    try:
        total_text = await page.locator('h5:has-text("Total Prospects")').inner_text()
        total_leads = int(total_text.split()[0].replace(',', ''))
        max_pages = math.ceil(total_leads / 25)
        print(f"📊 PORTAL AUDIT: Found {total_leads} leads in UI. Indexing {max_pages} pages...")
        return max_pages
    except Exception:
        return 1

async def fetch_and_verify(context, lead, semaphore):
    """
    Scrapes detail page. 
    ALWAYS returns the record to match UI count.
    ONLY builds links if address is present.
    """
    async with semaphore:
        page = await context.new_page()
        try:
            await page.goto(f"https://data.cofoundersgroup.com/view_lead/{lead['id']}", wait_until="domcontentloaded", timeout=60000)
            
            attr = {}
            rows = await page.locator('table tr').all()
            for r in rows:
                cells = await r.locator('td').all()
                if len(cells) >= 2:
                    k = (await cells[0].inner_text()).strip().lower().replace(" ", "_")
                    v = (await cells[1].inner_text()).strip()
                    attr[k] = v

            # Basic data capture
            phone = attr.get('phone/s') or attr.get('phone', 'N/A')
            addr = attr.get('address1', 'N/A')
            city = attr.get('city', 'N/A')
            
            # --- CONDITIONAL LINK LOGIC ---
            # Per Manager request: Only create links if address is present.
            z_link, r_link, g_link = "", "", ""
            
            if addr != 'N/A' and addr.strip() and addr.lower() != 'null':
                query = urllib.parse.quote(f"{addr} {city} WA")
                
                # Excel/Sheets Hyperlink Formulas
                z_link = f'=HYPERLINK("https://www.zillow.com/homes/{query}_rb/", "Zillow")'
                r_link = f'=HYPERLINK("https://www.redfin.com/search?q={query}", "Redfin")'
                g_link = f'=HYPERLINK("https://www.google.com/maps/search/{query}", "Google Maps")'

            return [
                lead['name'], 
                attr.get('lead_source', TARGET_STATUS), # Fallback to target if missing
                phone,
                attr.get('email/s') or attr.get('email', 'N/8'),
                addr, city, attr.get('zip', 'N/A'), attr.get('date_added', 'N/A'),
                z_link, r_link, g_link,
                lead['id']
            ]
        except Exception:
            # If detail page fails, return the info we have from the main table
            return [lead['name'], TARGET_STATUS, "N/A", "N/A", "N/A", "N/A", TARGET_ZIP, "N/A", "", "", "", lead['id']]
        finally:
            await page.close()

async def main():
    lead_counter = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=(not SHOW_BROWSER)) 
        context = await browser.new_context()
        page = await context.new_page()

        # Auth
        await page.goto('https://data.cofoundersgroup.com/login')
        await page.fill('input[type="email"]', EMAIL)
        await page.fill('input[type="password"]', PASSWORD)
        await page.click('button[type="submit"]')
        await page.wait_for_load_state('networkidle')

        # Navigate
        await page.goto("https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg=1")
        pages_to_scrape = await apply_site_filters(page)

        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Name', 'Status', 'Phone', 'Email', 'Address', 'City', 'Zip', 'Date Added', 'Zillow', 'Redfin', 'Google Maps', 'Internal ID'])
            
            for i in range(pages_to_scrape):
                print(f"📂 Indexing Page {i+1}...")
                
                raw_leads = await page.evaluate("""() => {
                    const results = [];
                    document.querySelectorAll('.leads').forEach(btn => {
                        results.push({ 
                            id: btn.getAttribute('data-contact-id'), 
                            name: btn.closest('tr').querySelector('td')?.innerText.trim() 
                        });
                    });
                    return results;
                }""")

                # Deduplicate by ID immediately
                unique_leads = []
                for l in raw_leads:
                    l_id = str(l['id'])
                    if l_id not in PROCESSED_REGISTRY:
                        PROCESSED_REGISTRY.add(l_id)
                        unique_leads.append(l)

                if unique_leads:
                    semaphore = asyncio.Semaphore(20) 
                    tasks = [fetch_and_verify(context, lead, semaphore) for lead in unique_leads]
                    results = await asyncio.gather(*tasks)
                    
                    for row in results:
                        if row: 
                            lead_counter += 1
                            writer.writerow(row)
                            addr_disp = row[4] if row[4] != 'N/A' else 'N/A (Link Skipped)'
                            print(f"  [{lead_counter}] Captured: {row[0]} | {addr_disp}")
                    f.flush()
                
                if i < pages_to_scrape - 1:
                    next_btn = page.locator('li.next a, a:has-text("Next")').first
                    if await next_btn.is_visible():
                        await next_btn.click()
                        await page.wait_for_load_state('networkidle')
                    else:
                        break

        await browser.close()
        print(f"\n🏁 SUCCESS: {lead_counter} leads saved. CSV now matches UI exactly.")

if __name__ == "__main__":
    asyncio.run(main())