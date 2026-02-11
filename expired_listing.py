"""
MCP TOOL: WA Real Estate Lead Scraper (Production v4)
DESCRIPTION: 1:1 UI Mirroring. Zero Duplicates. Dynamic Filters.
             - Filename: WA_[Status]_[Zip]_[Date].csv
             - Scrape All: If max_limit is None or 0, it pulls every available record.
             - Clickability: Standard Excel/Sheets HYPERLINK formulas.
"""

import os
import csv
import asyncio
import math
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')
PROJECT_DIR = "/Users/bharaths/Projects/expleads"

async def apply_dynamic_filters(page, zip_code=None, status=None):
    """
    Applies filters if provided. If blank, pulls the general pool.
    """
    if zip_code or status:
        is_collapsed = await page.locator('div.ibox.collapsed').is_visible()
        if is_collapsed:
            await page.locator('.collapse-link').click()

        if zip_code:
            await page.locator('input[name="zip"]').fill(str(zip_code))
            print(f"🔍 Applying Zip Filter: {zip_code}")

        if status:
            status_container = page.locator("#mls_status_chosen")
            await status_container.get_by_role("textbox").click()
            await page.locator(f'#mls_status_chosen li:has-text("{status}")').click()
            print(f"🔍 Applying Status Filter: {status}")

        await page.locator('button:has-text("Update List")').click()
        await page.wait_for_load_state('networkidle')
    else:
        print("🌐 No filters applied. Scraping general pool...")

    try:
        total_text = await page.locator('h5:has-text("Total Prospects")').inner_text()
        total_leads = int(total_text.split()[0].replace(',', ''))
        return total_leads
    except:
        return 0

async def fetch_lead_details(context, lead, semaphore):
    """Deep-scrapes details and builds CLICKABLE research links."""
    async with semaphore:
        page = await context.new_page()
        try:
            await page.goto(f"https://data.cofoundersgroup.com/view_lead/{lead['id']}", timeout=60000)
            attr = {}
            rows = await page.locator('table tr').all()
            for r in rows:
                cells = await r.locator('td').all()
                if len(cells) >= 2:
                    k = (await cells[0].inner_text()).strip().lower().replace(" ", "_")
                    v = (await cells[1].inner_text()).strip()
                    attr[k] = v

            phone = attr.get('phone/s') or attr.get('phone', 'N/A')
            addr = attr.get('address1', 'N/A')
            city = attr.get('city', 'N/A')
            
            z_link, r_link, g_link = "", "", ""
            if addr != 'N/A' and addr.strip() and addr.lower() != 'null':
                query = urllib.parse.quote(f"{addr} {city} WA")
                z_link = f'=HYPERLINK("https://www.zillow.com/homes/{query}_rb/", "Zillow")'
                r_link = f'=HYPERLINK("https://www.redfin.com/search?q={query}", "Redfin")'
                g_link = f'=HYPERLINK("https://www.google.com/maps/search/{query}", "Google Maps")'

            return [
                lead['name'], attr.get('lead_source', 'N/A'), phone,
                attr.get('email/s') or attr.get('email', 'N/A'),
                addr, city, attr.get('zip', 'N/A'), attr.get('date_added', 'N/A'),
                z_link, r_link, g_link, lead['id']
            ]
        except:
            return [lead['name'], "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "", "", "", lead['id']]
        finally:
            await page.close()

async def run_scrape(zip_code=None, status=None, max_limit=None):
    """
    Main Logic.
    If max_limit is None or 0, it scrapes ALL available records.
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    z_label = zip_code if zip_code else "ALL"
    s_label = status if status else "General"
    filename = f"WA_{s_label}_{z_label}_{current_date}.csv"
    
    processed_registry = set()
    leads_saved = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Auth
        await page.goto('https://data.cofoundersgroup.com/login')
        await page.fill('input[type="email"]', EMAIL)
        await page.fill('input[type="password"]', PASSWORD)
        await page.click('button[type="submit"]')
        await page.wait_for_load_state('networkidle')

        await page.goto("https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg=1")
        total_available = await apply_dynamic_filters(page, zip_code, status)
        
        # LOGIC: Determine how many to scrape
        if max_limit and max_limit > 0:
            to_process = min(total_available, max_limit)
        else:
            to_process = total_available # SCRAPE ALL MODE
        
        pages_needed = math.ceil(to_process / 25)
        print(f"📊 PORTAL AUDIT: Found {total_available} total. Scraper will capture: {to_process}")

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['Name', 'Status', 'Phone', 'Email', 'Address', 'City', 'Zip', 'Date Added', 'Zillow', 'Redfin', 'Google Maps', 'Internal ID'])
            
            for i in range(pages_needed):
                if leads_saved >= to_process: break
                print(f"📂 Processing Page {i+1}...")

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

                # FIXED: Immediate Locking to prevent duplicates
                unique_batch = []
                for l in raw_leads:
                    l_id = str(l['id'])
                    if l_id not in processed_registry and leads_saved < to_process:
                        processed_registry.add(l_id)
                        unique_batch.append(l)

                if unique_batch:
                    semaphore = asyncio.Semaphore(15) 
                    tasks = [fetch_lead_details(context, lead, semaphore) for lead in unique_batch]
                    results = await asyncio.gather(*tasks)
                    for row in results:
                        if row:
                            writer.writerow(row)
                            leads_saved += 1
                            print(f"  [{leads_saved}] Saved: {row[0]}")
                    f.flush()
                
                if i < pages_needed - 1:
                    await page.locator('li.next a, a:has-text("Next")').first.click()
                    await page.wait_for_load_state('networkidle')

        await browser.close()
        print(f"\n🏁 FINISHED: {leads_saved} unique leads saved to {filename}")
        return filename

if __name__ == "__main__":
    # MANUAL TEST:
    # Set max_limit=None to scrape everything.
    # Set zip_code=None and status=None to scrape general pool.
    asyncio.run(run_scrape(zip_code="98072", status="Expired", max_limit=None))