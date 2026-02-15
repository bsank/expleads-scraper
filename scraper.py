"""
MCP TOOL: WA Real Estate Lead Scraper (Production v5)
DESCRIPTION: 1:1 UI Mirroring with Absolute Pathing.
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

# --- ⚙️ FOLDER CONFIGURATION ⚙️ ---
# This ensures the CSV saves to your MacBook folder, NOT Claude's system folder.
PROJECT_DIR = "/Users/bharaths/Projects/expleads"
# ---------------------------------

async def apply_site_filters(page, zip_code, status):
    """Navigates the UI and sets filters."""
    is_collapsed = await page.locator('div.ibox.collapsed').is_visible()
    if is_collapsed:
        await page.locator('.collapse-link').click()

    await page.locator('input[name="zip"]').fill(str(zip_code))
    status_container = page.locator("#mls_status_chosen")
    await status_container.get_by_role("textbox").click()
    await page.locator(f'#mls_status_chosen li:has-text("{status}")').click()

    await page.locator('button:has-text("Update List")').click()
    await page.wait_for_load_state('networkidle')
    
    try:
        total_text = await page.locator('h5:has-text("Total Prospects")').inner_text()
        total_leads = int(total_text.split()[0].replace(',', ''))
        return total_leads
    except Exception:
        return 0

async def fetch_and_verify(context, lead, semaphore, status):
    """Deep-scrapes lead details."""
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
                lead['name'], status, phone,
                attr.get('email/s') or attr.get('email', 'N/A'),
                addr, city, attr.get('zip', 'N/A'), attr.get('date_added', 'N/A'),
                z_link, r_link, g_link, lead['id']
            ]
        except Exception:
            return [lead['name'], status, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "", "", "", lead['id']]
        finally:
            await page.close()

async def run_scrape(zip_code="98072", status="Expired", max_limit=None):
    """
    Main Logic.
    Supports both Agent calls and Manual runs.
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"WA_{status}_{zip_code}_{current_date}.csv"
    
    # CRITICAL: This full path fixes the "read-only" error
    full_output_path = os.path.join(PROJECT_DIR, filename)
    
    processed_registry = set()
    lead_counter = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Login
        await page.goto('https://data.cofoundersgroup.com/login')
        await page.fill('input[type="email"]', EMAIL)
        await page.fill('input[type="password"]', PASSWORD)
        await page.click('button[type="submit"]')
        await page.wait_for_load_state('networkidle')

        await page.goto("https://data.cofoundersgroup.com/pool_view.php?id=16&sp=1&fltr=0&pg=1")
        total_available = await apply_site_filters(page, zip_code, status)
        
        # Limit Logic
        to_process = min(total_available, max_limit) if max_limit else total_available
        pages_needed = math.ceil(to_process / 25)

        print(f"📊 PORTAL AUDIT: Found {total_available}. Processing: {to_process}")

        with open(full_output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['Name', 'Status', 'Phone', 'Email', 'Address', 'City', 'Zip', 'Date Added', 'Zillow', 'Redfin', 'Google Maps', 'Internal ID'])
            
            for i in range(pages_needed):
                if lead_counter >= to_process: break
                
                page_raw_leads = await page.evaluate("""() => {
                    const results = [];
                    document.querySelectorAll('.leads').forEach(btn => {
                        results.push({ 
                            id: btn.getAttribute('data-contact-id'), 
                            name: btn.closest('tr').querySelector('td')?.innerText.trim() 
                        });
                    });
                    return results;
                }""")

                unique_batch = []
                for l in page_raw_leads:
                    if l['id'] not in processed_registry and lead_counter < to_process:
                        processed_registry.add(l['id'])
                        unique_batch.append(l)

                if unique_batch:
                    semaphore = asyncio.Semaphore(15) 
                    tasks = [fetch_and_verify(context, lead, semaphore, status) for lead in unique_batch]
                    results = await asyncio.gather(*tasks)
                    for row in results:
                        if row:
                            writer.writerow(row)
                            lead_counter += 1
                    f.flush()
                
                if i < pages_needed - 1:
                    await page.locator('li.next a, a:has-text("Next")').first.click()
                    await page.wait_for_load_state('networkidle')

        await browser.close()
        return full_output_path

if __name__ == "__main__":
    # This runs if you hit "Run" in VS Code
    asyncio.run(run_scrape(zip_code="98072", status="Expired"))