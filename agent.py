from mcp.server.fastmcp import FastMCP
import expired_listing as scraper # This imports your run_scrape function
import asyncio

# Initialize the MCP Server
mcp = FastMCP("WA-Real-Estate-Agent")

@mcp.tool()
async def fetch_expired_leads(zip_code: str = None, status: str = "Expired", max_limit: int = None):
    """
    Search for real estate leads on the portal.
    Args:
        zip_code: The 5-digit zip code (e.g., '98072'). Leave blank for all.
        status: The lead status (e.g., 'Expired', 'Active'). Defaults to Expired.
        max_limit: Number of leads to fetch. Leave blank to fetch ALL records.
    """
    # Notify the user via the agent console
    print(f"🤖 Agent: Starting scrape for {status} leads in Zip {zip_code or 'All'}...")
    
    # Trigger your actual scraper logic
    # We use await because run_scrape is an async function
    try:
        filename = await scraper.run_scrape(
            zip_code=zip_code, 
            status=status, 
            max_limit=max_limit
        )
        return f"✅ Scrape Complete! I found the leads and saved them to: {filename}"
    except Exception as e:
        return f"❌ Error during scrape: {str(e)}"

if __name__ == "__main__":
    mcp.run()