from mcp.server.fastmcp import FastMCP
import scraper as scraper # Ensure this matches your filename (2 p's)
import asyncio
import pandas as pd
import os

# Initialize the MCP Server
mcp = FastMCP("WA-Real-Estate-Agent")

@mcp.tool()
async def list_and_read_leads():
    """
    Finds the most recent lead CSV file in the project folder and reads it.
    Use this when you need to analyze the data after a scrape.
    """
    folder = "/Users/bharaths/Projects/expleads"
    try:
        # Get all CSV files in the folder
        files = [f for f in os.listdir(folder) if f.endswith('.csv')]
        if not files:
            return "❌ No CSV lead files found in the folder."
        
        # Sort by creation time to find the newest one
        files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
        latest_file = files[0]
        file_path = os.path.join(folder, latest_file)
        
        # Read the file to return to Claude
        df = pd.read_csv(file_path)
        return f"📖 Reading latest file: {latest_file}\n\n{df.to_csv(index=False)}"
    except Exception as e:
        return f"❌ Error reading files: {str(e)}"

@mcp.tool()
async def analyze_leads(csv_data: str):
    """
    Takes raw CSV text and summarizes the top 5 most 
    promising leads based on 'Date Added'.
    """
    try:
        from io import StringIO
        df = pd.read_csv(StringIO(csv_data))
        
        # Ensure Date Added is treated as a date for sorting
        df['Date Added'] = pd.to_datetime(df['Date Added'])
        top_leads = df.sort_values(by='Date Added', ascending=False).head(5)
        
        summary = "✅ Top 5 Newest Leads:\n"
        for _, row in top_leads.iterrows():
            summary += f"- {row['Name']} in {row['City']} (Phone: {row['Phone']})\n"
        return summary
    except Exception as e:
        return f"❌ Error analyzing leads: {e}"

@mcp.tool()
async def fetch_expired_leads(zip_code: str = None, status: str = "Expired", max_limit: int = None):
    """
    Search for real estate leads on the portal.
    Args:
        zip_code: The 5-digit zip code (e.g., '98072').
        status: The lead status (e.g., 'Expired', 'Active').
        max_limit: Number of leads to fetch.
    """
    print(f"🤖 Agent: Starting scrape for {status} leads in Zip {zip_code or 'All'}...")
    
    try:
        filename = await scraper.run_scrape(
            zip_code=zip_code, 
            status=status, 
            max_limit=max_limit
        )
        return f"✅ Scrape Complete! Saved to: {filename}"
    except Exception as e:
        return f"❌ Error during scrape: {str(e)}"

if __name__ == "__main__":
    mcp.run()