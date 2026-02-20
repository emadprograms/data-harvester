import os
import discord
from discord.ext import commands
import requests
import asyncio
from dotenv import load_dotenv

# Load local environment variables if present
load_dotenv()

# Configuration from Environment Variables
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_PAT")
GITHUB_REPO = os.getenv("GITHUB_REPO", "emadprograms/data-harvester")
WORKFLOW_FILENAME = os.getenv("WORKFLOW_FILENAME", "harvest.yml")

# Setup intents for message reading
intents = discord.Intents.default()
intents.message_content = True

# Initialize Bot
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f'✅ Harvester Bot online: {bot.user.name} ({bot.user.id})')
    print('Ready for data collection.')

@bot.command(name="harvest")
async def trigger_harvest(ctx):
    """Triggers the data harvest workflow."""
    
    # Visual feedback so user knows it instantly triggered
    status_msg = await ctx.send("� **Starting the harvest...**")
    
    # Prepare GitHub API request
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILENAME}/dispatches"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    # We trigger the workflow on the 'main' branch
    data = {
        "ref": "main"
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        # GitHub returns 204 No Content on a successful dispatch
        if response.status_code == 204:
            # Retry loop to fetch the latest run for this workflow (it can take time to appear)
            run_url = None
            runs_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILENAME}/runs"
            
            for attempt in range(3):
                await asyncio.sleep(2 + (attempt * 2))  # Exponentially wait: 2s, 4s, 6s
                try:
                    runs_response = requests.get(runs_url, headers=headers, params={"per_page": 1})
                    if runs_response.status_code == 200:
                        runs_data = runs_response.json()
                        runs = runs_data.get("workflow_runs", [])
                        if runs:
                            run_url = runs[0].get("html_url")
                            break
                except Exception as e:
                    print(f"Attempt {attempt + 1} to fetch run link failed: {e}")

            if run_url:
                run_link_msg = f"\n\n🔗 **Live Workflow Link:**\n{run_url}\n*You can monitor the progress via the link above.*"
            else:
                run_link_msg = "\n\n⚠️ **Note:** Could not capture the live run link immediately. Please check the Actions tab manually."

            await status_msg.edit(content=f"✅ **Harvest successfully triggered!**{run_link_msg}\n\n> The final report will be posted here once the run completes.")
            print(f"Triggered harvest via Discord user: {ctx.author}")
        else:
            error_details = response.json().get("message", response.text)
            await status_msg.edit(content=f"❌ **Failed to trigger harvest.**\n> GitHub API Error: `{error_details}`")
            print(f"Failed to trigger: {response.status_code} - {response.text}")
            
    except Exception as e:
        await status_msg.edit(content=f"⚠️ **Internal Error:** Could not reach GitHub.\n`{str(e)}`")
        print(f"Exception triggering workflow: {e}")

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("❌ CRITICAL: DISCORD_BOT_TOKEN is missing.")
        exit(1)
    if not GITHUB_TOKEN:
        print("❌ CRITICAL: GITHUB_PAT is missing.")
        exit(1)
        
    print("Starting bot...")
    bot.run(DISCORD_TOKEN)
