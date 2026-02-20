import os
import discord
from discord.ext import commands
import requests
import json
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
    print(f'✅ Logged in as {bot.user.name} ({bot.user.id})')
    print('Bot is ready to receive commands.')

@bot.command(name="harvest")
async def trigger_harvest(ctx):
    """Triggers the GitHub Actions harvest workflow."""
    
    # Visual feedback so user knows it instantly triggered
    status_msg = await ctx.send("🚀 Sending signal to GitHub Actions...")
    
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
            await status_msg.edit(content="✅ **Harvester workflow successfully triggered!**\n> It may take a few seconds for the runner to boot. You will receive the final report here shortly.")
            print(f"Triggered harvest via Discord user: {ctx.author}")
        else:
            error_details = response.json().get("message", response.text)
            await status_msg.edit(content=f"❌ **Failed to trigger workflow.**\nGitHub API Error ({response.status_code}): `{error_details}`")
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
