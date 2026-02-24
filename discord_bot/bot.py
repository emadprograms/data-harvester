import os
import discord
from discord.ext import commands
from discord import ui
import requests
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

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

# -----------------------------------------------------------------------------
# Validation Helpers
# -----------------------------------------------------------------------------

def validate_date(date_str: str):
    """
    Validates a date string and returns (is_valid, error_message, formatted_date).
    """
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        now_utc = datetime.now(timezone.utc).date()
        
        # Safety Check 1: No Future Dates
        if target_date > now_utc:
            return False, f"❌ **Future Date Error:** `{date_str}` hasn't happened yet! I can't harvest data from the future.", None
            
        # Safety Check 2: API Historical Limits (90 Days)
        oldest_allowed = now_utc - timedelta(days=90)
        if target_date < oldest_allowed:
            return False, f"❌ **Historical Limit:** `{date_str}` is too old. API limits restrict harvests to the last 90 days.", None
            
        return True, None, target_date.strftime("%Y-%m-%d")
    except ValueError:
        return False, f"❌ **Format Error:** `{date_str}` is not a valid date. Please use `YYYY-MM-DD`.", None

def get_help_embed():
    """Returns a nicely formatted help embed for the !updatedata command."""
    embed = discord.Embed(
        title="🚜 Harvester Command Guide",
        description="Trigger a manual data harvest for specific dates.",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="Usage", 
        value="`!updatedata` (Opens interactive picker)\n`!updatedata 0` (Today)\n`!updatedata -1` (Yesterday)\n`!updatedata YYYY-MM-DD` (Specific Date)", 
        inline=False
    )
    embed.add_field(
        name="Examples", 
        value="`!updatedata -5`\n`!updatedata 2026-02-10`", 
        inline=False
    )
    embed.add_field(
        name="Constraints", 
        value="• No future dates allowed.\n• Maximum 90 days in the past.", 
        inline=False
    )
    return embed

# -----------------------------------------------------------------------------
# GitHub Trigger Helper
# -----------------------------------------------------------------------------

async def trigger_github_harvest(interaction_or_ctx, date_str: str):
    """Triggers the GitHub Actions workflow."""
    # Send initial status
    if isinstance(interaction_or_ctx, discord.Interaction):
        if not interaction_or_ctx.response.is_done():
            await interaction_or_ctx.response.send_message(f"⏳ **Initializing harvest for `{date_str}`...**")
        status_msg = await interaction_or_ctx.original_response()
    else:
        status_msg = await interaction_or_ctx.send(f"⏳ **Initializing harvest for `{date_str}`...**")

    # Prepare GitHub API request
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILENAME}/dispatches"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    data = {"ref": "main", "inputs": {"target_date": date_str}}
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 204:
            run_url = None
            runs_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILENAME}/runs"
            
            # Wait loop to find the run link
            for attempt in range(4):
                await asyncio.sleep(2 + attempt)
                try:
                    r = requests.get(runs_url, headers=headers, params={"per_page": 1})
                    if r.status_code == 200:
                        runs = r.json().get("workflow_runs", [])
                        if runs:
                            run_url = runs[0].get("html_url")
                            break
                except: pass

            link = f"\n\n🔗 **Live Progress:**\n{run_url}" if run_url else "\n\n⚠️ Workflow started. Check GitHub Actions for progress."
            await status_msg.edit(content=f"✅ **Harvest Triggered for `{date_str}`!**{link}")
        else:
            err = response.json().get("message", response.text)
            await status_msg.edit(content=f"❌ **GitHub API Error:** `{err}`")
    except Exception as e:
        await status_msg.edit(content=f"⚠️ **System Error:** `{str(e)}`")

# -----------------------------------------------------------------------------
# UI Components
# -----------------------------------------------------------------------------

class DateSelectionModal(ui.Modal, title='Manual Data Harvest'):
    date_input = ui.TextInput(
        label='Target Date (YYYY-MM-DD)',
        placeholder='e.g. 2026-02-18',
        min_length=10,
        max_length=10,
        default=datetime.now(timezone.utc).strftime('%Y-%m-%d')
    )

    async def on_submit(self, interaction: discord.Interaction):
        valid, error, date_str = validate_date(self.date_input.value)
        if not valid:
            await interaction.response.send_message(error, ephemeral=True)
            return
        await trigger_github_harvest(interaction, date_str)

class DatePickerView(ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @ui.button(label="Open Date Picker Modal", style=discord.ButtonStyle.primary, emoji="📅")
    async def open_modal(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(DateSelectionModal())

# -----------------------------------------------------------------------------
# Commands
# -----------------------------------------------------------------------------

@bot.event
async def on_ready():
    print(f'✅ Bot online as {bot.user}')

@bot.command(name="updatedata")
async def update_data(ctx, arg: str = None):
    """
    Triggers a data harvest. Supports offsets, dates, or interactive picker.
    """
    # 1. No Argument -> Show help and picker button
    if arg is None:
        await ctx.send(embed=get_help_embed(), view=DatePickerView())
        return

    # 2. Check for Offset (Integer)
    try:
        offset = int(arg)
        target_date = datetime.now(timezone.utc).date() + timedelta(days=offset)
        arg = target_date.strftime("%Y-%m-%d")
    except ValueError:
        pass # Not an integer, treat as date string

    # 3. Validate and Trigger
    is_valid, error_msg, final_date = validate_date(arg)
    if not is_valid:
        await ctx.send(content=error_msg, embed=get_help_embed())
        return

    await trigger_github_harvest(ctx, final_date)

@update_data.error
async def update_data_error(ctx, error):
    """Global error handler for !updatedata."""
    await ctx.send(f"⚠️ **Command Error:** {str(error)}", embed=get_help_embed())

if __name__ == "__main__":
    if not DISCORD_TOKEN or not GITHUB_TOKEN:
        print("❌ CRITICAL: Missing tokens.")
        exit(1)
    bot.run(DISCORD_TOKEN)
