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
ACTIONS_URL = f"https://github.com/{GITHUB_REPO}/actions"

# Setup intents for message reading
intents = discord.Intents.default()
intents.message_content = True

# Initialize Bot
bot = commands.Bot(command_prefix="!", intents=intents)

# -----------------------------------------------------------------------------
# Internal Logic Helpers
# -----------------------------------------------------------------------------

def get_target_date(date_input: str = None) -> str | None:
    """
    Parses date input. Supports:
    - None -> Returns None (Forces picker)
    - "0" -> Today (UTC)
    - "-1", "-2", etc. -> Days relative to today
    - "YYYY-MM-DD" -> Specific date
    """
    today = datetime.now(timezone.utc)
    if not date_input:
        return None
    
    if date_input == "0":
        return today.strftime("%Y-%m-%d")
    
    # Handle relative dates (e.g. -1, -5)
    if date_input.startswith("-") and date_input[1:].isdigit():
        try:
            days_back = int(date_input[1:])
            target = today - timedelta(days=days_back)
            return target.strftime("%Y-%m-%d")
        except: pass

    return date_input # Return as-is for validation later

async def trigger_github_harvest(interaction_or_ctx, date_str: str):
    """Triggers the GitHub Actions workflow."""
    # Validate date first
    try:
        target_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        now_utc = datetime.now(timezone.utc).date()
        if target_dt > now_utc:
            msg = f"❌ **Future Date Error:** `{date_str}` hasn't happened yet!"
            if isinstance(interaction_or_ctx, discord.Interaction):
                await interaction_or_ctx.response.send_message(msg, ephemeral=True)
            else:
                await interaction_or_ctx.send(msg)
            return
    except ValueError:
        msg = f"❌ **Format Error:** `{date_str}` is not YYYY-MM-DD."
        if isinstance(interaction_or_ctx, discord.Interaction):
            await interaction_or_ctx.response.send_message(msg, ephemeral=True)
        else:
            await interaction_or_ctx.send(msg)
        return

    # Send initial status
    if isinstance(interaction_or_ctx, discord.Interaction):
        if not interaction_or_ctx.response.is_done():
            await interaction_or_ctx.response.send_message(f"⏳ **Starting the harvest for {date_str}...** 🚀")
        status_msg = await interaction_or_ctx.original_response()
    else:
        status_msg = await interaction_or_ctx.send(f"⏳ **Starting the harvest for {date_str}...** 🚀")

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

            link = f"\n\n🔗 [Monitor Progress](<{run_url or ACTIONS_URL}>) 📡⏱️"
            await status_msg.edit(content=f"✅ **Harvest Triggered for {date_str}!**{link}")
        else:
            err = response.json().get("message", response.text)
            await status_msg.edit(content=f"❌ **GitHub API Error:** `{err}`")
    except Exception as e:
        await status_msg.edit(content=f"⚠️ **System Error:** `{str(e)}`")

# -----------------------------------------------------------------------------
# UI Components
# -----------------------------------------------------------------------------

class CustomDateModal(ui.Modal, title='Enter Custom Date'):
    def __init__(self, action_callback, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.action_callback = action_callback

    date_val = ui.TextInput(
        label='Date (YYYY-MM-DD)',
        placeholder='2026-02-22',
        required=True,
        min_length=10,
        max_length=10
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.action_callback(interaction, self.date_val.value)

class DateSelectionView(ui.View):
    def __init__(self, action_callback):
        super().__init__(timeout=180)
        self.action_callback = action_callback
        
        options = []
        today = datetime.now(timezone.utc)
        for i in range(14):
            target = today - timedelta(days=i)
            date_str = target.strftime("%Y-%m-%d")
            if i == 0:
                label = "Today (0)"
            elif i == 1:
                label = "Yesterday (-1)"
            else:
                day_name = target.strftime("%A")
                label = f"{day_name} (-{i})"
            
            options.append(discord.SelectOption(label=label, description=date_str, value=date_str))
        
        self.add_item(DateDropdown(options, action_callback))

    @ui.button(label="⌨️ Manual Date Entry", style=discord.ButtonStyle.secondary)
    async def manual_date(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.send_modal(CustomDateModal(self.action_callback))

class DateDropdown(ui.Select):
    def __init__(self, options, action_callback):
        super().__init__(placeholder="📅 Select a date...", min_values=1, max_values=1, options=options)
        self.action_callback = action_callback

    async def callback(self, interaction: discord.Interaction):
        # Edit original message to show progress and remove the view
        await interaction.response.edit_message(content=f"🗓️ **Selected Date:** {self.values[0]}\nInitializing harvest... 🚀", view=None)
        await self.action_callback(interaction, self.values[0])

# -----------------------------------------------------------------------------
# Bot Commands
# -----------------------------------------------------------------------------

@bot.event
async def on_ready():
    print(f'✅ Harvester Bot Online | Logged in as: {bot.user.name}')

@bot.command(name="updatedata")
async def update_data(ctx, date_indicator: str = None):
    """
    Triggers a data harvest.
    !updatedata        -> Opens interactive date picker
    !updatedata 0      -> Today
    !updatedata -1     -> Yesterday
    !updatedata YYYY-MM-DD -> Specific Date
    """
    target_date = get_target_date(date_indicator)

    if not target_date:
        # Show interactive picker
        view = DateSelectionView(action_callback=trigger_github_harvest)
        await ctx.send("🗓️ **Select Date for Data Harvest:**", view=view)
    else:
        # Direct trigger with validation
        await trigger_github_harvest(ctx, target_date)

if __name__ == "__main__":
    if not DISCORD_TOKEN or not GITHUB_TOKEN:
        print("❌ CRITICAL: Missing tokens.")
        exit(1)
    bot.run(DISCORD_TOKEN)
