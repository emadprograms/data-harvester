/**
 * Data Harvester Dashboard - Harvester Controls, Runner Logs & Symbol Management
 */

// --- Harvester Automation & Logs ---
async function triggerHarvestRun() {
  const dateInput = document.getElementById('harvester-date-input');
  const dateVal = dateInput ? dateInput.value : null;
  const btn = document.getElementById('harvest-run-btn');
  if (btn) {
    btn.disabled = true;
    btn.innerText = "⏳ Dispatching...";
  }

  try {
    const res = await fetch(`${API_BASE}/api/harvester/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: dateVal || null })
    });
    const data = await res.json();
    if (data.success) {
      showToast("🚀 Harvest job started in background!");
      pollHarvesterLogs();
    } else {
      showToast(`❌ Error: ${data.message}`, "error");
    }
  } catch (err) {
    showToast("❌ Network error starting harvest", "error");
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerText = "▶ Trigger Harvest";
    }
  }
}

async function pollHarvesterLogs() {
  try {
    const res = await fetch(`${API_BASE}/api/harvester/logs`);
    if (!res.ok) return;
    const data = await res.json();
    const term = document.getElementById('terminal-logs');
    if (term && data.logs && data.logs.length > 0) {
      term.innerText = data.logs.join('');
      term.scrollTop = term.scrollHeight;
    }

    const resSt = await fetch(`${API_BASE}/api/harvester/status`);
    if (resSt.ok) {
      const stData = await resSt.json();
      const pill = document.getElementById('harvester-status-pill');
      if (pill) {
        pill.innerText = stData.status;
        pill.className = `px-2 py-0.5 rounded text-[11px] font-bold font-mono ${
          stData.status === 'RUNNING' ? 'bg-amber-950 text-amber-400 border border-amber-800' :
          stData.status === 'COMPLETED' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' :
          stData.status === 'FAILED' ? 'bg-rose-950 text-rose-400 border border-rose-800' :
          'bg-slate-800 text-slate-300'
        }`;
      }
    }
  } catch (err) {
    console.error("Error polling logs:", err);
  }
}

function clearTerminalLogs() {
  const term = document.getElementById('terminal-logs');
  if (term) term.innerText = "Console cleared.\n";
}

// --- Stream Hot Reload ---
async function triggerStreamReload() {
  try {
    const res = await fetch(`${API_BASE}/api/streamer/reload`, { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      showToast("⚡ Hot-reload signal triggered! Streamer updating subscriptions.");
    }
  } catch (err) {
    showToast("❌ Could not trigger streamer reload", "error");
  }
}

// --- Symbol Management Modals ---
function openAddSymbolModal() {
  const modal = document.getElementById('add-symbol-modal');
  const input = document.getElementById('input-display');
  if (modal) modal.classList.remove('hidden');
  if (input) input.focus();
}

function closeAddSymbolModal() {
  const modal = document.getElementById('add-symbol-modal');
  if (modal) modal.classList.add('hidden');
}

async function handleAddSymbol(e) {
  e.preventDefault();
  const disp = document.getElementById('input-display').value.trim().toUpperCase();
  const cap = document.getElementById('input-capital').value.trim().toUpperCase() || disp;
  const mas = document.getElementById('input-massive').value.trim().toUpperCase() || disp;
  const yah = document.getElementById('input-yahoo').value.trim().toUpperCase() || null;

  try {
    const res = await fetch(`${API_BASE}/api/symbols`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        display_name: disp,
        capital_ticker: cap,
        massive_ticker: mas,
        yahoo_ticker: yah
      })
    });
    const data = await res.json();
    if (data.success) {
      showToast(`✅ Symbol ${disp} added and streamer hot-reloaded!`);
      closeAddSymbolModal();
      const form = document.getElementById('add-symbol-form');
      if (form) form.reset();
      fetchAllData();
    } else {
      showToast(`❌ Error: ${data.error}`, 'error');
    }
  } catch (err) {
    showToast(`❌ Network error adding symbol`, 'error');
  }
}

async function handleDeleteSymbol(symbol) {
  if (!confirm(`Are you sure you want to remove ${symbol} from active tracking?`)) return;
  try {
    const res = await fetch(`${API_BASE}/api/symbols/${encodeURIComponent(symbol)}`, { method: 'DELETE' });
    const data = await res.json();
    if (data.success) {
      showToast(`🗑️ Symbol ${symbol} removed and streamer signaled!`);
      fetchAllData();
    } else {
      showToast(`❌ Error removing symbol`, 'error');
    }
  } catch (err) {
    showToast(`❌ Network error`, 'error');
  }
}

function openHarvestModal() {
  switchTab('harvester');
}
