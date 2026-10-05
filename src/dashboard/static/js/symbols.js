/**
 * Data Harvester Dashboard - Symbol Management & Streamer Reload
 */
// --- Harvester Automation & Logs ---
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
  const dbn = document.getElementById('input-databento').value.trim().toUpperCase() || disp;

  try {
    const res = await fetch(`${API_BASE}/api/streaming/symbols`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        display_name: disp,
        capital_ticker: cap,
        databento_ticker: dbn
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
    const res = await fetch(`${API_BASE}/api/streaming/symbols/${encodeURIComponent(symbol)}`, { method: 'DELETE' });
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

