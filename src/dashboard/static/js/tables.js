/**
 * Data Harvester Dashboard - Table Renderers, Filters, and CSV Exporters
 */

// --- Monitored Streaming Symbols Management ---
async function loadStreamingSymbolsTable() {
  const tbody = document.getElementById('streaming-symbols-table-body');
  const countBadge = document.getElementById('streaming-symbols-count-badge');
  if (!tbody) return;

  try {
    const res = await fetch(`${API_BASE}/api/streaming/symbols`);
    if (!res.ok) {
      tbody.innerHTML = '<tr><td colspan="5" class="py-6 text-center text-rose-400">Failed to load monitored symbols.</td></tr>';
      return;
    }
    const data = await res.json();
    const symbols = data.symbols || [];
    renderStreamingSymbolsTable(symbols);

    if (countBadge) {
      countBadge.innerText = `${symbols.length} symbol${symbols.length === 1 ? '' : 's'}`;
    }
  } catch (err) {
    console.error("Error loading streaming symbols table:", err);
    tbody.innerHTML = '<tr><td colspan="5" class="py-6 text-center text-rose-400">Network error loading monitored symbols.</td></tr>';
  }
}

function renderStreamingSymbolsTable(symbols) {
  const tbody = document.getElementById('streaming-symbols-table-body');
  if (!tbody) return;
  tbody.innerHTML = '';

  if (!symbols || symbols.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="py-6 text-center text-slate-500">No symbols registered in streaming.duckdb. Add one above.</td></tr>';
    return;
  }

  symbols.forEach(s => {
    const sym = typeof s === 'string' ? s : (s.display_name || s.symbol || '');
    const capTicker = typeof s === 'object' ? (s.capital_ticker || '—') : '—';
    const dbnTicker = typeof s === 'object' ? (s.databento_ticker || '—') : '—';
    const isActive = typeof s === 'object' ? (s.is_active !== false) : true;

    const tr = document.createElement('tr');
    tr.className = "hover:bg-slate-800/40 transition";
    tr.innerHTML = `
      <td class="py-3 px-4 font-bold text-white flex items-center gap-2">
        <span class="h-2 w-2 rounded-full ${isActive ? 'bg-indigo-500' : 'bg-slate-600'}"></span>
        ${sym}
      </td>
      <td class="py-3 px-4 text-slate-300 font-mono text-[11px]">${capTicker}</td>
      <td class="py-3 px-4 text-slate-400 font-mono text-[11px]">${dbnTicker}</td>
      <td class="py-3 px-4 text-center">
        <span class="px-2 py-0.5 rounded text-[10px] font-mono ${isActive ? 'bg-indigo-950 text-indigo-400 border border-indigo-800' : 'bg-slate-800 text-slate-400'} font-bold">
          ${isActive ? 'TRACKED' : 'INACTIVE'}
        </span>
      </td>
      <td class="py-3 px-4 text-right space-x-1 whitespace-nowrap">
        <button data-symbol="${sym}" onclick="deleteStreamingSymbol('${sym}')" class="px-2.5 py-1 bg-rose-950/60 hover:bg-rose-900 border border-rose-800 text-rose-300 rounded text-[11px] font-semibold transition">
          Remove
        </button>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

async function addStreamingSymbol() {
  const symInput = document.getElementById('add-streaming-symbol-input');
  const capInput = document.getElementById('add-streaming-capital-input');
  const dbnInput = document.getElementById('add-streaming-databento-input');

  const symbol = symInput ? symInput.value.trim().toUpperCase() : '';
  const capitalTicker = capInput ? capInput.value.trim() : '';
  const databentoTicker = dbnInput ? dbnInput.value.trim() : '';

  if (!symbol) {
    showToast("Please enter a symbol display name", "error");
    if (symInput) symInput.focus();
    return;
  }

  try {
    const res = await fetch(`${API_BASE}/api/streaming/symbols`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        symbol: symbol,
        display_name: symbol,
        capital_ticker: capitalTicker || symbol,
        databento_ticker: databentoTicker || symbol,
      })
    });
    const data = await res.json();
    if (data.success) {
      showToast(`✅ Streaming symbol ${symbol} added and streamer signaled!`);
      if (symInput) symInput.value = '';
      if (capInput) capInput.value = '';
      if (dbnInput) dbnInput.value = '';
      await loadStreamingSymbolsTable();
      if (typeof fetchStreamingSymbols === 'function') {
        await fetchStreamingSymbols();
      }
    } else {
      showToast(`❌ Error: ${data.error || 'Failed to add symbol'}`, 'error');
    }
  } catch (err) {
    showToast("❌ Network error adding streaming symbol", "error");
  }
}

async function deleteStreamingSymbol(symbol) {
  if (!confirm(`Are you sure? Removing this symbol will delete all its tick data from streaming.duckdb.`)) {
    return;
  }

  try {
    const res = await fetch(`${API_BASE}/api/streaming/symbols/${encodeURIComponent(symbol)}`, {
      method: 'DELETE'
    });
    const data = await res.json();
    if (data.success) {
      showToast(`🗑️ Streaming symbol ${symbol} and all tick data deleted!`);
      await loadStreamingSymbolsTable();
      if (typeof fetchStreamingSymbols === 'function') {
        await fetchStreamingSymbols();
      }
    } else {
      showToast(`❌ Error: ${data.error || 'Failed to delete symbol'}`, 'error');
    }
  } catch (err) {
    showToast("❌ Network error deleting streaming symbol", "error");
  }
}

window.loadStreamingSymbolsTable = loadStreamingSymbolsTable;
window.renderStreamingSymbolsTable = renderStreamingSymbolsTable;
window.addStreamingSymbol = addStreamingSymbol;
window.deleteStreamingSymbol = deleteStreamingSymbol;

