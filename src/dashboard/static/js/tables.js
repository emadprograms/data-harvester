/**
 * Data Harvester Dashboard - Table Renderers, Filters, and CSV Exporters
 */

// --- Raw Candle Inspector Table ---
function renderInspectorTable(candles) {
  const tbody = document.getElementById('inspector-table-body');
  const badge = document.getElementById('inspector-count-badge');
  const thead = document.getElementById('inspector-thead');
  const desc = document.getElementById('inspector-desc');
  if (!tbody) return;
  tbody.innerHTML = '';

  if (thead) {
    if (currentDbSource === 'streaming') {
      thead.innerHTML = `
        <tr>
          <th class="py-2.5 px-4">Timestamp (UTC)</th>
          <th class="py-2.5 px-4 text-right">Open</th>
          <th class="py-2.5 px-4 text-right">High</th>
          <th class="py-2.5 px-4 text-right">Low</th>
          <th class="py-2.5 px-4 text-right">Close</th>
          <th class="py-2.5 px-4 text-right">Volume</th>
          <th class="py-2.5 px-4 text-center">Ticks</th>
          <th class="py-2.5 px-4 text-center">Source</th>
        </tr>
      `;
    } else {
      thead.innerHTML = `
        <tr>
          <th class="py-2.5 px-4">Timestamp (UTC)</th>
          <th class="py-2.5 px-4 text-right">Open</th>
          <th class="py-2.5 px-4 text-right">High</th>
          <th class="py-2.5 px-4 text-right">Low</th>
          <th class="py-2.5 px-4 text-right">Close</th>
          <th class="py-2.5 px-4 text-right">Volume</th>
          <th class="py-2.5 px-4 text-center">Session</th>
          <th class="py-2.5 px-4 text-center">Source</th>
        </tr>
      `;
    }
  }

  if (desc) {
    desc.innerText = currentDbSource === 'historical'
      ? "Inspect underlying 1-minute or aggregated bars directly from data/historical.duckdb."
      : "Inspect dynamically resampled candles from raw tick quotes in data/streaming.duckdb.";
  }

  if (badge) {
    const dbName = currentDbSource === 'historical' ? 'historical.duckdb' : 'streaming.duckdb';
    badge.innerText = `${candles.length.toLocaleString()} bars (${dbName})`;
  }

  if (!candles || candles.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="py-6 text-center text-slate-500">No bars found for current selection.</td></tr>';
    return;
  }

  // Show newest first in table
  const reversed = [...candles].reverse();
  reversed.slice(0, 150).forEach(c => {
    const tr = document.createElement('tr');
    tr.className = "hover:bg-slate-800/40 transition";
    const isUp = c.close >= c.open;

    const tagCol = currentDbSource === 'streaming'
      ? `<span class="px-2 py-0.5 rounded text-[10px] bg-slate-800 text-indigo-400 font-mono font-bold">${c.tick_count || 1} ticks</span>`
      : `<span class="px-2 py-0.5 rounded text-[10px] bg-slate-800 text-slate-400 font-mono">${c.session || 'REG'}</span>`;

    tr.innerHTML = `
      <td class="py-2.5 px-4 font-mono text-slate-300">${c.time_str}</td>
      <td class="py-2.5 px-4 text-right font-mono">${c.open ? c.open.toFixed(2) : '--'}</td>
      <td class="py-2.5 px-4 text-right font-mono text-emerald-400">${c.high ? c.high.toFixed(2) : '--'}</td>
      <td class="py-2.5 px-4 text-right font-mono text-rose-400">${c.low ? c.low.toFixed(2) : '--'}</td>
      <td class="py-2.5 px-4 text-right font-mono font-bold ${isUp ? 'text-emerald-400' : 'text-rose-400'}">${c.close ? c.close.toFixed(2) : '--'}</td>
      <td class="py-2.5 px-4 text-right font-mono text-slate-400">${Number(c.volume || 0).toLocaleString()}</td>
      <td class="py-2.5 px-4 text-center">
        ${tagCol}
      </td>
      <td class="py-2.5 px-4 text-center">
        <span class="px-2 py-0.5 rounded text-[10px] ${
          c.source === 'MASSIVE' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' :
          c.source === 'BINANCE' ? 'bg-amber-950 text-amber-400 border border-amber-800' :
          c.source === 'CAPITAL_STREAM' ? 'bg-indigo-950 text-indigo-400 border border-indigo-800' :
          c.source === 'CAPITAL' ? 'bg-blue-950 text-blue-400 border border-blue-800' :
          'bg-slate-800 text-slate-300'
        } font-mono font-bold">${c.source || 'UNKNOWN'}</span>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function filterInspectorTable() {
  const input = document.getElementById('inspector-search');
  if (!input) return;
  const q = input.value.trim().toLowerCase();
  if (!q) {
    renderInspectorTable(loadedCandles);
    return;
  }
  const filtered = loadedCandles.filter(c => c.time_str.toLowerCase().includes(q));
  renderInspectorTable(filtered);
}

function exportCandlesToCSV() {
  if (!loadedCandles || loadedCandles.length === 0) {
    showToast("No candle data to export", "error");
    return;
  }

  const headers = ["Timestamp", "Open", "High", "Low", "Close", "Volume", "Source", "Session"];
  const rows = loadedCandles.map(c => [
    `"${c.time_str}"`, c.open, c.high, c.low, c.close, c.volume, `"${c.source}"`, `"${c.session}"`
  ]);

  const csvContent = "data:text/csv;charset=utf-8," + [headers.join(","), ...rows.map(e => e.join(","))].join("\n");
  const encodedUri = encodeURI(csvContent);
  const link = document.createElement("a");
  link.setAttribute("href", encodedUri);
  link.setAttribute("download", `${currentSymbol}_${currentTimeframe}_candles.csv`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  showToast(`📥 Exported ${loadedCandles.length} candles to CSV!`);
}

// --- Symbol Matrix & Inventory Table ---
function renderSymbolMatrix(symbols) {
  const tbody = document.getElementById('symbols-matrix-body');
  if (!tbody) return;
  tbody.innerHTML = '';

  if (!symbols || symbols.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="py-6 text-center text-slate-500">No symbols found in symbol_map.</td></tr>';
    return;
  }

  symbols.forEach(s => {
    const tr = document.createElement('tr');
    tr.className = "hover:bg-slate-800/40 transition";
    const dateRangeStr = (s.first_timestamp && s.last_timestamp) 
      ? `${s.first_timestamp.split(' ')[0]} → ${s.last_timestamp.split(' ')[0]}` 
      : '—';

    tr.innerHTML = `
      <td class="py-3 px-4 font-bold text-white flex items-center gap-2">
        <span class="h-2 w-2 rounded-full ${s.freshness === 'FRESH' ? 'bg-emerald-500' : 'bg-slate-600'}"></span>
        ${s.display_name}
      </td>
      <td class="py-3 px-4 text-slate-400 font-mono text-[11px]">${s.asset_class}</td>
      <td class="py-3 px-4 text-right font-bold text-white font-mono">${s.bar_count.toLocaleString()}</td>
      <td class="py-3 px-4 text-slate-400 font-mono text-[11px]">${dateRangeStr}</td>
      <td class="py-3 px-4 text-right font-bold font-mono ${s.latest_close ? 'text-emerald-400' : 'text-slate-500'}">
        ${s.latest_close !== null ? `$${s.latest_close.toFixed(2)}` : '—'}
      </td>
      <td class="py-3 px-4 text-center">
        <span class="px-2 py-0.5 rounded text-[10px] font-mono ${
          s.freshness === 'FRESH' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' :
          s.freshness === 'STALE' ? 'bg-amber-950 text-amber-400 border border-amber-800' :
          'bg-slate-800 text-slate-400'
        }">${s.freshness}</span>
      </td>
      <td class="py-3 px-4 text-slate-300 font-mono text-[11px] truncate max-w-xs">
        ${s.sources_list}
      </td>
      <td class="py-3 px-4 text-right space-x-1 whitespace-nowrap">
        <button onclick="selectSymbolInChart('${s.display_name}')" class="px-2 py-1 bg-emerald-950/80 hover:bg-emerald-900 border border-emerald-800 text-emerald-300 rounded text-[11px] transition">
          Chart
        </button>
        <button onclick="auditSpecificSymbol('${s.display_name}')" class="px-2 py-1 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded text-[11px] transition">
          Audit
        </button>
        <button onclick="handleDeleteSymbol('${s.display_name}')" class="px-2 py-1 bg-rose-950/60 hover:bg-rose-900 border border-rose-800 text-rose-300 rounded text-[11px] transition">
          Remove
        </button>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function filterSymbolMatrix() {
  const input = document.getElementById('symbol-search');
  if (!input) return;
  const q = input.value.trim().toLowerCase();
  if (!q) {
    renderSymbolMatrix(allSymbolsCoverage);
    return;
  }
  const filtered = allSymbolsCoverage.filter(s => 
    s.display_name.toLowerCase().includes(q) || 
    s.asset_class.toLowerCase().includes(q) ||
    (s.sources_list && s.sources_list.toLowerCase().includes(q))
  );
  renderSymbolMatrix(filtered);
}

function auditSpecificSymbol(sym) {
  const target = document.getElementById('integrity-symbol-target');
  if (target) target.value = sym;
  switchTab('integrity');
  runIntegrityAudit();
}
