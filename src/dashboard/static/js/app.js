/**
 * Data Harvester Dashboard - Application Bootstrap, Routing & Coordination
 */

// --- Tab Switching & Navigation ---
function switchTab(tabId) {
  const tabs = ['charts', 'stream', 'symbols', 'integrity', 'harvester'];
  tabs.forEach(t => {
    const el = document.getElementById(`tab-${t}`);
    const btn = document.getElementById(`tab-btn-${t}`);
    if (el) el.classList.add('hidden');
    if (btn) {
      btn.className = "py-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200 flex items-center gap-2 font-medium";
    }
  });

  const activeEl = document.getElementById(`tab-${tabId}`);
  const activeBtn = document.getElementById(`tab-btn-${tabId}`);
  if (activeEl) activeEl.classList.remove('hidden');
  if (activeBtn) {
    activeBtn.className = "py-3 border-b-2 border-emerald-500 text-white flex items-center gap-2 font-semibold";
  }

  if (tabId === 'charts') {
    if (!tvChart) {
      initChart();
    } else {
      resizeChart();
    }
  }
}

// --- Symbol Coverage Matrix Loader ---
async function fetchSymbolsCoverage() {
  try {
    const res = await fetch(`${API_BASE}/api/symbols/coverage`);
    if (!res.ok) return;
    const data = await res.json();
    allSymbolsCoverage = data.symbols || [];

    // KPI Counters
    const kpiRows = document.getElementById('kpi-hist-rows');
    const kpiSyms = document.getElementById('kpi-symbols-count');
    if (kpiRows) kpiRows.innerText = Number(data.total_bars_database || 0).toLocaleString();
    if (kpiSyms) kpiSyms.innerText = `${data.total_symbols || 0} symbols`;

    // Populate Symbol Selector Dropdown if not already fully populated
    const select = document.getElementById('chart-symbol-select');
    if (select && select.children.length <= 8) {
      select.innerHTML = '';
      allSymbolsCoverage.forEach(s => {
        const opt = document.createElement('option');
        opt.value = s.display_name;
        opt.innerText = `${s.display_name} (${s.asset_class} • ${s.bar_count.toLocaleString()} bars)`;
        if (s.display_name === currentSymbol) opt.selected = true;
        select.appendChild(opt);
      });
    }

    renderSymbolMatrix(allSymbolsCoverage);
  } catch (err) {
    console.error("Error fetching symbols coverage:", err);
  }
}

// --- Master Polling Loop ---
function fetchAllData() {
  fetchSystemHealth();
  fetchMarketSession();
  fetchStreamStatus();
  fetchStreamTape();
  fetchSymbolsCoverage();
}

// --- Application Boot Initialization ---
window.addEventListener('DOMContentLoaded', () => {
  initChart();
  fetchAllData();
  setInterval(fetchMarketSession, 1000);
  setInterval(fetchStreamStatus, 4000);
  setInterval(fetchStreamTape, 3000);
  setInterval(pollHarvesterLogs, 5000);
});
