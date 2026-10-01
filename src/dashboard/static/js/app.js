/**
 * Data Harvester Dashboard - Application Bootstrap, Routing & Coordination
 */

// --- Dashboard View Switching (Sidebar Navigation) ---
function switchDashboardView(viewId) {
  currentDashboardView = viewId;
  const histView = document.getElementById('view-historical');
  const strmView = document.getElementById('view-streaming');
  const navHist = document.getElementById('nav-historical');
  const navStrm = document.getElementById('nav-streaming');

  if (viewId === 'historical') {
    if (histView) histView.classList.remove('hidden');
    if (strmView) strmView.classList.add('hidden');
    if (navHist) {
      navHist.className = "w-full text-left px-3 py-2.5 rounded-lg flex items-center gap-3 text-xs font-semibold bg-emerald-600/20 text-emerald-400 border border-emerald-500/30 transition";
    }
    if (navStrm) {
      navStrm.className = "w-full text-left px-3 py-2.5 rounded-lg flex items-center gap-3 text-xs font-medium text-slate-400 hover:text-white hover:bg-slate-800/60 border border-transparent transition";
    }
    if (!tvChart) {
      initHistoricalChart();
    } else {
      setTimeout(resizeHistoricalChart, 50);
    }
  } else if (viewId === 'streaming') {
    if (histView) histView.classList.add('hidden');
    if (strmView) strmView.classList.remove('hidden');
    if (navHist) {
      navHist.className = "w-full text-left px-3 py-2.5 rounded-lg flex items-center gap-3 text-xs font-medium text-slate-400 hover:text-white hover:bg-slate-800/60 border border-transparent transition";
    }
    if (navStrm) {
      navStrm.className = "w-full text-left px-3 py-2.5 rounded-lg flex items-center gap-3 text-xs font-semibold bg-indigo-600/20 text-indigo-400 border border-indigo-500/30 transition";
    }
    if (!tvStreamingChart) {
      initStreamingChart();
    } else {
      setTimeout(resizeStreamingChart, 50);
    }
    fetchStreamingSymbols();
  }
}

// --- Historical Tab Switching & Navigation ---
function switchTab(tabId) {
  if (tabId === 'stream') {
    switchDashboardView('streaming');
    return;
  }

  // Ensure we are in historical view
  switchDashboardView('historical');

  const tabs = ['charts', 'symbols', 'integrity', 'harvester'];
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
      initHistoricalChart();
    } else {
      setTimeout(resizeHistoricalChart, 50);
    }
  }
}

// --- Symbol Coverage Matrix Loader (Historical Symbols) ---
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

    // Populate Historical Symbol Selector Dropdown if not already populated
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
    console.error("Error fetching historical symbols coverage:", err);
  }
}

// --- Dedicated Streaming Symbols Loader ---
async function fetchStreamingSymbols() {
  try {
    const res = await fetch(`${API_BASE}/api/streaming/symbols`);
    if (!res.ok) return;
    const data = await res.json();
    streamingSymbolsList = data.symbols || [];

    const select = document.getElementById('streaming-symbol-select');
    if (select) {
      select.innerHTML = '';
      streamingSymbolsList.forEach(s => {
        const sym = typeof s === 'string' ? s : (s.display_name || s.symbol);
        const opt = document.createElement('option');
        opt.value = sym;
        opt.innerText = sym;
        if (sym === currentStreamingSymbol) opt.selected = true;
        select.appendChild(opt);
      });
    }
  } catch (err) {
    console.error("Error fetching streaming symbols:", err);
  }
}

// --- Master Polling Loop ---
function fetchAllData() {
  fetchSystemHealth();
  fetchMarketSession();
  fetchStreamStatus();
  fetchStreamTape();
  fetchSymbolsCoverage();
  fetchStreamingSymbols();
}

// --- Application Boot Initialization ---
window.addEventListener('DOMContentLoaded', () => {
  initHistoricalChart();
  fetchAllData();
  setInterval(fetchMarketSession, 1000);
  setInterval(fetchStreamStatus, 4000);
  setInterval(fetchStreamTape, 3000);
  setInterval(pollHarvesterLogs, 5000);
});
