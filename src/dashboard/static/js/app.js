/**
 * Data Harvester Dashboard - Application Bootstrap, Routing & Coordination
 */

// --- Dashboard View (single view: the Parquet tick lake) ---
// The Historical Dashboard was removed in v5.0, so there is nothing to switch
// between; the entry points are kept so existing callers keep working.
function switchDashboardView(viewId) {
  currentDashboardView = 'streaming';
  const strmView = document.getElementById('view-streaming');
  if (strmView) strmView.classList.remove('hidden');

  if (!tvStreamingChart) {
    initStreamingChart();
  } else {
    setTimeout(resizeStreamingChart, 50);
  }
  fetchStreamingSymbols();
  fetchStreamStatus();
  fetchStreamTape();
  if (typeof loadStreamingContinuity === 'function') {
    const isExtended = (typeof currentContinuityExtended !== 'undefined') ? currentContinuityExtended : true;
    loadStreamingContinuity('all', 5, isExtended);
  }
}

function switchTab(tabId) {
  switchDashboardView('streaming');
}

// --- Streaming Tab Switching & Navigation ---
currentStreamingTab = 'chart';

function switchStreamingTab(tabId) {
  let normId = (tabId || 'chart').toLowerCase();
  if (normId.includes('chart')) normId = 'chart';
  else if (normId.includes('tape') || normId.includes('feed') || normId.includes('ticker')) normId = 'tape';
  else if (normId.includes('daemon') || normId.includes('control')) normId = 'daemon';
  else if (normId.includes('symbol')) normId = 'symbols';
  else normId = 'chart';

  currentStreamingTab = normId;

  // Ensure streaming dashboard is active
  if (currentDashboardView !== 'streaming') {
    switchDashboardView('streaming');
  }

  const tabs = ['chart', 'tape', 'daemon', 'symbols'];
  tabs.forEach(t => {
    const el = document.getElementById(`streaming-tab-${t}`) || document.getElementById(`stream-tab-${t}`);
    const btn = document.getElementById(`streaming-tab-btn-${t}`);
    if (el) el.classList.add('hidden');
    if (btn) {
      btn.className = "py-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200 flex items-center gap-2 font-medium";
    }
  });

  const activeEl = document.getElementById(`streaming-tab-${normId}`) || document.getElementById(`stream-tab-${normId}`);
  const activeBtn = document.getElementById(`streaming-tab-btn-${normId}`);
  if (activeEl) activeEl.classList.remove('hidden');
  if (activeBtn) {
    activeBtn.className = "py-3 border-b-2 border-indigo-500 text-white flex items-center gap-2 font-semibold";
  }

  if (normId === 'chart') {
    if (!tvStreamingChart) {
      initStreamingChart();
    } else {
      setTimeout(resizeStreamingChart, 50);
    }
  } else if (normId === 'symbols') {
    if (typeof loadStreamingSymbolsTable === 'function') {
      loadStreamingSymbolsTable();
    }
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
  fetchStreamingSymbols();
  if (typeof loadStreamingSymbolsTable === 'function') {
    loadStreamingSymbolsTable();
  }
}

// Expose routing handlers to window
window.switchStreamingTab = switchStreamingTab;

// --- Application Boot Initialization ---
window.addEventListener('DOMContentLoaded', () => {
  initStreamingChart();
  fetchAllData();
  setInterval(fetchMarketSession, 1000);
  setInterval(fetchStreamStatus, 4000);
  setInterval(fetchStreamTape, 3000);
});
