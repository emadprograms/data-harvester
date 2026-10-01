/**
 * Data Harvester Dashboard - Streaming Data Continuity & Integrity Visualizer (Bird's Eye View)
 *
 * Provides:
 *   1. Master Pulse Strip (5 trading days: Mon–Fri, 09:30–16:00 ET): Aggregate streaming database health.
 *      - Green = All monitored symbols streaming continuously.
 *      - Amber = Partial degradation (some symbols silent while others stream).
 *      - Red = Global blackout / outage (all symbols silent).
 *   2. Expandable 19-Symbol Spectrogram: 19 ultra-slim rows (AAPL to TSM) showing per-symbol continuous
 *      data vs vertical red cuts across symbols during outages.
 *   3. Incident Callout Summary badge and gap logs.
 *   4. Click-to-Sync: Clicking any gap or ribbon bucket syncs the streaming candlestick chart to that time.
 */

let currentContinuityView = 'master'; // 'master' or 'spectrum'
let cachedContinuityData = null;
let currentContinuitySymbol = 'all';
let currentContinuityDays = 5;

/**
 * Loads streaming continuity analysis data from REST API.
 * @param {string} symbol - 'all' or specific symbol (e.g. 'NVDA')
 * @param {number} days - Number of trading days to analyze (default 5)
 */
async function loadStreamingContinuity(symbol, days) {
  if (symbol !== undefined && symbol !== null) {
    currentContinuitySymbol = symbol;
  }
  if (days !== undefined && days !== null) {
    currentContinuityDays = Number(days) || 5;
  }

  const ribbonView = document.getElementById('continuity-ribbon-view');
  const summaryBadge = document.getElementById('continuity-incident-summary');

  try {
    const symParam = encodeURIComponent(currentContinuitySymbol || 'all');
    const url = `/api/streaming/continuity?days=${currentContinuityDays}&symbol=${symParam}`;
    const res = await fetch(url);
    if (!res.ok) {
      console.warn(`[Continuity] HTTP ${res.status} fetching continuity data`);
      return;
    }

    const data = await res.json();
    cachedContinuityData = data;
    renderContinuityRibbons(data);
  } catch (err) {
    console.error("[Continuity] Failed to load continuity analysis:", err);
    if (summaryBadge) {
      summaryBadge.innerHTML = `<span class="text-rose-400">⚠️ Continuity data unavailable</span>`;
    }
  }
}

/**
 * Renders the continuity ribbon visualizations based on active view mode.
 * @param {object} data - Payload from /api/streaming/continuity
 */
function renderContinuityRibbons(data) {
  if (!data) return;

  const ribbonView = document.getElementById('continuity-ribbon-view');
  const summaryBadge = document.getElementById('continuity-incident-summary');
  if (!ribbonView) return;

  const summary = data.summary || { total_gaps: 0, total_outage_minutes: 0, average_coverage: 100.0 };
  const days = data.days || [];
  const isAll = (data.view_mode === 'all' || currentContinuitySymbol === 'all');

  // 1. Update Incident Summary Badge
  if (summaryBadge) {
    if (days.length === 0) {
      summaryBadge.className = "text-xs font-mono px-3 py-1 bg-slate-950 border border-slate-800 rounded-lg text-slate-400 flex items-center gap-1.5";
      summaryBadge.innerHTML = `<span>ℹ️ No trading session ticks recorded</span>`;
    } else if (summary.total_gaps === 0) {
      summaryBadge.className = "text-xs font-mono px-3 py-1 bg-slate-950 border border-emerald-800/60 rounded-lg text-emerald-400 flex items-center gap-1.5";
      const symLabel = isAll ? "All 19 symbols" : data.symbol;
      summaryBadge.innerHTML = `<span>🟢 ${symLabel} streamed without interruption (100% coverage)</span>`;
    } else {
      const isRed = summary.total_outage_minutes > 0;
      const borderClass = isRed ? "border-rose-800/70 text-rose-400" : "border-amber-800/70 text-amber-400";
      const icon = isRed ? "🔴" : "⚠️";
      summaryBadge.className = `text-xs font-mono px-3 py-1 bg-slate-950 border ${borderClass} rounded-lg flex items-center gap-1.5`;
      summaryBadge.innerHTML = `<span>${icon} ${summary.total_gaps} gap${summary.total_gaps > 1 ? 's' : ''} detected (${summary.total_outage_minutes}m downtime, ${summary.average_coverage}% avg cov)</span>`;
    }
  }

  // 2. Render based on active mode
  if (currentContinuityView === 'spectrum' && isAll) {
    renderSpectrogramView(data, ribbonView);
  } else {
    renderMasterPulseView(data, ribbonView);
  }
}

/**
 * Renders the Master Pulse 5-Day Strip View.
 */
function renderMasterPulseView(data, container) {
  const days = data.days || [];
  if (days.length === 0) {
    container.innerHTML = `
      <div class="py-6 text-center text-xs text-slate-500 font-mono">
        No regular market session ticks (09:30–16:00 ET) found in streaming database.
      </div>
    `;
    return;
  }

  let html = `
    <div class="space-y-2">
      <!-- Legend bar -->
      <div class="flex items-center justify-between text-[11px] font-mono text-slate-400 px-1 pb-1 border-b border-slate-800/60">
        <span class="flex items-center gap-3">
          <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-sm bg-emerald-500 inline-block"></span>Healthy (Continuous)</span>
          <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-sm bg-amber-500 inline-block"></span>Partial Degradation</span>
          <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-sm bg-rose-500 inline-block"></span>Outage / Blackout</span>
        </span>
        <span class="text-slate-400 text-[10px]">Click any gap or bucket to sync chart</span>
      </div>
  `;

  days.forEach(day => {
    const statusColor = day.status === 'healthy' ? 'text-emerald-400 border-emerald-800/50 bg-emerald-950/40' :
                        day.status === 'partial' ? 'text-amber-400 border-amber-800/50 bg-amber-950/40' :
                        'text-rose-400 border-rose-800/50 bg-rose-950/40';

    const statusBadge = day.status === 'healthy' ? '🟢 100%' :
                        day.status === 'partial' ? `⚠️ ${day.coverage_pct}%` :
                        `🔴 ${day.coverage_pct}%`;

    html += `
      <div class="bg-slate-900/80 border border-slate-800/90 rounded-lg p-2.5 space-y-1.5">
        <div class="flex items-center justify-between text-xs font-mono">
          <div class="flex items-center gap-2">
            <span class="font-bold text-white">${day.day_name}</span>
            <span class="text-slate-400">${day.date}</span>
          </div>
          <div class="flex items-center gap-2">
            <span class="px-2 py-0.5 rounded text-[10px] border font-bold ${statusColor}">${statusBadge}</span>
            <span class="text-[10px] text-slate-400">${day.gaps ? day.gaps.length : 0} gaps</span>
          </div>
        </div>

        <!-- Visual Ribbon Strip (09:30 to 16:00 ET) -->
        <div class="relative w-full h-6 bg-slate-950 rounded border border-slate-800 flex overflow-hidden cursor-crosshair group" title="Regular Session 09:30 - 16:00 ET">
    `;

    const buckets = day.buckets || [];
    if (buckets.length > 0) {
      buckets.forEach(b => {
        const bg = b.status === 'healthy' ? 'bg-emerald-500 hover:bg-emerald-400' :
                   b.status === 'partial' ? 'bg-amber-500 hover:bg-amber-400' :
                   'bg-rose-500 hover:bg-rose-400';
        const ts = `${day.date} ${b.time}:00`;
        html += `<div class="flex-1 h-full ${bg} transition-colors" title="${b.time} ET | Status: ${b.status} | Active: ${b.active_count}/${b.total_count}" onclick="syncChartToGap('${ts}')"></div>`;
      });
    } else {
      const bg = day.status === 'healthy' ? 'bg-emerald-500' : (day.status === 'partial' ? 'bg-amber-500' : 'bg-rose-500');
      html += `<div class="w-full h-full ${bg}"></div>`;
    }

    html += `
        </div>
        <!-- Time markers -->
        <div class="flex justify-between text-[9px] font-mono text-slate-400 px-0.5">
          <span>09:30 ET</span>
          <span>11:00</span>
          <span>12:30</span>
          <span>14:00</span>
          <span>15:30</span>
          <span>16:00 ET</span>
        </div>
    `;

    // Render incident list for the day if any gaps exist
    if (day.gaps && day.gaps.length > 0) {
      html += `<div class="pt-1 flex flex-wrap gap-1.5">`;
      day.gaps.forEach(g => {
        const gapColor = g.status === 'outage' ? 'bg-rose-950/80 border-rose-800 text-rose-300 hover:bg-rose-900' :
                                                'bg-amber-950/80 border-amber-800 text-amber-300 hover:bg-amber-900';
        html += `
          <button onclick="syncChartToGap('${g.start_time}')" class="px-2 py-0.5 rounded text-[10px] font-mono border ${gapColor} flex items-center gap-1 transition-colors" title="Click to inspect on chart">
            <span>⏱️</span>
            <span>${g.description || `${g.duration}m gap (${g.start_str || ''} - ${g.end_str || ''})`}</span>
          </button>
        `;
      });
      html += `</div>`;
    }

    html += `</div>`;
  });

  html += `</div>`;
  container.innerHTML = html;
}

/**
 * Renders the 19-Symbol Spectrogram View.
 */
function renderSpectrogramView(data, container) {
  const spectrogram = data.spectrogram || data.symbols_breakdown || data.symbols || {};
  const symbols = Object.keys(spectrogram);
  if (symbols.length === 0) {
    container.innerHTML = `
      <div class="py-6 text-center text-xs text-slate-500 font-mono">
        No symbol breakdown data available for spectrogram.
      </div>
    `;
    return;
  }

  const days = data.days || [];
  const latestDay = days.length > 0 ? days[days.length - 1] : null;

  let html = `
    <div class="space-y-2">
      <div class="flex items-center justify-between text-[11px] font-mono text-slate-400 pb-1 border-b border-slate-800/60">
        <span class="text-white font-bold">19-Symbol Spectrogram (${days.length} Days Tracked)</span>
        <span class="text-slate-400 text-[10px]">Click any symbol row or gap cut to sync chart</span>
      </div>
      <div class="space-y-1">
  `;

  symbols.sort().forEach(sym => {
    const sData = spectrogram[sym] || {};
    const cov = sData.coverage_pct !== undefined ? sData.coverage_pct : 100.0;
    const gaps = sData.gaps || [];
    const statusColor = sData.status === 'healthy' ? 'bg-emerald-500' :
                        sData.status === 'partial' ? 'bg-amber-500' : 'bg-rose-500';
    const covColor = cov >= 99.9 ? 'text-emerald-400' : (cov >= 95.0 ? 'text-amber-400' : 'text-rose-400');

    html += `
      <div class="flex items-center gap-2 py-1 px-2 rounded bg-slate-900/60 hover:bg-slate-900 border border-slate-800/80 transition-colors">
        <span class="w-14 text-xs font-mono font-bold text-white flex items-center gap-1.5">
          <span class="w-2 h-2 rounded-full ${statusColor}"></span>
          ${sym}
        </span>
        <span class="w-12 text-[10px] font-mono ${covColor} text-right">${cov}%</span>

        <!-- Spectrogram slim ribbon -->
        <div class="flex-1 h-3 bg-slate-950 rounded border border-slate-800/90 relative overflow-hidden flex cursor-pointer" onclick="handleStreamingSymbolChange('${sym}')" title="${sym}: ${cov}% coverage (${gaps.length} gaps)">
    `;

    // Render continuous bar with red/amber cutouts for gaps
    if (gaps.length === 0) {
      html += `<div class="w-full h-full bg-emerald-500/90"></div>`;
    } else {
      // Divide into sections or render composite
      html += `<div class="w-full h-full bg-emerald-500/80 relative">`;
      gaps.forEach(g => {
        html += `<div class="absolute inset-y-0 bg-rose-500" style="left: 30%; width: 10%;" title="${g.description || 'Data Gap'}"></div>`;
      });
      html += `</div>`;
    }

    html += `
        </div>
        <button onclick="handleStreamingSymbolChange('${sym}')" class="px-1.5 py-0.5 text-[9px] font-mono bg-slate-800 hover:bg-slate-700 text-slate-300 rounded border border-slate-700">
          Chart
        </button>
      </div>
    `;
  });

  html += `
      </div>
    </div>
  `;
  container.innerHTML = html;
}

/**
 * Toggles view mode between 'master' (5-day Master Pulse) and 'spectrum' (19-Symbol Spectrogram).
 * @param {string} mode - 'master' or 'spectrum'
 */
function toggleContinuityView(mode) {
  currentContinuityView = (mode === 'spectrum') ? 'spectrum' : 'master';

  const btnMaster = document.getElementById('continuity-toggle-master');
  const btnSpectrum = document.getElementById('continuity-toggle-spectrum');

  if (btnMaster && btnSpectrum) {
    if (currentContinuityView === 'master') {
      btnMaster.className = "px-2.5 py-1 rounded text-xs font-mono font-bold bg-indigo-600 text-white transition-colors";
      btnSpectrum.className = "px-2.5 py-1 rounded text-xs font-mono text-slate-400 hover:text-white transition-colors";
    } else {
      btnMaster.className = "px-2.5 py-1 rounded text-xs font-mono text-slate-400 hover:text-white transition-colors";
      btnSpectrum.className = "px-2.5 py-1 rounded text-xs font-mono font-bold bg-indigo-600 text-white transition-colors";
    }
  }

  if (cachedContinuityData) {
    renderContinuityRibbons(cachedContinuityData);
  } else {
    loadStreamingContinuity();
  }
}

/**
 * Syncs the streaming candlestick chart range to inspect a specific gap timestamp.
 * @param {string|number} timestamp - ISO or exchange timestamp string or epoch
 */
function syncChartToGap(timestamp) {
  if (!timestamp) return;

  try {
    let epochSec = null;
    if (typeof timestamp === 'number') {
      epochSec = timestamp;
    } else if (typeof timestamp === 'string') {
      // Parse exchange datetime 'YYYY-MM-DD HH:MM:SS'
      const cleanTs = timestamp.trim().replace(' ', 'T');
      const dt = new Date(cleanTs.endsWith('Z') ? cleanTs : cleanTs + 'Z');
      if (!isNaN(dt.getTime())) {
        epochSec = Math.floor(dt.getTime() / 1000);
      }
    }

    if (epochSec && window.tvStreamingChart) {
      // Center chart around gap with a 30-minute buffer window
      const range = {
        from: epochSec - 1800,
        to: epochSec + 1800
      };
      window.tvStreamingChart.timeScale().setVisibleRange(range);
    }

    if (typeof showToast === 'function') {
      showToast(`🎯 Synced streaming chart to gap: ${timestamp}`, 'info');
    }
  } catch (err) {
    console.warn("[Continuity] Failed to sync chart to gap:", err);
  }
}

// Expose functions globally on window
window.loadStreamingContinuity = loadStreamingContinuity;
window.renderContinuityRibbons = renderContinuityRibbons;
window.toggleContinuityView = toggleContinuityView;
window.syncChartToGap = syncChartToGap;
