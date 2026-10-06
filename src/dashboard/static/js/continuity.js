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

let currentContinuityView = 'spectrum'; // 'master' or 'spectrum'
let cachedContinuityData = null;
let cachedAllContinuityData = null;
let cachedSymbolContinuityData = null;
let currentContinuitySymbol = 'all';
let currentContinuityDays = 5;
let currentContinuityWeekStart = null;
let currentContinuityDayDate = null;
let currentContinuityExtended = true;

// Expose state globally on window and global
if (typeof window !== 'undefined') {
  window.currentContinuityView = currentContinuityView;
  window.currentContinuitySymbol = currentContinuitySymbol;
  window.cachedContinuityData = cachedContinuityData;
  window.cachedAllContinuityData = cachedAllContinuityData;
  window.cachedSymbolContinuityData = cachedSymbolContinuityData;
  window.currentContinuityWeekStart = currentContinuityWeekStart;
  window.currentContinuityDayDate = currentContinuityDayDate;
  window.currentContinuityExtended = currentContinuityExtended;
  window.currentContinuityDays = currentContinuityDays;
}
if (typeof global !== 'undefined') {
  global.currentContinuityView = currentContinuityView;
  global.currentContinuitySymbol = currentContinuitySymbol;
  global.cachedContinuityData = cachedContinuityData;
  global.cachedAllContinuityData = cachedAllContinuityData;
  global.cachedSymbolContinuityData = cachedSymbolContinuityData;
  global.currentContinuityWeekStart = currentContinuityWeekStart;
  global.currentContinuityDayDate = currentContinuityDayDate;
  global.currentContinuityExtended = currentContinuityExtended;
  global.currentContinuityDays = currentContinuityDays;
}

function hasSpectrogramData(d) {
  if (!d) return false;
  const spec = d.spectrogram || d.symbols_breakdown || d.symbols;
  return !!(spec && typeof spec === 'object' && Object.keys(spec).length > 0);
}

/**
 * Populates the #continuity-week-select dropdown from available_weeks metadata.
 */
function populateWeekSelect(weeks, selectedWeek) {
  const select = document.getElementById('continuity-week-select');
  if (!select || !weeks || !Array.isArray(weeks)) return;

  const currentVal = select.value || selectedWeek;
  const optionsHtml = weeks.map(w => {
    const isSel = (w.week_start === currentVal || w.week_start === selectedWeek);
    return `<option value="${w.week_start}" ${isSel ? 'selected' : ''}>${w.label || w.week_start}</option>`;
  }).join('');

  select.innerHTML = optionsHtml;
  if (selectedWeek) {
    select.value = selectedWeek;
  }
}

/**
 * Toggles global extended trading hours mode (04:00-20:00 ET vs 09:30-16:00 ET).
 * Updates UI labels, subtitle, badges, and triggers fresh data fetches.
 * @param {boolean} checked - Whether extended hours should be enabled
 */
function toggleExtendedHours(checked) {
  currentContinuityExtended = Boolean(checked);
  if (typeof window !== 'undefined') window.currentContinuityExtended = currentContinuityExtended;
  if (typeof global !== 'undefined') global.currentContinuityExtended = currentContinuityExtended;

  const toggleEl = document.getElementById('continuity-extended-toggle');
  if (toggleEl && toggleEl.checked !== currentContinuityExtended) {
    toggleEl.checked = currentContinuityExtended;
  }
  const subtitleEl = document.getElementById('continuity-hours-subtitle');
  if (subtitleEl) {
    subtitleEl.innerText = currentContinuityExtended
      ? '04:00–20:00 ET (Click any symbol row to inspect detail & chart)'
      : '09:30–16:00 ET (Click any symbol row to inspect detail & chart)';
  }
  const badgeEl = document.getElementById('detail-session-hours-badge');
  if (badgeEl) {
    badgeEl.classList.add('hidden');
    badgeEl.innerText = currentContinuityExtended
      ? 'Extended Hours (04:00–20:00 ET)'
      : 'Regular Hours (09:30–16:00 ET)';
  }

  const detailView = document.getElementById('streaming-detail-view');
  const isDetailOpen = detailView && !detailView.classList.contains('hidden');

  if (isDetailOpen) {
    loadStreamingContinuity(currentContinuitySymbol, 1, currentContinuityExtended, null, currentContinuityDayDate);
    if (typeof loadStreamingChart === 'function') {
      loadStreamingChart(currentContinuityDayDate, currentContinuityExtended ? 'extended' : 'regular');
    }
  } else {
    const weekStart = (typeof currentContinuityWeekStart !== 'undefined' && currentContinuityWeekStart) ||
                      (typeof window !== 'undefined' && window.currentContinuityWeekStart);
    loadStreamingContinuity(currentContinuitySymbol || 'all', currentContinuityDays || 5, currentContinuityExtended, weekStart, null);
  }
}
const handleExtendedToggle = toggleExtendedHours;

/**
 * Handler for #continuity-week-select changes.
 */
function handleWeekChange(weekStart) {
  currentContinuityWeekStart = weekStart;
  currentContinuityDayDate = null;
  if (typeof window !== 'undefined') {
    window.currentContinuityWeekStart = currentContinuityWeekStart;
    window.currentContinuityDayDate = null;
  }
  if (typeof global !== 'undefined') {
    global.currentContinuityWeekStart = currentContinuityWeekStart;
    global.currentContinuityDayDate = null;
  }
  loadStreamingContinuity('all', 5, currentContinuityExtended, weekStart, null);
}

/**
 * Loads streaming continuity analysis data from REST API.
 * @param {string} symbol - 'all' or specific symbol (e.g. 'NVDA')
 * @param {number} days - Number of trading days to analyze (default 5)
 * @param {boolean} [extended=null] - Whether to include extended trading hours (04:00–20:00 ET)
 * @param {string} [weekStart=null] - Week start date YYYY-MM-DD
 * @param {string} [targetDate=null] - Single day target date YYYY-MM-DD
 */
async function loadStreamingContinuity(symbol, days, extended = null, weekStart = null, targetDate = null) {
  if (symbol !== undefined && symbol !== null) {
    currentContinuitySymbol = symbol;
  }
  if (days !== undefined && days !== null) {
    currentContinuityDays = Number(days) || 5;
  }
  if (weekStart !== undefined && weekStart !== null) {
    currentContinuityWeekStart = weekStart;
  }
  if (targetDate !== undefined && targetDate !== null) {
    currentContinuityDayDate = targetDate;
  }

  const isExtended = (extended !== null && extended !== undefined)
    ? Boolean(extended)
    : ((typeof currentContinuityExtended !== 'undefined') ? Boolean(currentContinuityExtended) : true);
  const ribbonView = document.getElementById('continuity-ribbon-view');
  const summaryBadge = document.getElementById('continuity-incident-summary');

  try {
    const symParam = encodeURIComponent(currentContinuitySymbol || 'all');
    let url = `/api/streaming/continuity?days=${currentContinuityDays}&symbol=${symParam}&extended=${isExtended ? 'true' : 'false'}&hours=${isExtended ? 'extended' : 'regular'}`;
    if (targetDate || currentContinuityDayDate) {
      url += `&date=${encodeURIComponent(targetDate || currentContinuityDayDate)}`;
    } else if (weekStart || currentContinuityWeekStart) {
      url += `&week_start=${encodeURIComponent(weekStart || currentContinuityWeekStart)}`;
    }

    const res = await fetch(url);
    if (!res.ok) {
      console.warn(`[Continuity] HTTP ${res.status} fetching continuity data`);
      return;
    }

    const data = await res.json();
    cachedContinuityData = data;

    if (data.available_weeks && Array.isArray(data.available_weeks)) {
      populateWeekSelect(data.available_weeks, data.week_start || currentContinuityWeekStart);
    }
    if (data.week_start && !currentContinuityWeekStart) {
      currentContinuityWeekStart = data.week_start;
    }

    if (data.view_mode === 'all' || hasSpectrogramData(data) || (Array.isArray(data.symbols) && data.symbols.length > 1)) {
      cachedAllContinuityData = data;
    } else {
      cachedSymbolContinuityData = data;
    }

    if (typeof window !== 'undefined') {
      window.cachedContinuityData = cachedContinuityData;
      window.cachedAllContinuityData = cachedAllContinuityData;
      window.cachedSymbolContinuityData = cachedSymbolContinuityData;
      window.currentContinuityView = currentContinuityView;
      window.currentContinuitySymbol = currentContinuitySymbol;
      window.currentContinuityWeekStart = currentContinuityWeekStart;
      window.currentContinuityDayDate = currentContinuityDayDate;
    }
    if (typeof global !== 'undefined') {
      global.cachedContinuityData = cachedContinuityData;
      global.cachedAllContinuityData = cachedAllContinuityData;
      global.cachedSymbolContinuityData = cachedSymbolContinuityData;
      global.currentContinuityView = currentContinuityView;
      global.currentContinuitySymbol = currentContinuitySymbol;
      global.currentContinuityWeekStart = currentContinuityWeekStart;
      global.currentContinuityDayDate = currentContinuityDayDate;
    }

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

  const is19SymbolSpectrum = Boolean(
    data && (
      data.view_mode === 'all' ||
      (Array.isArray(data.symbols) && data.symbols.length > 1) ||
      hasSpectrogramData(data)
    )
  );

  const displayData = is19SymbolSpectrum
    ? data
    : (((typeof cachedAllContinuityData !== 'undefined' && cachedAllContinuityData && hasSpectrogramData(cachedAllContinuityData)) ||
        (typeof window !== 'undefined' && window.cachedAllContinuityData && hasSpectrogramData(window.cachedAllContinuityData)))
          ? (cachedAllContinuityData || window.cachedAllContinuityData)
          : data);

  const summary = displayData.summary || { total_gaps: 0, total_outage_minutes: 0, average_coverage: 100.0 };
  const days = displayData.days || [];
  const isAll = (displayData.view_mode === 'all' || currentContinuitySymbol === 'all' || is19SymbolSpectrum);

  // 1. Update Incident Summary Badge
  if (summaryBadge) {
    if (days.length === 0) {
      summaryBadge.className = "text-xs font-mono px-3 py-1 bg-slate-950 border border-slate-800 rounded-lg text-slate-400 flex items-center gap-1.5";
      summaryBadge.innerHTML = `<span>ℹ️ No trading session ticks recorded</span>`;
    } else if (summary.total_gaps === 0) {
      summaryBadge.className = "text-xs font-mono px-3 py-1 bg-slate-950 border border-emerald-800/60 rounded-lg text-emerald-400 flex items-center gap-1.5";
      const symLabel = isAll ? "All 19 symbols" : (displayData.symbol || currentContinuitySymbol);
      summaryBadge.innerHTML = `<span>🟢 ${symLabel} streamed without interruption (100% coverage)</span>`;
    } else {
      const isRed = summary.total_outage_minutes > 0;
      const borderClass = isRed ? "border-rose-800/70 text-rose-400" : "border-amber-800/70 text-amber-400";
      const icon = isRed ? "🔴" : "⚠️";
      summaryBadge.className = `text-xs font-mono px-3 py-1 bg-slate-950 border ${borderClass} rounded-lg flex items-center gap-1.5`;
      summaryBadge.innerHTML = `<span>${icon} ${summary.total_gaps} gap${summary.total_gaps > 1 ? 's' : ''} detected (${summary.total_outage_minutes}m downtime, ${summary.average_coverage}% avg cov)</span>`;
    }
  }

  // 2. Render based on active mode / payload type
  if (is19SymbolSpectrum) {
    cachedAllContinuityData = data;
    if (typeof window !== 'undefined') window.cachedAllContinuityData = data;
    if (typeof global !== 'undefined') global.cachedAllContinuityData = data;
    renderSpectrogramView(data, ribbonView);
  } else {
    // Single-symbol / single-day drill-down data
    const detailContainer = document.getElementById('detail-continuity-ribbons');
    if (detailContainer) {
      renderMasterPulseView(data, detailContainer);
    }
    // DO NOT overwrite #continuity-ribbon-view with single-symbol Master Pulse!
    const ribbonHasSpectrogram = ribbonView.innerHTML && ribbonView.innerHTML.includes('Spectrogram');
    if (!ribbonHasSpectrogram) {
      const allCached = (typeof cachedAllContinuityData !== 'undefined' && cachedAllContinuityData) ||
                        (typeof window !== 'undefined' && window.cachedAllContinuityData);
      if (allCached) {
        renderSpectrogramView(allCached, ribbonView);
      }
    }
  }
}

/**
 * Renders the Master Pulse View (supporting both 09:30-16:00 regular and 04:00-20:00 extended hours).
 */
function renderMasterPulseView(data, container) {
  if (!container) return;
  const days = data.days || [];
  const isExtended = Boolean(data.extended_hours || data.hours === 'extended');
  const isSingleDay = (days.length === 1) || Boolean(data.target_date) || (container && container.id === 'detail-continuity-ribbons');

  if (days.length === 0) {
    container.innerHTML = `
      <div class="py-6 text-center text-xs text-slate-500 font-mono">
        No regular market session ticks found in streaming database.
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

        <!-- Visual Ribbon Strip -->
        <div class="relative w-full h-6 bg-slate-950 rounded border border-slate-800 flex overflow-hidden cursor-crosshair group" title="${isExtended ? 'Extended Session 04:00 - 20:00 ET' : 'Regular Session 09:30 - 16:00 ET'}">
    `;

    const buckets = day.buckets || [];
    if (buckets.length > 0) {
      buckets.forEach(b => {
        const bg = b.status === 'healthy' ? 'bg-emerald-500 hover:bg-emerald-400' :
                   b.status === 'partial' ? 'bg-amber-500 hover:bg-amber-400' :
                   'bg-rose-500 hover:bg-rose-400';
        const ts = `${day.date} ${b.time}:00`;
        const syncArg = b.start_epoch !== undefined ? b.start_epoch : `'${ts}'`;
        const clickAttr = isSingleDay ? '' : ` onclick="syncChartToGap(${syncArg})"`;
        html += `<div class="flex-1 h-full ${bg} transition-colors" title="${b.time} ET | Status: ${b.status} | Active: ${b.active_count}/${b.total_count}"${clickAttr}></div>`;
      });
    } else {
      const bg = day.status === 'healthy' ? 'bg-emerald-500' : (day.status === 'partial' ? 'bg-amber-500' : 'bg-rose-500');
      html += `<div class="w-full h-full ${bg}"></div>`;
    }

    html += `
        </div>
    `;

    if (isExtended) {
      html += `
        <!-- Phase markers: Pre 04:00, Open 09:30, Close 16:00, Post 20:00 (true offsets on 960m timeline) -->
        <div class="relative w-full h-4 text-[9px] font-mono text-slate-400 mt-1">
          <span class="absolute left-0 text-amber-400 font-semibold">Pre 04:00</span>
          <span class="absolute left-[34.38%] -translate-x-1/2 text-emerald-400 font-bold whitespace-nowrap">Open 09:30</span>
          <span class="absolute left-[50.0%] -translate-x-1/2 text-slate-400 whitespace-nowrap">12:00</span>
          <span class="absolute left-[75.0%] -translate-x-1/2 text-indigo-400 font-bold whitespace-nowrap">Close 16:00</span>
          <span class="absolute right-0 text-rose-400 font-semibold">Post 20:00</span>
        </div>
      `;
    } else {
      html += `
        <!-- Time markers (Regular market session: 390m timeline) -->
        <div class="relative w-full h-4 text-[9px] font-mono text-slate-400 mt-1">
          <span class="absolute left-0 text-emerald-400 font-bold">09:30 ET</span>
          <span class="absolute left-[23.08%] -translate-x-1/2 whitespace-nowrap">11:00</span>
          <span class="absolute left-[46.15%] -translate-x-1/2 whitespace-nowrap">12:30</span>
          <span class="absolute left-[69.23%] -translate-x-1/2 whitespace-nowrap">14:00</span>
          <span class="absolute left-[92.31%] -translate-x-1/2 whitespace-nowrap">15:30</span>
          <span class="absolute right-0 text-indigo-400 font-bold">16:00 ET</span>
        </div>
      `;
    }

    // Render incident list for the day if any gaps exist
    if (day.gaps && day.gaps.length > 0) {
      html += `<div class="pt-1 flex flex-wrap gap-1.5">`;
      day.gaps.forEach(g => {
        const gapColor = g.status === 'outage' ? 'bg-rose-950/80 border-rose-800 text-rose-300' :
                                                'bg-amber-950/80 border-amber-800 text-amber-300';
        if (isSingleDay) {
          html += `
            <span class="px-2 py-0.5 rounded text-[10px] font-mono border ${gapColor} flex items-center gap-1" title="${g.description || 'Gap incident'}">
              <span>⏱️</span>
              <span>${g.description || `${g.duration}m gap (${g.start_str || ''} - ${g.end_str || ''})`}</span>
            </span>
          `;
        } else {
          const gapSyncArg = (g.start_epoch !== undefined && g.end_epoch !== undefined)
            ? `{start_epoch: ${g.start_epoch}, end_epoch: ${g.end_epoch}}`
            : `'${g.start_time}'`;
          html += `
            <button onclick="syncChartToGap(${gapSyncArg})" class="px-2 py-0.5 rounded text-[10px] font-mono border ${gapColor} hover:bg-slate-850 flex items-center gap-1 transition-colors" title="Click to inspect on chart">
              <span>⏱️</span>
              <span>${g.description || `${g.duration}m gap (${g.start_str || ''} - ${g.end_str || ''})`}</span>
            </button>
          `;
        }
      });
      html += `</div>`;
    }

    html += `</div>`;
  });

  html += `</div>`;
  container.innerHTML = html;
}

/**
 * Renders the 19-Symbol Spectrogram View with timeline headers and symbol row drill-downs.
 */
function renderSpectrogramView(data, container) {
  if (!container && typeof document !== 'undefined') {
    container = document.getElementById('streaming-spectrum-view') || document.getElementById('continuity-ribbon-view');
  }
  if (!container) return;
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
  const latestDayDate = (days.length > 0 && days[days.length - 1].date) ? days[days.length - 1].date : '';

  let html = `
    <div class="space-y-2">
      <div class="flex items-center justify-between text-[11px] font-mono text-slate-400 pb-1 border-b border-slate-800/60">
        <span class="text-white font-bold">19-Symbol Spectrogram (${days.length} Days Monitored)</span>
        <span class="text-slate-400 text-[10px]">Click any specific day cell to open daily chart</span>
      </div>

      <!-- Day column headers across timeline -->
      ${days.length > 0 ? `
      <div class="flex items-center gap-2 py-1 px-2 text-[10px] font-mono text-slate-400">
        <span class="w-14">Symbol</span>
        <span class="w-12 text-right">Cov</span>
        <div class="flex-1 flex justify-between px-1">
          ${days.map(d => `<span class="flex-1 text-center font-bold text-slate-300 border-l border-slate-800 first:border-l-0">${d.day_name ? d.day_name.slice(0, 3) : 'Day'} <span class="text-[9px] text-slate-500">${d.date ? d.date.slice(5) : ''}</span></span>`).join('')}
        </div>
      </div>` : ''}

      <div class="space-y-1">
  `;

  symbols.sort().forEach(sym => {
    const sData = spectrogram[sym] || {};
    const cov = sData.coverage_pct !== undefined ? sData.coverage_pct : 100.0;
    const gaps = sData.gaps || [];
    const statusColor = sData.status === 'healthy' ? 'bg-emerald-500' :
                        sData.status === 'partial' ? 'bg-amber-500' : 'bg-rose-500';
    const covColor = cov >= 99.9 ? 'text-emerald-400' : (cov >= 95.0 ? 'text-amber-400' : 'text-rose-400');

    const clickRowHandler = latestDayDate
      ? `if (typeof openSymbolDayDetail === 'function') { openSymbolDayDetail('${sym}', '${latestDayDate}'); } else if (typeof window !== 'undefined' && typeof window.openSymbolDayDetail === 'function') { window.openSymbolDayDetail('${sym}', '${latestDayDate}'); } else { openSymbolDetail('${sym}'); }`
      : `openSymbolDetail('${sym}')`;

    html += `
      <div class="flex items-center gap-2 py-1 px-2 rounded bg-slate-900/60 hover:bg-slate-900 border border-slate-800/80 transition-colors cursor-pointer group" onclick="${clickRowHandler}">
        <span class="w-14 text-xs font-mono font-bold text-white flex items-center gap-1.5 group-hover:text-indigo-400 transition-colors">
          <span class="w-2 h-2 rounded-full ${statusColor}"></span>
          ${sym}
        </span>
        <span class="w-12 text-[10px] font-mono ${covColor} text-right">${cov}%</span>

        <!-- Spectrogram slim ribbon with day dividers -->
        <div class="flex-1 h-3.5 bg-slate-950 rounded border border-slate-800/90 relative overflow-hidden flex" title="${sym}: ${cov}% coverage (${gaps.length} gaps) - Click day to drill down">
    `;

    const renderGapMarkers = (gapList) => {
      return (gapList || []).map(g => {
        let minOfDay = null;
        if (g.start_str && typeof g.start_str === 'string' && g.start_str.includes(':')) {
          const [h, m] = g.start_str.split(':').map(Number);
          if (!isNaN(h) && !isNaN(m)) minOfDay = h * 60 + m;
        } else if (g.start_time && typeof g.start_time === 'string' && g.start_time.includes(':')) {
          const tPart = g.start_time.split(' ')[1] || g.start_time;
          const [h, m] = tPart.split(':').map(Number);
          if (!isNaN(h) && !isNaN(m)) minOfDay = h * 60 + m;
        } else if (g.start_epoch) {
          try {
            const d = new Date(g.start_epoch * 1000);
            const etStr = d.toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour12: false, hour: '2-digit', minute: '2-digit' });
            const [h, m] = etStr.split(':').map(Number);
            if (!isNaN(h) && !isNaN(m)) minOfDay = h * 60 + m;
          } catch (_) {}
        }
        const isGlobalExtended = (typeof window !== 'undefined' && typeof window.currentContinuityExtended === 'boolean')
          ? window.currentContinuityExtended
          : ((typeof global !== 'undefined' && typeof global.currentContinuityExtended === 'boolean')
              ? global.currentContinuityExtended
              : currentContinuityExtended);
        const isExtended = (data && (data.hours === 'regular' || data.extended_hours === false))
          ? false
          : Boolean(data.extended_hours || data.hours === 'extended' || isGlobalExtended);
        const baseStartMin = isExtended ? 240 : 570;
        const spanMin = isExtended ? 960 : 390;
        if (minOfDay === null) minOfDay = baseStartMin;

        const offsetMin = Math.max(0, minOfDay - baseStartMin);
        const leftPct = Math.min(100, Math.max(0, (offsetMin / spanMin) * 100));
        const dur = g.duration || g.duration_minutes || g.missing_minutes || 1;
        const widthPct = Math.min(100 - leftPct, Math.max(0.4, (dur / spanMin) * 100));
        return `<div class="absolute inset-y-0 bg-rose-500 rounded-[1px] pointer-events-none" style="left: ${leftPct.toFixed(2)}%; width: max(2px, ${widthPct.toFixed(2)}%);" title="${g.description || `${dur}m gap`}"></div>`;
      }).join('');
    };

    if (days.length > 0) {
      days.forEach((day, idx) => {
        const borderDivider = idx < days.length - 1 ? 'border-r border-slate-800' : '';
        const dayGaps = (day.gaps || []).filter(g => g.symbol === sym || (g.impacted_symbols && g.impacted_symbols.includes(sym)));
        const clickDayHandler = `event.stopPropagation(); if (typeof openSymbolDayDetail === 'function') { openSymbolDayDetail('${sym}', '${day.date}'); } else if (typeof window !== 'undefined' && typeof window.openSymbolDayDetail === 'function') { window.openSymbolDayDetail('${sym}', '${day.date}'); } else { openSymbolDetail('${sym}'); }`;

        if (dayGaps.length === 0) {
          html += `<div onclick="${clickDayHandler}" class="flex-1 h-full bg-emerald-500/80 hover:bg-emerald-400/90 ${borderDivider} cursor-pointer" title="${sym} • ${day.day_name} (${day.date}): 100% - Click to view single-day detail"></div>`;
        } else {
          html += `
            <div onclick="${clickDayHandler}" class="flex-1 h-full bg-emerald-500/70 hover:bg-emerald-400/80 relative ${borderDivider} cursor-pointer" title="${sym} • ${day.day_name} (${day.date}): ${dayGaps.length} gap(s) - Click to view single-day detail">
              ${renderGapMarkers(dayGaps)}
            </div>
          `;
        }
      });
    } else {
      if (gaps.length === 0) {
        html += `<div class="w-full h-full bg-emerald-500/90"></div>`;
      } else {
        html += `
          <div class="w-full h-full bg-emerald-500/80 relative">
            ${renderGapMarkers(gaps)}
          </div>
        `;
      }
    }

    html += `
        </div>
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
  if (typeof window !== 'undefined') {
    window.currentContinuityView = currentContinuityView;
  }

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

  if (currentContinuityView === 'spectrum') {
    if (cachedAllContinuityData && hasSpectrogramData(cachedAllContinuityData)) {
      renderContinuityRibbons(cachedAllContinuityData);
    } else {
      loadStreamingContinuity('all', currentContinuityDays);
    }
  } else {
    const masterData = cachedSymbolContinuityData || cachedContinuityData || cachedAllContinuityData;
    if (masterData) {
      renderContinuityRibbons(masterData);
    } else {
      loadStreamingContinuity(currentContinuitySymbol || 'all', currentContinuityDays);
    }
  }
}

/**
 * Syncs the streaming candlestick chart range to inspect a specific gap timestamp or epoch seconds.
 * @param {string|number|object} timestamp - ISO / exchange timestamp string, epoch seconds, or gap object
 * @param {number} [epoch] - Optional direct UTC epoch seconds
 */
function syncChartToGap(timestamp, epoch) {
  if (timestamp === undefined && epoch === undefined) return;

  try {
    let epochSec = null;
    let range = null;

    if (epoch !== undefined && typeof epoch === 'number') {
      epochSec = epoch;
      range = { from: epochSec - 1800, to: epochSec + 1800 };
    } else if (typeof timestamp === 'number') {
      epochSec = timestamp;
      range = { from: epochSec - 1800, to: epochSec + 1800 };
    } else if (typeof timestamp === 'object' && timestamp !== null) {
      const s = timestamp.start_epoch !== undefined ? timestamp.start_epoch : timestamp.start;
      const e = timestamp.end_epoch !== undefined ? timestamp.end_epoch : (timestamp.end !== undefined ? timestamp.end : s);
      if (s !== undefined && s !== null) {
        epochSec = s;
        range = { from: s - 1800, to: (e !== undefined && e !== null ? e : s) + 1800 };
      }
    } else if (typeof timestamp === 'string') {
      // Parse exchange datetime 'YYYY-MM-DD HH:MM:SS'
      const cleanTs = timestamp.trim().replace(' ', 'T');
      const dt = new Date(cleanTs.endsWith('Z') ? cleanTs : cleanTs + 'Z');
      if (!isNaN(dt.getTime())) {
        epochSec = Math.floor(dt.getTime() / 1000);
        range = { from: epochSec - 1800, to: epochSec + 1800 };
      }
    }

    const chart = (typeof window !== 'undefined' && window.tvStreamingChart) ||
                  (typeof tvStreamingChart !== 'undefined' ? tvStreamingChart : null) ||
                  (typeof global !== 'undefined' && global.tvStreamingChart);

    if (range && chart && chart.timeScale) {
      chart.timeScale().setVisibleRange(range);
    }

    if (typeof showToast === 'function') {
      const label = (typeof timestamp === 'object' && timestamp !== null)
        ? (timestamp.start_time || timestamp.description || 'incident')
        : String(timestamp);
      showToast(`🎯 Synced streaming chart to gap: ${label}`, 'info');
    }
  } catch (err) {
    console.warn("[Continuity] Failed to sync chart to gap:", err);
  }
}

// Expose functions globally on window and global
if (typeof window !== 'undefined') {
  window.loadStreamingContinuity = loadStreamingContinuity;
  window.handleWeekChange = handleWeekChange;
  window.populateWeekSelect = populateWeekSelect;
  window.renderContinuityRibbons = renderContinuityRibbons;
  window.renderMasterPulseView = renderMasterPulseView;
  window.renderSpectrogramView = renderSpectrogramView;
  window.toggleContinuityView = toggleContinuityView;
  window.toggleExtendedHours = toggleExtendedHours;
  window.handleExtendedToggle = handleExtendedToggle;
  window.syncChartToGap = syncChartToGap;
  window.currentContinuityView = currentContinuityView;
  window.currentContinuityWeekStart = currentContinuityWeekStart;
  window.currentContinuityDayDate = currentContinuityDayDate;
  window.currentContinuityExtended = currentContinuityExtended;
  window.cachedContinuityData = cachedContinuityData;
  window.cachedAllContinuityData = cachedAllContinuityData;
  window.cachedSymbolContinuityData = cachedSymbolContinuityData;
  window.currentContinuitySymbol = currentContinuitySymbol;
  window.currentContinuityDays = currentContinuityDays;
}
if (typeof global !== 'undefined') {
  global.loadStreamingContinuity = loadStreamingContinuity;
  global.handleWeekChange = handleWeekChange;
  global.populateWeekSelect = populateWeekSelect;
  global.renderContinuityRibbons = renderContinuityRibbons;
  global.renderMasterPulseView = renderMasterPulseView;
  global.renderSpectrogramView = renderSpectrogramView;
  global.toggleContinuityView = toggleContinuityView;
  global.toggleExtendedHours = toggleExtendedHours;
  global.handleExtendedToggle = handleExtendedToggle;
  global.syncChartToGap = syncChartToGap;
  global.currentContinuityView = currentContinuityView;
  global.currentContinuityWeekStart = currentContinuityWeekStart;
  global.currentContinuityDayDate = currentContinuityDayDate;
  global.currentContinuityExtended = currentContinuityExtended;
  global.cachedContinuityData = cachedContinuityData;
  global.cachedAllContinuityData = cachedAllContinuityData;
  global.cachedSymbolContinuityData = cachedSymbolContinuityData;
  global.currentContinuitySymbol = currentContinuitySymbol;
  global.currentContinuityDays = currentContinuityDays;
}
