/**
 * Data Harvester Dashboard - Chart Engine (TradingView Lightweight Charts)
 *
 * Timestamp contract for the Historical Database page (an exchange-local chart):
 *   - /api/candles returns `time` as a true UTC epoch (seconds) and `time_str` as the very same
 *     instant already rendered on the NYSE clock (America/New_York, EST/EDT).
 *   - Every label drawn here (X-axis ticks, crosshair badge, OHLCV legend) is formatted through
 *     Intl with an explicit `timeZone`, so the 09:30 opening bell and its volume spike always read
 *     09:30 ET — no matter which timezone the dashboard server or the operator's browser runs in.
 */

const EXCHANGE_TIMEZONE = 'America/New_York';
const EXCHANGE_TIMEZONE_LABEL = 'ET';

let chartTimezone = EXCHANGE_TIMEZONE;
const exchangeFormatterCache = {};

/**
 * Adopts the timezone advertised by the candle API (falls back to NYSE time if it is unusable).
 * @param {string} tz - IANA timezone name from the API payload (`timezone`).
 */
function setChartTimezone(tz) {
  if (!tz || typeof tz !== 'string') {
    chartTimezone = EXCHANGE_TIMEZONE;
    return;
  }
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: tz });
    chartTimezone = tz;
  } catch (err) {
    chartTimezone = EXCHANGE_TIMEZONE;
  }
}

function getExchangeFormatter(options) {
  const cacheKey = `${chartTimezone}|${JSON.stringify(options)}`;
  if (!exchangeFormatterCache[cacheKey]) {
    exchangeFormatterCache[cacheKey] = new Intl.DateTimeFormat(
      'en-US',
      Object.assign({ timeZone: chartTimezone, hourCycle: 'h23' }, options)
    );
  }
  return exchangeFormatterCache[cacheKey];
}

/**
 * Breaks a candle epoch into exchange-local calendar parts ({year, month, day, hour, minute, second}).
 * @param {number} epochSeconds - True UTC epoch seconds of the bar start.
 */
function exchangeTimeParts(epochSeconds) {
  const formatter = getExchangeFormatter({
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit'
  });
  const parts = {};
  formatter.formatToParts(new Date(epochSeconds * 1000)).forEach(p => { parts[p.type] = p.value; });
  return parts;
}

/** 'YYYY-MM-DD HH:mm:ss' on the exchange clock. */
function formatExchangeDateTime(epochSeconds) {
  const p = exchangeTimeParts(epochSeconds);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

/** 'HH:mm' (or 'HH:mm:ss') on the exchange clock. */
function formatExchangeClock(epochSeconds, withSeconds = false) {
  const p = exchangeTimeParts(epochSeconds);
  return withSeconds ? `${p.hour}:${p.minute}:${p.second}` : `${p.hour}:${p.minute}`;
}

/** 'Jul 15' on the exchange clock. */
function formatExchangeDay(epochSeconds) {
  return getExchangeFormatter({ month: 'short', day: 'numeric' }).format(new Date(epochSeconds * 1000));
}

/** 'Jul' on the exchange clock. */
function formatExchangeMonth(epochSeconds) {
  return getExchangeFormatter({ month: 'short' }).format(new Date(epochSeconds * 1000));
}

/** '2026' on the exchange clock. */
function formatExchangeYear(epochSeconds) {
  return getExchangeFormatter({ year: 'numeric' }).format(new Date(epochSeconds * 1000));
}

/** True when the bar starts exactly at exchange-local midnight (i.e. a new trading day). */
function isExchangeDayStart(epochSeconds) {
  const p = exchangeTimeParts(epochSeconds);
  return p.hour === '00' && p.minute === '00' && p.second === '00';
}

/** Full human-readable exchange label, e.g. '2026-07-15 09:30:00 ET'. */
function formatExchangeLabel(epochSeconds) {
  return `${formatExchangeDateTime(epochSeconds)} ${EXCHANGE_TIMEZONE_LABEL}`;
}

/**
 * Resolves the display string for a candle: prefers the exchange-local string computed by the
 * backend (single source of truth for EST/EDT) and falls back to local Intl formatting.
 */
function candleTimeLabel(candle) {
  if (!candle) return '--';
  if (candle.time_str) return `${candle.time_str} ${EXCHANGE_TIMEZONE_LABEL}`;
  if (typeof candle.time === 'number') return formatExchangeLabel(candle.time);
  return '--';
}

/** Keeps the legend badge honest about which exchange timezone the loaded bars are rendered in. */
function updateTzBadge(tz) {
  const badge = document.getElementById('legend-tz-badge');
  if (!badge) return;
  badge.innerText = (!tz || tz === EXCHANGE_TIMEZONE)
    ? 'NYSE (ET)'
    : `${tz} (${EXCHANGE_TIMEZONE_LABEL})`;
}

/** O(1) bar lookup by epoch, rebuilt on every chart load (used by the crosshair + time badge). */
let candleTimeIndex = new Map();

function findCandleByTime(time) {
  if (candleTimeIndex.has(time)) return candleTimeIndex.get(time);
  return (loadedCandles || []).find(c => c.time === time) || null;
}

function initChart() {
  const container = document.getElementById('tv-chart-container');
  if (!container || tvChart) return;

  tvChart = LightweightCharts.createChart(container, {
    layout: {
      background: { color: '#090d16' },
      textColor: '#94a3b8',
      fontSize: 11,
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace'
    },
    grid: {
      vertLines: { color: '#1e293b' },
      horzLines: { color: '#1e293b' }
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: { color: '#475569', width: 1, style: 1 },
      horzLine: { color: '#475569', width: 1, style: 1 }
    },
    localization: {
      locale: 'en-US',
      // Crosshair time badge: exchange-local label (backend `time_str` when the bar is known)
      timeFormatter: (time) => {
        if (typeof time !== 'number') return time ? String(time) : '--';
        return candleTimeLabel(findCandleByTime(time) || { time });
      }
    },
    timeScale: {
      borderColor: '#334155',
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter: (time, tickMarkType, locale) => {
        if (typeof time !== 'number') return null;
        // Lightweight Charts weights tick marks on the UTC calendar; re-anchor every label to the
        // exchange day so an evening/post-market bar never renders a stray UTC date tick.
        const dayStart = isExchangeDayStart(time);
        switch (tickMarkType) {
          case 0: // Year
            return dayStart ? formatExchangeYear(time) : formatExchangeDay(time);
          case 1: // Month
            return dayStart ? formatExchangeMonth(time) : formatExchangeDay(time);
          case 2: // DayOfMonth
            return dayStart ? formatExchangeDay(time) : formatExchangeClock(time);
          case 3: // Time
            return formatExchangeClock(time);
          case 4: // TimeWithSeconds
            return formatExchangeClock(time, true);
          default:
            return null;
        }
      }
    },
    rightPriceScale: {
      borderColor: '#334155',
      scaleMargins: { top: 0.1, bottom: 0.25 }
    }
  });

  candleSeries = tvChart.addCandlestickSeries({
    upColor: '#10b981',
    downColor: '#f43f5e',
    borderVisible: false,
    wickUpColor: '#10b981',
    wickDownColor: '#f43f5e'
  });

  volumeSeries = tvChart.addHistogramSeries({
    priceFormat: { type: 'volume' },
    priceScaleId: '', // Overlay on same scale
    scaleMargins: { top: 0.82, bottom: 0 }
  });

  // Crosshair move listener to update price legend
  tvChart.subscribeCrosshairMove(param => {
    if (!param || !param.time || !param.seriesData || !param.seriesData.get(candleSeries)) {
      if (loadedCandles.length > 0) {
        updateLegend(loadedCandles[loadedCandles.length - 1]);
      }
      return;
    }
    const data = param.seriesData.get(candleSeries);
    const candleMatch = findCandleByTime(param.time);
    updateLegend(candleMatch || data);
  });

  window.addEventListener('resize', resizeChart);
  loadChartData();
}

function resizeChart() {
  const container = document.getElementById('tv-chart-container');
  if (tvChart && container) {
    tvChart.applyOptions({
      width: container.clientWidth,
      height: container.clientHeight
    });
  }
}

function updateLegend(candle) {
  if (!candle) return;
  const symEl = document.getElementById('legend-symbol');
  const timeEl = document.getElementById('legend-time');
  const openEl = document.getElementById('legend-open');
  const highEl = document.getElementById('legend-high');
  const lowEl = document.getElementById('legend-low');
  const closeEl = document.getElementById('legend-close');
  const volEl = document.getElementById('legend-volume');
  const chgEl = document.getElementById('legend-change');
  const srcEl = document.getElementById('legend-source');
  const dbBadge = document.getElementById('legend-db-badge');

  if (symEl) symEl.innerText = `${currentSymbol} (${currentTimeframe.toUpperCase()})`;
  if (timeEl) timeEl.innerText = candleTimeLabel(candle);
  if (openEl) openEl.innerText = Number(candle.open).toFixed(2);
  if (highEl) highEl.innerText = Number(candle.high).toFixed(2);
  if (lowEl) lowEl.innerText = Number(candle.low).toFixed(2);
  if (closeEl) closeEl.innerText = Number(candle.close).toFixed(2);
  if (volEl) volEl.innerText = Number(candle.volume || 0).toLocaleString();
  
  if (chgEl) {
    const change = candle.close - candle.open;
    const changePct = candle.open ? ((change / candle.open) * 100).toFixed(2) : 0;
    chgEl.innerText = `${change >= 0 ? '+' : ''}${change.toFixed(2)} (${changePct}%)`;
    chgEl.className = change >= 0 ? 'text-emerald-400 font-bold' : 'text-rose-400 font-bold';
  }

  if (srcEl) {
    srcEl.innerText = candle.source || (currentDbSource === 'historical' ? 'MASSIVE' : 'CAPITAL_STREAM');
  }

  if (dbBadge) {
    if (currentDbSource === 'historical') {
      dbBadge.innerText = "HISTORICAL DB";
      dbBadge.className = "px-2 py-0.5 rounded text-[10px] bg-emerald-950 text-emerald-400 border border-emerald-800 font-mono font-bold";
    } else {
      dbBadge.innerText = "STREAMING DB";
      dbBadge.className = "px-2 py-0.5 rounded text-[10px] bg-indigo-950 text-indigo-400 border border-indigo-800 font-mono font-bold";
    }
  }
}

function setDbSource(source) {
  currentDbSource = source;
  const btnHist = document.getElementById('src-historical');
  const btnStream = document.getElementById('src-streaming');
  const pill = document.getElementById('db-source-badge-pill');
  const pillText = document.getElementById('db-source-pill-text');
  const notice = document.getElementById('db-source-notice');

  if (source === 'historical') {
    if (btnHist) btnHist.className = "px-3 py-1.5 rounded text-xs font-mono font-bold bg-emerald-600 text-white flex items-center gap-1.5 shadow-sm transition";
    if (btnStream) btnStream.className = "px-3 py-1.5 rounded text-xs font-mono font-bold text-slate-400 hover:text-white flex items-center gap-1.5 transition";
    if (pill) pill.className = "px-3 py-1 rounded-full text-xs font-mono font-bold bg-emerald-950 text-emerald-400 border border-emerald-800 flex items-center gap-2";
    if (pillText) pillText.innerText = "Mode: CANONICAL HISTORICAL ARCHIVE";
    if (notice) {
      notice.className = "text-xs px-4 py-2.5 rounded-lg bg-emerald-950/30 border border-emerald-800/50 text-emerald-300 font-mono flex items-center justify-between";
      notice.innerHTML = `
        <div class="flex items-center gap-2">
          <span>🏛️</span>
          <span><strong>Historical Archive Mode</strong>: Querying permanent canonical 1m bars from <code>data/historical.duckdb</code> with Source-Tiering (Massive, Binance, Capital REST). Displayed in <strong>US Eastern Time (NYSE: EST/EDT)</strong>. Fully backfillable.</span>
        </div>
        <span class="text-[11px] text-emerald-400/80 font-mono">NYSE Exchange Time (ET)</span>
      `;
    }
  } else {
    if (btnHist) btnHist.className = "px-3 py-1.5 rounded text-xs font-mono font-bold text-slate-400 hover:text-white flex items-center gap-1.5 transition";
    if (btnStream) btnStream.className = "px-3 py-1.5 rounded text-xs font-mono font-bold bg-indigo-600 text-white flex items-center gap-1.5 shadow-sm transition";
    if (pill) pill.className = "px-3 py-1 rounded-full text-xs font-mono font-bold bg-indigo-950 text-indigo-400 border border-indigo-800 flex items-center gap-2";
    if (pillText) pillText.innerText = "Mode: LIVE STREAM TICK BUFFER";
    if (notice) {
      notice.className = "text-xs px-4 py-2.5 rounded-lg bg-indigo-950/30 border border-indigo-800/50 text-indigo-300 font-mono flex items-center justify-between";
      notice.innerHTML = `
        <div class="flex items-center gap-2">
          <span>⚡</span>
          <span><strong>Live Stream Buffer Mode</strong>: Querying dynamic resampled candles from <code>data/streaming.duckdb</code> (Capital.com WebSocket). Ephemeral session data (not backfillable if offline).</span>
        </div>
        <span class="text-[11px] text-indigo-400/80 font-mono">Dynamic tick-to-bar aggregation</span>
      `;
    }
  }
  loadChartData();
}

async function loadChartData() {
  const loading = document.getElementById('chart-loading');
  if (loading) loading.classList.remove('hidden');

  const limitSelect = document.getElementById('chart-limit-select');
  currentLimit = limitSelect ? parseInt(limitSelect.value) : 500;

  try {
    const res = await fetch(`${API_BASE}/api/candles?symbol=${encodeURIComponent(currentSymbol)}&tf=${currentTimeframe}&limit=${currentLimit}&source=${currentDbSource}`);
    if (!res.ok) throw new Error("Failed to fetch candle data");
    const data = await res.json();

    loadedCandles = data.candles || [];
    // Render axis/crosshair labels in the timezone the API actually converted to (NYSE time).
    setChartTimezone(data.timezone);
    updateTzBadge(chartTimezone);
    candleTimeIndex = new Map(loadedCandles.map(c => [c.time, c]));

    if (candleSeries && volumeSeries && tvChart) {
      const chartCandles = loadedCandles.map(c => ({
        time: c.time,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close
      }));

      const chartVolumes = loadedCandles.map(c => ({
        time: c.time,
        value: c.volume || 0,
        color: (c.close >= c.open) ? 'rgba(16, 185, 129, 0.4)' : 'rgba(244, 63, 94, 0.4)'
      }));

      candleSeries.setData(chartCandles);
      volumeSeries.setData(chartVolumes);
      tvChart.timeScale().fitContent();

      if (loadedCandles.length > 0) {
        updateLegend(loadedCandles[loadedCandles.length - 1]);
      }
    }

    renderInspectorTable(loadedCandles);
  } catch (err) {
    console.error("Error loading chart data:", err);
    showToast("Error loading chart candles", "error");
  } finally {
    if (loading) loading.classList.add('hidden');
  }
}

function setTimeframe(tf) {
  currentTimeframe = tf;
  ['1m', '5m', '15m', '30m', '1h', '4h', '1d'].forEach(t => {
    const btn = document.getElementById(`tf-${t}`);
    if (btn) {
      if (t === tf) {
        btn.className = "px-2.5 py-1 rounded text-xs font-mono font-bold bg-emerald-600 text-white";
      } else {
        btn.className = "px-2.5 py-1 rounded text-xs font-mono text-slate-400 hover:text-white";
      }
    }
  });
  loadChartData();
}

function handleSymbolChange(sym) {
  currentSymbol = sym.toUpperCase();
  const select = document.getElementById('chart-symbol-select');
  if (select) select.value = currentSymbol;
  loadChartData();
}

function selectSymbolInChart(sym) {
  handleSymbolChange(sym);
  switchTab('charts');
}
