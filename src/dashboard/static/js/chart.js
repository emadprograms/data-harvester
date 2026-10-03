/**
 * Data Harvester Dashboard - Chart Engine (TradingView Lightweight Charts)
 *
 * Dedicated Segregated Dual-Chart Architecture:
 *   1. Historical Chart Controller:
 *      - Dedicated to data/historical.duckdb (canonical minute_data archive).
 *      - Renders in US Eastern Time (NYSE: America/New_York, EST/EDT).
 *      - Controlled by initHistoricalChart() and loadHistoricalChart().
 *   2. Streaming Chart Controller:
 *      - Dedicated to data/streaming.duckdb (live raw tick_data buffer).
 *      - Dynamic real-time candlestick aggregation from streaming ticks.
 *      - Controlled by initStreamingChart() and loadStreamingChart().
 */

const EXCHANGE_TIMEZONE = 'America/New_York';
const EXCHANGE_TIMEZONE_LABEL = 'ET';

if (typeof tvStreamingChart === 'undefined') {
  var tvStreamingChart = null;
}
if (typeof currentStreamingLimit === 'undefined') {
  var currentStreamingLimit = 10000;
}
if (typeof gapShadingPlugin === 'undefined') {
  var gapShadingPlugin = null;
}

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

// ============================================================================
// 1. DEDICATED HISTORICAL CHART CONTROLLER
// ============================================================================

function initHistoricalChart() {
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
        const dayStart = isExchangeDayStart(time);
        switch (tickMarkType) {
          case 0: return dayStart ? formatExchangeYear(time) : formatExchangeDay(time);
          case 1: return dayStart ? formatExchangeMonth(time) : formatExchangeDay(time);
          case 2: return dayStart ? formatExchangeDay(time) : formatExchangeClock(time);
          case 3: return formatExchangeClock(time);
          case 4: return formatExchangeClock(time, true);
          default: return null;
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
    priceScaleId: '',
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
  loadHistoricalChart();
}

/** Backward-compatible alias for existing tests */
function initChart() {
  initHistoricalChart();
}

function resizeHistoricalChart() {
  const container = document.getElementById('tv-chart-container');
  if (tvChart && container && container.clientWidth > 0) {
    tvChart.applyOptions({
      width: container.clientWidth,
      height: container.clientHeight || 450
    });
  }
}

async function loadHistoricalChart() {
  const loading = document.getElementById('chart-loading');
  if (loading) loading.classList.remove('hidden');

  const limitSelect = document.getElementById('chart-limit-select');
  currentLimit = limitSelect ? parseInt(limitSelect.value) : 500;

  try {
    const res = await fetch(`${API_BASE}/api/historical/candles?symbol=${encodeURIComponent(currentSymbol)}&tf=${currentTimeframe}&limit=${currentLimit}`);
    if (!res.ok) throw new Error("Failed to fetch historical candle data");
    const data = await res.json();

    loadedCandles = data.candles || [];
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
    console.error("Error loading historical chart data:", err);
    showToast("Error loading historical chart candles", "error");
  } finally {
    if (loading) loading.classList.add('hidden');
  }
}

/** Backward-compatible alias for existing callers */
function loadChartData() {
  loadHistoricalChart();
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
    srcEl.innerText = candle.source || 'MASSIVE';
  }

  if (dbBadge) {
    dbBadge.innerText = "HISTORICAL DB";
    dbBadge.className = "px-2 py-0.5 rounded text-[10px] bg-emerald-950 text-emerald-400 border border-emerald-800 font-mono font-bold";
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
  loadHistoricalChart();
}

function handleSymbolChange(sym) {
  currentSymbol = sym.toUpperCase();
  const select = document.getElementById('chart-symbol-select');
  if (select) select.value = currentSymbol;
  loadHistoricalChart();
}

function selectSymbolInChart(sym) {
  handleSymbolChange(sym);
  if (typeof switchDashboardView === 'function') {
    switchDashboardView('historical');
  }
  if (typeof switchTab === 'function') {
    switchTab('charts');
  }
}

// ============================================================================
// GAP SHADING CUSTOM SERIES PRIMITIVE (ISeriesPrimitive)
// ============================================================================

class GapShadingRenderer {
  constructor(data) {
    this._data = data;
  }

  draw(target) {
    if (!this._data || !this._data.bars || this._data.bars.length === 0) return;
    if (!target || typeof target.useMediaCoordinateSpace !== 'function') return;
    target.useMediaCoordinateSpace((scope) => {
      const ctx = scope.context;
      const barSpacing = this._data.barSpacing || 6;
      const halfWidth = barSpacing / 2;
      for (const bar of this._data.bars) {
        ctx.fillStyle = bar.color || 'rgba(244, 63, 94, 0.18)';
        ctx.fillRect(
          Math.round(bar.x - halfWidth),
          0,
          Math.ceil(barSpacing),
          scope.mediaSize.height
        );
      }
    });
  }
}

class GapShadingPaneView {
  constructor(plugin) {
    this._plugin = plugin;
  }

  zOrder() {
    return 'bottom';
  }

  renderer() {
    return new GapShadingRenderer(this._plugin._getViewData());
  }
}

class GapShadingPlugin {
  constructor() {
    this._chart = null;
    this._series = null;
    this._seriesData = [];
    this._gaps = [];
    this._paneViews = [new GapShadingPaneView(this)];
    this._cache = null;
    this._lastLogicalRange = { from: -1, to: -1 };
    this._lastWidth = 0;
    this._lastBarSpacing = 0;
    this._requestUpdate = () => {};
  }

  attached({ chart, series, requestUpdate }) {
    this._chart = chart;
    this._series = series;
    this._requestUpdate = requestUpdate || (() => {});
  }

  detached() {
    this._chart = null;
    this._series = null;
    this._requestUpdate = () => {};
  }

  setGaps(gaps, seriesData = null) {
    this._gaps = Array.isArray(gaps) ? gaps : [];
    this._seriesData = seriesData || [];
    this.updateAllViews();
  }

  getGaps() {
    return this._gaps;
  }

  updateAllViews() {
    this._cache = null;
    if (typeof this._requestUpdate === 'function') {
      this._requestUpdate();
    }
  }

  paneViews() {
    return this._paneViews;
  }

  _getViewData() {
    const timeScale = this._chart && typeof this._chart.timeScale === 'function'
      ? this._chart.timeScale()
      : null;

    let barSpacing = 6;
    if (timeScale && typeof timeScale.options === 'function') {
      const opts = timeScale.options();
      if (opts && typeof opts.barSpacing === 'number') {
        barSpacing = opts.barSpacing;
      }
    }

    let data = (this._seriesData && this._seriesData.length > 0)
      ? this._seriesData
      : (this._series && typeof this._series.data === 'function' ? this._series.data() : []);

    if ((!data || data.length === 0) && typeof global !== 'undefined' && global.lastSetCandles) {
      data = global.lastSetCandles;
    }
    if ((!data || data.length === 0) && typeof loadedStreamingCandles !== 'undefined') {
      data = loadedStreamingCandles;
    }

    if (!data || !Array.isArray(data) || !this._gaps || this._gaps.length === 0) {
      return { bars: [], barSpacing };
    }

    const isTimeInGap = (t, gap) => {
      const start = gap.start_epoch;
      const end = (gap.duration && gap.duration > 0)
        ? (gap.start_epoch + (gap.duration - 1) * 60)
        : (gap.end_epoch !== undefined ? gap.end_epoch : gap.start_epoch);
      return t >= start && t <= end;
    };

    const bars = [];
    for (let i = 0; i < data.length; i++) {
      const d = data[i];
      if (!d || typeof d.time !== 'number') continue;

      const matchingGap = this._gaps.find(g => isTimeInGap(d.time, g));
      if (matchingGap) {
        const status = (matchingGap.status || matchingGap.type || '').toLowerCase();
        const color = (status === 'partial') ? 'rgba(245, 158, 11, 0.18)' : 'rgba(244, 63, 94, 0.18)';
        let x = (timeScale && typeof timeScale.timeToCoordinate === 'function')
          ? timeScale.timeToCoordinate(d.time)
          : (i * barSpacing);
        if (x === null || x === undefined || isNaN(x)) {
          x = i * barSpacing;
        }
        bars.push({
          time: d.time,
          x: x,
          color: color,
          status: status || 'outage'
        });
      }
    }

    return { bars, barSpacing };
  }
}

if (typeof window !== 'undefined') {
  window.GapShadingRenderer = GapShadingRenderer;
  window.GapShadingPaneView = GapShadingPaneView;
  window.GapShadingPlugin = GapShadingPlugin;
}
if (typeof global !== 'undefined') {
  global.GapShadingRenderer = GapShadingRenderer;
  global.GapShadingPaneView = GapShadingPaneView;
  global.GapShadingPlugin = GapShadingPlugin;
}

// ============================================================================
// 2. DEDICATED STREAMING CHART CONTROLLER
// ============================================================================

let streamingCandleTimeIndex = new Map();

function findStreamingCandleByTime(time) {
  if (streamingCandleTimeIndex.has(time)) return streamingCandleTimeIndex.get(time);
  return (loadedStreamingCandles || []).find(c => c.time === time) || null;
}

function initStreamingChart() {
  const container = document.getElementById('streaming-chart-container');
  if (!container || (typeof tvStreamingChart !== 'undefined' && tvStreamingChart)) return;

  tvStreamingChart = LightweightCharts.createChart(container, {
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
      mode: (typeof LightweightCharts !== 'undefined' && LightweightCharts.CrosshairMode && LightweightCharts.CrosshairMode.Normal !== undefined) ? LightweightCharts.CrosshairMode.Normal : 0,
      vertLine: { color: '#6366f1', width: 1, style: 1 },
      horzLine: { color: '#6366f1', width: 1, style: 1 }
    },
    localization: {
      locale: 'en-US',
      timeFormatter: (time) => {
        if (typeof time !== 'number') return time ? String(time) : '--';
        return candleTimeLabel(findStreamingCandleByTime(time) || { time });
      }
    },
    timeScale: {
      borderColor: '#334155',
      timeVisible: true,
      secondsVisible: true,
      tickMarkFormatter: (time, tickMarkType, locale) => {
        if (typeof time !== 'number') return null;
        const dayStart = isExchangeDayStart(time);
        switch (tickMarkType) {
          case 0: return dayStart ? formatExchangeYear(time) : formatExchangeDay(time);
          case 1: return dayStart ? formatExchangeMonth(time) : formatExchangeDay(time);
          case 2: return dayStart ? formatExchangeDay(time) : formatExchangeClock(time);
          case 3: return formatExchangeClock(time);
          case 4: return formatExchangeClock(time, true);
          default: return null;
        }
      }
    },
    rightPriceScale: {
      borderColor: '#334155',
      scaleMargins: { top: 0.1, bottom: 0.25 }
    }
  });

  if (typeof window !== 'undefined') {
    window.tvStreamingChart = tvStreamingChart;
  }
  if (typeof global !== 'undefined') {
    global.tvStreamingChart = tvStreamingChart;
  }

  streamingCandleSeries = tvStreamingChart.addCandlestickSeries({
    upColor: '#6366f1',
    downColor: '#f43f5e',
    borderVisible: false,
    wickUpColor: '#6366f1',
    wickDownColor: '#f43f5e'
  });

  streamingVolumeSeries = tvStreamingChart.addHistogramSeries({
    priceFormat: { type: 'volume' },
    priceScaleId: '',
    scaleMargins: { top: 0.82, bottom: 0 }
  });

  gapShadingPlugin = new GapShadingPlugin();
  if (streamingCandleSeries && typeof streamingCandleSeries.attachPrimitive === 'function') {
    streamingCandleSeries.attachPrimitive(gapShadingPlugin);
  }
  if (typeof window !== 'undefined') {
    window.gapShadingPlugin = gapShadingPlugin;
    window.GapShadingPlugin = GapShadingPlugin;
    window.GapShadingPaneView = GapShadingPaneView;
    window.GapShadingRenderer = GapShadingRenderer;
  }
  if (typeof global !== 'undefined') {
    global.gapShadingPlugin = gapShadingPlugin;
    global.GapShadingPlugin = GapShadingPlugin;
    global.GapShadingPaneView = GapShadingPaneView;
    global.GapShadingRenderer = GapShadingRenderer;
  }

  tvStreamingChart.subscribeCrosshairMove(param => {
    if (!param || !param.time || !param.seriesData || !param.seriesData.get(streamingCandleSeries)) {
      if (loadedStreamingCandles.length > 0) {
        updateStreamingLegend(loadedStreamingCandles[loadedStreamingCandles.length - 1]);
      }
      return;
    }
    const data = param.seriesData.get(streamingCandleSeries);
    const candleMatch = findStreamingCandleByTime(param.time);
    updateStreamingLegend(candleMatch || data);
  });

  window.addEventListener('resize', resizeChart);
  loadStreamingChart();
}

function resizeStreamingChart() {
  const container = document.getElementById('streaming-chart-container');
  if (tvStreamingChart && container && container.clientWidth > 0) {
    tvStreamingChart.applyOptions({
      width: container.clientWidth,
      height: container.clientHeight || 450
    });
  }
}

async function loadStreamingChart(targetDate = null, targetHours = null) {
  const loading = document.getElementById('streaming-chart-loading');
  if (loading) loading.classList.remove('hidden');

  const limitSelect = document.getElementById('streaming-chart-limit-select');
  if (limitSelect && parseInt(limitSelect.value) >= 5000) {
    currentStreamingLimit = parseInt(limitSelect.value);
  } else {
    currentStreamingLimit = 10000;
    if (limitSelect) limitSelect.value = '10000';
  }
  if (typeof window !== 'undefined') window.currentStreamingLimit = currentStreamingLimit;
  if (typeof global !== 'undefined') global.currentStreamingLimit = currentStreamingLimit;

  try {
    let fetchUrl = `${API_BASE}/api/streaming/candles?symbol=${encodeURIComponent(currentStreamingSymbol)}&tf=${currentStreamingTimeframe}&limit=${currentStreamingLimit}`;
    const isGlobalExtended = (typeof window !== 'undefined' && typeof window.currentContinuityExtended === 'boolean')
      ? window.currentContinuityExtended
      : ((typeof global !== 'undefined' && typeof global.currentContinuityExtended === 'boolean')
          ? global.currentContinuityExtended
          : ((typeof currentContinuityExtended !== 'undefined') ? Boolean(currentContinuityExtended) : true));

    let hoursParam = 'extended';
    if (targetHours) {
      hoursParam = targetHours;
    } else {
      hoursParam = isGlobalExtended ? 'extended' : 'regular';
    }

    if (targetDate) {
      fetchUrl += `&date=${encodeURIComponent(targetDate)}&hours=${encodeURIComponent(hoursParam)}`;
    }

    const res = await fetch(fetchUrl);
    if (!res.ok) throw new Error("Failed to fetch streaming candle data");
    const data = await res.json();

    loadedStreamingCandles = data.candles || [];
    streamingCandleTimeIndex = new Map(loadedStreamingCandles.map(c => [c.time, c]));

    const symEl = document.getElementById('streaming-legend-symbol');
    if (symEl) {
      symEl.innerText = (data.symbol || currentStreamingSymbol).toUpperCase();
    }

    const durationEl = document.getElementById('streaming-legend-duration');
    if (durationEl) {
      const activeDate = data.date || targetDate || (typeof currentContinuityDayDate !== 'undefined' ? currentContinuityDayDate : null);
      const isExtended = (data.hours || hoursParam || 'extended').toLowerCase() === 'extended';
      const hoursWindow = isExtended ? '04:00–20:00 ET (Extended)' : '09:30–16:00 ET (Regular)';
      durationEl.innerText = activeDate ? `${activeDate} • ${hoursWindow}` : hoursWindow;
    }

    const ticksEl = document.getElementById('streaming-legend-ticks');
    if (ticksEl) {
      let tickCount = 0;
      if (typeof data.day_total_ticks === 'number') {
        tickCount = data.day_total_ticks;
      } else if (typeof data.session_total_ticks === 'number') {
        tickCount = data.session_total_ticks;
      } else if (loadedStreamingCandles && loadedStreamingCandles.length > 0) {
        tickCount = loadedStreamingCandles.reduce((acc, c) => acc + (c.tick_count || 0), 0);
      }
      ticksEl.innerText = `${tickCount.toLocaleString()} ticks in database`;
    }

    if (streamingCandleSeries && streamingVolumeSeries && tvStreamingChart) {
      let chartCandles = [];
      let chartVolumes = [];

      const startEpoch = data.session_start_epoch;
      const endEpoch = data.session_end_epoch;

      if (targetDate && typeof startEpoch === 'number' && typeof endEpoch === 'number' && endEpoch > startEpoch) {
        // Single-day drilldown: generate Lightweight Charts whitespace items for missing minutes
        const candleMap = new Map();
        loadedStreamingCandles.forEach(c => candleMap.set(c.time, c));

        for (let t = startEpoch; t < endEpoch; t += 60) {
          if (candleMap.has(t)) {
            const c = candleMap.get(t);
            chartCandles.push({
              time: t,
              open: c.open,
              high: c.high,
              low: c.low,
              close: c.close
            });
            chartVolumes.push({
              time: t,
              value: c.volume || 0,
              color: (c.close >= c.open) ? 'rgba(99, 102, 241, 0.4)' : 'rgba(244, 63, 94, 0.4)'
            });
          } else {
            // Whitespace item ({ time: t } without OHLC) renders physical empty gap on time axis
            chartCandles.push({ time: t });
          }
        }
      } else {
        chartCandles = loadedStreamingCandles.map(c => ({
          time: c.time,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close
        }));

        chartVolumes = loadedStreamingCandles.map(c => ({
          time: c.time,
          value: c.volume || 0,
          color: (c.close >= c.open) ? 'rgba(99, 102, 241, 0.4)' : 'rgba(244, 63, 94, 0.4)'
        }));
      }

      streamingCandleSeries.setData(chartCandles);
      streamingVolumeSeries.setData(chartVolumes);

      if (gapShadingPlugin && typeof gapShadingPlugin.setGaps === 'function') {
        gapShadingPlugin.setGaps(data.gaps || [], chartCandles);
      } else if (typeof window !== 'undefined' && window.gapShadingPlugin && typeof window.gapShadingPlugin.setGaps === 'function') {
        window.gapShadingPlugin.setGaps(data.gaps || [], chartCandles);
      } else if (typeof global !== 'undefined' && global.gapShadingPlugin && typeof global.gapShadingPlugin.setGaps === 'function') {
        global.gapShadingPlugin.setGaps(data.gaps || [], chartCandles);
      }
      if (streamingCandleSeries && typeof streamingCandleSeries.setMarkers === 'function') {
        streamingCandleSeries.setMarkers([]);
      }

      tvStreamingChart.timeScale().fitContent();
    }

    if (typeof loadStreamingContinuity === 'function' && !targetDate) {
      const detailView = document.getElementById('streaming-detail-view');
      const isDetailOpen = detailView && !detailView.classList.contains('hidden');
      const isGlobalExtended = (typeof currentContinuityExtended !== 'undefined') ? Boolean(currentContinuityExtended) : true;

      if (isDetailOpen) {
        // Detail view is open: fetch continuity for that specific symbol
        loadStreamingContinuity(currentStreamingSymbol, 1, isGlobalExtended);
      } else {
        // Spectrum view is open: make sure 'all' is loaded/cached, NEVER fetch single-symbol NVDA here
        const allCached = (typeof cachedAllContinuityData !== 'undefined' && cachedAllContinuityData) ||
                          (typeof window !== 'undefined' && window.cachedAllContinuityData);
        if (!allCached) {
          loadStreamingContinuity('all', currentContinuityDays || 5, isGlobalExtended);
        }
      }
    }
  } catch (err) {
    console.error("Error loading streaming chart data:", err);
    showToast("Error loading streaming chart candles", "error");
  } finally {
    if (loading) loading.classList.add('hidden');
  }
}

function updateStreamingLegend(candle) {
  if (!candle) return;
  const timeEl = document.getElementById('streaming-legend-time');
  const openEl = document.getElementById('streaming-legend-open');
  const highEl = document.getElementById('streaming-legend-high');
  const lowEl = document.getElementById('streaming-legend-low');
  const closeEl = document.getElementById('streaming-legend-close');
  const volEl = document.getElementById('streaming-legend-volume');
  const chgEl = document.getElementById('streaming-legend-change');

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
    chgEl.className = change >= 0 ? 'text-indigo-400 font-bold' : 'text-rose-400 font-bold';
  }
}

function setStreamingTimeframe(tf) {
  currentStreamingTimeframe = tf;
  ['1m', '5m', '15m', '30m', '1h'].forEach(t => {
    const btn = document.getElementById(`streaming-tf-${t}`);
    if (btn) {
      if (t === tf) {
        btn.className = "px-2.5 py-1 rounded text-xs font-mono font-bold bg-indigo-600 text-white";
      } else {
        btn.className = "px-2.5 py-1 rounded text-xs font-mono text-slate-400 hover:text-white";
      }
    }
  });
  loadStreamingChart();
}

function handleStreamingSymbolChange(sym) {
  currentStreamingSymbol = sym.toUpperCase();
  const select = document.getElementById('streaming-symbol-select');
  if (select) select.value = currentStreamingSymbol;

  // Preserve 'spectrum' mode in currentContinuityView if active
  const isSpectrum = (typeof currentContinuityView !== 'undefined' && currentContinuityView === 'spectrum') ||
                     (typeof window !== 'undefined' && window.currentContinuityView === 'spectrum');
  if (isSpectrum) {
    if (typeof currentContinuityView !== 'undefined') currentContinuityView = 'spectrum';
    if (typeof window !== 'undefined') window.currentContinuityView = 'spectrum';
  }

  loadStreamingChart();
  // Continuity refresh is handled inside loadStreamingChart (loadStreamingContinuity)
  // according to currentContinuityView, avoiding redundant clobbering of the spectrogram.
}

function selectSymbolInStreamingChart(sym) {
  if (typeof switchDashboardView === 'function') {
    switchDashboardView('streaming');
  }
  if (typeof switchStreamingTab === 'function') {
    switchStreamingTab('chart');
  }
  handleStreamingSymbolChange(sym);
}

// Global chart resizer
function resizeChart() {
  resizeHistoricalChart();
  resizeStreamingChart();
}

/**
 * Redesign: Opens single-symbol detail view with extended hours continuity and chart.
 * Hides spectrum view, shows detail view and chart card, and triggers extended data load.
 * @param {string} symbol - Ticker symbol (e.g. 'NVDA')
 */
function openSymbolDetail(symbol) {
  if (!symbol) return;
  currentStreamingSymbol = symbol.toUpperCase();
  if (typeof window !== 'undefined') window.currentStreamingSymbol = currentStreamingSymbol;
  if (typeof global !== 'undefined') global.currentStreamingSymbol = currentStreamingSymbol;

  const select = document.getElementById('streaming-symbol-select');
  if (select) select.value = currentStreamingSymbol;

  const specEl = document.getElementById('streaming-spectrum-view');
  if (specEl) specEl.classList.add('hidden');

  const detailEl = document.getElementById('streaming-detail-view');
  if (detailEl) detailEl.classList.remove('hidden');

  const chartCardEl = document.getElementById('streaming-chart-card');
  if (chartCardEl) chartCardEl.classList.remove('hidden');

  const titleEl = document.getElementById('streaming-detail-symbol-title');
  if (titleEl) {
    titleEl.innerText = `${currentStreamingSymbol}`;
  }

  const isExtended = (typeof window !== 'undefined' && typeof window.currentContinuityExtended === 'boolean')
    ? window.currentContinuityExtended
    : ((typeof global !== 'undefined' && typeof global.currentContinuityExtended === 'boolean')
        ? global.currentContinuityExtended
        : ((typeof currentContinuityExtended !== 'undefined') ? Boolean(currentContinuityExtended) : true));

  if (typeof loadStreamingContinuity === 'function') {
    loadStreamingContinuity(currentStreamingSymbol, 5, isExtended);
  }
  if (typeof loadStreamingChart === 'function') {
    loadStreamingChart();
  }
  if (typeof resizeStreamingChart === 'function') {
    resizeStreamingChart();
  }
}

/**
 * Day-Specific Drill-Down: Opens single-day detail view and single-day chart for that symbol and date.
 * Hides spectrum view, shows detail view and chart card, and triggers single-day data loads.
 * @param {string} symbol - Ticker symbol (e.g. 'NVDA')
 * @param {string} date - Session date string 'YYYY-MM-DD'
 */
function openSymbolDayDetail(symbol, date) {
  if (!symbol) return;
  currentStreamingSymbol = symbol.toUpperCase();
  currentContinuitySymbol = symbol.toUpperCase();
  currentContinuityDays = 1;
  currentContinuityDayDate = date || null;
  if (typeof window !== 'undefined') {
    window.currentStreamingSymbol = currentStreamingSymbol;
    window.currentContinuitySymbol = currentContinuitySymbol;
    window.currentContinuityDays = currentContinuityDays;
    window.currentContinuityDayDate = currentContinuityDayDate;
  }
  if (typeof global !== 'undefined') {
    global.currentStreamingSymbol = currentStreamingSymbol;
    global.currentContinuitySymbol = currentContinuitySymbol;
    global.currentContinuityDays = currentContinuityDays;
    global.currentContinuityDayDate = currentContinuityDayDate;
  }

  const select = document.getElementById('streaming-symbol-select');
  if (select) select.value = currentStreamingSymbol;

  const specEl = document.getElementById('streaming-spectrum-view');
  if (specEl) specEl.classList.add('hidden');

  const detailEl = document.getElementById('streaming-detail-view');
  if (detailEl) detailEl.classList.remove('hidden');

  const chartCardEl = document.getElementById('streaming-chart-card');
  if (chartCardEl) chartCardEl.classList.remove('hidden');

  const titleEl = document.getElementById('streaming-detail-symbol-title');
  if (titleEl) {
    titleEl.innerText = `${currentStreamingSymbol} • ${date || 'Session'}`;
  }

  const isExtended = (typeof window !== 'undefined' && typeof window.currentContinuityExtended === 'boolean')
    ? window.currentContinuityExtended
    : ((typeof global !== 'undefined' && typeof global.currentContinuityExtended === 'boolean')
        ? global.currentContinuityExtended
        : ((typeof currentContinuityExtended !== 'undefined') ? Boolean(currentContinuityExtended) : true));

  if (typeof loadStreamingContinuity === 'function') {
    loadStreamingContinuity(currentContinuitySymbol, 1, isExtended, null, date);
  }
  if (typeof loadStreamingChart === 'function') {
    loadStreamingChart(date, isExtended ? 'extended' : 'regular');
  }
  if (typeof resizeStreamingChart === 'function') {
    resizeStreamingChart();
  }
}

/**
 * Redesign: Closes single-symbol detail view and returns to 19-symbol spectrum view.
 */
function closeSymbolDetail() {
  currentContinuityDayDate = null;
  if (typeof window !== 'undefined') window.currentContinuityDayDate = null;
  if (typeof global !== 'undefined') global.currentContinuityDayDate = null;

  const specEl = document.getElementById('streaming-spectrum-view');
  if (specEl) specEl.classList.remove('hidden');

  const detailEl = document.getElementById('streaming-detail-view');
  if (detailEl) detailEl.classList.add('hidden');

  const chartCardEl = document.getElementById('streaming-chart-card');
  if (chartCardEl) chartCardEl.classList.add('hidden');

  if (typeof loadStreamingContinuity === 'function') {
    const isExtended = (typeof window !== 'undefined' && typeof window.currentContinuityExtended === 'boolean')
      ? window.currentContinuityExtended
      : ((typeof global !== 'undefined' && typeof global.currentContinuityExtended === 'boolean')
          ? global.currentContinuityExtended
          : ((typeof currentContinuityExtended !== 'undefined') ? Boolean(currentContinuityExtended) : true));
    const weekStart = (typeof currentContinuityWeekStart !== 'undefined' && currentContinuityWeekStart) ||
                      (typeof window !== 'undefined' && window.currentContinuityWeekStart);
    loadStreamingContinuity('all', 5, isExtended, weekStart, null);
  }
}

// Global window and environment exports
if (typeof window !== 'undefined') {
  window.openSymbolDetail = openSymbolDetail;
  window.openSymbolDayDetail = openSymbolDayDetail;
  window.closeSymbolDetail = closeSymbolDetail;
  window.initStreamingChart = initStreamingChart;
  window.loadStreamingChart = loadStreamingChart;
  window.resizeStreamingChart = resizeStreamingChart;
  window.gapShadingPlugin = gapShadingPlugin;
  window.GapShadingPlugin = GapShadingPlugin;
  window.GapShadingPaneView = GapShadingPaneView;
  window.GapShadingRenderer = GapShadingRenderer;
}
if (typeof global !== 'undefined') {
  global.openSymbolDetail = openSymbolDetail;
  global.openSymbolDayDetail = openSymbolDayDetail;
  global.closeSymbolDetail = closeSymbolDetail;
  global.initStreamingChart = initStreamingChart;
  global.loadStreamingChart = loadStreamingChart;
  global.resizeStreamingChart = resizeStreamingChart;
  global.gapShadingPlugin = gapShadingPlugin;
  global.GapShadingPlugin = GapShadingPlugin;
  global.GapShadingPaneView = GapShadingPaneView;
  global.GapShadingRenderer = GapShadingRenderer;
}
