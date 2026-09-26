/**
 * Data Harvester Dashboard - Chart Engine (TradingView Lightweight Charts)
 */

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
      timeFormatter: (time) => {
        if (typeof time === 'number') {
          const d = new Date(time * 1000);
          return d.toISOString().replace('T', ' ').slice(0, 16) + ' ET';
        }
        return String(time);
      }
    },
    timeScale: {
      borderColor: '#334155',
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter: (time, tickMarkType, locale) => {
        if (typeof time !== 'number') return null;
        const d = new Date(time * 1000);
        switch (tickMarkType) {
          case 0: // Year
            return String(d.getUTCFullYear());
          case 1: // Month
            return d.toLocaleString('en-US', { timeZone: 'UTC', month: 'short' });
          case 2: // DayOfMonth
            return `${d.toLocaleString('en-US', { timeZone: 'UTC', month: 'short' })} ${d.getUTCDate()}`;
          case 3: // Time
            return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
          case 4: // TimeWithSeconds
            return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}:${String(d.getUTCSeconds()).padStart(2, '0')}`;
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
    const candleMatch = loadedCandles.find(c => c.time === param.time);
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
  if (timeEl) {
    if (candle.time_str) {
      timeEl.innerText = `${candle.time_str} ET`;
    } else if (candle.time) {
      const d = new Date(candle.time * 1000);
      timeEl.innerText = `${d.toISOString().replace('T', ' ').slice(0, 19)} ET`;
    } else {
      timeEl.innerText = '--';
    }
  }
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
