/**
 * Data Harvester Dashboard - Telemetry, Stream Tape, Market Session & Integrity
 */

// --- Stream Telemetry & Ticker Tape ---
async function fetchStreamTape() {
  try {
    const res = await fetch(`${API_BASE}/api/stream/tape?limit=50`);
    if (!res.ok) return;
    const data = await res.json();
    const ticks = data.ticks || [];

    // Render Tape Table
    const tbody = document.getElementById('tape-table-body');
    if (tbody) {
      tbody.innerHTML = '';
      if (ticks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="py-6 text-center text-slate-500">No stream ticks recorded yet.</td></tr>';
      } else {
        ticks.forEach(t => {
          const tr = document.createElement('tr');
          tr.className = "hover:bg-slate-800/40 transition";
          tr.innerHTML = `
            <td class="py-2.5 px-4 text-slate-400 font-mono">${t.timestamp}</td>
            <td class="py-2.5 px-4 font-bold text-white font-mono">${t.symbol}</td>
            <td class="py-2.5 px-4 text-right font-bold text-emerald-400 font-mono">${t.price !== null ? t.price.toFixed(4) : '--'}</td>
            <td class="py-2.5 px-4 text-right text-slate-300 font-mono">${t.bid !== null ? t.bid.toFixed(4) : '--'}</td>
            <td class="py-2.5 px-4 text-right text-slate-300 font-mono">${t.ask !== null ? t.ask.toFixed(4) : '--'}</td>
            <td class="py-2.5 px-4 text-right text-amber-400 font-mono">${t.spread !== null ? t.spread.toFixed(4) : '--'}</td>
            <td class="py-2.5 px-4 text-center">
              <span class="px-2 py-0.5 rounded text-[10px] bg-indigo-950 text-indigo-400 border border-indigo-800 font-mono">${t.source}</span>
            </td>
          `;
          tbody.appendChild(tr);
        });
      }
    }

    // Render Ticker Cards Grid
    renderTickerGrid(ticks);
  } catch (err) {
    console.error("Error fetching stream tape:", err);
  }
}

function renderTickerGrid(ticks) {
  const grid = document.getElementById('stream-ticker-grid');
  if (!grid) return;

  // Group latest tick per symbol
  const latestPerSym = {};
  ticks.forEach(t => {
    if (!latestPerSym[t.symbol]) {
      latestPerSym[t.symbol] = t;
    }
  });

  grid.innerHTML = '';
  Object.keys(latestPerSym).forEach(sym => {
    const t = latestPerSym[sym];
    const prev = previousTicks[sym];
    const isUp = prev ? t.price >= prev.price : true;
    const flashClass = prev ? (t.price > prev.price ? 'flash-up' : t.price < prev.price ? 'flash-down' : '') : '';
    previousTicks[sym] = t;

    const card = document.createElement('div');
    card.className = `p-3 rounded-xl bg-slate-950 border border-slate-800/80 cursor-pointer hover:border-emerald-500/50 transition ${flashClass}`;
    card.onclick = () => selectSymbolInChart(sym);
    card.innerHTML = `
      <div class="flex items-center justify-between">
        <span class="font-bold text-white font-mono text-xs">${sym}</span>
        <span class="text-[10px] font-mono text-slate-500">${t.source}</span>
      </div>
      <div class="mt-1 text-base font-bold font-mono ${isUp ? 'text-emerald-400' : 'text-rose-400'}">
        $${t.price ? t.price.toFixed(2) : '--'}
      </div>
      <div class="mt-1 flex items-center justify-between text-[10px] text-slate-400 font-mono">
        <span>Sp: ${t.spread !== null ? t.spread.toFixed(2) : '--'}</span>
        <span class="text-slate-500">${t.timestamp.split(' ')[1] || ''}</span>
      </div>
    `;
    grid.appendChild(card);
  });
}

// --- Stream Status & Process Telemetry ---
async function fetchStreamStatus() {
  try {
    const res = await fetch(`${API_BASE}/api/stream/status`);
    if (!res.ok) return;
    const data = await res.json();

    // Top bar status badge
    const badge = document.getElementById('stream-status-badge');
    const text = document.getElementById('stream-status-text');
    if (badge && text) {
      if (data.is_alive) {
        badge.className = "flex items-center gap-2 px-3 py-1.5 rounded-md bg-emerald-950/70 border border-emerald-800 text-xs font-medium text-emerald-400";
        text.innerHTML = `<span class="h-2 w-2 rounded-full bg-emerald-500 live-pulse"></span> STREAM: LIVE (PID ${data.pid})`;
      } else {
        badge.className = "flex items-center gap-2 px-3 py-1.5 rounded-md bg-slate-800 border border-slate-700 text-xs font-medium text-slate-400";
        text.innerHTML = `<span class="h-2 w-2 rounded-full bg-slate-500"></span> STREAM: IDLE`;
      }
    }

    // Tab 2 Daemon info
    const dPill = document.getElementById('stream-daemon-pill');
    const dDetails = document.getElementById('stream-daemon-details');
    if (dPill) {
      dPill.className = `px-2 py-0.5 rounded text-[11px] font-bold font-mono ${
        data.is_alive ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' : 'bg-slate-800 text-slate-400'
      }`;
      dPill.innerText = data.is_alive ? `ACTIVE (PID ${data.pid})` : 'STOPPED / STANDBY';
    }
    if (dDetails) {
      dDetails.innerHTML = `Process ID: <span class="font-mono text-slate-300">${data.pid || 'None'}</span> • Total ticks: <span class="font-mono text-white font-bold">${Number(data.ticks_total || 0).toLocaleString()}</span> • Ingestion rate: <span class="font-mono text-emerald-400 font-bold">${data.ticks_last_minute || 0} ticks/min</span>`;
    }

    // KPI card update
    const kpiRate = document.getElementById('kpi-stream-rate');
    if (kpiRate) kpiRate.innerText = `${data.ticks_last_minute || 0} ticks/m`;
  } catch (err) {
    console.error("Error fetching stream status:", err);
  }
}

// --- Market Session & Clock ---
async function fetchMarketSession() {
  try {
    const res = await fetch(`${API_BASE}/api/market/session`);
    if (!res.ok) return;
    const data = await res.json();

    // Top Clock
    const clockEt = document.getElementById('clock-et');
    if (clockEt && data.time_et) clockEt.innerText = data.time_et.split(' ')[1];
    
    // Phase Badge
    const badge = document.getElementById('market-phase-badge');
    if (badge && data.phase) {
      badge.innerText = data.phase.replace('_', ' ');
      if (data.phase === 'REGULAR') {
        badge.className = "px-2 py-0.5 rounded text-[11px] font-bold bg-emerald-950 text-emerald-400 border border-emerald-800";
      } else if (data.phase === 'PRE_MARKET' || data.phase === 'AFTER_HOURS') {
        badge.className = "px-2 py-0.5 rounded text-[11px] font-bold bg-amber-950 text-amber-400 border border-amber-800";
      } else {
        badge.className = "px-2 py-0.5 rounded text-[11px] font-bold bg-slate-800 text-slate-400";
      }
    }

    // Cutoff Countdown
    const countdown = document.getElementById('clock-countdown');
    if (countdown && data.seconds_to_session_cutoff !== undefined) {
      const secs = data.seconds_to_session_cutoff;
      const hrs = Math.floor(secs / 3600);
      const mins = Math.floor((secs % 3600) / 60);
      const remSecs = secs % 60;
      countdown.innerText = 
        `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${remSecs.toString().padStart(2, '0')}`;
    }

    // KPI Market Session
    const kpiStatus = document.getElementById('kpi-session-status');
    const kpiDate = document.getElementById('kpi-target-date');
    if (kpiStatus) kpiStatus.innerText = data.phase;
    if (kpiDate) kpiDate.innerText = data.active_session_date;
  } catch (err) {
    console.error("Error fetching market session:", err);
  }
}

// --- System & Database Health Telemetry ---
async function fetchSystemHealth() {
  try {
    const res = await fetch(`${API_BASE}/api/status`);
    if (!res.ok) return;
    const data = await res.json();

    if (data.historical) {
      if (data.historical.size_mb) {
        const el = document.getElementById('kpi-hist-size');
        if (el) el.innerText = `${data.historical.size_mb} MB`;
      }
      if (data.historical.market_data_rows) {
        const el = document.getElementById('kpi-hist-rows');
        if (el) el.innerText = Number(data.historical.market_data_rows).toLocaleString();
      }
      if (data.historical.min_timestamp && data.historical.max_timestamp) {
        const dMin = data.historical.min_timestamp.slice(0, 7);
        const dMax = data.historical.max_timestamp.slice(0, 7);
        const el = document.getElementById('kpi-hist-span');
        if (el) el.innerText = `${dMin} → ${dMax}`;
      }
      if (data.historical.symbols_count) {
        const el = document.getElementById('kpi-symbols-count');
        if (el) el.innerText = `${data.historical.symbols_count} symbols`;
      }
    }
    if (data.streaming) {
      const elRows = document.getElementById('kpi-stream-rows');
      if (elRows) elRows.innerText = Number(data.streaming.ticks_rows || 0).toLocaleString();
      if (data.streaming.max_timestamp) {
        const elTs = document.getElementById('kpi-stream-last-ts');
        if (elTs) elTs.innerText = data.streaming.max_timestamp.split(' ')[1] || data.streaming.max_timestamp;
      }
    }
  } catch (err) {
    console.error("Error fetching system health:", err);
  }
}

// --- Data Integrity Audits ---
async function runIntegrityAudit() {
  const btn = document.getElementById('run-audit-btn');
  const targetSymInput = document.getElementById('integrity-symbol-target');
  const targetSym = targetSymInput ? targetSymInput.value : '';
  if (btn) {
    btn.innerText = "⏳ Auditing...";
    btn.disabled = true;
  }

  try {
    const url = targetSym ? `${API_BASE}/api/integrity?symbol=${encodeURIComponent(targetSym)}` : `${API_BASE}/api/integrity`;
    const res = await fetch(url);
    const data = await res.json();

    const banner = document.getElementById('audit-banner');
    const icon = document.getElementById('audit-result-icon');
    const title = document.getElementById('audit-result-title');
    const sub = document.getElementById('audit-result-subtitle');

    if (banner && icon && title && sub) {
      if (data.overall_passed) {
        banner.className = "p-5 rounded-xl border border-emerald-800 bg-emerald-950/40 flex flex-wrap items-center justify-between gap-4";
        icon.innerText = "✅";
        title.innerText = "Integrity Verified: ALL CHECKS PASSED";
        sub.innerText = "No corrupted candles, zero OHLCV inversions, and continuous time-series confirmed.";
        const b = document.getElementById('kpi-integrity-badge');
        const s = document.getElementById('kpi-integrity-status');
        if (b) b.innerText = "PASSED";
        if (s) {
          s.innerText = "PASSED";
          s.className = "text-lg font-bold font-mono text-emerald-400";
        }
      } else {
        banner.className = "p-5 rounded-xl border border-amber-800 bg-amber-950/40 flex flex-wrap items-center justify-between gap-4";
        icon.innerText = "⚠️";
        title.innerText = "Audit Warning: Anomalies or Gaps Detected";
        sub.innerText = "Review detected gaps or price discrepancies below.";
        const b = document.getElementById('kpi-integrity-badge');
        const s = document.getElementById('kpi-integrity-status');
        if (b) b.innerText = "WARNING";
        if (s) {
          s.innerText = "REVIEW";
          s.className = "text-lg font-bold font-mono text-amber-400";
        }
      }
    }

    // Render Gaps
    const gapsCont = document.getElementById('gaps-container');
    if (gapsCont) {
      gapsCont.innerHTML = '';
      if (data.gaps && data.gaps.length > 0) {
        data.gaps.forEach(g => {
          const row = document.createElement('div');
          row.className = "p-3 bg-slate-950 border border-slate-800 rounded-lg flex items-center justify-between text-xs";
          row.innerHTML = `
            <div class="flex items-center gap-3">
              <span class="font-bold text-white font-mono">${g.symbol}</span>
              <span class="text-slate-400">${g.actual_bars} bars recorded</span>
              <span class="text-slate-500">•</span>
              <span class="${g.missing_minutes === 0 ? 'text-emerald-400' : 'text-amber-400'} font-semibold font-mono">
                ${g.missing_minutes} missing mins (${g.coverage_pct}% coverage)
              </span>
            </div>
            <span class="text-xs ${g.passed ? 'text-emerald-400' : 'text-amber-400'} font-bold">
              ${g.passed ? '✓ CONTINUOUS' : '⚠️ GAPS DETECTED'}
            </span>
          `;
          gapsCont.appendChild(row);
        });
      }
    }

    // Render Anomalies
    const anomCont = document.getElementById('anomalies-container');
    if (anomCont) {
      anomCont.innerHTML = '';
      const a = data.anomalies;
      if (a) {
        const aBox = document.createElement('div');
        aBox.className = "p-3 bg-slate-950 border border-slate-800 rounded-lg text-xs flex items-center justify-between";
        aBox.innerHTML = `
          <div>
            <span class="font-bold text-white">OHLCV Consistency:</span>
            <span class="text-slate-400 ml-2">${a.total_bars_inspected.toLocaleString()} bars inspected</span>
            <span class="text-slate-500 mx-2">•</span>
            <span class="${a.anomaly_count === 0 ? 'text-emerald-400' : 'text-rose-400'} font-bold">
              ${a.anomaly_count} anomalies found
            </span>
          </div>
          <span class="${a.passed ? 'text-emerald-400' : 'text-rose-400'} font-bold">
            ${a.passed ? '✓ 100% SANITY PASS' : '❌ ANOMALY DETECTED'}
          </span>
        `;
        anomCont.appendChild(aBox);
      }
    }

    // Render Drift
    const driftCont = document.getElementById('drift-container');
    if (driftCont) {
      driftCont.innerHTML = '';
      if (data.drift && data.drift.length > 0) {
        data.drift.forEach(d => {
          const dRow = document.createElement('div');
          dRow.className = "p-3 bg-slate-950 border border-slate-800 rounded-lg flex items-center justify-between text-xs";
          dRow.innerHTML = `
            <div>
              <span class="font-bold text-white font-mono">${d.symbol}</span>
              <span class="text-slate-400 ml-2">${d.overlapping_bars} matching bars</span>
              <span class="text-slate-500 mx-2">•</span>
              <span class="text-slate-300 font-mono">Mean Drift: $${d.mean_absolute_drift.toFixed(4)} (Max: $${d.max_drift.toFixed(4)})</span>
            </div>
            <span class="${d.passed ? 'text-emerald-400' : 'text-amber-400'} font-bold">
              ${d.passed ? '✓ DRIFT OK' : '⚠️ HIGH DRIFT'}
            </span>
          `;
          driftCont.appendChild(dRow);
        });
      }
    }

    showToast("✅ Integrity audit finished!");
  } catch (err) {
    showToast("❌ Audit error", "error");
  } finally {
    if (btn) {
      btn.innerText = "🔍 Run Deep Audit";
      btn.disabled = false;
    }
  }
}
