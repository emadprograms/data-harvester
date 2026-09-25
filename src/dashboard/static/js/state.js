/**
 * Data Harvester Dashboard - Global State & UI Feedback
 */

// Host and API configuration
const API_BASE = window.location.origin;

// Application State
let currentSymbol = 'NVDA';
let currentTimeframe = '1m';
let currentLimit = 500;
let currentDbSource = 'historical'; // 'historical' or 'streaming'
let loadedCandles = [];
let allSymbolsCoverage = [];
let previousTicks = {};

// TradingView Chart reference handles
let tvChart = null;
let candleSeries = null;
let volumeSeries = null;

/**
 * Display floating toast notification in bottom right corner.
 * @param {string} message - Notification text
 * @param {'success'|'error'} type - Message type
 */
function showToast(message, type = 'success') {
  const toast = document.getElementById('toast');
  if (!toast) return;
  toast.innerText = message;
  toast.className = `fixed bottom-6 right-6 px-4 py-3 rounded-lg shadow-xl text-xs font-semibold transition-all transform z-50 flex items-center gap-2 ${
    type === 'success' ? 'bg-emerald-600 text-white' : 'bg-rose-600 text-white'
  }`;
  toast.classList.remove('translate-y-20', 'opacity-0');
  setTimeout(() => {
    toast.classList.add('translate-y-20', 'opacity-0');
  }, 3500);
}
