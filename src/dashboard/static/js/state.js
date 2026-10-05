/**
 * Data Harvester Dashboard - Global State & UI Feedback
 */

// Host and API configuration
const API_BASE = window.location.origin;

// Application State - Parquet Tick Lake Dashboard
var currentSymbol = 'NVDA';
var currentTimeframe = '1m';
var currentLimit = 500;
var currentDbSource = 'streaming'; // The Parquet tick lake is the only store
var loadedCandles = [];
var allSymbolsCoverage = [];
var previousTicks = {};

// Application State - Streaming Dashboard
var currentStreamingSymbol = 'NVDA';
var currentStreamingTimeframe = '1m';
var currentStreamingLimit = 10000;
if (typeof window !== 'undefined') window.currentStreamingLimit = currentStreamingLimit;
if (typeof global !== 'undefined') global.currentStreamingLimit = currentStreamingLimit;
var loadedStreamingCandles = [];
var streamingSymbolsList = [];
var currentDashboardView = 'streaming'; // single view: the lake
var currentStreamingTab = 'chart'; // 'chart', 'tape', or 'daemon'

// TradingView Chart reference handles - Lake
var tvChart = null;
var candleSeries = null;
var volumeSeries = null;

// TradingView Chart reference handles - Streaming
var tvStreamingChart = null;
var streamingCandleSeries = null;
var streamingVolumeSeries = null;
var gapShadingPlugin = null;

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
