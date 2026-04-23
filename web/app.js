/* ═══════════════════════════════════════════════════════════
   PowerTrader · Obsidian Command · Frontend
   Multi-exchange: control (zero-friction baseline) vs kraken (real)
   ═══════════════════════════════════════════════════════════ */

const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];

const XK_COLORS = { control: '#00D4FF', kraken: '#F0B429' };
const XK_LABELS = { control: 'C', kraken: 'K' };

const state = {
  coins: [],
  exchangeList: [],
  exchangeData: {},  // { control: {account, pnl}, kraken: {account, pnl} }
  positions: {},     // { control: {...}, kraken: {...} }
  dca24h: {},        // { control: {...}, kraken: {...} }
  tradeStartLevel: 1,
  selectedCoin: null,
  selectedTf: '1hour',
  neuralRunning: false,
  traderRunning: false,
  chart: null,
  candleSeries: null,
  acctSeries: {},    // { control: LineSeries, kraken: LineSeries }
  priceLines: [],
  chartRefreshTimer: null,
  chartMode: 'candle',
  accountRange: 0,
  logRefreshTimer: null,
  settings: {},
  cardMode: 'simple',
  historyFilterCoin: null,
};

const TF_LIST = ['1min','5min','15min','30min','1hour','2hour','4hour','8hour','12hour','1day','1week'];
const TF_REFRESH_MS = {
  '1min': 5_000, '5min': 15_000, '15min': 30_000, '30min': 45_000,
  '1hour': 60_000, '2hour': 90_000, '4hour': 120_000,
  '8hour': 180_000, '12hour': 300_000, '1day': 300_000, '1week': 600_000,
};
const TF_SECONDS = {
  '1min': 60, '5min': 300, '15min': 900, '30min': 1800,
  '1hour': 3600, '2hour': 7200, '4hour': 14400,
  '8hour': 28800, '12hour': 43200, '1day': 86400, '1week': 604800,
};
const ACCT_RANGES = [
  {label: '1D', hours: 24}, {label: '3D', hours: 72}, {label: '1W', hours: 168},
  {label: '2W', hours: 336}, {label: '1M', hours: 720}, {label: 'ALL', hours: 0},
];
const CHART_COLORS = {
  bg: '#0B0B14',
  text: '#555570',
  grid: '#1A1A2E',
  up: '#00CC66',
  down: '#FF4466',
  border: '#2A2A48',
};

function fmtUSD(v) {
  if (v == null || isNaN(v)) return '—';
  const n = Number(v);
  if (Math.abs(n) >= 1000) return '$' + n.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  if (Math.abs(n) >= 1) return '$' + n.toFixed(2);
  return '$' + n.toPrecision(4);
}

function fmtPct(v) {
  if (v == null || isNaN(v)) return '—';
  const n = Number(v);
  return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
}

function fmtQty(v, coin) {
  if (v == null) return '—';
  const n = Number(v);
  if (n === 0) return '0';
  if (n >= 100) return n.toFixed(2);
  if (n >= 1) return n.toFixed(4);
  return n.toPrecision(4);
}

function fmtPrice(v) {
  if (v == null || isNaN(v)) return '—';
  const n = Number(v);
  if (n >= 1000) return n.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  if (n >= 1) return n.toFixed(2);
  if (n >= 0.01) return n.toFixed(4);
  return n.toPrecision(4);
}

function fmtTime(ts) {
  if (!ts) return '';
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('en-US', {hour: '2-digit', minute: '2-digit', hour12: false});
}

function fmtDateTime(ts) {
  if (!ts) return '';
  const d = new Date(ts * 1000);
  const Y = d.getFullYear();
  const M = String(d.getMonth() + 1).padStart(2, '0');
  const D = String(d.getDate()).padStart(2, '0');
  const h = String(d.getHours()).padStart(2, '0');
  const m = String(d.getMinutes()).padStart(2, '0');
  return `${Y}-${M}-${D} ${h}:${m}`;
}

function fmtDate(ts) {
  if (!ts) return '';
  const d = new Date(ts * 1000);
  return d.toLocaleDateString('en-US', {month: 'short', day: 'numeric'}) + ' ' +
         d.toLocaleTimeString('en-US', {hour: '2-digit', minute: '2-digit', hour12: false});
}

// ── API ──

async function api(path, opts = {}) {
  const resp = await fetch('/api/' + path, {
    headers: {'Content-Type': 'application/json'},
    ...opts,
  });
  return resp.json();
}

async function apiPost(path) { return api(path, {method: 'POST'}); }
async function apiPut(path, body) { return api(path, {method: 'PUT', body: JSON.stringify(body)}); }

// ── WebSocket ──

let ws = null;
let wsRetry = 0;

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => { wsRetry = 0; };
  ws.onclose = () => {
    const delay = Math.min(1000 * Math.pow(2, wsRetry++), 30000);
    setTimeout(connectWS, delay);
  };
  ws.onerror = () => {};
  ws.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      handleWSMessage(msg);
    } catch {}
  };
}

function handleWSMessage(msg) {
  switch (msg.type) {
    case 'trader_status':
      updateTraderStatus(msg.exchange, msg.data);
      break;
    case 'signals':
      updateSignals(msg.data);
      break;
    case 'pnl':
      updatePnl(msg.exchange, msg.data);
      break;
    case 'system':
      updateSystemStatus(msg.data);
      break;
    case 'runner_ready':
      break;
  }
}

// ── Initialize ──

async function init() {
  setupTabs();
  setupMobileNav();
  setupButtons();
  setupTimeframes();
  setupSash();
  setupViewToggle();

  await refreshAll();
  connectWS();

  setInterval(refreshAll, 10000);
}

async function refreshAll() {
  try {
    const [statusData, coinsData, posData, settingsData] = await Promise.all([
      api('status'), api('coins'), api('positions'), api('settings'),
    ]);

    state.settings = settingsData;
    state.tradeStartLevel = settingsData.trade_start_level || 1;
    state.exchangeList = statusData.exchange_list || ['control'];

    if (statusData.exchanges) {
      state.exchangeData = statusData.exchanges;
    }

    updateSystemStatus(statusData.system);
    renderTopbarAccounts();

    if (coinsData.coins) {
      state.coins = coinsData.coins;
    }

    if (posData.positions) {
      state.positions = posData.positions;
      state.dca24h = posData.dca_24h || {};
    }

    mergePositionsIntoCoins();
    renderCoinGrid();
    populateLogSourceDropdown();

    if ($('#tab-compare').classList.contains('active')) loadCompare();
    renderTraining(coinsData.coins);
    renderSettings(settingsData);
  } catch (e) {
    console.error('refreshAll failed:', e);
  }
}

// ── System Status ──

function updateSystemStatus(sys) {
  if (!sys) return;
  state.neuralRunning = sys.neural_running;
  state.traderRunning = sys.trader_running;
  if (sys.traders) state.tradersStatus = sys.traders;

  const pillNeural = $('#pill-neural');
  const pillTrader = $('#pill-trader');

  pillNeural.className = 'vital-pill ' + (sys.neural_running ? 'running' : 'stopped');
  pillTrader.className = 'vital-pill ' + (sys.trader_running ? 'running' : 'stopped');

  const running = sys.neural_running || sys.trader_running;
  const btnStart = $('#btn-start-all');
  const btnStop = $('#btn-stop-all');
  btnStart.style.display = running ? 'none' : '';
  btnStop.style.display = running ? '' : 'none';

  const hasPositions = Object.values(state.positions || {}).some(xkPos =>
    Object.values(xkPos || {}).some(p => p && p.quantity > 0)
  );

  const btnClose = $('#btn-close-all');
  if (btnClose) btnClose.disabled = !hasPositions;

  const btnSync = $('#btn-sync-control');
  if (btnSync) {
    btnSync.disabled = sys.trader_running || hasPositions;
  }
}

// ── Topbar Account Display ──

function renderTopbarAccounts() {
  const container = $('#topbar-account');
  if (!container) return;
  const xks = state.exchangeList;

  let html = '<table class="xk-table"><thead><tr><th></th><th>Portfolio</th><th>Buying Power</th><th>Holdings</th><th>Inv %</th><th>P&L</th>';
  if (xks.length >= 2) html += '<th>Δ</th>';
  html += '</tr></thead><tbody>';

  xks.forEach((xk, i) => {
    const xd = state.exchangeData[xk] || {};
    const acct = xd.account || {};
    const pnl = xd.pnl || {};
    const total = acct.total_account_value || 0;
    const bp = acct.buying_power || 0;
    const holdings = acct.holdings_sell_value || 0;
    const pctInv = acct.percent_in_trade || 0;
    const realized = pnl.total_realized_profit_usd || 0;
    const rClass = realized >= 0 ? 'positive' : 'negative';
    const color = XK_COLORS[xk] || '#888';

    html += `<tr>
      <td class="xk-table-name"><span class="xk-dot" style="background:${color}"></span>${xk}</td>
      <td class="xk-table-val" data-xk-total="${xk}">${fmtUSD(total)}</td>
      <td class="xk-table-val" data-xk-bp="${xk}">${fmtUSD(bp)}</td>
      <td class="xk-table-val" data-xk-hold="${xk}">${fmtUSD(holdings)}</td>
      <td class="xk-table-val" data-xk-inv="${xk}">${pctInv.toFixed(1)}%</td>
      <td class="xk-table-val ${rClass}" data-xk-pnl="${xk}">${(realized >= 0 ? '+' : '') + fmtUSD(realized)}</td>`;

    if (xks.length >= 2) {
      if (i === 0) {
        html += '<td class="xk-table-val xk-table-dash">—</td>';
      } else {
        const t0 = (state.exchangeData[xks[0]]?.account?.total_account_value || 0);
        const t1 = (state.exchangeData[xks[1]]?.account?.total_account_value || 0);
        const delta = t1 - t0;
        const dPct = t0 > 0 ? (delta / t0) * 100 : 0;
        const dClass = delta >= 0 ? 'positive' : 'negative';
        html += `<td class="xk-table-val ${dClass}" id="acct-delta">${(delta >= 0 ? '+' : '') + fmtUSD(delta)}</td>`;
      }
    }
    html += '</tr>';
  });

  html += '</tbody></table>';
  container.innerHTML = html;
}

function updateTopbarExchange(xk, account, pnl) {
  if (account) {
    if (state.exchangeData[xk]) state.exchangeData[xk].account = account;
    const setEl = (attr, val) => { const el = $(`[${attr}="${xk}"]`); if (el) el.textContent = val; };
    setEl('data-xk-total', fmtUSD(account.total_account_value || 0));
    setEl('data-xk-bp', fmtUSD(account.buying_power || 0));
    setEl('data-xk-hold', fmtUSD(account.holdings_sell_value || 0));
    setEl('data-xk-inv', (account.percent_in_trade || 0).toFixed(1) + '%');
  }
  if (pnl) {
    if (state.exchangeData[xk]) state.exchangeData[xk].pnl = pnl;
    const v = pnl.total_realized_profit_usd || 0;
    const el = $(`[data-xk-pnl="${xk}"]`);
    if (el) {
      el.textContent = (v >= 0 ? '+' : '') + fmtUSD(v);
      el.className = 'xk-table-val ' + (v >= 0 ? 'positive' : 'negative');
    }
  }

  if (state.exchangeList.length >= 2) {
    const xks = state.exchangeList;
    const t0 = (state.exchangeData[xks[0]]?.account?.total_account_value || 0);
    const t1 = (state.exchangeData[xks[1]]?.account?.total_account_value || 0);
    const delta = t1 - t0;
    const el = $('#acct-delta');
    if (el) {
      el.textContent = (delta >= 0 ? '+' : '') + fmtUSD(delta);
      el.className = 'xk-table-val ' + (delta >= 0 ? 'positive' : 'negative');
    }
  }
}

function updatePnl(xk, pnl) {
  if (!pnl || !xk) return;
  updateTopbarExchange(xk, null, pnl);
}

// ── Trader Status (from WS) ──

function updateTraderStatus(xk, data) {
  if (!data || !xk) return;
  if (data.account) updateTopbarExchange(xk, data.account, null);
  if (data.positions) {
    if (!state.positions[xk]) state.positions[xk] = {};
    state.positions[xk] = data.positions;
    mergePositionsIntoCoins();
    renderCoinGrid();
    if ($('#tab-lth').classList.contains('active')) renderLTH();
    if (state.selectedCoin) {
      updateCoinPosition(state.selectedCoin);
      updateMidPriceLine();
    }
    if ($('#tab-compare').classList.contains('active')) loadCompare();
  }
}

// ── Coin Grid (unified signal + position cards) ──

let _lastCardMode = null;

function _createSimpleCard(coin) {
  const card = document.createElement('div');
  card.className = 'coin-card simple';
  card.dataset.coin = coin;
  card.innerHTML = `
    <span class="cc-name">${coin}</span>
    <span class="cc-field"><span class="cc-mid" data-f="mid"></span></span>
    <span class="cc-field cc-pos-fields" data-f="pos-fields"></span>
    <span class="cc-field cc-ls" title="Long / Short (0–7)">
      <span class="cc-long" data-f="long"></span><span class="cc-sep">/</span><span class="cc-short" data-f="short"></span>
    </span>`;
  card.addEventListener('click', () => selectCoin(coin));
  return card;
}

function _createDetailCard(coin) {
  const card = document.createElement('div');
  card.className = 'coin-card detail';
  card.dataset.coin = coin;
  card.innerHTML = `
    <div class="cc-header">
      <div class="cc-name-col">
        <span class="cc-name">${coin}</span>
        <span class="cc-mid" data-f="mid"></span>
      </div>
      <div class="cc-bars" title="Signal strength (0–7) · Marker = trade start level">
        <div class="cc-bar-row"><span class="cc-bar-lbl">L</span><div class="signal-bar"><div class="signal-bar-fill long" data-f="long-bar"></div><div class="signal-bar-marker" data-f="marker"></div></div><span class="cc-long" data-f="long"></span></div>
        <div class="cc-bar-row"><span class="cc-bar-lbl">S</span><div class="signal-bar"><div class="signal-bar-fill short" data-f="short-bar"></div></div><span class="cc-short" data-f="short"></span></div>
      </div>
    </div>
    <div class="cc-position" data-f="pos-section"></div>`;
  card.addEventListener('click', () => selectCoin(coin));
  return card;
}

function _updateCard(card, c, modeOverride) {
  const mode = modeOverride || (card.classList.contains('detail') ? 'detail' : 'simple');
  const xks = state.exchangeList;
  const positions = c.positions || {};
  const hasAnyPos = xks.some(xk => positions[xk] && positions[xk].quantity > 0);
  const mid = c.mid_price || 0;
  const tsl = state.tradeStartLevel || 1;
  const isTradeReady = c.long_signal >= tsl && c.short_signal === 0;

  card.classList.toggle('active', state.selectedCoin === c.coin);
  card.classList.toggle('trade-ready', isTradeReady);
  card.classList.toggle('has-position', hasAnyPos);

  const f = key => card.querySelector(`[data-f="${key}"]`);

  f('mid').textContent = fmtPrice(mid);
  f('long').textContent = c.long_signal;
  f('short').textContent = c.short_signal;

  if (mode === 'simple') {
    const container = f('pos-fields');
    if (hasAnyPos) {
      let html = '';
      xks.forEach(xk => {
        const pos = positions[xk];
        if (!pos || pos.quantity <= 0) return;
        const pnl = pos.gain_loss_pct_buy;
        const pnlClass = pnl >= 0 ? 'positive' : 'negative';
        const color = XK_COLORS[xk] || '#888';
        html += `<span class="cc-xk-pos"><span class="cc-xk-tag" style="color:${color}">${XK_LABELS[xk] || xk[0].toUpperCase()}</span>${fmtUSD(pos.value_usd)} <span class="cc-pnl ${pnlClass}">${fmtPct(pnl)}</span></span>`;
      });
      container.innerHTML = html;
    } else {
      container.innerHTML = '';
    }
  } else {
    const longPct = (c.long_signal / 7) * 100;
    const shortPct = (c.short_signal / 7) * 100;
    const markerPos = (tsl / 7) * 100;
    f('long-bar').style.width = longPct + '%';
    f('short-bar').style.width = shortPct + '%';
    f('marker').style.left = markerPos + '%';

    const posEl = f('pos-section');
    let posHtml = '';
    xks.forEach(xk => {
      const pos = positions[xk];
      if (!pos || pos.quantity <= 0) return;
      const pnl = pos.gain_loss_pct_buy;
      const pnlClass = pnl >= 0 ? 'positive' : 'negative';
      const color = XK_COLORS[xk] || '#888';
      const maxDca = state.settings.max_dca_buys_per_24h || 1;
      const dca24 = (state.dca24h[xk] || {})[c.coin] || 0;
      const sellPrice = pos.trail_line > 0 ? fmtPrice(pos.trail_line) : '—';
      posHtml += `
        <div class="cc-xk-section">
          <div class="cc-pos-header">
            <span class="cc-xk-label" style="color:${color}">${xk}</span>
            <span class="cc-pos-value">${fmtUSD(pos.value_usd)}</span>
            <span class="cc-pos-pnl ${pnlClass}">${fmtPct(pnl)}</span>
          </div>
          <div class="cc-pos-grid">
            <div class="cc-pf"><span class="cc-pf-l">Qty</span><span class="cc-pf-v">${fmtQty(pos.quantity, c.coin)}</span></div>
            <div class="cc-pf"><span class="cc-pf-l">Avg Cost</span><span class="cc-pf-v">${fmtPrice(pos.avg_cost_basis)}</span></div>
            <div class="cc-pf"><span class="cc-pf-l">Bid / Ask</span><span class="cc-pf-v">${fmtPrice(pos.current_sell_price)} / ${fmtPrice(pos.current_buy_price)}</span></div>
            <div class="cc-pf"><span class="cc-pf-l">Sell Level</span><span class="cc-pf-v">${sellPrice}</span></div>
            <div class="cc-pf"><span class="cc-pf-l">DCA</span><span class="cc-pf-v">${pos.dca_triggered_stages > 0 ? '<span class="pos-dca-chip">stg ' + pos.dca_triggered_stages + '</span>' : '—'} ${pos.trail_active ? '<span class="pos-trail-active">TRAILING</span>' : ''} <span class="cc-dca24">${dca24}/${maxDca} 24h</span></span></div>
          </div>
        </div>`;
    });
    if (posHtml) {
      posEl.style.display = '';
      posEl.innerHTML = posHtml;
    } else {
      posEl.style.display = 'none';
      posEl.innerHTML = '';
    }
  }
}

function renderCoinGrid() {
  const grid = $('#signal-grid');
  const coins = state.coins;
  if (!coins.length) return;

  const mode = state.cardMode;
  const sorted = [...coins].sort((a, b) => a.coin.localeCompare(b.coin));
  const modeChanged = _lastCardMode !== mode;

  if (modeChanged) {
    _lastCardMode = mode;
    state._expandedCoin = null;
    grid.innerHTML = '';
    const creator = mode === 'simple' ? _createSimpleCard : _createDetailCard;
    sorted.forEach(c => grid.appendChild(creator(c.coin)));
  }

  const existingCards = {};
  $$('.coin-card', grid).forEach(c => { existingCards[c.dataset.coin] = c; });
  const expanded = mode === 'simple' ? state._expandedCoin : null;

  sorted.forEach(c => {
    let card = existingCards[c.coin];
    if (!card) {
      const isExpanded = expanded === c.coin;
      card = isExpanded ? _createDetailCard(c.coin) : (mode === 'simple' ? _createSimpleCard : _createDetailCard)(c.coin);
      grid.appendChild(card);
    }
    _updateCard(card, c);
    delete existingCards[c.coin];
  });

  Object.values(existingCards).forEach(c => c.remove());
}

function updateSignals(signals) {
  if (!signals) return;
  state.coins.forEach(coin => {
    const sig = signals[coin.coin];
    if (sig) {
      coin.long_signal = sig.long;
      coin.short_signal = sig.short;
      coin.long_price_levels = sig.long_prices;
      coin.short_price_levels = sig.short_prices;
    }
  });
  renderCoinGrid();
  if (state.selectedCoin) updateChartPriceLines();
}

function mergePositionsIntoCoins() {
  state.coins.forEach(coin => {
    if (!coin.positions) coin.positions = {};
    state.exchangeList.forEach(xk => {
      const xkPos = (state.positions[xk] || {})[coin.coin];
      coin.positions[xk] = (xkPos && xkPos.quantity > 0) ? xkPos : null;
    });
  });
}

// ── Coin Selection & Chart ──

function selectCoin(coin) {
  const wasAccount = state.chartMode === 'account';
  state.selectedCoin = coin;
  state.chartMode = 'candle';

  if (state.cardMode === 'simple') {
    const grid = $('#signal-grid');
    const prev = state._expandedCoin;
    if (prev && prev !== coin) {
      const oldCard = grid.querySelector(`.coin-card[data-coin="${prev}"]`);
      if (oldCard) _replaceCard(grid, oldCard, prev, 'simple');
    }
    if (coin !== prev) {
      const curCard = grid.querySelector(`.coin-card[data-coin="${coin}"]`);
      if (curCard) _replaceCard(grid, curCard, coin, 'detail');
    }
    state._expandedCoin = coin;
  }

  $$('.coin-card').forEach(c => c.classList.toggle('active', c.dataset.coin === coin));
  const pb = $('#acct-block-portfolio');
  if (pb) pb.classList.remove('active');

  $('#chart-coin-label').textContent = coin;
  $('#tf-selector').style.display = '';
  $('#panel-chart').classList.add('mobile-active');

  if (wasAccount) rebuildTimeframeButtons();

  loadChart(coin, state.selectedTf);
  updateCoinPosition(coin);

  state.historyFilterCoin = coin;
  _updateHistoryTabLabel();
  if ($('#tab-history').classList.contains('active')) loadTradeHistory();
}

function _updateHistoryTabLabel() {
  const btn = $('[data-tab="history"]');
  if (!btn) return;
  btn.textContent = state.historyFilterCoin ? `History: ${state.historyFilterCoin}` : 'History';
}

function _replaceCard(grid, oldCard, coin, mode) {
  const newCard = mode === 'simple' ? _createSimpleCard(coin) : _createDetailCard(coin);
  const c = state.coins.find(x => x.coin === coin);
  if (c) _updateCard(newCard, c, mode);
  grid.replaceChild(newCard, oldCard);
}

function selectAccountChart(hours) {
  state.chartMode = 'account';
  state.accountRange = hours != null ? hours : state.accountRange;

  if (state.cardMode === 'simple' && state._expandedCoin) {
    const grid = $('#signal-grid');
    const oldCard = grid.querySelector(`.coin-card[data-coin="${state._expandedCoin}"]`);
    if (oldCard) _replaceCard(grid, oldCard, state._expandedCoin, 'simple');
    state._expandedCoin = null;
  }
  state.selectedCoin = null;

  $$('.coin-card').forEach(c => c.classList.remove('active'));
  const pb = $('#acct-block-portfolio');
  if (pb) pb.classList.add('active');

  $('#chart-coin-label').textContent = 'PORTFOLIO';
  $('#tf-selector').style.display = 'none';
  $('#coin-position').classList.add('hidden');
  $('#panel-chart').classList.add('mobile-active');

  loadAccountChart(state.accountRange);
}

async function loadChart(coin, tf) {
  const container = $('#chart-container');

  if (state.chartRefreshTimer) {
    clearInterval(state.chartRefreshTimer);
    state.chartRefreshTimer = null;
  }
  if (state.chart) {
    state.chart.remove();
    state.chart = null;
    state.candleSeries = null;
    state.priceLines = [];
    state._midPriceLine = null;
    state._currentBar = null;
  }

  state.chart = LightweightCharts.createChart(container, {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: {
      background: {type: 'solid', color: CHART_COLORS.bg},
      textColor: CHART_COLORS.text,
      fontFamily: "'Azeret Mono', monospace",
      fontSize: 10,
    },
    grid: {
      vertLines: {color: CHART_COLORS.grid},
      horzLines: {color: CHART_COLORS.grid},
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: {color: '#3A3A60', style: 2, width: 1},
      horzLine: {color: '#3A3A60', style: 2, width: 1},
    },
    timeScale: {
      borderColor: CHART_COLORS.border,
      timeVisible: true,
      secondsVisible: false,
    },
    rightPriceScale: {
      borderColor: CHART_COLORS.border,
    },
  });

  state.candleSeries = state.chart.addCandlestickSeries({
    upColor: CHART_COLORS.up,
    downColor: CHART_COLORS.down,
    borderUpColor: CHART_COLORS.up,
    borderDownColor: CHART_COLORS.down,
    wickUpColor: CHART_COLORS.up,
    wickDownColor: CHART_COLORS.down,
  });

  try {
    const data = await api(`candles/${coin}?timeframe=${tf}&limit=300`);
    if (data.candles && data.candles.length > 0) {
      state.candleSeries.setData(data.candles);
      state.chart.timeScale().fitContent();
      const last = data.candles[data.candles.length - 1];
      state._currentBar = { ...last };
    }
  } catch (e) {
    console.error('Failed to load candles:', e);
  }

  updateChartPriceLines();
  updateMidPriceLine();
  await updateChartTradeMarkers(coin);

  const refreshMs = TF_REFRESH_MS[tf] || 60_000;
  state.chartRefreshTimer = setInterval(async () => {
    if (!state.candleSeries || state.selectedCoin !== coin || state.selectedTf !== tf) return;
    try {
      const fresh = await api(`candles/${coin}?timeframe=${tf}&limit=2`);
      if (fresh.candles && fresh.candles.length) {
        fresh.candles.forEach(c => state.candleSeries.update(c));
        const last = fresh.candles[fresh.candles.length - 1];
        state._currentBar = { ...last };
      }
      updateChartPriceLines();
      updateChartTradeMarkers(coin);
    } catch {}
  }, refreshMs);

  const resizeObserver = new ResizeObserver(() => {
    if (state.chart) {
      state.chart.applyOptions({
        width: container.clientWidth,
        height: container.clientHeight,
      });
    }
  });
  resizeObserver.observe(container);
}

async function loadAccountChart(hours) {
  const container = $('#chart-container');

  if (state.chartRefreshTimer) {
    clearInterval(state.chartRefreshTimer);
    state.chartRefreshTimer = null;
  }
  if (state.chart) {
    state.chart.remove();
    state.chart = null;
    state.candleSeries = null;
    state.acctSeries = {};
    state.priceLines = [];
  }

  state.chart = LightweightCharts.createChart(container, {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: {
      background: {type: 'solid', color: CHART_COLORS.bg},
      textColor: CHART_COLORS.text,
      fontFamily: "'Azeret Mono', monospace",
      fontSize: 10,
    },
    grid: {
      vertLines: {color: CHART_COLORS.grid},
      horzLines: {color: CHART_COLORS.grid},
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: {color: '#3A3A60', style: 2, width: 1},
      horzLine: {color: '#3A3A60', style: 2, width: 1},
    },
    timeScale: {
      borderColor: CHART_COLORS.border,
      timeVisible: true,
      secondsVisible: false,
    },
    rightPriceScale: {
      borderColor: CHART_COLORS.border,
    },
    localization: {
      priceFormatter: v => '$' + v.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}),
    },
  });

  state.exchangeList.forEach(xk => {
    state.acctSeries[xk] = state.chart.addLineSeries({
      color: XK_COLORS[xk] || '#8888A8',
      lineWidth: 2,
      title: xk,
      priceFormat: {type: 'price', precision: 2, minMove: 0.01},
    });
  });

  await _applyAccountData(hours);

  state.chart.timeScale().fitContent();

  const tfContainer = $('#tf-selector');
  tfContainer.style.display = '';
  tfContainer.innerHTML = '';
  ACCT_RANGES.forEach(r => {
    const btn = document.createElement('button');
    btn.className = 'tf-btn' + (r.hours === hours ? ' active' : '');
    btn.textContent = r.label;
    btn.addEventListener('click', () => selectAccountChart(r.hours));
    tfContainer.appendChild(btn);
  });

  state.chartRefreshTimer = setInterval(async () => {
    if (state.chartMode !== 'account') return;
    await _applyAccountData(state.accountRange);
  }, 30_000);

  const resizeObserver = new ResizeObserver(() => {
    if (state.chart) {
      state.chart.applyOptions({width: container.clientWidth, height: container.clientHeight});
    }
  });
  resizeObserver.observe(container);
}

async function _applyAccountData(hours) {
  try {
    const qs = hours > 0 ? `?hours=${hours}` : '';
    const data = await api('account-history' + qs);
    const histByXk = data.history || {};
    state.exchangeList.forEach(xk => {
      const series = state.acctSeries[xk];
      const points = (histByXk[xk] || []).map(h => ({time: Math.floor(h.ts), value: h.total_account_value}));
      if (series && points.length > 0) series.setData(points);
    });
  } catch {}
}

async function updateChartTradeMarkers(coin) {
  if (!state.candleSeries || !coin) return;
  try {
    const data = await api(`coins/${coin}`);
    const allTrades = [];
    const tradesByXk = data.trades || {};
    state.exchangeList.forEach(xk => {
      (tradesByXk[xk] || []).forEach(t => allTrades.push({...t, _xk: xk}));
    });

    if (allTrades.length === 0) { state.candleSeries.setMarkers([]); return; }

    const markers = allTrades.map(t => {
      const side = (t.side || '').toLowerCase();
      const tag = (t.tag || '').toUpperCase();
      const xkLabel = XK_LABELS[t._xk] || t._xk[0].toUpperCase();
      let label, color, shape, position;

      if (side === 'buy') {
        label = tag === 'DCA' ? `${xkLabel}:DCA` : `${xkLabel}:BUY`;
        color = tag === 'DCA' ? '#A855F7' : '#FF4466';
        shape = 'arrowUp';
        position = 'belowBar';
      } else {
        label = `${xkLabel}:SELL`;
        color = '#00CC66';
        shape = 'arrowDown';
        position = 'aboveBar';
      }

      return { time: Math.floor(t.ts), position, color, shape, text: label };
    });

    markers.sort((a, b) => a.time - b.time);
    state.candleSeries.setMarkers(markers);
  } catch {}
}

function updateChartPriceLines() {
  if (!state.candleSeries || !state.selectedCoin) return;

  state.priceLines.forEach(pl => {
    try { state.candleSeries.removePriceLine(pl); } catch {}
  });
  state.priceLines = [];

  const coinData = state.coins.find(c => c.coin === state.selectedCoin);
  if (!coinData) return;

  const longPrices = coinData.long_price_levels || [];
  const shortPrices = coinData.short_price_levels || [];

  longPrices.forEach((price, i) => {
    if (!price) return;
    const pl = state.candleSeries.createPriceLine({
      price: price,
      color: `rgba(0, 212, 255, ${0.3 + (i * 0.08)})`,
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: `N${i + 1}`,
    });
    state.priceLines.push(pl);
  });

  shortPrices.forEach((price, i) => {
    if (!price) return;
    const pl = state.candleSeries.createPriceLine({
      price: price,
      color: `rgba(255, 68, 102, ${0.2 + (i * 0.06)})`,
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: false,
    });
    state.priceLines.push(pl);
  });

  state.exchangeList.forEach(xk => {
    const pos = (state.positions[xk] || {})[state.selectedCoin];
    if (!pos || pos.quantity <= 0) return;
    const color = XK_COLORS[xk] || '#888';
    const label = XK_LABELS[xk] || xk[0].toUpperCase();

    if (pos.avg_cost_basis) {
      const pl = state.candleSeries.createPriceLine({
        price: pos.avg_cost_basis, color, lineWidth: 1, lineStyle: 0,
        axisLabelVisible: true, title: `${label}:AVG`,
      });
      state.priceLines.push(pl);
    }
    if (pos.trail_line && pos.trail_line > 0) {
      const pl = state.candleSeries.createPriceLine({
        price: pos.trail_line, color: '#00CC66', lineWidth: 2, lineStyle: 0,
        axisLabelVisible: true, title: `${label}:SELL`,
      });
      state.priceLines.push(pl);
    }
    if (pos.dca_line_price) {
      const pl = state.candleSeries.createPriceLine({
        price: pos.dca_line_price, color: '#A855F7', lineWidth: 1, lineStyle: 2,
        axisLabelVisible: true, title: `${label}:DCA`,
      });
      state.priceLines.push(pl);
    }
  });
}

function updateMidPriceLine() {
  if (!state.candleSeries || !state.selectedCoin || state.chartMode !== 'candle') return;
  const coinData = state.coins.find(c => c.coin === state.selectedCoin);
  if (!coinData) return;
  const mid = coinData.mid_price || 0;
  if (!mid) return;

  if (state._midPriceLine) {
    try { state.candleSeries.removePriceLine(state._midPriceLine); } catch {}
    state._midPriceLine = null;
  }
  state._midPriceLine = state.candleSeries.createPriceLine({
    price: mid,
    color: '#00CC66',
    lineWidth: 1,
    lineStyle: 0,
    axisLabelVisible: true,
    title: 'MID',
  });

  const tfSec = TF_SECONDS[state.selectedTf] || 3600;
  const now = Math.floor(Date.now() / 1000);
  const barTime = Math.floor(now / tfSec) * tfSec;

  const cur = state._currentBar;
  if (cur && cur.time === barTime) {
    cur.close = mid;
    cur.high = Math.max(cur.high, mid);
    cur.low = Math.min(cur.low, mid);
  } else {
    state._currentBar = { time: barTime, open: mid, high: mid, low: mid, close: mid };
  }
  state.candleSeries.update(state._currentBar);
}

function updateCoinPosition(coin) {
  const container = $('#coin-position');
  const xks = state.exchangeList;
  const anyPos = xks.some(xk => {
    const pos = (state.positions[xk] || {})[coin];
    return pos && pos.quantity > 0;
  });

  if (!anyPos) {
    container.classList.add('hidden');
    return;
  }

  container.classList.remove('hidden');
  let html = '';
  xks.forEach(xk => {
    const pos = (state.positions[xk] || {})[coin];
    if (!pos || pos.quantity <= 0) return;
    const pnl = pos.gain_loss_pct_buy;
    const pnlClass = pnl >= 0 ? 'positive' : 'negative';
    const color = XK_COLORS[xk] || '#888';
    html += `
      <div class="pos-stat-group">
        <span class="pos-stat-xk" style="color:${color}">${xk}</span>
        <div class="pos-stat"><span class="pos-stat-label">Value</span><span class="pos-stat-value">${fmtUSD(pos.value_usd)}</span></div>
        <div class="pos-stat"><span class="pos-stat-label">P&L</span><span class="pos-stat-value ${pnlClass}">${fmtPct(pnl)}</span></div>
        <div class="pos-stat"><span class="pos-stat-label">Avg Cost</span><span class="pos-stat-value">${fmtPrice(pos.avg_cost_basis)}</span></div>
      </div>`;
  });
  container.innerHTML = html;
}

// ── Trade History Tab ──

async function loadTradeHistory() {
  const data = await api('trades?limit=200');
  if (!data.trades) return;

  const container = $('#history-list');
  let allTrades = [];
  const tradesByXk = data.trades;
  state.exchangeList.forEach(xk => {
    (tradesByXk[xk] || []).forEach(t => allTrades.push({...t, _xk: xk}));
  });

  allTrades.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  const filter = state.historyFilterCoin;
  if (filter) {
    allTrades = allTrades.filter(t => (t.symbol || '').startsWith(filter + '_'));
  }

  if (allTrades.length === 0) {
    container.innerHTML = '<div class="empty-state">No trade history</div>';
    return;
  }

  container.innerHTML = allTrades.map(t => {
    const coin = (t.symbol || '').replace('_USD', '');
    const tagClass = t.tag || '';
    const tagHtml = t.tag ? `<span class="hist-tag ${tagClass}">${t.tag}</span>` : '';
    const isSell = t.side === 'sell';
    const hasPnl = isSell && t.pnl_pct != null;
    const pnlClass = hasPnl ? (t.pnl_pct >= 0 ? 'positive' : 'negative') : '';
    const xkColor = XK_COLORS[t._xk] || '#888';
    const xkBadge = `<span class="hist-xk" style="color:${xkColor}">${XK_LABELS[t._xk] || t._xk[0].toUpperCase()}</span>`;

    if (isSell && hasPnl) {
      return `
        <div class="hist-row hist-row-sell" data-coin="${coin}">
          <span class="hist-time">${fmtDateTime(t.ts)}</span>
          ${xkBadge}
          <span class="hist-side sell">sell</span>
          <span>${coin} ${fmtQty(t.qty, coin)} @ ${fmtPrice(t.price)} ${tagHtml}</span>
          <span class="hist-amount">${fmtUSD(t.notional_usd)}</span>
          <span class="hist-sell-detail">
            <span class="hist-pnl ${pnlClass}">${fmtPct(t.pnl_pct)}</span>
            ${t.realized_profit_usd != null ? `<span class="hist-pnl ${pnlClass}">${fmtUSD(t.realized_profit_usd)}</span>` : ''}
          </span>
        </div>
      `;
    }

    return `
      <div class="hist-row" data-coin="${coin}">
        <span class="hist-time">${fmtDateTime(t.ts)}</span>
        ${xkBadge}
        <span class="hist-side ${t.side}">${t.side}</span>
        <span>${coin} ${fmtQty(t.qty, coin)} @ ${fmtPrice(t.price)} ${tagHtml}</span>
        <span class="hist-amount">${fmtUSD(t.notional_usd)}</span>
      </div>
    `;
  }).join('');

  $$('.hist-row[data-coin]', container).forEach(row => {
    row.addEventListener('click', () => selectCoin(row.dataset.coin));
  });
}

// ── Compare Tab ──

function cmpVal(v) {
  if (v == null || isNaN(v)) return '—';
  return Number(v).toFixed(2);
}
function cmpPct(v) {
  if (v == null || isNaN(v)) return '—';
  const n = Number(v);
  return (n >= 0 ? '+' : '') + n.toFixed(2);
}

async function loadCompare() {
  const container = $('#compare-content');
  try {
    const data = await api('comparison');
    if (!data.coins) return;

    const xks = data.exchanges || state.exchangeList;
    const multi = xks.length >= 2;
    const c0 = XK_COLORS[xks[0]] || '#888';
    const c1 = multi ? (XK_COLORS[xks[1]] || '#888') : c0;

    let html = '<table class="compare-table"><thead>';
    html += '<tr class="compare-hdr-top"><th rowspan="2">Coin</th>';
    html += '<th colspan="2">Value $</th><th colspan="2">P&L %</th>';
    if (multi) html += '<th colspan="2">Δ</th><th rowspan="2">Fees $</th>';
    html += '</tr><tr class="compare-hdr-sub">';
    html += `<th style="color:${c0}">${xks[0]}</th><th style="color:${c1}">${multi ? xks[1] : ''}</th>`;
    html += `<th style="color:${c0}">${xks[0]}</th><th style="color:${c1}">${multi ? xks[1] : ''}</th>`;
    if (multi) html += '<th>$</th><th>%</th>';
    html += '</tr></thead><tbody>';

    data.coins.forEach(row => {
      const hasPos = xks.some(xk => (row[xk]?.value_usd || 0) > 0) || row.coin === 'USDT';
      const dimClass = hasPos ? '' : ' compare-dim';
      html += `<tr class="${dimClass}"><td class="compare-coin">${row.coin}</td>`;
      xks.forEach(xk => {
        const d = row[xk] || {};
        html += `<td>${cmpVal(d.value_usd)}</td>`;
      });
      xks.forEach(xk => {
        const d = row[xk] || {};
        const pnlClass = hasPos ? ((d.pnl_pct || 0) >= 0 ? 'positive' : 'negative') : '';
        html += `<td class="${pnlClass}">${cmpPct(d.pnl_pct)}</td>`;
      });
      if (multi) {
        const d0 = row[xks[0]] || {};
        const d1 = row[xks[1]] || {};
        const dVal = (d1.value_usd || 0) - (d0.value_usd || 0);
        const dvClass = hasPos ? (dVal >= 0 ? 'positive' : 'negative') : '';
        const dPnl = (d1.pnl_pct || 0) - (d0.pnl_pct || 0);
        const dpClass = hasPos ? (dPnl >= 0 ? 'positive' : 'negative') : '';
        html += `<td class="${dvClass}">${cmpVal(dVal)}</td>`;
        html += `<td class="${dpClass}">${cmpPct(dPnl)}</td>`;
        html += `<td>${cmpVal(d1.total_fees)}</td>`;
      }
      html += '</tr>';
    });

    if (data.totals && multi) {
      const t0 = data.totals[xks[0]] || {};
      const t1 = data.totals[xks[1]] || {};
      html += `<tr class="compare-totals"><td>TOTAL</td>`;
      html += '<td>—</td><td>—</td>';
      xks.forEach(xk => {
        const t = data.totals[xk] || {};
        const rClass = (t.realized_profit || 0) >= 0 ? 'positive' : 'negative';
        html += `<td class="${rClass}">${cmpVal(t.realized_profit)}</td>`;
      });
      const delta = (t1.realized_profit || 0) - (t0.realized_profit || 0);
      const dClass = delta >= 0 ? 'positive' : 'negative';
      html += '<td>—</td>';
      html += `<td class="${dClass}">${cmpVal(delta)}</td>`;
      html += `<td>${cmpVal(t1.total_fees)}</td>`;
      html += '</tr>';
    }

    html += '</tbody></table>';
    container.innerHTML = html;
  } catch (e) {
    container.innerHTML = '<div class="empty-state">Failed to load comparison</div>';
  }
}

// ── LTH Tab ──

function renderLTH() {
  const container = $('#lth-list');
  const primaryXk = state.exchangeList[0] || 'control';
  const positions = state.positions[primaryXk] || {};
  const lthCoins = Object.entries(positions).filter(([_, p]) => p.lth_reserved_qty > 0);

  if (lthCoins.length === 0) {
    container.innerHTML = '<div class="empty-state">No long-term holdings</div>';
    return;
  }

  lthCoins.sort((a, b) => {
    const va = a[1].lth_reserved_qty * (a[1].current_buy_price || 0);
    const vb = b[1].lth_reserved_qty * (b[1].current_buy_price || 0);
    return vb - va;
  });

  container.innerHTML = lthCoins.map(([coin, p]) => {
    const value = p.lth_reserved_qty * (p.current_buy_price || 0);
    return `
      <div class="pos-card">
        <div class="pos-card-header">
          <span class="pos-coin">${coin}</span>
          <span class="pos-pnl" style="color: var(--gold)">${fmtUSD(value)}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">Quantity</span>
          <span class="pos-field-value">${fmtQty(p.lth_reserved_qty, coin)}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">Price</span>
          <span class="pos-field-value">${fmtPrice(p.current_buy_price)}</span>
        </div>
      </div>
    `;
  }).join('');
}

// ── Training Tab ──

async function loadAndRenderTraining() {
  try {
    const data = await api('coins');
    if (data.coins) renderTraining(data.coins);
  } catch (e) {
    console.error('loadAndRenderTraining failed:', e);
  }
}

function renderTraining(coins) {
  const container = $('#training-list');
  container.querySelectorAll('.train-log-panel').forEach(el => {
    if (el.dataset.timer) clearInterval(Number(el.dataset.timer));
  });
  if (!coins || coins.length === 0) {
    container.innerHTML = '<div class="empty-state">No coins configured</div>';
    return;
  }

  const sorted = [...coins].sort((a, b) => {
    const aFail = (a.training_state === 'FAILED') ? 0 : 1;
    const bFail = (b.training_state === 'FAILED') ? 0 : 1;
    if (aFail !== bFail) return aFail - bFail;
    return a.coin.localeCompare(b.coin);
  });

  container.innerHTML = sorted.map(c => {
    const tState = c.training_running ? 'TRAINING' : (c.training_state || 'UNKNOWN');
    const trained = !c.training_running && c.is_trained;
    const lastTs = c.last_trained_ts;
    const ageText = lastTs > 0 ? fmtDate(lastTs) : 'Never';
    const fail = c.training_running ? null : c.training_failure;

    let failHtml = '';
    if (fail && fail.exception_type) {
      const esc = s => String(s).replace(/</g, '&lt;').replace(/>/g, '&gt;');
      const utc = ts => ts ? new Date(ts * 1000).toISOString().replace('T', ' ').replace(/\.\d+Z/, ' UTC') : '—';
      const tsKeys = new Set(['start_time', 'end_time']);
      const fmtVal = (k, v) => tsKeys.has(k) && /^\d{9,}$/.test(String(v)) ? utc(Number(v)) : esc(v);
      const stateRows = fail.trainer_state ? Object.entries(fail.trainer_state)
        .filter(([, v]) => !Array.isArray(v))
        .map(([k, v]) => `<tr><td class="fail-key">${esc(k)}</td><td>${fmtVal(k, v)}</td></tr>`)
        .join('') : '';

      failHtml = `
        <div class="train-failure" onclick="this.classList.toggle('open')">
          <div class="train-failure-summary">${esc(fail.exception_type)}: ${esc(fail.exception_message || '')}</div>
          <div class="train-failure-detail">
            <table class="fail-table">
              <tr><td class="fail-key">Failed at</td><td>${utc(fail.timestamp)}</td></tr>
              <tr><td class="fail-key">Started at</td><td>${utc(fail.started_at)}</td></tr>
              ${stateRows}
            </table>
            <div class="fail-tb-label">Traceback</div>
            <pre class="train-failure-tb">${esc(fail.traceback || '')}</pre>
          </div>
        </div>`;
    }

    return `
      <div class="train-row" data-train-coin="${c.coin}">
        <span class="train-coin">${c.coin}</span>
        <div>
          <span style="font-family: var(--font-mono); font-size: 10px; color: var(--text-muted)">
            ${ageText}
          </span>
        </div>
        <div class="train-actions">
          <span class="train-status ${tState}">${trained ? 'TRAINED' : tState}</span>
          <button class="btn btn-small btn-secondary" onclick="toggleTrainerLog('${c.coin}')">Log</button>
          <button class="btn btn-small btn-secondary" onclick="trainCoin('${c.coin}')">Train</button>
        </div>
      </div>${failHtml}
    `;
  }).join('');
}

window.trainCoin = async function(coin) {
  await apiPost(`train/${coin}`);
  setTimeout(loadAndRenderTraining, 1000);
};

window.toggleTrainerLog = function(coin) {
  const id = `train-log-${coin}`;
  const existing = document.getElementById(id);
  if (existing) {
    const timer = existing.dataset.timer;
    if (timer) clearInterval(Number(timer));
    existing.remove();
    return;
  }

  const row = document.querySelector(`[data-train-coin="${coin}"]`);
  if (!row) return;

  const logEl = document.createElement('div');
  logEl.id = id;
  logEl.className = 'train-log-panel';
  logEl.innerHTML = '<pre class="train-log-output">Loading...</pre>';
  row.after(logEl);

  const refresh = async () => {
    try {
      const data = await api(`logs/trainer-${coin.toLowerCase()}`);
      const pre = logEl.querySelector('pre');
      if (pre) {
        pre.textContent = (data.lines || []).join('\n') || '(no output)';
        pre.scrollTop = pre.scrollHeight;
      }
    } catch {}
  };

  refresh();
  const timer = setInterval(refresh, 3000);
  logEl.dataset.timer = timer;
};

// ── Settings Tab ──

async function loadAndRenderSettings() {
  try {
    const s = await api('settings');
    state.settings = s;
    renderSettings(s);
  } catch (e) {
    console.error('loadAndRenderSettings failed:', e);
  }
}

function renderSettings(s) {
  if (!s) return;
  const form = $('#settings-form');

  form.innerHTML = `
    <div class="settings-group">
      <div class="settings-group-title">General</div>
      <div class="settings-field">
        <label>Coins (comma-separated)</label>
        <input type="text" id="set-coins" value="${(s.coins || []).join(', ')}">
      </div>
      <div class="settings-field">
        <label>Exchanges (comma-separated)</label>
        <input type="text" id="set-exchanges" value="${(s.exchanges || ['control']).join(', ')}">
      </div>
      <div class="settings-field">
        <label>Live Price Source</label>
        <select id="set-price-source">
          <option value="kraken" ${(s.live_price_source || 'kraken') === 'kraken' ? 'selected' : ''}>Kraken</option>
          <option value="kucoin" ${s.live_price_source === 'kucoin' ? 'selected' : ''}>KuCoin</option>
        </select>
      </div>
    </div>
    <div class="settings-group">
      <div class="settings-group-title">Trading</div>
      <div class="settings-field">
        <label>Trade Start Level (1-7)</label>
        <input type="number" id="set-tsl" value="${s.trade_start_level || 1}" min="1" max="7">
      </div>
      <div class="settings-field">
        <label>Start Allocation %</label>
        <input type="number" id="set-alloc" value="${s.start_allocation_pct || 0.5}" step="0.1">
      </div>
      <div class="settings-field">
        <label>DCA Levels (% list)</label>
        <input type="text" id="set-dca" value="${(s.dca_levels || []).join(', ')}">
      </div>
      <div class="settings-field">
        <label>DCA Multiplier</label>
        <input type="number" id="set-dca-mult" value="${s.dca_multiplier || 2}" step="0.5">
      </div>
      <div class="settings-field">
        <label>Max DCA Buys / 24h</label>
        <input type="number" id="set-max-dca" value="${s.max_dca_buys_per_24h || 1}" min="1">
      </div>
    </div>
    <div class="settings-group">
      <div class="settings-group-title">Trailing Profit</div>
      <div class="settings-field">
        <label>PM Start % (no DCA)</label>
        <input type="number" id="set-pm-no" value="${s.pm_start_pct_no_dca || 3}" step="0.5">
      </div>
      <div class="settings-field">
        <label>PM Start % (with DCA)</label>
        <input type="number" id="set-pm-dca" value="${s.pm_start_pct_with_dca || 3}" step="0.5">
      </div>
      <div class="settings-field">
        <label title="Once in profit, the sell line trails the peak price by this %. E.g. 0.5% gap: if peak is $100, sell line sits at $99.50. The line only ratchets up, never down — locking in gains while allowing room to fluctuate.">Trailing Gap %</label>
        <input type="number" id="set-gap" value="${s.trailing_gap_pct || 0.1}" step="0.05">
      </div>
    </div>
    <div class="settings-group">
      <div class="settings-group-title">Long-Term Holdings</div>
      <div class="settings-field">
        <label>LTH Coins</label>
        <input type="text" id="set-lth" value="${(s.long_term_holdings || []).join(', ')}">
      </div>
      <div class="settings-field">
        <label>LTH Profit Allocation %</label>
        <input type="number" id="set-lth-pct" value="${s.lth_profit_alloc_pct || 50}" step="5">
      </div>
    </div>
    <div class="settings-group">
      <div class="settings-group-title">Control Exchange</div>
      <div class="settings-field">
        <label title="0 = auto-sync from Kraken balance on first run">Starting USD (0 = sync from Kraken)</label>
        <input type="number" id="set-ctrl-usd" value="${s.control_starting_usd || 0}" step="1000">
      </div>
    </div>
    <div class="settings-group">
      <div class="settings-group-title">Startup</div>
      <div class="settings-field settings-field-toggle">
        <label>Auto-start scripts on launch</label>
        <input type="checkbox" id="set-autostart" ${s.auto_start_scripts ? 'checked' : ''}>
      </div>
    </div>
    <div class="settings-save">
      <button class="btn btn-primary" id="btn-save-settings">Save Settings</button>
    </div>
  `;

  $('#btn-save-settings').addEventListener('click', saveSettings);
}

async function saveSettings() {
  const current = state.settings;
  const updated = {...current};

  updated.coins = $('#set-coins').value.split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
  updated.exchanges = $('#set-exchanges').value.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
  updated.exchange = updated.exchanges[0] || 'control';
  updated.live_price_source = $('#set-price-source').value;
  updated.trade_start_level = parseInt($('#set-tsl').value) || 1;
  updated.start_allocation_pct = parseFloat($('#set-alloc').value) || 0.5;
  updated.dca_levels = $('#set-dca').value.split(',').map(s => parseFloat(s.trim())).filter(v => !isNaN(v));
  updated.dca_multiplier = parseFloat($('#set-dca-mult').value) || 2;
  updated.max_dca_buys_per_24h = parseInt($('#set-max-dca').value) || 1;
  updated.pm_start_pct_no_dca = parseFloat($('#set-pm-no').value) || 3;
  updated.pm_start_pct_with_dca = parseFloat($('#set-pm-dca').value) || 3;
  updated.trailing_gap_pct = parseFloat($('#set-gap').value) || 0.1;
  updated.long_term_holdings = $('#set-lth').value.split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
  updated.lth_profit_alloc_pct = parseFloat($('#set-lth-pct').value) || 50;
  updated.control_starting_usd = parseFloat($('#set-ctrl-usd').value) || 0;
  updated.auto_start_scripts = $('#set-autostart').checked;

  const result = await apiPut('settings', updated);
  if (result.ok) {
    const btn = $('#btn-save-settings');
    btn.textContent = 'Saved!';
    setTimeout(() => { btn.textContent = 'Save Settings'; }, 2000);
  }
}

// ── Logs Tab ──

function populateLogSourceDropdown() {
  const select = $('#log-source');
  if (!select) return;
  const currentVal = select.value;
  select.innerHTML = '<option value="neural">Neural Runner</option>';
  state.exchangeList.forEach(xk => {
    select.innerHTML += `<option value="trader-${xk}">Trader: ${xk}</option>`;
  });
  if (currentVal) select.value = currentVal;
}

async function refreshLogs() {
  const source = $('#log-source').value;
  const data = await api(`logs/${source}`);
  const output = $('#log-output');
  output.textContent = (data.lines || []).join('\n');
  output.scrollTop = output.scrollHeight;
}

// ── Tabs ──

function setupTabs() {
  $$('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      $$('.tab-btn').forEach(b => b.classList.toggle('active', b === btn));
      $$('.tab-content').forEach(c => c.classList.toggle('active', c.id === 'tab-' + tab));

      if (state.logRefreshTimer) { clearInterval(state.logRefreshTimer); state.logRefreshTimer = null; }
      if (tab === 'history') { state.historyFilterCoin = null; _updateHistoryTabLabel(); loadTradeHistory(); }
      if (tab === 'compare') loadCompare();
      if (tab === 'lth') renderLTH();
      if (tab === 'training') loadAndRenderTraining();
      if (tab === 'settings') loadAndRenderSettings();
      if (tab === 'logs') {
        refreshLogs();
        state.logRefreshTimer = setInterval(refreshLogs, 3000);
      }
    });
  });
}

// ── Mobile Nav ──

function setupMobileNav() {
  const panels = {
    signals: '#panel-signals',
    chart: '#panel-chart',
    trades: '#panel-trades',
    settings: '#panel-trades',
  };

  $$('.mnav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const view = btn.dataset.view;
      $$('.mnav-btn').forEach(b => b.classList.toggle('active', b === btn));
      $$('.panel').forEach(p => p.classList.remove('mobile-active'));

      const panelSel = panels[view];
      if (panelSel) $(panelSel).classList.add('mobile-active');

      if (view === 'settings') {
        $$('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === 'settings'));
        $$('.tab-content').forEach(c => c.classList.toggle('active', c.id === 'tab-settings'));
      }
    });
  });

  $('#panel-signals').classList.add('mobile-active');
}

// ── Buttons ──

function setupButtons() {
  $('#btn-start-all').addEventListener('click', async () => {
    $('#btn-start-all').disabled = true;
    await apiPost('start-all');
    setTimeout(refreshAll, 2000);
    setTimeout(() => { $('#btn-start-all').disabled = false; }, 5000);
  });

  $('#btn-stop-all').addEventListener('click', async () => {
    await apiPost('stop-all');
    setTimeout(refreshAll, 1000);
  });

  $('#btn-train-all').addEventListener('click', async () => {
    $('#btn-train-all').disabled = true;
    await apiPost('train-all');
    setTimeout(refreshAll, 2000);
    setTimeout(() => { $('#btn-train-all').disabled = false; }, 5000);
  });

  $('#btn-close-all').addEventListener('click', async () => {
    if (!confirm('Close ALL positions on ALL exchanges?')) return;
    $('#btn-close-all').disabled = true;
    await apiPost('close-all');
    setTimeout(refreshAll, 2000);
    setTimeout(() => { $('#btn-close-all').disabled = false; }, 5000);
  });

  $('#btn-sync-control').addEventListener('click', async () => {
    $('#btn-sync-control').disabled = true;
    const res = await apiPost('sync-control');
    if (res && !res.ok) alert(res.error || 'Sync failed');
    setTimeout(refreshAll, 1000);
    setTimeout(() => { $('#btn-sync-control').disabled = false; }, 3000);
  });

  $('#btn-refresh-logs').addEventListener('click', refreshLogs);

  $('#acct-block-portfolio').addEventListener('click', () => selectAccountChart(0));
}

// ── Timeframes ──

function rebuildTimeframeButtons() {
  const container = $('#tf-selector');
  container.innerHTML = '';
  TF_LIST.forEach(tf => {
    const btn = document.createElement('button');
    btn.className = 'tf-btn' + (tf === state.selectedTf ? ' active' : '');
    btn.textContent = tf.replace('hour', 'H').replace('min', 'm').replace('day', 'D').replace('week', 'W');
    btn.addEventListener('click', () => {
      state.selectedTf = tf;
      $$('.tf-btn').forEach(b => b.classList.toggle('active', b === btn));
      if (state.selectedCoin) loadChart(state.selectedCoin, tf);
    });
    container.appendChild(btn);
  });
}

function setupTimeframes() { rebuildTimeframeButtons(); }

// ── Sashes (drag to resize panels) ──

function _initSash(sashEl, cssVar, storageKey, calcSize) {
  const app = $('#app');
  const stored = localStorage.getItem(storageKey);
  if (stored) app.style.setProperty(cssVar, stored + 'px');

  let dragging = false;

  sashEl.addEventListener('mousedown', (e) => {
    e.preventDefault();
    dragging = true;
    sashEl.classList.add('dragging');
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  });

  window.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    app.style.setProperty(cssVar, calcSize(e.clientX) + 'px');
  });

  window.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    sashEl.classList.remove('dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    const val = getComputedStyle(app).getPropertyValue(cssVar).trim();
    localStorage.setItem(storageKey, parseInt(val));
    if (state.chart) state.chart.applyOptions({ autoSize: true });
  });
}

function setupSash() {
  const sashLeft = $('#sash-left');
  const sashRight = $('#sash');

  if (sashLeft) _initSash(sashLeft, '--lh-width', 'pt-lh-width',
    x => Math.max(200, Math.min(600, x)));

  if (sashRight) _initSash(sashRight, '--rh-width', 'pt-rh-width',
    x => Math.max(280, Math.min(700, window.innerWidth - x)));
}

// ── View Toggle ──

function setupViewToggle() {
  const stored = localStorage.getItem('pt-card-mode');
  if (stored === 'detail' || stored === 'simple') state.cardMode = stored;

  $$('.vtog-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.mode === state.cardMode);
    btn.addEventListener('click', () => {
      state.cardMode = btn.dataset.mode;
      $$('.vtog-btn').forEach(b => b.classList.toggle('active', b === btn));
      localStorage.setItem('pt-card-mode', state.cardMode);
      renderCoinGrid();
    });
  });
}

// ── Boot ──
document.addEventListener('DOMContentLoaded', init);
