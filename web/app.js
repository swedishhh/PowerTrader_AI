/* ═══════════════════════════════════════════════════════════
   PowerTrader · Obsidian Command · Frontend
   ═══════════════════════════════════════════════════════════ */

const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];

const state = {
  coins: [],
  positions: {},
  tradeStartLevel: 1,
  selectedCoin: null,
  selectedTf: '1hour',
  neuralRunning: false,
  traderRunning: false,
  chart: null,
  candleSeries: null,
  priceLines: [],
  settings: {},
};

const TF_LIST = ['1min','5min','15min','30min','1hour','2hour','4hour','8hour','12hour','1day','1week'];
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
      updateTraderStatus(msg.data);
      break;
    case 'signals':
      updateSignals(msg.data);
      break;
    case 'pnl':
      updatePnl(msg.data);
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

  await refreshAll();
  connectWS();

  setInterval(refreshAll, 10000);
}

async function refreshAll() {
  try {
    const [statusData, coinsData] = await Promise.all([api('status'), api('coins')]);

    state.settings = statusData;
    state.tradeStartLevel = statusData.trade_start_level || statusData.coins?.trade_start_level || 1;

    updateSystemStatus(statusData.system);
    updateAccountDisplay(statusData.account, statusData.pnl);

    if (coinsData.coins) {
      state.coins = coinsData.coins;
      renderSignalGrid(coinsData.coins);
      updatePositionsFromCoins(coinsData.coins);
    }

    const settingsData = await api('settings');
    state.tradeStartLevel = settingsData.trade_start_level || 1;
    state.settings = settingsData;

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

  const pillNeural = $('#pill-neural');
  const pillTrader = $('#pill-trader');

  pillNeural.className = 'vital-pill ' + (sys.neural_running ? 'running' : 'stopped');
  pillTrader.className = 'vital-pill ' + (sys.trader_running ? 'running' : 'stopped');

  const btnStart = $('#btn-start-all');
  const btnStop = $('#btn-stop-all');
  if (sys.neural_running || sys.trader_running) {
    btnStart.style.display = 'none';
    btnStop.style.display = '';
  } else {
    btnStart.style.display = '';
    btnStop.style.display = 'none';
  }
}

// ── Account Display ──

function updateAccountDisplay(account, pnl) {
  if (account) {
    $('#acct-total').textContent = fmtUSD(account.total_account_value);
    $('#acct-power').textContent = fmtUSD(account.buying_power);
  }
  if (pnl) {
    const el = $('#acct-pnl');
    const v = pnl.total_realized_profit_usd || 0;
    el.textContent = (v >= 0 ? '+' : '') + fmtUSD(v);
    el.className = 'acct-value ' + (v >= 0 ? 'positive' : 'negative');
  }
}

function updatePnl(pnl) {
  if (!pnl) return;
  const el = $('#acct-pnl');
  const v = pnl.total_realized_profit_usd || 0;
  el.textContent = (v >= 0 ? '+' : '') + fmtUSD(v);
  el.className = 'acct-value ' + (v >= 0 ? 'positive' : 'negative');
}

// ── Trader Status (from WS) ──

function updateTraderStatus(data) {
  if (!data) return;
  if (data.account) updateAccountDisplay(data.account, null);
  if (data.positions) {
    state.positions = data.positions;
    renderPositions(data.positions);
    updateSignalPositionChips();
    if (state.selectedCoin) updateCoinPosition(state.selectedCoin);
  }
}

// ── Signal Grid ──

function renderSignalGrid(coins) {
  const grid = $('#signal-grid');
  const existingCards = {};
  $$('.signal-card', grid).forEach(c => { existingCards[c.dataset.coin] = c; });

  coins.forEach((coin, i) => {
    let card = existingCards[coin.coin];
    const isNew = !card;

    if (isNew) {
      card = document.createElement('div');
      card.className = 'signal-card';
      card.dataset.coin = coin.coin;
      card.innerHTML = `
        <div>
          <div class="signal-coin">${coin.coin}</div>
          <div class="signal-price" data-field="price"></div>
        </div>
        <div class="signal-bar-wrap">
          <div class="signal-bar-label"><span>LONG</span><span data-field="long-pct"></span></div>
          <div class="signal-bar">
            <div class="signal-bar-fill long" data-field="long-fill"></div>
            <div class="signal-bar-marker" data-field="marker"></div>
          </div>
          <div class="signal-bar-label"><span>SHORT</span><span data-field="short-pct"></span></div>
          <div class="signal-bar">
            <div class="signal-bar-fill short" data-field="short-fill"></div>
          </div>
        </div>
        <div class="signal-values">
          <div class="signal-val-long" data-field="long-val"></div>
          <div class="signal-val-short" data-field="short-val"></div>
          <div data-field="pos-chip"></div>
        </div>
      `;
      card.addEventListener('click', () => selectCoin(coin.coin));
      grid.appendChild(card);
    }

    if (state.selectedCoin === coin.coin) card.classList.add('active');
    else card.classList.remove('active');

    const longPct = (coin.long_signal / 7) * 100;
    const shortPct = (coin.short_signal / 7) * 100;

    card.querySelector('[data-field="long-fill"]').style.width = longPct + '%';
    card.querySelector('[data-field="short-fill"]').style.width = shortPct + '%';
    card.querySelector('[data-field="long-val"]').textContent = coin.long_signal;
    card.querySelector('[data-field="short-val"]').textContent = 'S:' + coin.short_signal;

    const tsl = state.tradeStartLevel || 1;
    const markerPos = (tsl / 7) * 100;
    const marker = card.querySelector('[data-field="marker"]');
    marker.style.left = markerPos + '%';

    const isTradeReady = coin.long_signal >= tsl && coin.short_signal === 0;
    card.classList.toggle('trade-ready', isTradeReady);

    const pos = coin.position;
    const priceEl = card.querySelector('[data-field="price"]');
    if (pos && pos.current_buy_price) {
      priceEl.textContent = fmtPrice(pos.current_buy_price);
    }

    const chipEl = card.querySelector('[data-field="pos-chip"]');
    if (pos && pos.quantity > 0) {
      const pnl = pos.gain_loss_pct_buy;
      chipEl.innerHTML = `<span class="signal-pos-chip in-trade">${fmtPct(pnl)}</span>`;
    } else {
      chipEl.innerHTML = '';
    }

    delete existingCards[coin.coin];
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
  renderSignalGrid(state.coins);
  if (state.selectedCoin) updateChartPriceLines();
}

function updateSignalPositionChips() {
  state.coins.forEach(coin => {
    const pos = state.positions[coin.coin];
    if (pos && pos.quantity > 0) {
      coin.position = pos;
    } else {
      coin.position = null;
    }
  });
}

// ── Coin Selection & Chart ──

function selectCoin(coin) {
  state.selectedCoin = coin;

  $$('.signal-card').forEach(c => c.classList.toggle('active', c.dataset.coin === coin));

  $('#chart-coin-label').textContent = coin;
  $('#panel-chart').classList.add('mobile-active');

  loadChart(coin, state.selectedTf);
  updateCoinPosition(coin);
}

async function loadChart(coin, tf) {
  const container = $('#chart-container');

  if (state.chart) {
    state.chart.remove();
    state.chart = null;
    state.candleSeries = null;
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
    }
  } catch (e) {
    console.error('Failed to load candles:', e);
  }

  updateChartPriceLines();

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

  const pos = state.positions[state.selectedCoin];
  if (pos && pos.quantity > 0) {
    if (pos.avg_cost_basis) {
      const pl = state.candleSeries.createPriceLine({
        price: pos.avg_cost_basis,
        color: '#F0B429',
        lineWidth: 1,
        lineStyle: 0,
        axisLabelVisible: true,
        title: 'AVG',
      });
      state.priceLines.push(pl);
    }
    if (pos.trail_active && pos.trail_line) {
      const pl = state.candleSeries.createPriceLine({
        price: pos.trail_line,
        color: '#00CC66',
        lineWidth: 2,
        lineStyle: 0,
        axisLabelVisible: true,
        title: 'SELL',
      });
      state.priceLines.push(pl);
    }
    if (pos.dca_line_price) {
      const pl = state.candleSeries.createPriceLine({
        price: pos.dca_line_price,
        color: '#A855F7',
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: 'DCA',
      });
      state.priceLines.push(pl);
    }
  }
}

function updateCoinPosition(coin) {
  const container = $('#coin-position');
  const pos = state.positions[coin];

  if (!pos || pos.quantity <= 0) {
    container.classList.add('hidden');
    return;
  }

  container.classList.remove('hidden');
  const pnl = pos.gain_loss_pct_buy;
  const pnlClass = pnl >= 0 ? 'positive' : 'negative';

  container.innerHTML = `
    <div class="pos-stat">
      <span class="pos-stat-label">Quantity</span>
      <span class="pos-stat-value">${fmtQty(pos.quantity, coin)}</span>
    </div>
    <div class="pos-stat">
      <span class="pos-stat-label">Value</span>
      <span class="pos-stat-value">${fmtUSD(pos.value_usd)}</span>
    </div>
    <div class="pos-stat">
      <span class="pos-stat-label">Avg Cost</span>
      <span class="pos-stat-value">${fmtPrice(pos.avg_cost_basis)}</span>
    </div>
    <div class="pos-stat">
      <span class="pos-stat-label">P&L</span>
      <span class="pos-stat-value ${pnlClass}">${fmtPct(pnl)}</span>
    </div>
    <div class="pos-stat">
      <span class="pos-stat-label">DCA Stage</span>
      <span class="pos-stat-value">${pos.dca_triggered_stages} ${pos.trail_active ? '<span class="pos-trail-active">⬡ TRAILING</span>' : ''}</span>
    </div>
    <div class="pos-stat">
      <span class="pos-stat-label">Next DCA</span>
      <span class="pos-stat-value">${pos.next_dca_display || '—'}</span>
    </div>
  `;
}

// ── Positions Tab ──

function renderPositions(positions) {
  const container = $('#positions-list');
  const active = Object.entries(positions).filter(([_, p]) => p.quantity > 0);

  if (active.length === 0) {
    container.innerHTML = '<div class="empty-state">No open positions</div>';
    return;
  }

  active.sort((a, b) => (b[1].value_usd || 0) - (a[1].value_usd || 0));

  container.innerHTML = active.map(([coin, p]) => {
    const pnl = p.gain_loss_pct_buy;
    const pnlClass = pnl >= 0 ? 'positive' : 'negative';
    return `
      <div class="pos-card" data-coin="${coin}">
        <div class="pos-card-header">
          <span class="pos-coin">${coin}</span>
          <span class="pos-pnl ${pnlClass}">${fmtPct(pnl)}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">Value</span>
          <span class="pos-field-value">${fmtUSD(p.value_usd)}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">Avg Cost</span>
          <span class="pos-field-value">${fmtPrice(p.avg_cost_basis)}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">Current</span>
          <span class="pos-field-value">${fmtPrice(p.current_buy_price)}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">DCA</span>
          <span class="pos-field-value">${p.dca_triggered_stages > 0 ? '<span class="pos-dca-chip">DCA ' + p.dca_triggered_stages + '</span>' : '—'} ${p.trail_active ? '<span class="pos-trail-active">TRAILING</span>' : ''}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">Quantity</span>
          <span class="pos-field-value">${fmtQty(p.quantity, coin)}</span>
        </div>
        <div class="pos-field">
          <span class="pos-field-label">Next DCA</span>
          <span class="pos-field-value">${p.next_dca_display || '—'}</span>
        </div>
      </div>
    `;
  }).join('');

  $$('.pos-card', container).forEach(card => {
    card.style.cursor = 'pointer';
    card.addEventListener('click', () => selectCoin(card.dataset.coin));
  });
}

// ── Trade History Tab ──

async function loadTradeHistory() {
  const data = await api('trades?limit=200');
  if (!data.trades) return;

  const container = $('#history-list');
  const trades = data.trades.reverse();

  if (trades.length === 0) {
    container.innerHTML = '<div class="empty-state">No trade history</div>';
    return;
  }

  container.innerHTML = trades.map(t => {
    const coin = (t.symbol || '').replace('_USD', '');
    const tagClass = t.tag || '';
    const tagHtml = t.tag ? `<span class="hist-tag ${tagClass}">${t.tag}</span>` : '';
    return `
      <div class="hist-row">
        <span class="hist-time">${fmtTime(t.ts)}</span>
        <span class="hist-side ${t.side}">${t.side}</span>
        <span>${coin} ${fmtQty(t.qty, coin)} ${tagHtml}</span>
        <span class="hist-amount">${fmtUSD(t.notional_usd)}</span>
      </div>
    `;
  }).join('');
}

// ── Training Tab ──

function renderTraining(coins) {
  const container = $('#training-list');
  if (!coins || coins.length === 0) {
    container.innerHTML = '<div class="empty-state">No coins configured</div>';
    return;
  }

  container.innerHTML = coins.map(c => {
    const tState = c.training_state || 'UNKNOWN';
    const trained = c.is_trained;
    const lastTs = c.last_trained_ts;
    const ageText = lastTs > 0 ? fmtDate(lastTs) : 'Never';

    return `
      <div class="train-row">
        <span class="train-coin">${c.coin}</span>
        <div>
          <span style="font-family: var(--font-mono); font-size: 10px; color: var(--text-muted)">
            ${ageText}
          </span>
        </div>
        <div class="train-actions">
          <span class="train-status ${tState}">${trained ? 'TRAINED' : tState}</span>
          <button class="btn btn-small btn-secondary" onclick="trainCoin('${c.coin}')">Train</button>
        </div>
      </div>
    `;
  }).join('');
}

window.trainCoin = async function(coin) {
  await apiPost(`train/${coin}`);
  setTimeout(refreshAll, 1000);
};

// ── Settings Tab ──

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
        <label>Exchange</label>
        <input type="text" id="set-exchange" value="${s.exchange || 'demo'}">
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
        <label>Trailing Gap %</label>
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
  updated.exchange = $('#set-exchange').value.trim().toLowerCase();
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

  const result = await apiPut('settings', updated);
  if (result.ok) {
    const btn = $('#btn-save-settings');
    btn.textContent = 'Saved!';
    setTimeout(() => { btn.textContent = 'Save Settings'; }, 2000);
  }
}

// ── Logs Tab ──

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

      if (tab === 'history') loadTradeHistory();
      if (tab === 'logs') refreshLogs();
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

  $('#btn-refresh-logs').addEventListener('click', refreshLogs);
}

// ── Timeframes ──

function setupTimeframes() {
  const container = $('#tf-selector');
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

// ── Boot ──
document.addEventListener('DOMContentLoaded', init);
