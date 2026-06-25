/**
 * ICT Gold Terminal — 24/7 Real-Time Signal Bot
 * Connects to TwelveData WebSocket → builds live candles → runs ICT signal engine
 * → sends Telegram alert instantly when B/A+/A++ fires
 *
 * Deploy free on Railway or Render (no server needed)
 * Env vars: TD_KEY, TG_TOKEN, TG_CHAT_ID
 */

const WebSocket = require('ws');

// ── CONFIG FROM ENV ──────────────────────────────────────────────────────────────
const TD_KEY     = process.env.TD_KEY     || '';
const TG_TOKEN   = process.env.TG_TOKEN   || '';
const TG_CHAT_ID = process.env.TG_CHAT_ID || '';

if (!TD_KEY || !TG_TOKEN || !TG_CHAT_ID) {
  console.error('Missing env vars: TD_KEY, TG_TOKEN, TG_CHAT_ID');
  process.exit(1);
}

// ── CANDLE AGGREGATOR ─────────────────────────────────────────────────────────────
// Builds OHLC candles from raw price ticks for multiple timeframes
const INTERVALS = { '5m': 5, '15m': 15, '1h': 60, '4h': 240 };
const MAX_CANDLES = 100;

const candles = { '5m': [], '15m': [], '1h': [], '4h': [] };
let currentCandle = { '5m': null, '15m': null, '1h': null, '4h': null };

function getIntervalStart(ts, minutes) {
  const ms = minutes * 60 * 1000;
  return Math.floor(ts / ms) * ms;
}

function processTick(price, ts) {
  for (const [tf, mins] of Object.entries(INTERVALS)) {
    const start = getIntervalStart(ts, mins);
    const cur   = currentCandle[tf];

    if (!cur || cur.t !== start) {
      // Close previous candle
      if (cur) {
        candles[tf].push({ ...cur });
        if (candles[tf].length > MAX_CANDLES) candles[tf].shift();
      }
      // Open new candle
      currentCandle[tf] = { t: start, o: price, h: price, l: price, c: price };
    } else {
      cur.h = Math.max(cur.h, price);
      cur.l = Math.min(cur.l, price);
      cur.c = price;
    }
  }
}

// ── ICT SIGNAL ENGINE ─────────────────────────────────────────────────────────────
function nowET() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
}

function computeSignal() {
  const h4 = [...candles['4h'], currentCandle['4h']].filter(Boolean);
  const h1 = [...candles['1h'], currentCandle['1h']].filter(Boolean);

  if (h4.length < 20 || h1.length < 10) return null;

  const et   = nowET();
  const hhmm = et.getHours() * 100 + et.getMinutes();

  // Kill Zone check
  const inLDN = hhmm >= 200  && hhmm <= 500;
  const inNY  = hhmm >= 830  && hhmm <= 1100;
  const inKZ  = inLDN || inNY;
  if (!inKZ) return null;
  const session = inLDN ? 'London' : 'NY';

  const n    = h4.length;
  const last = h4[n - 1];
  const prev = h4.slice(-20);

  // Structure
  const swH = Math.max(...prev.map(c => c.h));
  const swL = Math.min(...prev.map(c => c.l));
  const eq  = (swH + swL) / 2;

  const prev3H = Math.max(...h4.slice(-4, -1).map(c => c.h));
  const prev3L = Math.min(...h4.slice(-4, -1).map(c => c.l));
  const bos_bull   = last.c > prev3H;
  const bos_bear   = last.c < prev3L;
  const choch_bull = last.l < prev3L && last.c > prev3L;
  const choch_bear = last.h > prev3H && last.c < prev3H;

  // Liquidity sweep
  const ph = h4.slice(-6, -1).map(c => c.h);
  const pl = h4.slice(-6, -1).map(c => c.l);
  const turtleBull = last.l < Math.min(...pl) && last.c > Math.min(...pl);
  const turtleBear = last.h > Math.max(...ph) && last.c < Math.max(...ph);
  const bslSwept   = last.h > swH * 0.999 && last.c < swH;
  const sslSwept   = last.l < swL * 1.001 && last.c > swL;
  const hasLiqSweep = turtleBull || turtleBear || bslSwept || sslSwept;

  // FVG
  const fvgs_bull = [], fvgs_bear = [];
  for (let i = 1; i < n - 1; i++) {
    if (h4[i-1] && h4[i+1]) {
      if (h4[i-1].h < h4[i+1].l) fvgs_bull.push({ top: h4[i+1].l, bot: h4[i-1].h });
      if (h4[i-1].l > h4[i+1].h) fvgs_bear.push({ top: h4[i-1].l, bot: h4[i+1].h });
    }
  }
  const atFvgBull = fvgs_bull.some(f => last.c >= f.bot * 0.998 && last.c <= f.top * 1.002);
  const atFvgBear = fvgs_bear.some(f => last.c >= f.bot * 0.998 && last.c <= f.top * 1.002);
  const hasEntryArray = atFvgBull || atFvgBear;

  // CISD
  const avgRange = h4.slice(-10).reduce((s, c) => s + (c.h - c.l), 0) / 10;
  const cisd = (last.h - last.l) > avgRange * 1.5 ? (last.c > last.o ? 'bull' : 'bear') : null;

  // H1 MSS
  const h1n = h1.length;
  const h1last = h1[h1n - 1];
  const h1HasMSS = h1n > 3 && h1last && (
    h1last.c > Math.max(...h1.slice(-4, -1).map(c => c.h)) ||
    h1last.c < Math.min(...h1.slice(-4, -1).map(c => c.l))
  );
  const hasCisd = !!(cisd || h1HasMSS);

  // Score
  let gs = 0;
  gs += 2; // KZ confirmed above
  if (bos_bull || bos_bear)     gs += 1;
  if (choch_bull || choch_bear) gs += 1;
  if (hasLiqSweep)              gs += 2;
  if (hasCisd)                  gs += 1;
  if (hasEntryArray)            gs += 1;
  if (cisd && hasLiqSweep && hasEntryArray) gs += 1;

  const fullConf = hasLiqSweep && hasCisd && hasEntryArray;
  const grade = fullConf && gs >= 8 ? 'A++' : gs >= 6 ? 'A+' : gs >= 4 ? 'B' : null;
  if (!grade) return null;

  // Direction
  const bullScore = (bos_bull?1:0) + (choch_bull?1:0) + (turtleBull?1:0) + (last.c < eq ?1:0) + (cisd==='bull'?1:0);
  const bearScore = (bos_bear?1:0) + (choch_bear?1:0) + (turtleBear?1:0) + (last.c > eq ?1:0) + (cisd==='bear'?1:0);
  if (bullScore === bearScore) return null;
  const dir    = bullScore > bearScore ? 'LONG' : 'SHORT';
  const isLong = dir === 'LONG';

  // Fib gate
  const fibPct = swH > swL ? (last.c - swL) / (swH - swL) * 100 : 50;
  if (isLong  && fibPct > 79) return null;
  if (!isLong && fibPct < 21) return null;

  // Trade levels
  const entry   = last.c;
  const slDist  = Math.max(last.h - last.l, avgRange) * 1.1;
  const sl      = isLong ? entry - slDist : entry + slDist;
  const tp1     = isLong ? entry + slDist * 1.5 : entry - slDist * 1.5;
  const tp2     = isLong ? swH : swL;
  const tp3     = isLong ? swH + slDist * 2 : swL - slDist * 2;
  const rr      = slDist > 0 ? Math.abs(tp2 - entry) / slDist : 0;

  const conditions = [];
  if (inKZ)          conditions.push(`Kill Zone: ${session}`);
  if (hasLiqSweep)   conditions.push(turtleBull||turtleBear ? 'Turtle Soup sweep' : `${isLong?'SSL':'BSL'} swept`);
  if (cisd)          conditions.push(`CISD ${cisd==='bull'?'▲':'▼'} H4`);
  if (choch_bull||choch_bear) conditions.push('CHoCH confirmed');
  if (hasEntryArray) conditions.push(`FVG array @ ${entry.toFixed(2)}`);
  conditions.push(`${isLong?'Discount':'Premium'} ${fibPct.toFixed(1)}% fib`);
  conditions.push(`DOL: ${isLong?'BSL':'SSL'} @ ${tp2.toFixed(2)}`);

  const timeStr = et.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });

  return { dir, grade, session, time: timeStr, entry, sl, slDist, tp1, tp2, tp3, rr, conditions, fibPct };
}

// ── TELEGRAM ──────────────────────────────────────────────────────────────────────
async function sendTelegram(sig) {
  const dir   = sig.dir === 'LONG' ? '🟢 LONG' : '🔴 SHORT';
  const badge = sig.grade === 'A++' ? '🏆 A++' : sig.grade === 'A+' ? '⭐ A+' : '🔵 B';
  const flag  = sig.session === 'London' ? '🇬🇧' : '🗽';

  const text = [
    `⚡ *SIGNAL POINTER — XAUUSD*`,
    `${dir}  ${badge}  ${flag} ${sig.session}`,
    `🕐 ${sig.time} ET`,
    ``,
    `┌─ TRADE LEVELS ───────────`,
    `│ ENTRY : \`${sig.entry.toFixed(2)}\``,
    `│ SL    : \`${sig.sl.toFixed(2)}\` (${sig.slDist.toFixed(1)} pts)`,
    `│ TP1   : \`${sig.tp1.toFixed(2)}\` (~1.5R)`,
    `│ TP2   : \`${sig.tp2.toFixed(2)}\` (DOL)`,
    `│ TP3   : \`${sig.tp3.toFixed(2)}\` (5R ext)`,
    `│ R:R   : ${sig.rr.toFixed(1)}:1`,
    `└──────────────────────────`,
    ``,
    `📋 *Conditions:*`,
    ...sig.conditions.map(c => `• ${c}`),
    ``,
    `_ICT 2022 · Signal Pointer · Auto-alert_`,
  ].join('\n');

  const r = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: TG_CHAT_ID, text, parse_mode: 'Markdown', disable_web_page_preview: true }),
  });
  const d = await r.json();
  if (!d.ok) console.error('Telegram error:', d.description);
  return d.ok;
}

// ── DEDUP ─────────────────────────────────────────────────────────────────────────
let lastSignalKey = '';
let lastSignalTime = 0;
let lastSignalEntry = 0;
const SIGNAL_COOLDOWN_MS = 90 * 60 * 1000;  // 90 min cooldown per signal direction
const SIGNAL_ZONE_PTS   = 40;               // ignore new signal if entry within 40pts of last

function shouldSend(sig) {
  const now = Date.now();

  // ── Quality gate: must have at least sweep OR FVG OR CISD beyond just KZ+fib+DOL
  const hasQuality = sig.conditions.some(c =>
    c.includes('sweep') || c.includes('swept') ||
    c.includes('FVG')   || c.includes('CISD')  ||
    c.includes('CHoCH')
  );
  if (!hasQuality) {
    console.log(`[SKIP] Grade ${sig.grade} @ ${sig.entry.toFixed(2)} — no sweep/FVG/CISD, only KZ+fib+DOL`);
    return false;
  }

  // ── Cooldown: same direction within 90 min
  const dirKey = `${sig.dir}_${sig.session}`;
  if (dirKey === lastSignalKey && now - lastSignalTime < SIGNAL_COOLDOWN_MS) {
    // Allow upgrade: if last signal was B and new is A+ or A++, send anyway
    if (!(sig.grade === 'A++' || sig.grade === 'A+')) {
      console.log(`[SKIP] ${sig.dir} @ ${sig.entry.toFixed(2)} — cooldown active (${Math.round((SIGNAL_COOLDOWN_MS-(now-lastSignalTime))/60000)}min left)`);
      return false;
    }
  }

  // ── Zone dedup: entry within 40pts of last signal (price just drifting)
  if (lastSignalEntry > 0 && Math.abs(sig.entry - lastSignalEntry) < SIGNAL_ZONE_PTS && dirKey === lastSignalKey) {
    console.log(`[SKIP] ${sig.dir} @ ${sig.entry.toFixed(2)} — too close to last signal @ ${lastSignalEntry.toFixed(2)} (${Math.abs(sig.entry-lastSignalEntry).toFixed(1)}pts)`);
    return false;
  }

  lastSignalKey   = dirKey;
  lastSignalTime  = now;
  lastSignalEntry = sig.entry;
  return true;
}

// ── ACTIVE TRADE MONITOR ──────────────────────────────────────────────────────────
// Tracks open signal and fires Telegram alerts when price hits TP1/TP2/TP3/SL
let activeTrade = null; // { dir, entry, sl, tp1, tp2, tp3, grade, session, tp1Hit, tp2Hit, tp3Hit, slHit, entryTime }

function setActiveTrade(sig) {
  activeTrade = {
    dir:       sig.dir,
    entry:     sig.entry,
    sl:        sig.sl,
    tp1:       sig.tp1,
    tp2:       sig.tp2,
    tp3:       sig.tp3,
    slDist:    sig.slDist,
    grade:     sig.grade,
    session:   sig.session,
    tp1Hit:    false,
    tp2Hit:    false,
    tp3Hit:    false,
    slHit:     false,
    entryTime: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', timeZone: 'America/New_York', hour12: false }) + ' ET',
  };
  console.log(`[TRADE] Active trade set: ${sig.dir} @ ${sig.entry.toFixed(2)} | SL ${sig.sl.toFixed(2)} | TP1 ${sig.tp1.toFixed(2)} | TP2 ${sig.tp2.toFixed(2)}`);
}

async function sendTradeUpdate(type, price) {
  const t = activeTrade;
  if (!t) return;

  const dir     = t.dir === 'LONG' ? '🟢 LONG' : '🔴 SHORT';
  const pnlPts  = t.dir === 'LONG' ? price - t.entry : t.entry - price;
  const pnlR    = (pnlPts / t.slDist).toFixed(1);
  const pnlSign = pnlPts >= 0 ? '+' : '';

  let emoji, headline, note;
  if (type === 'TP1') { emoji = '🎯'; headline = 'TP1 HIT — Move SL to breakeven'; note = `Secured ~1.5R — SL → ${t.entry.toFixed(2)}`; }
  if (type === 'TP2') { emoji = '💰'; headline = 'TP2 HIT — DOL Reached!';          note = `Strong target hit — consider closing or trailing`; }
  if (type === 'TP3') { emoji = '🏆'; headline = 'TP3 HIT — Full Extension!';        note = `Maximum target reached — close all remaining`; }
  if (type === 'SL')  { emoji = '🛑'; headline = 'STOP LOSS HIT';                    note = `Loss contained. Wait for next KZ setup.`; }

  const text = [
    `${emoji} *${headline}*`,
    ``,
    `${dir}  ${t.grade}  @ Entry \`${t.entry.toFixed(2)}\``,
    `📍 Price now: \`${price.toFixed(2)}\``,
    `📊 P&L: ${pnlSign}${pnlPts.toFixed(1)} pts  (${pnlSign}${pnlR}R)`,
    ``,
    `┌─ LEVELS ─────────────────`,
    `│ Entry : \`${t.entry.toFixed(2)}\``,
    `│ SL    : \`${t.sl.toFixed(2)}\`  ${type === 'SL' ? '← HIT ✗' : ''}`,
    `│ TP1   : \`${t.tp1.toFixed(2)}\`  ${t.tp1Hit ? '✓' : type === 'TP1' ? '← HIT ✓' : ''}`,
    `│ TP2   : \`${t.tp2.toFixed(2)}\`  ${t.tp2Hit ? '✓' : type === 'TP2' ? '← HIT ✓' : ''}`,
    `│ TP3   : \`${t.tp3.toFixed(2)}\`  ${t.tp3Hit ? '✓' : type === 'TP3' ? '← HIT ✓' : ''}`,
    `└──────────────────────────`,
    ``,
    `💡 ${note}`,
    `_Signal Pointer · ICT 2022_`,
  ].join('\n');

  const r = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: TG_CHAT_ID, text, parse_mode: 'Markdown', disable_web_page_preview: true }),
  });
  const d = await r.json();
  if (d.ok) console.log(`[TRADE] ${type} alert sent ✓`);
  else console.error(`[TRADE] Telegram error:`, d.description);
}

function checkTradeLevels(price) {
  const t = activeTrade;
  if (!t || t.slHit || t.tp3Hit) return;
  const isLong = t.dir === 'LONG';

  // SL hit
  if (!t.slHit && (isLong ? price <= t.sl : price >= t.sl)) {
    t.slHit = true;
    activeTrade = null; // close trade
    sendTradeUpdate('SL', price);
    console.log(`[TRADE] SL hit @ ${price.toFixed(2)}`);
    return;
  }
  // TP1
  if (!t.tp1Hit && (isLong ? price >= t.tp1 : price <= t.tp1)) {
    t.tp1Hit = true;
    sendTradeUpdate('TP1', price);
    console.log(`[TRADE] TP1 hit @ ${price.toFixed(2)}`);
  }
  // TP2
  if (t.tp1Hit && !t.tp2Hit && (isLong ? price >= t.tp2 : price <= t.tp2)) {
    t.tp2Hit = true;
    sendTradeUpdate('TP2', price);
    console.log(`[TRADE] TP2 hit @ ${price.toFixed(2)}`);
  }
  // TP3
  if (t.tp2Hit && !t.tp3Hit && (isLong ? price >= t.tp3 : price <= t.tp3)) {
    t.tp3Hit = true;
    activeTrade = null; // fully closed
    sendTradeUpdate('TP3', price);
    console.log(`[TRADE] TP3 hit @ ${price.toFixed(2)}`);
  }
}

// ── WEBSOCKET CONNECTION ──────────────────────────────────────────────────────────
let ws = null;
let tickCount = 0;
let reconnectDelay = 3000;

function connect() {
  console.log(`[${new Date().toISOString()}] Connecting to TwelveData WebSocket...`);

  ws = new WebSocket(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${TD_KEY}`);

  ws.on('open', () => {
    console.log('WS connected — subscribing to XAU/USD');
    reconnectDelay = 3000;
    ws.send(JSON.stringify({
      action: 'subscribe',
      params: { symbols: 'XAU/USD' }
    }));
    // Status ping every 30 min
    setInterval(() => {
      const et = nowET();
      const hhmm = et.getHours() * 100 + et.getMinutes();
      const inKZ = (hhmm >= 200 && hhmm <= 500) || (hhmm >= 830 && hhmm <= 1100);
      console.log(`[PING] Ticks: ${tickCount} | KZ: ${inKZ} | H4 candles: ${candles['4h'].length} | Last price: ${currentCandle['4h']?.c || 'n/a'}`);
    }, 30 * 60 * 1000);
  });

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.event === 'price' && msg.symbol === 'XAU/USD' && msg.price) {
        const price = parseFloat(msg.price);
        const ts    = msg.timestamp ? msg.timestamp * 1000 : Date.now();
        processTick(price, ts);
        tickCount++;

        // Check active trade levels on EVERY tick (real-time TP/SL monitoring)
        checkTradeLevels(price);

        // Check for new signal every 50 ticks
        if (tickCount % 50 === 0) {
          const sig = computeSignal();
          if (sig && shouldSend(sig)) {
            console.log(`[SIGNAL] ${sig.dir} ${sig.grade} @ ${sig.entry.toFixed(2)} — sending to Telegram`);
            sendTelegram(sig).then(ok => {
              if (ok) {
                console.log('[SIGNAL] Telegram sent ✓');
                setActiveTrade(sig); // start monitoring TP/SL
              }
            });
          }
        }
      }
    } catch (e) {
      console.error('Parse error:', e.message);
    }
  });

  ws.on('error', (e) => {
    console.error('WS error:', e.message);
  });

  ws.on('close', (code) => {
    console.log(`WS closed (${code}) — reconnecting in ${reconnectDelay / 1000}s`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 60000); // exponential backoff, max 1 min
  });
}

// ── STARTUP ───────────────────────────────────────────────────────────────────────
console.log('=================================================');
console.log(' ICT Gold Terminal — 24/7 Signal Bot');
console.log(' Pair: XAUUSD | Grades: B / A+ / A++');
console.log(' Kill Zones: London 02:00-05:00 ET | NY 08:30-11:00 ET');
console.log('=================================================');

// Seed candles from REST before WS (so engine has data immediately on start)
async function seedCandles() {
  for (const [tf, interval] of [['4h','4h'],['1h','1h'],['15m','15min']]) {
    try {
      const url = `https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=${interval}&outputsize=80&format=JSON&apikey=${TD_KEY}`;
      const r = await fetch(url);
      const d = await r.json();
      if (d.values) {
        candles[tf] = d.values.map(v => ({
          t: new Date(v.datetime).getTime(),
          o: parseFloat(v.open), h: parseFloat(v.high),
          l: parseFloat(v.low),  c: parseFloat(v.close),
        })).reverse();
        console.log(`Seeded ${candles[tf].length} candles for ${tf}`);
      }
    } catch (e) { console.error(`Seed error ${tf}:`, e.message); }
  }
}

// HTTP server (Railway/Render require a port to stay alive)
const http = require('http');
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};
http.createServer((req, res) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS);
    res.end();
    return;
  }
  const et    = nowET();
  const hhmm  = et.getHours() * 100 + et.getMinutes();
  const inKZ  = (hhmm >= 200 && hhmm <= 500) || (hhmm >= 830 && hhmm <= 1100);
  const h4len = candles['4h'].length;
  const price = currentCandle['4h']?.c || 0;

    // /test — send a real test signal to Telegram to confirm pipeline
  if (req.url === '/test') {
    const livePrice = currentCandle['4h']?.c || 3990;
    const testSig = {
      dir: 'LONG', grade: 'A+', session: 'NY', time: '09:15',
      entry: livePrice, sl: livePrice - 12.5, slDist: 12.5,
      tp1: livePrice + 18.75, tp2: livePrice + 35, tp3: livePrice + 62.5, rr: 2.8,
      conditions: [
        'Kill Zone: NY AM',
        'SSL swept (Turtle Soup)',
        'CISD ▲ on H4',
        'FVG entry array confirmed',
        `Discount zone (fib 38.2%)`,
        `DOL: BSL @ ${(livePrice + 35).toFixed(2)}`,
        '⚠ TEST MESSAGE — confirming bot pipeline',
      ],
    };
    sendTelegram(testSig).then(ok => {
      res.writeHead(200, { 'Content-Type': 'application/json', ...CORS });
      res.end(JSON.stringify({ sent: ok, price: livePrice.toFixed(2), message: ok ? 'Check your Telegram group!' : 'Telegram send failed — check TG_TOKEN and TG_CHAT_ID' }));
    });
    return;
  }

  res.writeHead(200, { 'Content-Type': 'application/json', ...CORS });
  res.end(JSON.stringify({
    status: 'running',
    wsConnected: ws?.readyState === 1,
    ticks: tickCount,
    price: price.toFixed(2),
    h4Candles: h4len,
    killZone: inKZ,
    session: inKZ ? (hhmm < 500 ? 'London' : 'NY') : 'Off-hours',
    lastSignal: lastSignalKey || 'none',
    activeTrade: activeTrade ? {
      dir: activeTrade.dir, grade: activeTrade.grade,
      entry: activeTrade.entry.toFixed(2), sl: activeTrade.sl.toFixed(2),
      tp1: activeTrade.tp1.toFixed(2), tp1Hit: activeTrade.tp1Hit,
      tp2: activeTrade.tp2.toFixed(2), tp2Hit: activeTrade.tp2Hit,
      tp3: activeTrade.tp3.toFixed(2), tp3Hit: activeTrade.tp3Hit,
    } : null,
    uptime: Math.floor(process.uptime()) + 's',
  }));
}).listen(process.env.PORT || 3000, () => {
  console.log(`Health check: http://localhost:${process.env.PORT || 3000}`);
});

// ── KEEP-ALIVE SELF-PING (Render free tier spins down after 15min inactivity) ───
// Pings own health endpoint every 10 minutes to stay awake
if (process.env.RENDER_EXTERNAL_URL) {
  const SELF_URL = process.env.RENDER_EXTERNAL_URL;
  setInterval(async () => {
    try {
      await fetch(SELF_URL);
      console.log(`[PING] Self-ping sent to ${SELF_URL}`);
    } catch (e) {
      console.warn('[PING] Self-ping failed:', e.message);
    }
  }, 10 * 60 * 1000); // every 10 minutes
  console.log(`Keep-alive enabled → pinging ${SELF_URL} every 10 min`);
}

seedCandles().then(() => connect());
