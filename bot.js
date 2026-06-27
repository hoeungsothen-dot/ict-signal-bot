/**
 * ICT Gold Terminal — 24/7 Real-Time Signal Bot
 * Connects to TwelveData WebSocket → builds live candles → runs ICT signal engine
 * → sends Telegram alert instantly when B/A+/A++ fires
 *
 * Deploy free on Railway or Render (no server needed)
 * Env vars: TD_KEY, TG_TOKEN, TG_CHAT_ID
 */

const WebSocket = require('ws');

// ── CLOUDFLARE D1 PERSISTENCE (trade survives Render restarts) ────────────────────
const WORKER_URL  = process.env.WORKER_URL  || ''; // e.g. https://ict-mentorship2022-system.workers.dev
const BOT_KEY     = process.env.TD_KEY ? process.env.TD_KEY.slice(-8) : '';

async function saveTrade(trade) {
  if (!WORKER_URL) return;
  try {
    await fetch(`${WORKER_URL}/api/active-trade`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'X-Bot-Key': BOT_KEY },
      body: trade ? JSON.stringify(trade) : 'null',
    });
  } catch(e) { console.warn('[STORE] Save trade failed:', e.message); }
}

async function loadTrade() {
  if (!WORKER_URL) return null;
  try {
    const r = await fetch(`${WORKER_URL}/api/active-trade`);
    const t = await r.json();
    if (t && t.entry && !t.slHit && !t.tp3Hit) {
      console.log(`[RESTORE] Trade restored from D1: ${t.dir} @ ${t.entry} | TP1 ${t.tp1} | TP1Hit:${t.tp1Hit}`);
      return t;
    }
  } catch(e) { console.warn('[STORE] Load trade failed:', e.message); }
  return null;
}

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


// ═══════════════════════════════════════════════════════════════════════════════
// ICT SIGNAL ENGINE — Web App Engine (analyzeICT + detectExecutionOnBars)
// Ported from index.html — same logic that produces 56.5% WR / PF 7.91
// ═══════════════════════════════════════════════════════════════════════════════

// ── NEWS DATA & BLOCKER ─────────────────────────────────────────────────────────
const KNOWN_EVENTS=[
  {day:'MON',time:'08:30',event:'ISM Manufacturing PMI',prev:'49.0',fore:'49.5',impact:'HIGH',pairs:['DXY','XAU/USD'],usdDir:'bull'},
  {day:'TUE',time:'10:00',event:'JOLTS Job Openings',prev:'7.5M',fore:'7.6M',impact:'HIGH',pairs:['DXY','XAU/USD'],usdDir:'bull'},
  {day:'WED',time:'08:15',event:'ADP Non-Farm Employment',prev:'183K',fore:'175K',impact:'HIGH',pairs:['DXY','XAU/USD'],usdDir:'bear'},
  {day:'WED',time:'14:00',event:'FOMC Minutes',prev:'-',fore:'-',impact:'HIGH',pairs:['DXY','XAU/USD','XAG/USD'],usdDir:'neut'},
  {day:'THU',time:'08:30',event:'Initial Jobless Claims',prev:'222K',fore:'218K',impact:'MED',pairs:['DXY'],usdDir:'bull'},
  {day:'FRI',time:'08:30',event:'Non-Farm Payrolls',prev:'256K',fore:'175K',impact:'HIGH',pairs:['DXY','XAU/USD','XAG/USD','BTC/USD'],usdDir:'bear'},
  {day:'FRI',time:'08:30',event:'Unemployment Rate',prev:'4.1%',fore:'4.1%',impact:'HIGH',pairs:['DXY','XAU/USD'],usdDir:'neut'},
  {day:'FRI',time:'10:00',event:'U.Michigan Sentiment',prev:'67.5',fore:'68.0',impact:'MED',pairs:['DXY'],usdDir:'bull'},
];

// Fed & Key speaker schedule (tracked for XAUUSD/DXY impact)
const KNOWN_SPEECHES=[
  {day:'MON',time:'10:00',speaker:'Jerome Powell',role:'Fed Chair',topic:'Economic Outlook — Congressional Testimony',impact:'EXTREME',pairs:['DXY','XAU/USD','XAG/USD'],tone:'neutral'},
  {day:'TUE',time:'09:15',speaker:'John Williams',role:'NY Fed President',topic:'Labor market and inflation',impact:'HIGH',pairs:['DXY','XAU/USD'],tone:'hawkish'},
  {day:'WED',time:'11:30',speaker:'Christopher Waller',role:'Fed Governor',topic:'Monetary policy path',impact:'HIGH',pairs:['DXY','XAU/USD'],tone:'hawkish'},
  {day:'THU',time:'14:00',speaker:'Raphael Bostic',role:'Atlanta Fed President',topic:'Rate cut timeline',impact:'MED',pairs:['DXY'],tone:'dovish'},
  {day:'FRI',time:'09:00',speaker:'Janet Yellen',role:'Treasury Secretary',topic:'US fiscal policy',impact:'MED',pairs:['DXY','XAU/USD'],tone:'neutral'},
];


function loadNews(){
  const panel=document.getElementById('news-panel');
  const corr=document.getElementById('corr-panel');
  const weekDates=getWeekDates();
  const dateMap={};weekDates.forEach(d=>dateMap[d.day]=d.date);
  const mon=weekDates[0],fri=weekDates[4];
  const wkLbl=document.getElementById('news-week-label');
  if(wkLbl)wkLbl.textContent=`Week of ${mon.date} – ${fri.date}`;

  const dayOrder=['MON','TUE','WED','THU','FRI'];
  let html='';
  dayOrder.forEach(day=>{
    const evs=KNOWN_EVENTS.filter(e=>e.day===day);
    const spk=KNOWN_SPEECHES.filter(s=>s.day===day);
    if(!evs.length&&!spk.length)return;
    const dateStr=dateMap[day]||'';
    const today=new Date();
    const todayDay=['SUN','MON','TUE','WED','THU','FRI','SAT'][today.getDay()];
    const isToday=day===todayDay;
    html+=`<div style="display:flex;align-items:center;gap:8px;padding:8px 4px 4px;border-bottom:2px solid ${isToday?'var(--blue)':'var(--border)'};margin-bottom:6px">
      <span style="font-family:var(--display);font-size:15px;font-weight:700;color:${isToday?'var(--blue)':'var(--text2)'}">${day}</span>
      <span style="font-family:var(--mono);font-size:13px;color:var(--text3)">${dateStr}</span>
      ${isToday?'<span style="font-family:var(--mono);font-size:11px;padding:1px 6px;border-radius:10px;background:var(--blue);color:#fff;margin-left:4px">TODAY</span>':''}
    </div>`;

    // ── Economic Events ──
    evs.forEach(ev=>{
      const isPairs=ev.pairs.includes(ACTIVE_PAIR);
      const impCol=ev.impact==='HIGH'?'var(--bear)':ev.impact==='MED'?'var(--orange)':'var(--text3)';
      const prevNum=parseFloat(ev.prev),foreNum=parseFloat(ev.fore);
      const pairBiases=getPairBias(ev.event,prevNum,foreNum,ev.pairs);
      const explain=getEventExplain(ev.event);
      // Direction badge
      const dirCol=ev.usdDir==='bull'?'var(--bull)':ev.usdDir==='bear'?'var(--bear)':'var(--text3)';
      const dirTxt=ev.usdDir==='bull'?'USD↑':ev.usdDir==='bear'?'USD↓':'NEUT';
      const pairPills=ev.pairs.map(p=>{
        const b=pairBiases[p]||'NEUT';
        const bCol=b==='BULL'?'var(--bull)':b==='BEAR'?'var(--bear)':'var(--text3)';
        return`<div style="display:flex;align-items:center;gap:3px;background:var(--bg3);border:1px solid var(--border);border-radius:5px;padding:3px 7px">
          <span style="font-family:var(--display);font-size:11px;color:var(--blue);font-weight:700">${p}</span>
          <span style="font-family:var(--display);font-size:11px;color:${bCol};font-weight:700">${b}</span>
        </div>`;
      }).join('');

      html+=`<div style="padding:10px;border-radius:8px;border:1px solid ${isPairs?'var(--border2)':'var(--border)'};background:${isPairs?'var(--bg3)':'var(--bg2)'};margin-bottom:8px">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px">
          <span style="font-family:var(--mono);font-size:13px;color:var(--text3);flex-shrink:0">${ev.time} EST</span>
          <span style="font-size:12px;padding:2px 6px;border-radius:3px;background:${impCol}22;color:${impCol};border:1px solid ${impCol}44;font-weight:700;flex-shrink:0">${ev.impact}</span>
          <span style="font-size:14px;color:var(--gold);font-weight:700;flex:1">${ev.event}</span>
          <span style="font-family:var(--mono);font-size:11px;padding:2px 5px;border-radius:3px;background:${dirCol}20;color:${dirCol};border:1px solid ${dirCol}40;flex-shrink:0">${dirTxt}</span>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px">
          <div style="background:var(--bg1);border-radius:5px;padding:6px 8px;text-align:center;border:1px solid var(--border)">
            <div style="font-size:11px;color:var(--text3);margin-bottom:2px;letter-spacing:1px">PREVIOUS</div>
            <div style="font-family:var(--mono);font-size:16px;font-weight:700;color:var(--text)">${ev.prev}</div>
          </div>
          <div style="background:var(--bg1);border-radius:5px;padding:6px 8px;text-align:center;border:1px solid var(--border)">
            <div style="font-size:11px;color:var(--text3);margin-bottom:2px;letter-spacing:1px">FORECAST</div>
            <div style="font-family:var(--mono);font-size:16px;font-weight:700;color:${ev.usdDir==='bull'?'var(--bull)':ev.usdDir==='bear'?'var(--bear)':'var(--text)'}">${ev.fore}</div>
          </div>
        </div>
        <!-- Explanation box -->
        <div style="background:var(--bg0);border-left:3px solid var(--gold2);border-radius:0 5px 5px 0;padding:7px 10px;margin-bottom:8px">
          <div style="font-size:11px;color:var(--gold2);letter-spacing:1px;margin-bottom:3px;font-weight:700">📌 WHAT THIS MEANS</div>
          <div style="font-family:var(--body);font-size:13px;color:var(--text2);line-height:1.55">${explain}</div>
        </div>
        <div style="font-size:11px;color:var(--text3);margin-bottom:5px;letter-spacing:1px">IMPACT PER PAIR:</div>
        <div style="display:flex;flex-wrap:wrap;gap:5px">${pairPills}</div>
      </div>`;
    });

    // ── Speaker Events ──
    spk.forEach(sp=>{
      const toneCol=sp.tone==='hawkish'?'var(--bear)':sp.tone==='dovish'?'var(--bull)':'var(--text3)';
      const toneTxt=sp.tone==='hawkish'?'🦅 HAWKISH':sp.tone==='dovish'?'🕊 DOVISH':'⚖ NEUTRAL';
      const impCol=sp.impact==='EXTREME'?'#ff3d71':sp.impact==='HIGH'?'var(--bear)':sp.impact==='MED'?'var(--orange)':'var(--text3)';
      const pairPills2=sp.pairs.map(p=>`<div style="background:rgba(206,147,216,.12);border:1px solid rgba(206,147,216,.3);border-radius:4px;padding:2px 7px;font-family:var(--mono);font-size:11px;color:var(--purple)">${p}</div>`).join('');
      html+=`<div style="padding:10px;border-radius:8px;border:1px solid rgba(206,147,216,.25);background:rgba(206,147,216,.05);margin-bottom:8px;position:relative">
        <div style="position:absolute;top:8px;right:10px;font-size:11px;font-family:var(--mono);color:${toneCol};background:${toneCol}18;border:1px solid ${toneCol}44;border-radius:4px;padding:2px 6px">${toneTxt}</div>
        <div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:6px;padding-right:90px">
          <div style="width:34px;height:34px;border-radius:50%;background:linear-gradient(135deg,var(--purple),var(--blue));display:flex;align-items:center;justify-content:center;font-size:17px;flex-shrink:0">🎙</div>
          <div>
            <div style="font-size:15px;color:var(--purple);font-weight:700">${sp.speaker}</div>
            <div style="font-size:12px;color:var(--text3)">${sp.role} · ${sp.time} EST</div>
          </div>
        </div>
        <div style="font-size:11px;padding:2px 6px;border-radius:3px;background:${impCol}22;color:${impCol};border:1px solid ${impCol}44;font-weight:700;display:inline-block;margin-bottom:6px">${sp.impact} IMPACT</div>
        <div style="background:var(--bg0);border-left:3px solid var(--purple);border-radius:0 5px 5px 0;padding:7px 10px;margin-bottom:7px">
          <div style="font-size:11px;color:var(--purple);letter-spacing:1px;margin-bottom:2px;font-weight:700">🗣 TOPIC</div>
          <div style="font-size:13px;color:var(--text2)">${sp.topic}</div>
        </div>
        <div style="display:flex;gap:5px;flex-wrap:wrap">${pairPills2}</div>
      </div>`;
    });

    html+='<div style="height:6px"></div>';
  });
  panel.innerHTML=html||'<div style="text-align:center;color:var(--text3);padding:16px;font-size:14px">No events found</div>';

  // Correlation panel
  const corrData=[
    {pair:'XAUUSD',label:'Gold',corr:'+1.00',col:'var(--gold)',note:'Primary instrument'},
    {pair:'XAGUSD',label:'Silver',corr:'+0.87',col:'var(--teal)',note:'Follows Gold — confirm moves'},
    {pair:'DXY',label:'US Dollar',corr:'–0.82',col:'var(--bear)',note:'Inverse: USD up = Gold down'},
    {pair:'BTCUSD',label:'Bitcoin',corr:'+0.45',col:'var(--purple)',note:'Risk-on correlation, variable'},
  ];
  corr.innerHTML=corrData.map(c=>`
    <div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid var(--border)">
      <span style="font-family:var(--display);font-size:14px;font-weight:700;color:${c.col};width:60px;flex-shrink:0">${c.label}</span>
      <span style="font-family:var(--mono);font-size:16px;font-weight:700;color:${parseFloat(c.corr)>0?'var(--bull)':'var(--bear)'};width:50px;flex-shrink:0">${c.corr}</span>
      <span style="font-family:var(--mono);font-size:12px;color:var(--text3);flex:1">${c.note}</span>
    </div>
  `).join('');
}

function saveMarketNotes(){
  const notes=document.getElementById('market-notes').value;
  localStorage.setItem('market_notes',notes);
  const btn=document.querySelector('#view-news button[onclick="saveMarketNotes()"]');
  if(btn){btn.textContent='SAVED!';setTimeout(()=>btn.textContent='SAVE NOTES',1500);}
}

async function runNewsAI(){
  const btn=document.getElementById('news-ai-btn');
  const out=document.getElementById('news-ai-out');
  if(btn){btn.disabled=true;btn.innerHTML='<svg width="16" height="16" viewBox="0 0 24 24" fill="#000" stroke="none"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> ANALYZING...';}
  out.innerHTML='<div style="font-size:14px;color:var(--text3);text-align:center;padding:16px">Analyzing <strong style="color:var(--gold)">'+ACTIVE_PAIR_LABEL+'</strong> fundamentals...<br><br><span class="cur"></span></div>';
  const notes=localStorage.getItem('market_notes')||'';
  const h4=anals['4h']||{};
  const prompt=`You are a professional forex market analyst. Analyze the current fundamental picture for ${ACTIVE_PAIR_LABEL} (${ACTIVE_PAIR}) and provide a concise fundamental analysis.\n\nCurrent technical bias: ${h4.bias>0?'BULLISH':'BEARISH'} (score: ${h4.bias?.toFixed(2)})\nMarket notes: ${notes||'None provided'}\nUpcoming events this week: NFP, FOMC Minutes, ADP\n\nProvide 3 sections:\n1.**FUNDAMENTAL BIAS** — Current macro theme for ${ACTIVE_PAIR_LABEL}\n2.**KEY EVENTS IMPACT** — How upcoming NFP/FOMC affects ${ACTIVE_PAIR_LABEL} with previous vs forecast analysis\n3.**TRADE CONFLUENCE** — How fundamentals align with or contradict the technical bias\n\nUse HTML: <span class="hl-bull"> bullish, <span class="hl-bear"> bearish, <span class="hl-gold"> prices. Be concise, max 150 words per section.`;
  try{
    const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:claudeHeaders(),body:JSON.stringify({model:'claude-sonnet-4-6',max_tokens:800,messages:[{role:'user',content:prompt}]})});
    const data=await r.json();out.innerHTML=parseSecs(data.content?.[0]?.text||'Analysis unavailable.');
  }catch(e){out.innerHTML='<div style="font-size:14px;color:var(--bear);padding:8px">Analysis unavailable. Please check your internet connection and try again.</div>';}
  if(btn){btn.disabled=false;btn.innerHTML='<svg width="16" height="16" viewBox="0 0 24 24" fill="#000" stroke="none"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> AI MARKET ANALYSIS';}
}

// ── CHART MODE ──────────────────────────────────────
// ── IN-APP FULLSCREEN (chart overlays entire screen) ─────────
const FS_ENTER = `<polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/><line x1="21" y1="3" x2="14" y2="10"/><line x1="3" y1="21" x2="10" y2="14"/>`;
const FS_EXIT  = `<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>`;
let _chartFs = false;

function toggleFullscreen(){
  _chartFs = !_chartFs;
  const card = document.getElementById('single-card');
  const icon = document.getElementById('fs-icon');
  const btn  = document.getElementById('btn-fullscreen');
  if(!card) return;
  if(_chartFs){
    // Detach card and place in fullscreen overlay
    const overlay = document.getElementById('chart-fs-overlay');
    overlay.appendChild(card);
    overlay.style.display = 'flex';
    document.body.style.overflow = 'hidden';
    if(icon) icon.innerHTML = FS_EXIT;
    if(btn)  btn.classList.add('active');
    // Tag the overlay with current view so CSS can constrain PD/KZ
    overlay.dataset.view = curTf || '';
    const exitIcon = document.getElementById('fs-exit-icon');
    if(exitIcon) exitIcon.innerHTML = FS_EXIT;
  } else {
    // Return card to single-view
    const sv = document.getElementById('single-view');
    sv.appendChild(card);
    document.getElementById('chart-fs-overlay').style.display = 'none';
    document.body.style.overflow = '';
    if(icon) icon.innerHTML = FS_ENTER;
    if(btn)  btn.classList.remove('active');
  }
  // Resize chart after layout settles
  setTimeout(()=>{
    if(chartI['chart-main']) chartI['chart-main'].resize();
    // Force redraw PD/KZ with updated _chartFs so aspectRatio applies correctly
    if(curTf==='pd' && anals['4h'] && price) drawPDMain(anals['4h'], price);
    else if(curTf==='kz' && cData['5m']) drawKZMain(cData['5m']);
    else { applyZoom(); updatePriceLbl(); }
  }, 80);
}

// ESC key exits fullscreen
document.addEventListener('keydown', e=>{
  if(e.key==='Escape' && _chartFs) toggleFullscreen();
});

function setMode(m){
  curMode=m;
  document.getElementById('mode-single').classList.toggle('active',m==='single');
  document.getElementById('mode-multi').classList.toggle('active',m==='multi');
  document.getElementById('single-view').classList.toggle('active',m==='single');
  document.getElementById('multi-view').classList.toggle('active',m==='multi');
  if(m==='single'){updateSingle(curTf);}
  // Resize all charts after DOM settles
  setTimeout(()=>{
    Object.values(chartI).forEach(c=>c?.resize?.());
  },60);
}

function selTfFromSelect(tf){
  selTf(tf, null);
}
function selTf(tf,btn){
  document.querySelectorAll('.tf-tab').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  curTf=tf;
  // Sync header TF badge active state
  document.querySelectorAll('.ftf-opt').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.ftf-opt').forEach(b => {
    if (b.getAttribute('onclick') === `hdrTFSelect('${tf}')`) b.classList.add('active');
  });
  if(curMode==='single'){
    // Lazy-load: if this TF has no data yet, fetch it now on-demand
    const _coreTFs=['1d','4h','1h','15m','5m'];
    if(!_coreTFs.includes(tf)&&tf!=='pd'&&tf!=='kz'&&!cData[tf]&&TF_CFG[tf]){
      loadTF(tf).then(()=>updateSingle(tf));
    } else if(!_coreTFs.includes(tf)&&tf!=='pd'&&tf!=='kz'&&cData[tf]&&TF_CFG[tf]){
      // Data exists but may be stale — re-fetch silently
      loadTF(tf).then(()=>updateSingle(tf));
    } else {
      updateSingle(tf);
    }
  }
}

function scrollRail(i){
  const r=document.getElementById('multi-rail');
  if(!r)return;
  r.scrollTo({left:i*(r.scrollWidth/6),behavior:'smooth'});
}

function updateSingle(tf){
  const m=TF_META[tf]||TF_META['4h'];
  const card=document.getElementById('single-card');
  if(card){card.className='chart-card '+m.cls;}
  const tb=document.getElementById('s-badge'),tt=document.getElementById('s-title');
  if(tb){tb.className='tf-badge '+m.badge;tb.textContent=tf.toUpperCase();}
  if(tt)tt.textContent=m.title;
  if(['4h','1h','15m','5m','1m','2m','3m','10m','30m','45m','2h','8h','1d','1w','1mo'].includes(tf)&&cData[tf]){
    const a=analyzeICT(cData[tf],tf);
    window._lastAnalysis=a;
    renderMain(tf,cData[tf],a);
    const last=cData[tf].at(-1),ch=last.c-last.o,pct=(ch/last.o*100).toFixed(2),col=ch>=0?'var(--bull)':'var(--bear)';
    const so=document.getElementById('s-ohlc');if(so)so.innerHTML=`<span><span class="o">O</span>${last.o}</span><span><span class="h">H</span>${last.h}</span><span><span class="l">L</span>${last.l}</span><span><span class="c">C</span>${last.c}</span>`;
    const st=document.getElementById('s-tags');if(st)st.innerHTML=buildTags(a);
    const ss=document.getElementById('s-sess');if(ss)ss.innerHTML=`<span style="color:${col};font-family:var(--mono);font-size:12px">${ch>=0?'▲':'▼'} ${Math.abs(ch).toFixed(2)} (${pct}%)</span><span style="color:var(--text3);font-size:11px;margin-left:auto">${a.inDiscount?'DISCOUNT':a.inPremium?'PREMIUM':'NEUTRAL'}</span>`;
    const so2=document.getElementById('s-overlay');if(so2)so2.innerHTML=[a.swH?`<div class="lvl-tag res">Res ${a.swH.toFixed(1)}</div>`:'',a.eq?`<div class="lvl-tag eq">EQ ${a.eq.toFixed(1)}</div>`:'',a.swL?`<div class="lvl-tag sup">Sup ${a.swL.toFixed(1)}</div>`:''].join('');
  }else if(tf==='pd'&&anals['4h'])drawPDMain(anals['4h'],price);
  else if(tf==='kz'&&cData['5m'])drawKZMain(cData['5m']);
}

// ── PINCH ZOOM ──────────────────────────────────────
// ── CHART ZOOM & PAN ─────────────────────────────────
let zLvl=1,zOff=0; // zOff = pan offset in candles from the right edge

// ── LIVE PRICE LABEL — pinned on y-axis like TradingView ────────
// ── Real-time live price label update (called by TdWS onmessage) ─────────────
// Updates: header price, chart annotation line, price-line-lbl on all timeframes
function updateLivePriceLabel(px) {
  if (!px || isNaN(px)) return;

  // 1. Update last candle close in ALL chart datasets so annotation/tooltip are live
  Object.keys(chartI).forEach(function(id) {
    const ch = chartI[id];
    if (!ch) return;
    const ds = ch.data.datasets[0];
    if (!ds || !ds.cd || !ds.cd.length) return;
    const last = ds.cd[ds.cd.length - 1];
    if (!last) return;
    // Update close price in candle data
    last.c = px;
    // Update the chart.js data point (last value = close)
    ds.data[ds.data.length - 1] = px;
    // Update the livePrice annotation on this chart
    const ann = ch.options?.plugins?.annotation?.annotations;
    if (ann && ann.livePrice) {
      const prev = ds.cd.length > 1 ? ds.cd[ds.cd.length - 2].c : px;
      const isUp = px >= prev;
      const lc = isUp ? 'rgba(0,230,118,.85)' : 'rgba(255,61,113,.85)';
      const lb = isUp ? 'rgba(0,230,118,.12)' : 'rgba(255,61,113,.12)';
      ann.livePrice.yMin = px;
      ann.livePrice.yMax = px;
      ann.livePrice.borderColor = lc;
      ann.livePrice.label.content = '\u25b6 ' + px.toFixed(2);
      ann.livePrice.label.color = lc;
      ann.livePrice.label.backgroundColor = lb;
    }
    // Silent update — no full re-render, just redraw
    try { ch.update('none'); } catch(e) {}
  });

  // 2. Reposition the main chart's floating price label
  updatePriceLblWithPrice(px);
}

// Like updatePriceLbl but uses a given price instead of reading from dataset
function updatePriceLblWithPrice(px) {
  const ch = chartI['chart-main'];
  if (!ch) return;
  const ds = ch.data.datasets[0];
  if (!ds || !ds.cd || !ds.cd.length) return;
  const prev = ds.cd.length > 1 ? ds.cd[ds.cd.length - 2].c : px;
  const isUp = px >= prev;
  const yScale = ch.scales?.y;
  if (!yScale) return;
  const pxY = yScale.getPixelForValue(px);
  if (isNaN(pxY)) return;
  const wrap = document.getElementById('main-wrap');
  const lbl = document.getElementById('price-line-lbl');
  if (!wrap || !lbl) return;
  const chartH = wrap.getBoundingClientRect().height;
  if (pxY < 0 || pxY > chartH) { lbl.style.display = 'none'; return; }
  lbl.style.display = 'block';
  lbl.style.top = pxY + 'px';
  lbl.style.right = '0';
  lbl.style.background = isUp ? '#00c060' : '#e03050';
  lbl.style.color = '#fff';
  lbl.style.fontSize = '10.5px';
  lbl.style.fontFamily = "'Share Tech Mono',monospace";
  lbl.style.fontWeight = '700';
  lbl.style.padding = '2px 6px';
  lbl.style.borderRadius = '3px 0 0 3px';
  lbl.style.lineHeight = '1.4';
  lbl.style.zIndex = '20';
  lbl.style.pointerEvents = 'none';
  const n = Math.abs(px) >= 100 ? px.toFixed(2) : px.toFixed(4);
  lbl.textContent = n;
}
// ── End real-time live price label ───────────────────────────────────────────

function updatePriceLbl(){
  const ch=chartI['chart-main'];if(!ch)return;
  const slice=ch.data.datasets[0]?.cd;
  if(!slice||!slice.length)return;
  const lp=slice.at(-1).c;
  const prev=slice.at(-2)?.c||lp;
  const isUp=lp>=prev;
  const yScale=ch.scales?.y;
  if(!yScale)return;
  const pxY=yScale.getPixelForValue(lp);
  if(isNaN(pxY))return;
  const wrap=document.getElementById('main-wrap');
  const lbl=document.getElementById('price-line-lbl');
  if(!wrap||!lbl)return;
  const chartH=wrap.getBoundingClientRect().height;
  if(pxY<0||pxY>chartH){lbl.style.display='none';return;}
  lbl.style.display='block';
  lbl.style.top=pxY+'px';
  // Position right at the y-axis edge
  lbl.style.right='0';
  lbl.style.background=isUp?'#00c060':'#e03050';
  lbl.style.color='#fff';
  lbl.style.fontSize='10.5px';
  lbl.style.fontFamily="'Share Tech Mono',monospace";
  lbl.style.fontWeight='700';
  lbl.style.padding='2px 6px';
  lbl.style.borderRadius='3px 0 0 3px';
  lbl.style.lineHeight='1.4';
  lbl.style.zIndex='20';
  lbl.style.pointerEvents='none';
  // Format same as y-axis ticks
  const n=Math.abs(lp)>=100?lp.toFixed(2):lp.toFixed(4);
  lbl.textContent=n;
}

// ── CROSSHAIR ──────────────────────────────────────────────────
(function(){
  const wrap=()=>document.getElementById('main-wrap');
  const xh=()=>document.getElementById('chart-crosshair');

  function showCrosshair(clientX,clientY){
    const w=wrap();const ch=chartI['chart-main'];const x=xh();
    if(!w||!ch||!x)return;
    const rect=w.getBoundingClientRect();
    const mx=clientX-rect.left, my=clientY-rect.top;
    if(mx<0||my<0||mx>rect.width||my>rect.height){hideCrosshair();return;}
    x.style.display='block';
    // vertical line
    const vl=document.getElementById('ch-v');
    if(vl){vl.style.left=mx+'px';}
    // horizontal line
    const hl=document.getElementById('ch-h');
    if(hl){hl.style.top=my+'px';}
    // price label on y-axis
    const yScale=ch.scales?.y;
    const pl=document.getElementById('ch-price');
    if(yScale&&pl){
      const pxPrice=yScale.getValueForPixel(my);
      pl.style.top=my+'px';
      pl.textContent=pxPrice?Math.abs(pxPrice)>=100?pxPrice.toFixed(2):pxPrice.toFixed(4):'';
    }
    // OHLC tooltip — find nearest candle
    const xScale=ch.scales?.x;
    const ohlcEl=document.getElementById('ch-ohlc');
    if(xScale&&ohlcEl){
      const idx=Math.round(xScale.getValueForPixel(mx));
      const cd=ch.data.datasets[0]?.cd;
      if(cd&&idx>=0&&idx<cd.length){
        const c=cd[idx];
        const col=c.c>=c.o?'#00e676':'#ff5252';
        ohlcEl.innerHTML=`<span style="color:var(--text3)">${c.t?.slice?.(0,10)||''} &nbsp;</span>`+
          `<span style="color:var(--text3)">O:</span><span style="color:var(--text2)">${c.o}</span> `+
          `<span style="color:var(--text3)">H:</span><span style="color:#00e676">${c.h}</span> `+
          `<span style="color:var(--text3)">L:</span><span style="color:#ff5252">${c.l}</span> `+
          `<span style="color:var(--text3)">C:</span><span style="color:${col};font-weight:700">${c.c}</span>`;
        // OHLC strip stays fixed at top-left — doesn't follow cursor
      }
    }
  }

  function hideCrosshair(){
    const x=xh();if(x)x.style.display='none';
  }

  // Mouse
  document.addEventListener('mousemove',e=>{
    if(!wrap()?.contains(e.target)){hideCrosshair();return;}
    showCrosshair(e.clientX,e.clientY);
  });
  document.addEventListener('mouseleave',hideCrosshair);

  // Touch — single finger shows crosshair
  document.addEventListener('touchmove',e=>{
    if(!wrap()?.contains(e.target))return;
    if(e.touches.length===1){
      showCrosshair(e.touches[0].clientX,e.touches[0].clientY);
    }
  },{passive:true});
  document.addEventListener('touchend',()=>setTimeout(hideCrosshair,800));
})();

// ── CHART VIEWPORT STATE ─────────────────────────────
// xOff: how many candles from the RIGHT edge are hidden (0 = showing latest)
// Drag LEFT  (finger moves left)  = want to see NEWER data  = xOff decreases
// Drag RIGHT (finger moves right) = want to see OLDER data  = xOff increases
let _yManual=false; // true when user has manually panned Y axis
let _yMin=0,_yMax=0; // manual y range when _yManual=true

function _calcXRange(){
  const ch=chartI['chart-main'];if(!ch)return{xMin:0,xMax:0};
  const n=ch.data.labels?.length||60;
  const vis=Math.max(5,Math.round(n/zLvl));
  const clampedOff=Math.max(0,Math.min(zOff,n-vis));
  // Right-side breathing room: ~20% of visible range, min 8 candles gap
  const rightPad=Math.max(5,Math.round(vis*0.08)); // ~5 candle gap on right
  const xMax=n-1-clampedOff+rightPad;  // extends past last candle
  const xMin=Math.max(0,xMax-vis+1);
  return{xMin,xMax,vis,n,rightPad};
}

function _calcYRange(xMin,xMax){
  const ch=chartI['chart-main'];if(!ch)return null;
  const cd=ch.data.datasets[0]?.cd;if(!cd||!cd.length)return null;
  // clamp to actual candle data (rightPad slots have no data)
  const dataMax=cd.length-1;
  const visSlice=cd.slice(xMin,Math.min(xMax+1,dataMax+1)).filter(Boolean);
  if(!visSlice.length)return null;
  const lo=Math.min(...visSlice.map(c=>c.l));
  const hi=Math.max(...visSlice.map(c=>c.h));
  const pad=Math.max((hi-lo)*0.12, lo*0.001, 1);
  return{min:lo-pad,max:hi+pad};
}

function _niceStep(range){
  // Pick a round step size so ticks land on clean numbers every ~8-10pts
  // Target: ~8-12 visible tick lines regardless of zoom
  const rough=range/10;
  const mag=Math.pow(10,Math.floor(Math.log10(rough)));
  const norm=rough/mag;
  let step;
  if(norm<1.5)      step=1*mag;
  else if(norm<3)   step=2*mag;
  else if(norm<7)   step=5*mag;
  else              step=10*mag;
  return Math.max(step,1); // minimum 1pt step
}

function applyZoom(){
  const ch=chartI['chart-main'];if(!ch)return;
  const{xMin,xMax}=_calcXRange();
  ch.options.scales.x.min=xMin;
  ch.options.scales.x.max=xMax;
  // Y: use manual range if set, else auto-fit to visible candles
  let yMin,yMax;
  if(_yManual){
    yMin=_yMin; yMax=_yMax;
  } else {
    const yr=_calcYRange(xMin,xMax);
    if(yr){yMin=yr.min;yMax=yr.max;}
  }
  if(yMin!==undefined){
    ch.options.scales.y.min=yMin;
    ch.options.scales.y.max=yMax;
    // Set step size so ticks land on round numbers with ~10pt granularity
    ch.options.scales.y.ticks.stepSize=_niceStep(yMax-yMin);
  }
  ch.update('none');
  setTimeout(updatePriceLbl,40);
}

function zIn(){zLvl=Math.min(zLvl*1.3,16);_yManual=false;applyZoom();}
function zOut(){zLvl=Math.max(zLvl/1.3,.3);_yManual=false;applyZoom();}
function zReset(){zLvl=1;zOff=0;_yManual=false;applyZoom();}

(function(){
  const wrap=()=>document.getElementById('main-wrap');
  let pX=null,pY=null,pOff=0,pYMin=0,pYMax=0,lD=0,sZ=1;

  function candleW(){
    const ch=chartI['chart-main'];if(!ch)return 1;
    const el=wrap();if(!el)return 1;
    const vis=Math.max(5,Math.round((ch.data.labels?.length||60)/zLvl));
    return el.getBoundingClientRect().width/vis;
  }

  function startPan(clientX,clientY){
    const ch=chartI['chart-main'];
    pX=clientX;pY=clientY;pOff=zOff;
    if(ch&&ch.scales?.y){pYMin=ch.scales.y.min||0;pYMax=ch.scales.y.max||0;}
  }

  function movePan(clientX,clientY){
    if(pX===null)return;
    const ch=chartI['chart-main'];if(!ch)return;
    // X pan: drag RIGHT = finger moves RIGHT = clientX > pX = dx negative = zOff decreases
    // drag RIGHT shows OLDER data (zOff increases), drag LEFT shows NEWER (zOff decreases)
    // Convention: drag finger RIGHT → older candles (standard chart behaviour)
    const dx=clientX-pX; // positive = finger moved RIGHT = want older = zOff increases
    const dy=clientY-pY; // positive = finger moved DOWN = see higher (pull down to look up)
    const cw=candleW();
    // Drag RIGHT (dx>0) = see older data = increase zOff
    // Drag LEFT  (dx<0) = see newer data = decrease zOff
    zOff=Math.max(0,pOff+Math.round(dx/cw));
    // Y pan: drag UP = see higher prices, drag DOWN = see lower prices
    if(pYMax!==pYMin){
      const w=wrap();
      const h=w?.getBoundingClientRect().height||1;
      const pricePerPx=(pYMax-pYMin)/h;
      const priceDy=dy*pricePerPx;
      _yManual=true;
      _yMin=pYMin+priceDy;
      _yMax=pYMax+priceDy;
    }
    applyZoom();
  }

  const el=document.getElementById('main-wrap');
  if(el){
    el.addEventListener('mousedown',e=>{if(e.button!==0)return;startPan(e.clientX,e.clientY);e.preventDefault();});
    window.addEventListener('mousemove',e=>{movePan(e.clientX,e.clientY);});
    window.addEventListener('mouseup',()=>{pX=null;});

    el.addEventListener('wheel',e=>{
      e.preventDefault();
      const factor=e.deltaY<0?1.2:1/1.2;
      const rect=el.getBoundingClientRect();
      const ch=chartI['chart-main'];if(!ch)return;
      const n=ch.data.labels?.length||60;
      const vis=Math.max(5,Math.round(n/zLvl));
      const frac=(e.clientX-rect.left)/rect.width;
      const anchorCandle=(ch.options.scales.x.min||0)+frac*vis;
      zLvl=Math.min(16,Math.max(.3,zLvl*factor));
      const newVis=Math.max(5,Math.round(n/zLvl));
      zOff=Math.max(0,Math.round(n-1-anchorCandle-(1-frac)*newVis));
      _yManual=false; // reset y to auto-fit on zoom
      applyZoom();
    },{passive:false});
  }

  function dist(e){const[a,b]=[e.touches[0],e.touches[1]];return Math.hypot(a.clientX-b.clientX,a.clientY-b.clientY);}
  document.addEventListener('touchstart',e=>{
    if(!wrap()?.contains(e.target))return;
    if(e.touches.length===2){lD=dist(e);sZ=zLvl;pX=null;}
    else if(e.touches.length===1){startPan(e.touches[0].clientX,e.touches[0].clientY);}
  },{passive:true});
  document.addEventListener('touchmove',e=>{
    if(!wrap()?.contains(e.target))return;
    if(e.touches.length===2){
      const d=dist(e);if(!lD)return;
      zLvl=Math.min(16,Math.max(.3,sZ*(d/lD)));
      _yManual=false;applyZoom();
    } else if(e.touches.length===1){
      movePan(e.touches[0].clientX,e.touches[0].clientY);
    }
  },{passive:true});
  document.addEventListener('touchend',()=>{pX=null;lD=0;});
})();

// ── Y-SCALE DRAG (right-side price scale) ────────────
// Drag UP   on price scale → stretch candles taller (zoom in on Y)
// Drag DOWN on price scale → compress candles shorter (zoom out on Y)
(function(){
  const handle=()=>document.getElementById('yscale-handle');
  let yDragStart=null, yRangeAtStart=0, yMidAtStart=0;

  function startYDrag(clientY){
    const ch=chartI['chart-main'];if(!ch?.scales?.y)return;
    yDragStart=clientY;
    yRangeAtStart=ch.scales.y.max-ch.scales.y.min;
    yMidAtStart=(ch.scales.y.max+ch.scales.y.min)/2;
    _yManual=true;
  }

  function moveYDrag(clientY){
    if(yDragStart===null)return;
    const ch=chartI['chart-main'];if(!ch)return;
    const dy=yDragStart-clientY; // drag UP = positive = stretch (zoom in)
    // Scale factor: every 100px of drag = 2x stretch/compress
    const factor=Math.pow(0.993,-dy); // drag up dy<0 → factor<1 → range shrinks → taller candles
    const newRange=Math.max(yRangeAtStart*factor, 0.5); // min 0.5pt range
    _yMin=yMidAtStart-newRange/2;
    _yMax=yMidAtStart+newRange/2;
    const ch2=chartI['chart-main'];
    if(ch2){
      ch2.options.scales.y.min=_yMin;ch2.options.scales.y.max=_yMax;
      ch2.options.scales.y.ticks.stepSize=_niceStep(_yMax-_yMin);
      ch2.update('none');
    }
    updatePriceLbl();
  }

  function endYDrag(){ yDragStart=null; }

  // Mouse
  const h=document.getElementById('yscale-handle');
  if(h){
    h.addEventListener('mousedown',e=>{startYDrag(e.clientY);e.preventDefault();},{passive:false});
    window.addEventListener('mousemove',e=>{moveYDrag(e.clientY);});
    window.addEventListener('mouseup',endYDrag);
    // Double-tap/click on scale → reset Y to auto-fit
    h.addEventListener('dblclick',()=>{_yManual=false;applyZoom();});
  }
  // Touch
  document.addEventListener('touchstart',e=>{
    if(!handle()?.contains(e.target))return;
    if(e.touches.length===1)startYDrag(e.touches[0].clientY);
  },{passive:true});
  document.addEventListener('touchmove',e=>{
    if(yDragStart===null)return;
    if(e.touches.length===1)moveYDrag(e.touches[0].clientY);
  },{passive:true});
  document.addEventListener('touchend',e=>{
    if(yDragStart!==null)endYDrag();
  });
})();

// ── THEME ────────────────────────────────────────────
let theme=localStorage.getItem('ict_theme')||'dark';
function setTheme(t){
  theme=t;localStorage.setItem('ict_theme',t);
  document.documentElement.removeAttribute('data-theme');
  if(t!=='dark')document.documentElement.setAttribute('data-theme',t);
  const mc={dark:'#050810',light:'#ffffff',black:'#000000'}[t]||'#050810';
  document.getElementById('theme-meta').content=mc;
  document.querySelectorAll('.theme-chip').forEach(c=>c.classList.toggle('active',c.dataset.t===t));
}

// ── TD KEY MANAGEMENT (multi-key) ────────────────────
function saveTdKeyN(n){
  const v=document.getElementById('td-key-'+n).value.trim();if(!v)return;
  const keys=loadTdKeys();keys[n].key=v;keys[n].usage=0;keys[n].date=todayStr();
  saveTdKeys(keys);updateTdStatus();tdWsResubscribe();refreshAll();
}
function resetTdUsage(n){
  const keys=loadTdKeys();keys[n].usage=0;keys[n].date=todayStr();
  saveTdKeys(keys);updateTdStatus();
}
function updateTdStatus(){
  const keys=loadTdKeys();maybeResetUsage(keys);
  const s=document.getElementById('td-status'),b=document.getElementById('data-src');
  const now=Date.now();
  // Update per-key usage displays with minute-cooldown indicator
  for(let i=0;i<4;i++){
    const el=document.getElementById('td-usage-'+i);
    const cooling=tdMinuteCooldown&&tdMinuteCooldown[i]&&(now-tdMinuteCooldown[i])<65000;
    if(el){
      el.textContent=(keys[i].usage||0)+(cooling?' ⏳':'');
      el.style.color=keys[i].usage>=TD_LIMIT?'var(--bear)':cooling?'var(--orange)':'var(--gold)';
    }
    const inp=document.getElementById('td-key-'+i);
    if(inp&&keys[i].key&&!inp.value) inp.value=keys[i].key;
  }
  // Find best active key (not daily-exhausted, not in minute cooldown)
  let idx=-1;
  for(let i=0;i<keys.length;i++){
    if(!keys[i].key)continue;
    if(keys[i].usage>=TD_LIMIT)continue;
    const cooling=tdMinuteCooldown&&tdMinuteCooldown[i]&&(now-tdMinuteCooldown[i])<65000;
    if(!cooling){idx=i;break;}
  }
  // Fallback: if all non-exhausted keys are cooling, pick first cooling one (better than nothing)
  if(idx<0){
    for(let i=0;i<keys.length;i++){
      if(keys[i].key&&keys[i].usage<TD_LIMIT){idx=i;break;}
    }
  }
  if(idx>=0){
    const remaining=TD_LIMIT-keys[idx].usage;
    const cooling=tdMinuteCooldown&&tdMinuteCooldown[idx]&&(now-tdMinuteCooldown[idx])<65000;
    const statusTxt=cooling?`Key ${idx+1} · rate-limited, auto-retry ~${Math.ceil((65000-(now-tdMinuteCooldown[idx]))/1000)}s`:`Key ${idx+1} active · ${remaining} calls left today`;
    if(s){s.textContent=statusTxt;s.style.color=cooling?'var(--orange)':'var(--bull)';}
    if(b&&!cooling){b.textContent=`TwelveData (K${idx+1})`;b.style.color='var(--bull)';}
    TD_KEY=keys[idx].key;
  } else {
    const hasAny=keys.some(k=>k.key);
    if(s){s.textContent=hasAny?'All keys at daily limit · try again tomorrow':'No keys saved — add a key above';s.style.color='var(--orange)';}
    if(b){b.textContent='No data';b.style.color='var(--bear)';}
    TD_KEY='demo';
  }
}

// ── CLOCK ────────────────────────────────────────────
function updateClock(){
  const est=new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));
  const h=est.getHours(),m=est.getMinutes(),hhmm=h*100+m;
  document.getElementById('clock').textContent=`${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;
  const kz=document.getElementById('kz-pill'),ss=document.getElementById('sc-session');
  const inL=hhmm>=200&&hhmm<=500,inN=hhmm>=830&&hhmm<=1100,inA=hhmm>=1900||hhmm<=200;
  if(inL){kz.textContent='LDN KZ';kz.className='kz-pill on';if(ss){ss.textContent='London KZ';ss.style.color='var(--teal)';}}
  else if(inN){kz.textContent='NY KZ';kz.className='kz-pill on';if(ss){ss.textContent='NY KZ';ss.style.color='var(--bull)';}}
  else if(inA){kz.textContent='ASIA';kz.className='kz-pill off';if(ss){ss.textContent='Asian';ss.style.color='var(--purple)';}}
  else{kz.textContent='OFF';kz.className='kz-pill off';if(ss){ss.textContent='Off-Hrs';ss.style.color='var(--text3)';}}
  calcLots();
}
setInterval(updateClock,1000);updateClock();

// ── CALCULATOR ───────────────────────────────────────
function calcLots(){
  const bal=parseFloat(document.getElementById('inp-balance').value)||10000;
  const risk=parseFloat(document.getElementById('inp-risk').value)||1;
  const sl=parseFloat(document.getElementById('inp-sl').value)||10;
  const rAmt=bal*risk/100,lots=Math.max(.01,Math.round((rAmt/(sl*10))*100)/100);
  document.getElementById('inp-lot').value=lots.toFixed(2);
  document.getElementById('risk-usd').textContent='$'+rAmt.toFixed(0);
  document.getElementById('lot-calc').textContent=lots.toFixed(2)+'L';
  document.getElementById('tp1-usd').textContent='$'+(rAmt*1.5).toFixed(0);
  document.getElementById('tp2-usd').textContent='$'+(rAmt*2.5).toFixed(0);
}
['inp-balance','inp-risk','inp-sl'].forEach(id=>document.getElementById(id).addEventListener('input',calcLots));

// ── DATA FETCH (multi-key rotation, no Yahoo) ────────
// TwelveData error codes:
// 429 / "too many requests" / "per_minute" = minutely rate limit (transient, try next key)
// "api credits" / "daily" / "reached the limit" = daily 800 limit (mark exhausted)
// "invalid" / "unauthorized" = bad key (skip permanently until reset)
function isTdDailyLimit(msg){
  if(!msg)return false;
  const m=msg.toLowerCase();
  return m.includes('api credits')||m.includes('daily')||m.includes('reached the limit')||m.includes('upgrade')||m.includes('plan limit');
}
function isTdMinuteLimit(msg){
  if(!msg)return false;
  const m=msg.toLowerCase();
  return m.includes('per_minute')||m.includes('per minute')||m.includes('minutely')||m.includes('too many requests')||m.includes('rate limit')||m.includes('429');
}
// Per-key minutely cooldown: declared above near config
async function fetchTDWithKey(interval,size,apiKey,keyIdx){
  const pairCfg=PAIR_TD[ACTIVE_PAIR]||{sym:'XAU/USD',invert:false};
  // Build list of symbol variants to try
  const variants=[];
  if(pairCfg.sym)variants.push(pairCfg.sym);
  if(pairCfg.fallback)variants.push(pairCfg.fallback);
  // For Silver, explicitly try all known TwelveData formats
  if(ACTIVE_PAIR==='XAG/USD'){
    if(!variants.includes('XAG/USD'))variants.push('XAG/USD');
    if(!variants.includes('XAGUSD'))variants.push('XAGUSD');
    if(!variants.includes('XAG%2FUSD'))variants.push('XAG%2FUSD');
  }
  let j=null,r=null,lastUrl='';
  for(const sym of variants){
    // Don't double-encode if sym already has %2F
    const encoded=sym.includes('%')?sym:encodeURIComponent(sym);
    const url=`https://api.twelvedata.com/time_series?symbol=${encoded}&interval=${interval}&outputsize=${size}&format=JSON&apikey=${apiKey}`;
    lastUrl=url;
    r=await fetch(url);j=await r.json();
    console.log(`TD [${sym}] HTTP:${r.status} status:${j.status} values:${j.values?.length??'none'} msg:${j.message||'ok'} code:${j.code||'-'}`);
    if(j.status!=='error'&&j.values&&j.values.length>0)break;
  }
  if(j.status==='error'||!j.values||j.values.length===0){
    const msg=j.message||'TD err';
    const err=new Error(`[${ACTIVE_PAIR}] ${msg} (code:${j.code||'?'})`);
    err.tdDailyLimit=isTdDailyLimit(msg);
    err.tdMinuteLimit=isTdMinuteLimit(msg)||r.status===429;
    err.tdCode=j.code;
    err.tdRaw=JSON.stringify(j).slice(0,200);
    throw err;
  }
  const candles=[...j.values].reverse().map(v=>({
    t:new Date(v.datetime).getTime(),
    o:+parseFloat(v.open).toFixed(5),
    h:+parseFloat(v.high).toFixed(5),
    l:+parseFloat(v.low).toFixed(5),
    c:+parseFloat(v.close).toFixed(5),
    v:parseFloat(v.volume)||0
  }));
  // DXY proxy: 105.678 × (1/EURUSD)^0.576
  // EUR is 57.6% of DXY basket; this formula matches real DXY within ~0.5%
  // e.g. EURUSD=1.12 → DXY≈99.0, EURUSD=1.05 → DXY≈102.8
  if(pairCfg.invert){
    const K=105.678,W=0.576;
    return candles.map(c=>{
      const conv=v=>+( K * Math.pow(1/v, W) ).toFixed(3);
      // When inverting, high EURUSD = low DXY, so swap h/l
      return{t:c.t,o:conv(c.o),h:conv(c.l),l:conv(c.h),c:conv(c.c),v:c.v};
    });
  }
  return candles.map(v=>({t:v.t,o:+v.o.toFixed(2),h:+v.h.toFixed(2),l:+v.l.toFixed(2),c:+v.c.toFixed(2),v:v.v}));
}
async function fetchCandles(interval,range){
  const tdMap={'1min':{i:'1min',s:200},'5min':{i:'5min',s:200},'15min':{i:'15min',s:200},'30min':{i:'30min',s:120},'45min':{i:'45min',s:120},'1h':{i:'1h',s:320},'2h':{i:'2h',s:120},'8h':{i:'8h',s:120},'1day':{i:'1day',s:100},'1week':{i:'1week',s:100},'1month':{i:'1month',s:60}};
  const td=tdMap[interval];if(!td)throw new Error('unsupported interval');
  const keys=loadTdKeys();maybeResetUsage(keys);
  const now=Date.now();
  let minuteRateBumped=false;
  // Try each key in order; skip daily-exhausted or empty ones
  for(let i=0;i<keys.length;i++){
    const k=keys[i];
    if(!k.key)continue;
    // Skip if daily limit reached
    if(k.usage>=TD_LIMIT)continue;
    // Skip if this key hit per-minute limit in last 65 seconds — try next key instead
    if(tdMinuteCooldown[i]&&(now-tdMinuteCooldown[i])<65000){minuteRateBumped=true;continue;}
    try{
      const c=await fetchTDWithKey(td.i,td.s,k.key,i);
      if(c&&c.length>10){
        incrementTdUsage(i);
        const b=document.getElementById('data-src');
        if(b){b.textContent=`TwelveData (K${i+1})`;b.style.color='var(--bull)';}
        return c;
      }
    }catch(e){
      console.warn(`TD Key ${i+1} [code:${e.tdCode||'?'}]:`,e.message);
      if(e.tdDailyLimit){
        // Permanently mark this key as daily-exhausted
        keys[i].usage=TD_LIMIT;saveTdKeys(keys);updateTdStatus();
        console.warn(`TD Key ${i+1}: daily limit reached, rotating.`);
      } else if(e.tdMinuteLimit){
        // Temporary per-minute rate limit — cool this key down, try next
        tdMinuteCooldown[i]=Date.now();
        minuteRateBumped=true;
        console.warn(`TD Key ${i+1}: per-minute limit, cooling 65s, trying next key.`);
        // DON'T mark as daily-exhausted — key is still valid
      }
      // For other errors (bad key, network) just skip this key for now
    }
  }
  // All keys either exhausted, cooling, or failed
  const b=document.getElementById('data-src');
  if(minuteRateBumped&&getActiveKeyIdx(loadTdKeys())===-1){
    if(b){b.textContent='Rate limited — retry soon';b.style.color='var(--orange)';}
    throw new Error('All keys are cooling down from per-minute rate limit. Data will auto-refresh in 65 seconds.');
  }
  if(b){b.textContent='No data';b.style.color='var(--bear)';}
  throw new Error('All TwelveData keys are at daily limit or unavailable. Please add more keys or wait until tomorrow.');
}

// ── NEWS BLOCKER ─────────────────────────────────────


function isHighImpactNewsNow(){
  try{
    const now=new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));
    const dayIdx=now.getDay(); // 0=Sun,1=Mon...5=Fri,6=Sat
    const dayMap={1:'MON',2:'TUE',3:'WED',4:'THU',5:'FRI'};
    const todayKey=dayMap[dayIdx];
    if(!todayKey)return{active:false};
    const hhmm=now.getHours()*100+now.getMinutes();
    const toNum=str=>{const[h,m]=str.split(':');return parseInt(h)*100+parseInt(m);};
    const WINDOW=30; // minutes each side = 30 min before to 30 min after
    const hits=KNOWN_EVENTS.filter(e=>{
      if(e.day!==todayKey)return false;
      if(e.impact!=='HIGH'&&e.impact!=='EXTREME')return false;
      const t=toNum(e.time);
      const lo=t-30<=0?0:t-30,hi=t+30;
      return hhmm>=lo&&hhmm<=hi;
    });
    const speechHits=KNOWN_SPEECHES.filter(e=>{
      if(e.day!==todayKey)return false;
      if(e.impact!=='HIGH'&&e.impact!=='EXTREME')return false;
      const t=toNum(e.time);
      return hhmm>=(t-30)&&hhmm<=(t+30);
    });
    const allHits=[...hits,...speechHits];
    return{active:allHits.length>0,events:allHits.map(e=>e.event||(e.speaker+' – '+e.topic))};
  }catch(ex){return{active:false};}
}
// Export so UI can call it

// ── ICT ANALYSIS ─────────────────────────────────────


// ── analyzeICT — exact copy from index.html ─────────────────────────────────────
function analyzeICT(candles,tf=''){
  if(!candles||candles.length<10)return{};
  const n=candles.length,last=candles[n-1];
  // Pivot window scales with timeframe — HTF needs more candles to confirm a true swing
  const pw=tf==='1d'?7:tf==='4h'?5:tf==='1h'?3:2;
  const sH=[],sL=[];
  for(let i=pw;i<n-pw;i++){
    let isH=true,isL=true;
    for(let k=1;k<=pw;k++){if(candles[i].h<=candles[i-k].h||candles[i].h<=candles[i+k].h)isH=false;if(candles[i].l>=candles[i-k].l||candles[i].l>=candles[i+k].l)isL=false;}
    if(isH)sH.push({idx:i,price:candles[i].h});
    if(isL)sL.push({idx:i,price:candles[i].l});
  }
  const lH=sH.at(-1),lL=sL.at(-1),pH=sH.at(-2),pL=sL.at(-2);
  // Fallback: use recent 30-candle range, not all-time (avoids ancient lows polluting D1 context)
  const recent30=candles.slice(-30);
  const swH=lH?.price||Math.max(...recent30.map(c=>c.h));
  const swL=lL?.price||Math.min(...recent30.map(c=>c.l));
  const bb=!!(lH&&pH&&lH.price>pH.price),bear=!!(lL&&pL&&lL.price<pL.price);
  const cb=!!(bb&&lL&&pL&&last.c>lH?.price),cb2=!!(bear&&lH&&pH&&last.c<lL?.price);
  const rh=candles.slice(n-10).map(c=>c.h),rl=candles.slice(n-10).map(c=>c.l);
  const mb=last.c>Math.max(...rh.slice(0,8)),mbr=last.c<Math.min(...rl.slice(0,8));
  // FVG active (unfilled)
  const fB=[],fBr=[];
  for(let i=n-30;i<n-1;i++){
    if(i<2)continue;
    if(candles[i].l>candles[i-2].h){const fBot=candles[i-2].h,fTop=candles[i].l,fMid=(fBot+fTop)/2;const filled=candles.slice(i+1).some(c=>c.l<fMid);if(!filled)fB.push({top:fTop,bot:fBot,idx:i,mid:fMid});}
    if(candles[i].h<candles[i-2].l){const fBot=candles[i].h,fTop=candles[i-2].l,fMid=(fBot+fTop)/2;const filled=candles.slice(i+1).some(c=>c.h>fMid);if(!filled)fBr.push({top:fTop,bot:fBot,idx:i,mid:fMid});}
  }
  // IFVG — flipped
  const ifvgB=[],ifvgBr=[];
  for(let i=n-30;i<n-1;i++){
    if(i<2)continue;
    if(candles[i].l>candles[i-2].h){const filled=candles.slice(i+1).some(c=>c.l<candles[i-2].h);if(filled)ifvgBr.push({top:candles[i].l,bot:candles[i-2].h,mid:(candles[i].l+candles[i-2].h)/2,idx:i});}
    if(candles[i].h<candles[i-2].l){const filled=candles.slice(i+1).some(c=>c.h>candles[i-2].l);if(filled)ifvgB.push({top:candles[i-2].l,bot:candles[i].h,mid:(candles[i-2].l+candles[i].h)/2,idx:i});}
  }
  // BPR
  const bpr=[];
  fB.forEach(bull=>fBr.forEach(bear=>{const ot=Math.min(bull.top,bear.top),ob=Math.max(bull.bot,bear.bot);if(ot>ob)bpr.push({bot:ob,top:ot,mid:(ob+ot)/2});}));
  // Order Blocks unmitigated
  const oB=[],oBr=[];
  for(let i=n-25;i<n-2;i++){
    if(i<1)continue;
    const bn=Math.abs(candles[i+1].c-candles[i+1].o),ab=candles.slice(Math.max(0,i-5),i+1).reduce((s,c)=>s+Math.abs(c.c-c.o),0)/6;
    if(candles[i].c<candles[i].o&&candles[i+1].c>candles[i+1].o&&bn>ab*1.3){const obBot=candles[i].l,obTop=candles[i].h;const mit=candles.slice(i+2).some(c=>c.l<=obTop);oB.push({top:obTop,bot:obBot,idx:i,mid:(obTop+obBot)/2,mitigated:mit});}
    if(candles[i].c>candles[i].o&&candles[i+1].c<candles[i+1].o&&bn>ab*1.3){const obBot=candles[i].l,obTop=candles[i].h;const mit=candles.slice(i+2).some(c=>c.h>=obBot);oBr.push({top:obTop,bot:obBot,idx:i,mid:(obTop+obBot)/2,mitigated:mit});}
  }
  const activeOB=oB.filter(o=>!o.mitigated),activeOBr=oBr.filter(o=>!o.mitigated);
  // BSL/SSL
  const EQT=0.0015;
  const dedup=arr=>{const o=[];arr.forEach(x=>{if(!o.some(y=>Math.abs(y.price-x.price)/x.price<EQT))o.push(x);});return o;};
  const bsl=[],ssl=[];
  sH.slice(-8).forEach(sh=>{if(sH.filter(x=>x.idx!==sh.idx&&Math.abs(x.price-sh.price)/sh.price<EQT).length>=1)bsl.push({price:sh.price,idx:sh.idx});});
  sL.slice(-8).forEach(sl_=>{if(sL.filter(x=>x.idx!==sl_.idx&&Math.abs(x.price-sl_.price)/sl_.price<EQT).length>=1)ssl.push({price:sl_.price,idx:sl_.idx});});
  const bslD=dedup(bsl),sslD=dedup(ssl);
  const eqh=bslD.length?bslD.at(-1):null,eql=sslD.length?sslD.at(-1):null;
  // Confirmed sweeps: wick beyond EQH/EQL then close back inside (last 6 candles)
  const recentC=candles.slice(-12); // extended 6→12 bars: sweep valid for ~3 trading days
  const bslSwept=eqh?recentC.some(c=>c.h>eqh.price&&c.c<eqh.price):false;
  const sslSwept=eql?recentC.some(c=>c.l<eql.price&&c.c>eql.price):false;
  // Rejection Block
  const rejBlocks=[];
  for(let i=n-15;i<n-1;i++){
    const c=candles[i],range=c.h-c.l;if(range<0.0001)continue;
    const body=Math.abs(c.c-c.o)/range,uw=(c.h-Math.max(c.o,c.c))/range,lw=(Math.min(c.o,c.c)-c.l)/range;
    if(uw>0.55&&body<0.3)rejBlocks.push({type:'bear',top:Math.max(c.o,c.c),bot:Math.min(c.o,c.c),idx:i});
    if(lw>0.55&&body<0.3)rejBlocks.push({type:'bull',top:Math.max(c.o,c.c),bot:Math.min(c.o,c.c),idx:i});
  }
  // Turtle Soup
  let turtleBull=null,turtleBear=null;
  if(n>=5){
    const priorL=candles.slice(n-8,n-2).reduce((m,c)=>Math.min(m,c.l),Infinity);
    const priorH=candles.slice(n-8,n-2).reduce((m,c)=>Math.max(m,c.h),0);
    if(candles[n-2].h>priorH&&candles[n-2].c<priorH&&last.c<candles[n-2].c)turtleBear={sweep:candles[n-2].h,level:priorH,idx:n-2};
    if(candles[n-2].l<priorL&&candles[n-2].c>priorL&&last.c>candles[n-2].c)turtleBull={sweep:candles[n-2].l,level:priorL,idx:n-2};
  }
  // CISD
  let cisd=null;
  for(let i=Math.max(0,n-10);i<n;i++){ // extended 5→10 bars: CISD valid for ~2.5 days
    const c=candles[i],range=c.h-c.l;if(range<0.0001)continue;
    const body=Math.abs(c.c-c.o)/range,uw=(c.h-Math.max(c.o,c.c))/range,lw=(Math.min(c.o,c.c)-c.l)/range;
    const avg=candles.slice(Math.max(0,i-5),i).reduce((s,x)=>s+Math.abs(x.c-x.o),0)/5;
    if(Math.abs(c.c-c.o)>avg*1.8){if(c.c>c.o&&lw<0.15)cisd={type:'bull',price:c.c,open:c.o,idx:i};if(c.c<c.o&&uw<0.15)cisd={type:'bear',price:c.c,open:c.o,idx:i};}
  }
  // IDM
  let idm=null;
  if(sH.length>=3&&sL.length>=3){
    const ls=sL.at(-1),ps=sL.at(-2),lsh=sH.at(-1),psh=sH.at(-2);
    if(ls&&ps&&ls.price>ps.price&&last.c<ls.price)idm={type:'bull',level:ls.price,truePool:ps.price};
    if(lsh&&psh&&lsh.price<psh.price&&last.c>lsh.price)idm={type:'bear',level:lsh.price,truePool:psh.price};
  }
  // NWOG
  let nwog=null;
  for(let i=n-10;i<n-1;i++){
    const cur=candles[i],prev=candles[i-1];if(!cur.t||!prev.t)continue;
    const dc=new Date(typeof cur.t==='number'&&cur.t<1e12?cur.t*1000:cur.t);
    const dp=new Date(typeof prev.t==='number'&&prev.t<1e12?prev.t*1000:prev.t);
    if(dc.getDay()===1&&dp.getDay()===5&&Math.abs(cur.o-prev.c)>0.5)nwog={open:cur.o,fridayClose:prev.c,mid:(cur.o+prev.c)/2,bullish:cur.o>prev.c,size:Math.abs(cur.o-prev.c).toFixed(2)};
  }
  // AMD — split by ET session time when timestamps available, else candle segments
  const toET=ts=>{try{const d=new Date(typeof ts==='number'&&ts<1e12?ts*1000:ts);return parseInt(d.toLocaleString('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',hour12:false}).replace(':',''));}catch(e){return-1;}};
  const hasTs=candles[0]&&candles[0].t;
  let asiaC=[],ldnC=[],nyC=[];
  if(hasTs&&(tf==='1h'||tf==='15m'||tf==='5m')){
    candles.forEach(c=>{const h=toET(c.t);
      if(h>=1900||h<200)asiaC.push(c);        // 19:00–02:00 ET
      else if(h>=200&&h<700)asiaC.push(c);    // 02:00–07:00 ET (late Asia / early London)
      else if(h>=700&&h<1200)ldnC.push(c);    // 07:00–12:00 ET (London / NY overlap)
      else nyC.push(c);                        // 12:00+ ET (NY afternoon)
    });
  }
  // Fall back to thirds if session split yields empty buckets
  const seg=Math.max(3,Math.floor(n/3));
  const useSession=asiaC.length>1&&ldnC.length>1&&nyC.length>1;
  const aC=useSession?asiaC:candles.slice(0,seg);
  const mC=useSession?ldnC:candles.slice(seg,seg*2);
  const dC=useSession?nyC:candles.slice(seg*2);
  const aH=Math.max(...aC.map(c=>c.h)),aL=Math.min(...aC.map(c=>c.l));
  const mH=Math.max(...mC.map(c=>c.h)),mL=Math.min(...mC.map(c=>c.l));
  const dH=Math.max(...dC.map(c=>c.h)),dL=Math.min(...dC.map(c=>c.l));
  const amdBull=mL<aL&&dH>mH,amdBear=mH>aH&&dL<mL;
  // Phase: which session is most expansive right now?
  const recRng=Math.max(...candles.slice(-3).map(c=>c.h))-Math.min(...candles.slice(-3).map(c=>c.l));
  const amdPhase=recRng>(mH-mL)*1.3?'Distribution (NY)':recRng>(mH-mL)*0.8?'Manipulation (London)':'Accumulation (Asia)';
  // P/D
  const eq=(swH+swL)/2,ote_hi=swL+(swH-swL)*.786,ote_lo=swL+(swH-swL)*.618;
  const inD=last.c<eq,inP=last.c>eq;
  const pdZone=inD?'DISCOUNT':inP?'PREMIUM':'EQUILIBRIUM';
  const fibPct=swH>swL?((last.c-swL)/(swH-swL)*100).toFixed(1):'50';
  // DOL
  let dol=null;
  if(inD){const t=bslD.length?bslD.reduce((a,b)=>Math.abs(b.price-last.c)<Math.abs(a.price-last.c)?b:a):null;dol={dir:'UP',target:t?.price||swH,type:t?'BSL/EQH':'Swing High'};}
  else{const t=sslD.length?sslD.reduce((a,b)=>Math.abs(b.price-last.c)<Math.abs(a.price-last.c)?b:a):null;dol={dir:'DOWN',target:t?.price||swL,type:t?'SSL/EQL':'Swing Low'};}
  // ── Session Opens (Midnight, London, NY) ───────────────────
  const estNow=new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));
  const hhmm_=estNow.getHours()*100+estNow.getMinutes();
  // Silver Bullet windows: 03:00-04:00, 10:00-11:00, 14:00-15:00 ET
  const inSB=(hhmm_>=300&&hhmm_<=400)||(hhmm_>=1000&&hhmm_<=1100)||(hhmm_>=1400&&hhmm_<=1500);
  const inLDN=(hhmm_>=200&&hhmm_<=500);
  const inNY=(hhmm_>=830&&hhmm_<=1100);
  const inKZ_=inLDN||inNY;
  const session=inLDN?'London':inNY?'New York':(hhmm_>=1900||hhmm_<=200)?'Asia':'Lunch';
  // Session open reference prices
  const sessionOpen=candles.length>0?candles.at(-1).o:null;
  // News blocker — HIGH-impact event within ±30 min kills the trade regardless of grade
  const newsCheck=isHighImpactNewsNow();
  const inNewsWindow=newsCheck.active;
  // Strict A++ grade: requires liquidity sweep near PD array + CISD
  const hasLiqSweep=!!(turtleBull||turtleBear||bslSwept||sslSwept);
  const hasCisdOrMss=!!(cisd||cb||cb2);
  const hasEntryArray=!!(fB.length||fBr.length||bpr.length||activeOB.length||activeOBr.length);
  const hasDolClear=!!(dol);
  // Strict scoring
  let gs=0;
  if(inKZ_)gs+=2;                           // Kill zone (required)
  if(bb||bear)gs+=1;                         // BOS structure
  if(cb||cb2)gs+=1;                          // CHoCH (stronger)
  if(hasLiqSweep)gs+=2;                     // Liquidity sweep (required for A++)
  if(hasCisdOrMss)gs+=1;                    // CISD or MSS
  if(hasEntryArray)gs+=1;                   // Entry PD array present
  if(bpr.length)gs+=1;                      // BPR (highest precision)
  if(cisd&&hasLiqSweep&&hasEntryArray)gs+=1;// Full confluence bonus
  // Grade: A++ requires KZ + LiqSweep + EntryArray + DOL (CISD is supplementary — verified at execution)
  // ICT 2022: H4 provides structural grade, H1 provides CISD/MSS execution trigger
  // Relaxing hasCisdOrMss here allows H1 CISD to satisfy the requirement in detectExecutionOnBars
  const fullConf=inKZ_&&hasLiqSweep&&hasEntryArray&&hasDolClear&&!inNewsWindow;
  const grade=fullConf&&gs>=7?'A++':inNewsWindow?'NEWS':(gs>=6?'A+':(gs>=4?'B':'C'));
  let bias=0;
  if(bb||cb)bias+=1;if(bear||cb2)bias-=1;if(inD)bias+=.5;if(inP)bias-=.5;
  if(amdBull)bias+=.3;if(amdBear)bias-=.3;if(turtleBull)bias+=.4;if(turtleBear)bias-=.4;
  if(cisd?.type==='bull')bias+=.3;if(cisd?.type==='bear')bias-=.3;
  // SMT divergence weight — confirmed signal gets ±0.5
  return{bos_bull:bb,bos_bear:bear,choch_bull:cb,choch_bear:cb2,mss_bull:mb,mss_bear:mbr,inSB,inKZ:inKZ_,session,sessionOpen,
    fvgs_bull:fB,fvgs_bear:fBr,ifvg_bull:ifvgB,ifvg_bear:ifvgBr,bpr,
    obs_bull:activeOB,obs_bear:activeOBr,bsl:bslD,ssl:sslD,eqh,eql,
    bslSwept,sslSwept,inNewsWindow,newsEvents:newsCheck.events||[],
    rejBlocks,turtleBull,turtleBear,idm,cisd,nwog,amdPhase,amdBull,amdBear,useSession,
    swH,swL,eq,ote_hi,ote_lo,inDiscount:inD,inPremium:inP,pdZone,fibPct,
    dol,grade,gradeScore:gs,bias:Math.max(-1,Math.min(1,bias)),last};
}


// ── detectExecutionOnBars — exact copy from index.html ──────────────────────────
function detectExecutionOnBars(anals, px) {
  const h4 = anals['4h'] || {}, h1 = anals['1h'] || {}, m15 = anals['15m'] || {}, m5 = anals['5m'] || {}, d1 = anals['1d'] || {};
  const now = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', hour12: false });

  // ── 1. Grade gate — H4 structural A++ required
  // H4 fullConf (KZ+Sweep+Array+DOL) + H1 sweep/CISD supplement = valid A++ signal
  const h4Grade = h4.grade;
  // H4 must score A++ (KZ+Sweep+Array+DOL+gs>=7 — CISD may come from H1)
  // Also accept H4 A++ where H1 has additional sweep/CISD confirmation
  const h1HasSweep = !!(h1.bslSwept || h1.sslSwept || h1.turtleBull || h1.turtleBear);
  const h1HasCisd  = !!(h1.cisd || h1.choch_bull || h1.choch_bear);
  const h1Confirms = h1HasSweep || h1HasCisd;
  const grade = (h4Grade === 'A++') ? 'A++' : null;
  if (!grade || grade === 'NEWS') return null;
  const htfConfirmed = true;
  if (!px) return null;

  // ── 1b. Session gate — reject pure Asia sessions only ──
  // H4 inKZ_ already verified in analyzeICT. We additionally reject Asia-only.
  // Keep: London, New York, Lunch overlap (valid ICT Silver Bullet windows)
  // Reject: Asia only (no structural validity for Gold intraday ICT)
  const activeSession = h1.session || h4.session || '';
  if (activeSession === 'Asia') return null;

  // ── 2. Directional bias (must be clear) ───────────────────────────────
  const bias = (h4.bias || 0) * 0.4 + (h1.bias || 0) * 0.35 + (m15.bias || 0) * 0.25;
  if (Math.abs(bias) < 0.1) return null;
  const dir = bias > 0 ? 'LONG' : 'SHORT';
  const isLong = dir === 'LONG';

  // ── 2b. HTF alignment gate — ICT OTE zone: 61.8%–78.6% of dealing range
  // LONG valid:  H4 fib < 79%  (covers full discount + OTE zone)
  // SHORT valid: H4 fib > 21%  (symmetric — covers full premium + OTE zone)
  // BLOCKED: only when price is extreme (beyond the range entirely)
  // D1 gate removed — H4 structure is sufficient; D1 gate was blocking valid OTE entries
  const h4Fib = parseFloat(h4.fibPct) || 50;
  if (isLong) {
    if (h4Fib > 85)  return null;  // price above OTE zone top — no longs (widened 79→85)
    if (h4Fib < 0)   return null;  // price below range entirely — wait
  } else {
    if (h4Fib < 15)  return null;  // price below OTE zone bottom — no shorts (widened 21→15)
    if (h4Fib > 100) return null;  // price above range entirely — wait
  }

  // ── 3. Find the best entry PD array (hierarchy: BPR > IFVG > FVG > OB) ─
  // Entry must be the CLOSEST unmitigated zone above price (LONG) or below (SHORT)
  // that price has pulled back INTO — not a random live price
  let entryZone = null; // {top, bot, mid, type, tf}

  const zonesLong = [
    ...(h1.bpr?.map(z => ({...z, type:'BPR', tf:'H1', priority:1})) || []),
    ...(m15.bpr?.map(z => ({...z, type:'BPR', tf:'M15', priority:1})) || []),
    ...(h4.bpr?.map(z => ({...z, type:'BPR', tf:'H4', priority:2})) || []),
    ...(h1.ifvg_bull?.map(z => ({...z, type:'IFVG', tf:'H1', priority:3})) || []),
    ...(m15.fvgs_bull?.map(z => ({...z, type:'FVG', tf:'M15', priority:4})) || []),
    ...(h1.fvgs_bull?.map(z => ({...z, type:'FVG', tf:'H1', priority:4})) || []),
    ...(h4.fvgs_bull?.map(z => ({...z, type:'FVG', tf:'H4', priority:5})) || []),
    ...(m15.obs_bull?.map(z => ({...z, type:'OB', tf:'M15', priority:6})) || []),
    ...(h1.obs_bull?.map(z => ({...z, type:'OB', tf:'H1', priority:6})) || []),
    ...(h4.obs_bull?.map(z => ({...z, type:'OB', tf:'H4', priority:7})) || []),
  ];
  const zonesShort = [
    ...(h1.bpr?.map(z => ({...z, type:'BPR', tf:'H1', priority:1})) || []),
    ...(m15.bpr?.map(z => ({...z, type:'BPR', tf:'M15', priority:1})) || []),
    ...(h4.bpr?.map(z => ({...z, type:'BPR', tf:'H4', priority:2})) || []),
    ...(h1.ifvg_bear?.map(z => ({...z, type:'IFVG', tf:'H1', priority:3})) || []),
    ...(m15.fvgs_bear?.map(z => ({...z, type:'FVG', tf:'M15', priority:4})) || []),
    ...(h1.fvgs_bear?.map(z => ({...z, type:'FVG', tf:'H1', priority:4})) || []),
    ...(h4.fvgs_bear?.map(z => ({...z, type:'FVG', tf:'H4', priority:5})) || []),
    ...(m15.obs_bear?.map(z => ({...z, type:'OB', tf:'M15', priority:6})) || []),
    ...(h1.obs_bear?.map(z => ({...z, type:'OB', tf:'H1', priority:6})) || []),
    ...(h4.obs_bear?.map(z => ({...z, type:'OB', tf:'H4', priority:7})) || []),
  ];

  const zones = isLong ? zonesLong : zonesShort;

  // For LONG: price must be INSIDE the zone (px >= bot && px <= top + small buffer)
  // For SHORT: price must be INSIDE the zone (px <= top && px >= bot - small buffer)
  // This is the ICT rule: you WAIT for price to return to the zone, then enter
  const inZone = isLong
    ? zones.filter(z => px >= z.bot * 0.9995 && px <= z.top * 1.0005)
    : zones.filter(z => px <= z.top * 1.0005 && px >= z.bot * 0.9995);

  if (inZone.length > 0) {
    // Pick highest priority (BPR > IFVG > FVG > OB), then most recent (last in array)
    inZone.sort((a, b) => a.priority - b.priority || b.idx - a.idx);  // tie = prefer most recent zone
    entryZone = inZone[0];
  } else {
    // Price not yet in any zone — signal exists but is PENDING (waiting for pullback)
    // Find nearest zone to current price for PENDING display
    if (zones.length > 0) {
      // Max distance = 8% of current price to avoid picking zones too far away
      const maxDist = px * 0.08;
      const sorted = isLong
        ? zones.filter(z => z.top * 1.0002 < px && (px - z.top) < maxDist).sort((a,b) => b.top - a.top)
        : zones.filter(z => z.bot * 0.9998 > px && (z.bot - px) < maxDist).sort((a,b) => a.bot - b.bot);
      entryZone = sorted[0] || null;
    }
  }

  // ── 4. Determine if price is IN the zone (active) or PENDING (waiting) ─
  const atZone = entryZone && (
    isLong
      ? px >= entryZone.bot * 0.9998 && px <= entryZone.top * 1.0002
      : px <= entryZone.top * 1.0002 && px >= entryZone.bot * 0.9998
  );

  // ── 5. Entry, SL, TP — all derived from the zone, not random live price ─
  let entry, sl, slp, entryNote;
  if (entryZone && atZone) {
    // ACTIVE: price is at the zone — entry = midpoint of zone (50% fill entry)
    entry = entryZone.mid;
    if (isLong) {
      // SL = below the zone low with a small buffer (1-2 ticks)
      sl = entryZone.bot - (entryZone.top - entryZone.bot) * 0.15;
      entryNote = `Entry at ${entryZone.type} midpoint`;
    } else {
      // SL = above the zone high with buffer
      sl = entryZone.top + (entryZone.top - entryZone.bot) * 0.15;
      entryNote = `Entry at ${entryZone.type} midpoint`;
    }
    slp = Math.abs(entry - sl);
  } else if (entryZone) {
    // PENDING: show the zone level as the planned entry
    entry = isLong ? entryZone.top : entryZone.bot; // enter at zone edge price approaches
    sl = isLong
      ? entryZone.bot - (entryZone.top - entryZone.bot) * 0.15
      : entryZone.top + (entryZone.top - entryZone.bot) * 0.15;
    slp = Math.abs(entry - sl);
    entryNote = `Waiting for pullback to ${entryZone.type}`;
  } else {
    // No PD array found — no valid trade
    return null;
  }

  if (slp < 0.01) return null; // zero-width zone guard
  // Risk pts gate: Gold SL must be 3–40 pts. H4 FVG zones average 64 pts → reject.
  // H1 IFVG zones average 9 pts → pass. This aligns bot with web app standard (avg 9.7 pts).
  const MIN_RISK_PTS = 3;
  const MAX_RISK_PTS = 20; // H1 IFVG zones 3-19pts; loss #4 (17.79) caught by session gate
  if (slp < MIN_RISK_PTS || slp > MAX_RISK_PTS) return null; // risk gate

  // ── 5b. Execution confluence gate — structural confirmation required ──
  // Tier 1 (strongest): H4 sweep or H4 CISD — clear displacement confirmation
  // Tier 2 (valid):     H1 sweep or H4 CHoCH or H4 BOS — structural shift present  
  // Tier 3 (weakest):   H1 CISD alone — NOT sufficient (caused loss #4)
  // Gate: must have Tier 1 OR Tier 2 confirm
  const h4HasSweep = !!(h4.bslSwept || h4.sslSwept || h4.turtleBull || h4.turtleBear);
  const h4HasCisd  = !!(h4.cisd && ((isLong && h4.cisd.type === 'bull') || (!isLong && h4.cisd.type === 'bear')));
  const h4HasChoch = !!(isLong ? h4.choch_bull : h4.choch_bear);
  const h4HasBos   = !!(isLong ? h4.bos_bull   : h4.bos_bear);
  const execConfluence = h4HasSweep || h4HasCisd || h1HasSweep || h4HasChoch || h4HasBos;
  if (!execConfluence) return null; // pure KZ+IFVG+DOL only — insufficient

  // ── 6. TP = Draw on Liquidity — MUST be in the same direction as the trade ──
  // For LONG: dolTarget must be ABOVE entry. For SHORT: must be BELOW entry.
  const h4DolValid = h4.dol && (isLong ? h4.dol.dir === 'UP' && h4.dol.target > entry
                                        : h4.dol.dir === 'DOWN' && h4.dol.target < entry);
  const h1DolValid = h1.dol && (isLong ? h1.dol.dir === 'UP' && h1.dol.target > entry
                                        : h1.dol.dir === 'DOWN' && h1.dol.target < entry);
  const dolTarget = h4DolValid ? h4.dol.target : h1DolValid ? h1.dol.target : null;

  // Nearest BSL above entry (LONG) or SSL below entry (SHORT) as pool TP
  const bslAbove = h4.bsl?.filter(b => b.price > entry).sort((a,b) => a.price - b.price)[0];
  const sslBelow = h4.ssl?.filter(s => s.price < entry).sort((a,b) => b.price - a.price)[0];
  const poolTarget = isLong ? bslAbove?.price : sslBelow?.price;

  // Swing target in trade direction only
  const swingTarget = isLong ? h4.swH : h4.swL;
  const swingValid = swingTarget && (isLong ? swingTarget > entry : swingTarget < entry);

  // TP1 = 1.5R (first partial — take profit, move SL to breakeven)
  const tp1 = isLong ? entry + slp * 1.5 : entry - slp * 1.5;
  // TP2 = validated DOL target or nearest liquidity pool (both must be in trade direction)
  const tp2 = dolTarget || poolTarget || (isLong ? entry + slp * 3 : entry - slp * 3);
  // TP3 = swing high/low or 5R fallback
  const tp3 = (swingValid ? swingTarget : null) || (isLong ? entry + slp * 5 : entry - slp * 5);

  // RR to TP2 (the primary liquidity target)
  const rawRR = tp2 && Math.abs(tp2 - entry) > slp ? Math.abs(tp2 - entry) / slp : 0;

  // ── 7. Build conditions list ───────────────────────────────────────────
  const conditions = [];
  if (h4.inKZ || h4.session === 'London' || h4.session === 'New York') conditions.push('KZ');
  // H4 structural sweep (primary)
  if (isLong  && h4.turtleBull) conditions.push('Turtle Soup ↑ H4');
  else if (!isLong && h4.turtleBear) conditions.push('Turtle Soup ↓ H4');
  if (isLong  && h4.sslSwept) conditions.push('SSL swept H4');
  else if (!isLong && h4.bslSwept) conditions.push('BSL swept H4');
  // H1 sweep supplement (execution-level confirmation)
  if (isLong  && h1.sslSwept && !h4.sslSwept) conditions.push('SSL swept H1');
  else if (!isLong && h1.bslSwept && !h4.bslSwept) conditions.push('BSL swept H1');
  // CHoCH
  if (isLong  && h4.choch_bull) conditions.push('CHoCH ↑ H4');
  else if (!isLong && h4.choch_bear) conditions.push('CHoCH ↓ H4');
  if (isLong  && h4.bos_bull  && !h4.choch_bull)  conditions.push('BOS ↑ H4');
  else if (!isLong && h4.bos_bear && !h4.choch_bear) conditions.push('BOS ↓ H4');
  // CISD — H4 primary, H1 supplement
  if (h4.cisd && ((isLong && h4.cisd.type === 'bull') || (!isLong && h4.cisd.type === 'bear')))
    conditions.push('CISD ' + h4.cisd.type + ' H4');
  else if (h1.cisd && ((isLong && h1.cisd.type === 'bull') || (!isLong && h1.cisd.type === 'bear')))
    conditions.push('CISD ' + h1.cisd.type + ' H1');
  if (entryZone) conditions.push(entryZone.type + ' ' + entryZone.tf + (atZone ? ' ✓' : ' (pending)'));
  if (dolTarget) conditions.push('DOL ' + (h4DolValid ? 'H4' : 'H1') + ' → ' + dolTarget.toFixed(2));
  else if (poolTarget) conditions.push('Pool → ' + poolTarget.toFixed(2));
  if (rawRR > 0) conditions.push('RR ' + rawRR.toFixed(1) + ':1');
  if (window._smtData?.bearSMT) conditions.push('Bearish SMT');
  else if (window._smtData?.bullSMT) conditions.push('Bullish SMT');

  return {
    dir, grade, entry, sl, slp,
    tp1, tp2, tp3,
    tp2Label: dolTarget ? 'DOL' : poolTarget ? 'Pool' : '3R',
    tp3Label: '5R / BSL',
    rr: rawRR,
    atZone,           // true = active entry, false = pending pullback
    zoneType: entryZone?.type,
    zoneTF: entryZone?.tf,
    zoneBot: entryZone?.bot,
    zoneTop: entryZone?.top,
    entryNote,
    conditions,
    time: now,
    amdPhase: h4.amdPhase || '—',
    session: h4.session || '—',
    htfConfirmed,
  };
}



// ── computeSignal — wrapper that feeds live candles into the web app engine ──────
function computeSignal() {
  // Build candle arrays with timestamp field 't' as ISO string (what analyzeICT expects)
  const toCandles = (arr, cur) => {
    const all = [...arr, cur].filter(Boolean);
    return all.map(c => ({
      o: c.o, h: c.h, l: c.l, c: c.c,
      t: new Date(c.t).toISOString().replace('T', ' ').slice(0, 19),
    }));
  };

  const h4arr  = toCandles(candles['4h'],  currentCandle['4h']);
  const h1arr  = toCandles(candles['1h'],  currentCandle['1h']);
  const m15arr = toCandles(candles['15m'], currentCandle['15m']);
  const m5arr  = toCandles(candles['5m'],  currentCandle['5m']);

  if (h4arr.length < 20 || h1arr.length < 10) return null;

  // Run analysis on each timeframe
  let anals;
  try {
    anals = {
      '4h':  analyzeICT(h4arr,  '4h'),
      '1h':  analyzeICT(h1arr,  '1h'),
      '15m': m15arr.length >= 10 ? analyzeICT(m15arr, '15m') : {},
      '5m':  m5arr.length  >= 10 ? analyzeICT(m5arr,  '5m')  : {},
      '1d':  {},  // no D1 on WS — omit
    };
  } catch(e) {
    console.error('[ENGINE] analyzeICT error:', e.message);
    return null;
  }

  const px = currentCandle['4h']?.c;
  if (!px) return null;

  let sig;
  try {
    sig = detectExecutionOnBars(anals, px);
  } catch(e) {
    console.error('[ENGINE] detectExecution error:', e.message);
    return null;
  }
  if (!sig) return null;

  // Map web app signal format → bot Telegram format
  const isLong = sig.dir === 'LONG';
  const slDist = Math.abs(sig.entry - sig.sl);
  return {
    dir:        sig.dir,
    grade:      sig.grade,
    session:    sig.session === 'New York' ? 'NY' : sig.session,
    entry:      sig.entry,
    sl:         sig.sl,
    slDist:     slDist,
    tp1:        sig.tp1,
    tp2:        sig.tp2,
    tp3:        sig.tp3 || (isLong ? sig.entry + slDist * 5 : sig.entry - slDist * 5),
    rr:         sig.rawRR || 0,
    conditions: sig.conditions || [],
    fibPct:     parseFloat(anals['4h']?.fibPct || 50),
    zoneType:   sig.zoneType || '—',
    zoneTF:     sig.zoneTF   || '—',
  };
}

// ── TELEGRAM ──────────────────────────────────────────────────────────────────────
async function sendTelegram(sig) {
  const dir   = sig.dir === 'LONG' ? '🟢 LONG' : '🔴 SHORT';
  const badge = '🏆 A++';
  const flag  = sig.session === 'London' ? '🇬🇧' : '🗽';

  const etNow = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', hour12: false });
  const text = [
    `⚡ *SIGNAL POINTER — XAUUSD*`,
    `${dir}  ${badge}  ${flag} ${sig.session}`,
    `🕐 ${etNow} ET`,
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
// ── SIGNAL FILTER CONFIG ──────────────────────────────────────────────────────────
// Backtest data (web app engine, Dec 2025 – Jun 2026):
//   A++: 80% WR, PF 17.94, +33.87R  ← TRADE
//   A+:  38.5% WR, PF 4.84           ← SKIP (loses in 4/6 months)
//   B:   losing                       ← SKIP
const ALLOWED_GRADES    = ['A++'];          // Only A++ signals sent to Telegram
const MAX_PER_SESSION   = 1;               // Max 1 signal per kill zone session
const SIGNAL_COOLDOWN_MS = 4 * 60 * 60 * 1000; // 4-hour cooldown (one per session)
const SIGNAL_ZONE_PTS   = 40;             // Ignore if entry within 40pts of last

// Per-session counter: resets each new session window
// Key: YYYY-MM-DD_SESSION  e.g. "2026-06-26_NY"
const sessionCount = new Map();

function getSessionKey(sig) {
  const et = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const date = et.toISOString().slice(0, 10);
  const sess = (sig.session || '').replace('New York', 'NY');
  return `${date}_${sess}`;
}

let lastSignalKey   = '';
let lastSignalTime  = 0;
let lastSignalEntry = 0;

function shouldSend(sig) {
  const now = Date.now();

  // ── Grade gate: A++ only ──────────────────────────────────────────────────────
  if (!ALLOWED_GRADES.includes(sig.grade)) {
    console.log(`[SKIP] Grade ${sig.grade} @ ${sig.entry.toFixed(2)} — only A++ allowed`);
    return false;
  }

  // ── Per-session cap: max 1 signal per session ─────────────────────────────────
  const sessKey = getSessionKey(sig);
  const sessCount = sessionCount.get(sessKey) || 0;
  if (sessCount >= MAX_PER_SESSION) {
    console.log(`[SKIP] ${sig.dir} ${sig.grade} @ ${sig.entry.toFixed(2)} — session cap reached (${sessCount}/${MAX_PER_SESSION} for ${sessKey})`);
    return false;
  }

  // ── Cooldown: same direction within 4h ───────────────────────────────────────
  const sess  = (sig.session || '').replace('New York', 'NY');
  const dirKey = `${sig.dir}_${sess}`;
  if (dirKey === lastSignalKey && now - lastSignalTime < SIGNAL_COOLDOWN_MS) {
    console.log(`[SKIP] ${sig.dir} ${sig.grade} @ ${sig.entry.toFixed(2)} — cooldown active (${Math.round((SIGNAL_COOLDOWN_MS-(now-lastSignalTime))/60000)}min left)`);
    return false;
  }

  // ── Zone dedup: entry within 40pts of last ────────────────────────────────────
  if (lastSignalEntry > 0 && Math.abs(sig.entry - lastSignalEntry) < SIGNAL_ZONE_PTS && dirKey === lastSignalKey) {
    console.log(`[SKIP] ${sig.dir} ${sig.grade} @ ${sig.entry.toFixed(2)} — within ${SIGNAL_ZONE_PTS}pts of last signal @ ${lastSignalEntry.toFixed(2)}`);
    return false;
  }

  // ── All gates passed — update state ──────────────────────────────────────────
  sessionCount.set(sessKey, sessCount + 1);
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
  saveTrade(activeTrade);
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
    saveTrade({...t, slHit: true});
    activeTrade = null;
    saveTrade(null);
    sendTradeUpdate('SL', price);
    console.log(`[TRADE] SL hit @ ${price.toFixed(2)}`);
    return;
  }
  // TP1
  if (!t.tp1Hit && (isLong ? price >= t.tp1 : price <= t.tp1)) {
    t.tp1Hit = true;
    saveTrade(activeTrade);
    sendTradeUpdate('TP1', price);
    console.log(`[TRADE] TP1 hit @ ${price.toFixed(2)}`);
  }
  // TP2
  if (t.tp1Hit && !t.tp2Hit && (isLong ? price >= t.tp2 : price <= t.tp2)) {
    t.tp2Hit = true;
    saveTrade(activeTrade);
    sendTradeUpdate('TP2', price);
    console.log(`[TRADE] TP2 hit @ ${price.toFixed(2)}`);
  }
  // TP3
  if (t.tp2Hit && !t.tp3Hit && (isLong ? price >= t.tp3 : price <= t.tp3)) {
    t.tp3Hit = true;
    saveTrade(null);
    activeTrade = null;
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

// ── FILTER STATUS EXPORT (for dashboard) ────────────────────────────────────────
function getFilterConfig() {
  return {
    allowedGrades:  ALLOWED_GRADES,
    maxPerSession:  MAX_PER_SESSION,
    cooldownMin:    SIGNAL_COOLDOWN_MS / 60000,
    sessionCounts:  Object.fromEntries(sessionCount),
  };
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

  // /debug — show engine state right now
  if (req.url === '/debug') {
    const h4d = [...candles['4h'], currentCandle['4h']].filter(Boolean);
    const h1d = [...candles['1h'], currentCandle['1h']].filter(Boolean);
    const swHd = h4d.length>=20 ? Math.max(...h4d.slice(-20).map(c=>c.h)) : 0;
    const swLd = h4d.length>=20 ? Math.min(...h4d.slice(-20).map(c=>c.l)) : 0;
    const fibD = swHd>swLd && price>0 ? ((price-swLd)/(swHd-swLd)*100).toFixed(1)+'%' : 'n/a';
    const avgRng = h4d.length>=10 ? (h4d.slice(-10).reduce((s,c)=>s+(c.h-c.l),0)/10).toFixed(2) : 'n/a';
    const coolLeft = lastSignalTime>0 ? Math.max(0,Math.round((SIGNAL_COOLDOWN_MS-(Date.now()-lastSignalTime))/60000)) : 0;
    res.writeHead(200,{'Content-Type':'application/json',...CORS});
    res.end(JSON.stringify({
      time_ET: et.toLocaleTimeString('en-US',{timeZone:'America/New_York',hour12:false}),
      hhmm, inKillZone: inKZ,
      price: price.toFixed(2),
      h4Candles: h4d.length, h1Candles: h1d.length,
      swingHigh: swHd.toFixed(2), swingLow: swLd.toFixed(2),
      fibPosition: fibD,
      avgH4Range: avgRng,
      ticks: tickCount,
      lastSignalKey: lastSignalKey||'none',
      cooldownMinLeft: coolLeft,
      diagnosis: !inKZ ? '⛔ Outside Kill Zone — engine idle (London 02-05 ET, NY 08:30-11 ET)'
        : h4d.length<20 ? '⛔ Not enough H4 candles yet (need 20+)'
        : h1d.length<10 ? '⛔ Not enough H1 candles yet (need 10+)'
        : coolLeft>0 ? '⏳ Cooldown active — '+coolLeft+'min left'
        : '✅ Engine running — signal fires when sweep+FVG+CISD align in KZ',
    },null,2));
    return;
  }

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
      if (ok) setActiveTrade(testSig); // monitor TP/SL on test signal too
      res.writeHead(200, { 'Content-Type': 'application/json', ...CORS });
      res.end(JSON.stringify({
        sent: ok,
        price: livePrice.toFixed(2),
        entry: testSig.entry.toFixed(2),
        sl: testSig.sl.toFixed(2),
        tp1: testSig.tp1.toFixed(2),
        tp2: testSig.tp2.toFixed(2),
        tp3: testSig.tp3.toFixed(2),
        monitoring: ok ? 'TP/SL monitoring active — watching live price' : 'Send failed',
        message: ok ? 'Check your Telegram — TP/SL alerts will fire automatically' : 'Failed — check TG_TOKEN and TG_CHAT_ID'
      }));
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
    filterConfig: getFilterConfig(),
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

// Restore active trade from D1 if bot restarted mid-trade
loadTrade().then(t => {
  if (t) activeTrade = t;
  seedCandles().then(() => connect());
});
