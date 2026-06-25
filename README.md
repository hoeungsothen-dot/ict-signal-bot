# ICT Signal Bot — 24/7 Real-Time XAUUSD Alerts

Connects to TwelveData WebSocket → runs ICT 2022 signal engine in real-time → sends to Telegram **instantly** when B/A+/A++ fires. No browser needed.

## Deploy on Railway (Free)

1. Push this folder to a GitHub repo (e.g. `ict-signal-bot`)
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Select your repo
4. Go to **Variables** and add:

| Variable | Value |
|---|---|
| `TD_KEY` | Your TwelveData API key |
| `TG_TOKEN` | Your Telegram Bot Token (from @BotFather) |
| `TG_CHAT_ID` | Your Telegram group Chat ID |

5. Deploy — done. It runs 24/7 forever.

## Deploy on Render (Free)

1. Push to GitHub
2. Go to [render.com](https://render.com) → New Web Service → Connect repo
3. Build command: `npm install`
4. Start command: `node bot.js`
5. Add env vars same as above
6. Deploy

## Health Check

Visit your Railway/Render URL to see live status:
```json
{
  "status": "running",
  "wsConnected": true,
  "ticks": 4821,
  "price": "3985.40",
  "h4Candles": 80,
  "killZone": true,
  "session": "NY",
  "lastSignal": "LONG_A+_NY_3980",
  "uptime": "3721s"
}
```

## What it does

- Connects to TwelveData WebSocket for real-time XAUUSD price ticks
- Seeds H4/H1/15M candles from REST API on startup
- Builds live OHLC candles from ticks (5M, 15M, 1H, 4H)
- Runs ICT signal engine on every 50 ticks
- Only fires during Kill Zones (London 02:00–05:00 ET, NY 08:30–11:00 ET)
- Sends Telegram alert immediately when grade B/A+/A++ detected
- 1-hour cooldown per same signal to avoid spam
- Auto-reconnects WebSocket if disconnected
