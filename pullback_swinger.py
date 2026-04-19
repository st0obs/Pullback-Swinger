"""
Pullback Swinger Bot — Kunal Desai "Bone Zone" Strategy
Pandas-free version for fast deploy on Render.
"""
import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import requests
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest, StopOrderRequest, GetOrdersRequest,
    TakeProfitRequest, StopLossRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus, OrderClass
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame
import pytz

# =============================================================================
# CONFIG
# =============================================================================
ALPACA_KEY = os.getenv("ALPACA_KEY", "PKNYUXUDPCT42R4RMEL4ETUIUH")
ALPACA_SECRET = os.getenv("ALPACA_SECRET", "9LRuW9U62YzJmkPg1zQ7447gMRRtkhMGCNzwdamUbUn8")
ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
POLYGON_KEY = os.getenv("POLYGON_KEY", "8RK1dh1JG0yGxsTdoUvv7wrc2fb25r4W")

TURSO_DB_URL = os.getenv("TURSO_DB_URL", "libsql://oanda-bot-st0obs.aws-us-east-2.turso.io")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzQ1NjE3NDQsImlkIjoiMDE5ZDJjMWUtOTMwMS03NzE5LTkwM2EtMzVkYzA4YTkyZWUxIiwicmlkIjoiZjlmODhhMzMtY2RiMS00ZGJmLWFjNzMtNDBkM2U1ODk0NmE0In0.WzYNF01DuIYesT4GR0_RWCD4yNPYVvKaDaeF_zkS-BA-DlRNrzVkrfMVijGhNOjHd8TV_L8MsT1bsY822_tEAw")

RISK_PER_TRADE_PCT = 0.01
MAX_POSITIONS = 5
MIN_RR_RATIO = 3.0
UNIVERSE_SIZE = 200
SCAN_INTERVAL_SEC = 300
TRADING_WINDOW_START = "10:30"
TRADING_WINDOW_END = "15:30"
FORCE_EXIT_TIME = "15:55"
MIN_PRICE = 5.0
MIN_AVG_VOLUME = 500_000
BONE_ZONE_LOOKBACK_DAYS = 3
SPY_TREND_CHECK = True

DATA_DIR = Path("/tmp/pullback_swinger") if os.getenv("RENDER") else Path.home() / "pullback_swinger_data"
DATA_DIR.mkdir(exist_ok=True)
POSITIONS_JSON = DATA_DIR / "active_positions.json"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("pullback_swinger")

trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)
data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
ET = pytz.timezone("US/Eastern")


# =============================================================================
# INDICATORS (pure python)
# =============================================================================
def ema_list(values: list, period: int) -> list:
    """EMA using standard alpha=2/(period+1)."""
    if not values:
        return []
    alpha = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + alpha * (v - out[-1]))
    return out


def sma_list(values: list, period: int) -> list:
    """SMA with None for first period-1 values."""
    out = []
    for i in range(len(values)):
        if i < period - 1:
            out.append(None)
        else:
            out.append(sum(values[i - period + 1:i + 1]) / period)
    return out


# =============================================================================
# TURSO
# =============================================================================
def turso_execute(sql: str, args: Optional[list] = None) -> Optional[dict]:
    url = TURSO_DB_URL.replace("libsql://", "https://") + "/v2/pipeline"
    stmt = {"sql": sql}
    if args:
        typed = []
        for v in args:
            if v is None:
                typed.append({"type": "null"})
            elif isinstance(v, bool):
                typed.append({"type": "integer", "value": "1" if v else "0"})
            elif isinstance(v, int):
                typed.append({"type": "integer", "value": str(v)})
            elif isinstance(v, float):
                typed.append({"type": "float", "value": v})
            else:
                typed.append({"type": "text", "value": str(v)})
        stmt["args"] = typed
    body = {"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]}
    try:
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {TURSO_AUTH_TOKEN}", "Content-Type": "application/json"},
            json=body, timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"Turso error ({resp.status_code}): {resp.text[:300]}")
            return None
        return resp.json()
    except Exception as e:
        log.error(f"Turso request failed: {e}")
        return None


def turso_init_tables():
    turso_execute("""
        CREATE TABLE IF NOT EXISTS pullback_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, event TEXT, ticker TEXT, shares INTEGER,
            entry REAL, stop REAL, target REAL, exit_price REAL,
            pnl REAL, rr_ratio REAL, stage TEXT, reason TEXT
        )
    """)
    turso_execute("""
        CREATE TABLE IF NOT EXISTS pullback_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, equity REAL, cash REAL, buying_power REAL,
            active_positions INTEGER, universe_size INTEGER,
            market_open INTEGER, in_trading_window INTEGER
        )
    """)
    turso_execute("""
        CREATE TABLE IF NOT EXISTS pullback_active (
            ticker TEXT PRIMARY KEY, shares INTEGER,
            entry REAL, stop REAL, target REAL, rr_ratio REAL,
            stage TEXT, entered_at TEXT,
            current_price REAL, unrealized_pnl REAL, updated_at TEXT
        )
    """)
    log.info("Turso tables ready")


def turso_log_trade(event: str, setup: dict, shares: int = 0, exit_price: float = 0,
                    pnl: float = 0, reason: str = ""):
    turso_execute(
        "INSERT INTO pullback_trades (timestamp, event, ticker, shares, entry, stop, target, exit_price, pnl, rr_ratio, stage, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            now_et().isoformat(), event, setup.get("ticker", ""),
            int(shares),
            float(setup.get("entry", 0)), float(setup.get("stop", 0)),
            float(setup.get("target", 0)), float(exit_price), float(pnl),
            float(setup.get("rr_ratio", 0)), setup.get("stage", ""), reason,
        ],
    )


def turso_upsert_active(active: dict):
    turso_execute("DELETE FROM pullback_active")
    for ticker, pos in active.items():
        current_price = pos.get("current_price", pos.get("entry", 0)) or 0
        unrealized = (current_price - pos.get("entry", 0)) * pos.get("shares", 0)
        turso_execute(
            "INSERT INTO pullback_active (ticker, shares, entry, stop, target, rr_ratio, stage, entered_at, current_price, unrealized_pnl, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                ticker, int(pos.get("shares", 0)),
                float(pos.get("entry", 0)), float(pos.get("stop", 0)),
                float(pos.get("target", 0)), float(pos.get("rr_ratio", 0)),
                pos.get("stage", ""), pos.get("entered_at", ""),
                float(current_price), float(unrealized),
                now_et().isoformat(),
            ],
        )


def turso_log_status(active: dict, universe_size: int):
    try:
        acc = trading.get_account()
        turso_execute(
            "INSERT INTO pullback_status (timestamp, equity, cash, buying_power, active_positions, universe_size, market_open, in_trading_window) VALUES (?,?,?,?,?,?,?,?)",
            [
                now_et().isoformat(),
                float(acc.equity), float(acc.cash), float(acc.buying_power),
                len(active), int(universe_size),
                1 if is_market_open() else 0,
                1 if in_trading_window() else 0,
            ],
        )
    except Exception as e:
        log.debug(f"status log failed: {e}")


# =============================================================================
# UTILITIES
# =============================================================================
def now_et() -> datetime:
    return datetime.now(ET)


def is_market_open() -> bool:
    try:
        return trading.get_clock().is_open
    except Exception:
        return False


def in_trading_window() -> bool:
    t = now_et().strftime("%H:%M")
    return TRADING_WINDOW_START <= t <= TRADING_WINDOW_END


def past_force_exit() -> bool:
    return now_et().strftime("%H:%M") >= FORCE_EXIT_TIME


# =============================================================================
# UNIVERSE
# =============================================================================
def build_universe() -> list:
    log.info("Building universe from Polygon gainers/losers/actives...")
    tickers = set()
    endpoints = [
        ("gainers", "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"),
        ("losers", "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/losers"),
    ]
    for name, url in endpoints:
        try:
            r = requests.get(url, params={"apiKey": POLYGON_KEY}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for t in data.get("tickers", []):
                    symbol = t.get("ticker")
                    last_price = (t.get("day") or {}).get("c", 0) or (t.get("prevDay") or {}).get("c", 0)
                    volume = (t.get("day") or {}).get("v", 0)
                    if symbol and last_price >= MIN_PRICE and volume >= MIN_AVG_VOLUME:
                        tickers.add(symbol)
                log.info(f"  {name}: added {len(data.get('tickers', []))} candidates")
        except Exception as e:
            log.error(f"  {name} fetch failed: {e}")
    core = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "AVGO",
            "NFLX", "CRM", "ORCL", "ADBE", "PLTR", "UBER", "SHOP", "SMCI", "COIN",
            "HOOD", "SOFI", "RBLX", "SNOW", "DDOG", "NET", "MDB", "CRWD", "PANW"]
    tickers.update(core)
    universe = sorted(tickers)[:UNIVERSE_SIZE]
    log.info(f"Universe built: {len(universe)} tickers")
    return universe


# =============================================================================
# DATA FETCHING
# =============================================================================
def get_daily_bars(ticker: str, days: int = 250):
    """Returns list of dicts: [{date, open, high, low, close, volume}, ...] chronological."""
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days + 50)
        req = StockBarsRequest(symbol_or_symbols=ticker, timeframe=TimeFrame.Day, start=start, end=end)
        bars = data_client.get_stock_bars(req)
        if ticker not in bars.data or not bars.data[ticker]:
            return None
        return [{
            "date": b.timestamp.date(),
            "open": b.open, "high": b.high, "low": b.low,
            "close": b.close, "volume": b.volume,
        } for b in bars.data[ticker]]
    except Exception as e:
        log.debug(f"bars fetch failed for {ticker}: {e}")
        return None


def get_latest_price(ticker: str):
    try:
        req = StockLatestQuoteRequest(symbol_or_symbols=ticker)
        quote = data_client.get_stock_latest_quote(req)
        if ticker in quote:
            q = quote[ticker]
            if q.bid_price and q.ask_price:
                return (q.bid_price + q.ask_price) / 2
            return q.ask_price or q.bid_price
    except Exception:
        pass
    return None


# =============================================================================
# BONE ZONE SETUP DETECTION
# =============================================================================
def check_bone_zone_setup(ticker: str, bars: list):
    if bars is None or len(bars) < 210:
        return None

    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    volumes = [b["volume"] for b in bars]

    ema9 = ema_list(closes, 9)
    ema20 = ema_list(closes, 20)
    sma50 = sma_list(closes, 50)
    sma200 = sma_list(closes, 200)
    vol_avg20 = sma_list(volumes, 20)

    n = len(bars)
    last_close = closes[-1]
    last_open = bars[-1]["open"]
    last_vol = volumes[-1]
    last_ema9 = ema9[-1]
    last_ema20 = ema20[-1]
    last_sma200 = sma200[-1]
    last_vol_avg = vol_avg20[-1]

    # Filter 1: Uptrend
    if last_sma200 is None or last_close < last_sma200: return None
    if sma50[-1] is None or sma50[-6] is None: return None
    if sma50[-1] <= sma50[-6]: return None
    if last_ema9 <= last_ema20: return None

    # Filter 2: Recent pullback INTO the Bone Zone
    pullback_happened = False
    pullback_low = None
    for idx in range(max(0, n - BONE_ZONE_LOOKBACK_DAYS - 1), n - 1):
        if lows[idx] <= ema9[idx] and lows[idx] >= ema20[idx] * 0.99:
            pullback_happened = True
            pullback_low = lows[idx] if pullback_low is None else min(pullback_low, lows[idx])
    if not pullback_happened: return None

    # Filter 3: Confirmation candle today
    if last_close <= last_open: return None
    if last_close <= last_ema9: return None
    if last_vol_avg is None or last_vol <= last_vol_avg: return None

    # Filter 4: Not extended
    if last_close > last_ema20 * 1.08: return None

    entry = last_close
    raw_stop = min(pullback_low, last_ema20)
    stop = round(raw_stop * 0.995, 2)
    risk_per_share = entry - stop
    if risk_per_share <= 0: return None
    target = round(entry + (risk_per_share * MIN_RR_RATIO), 2)
    if target > entry * 1.10:
        target = round(entry * 1.10, 2)
        actual_rr = (target - entry) / risk_per_share
        if actual_rr < 2.0: return None
    actual_rr = round((target - entry) / risk_per_share, 2)

    return {
        "ticker": ticker, "entry": round(entry, 2), "stop": stop, "target": target,
        "risk_per_share": round(risk_per_share, 2), "rr_ratio": actual_rr,
        "ema9": round(last_ema9, 2), "ema20": round(last_ema20, 2),
        "sma200": round(last_sma200, 2),
        "volume": int(last_vol), "vol_avg20": int(last_vol_avg),
        "detected_at": now_et().isoformat(),
    }


def check_spy_market_gate() -> bool:
    if not SPY_TREND_CHECK:
        return True
    bars = get_daily_bars("SPY", days=250)
    if bars is None or len(bars) < 200:
        log.warning("Could not fetch SPY — allowing trades (fail-open)")
        return True
    closes = [b["close"] for b in bars]
    sma200 = sma_list(closes, 200)
    last_close = closes[-1]
    last_sma200 = sma200[-1]
    gate_ok = last_close > last_sma200
    log.info(f"SPY gate: price=${last_close:.2f} 200DMA=${last_sma200:.2f} → {'OPEN' if gate_ok else 'CLOSED'}")
    return gate_ok


# =============================================================================
# POSITION MANAGEMENT
# =============================================================================
def load_active_positions() -> dict:
    if POSITIONS_JSON.exists():
        try: return json.loads(POSITIONS_JSON.read_text())
        except Exception: return {}
    return {}


def save_active_positions(positions: dict):
    POSITIONS_JSON.write_text(json.dumps(positions, indent=2, default=str))


def get_account_equity() -> float:
    return float(trading.get_account().equity)


def calc_position_size(entry: float, stop: float, equity: float) -> int:
    risk_dollars = equity * RISK_PER_TRADE_PCT
    risk_per_share = entry - stop
    return max(1, int(risk_dollars / risk_per_share))


def place_bracket_order(setup: dict, shares: int):
    ticker = setup["ticker"]
    try:
        req = MarketOrderRequest(
            symbol=ticker, qty=shares, side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=setup["target"]),
            stop_loss=StopLossRequest(stop_price=setup["stop"]),
        )
        order = trading.submit_order(req)
        log.info(f"✅ ENTRY {ticker}: {shares} shares @ market, stop=${setup['stop']}, target=${setup['target']}, order_id={order.id}")
        return str(order.id)
    except Exception as e:
        log.error(f"❌ order failed for {ticker}: {e}")
        return None


def sync_with_alpaca_positions(active: dict) -> dict:
    try:
        alpaca_positions = trading.get_all_positions()
        alpaca_tickers = {p.symbol for p in alpaca_positions}
        for t in list(active.keys()):
            if t not in alpaca_tickers:
                pos = active[t]
                exit_price = get_latest_price(t) or pos.get("entry", 0)
                pnl = (exit_price - pos.get("entry", 0)) * pos.get("shares", 0)
                reason = "target hit" if exit_price >= pos.get("target", 0) else "stop hit"
                turso_log_trade("EXIT", pos, shares=pos.get("shares", 0), exit_price=exit_price, pnl=pnl, reason=reason)
                log.info(f"🔚 {t} closed by Alpaca: exit~${exit_price:.2f}, pnl=${pnl:.2f} ({reason})")
                del active[t]
        for p in alpaca_positions:
            if p.symbol not in active:
                active[p.symbol] = {
                    "ticker": p.symbol, "shares": int(p.qty),
                    "entry": float(p.avg_entry_price),
                    "stop": float(p.avg_entry_price) * 0.98,
                    "target": float(p.avg_entry_price) * 1.05,
                    "entered_at": now_et().isoformat(),
                    "stage": "RUNNING", "rr_ratio": 0,
                }
        save_active_positions(active)
    except Exception as e:
        log.error(f"sync failed: {e}")
    return active


def scan_and_enter(universe: list, active: dict):
    if len(active) >= MAX_POSITIONS:
        log.info(f"Max positions ({MAX_POSITIONS}) reached, skipping scan")
        return
    if not check_spy_market_gate():
        log.info("SPY gate CLOSED — no new entries today")
        return

    log.info(f"Scanning {len(universe)} tickers...")
    candidates = []
    for ticker in universe:
        if ticker in active: continue
        bars = get_daily_bars(ticker)
        if bars is None: continue
        setup = check_bone_zone_setup(ticker, bars)
        if setup: candidates.append(setup)

    if not candidates:
        log.info("No setups found this scan")
        return

    candidates.sort(key=lambda s: s["rr_ratio"], reverse=True)
    log.info(f"Found {len(candidates)} setups. Top 10:")
    for i, s in enumerate(candidates[:10]):
        log.info(f"  {i+1}. {s['ticker']} entry=${s['entry']} stop=${s['stop']} target=${s['target']} R/R={s['rr_ratio']}")

    equity = get_account_equity()
    slots = MAX_POSITIONS - len(active)
    for setup in candidates[:slots]:
        shares = calc_position_size(setup["entry"], setup["stop"], equity)
        order_id = place_bracket_order(setup, shares)
        if order_id:
            active[setup["ticker"]] = {
                **setup, "shares": shares, "order_id": order_id,
                "entered_at": now_et().isoformat(), "stage": "RUNNING",
            }
            turso_log_trade("ENTRY", setup, shares=shares)
    save_active_positions(active)


def manage_positions(active: dict):
    if not active: return
    for ticker, pos in list(active.items()):
        try:
            current_price = get_latest_price(ticker)
            if current_price is None: continue
            pos["current_price"] = current_price
            entry = pos.get("entry", 0)
            stop = pos.get("stop", 0)
            risk = entry - stop
            if risk <= 0: continue
            unrealized_rr = (current_price - entry) / risk

            if unrealized_rr >= 1.0 and pos.get("stage") == "RUNNING":
                new_stop = round(entry * 1.001, 2)
                update_stop_loss(ticker, pos, new_stop)
                pos["stop"] = new_stop
                pos["stage"] = "BREAKEVEN"
                log.info(f"💵 {ticker} hit 1:1, stop → breakeven ${new_stop}")

            if unrealized_rr >= 2.0 and pos.get("stage") in ("RUNNING", "BREAKEVEN"):
                bars = get_daily_bars(ticker, days=60)
                if bars is not None and len(bars) > 20:
                    closes = [b["close"] for b in bars]
                    ema9_arr = ema_list(closes, 9)
                    ema9_now = ema9_arr[-1]
                    trail_stop = round(ema9_now * 0.995, 2)
                    if trail_stop > pos["stop"]:
                        update_stop_loss(ticker, pos, trail_stop)
                        pos["stop"] = trail_stop
                        pos["stage"] = "TRAILING"
                        log.info(f"📈 {ticker} trailing 9 EMA, stop → ${trail_stop}")
        except Exception as e:
            log.error(f"manage {ticker} failed: {e}")
    save_active_positions(active)


def update_stop_loss(ticker: str, pos: dict, new_stop: float):
    try:
        orders = trading.get_orders(filter=GetOrdersRequest(status="all", symbols=[ticker]))
        for o in orders:
            if o.order_type == "stop" and o.side == OrderSide.SELL and o.status in [OrderStatus.NEW, OrderStatus.ACCEPTED, OrderStatus.HELD]:
                trading.cancel_order_by_id(o.id)
                time.sleep(0.5)
                new_order = StopOrderRequest(
                    symbol=ticker, qty=pos["shares"], side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY, stop_price=new_stop,
                )
                trading.submit_order(new_order)
                break
    except Exception as e:
        log.error(f"update stop for {ticker} failed: {e}")


def force_exit_all(active: dict):
    if not active: return
    log.info("🔔 EOD force-exit triggered")
    for ticker, pos in list(active.items()):
        try:
            trading.close_position(ticker)
            exit_price = get_latest_price(ticker) or pos.get("entry", 0)
            pnl = (exit_price - pos.get("entry", 0)) * pos.get("shares", 0)
            log.info(f"🚪 EOD exit {ticker}: {pos.get('shares', 0)} @ ~${exit_price:.2f}, P&L=${pnl:.2f}")
            turso_log_trade("EXIT_EOD", pos, shares=pos.get("shares", 0), exit_price=exit_price, pnl=pnl, reason="EOD force exit")
        except Exception as e:
            log.error(f"force exit {ticker} failed: {e}")
    active.clear()
    save_active_positions(active)


# =============================================================================
# MAIN
# =============================================================================
def main():
    log.info("=" * 70)
    log.info("Pullback Swinger Bot — Kunal Desai Bone Zone Strategy")
    log.info("=" * 70)
    log.info(f"Paper account: {ALPACA_BASE_URL}")
    log.info(f"Current ET time: {now_et().strftime('%Y-%m-%d %H:%M:%S %Z')}")

    try:
        acc = trading.get_account()
        log.info(f"Connected. Equity=${float(acc.equity):,.2f}  BP=${float(acc.buying_power):,.2f}")
    except Exception as e:
        log.error(f"Alpaca connection failed: {e}")
        return

    turso_init_tables()

    universe = []
    universe_built_date = None
    last_scan = 0
    last_status = 0
    last_active_sync = 0
    last_heartbeat = 0

    while True:
        try:
            now = now_et()
            date_today = now.strftime("%Y-%m-%d")

            if (universe_built_date != date_today) and now.strftime("%H:%M") >= "09:00":
                universe = build_universe()
                universe_built_date = date_today

            active = load_active_positions()
            active = sync_with_alpaca_positions(active)

            if is_market_open():
                manage_positions(active)
                if in_trading_window() and (time.time() - last_scan) >= SCAN_INTERVAL_SEC:
                    if universe:
                        scan_and_enter(universe, active)
                    last_scan = time.time()
                if past_force_exit():
                    force_exit_all(active)

            if (time.time() - last_active_sync) >= 120:
                turso_upsert_active(active)
                last_active_sync = time.time()

            if (time.time() - last_status) >= 600:
                turso_log_status(active, len(universe))
                last_status = time.time()

            # Status log every 10 min, but only during market hours
            if (time.time() - last_heartbeat) >= 600 and is_market_open():
                log.info(f"💓 {now.strftime('%H:%M:%S ET')} | window={in_trading_window()} | active={len(active)} | universe={len(universe)}")
                last_heartbeat = time.time()

            time.sleep(30)
        except KeyboardInterrupt:
            log.info("Shutdown requested")
            break
        except Exception as e:
            log.exception(f"Main loop error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
