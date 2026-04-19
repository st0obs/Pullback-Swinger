"""
Pullback Swinger Bot — Kunal Desai "Bone Zone" Strategy
========================================================
Strategy: Buy pullbacks in uptrending stocks when price enters the Bone Zone
(between 9 EMA and 20 EMA) on declining volume, then bounces with a green
confirmation candle on increasing volume.

Entry Criteria (ALL must be true):
  1. Stock in uptrend: price > 200DMA, 50DMA rising
  2. Green Bone Zone: 9 EMA > 20 EMA
  3. Price pulled into Bone Zone in last 1-3 days
  4. Today's candle is green (close > open) AND closes back above 9 EMA
  5. Volume on confirmation day > 20-day average volume
  6. SPY > 200DMA (market gate)

Exit Rules:
  - Stop: low of pullback OR below 20 EMA (whichever is lower)
  - Target: minimum 3:1 R/R (so if risk is $1, target is $3 above entry)
  - At 1:1 profit: move stop to break-even
  - At 2:1 profit: trail stop using 9 EMA
  - Close all positions by 3:55 PM ET if not already exited (no overnight holds 
    initially; we'll evaluate after 2 weeks of paper data)

Position Sizing:
  - Risk 1% of account per trade
  - Max 5 simultaneous positions
  - Shares = (account_risk_dollars) / (entry_price - stop_price)

Trading Window: 10:30 AM - 3:30 PM ET (avoid opening volatility + closing chaos)
Scan Frequency: Every 5 minutes during trading window

Data: Alpaca IEX real-time bars, Polygon for pre-market universe build
"""

import os
import time
import json
import logging
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import requests
import pandas as pd
import numpy as np
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest, LimitOrderRequest, StopOrderRequest,
    GetOrdersRequest, ClosePositionRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame

# =============================================================================
# CONFIG
# =============================================================================
ALPACA_KEY = os.getenv("ALPACA_KEY", "PKNYUXUDPCT42R4RMEL4ETUIUH")
ALPACA_SECRET = os.getenv("ALPACA_SECRET", "9LRuW9U62YzJmkPg1zQ7447gMRRtkhMGCNzwdamUbUn8")
ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
POLYGON_KEY = os.getenv("POLYGON_KEY", "8RK1dh1JG0yGxsTdoUvv7wrc2fb25r4W")

# Strategy config
RISK_PER_TRADE_PCT = 0.01          # 1% account risk per trade
MAX_POSITIONS = 5
MIN_RR_RATIO = 3.0                 # Minimum 3:1 risk/reward
UNIVERSE_SIZE = 200                # Top 200 most active names
SCAN_INTERVAL_SEC = 300            # 5 minutes between scans
TRADING_WINDOW_START = "10:30"     # ET — after opening range completes
TRADING_WINDOW_END = "15:30"       # ET — stop opening new positions
FORCE_EXIT_TIME = "15:55"          # ET — close all EOD
MIN_PRICE = 5.0                    # No penny stocks
MIN_AVG_VOLUME = 500_000           # Liquid only
BONE_ZONE_LOOKBACK_DAYS = 3        # Price must have pulled into zone in last N days

# Market gate — skip new entries if SPY < 200DMA
SPY_TREND_CHECK = True

# File paths
DATA_DIR = Path("/tmp/pullback_swinger") if os.getenv("RENDER") else Path.home() / "pullback_swinger_data"
DATA_DIR.mkdir(exist_ok=True)
TRADE_LOG_CSV = DATA_DIR / "trade_log.csv"
DAILY_LOG = DATA_DIR / "daily.log"
DASHBOARD_HTML = DATA_DIR / "dashboard.html"
POSITIONS_JSON = DATA_DIR / "active_positions.json"

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(DAILY_LOG),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("pullback_swinger")

# =============================================================================
# CLIENTS
# =============================================================================
trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)
data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

import pytz
ET = pytz.timezone("US/Eastern")

# =============================================================================
# UTILITIES
# =============================================================================
def now_et() -> datetime:
    return datetime.now(ET)


def is_market_open() -> bool:
    """True if market is open right now."""
    try:
        clock = trading.get_clock()
        return clock.is_open
    except Exception as e:
        log.error(f"clock check failed: {e}")
        return False


def in_trading_window() -> bool:
    """True if current ET time is within trading window (10:30-15:30)."""
    t = now_et().strftime("%H:%M")
    return TRADING_WINDOW_START <= t <= TRADING_WINDOW_END


def past_force_exit() -> bool:
    return now_et().strftime("%H:%M") >= FORCE_EXIT_TIME


# =============================================================================
# UNIVERSE BUILDER — Polygon pre-market top movers + liquid names
# =============================================================================
def build_universe() -> list[str]:
    """
    Build today's universe: top gainers + top losers + most active by volume.
    Run this at 9:00 AM ET. Caches result to disk.
    """
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
            else:
                log.warning(f"  {name} endpoint returned {r.status_code}")
        except Exception as e:
            log.error(f"  {name} fetch failed: {e}")

    # Add a baseline of always-liquid core names
    core = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "AVGO",
            "NFLX", "CRM", "ORCL", "ADBE", "PLTR", "UBER", "SHOP", "SMCI", "COIN",
            "HOOD", "SOFI", "RBLX", "SNOW", "DDOG", "NET", "MDB", "CRWD", "PANW"]
    tickers.update(core)

    # Limit to UNIVERSE_SIZE
    universe = sorted(tickers)[:UNIVERSE_SIZE]
    log.info(f"Universe built: {len(universe)} tickers")
    return universe


# =============================================================================
# INDICATORS
# =============================================================================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()


# =============================================================================
# DATA FETCHING
# =============================================================================
def get_daily_bars(ticker: str, days: int = 250) -> Optional[pd.DataFrame]:
    """Fetch daily bars from Alpaca."""
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days + 50)  # buffer for weekends
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
        )
        bars = data_client.get_stock_bars(req)
        if ticker not in bars.data or not bars.data[ticker]:
            return None
        df = pd.DataFrame([{
            "date": b.timestamp.date(),
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
        } for b in bars.data[ticker]])
        return df
    except Exception as e:
        log.debug(f"bars fetch failed for {ticker}: {e}")
        return None


def get_latest_price(ticker: str) -> Optional[float]:
    """Get latest quote price."""
    try:
        req = StockLatestQuoteRequest(symbol_or_symbols=ticker)
        quote = data_client.get_stock_latest_quote(req)
        if ticker in quote:
            q = quote[ticker]
            # Use mid-price (average of bid and ask)
            if q.bid_price and q.ask_price:
                return (q.bid_price + q.ask_price) / 2
            return q.ask_price or q.bid_price
    except Exception as e:
        log.debug(f"price fetch failed for {ticker}: {e}")
    return None


# =============================================================================
# STRATEGY — BONE ZONE PULLBACK DETECTION
# =============================================================================
def check_bone_zone_setup(ticker: str, df: pd.DataFrame) -> Optional[dict]:
    """
    Analyze a ticker's daily bars for the Bone Zone pullback setup.
    Returns setup dict if valid, None if not.
    """
    if df is None or len(df) < 210:
        return None

    # Compute indicators
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema20"] = ema(df["close"], 20)
    df["sma50"] = sma(df["close"], 50)
    df["sma200"] = sma(df["close"], 200)
    df["vol_avg20"] = sma(df["volume"], 20)

    last = df.iloc[-1]
    prev = df.iloc[-2]
    price = last["close"]

    # ===== FILTER 1: Uptrend confirmation =====
    if price < last["sma200"]:
        return None  # below 200DMA = no longs
    if last["sma50"] <= df.iloc[-6]["sma50"]:
        return None  # 50DMA must be rising
    if last["ema9"] <= last["ema20"]:
        return None  # Bone Zone must be GREEN (9 EMA above 20 EMA)

    # ===== FILTER 2: Recent pullback INTO the Bone Zone =====
    # Check if in last BONE_ZONE_LOOKBACK_DAYS the low touched the zone (between 9 and 20 EMA)
    recent = df.iloc[-(BONE_ZONE_LOOKBACK_DAYS + 1):-1]  # yesterday + a few days back
    pullback_happened = False
    pullback_low = None
    for _, row in recent.iterrows():
        # Low touched the zone = low was <= 9 EMA and >= 20 EMA
        if row["low"] <= row["ema9"] and row["low"] >= row["ema20"] * 0.99:
            pullback_happened = True
            pullback_low = row["low"] if pullback_low is None else min(pullback_low, row["low"])
    if not pullback_happened:
        return None

    # ===== FILTER 3: Confirmation candle TODAY =====
    # Today green: close > open
    if last["close"] <= last["open"]:
        return None
    # Today closes back above 9 EMA
    if last["close"] <= last["ema9"]:
        return None
    # Today's volume > 20-day average
    if last["volume"] <= last["vol_avg20"]:
        return None

    # ===== FILTER 4: Not extended (avoid buying into parabolic moves) =====
    # Price should not be more than 8% above 20 EMA (too extended)
    if price > last["ema20"] * 1.08:
        return None

    # ===== COMPUTE ENTRY, STOP, TARGET =====
    entry = price
    # Stop = minimum of pullback low and 20 EMA, minus a small buffer
    raw_stop = min(pullback_low, last["ema20"])
    stop = round(raw_stop * 0.995, 2)  # half a percent buffer below the level

    risk_per_share = entry - stop
    if risk_per_share <= 0:
        return None
    # Target = 3:1 R/R minimum
    target = round(entry + (risk_per_share * MIN_RR_RATIO), 2)

    # Sanity: target should not be absurdly far (cap at 10% from entry)
    if target > entry * 1.10:
        target = round(entry * 1.10, 2)
        # Recompute R/R with capped target
        actual_rr = (target - entry) / risk_per_share
        if actual_rr < 2.0:  # hard floor of 2:1 even after cap
            return None

    actual_rr = round((target - entry) / risk_per_share, 2)

    return {
        "ticker": ticker,
        "entry": round(entry, 2),
        "stop": stop,
        "target": target,
        "risk_per_share": round(risk_per_share, 2),
        "rr_ratio": actual_rr,
        "ema9": round(last["ema9"], 2),
        "ema20": round(last["ema20"], 2),
        "sma200": round(last["sma200"], 2),
        "volume": int(last["volume"]),
        "vol_avg20": int(last["vol_avg20"]),
        "detected_at": now_et().isoformat(),
    }


def check_spy_market_gate() -> bool:
    """Market gate: only trade if SPY > 200DMA."""
    if not SPY_TREND_CHECK:
        return True
    df = get_daily_bars("SPY", days=250)
    if df is None or len(df) < 200:
        log.warning("Could not fetch SPY — allowing trades (data fail-open)")
        return True
    df["sma200"] = sma(df["close"], 200)
    last = df.iloc[-1]
    gate_ok = last["close"] > last["sma200"]
    log.info(f"SPY gate: price=${last['close']:.2f} 200DMA=${last['sma200']:.2f} → {'OPEN' if gate_ok else 'CLOSED'}")
    return gate_ok


# =============================================================================
# POSITION MANAGEMENT
# =============================================================================
def load_active_positions() -> dict:
    if POSITIONS_JSON.exists():
        try:
            return json.loads(POSITIONS_JSON.read_text())
        except Exception:
            return {}
    return {}


def save_active_positions(positions: dict):
    POSITIONS_JSON.write_text(json.dumps(positions, indent=2, default=str))


def get_account_equity() -> float:
    acc = trading.get_account()
    return float(acc.equity)


def calc_position_size(entry: float, stop: float, equity: float) -> int:
    """Shares = (account * risk_pct) / risk_per_share."""
    risk_dollars = equity * RISK_PER_TRADE_PCT
    risk_per_share = entry - stop
    shares = int(risk_dollars / risk_per_share)
    return max(1, shares)


def place_bracket_order(setup: dict, shares: int) -> Optional[str]:
    """Place a bracket order: entry + stop-loss + take-profit all at once."""
    ticker = setup["ticker"]
    try:
        from alpaca.trading.requests import TakeProfitRequest, StopLossRequest
        from alpaca.trading.enums import OrderClass

        req = MarketOrderRequest(
            symbol=ticker,
            qty=shares,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
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
    """Reconcile local state with actual Alpaca positions."""
    try:
        alpaca_positions = trading.get_all_positions()
        alpaca_tickers = {p.symbol for p in alpaca_positions}

        # Remove local entries where Alpaca no longer holds the position
        removed = []
        for t in list(active.keys()):
            if t not in alpaca_tickers:
                removed.append(t)
                del active[t]
        if removed:
            log.info(f"Synced out closed positions: {removed}")

        # For any Alpaca position not in our local state, add a stub (shouldn't happen normally)
        for p in alpaca_positions:
            if p.symbol not in active:
                active[p.symbol] = {
                    "ticker": p.symbol,
                    "shares": int(p.qty),
                    "entry": float(p.avg_entry_price),
                    "stop": float(p.avg_entry_price) * 0.98,  # placeholder
                    "target": float(p.avg_entry_price) * 1.05,  # placeholder
                    "entered_at": now_et().isoformat(),
                    "stage": "RUNNING",
                }
        save_active_positions(active)
    except Exception as e:
        log.error(f"sync failed: {e}")
    return active


# =============================================================================
# TRADE LOG CSV
# =============================================================================
def log_trade(event: str, setup: dict, shares: int = 0, exit_price: float = 0,
              pnl: float = 0, reason: str = ""):
    """Append a trade event to the CSV."""
    new = not TRADE_LOG_CSV.exists()
    with open(TRADE_LOG_CSV, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["timestamp", "event", "ticker", "shares", "entry",
                        "stop", "target", "exit_price", "pnl", "rr_ratio", "reason"])
        w.writerow([
            now_et().isoformat(),
            event,
            setup.get("ticker"),
            shares,
            setup.get("entry"),
            setup.get("stop"),
            setup.get("target"),
            exit_price,
            round(pnl, 2),
            setup.get("rr_ratio"),
            reason,
        ])


# =============================================================================
# MAIN SCAN LOOP
# =============================================================================
def scan_and_enter(universe: list[str], active: dict):
    """Scan universe for setups, enter trades if signals found and slots available."""
    if len(active) >= MAX_POSITIONS:
        log.info(f"Max positions reached ({MAX_POSITIONS}), skipping scan")
        return

    if not check_spy_market_gate():
        log.info("SPY gate CLOSED — no new entries today")
        return

    log.info(f"Scanning {len(universe)} tickers...")
    candidates = []
    for ticker in universe:
        if ticker in active:
            continue  # already in position
        df = get_daily_bars(ticker)
        if df is None:
            continue
        setup = check_bone_zone_setup(ticker, df)
        if setup:
            candidates.append(setup)

    if not candidates:
        log.info("No setups found this scan")
        return

    # Rank by R/R ratio descending
    candidates.sort(key=lambda s: s["rr_ratio"], reverse=True)
    log.info(f"Found {len(candidates)} setups. Top 10:")
    for i, s in enumerate(candidates[:10]):
        log.info(f"  {i+1}. {s['ticker']} entry=${s['entry']} stop=${s['stop']} target=${s['target']} R/R={s['rr_ratio']}")

    # Enter trades up to MAX_POSITIONS
    equity = get_account_equity()
    slots_available = MAX_POSITIONS - len(active)
    for setup in candidates[:slots_available]:
        shares = calc_position_size(setup["entry"], setup["stop"], equity)
        order_id = place_bracket_order(setup, shares)
        if order_id:
            active[setup["ticker"]] = {
                **setup,
                "shares": shares,
                "order_id": order_id,
                "entered_at": now_et().isoformat(),
                "stage": "RUNNING",
            }
            log_trade("ENTRY", setup, shares=shares)
    save_active_positions(active)


def manage_positions(active: dict):
    """
    Check each active position and update trailing stop logic.
    Bracket orders handle stop/target automatically via Alpaca.
    We add: move stop to breakeven at 1:1, trail 9 EMA after 2:1.
    """
    if not active:
        return
    for ticker, pos in list(active.items()):
        try:
            current_price = get_latest_price(ticker)
            if current_price is None:
                continue

            entry = pos["entry"]
            stop = pos["stop"]
            target = pos["target"]
            risk = entry - stop

            unrealized_rr = (current_price - entry) / risk if risk > 0 else 0

            # Move stop to breakeven at 1:1
            if unrealized_rr >= 1.0 and pos.get("stage") == "RUNNING":
                new_stop = round(entry * 1.001, 2)  # tiny above entry
                update_stop_loss(ticker, pos, new_stop)
                pos["stop"] = new_stop
                pos["stage"] = "BREAKEVEN"
                log.info(f"💵 {ticker} hit 1:1, stop moved to breakeven ${new_stop}")

            # After 2:1, trail 9 EMA
            if unrealized_rr >= 2.0 and pos.get("stage") in ("RUNNING", "BREAKEVEN"):
                df = get_daily_bars(ticker, days=60)
                if df is not None and len(df) > 20:
                    df["ema9"] = ema(df["close"], 9)
                    ema9_now = df.iloc[-1]["ema9"]
                    trail_stop = round(ema9_now * 0.995, 2)
                    if trail_stop > pos["stop"]:
                        update_stop_loss(ticker, pos, trail_stop)
                        pos["stop"] = trail_stop
                        pos["stage"] = "TRAILING"
                        log.info(f"📈 {ticker} trailing 9 EMA, stop moved to ${trail_stop}")
        except Exception as e:
            log.error(f"manage {ticker} failed: {e}")
    save_active_positions(active)


def update_stop_loss(ticker: str, pos: dict, new_stop: float):
    """
    Cancel the existing stop-loss child order in the bracket and submit a new one.
    Alpaca bracket orders: the stop-loss is a child order we can replace.
    """
    try:
        # Get the bracket's child orders
        orders = trading.get_orders(filter=GetOrdersRequest(status="all", symbols=[ticker]))
        for o in orders:
            if o.order_type == "stop" and o.side == OrderSide.SELL and o.status in [OrderStatus.NEW, OrderStatus.ACCEPTED, OrderStatus.HELD]:
                # Replace via cancel + new stop order
                trading.cancel_order_by_id(o.id)
                time.sleep(0.5)
                new_order = StopOrderRequest(
                    symbol=ticker,
                    qty=pos["shares"],
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY,
                    stop_price=new_stop,
                )
                trading.submit_order(new_order)
                break
    except Exception as e:
        log.error(f"update stop for {ticker} failed: {e}")


def force_exit_all(active: dict):
    """Close all positions at end of day."""
    if not active:
        return
    log.info("🔔 EOD force-exit triggered")
    for ticker, pos in list(active.items()):
        try:
            # Close position at market
            trading.close_position(ticker)
            # Fetch exit price
            exit_price = get_latest_price(ticker) or pos["entry"]
            pnl = (exit_price - pos["entry"]) * pos["shares"]
            log.info(f"🚪 EOD exit {ticker}: {pos['shares']} shares @ ~${exit_price:.2f}, P&L=${pnl:.2f}")
            log_trade("EXIT_EOD", pos, shares=pos["shares"], exit_price=exit_price, pnl=pnl, reason="EOD force exit")
        except Exception as e:
            log.error(f"force exit {ticker} failed: {e}")
    active.clear()
    save_active_positions(active)


# =============================================================================
# DASHBOARD
# =============================================================================
def render_dashboard():
    """Generate a simple HTML dashboard."""
    active = load_active_positions()

    # Load trade log
    closed_trades = []
    if TRADE_LOG_CSV.exists():
        df = pd.read_csv(TRADE_LOG_CSV)
        closed_trades = df[df["event"].str.startswith("EXIT")].to_dict("records")

    total_pnl = sum(t.get("pnl", 0) for t in closed_trades)
    today_str = now_et().strftime("%Y-%m-%d")
    today_trades = [t for t in closed_trades if t.get("timestamp", "").startswith(today_str)]
    today_pnl = sum(t.get("pnl", 0) for t in today_trades)
    wins = sum(1 for t in closed_trades if t.get("pnl", 0) > 0)
    losses = sum(1 for t in closed_trades if t.get("pnl", 0) < 0)
    win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0

    try:
        equity = get_account_equity()
    except Exception:
        equity = 0

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>Pullback Swinger Dashboard</title>
<meta http-equiv="refresh" content="30">
<style>
body {{ font-family: -apple-system, sans-serif; background: #0d1222; color: #e4e9f2; margin: 0; padding: 24px; }}
h1 {{ margin: 0 0 4px; }}
.subtitle {{ color: #8a94a6; font-size: 13px; margin-bottom: 20px; }}
.stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }}
.stat {{ background: #141a2e; padding: 16px; border-radius: 8px; }}
.stat-label {{ color: #8a94a6; font-size: 11px; text-transform: uppercase; }}
.stat-value {{ font-size: 22px; font-weight: 700; margin-top: 4px; }}
.green {{ color: #4ade80; }}
.red {{ color: #f87171; }}
.yellow {{ color: #fbbf24; }}
table {{ width: 100%; border-collapse: collapse; background: #141a2e; border-radius: 8px; overflow: hidden; margin-bottom: 24px; }}
th {{ text-align: left; padding: 10px 12px; background: #1e2740; font-size: 11px; text-transform: uppercase; color: #8a94a6; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #1e2740; font-size: 13px; }}
tr:last-child td {{ border-bottom: none; }}
h2 {{ margin: 24px 0 12px; font-size: 16px; text-transform: uppercase; color: #8a94a6; letter-spacing: 0.5px; }}
</style>
</head><body>
<h1>Pullback Swinger Bot</h1>
<div class="subtitle">Kunal Desai Bone Zone Strategy · Updated {now_et().strftime('%Y-%m-%d %H:%M:%S ET')} · Auto-refresh 30s</div>

<div class="stats">
  <div class="stat"><div class="stat-label">Account Equity</div><div class="stat-value">${equity:,.2f}</div></div>
  <div class="stat"><div class="stat-label">Today P&L</div><div class="stat-value {'green' if today_pnl >= 0 else 'red'}">${today_pnl:+,.2f}</div></div>
  <div class="stat"><div class="stat-label">Cumulative P&L</div><div class="stat-value {'green' if total_pnl >= 0 else 'red'}">${total_pnl:+,.2f}</div></div>
  <div class="stat"><div class="stat-label">Active Positions</div><div class="stat-value yellow">{len(active)} / {MAX_POSITIONS}</div></div>
  <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value">{win_rate:.1f}% ({wins}W / {losses}L)</div></div>
  <div class="stat"><div class="stat-label">Total Trades</div><div class="stat-value">{len(closed_trades)}</div></div>
</div>

<h2>Active Positions</h2>
<table><thead><tr>
<th>Ticker</th><th>Shares</th><th>Entry</th><th>Stop</th><th>Target</th><th>R/R</th><th>Stage</th><th>Entered</th>
</tr></thead><tbody>
"""
    for t, p in active.items():
        html += f"<tr><td><strong>{t}</strong></td><td>{p.get('shares','?')}</td><td>${p.get('entry','?')}</td><td>${p.get('stop','?')}</td><td>${p.get('target','?')}</td><td>{p.get('rr_ratio','?')}:1</td><td>{p.get('stage','?')}</td><td>{p.get('entered_at','?')[:16]}</td></tr>"
    if not active:
        html += "<tr><td colspan='8' style='text-align:center;color:#8a94a6;'>No active positions</td></tr>"
    html += "</tbody></table>"

    html += "<h2>Today's Closed Trades</h2>"
    html += "<table><thead><tr><th>Time</th><th>Event</th><th>Ticker</th><th>Shares</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Reason</th></tr></thead><tbody>"
    for t in today_trades[-20:][::-1]:
        pnl_val = t.get("pnl", 0)
        pnl_cls = "green" if pnl_val >= 0 else "red"
        html += f"<tr><td>{str(t.get('timestamp',''))[11:19]}</td><td>{t.get('event','')}</td><td><strong>{t.get('ticker','')}</strong></td><td>{t.get('shares','')}</td><td>${t.get('entry','')}</td><td>${t.get('exit_price','')}</td><td class='{pnl_cls}'>${pnl_val:+.2f}</td><td>{t.get('reason','')}</td></tr>"
    if not today_trades:
        html += "<tr><td colspan='8' style='text-align:center;color:#8a94a6;'>No closed trades today yet</td></tr>"
    html += "</tbody></table>"

    html += "<h2>Recent Closed Trades (last 30)</h2>"
    html += "<table><thead><tr><th>Time</th><th>Ticker</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Reason</th></tr></thead><tbody>"
    for t in closed_trades[-30:][::-1]:
        pnl_val = t.get("pnl", 0)
        pnl_cls = "green" if pnl_val >= 0 else "red"
        html += f"<tr><td>{str(t.get('timestamp',''))[:16]}</td><td><strong>{t.get('ticker','')}</strong></td><td>${t.get('entry','')}</td><td>${t.get('exit_price','')}</td><td class='{pnl_cls}'>${pnl_val:+.2f}</td><td>{t.get('reason','')}</td></tr>"
    if not closed_trades:
        html += "<tr><td colspan='6' style='text-align:center;color:#8a94a6;'>No trades yet</td></tr>"
    html += "</tbody></table>"

    html += "</body></html>"
    DASHBOARD_HTML.write_text(html)


# =============================================================================
# MAIN LOOP
# =============================================================================
def main():
    log.info("=" * 70)
    log.info("Pullback Swinger Bot starting — Kunal Desai Bone Zone Strategy")
    log.info("=" * 70)
    log.info(f"Paper account: {ALPACA_BASE_URL}")
    try:
        acc = trading.get_account()
        log.info(f"Connected. Equity=${float(acc.equity):,.2f}  Buying Power=${float(acc.buying_power):,.2f}")
    except Exception as e:
        log.error(f"Alpaca connection failed: {e}")
        return

    universe = []
    universe_built_date = None
    last_scan = 0
    last_dashboard = 0

    while True:
        try:
            now = now_et()
            date_today = now.strftime("%Y-%m-%d")

            # Build universe at 9:00 AM ET each day
            if (universe_built_date != date_today) and now.strftime("%H:%M") >= "09:00":
                universe = build_universe()
                universe_built_date = date_today

            active = load_active_positions()
            active = sync_with_alpaca_positions(active)

            if is_market_open():
                # Position management runs always when market is open
                manage_positions(active)

                # Scanning only during trading window
                if in_trading_window() and (time.time() - last_scan) >= SCAN_INTERVAL_SEC:
                    if universe:
                        scan_and_enter(universe, active)
                    last_scan = time.time()

                # Force exit near close
                if past_force_exit():
                    force_exit_all(active)

            # Dashboard every 30 seconds
            if (time.time() - last_dashboard) >= 30:
                render_dashboard()
                last_dashboard = time.time()

            # Status snapshot every 10 min
            if int(time.time()) % 600 < 30:
                log.info(f"📊 Status: market_open={is_market_open()} in_window={in_trading_window()} active={len(active)} universe={len(universe)}")

            time.sleep(30)
        except KeyboardInterrupt:
            log.info("Shutdown requested")
            break
        except Exception as e:
            log.exception(f"Main loop error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
