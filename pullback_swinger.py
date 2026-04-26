"""
Pullback Swinger — IV Crush Iron Condor Bot
=============================================
Trades iron condors on mega-cap earnings to capture IV crush.

Strategy (locked from 24-month backtest):
  - Tickers: AAPL, JPM, WMT, NVDA (88-90% WR)
  - Setup: Iron condor 5% OTM short, $5 wide wings
  - Entry: 3:55 PM ET on AMC earnings days, prior day 3:55 PM for BMO
  - Exit: Next morning at 9:35 AM ET (after IV crushes)
  - Capital: $1,000, ~2 contracts per event

Backtest results:
  - 29 trades, 89.7% WR, $70 avg per IC, $2,037 total over 24 months
  - Realistic projection: ~$130-160/month after slippage

Render:
  - Repo: Pullback-Swinger
  - Build: pip install -r requirements1.txt
  - Start: pip3 install --break-system-packages -r requirements1.txt && python3 pullback_swinger.py
"""

import os
import json
import time
import math
import requests
from datetime import datetime, date, timedelta, timezone

try:
    import yfinance as yf
    import pandas as pd
    from zoneinfo import ZoneInfo
except ImportError:
    import subprocess
    subprocess.check_call(["pip3", "install", "--quiet", "--break-system-packages",
                           "yfinance", "pandas", "tzdata"])
    import yfinance as yf
    import pandas as pd
    from zoneinfo import ZoneInfo


# ============================================================
# CONFIG
# ============================================================

ALPACA_KEY = "AKBGJ7GBUS3UO7LYNZX644HBBV"
ALPACA_SECRET = "GLQeisiZcQZCtgBgSjevWhoPk3EtMkDBZG1qDVC8W7UQ"
ALPACA_BASE = "https://paper-api.alpaca.markets/v2"
ALPACA_DATA = "https://data.alpaca.markets/v2"

ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
    "Content-Type": "application/json",
}

TELEGRAM_TOKEN = "8437072494:AAE4ZEEUiJo77u6WWgu19DhkOq-EFxnYsBs"
TELEGRAM_CHAT = "8584742497"

# Strategy params (locked)
TICKERS = ["AAPL", "JPM", "WMT", "NVDA"]
OTM_PCT = 5.0
WING_WIDTH = 5.0
PAPER_CAPITAL = 1000.0
CAPITAL_USE_PCT = 0.90  # use 90% of $1K per event
ENTRY_HOUR = 15
ENTRY_MIN = 55
EXIT_HOUR = 9
EXIT_MIN = 35

# Earnings calendar refresh
EARNINGS_REFRESH_HOUR = 9
EARNINGS_REFRESH_MIN = 0

STATE_FILE = "/tmp/pullback_state.json"
HEARTBEAT_MINUTES = 15

ET = ZoneInfo("America/New_York")


# ============================================================
# UTILITIES
# ============================================================

def log(msg):
    ts = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def now_et():
    return datetime.now(ET)


def is_market_hours():
    et = now_et()
    if et.weekday() >= 5:
        return False
    h, m = et.hour, et.minute
    if h < 9 or (h == 9 and m < 30):
        return False
    if h >= 16:
        return False
    return True


def telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT, "text": f"[IC] {msg}"},
                      timeout=10)
    except Exception as e:
        log(f"Telegram fail: {e}")


def load_state():
    if not os.path.exists(STATE_FILE):
        return new_state()
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception as e:
        log(f"State load fail: {e}, using fresh")
        return new_state()


def save_state(state):
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, default=str)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log(f"State save fail: {e}")


def new_state():
    return {
        "today": None,
        "earnings_calendar": {},     # ticker -> {"date": iso, "timing": "AMC"|"BMO"}
        "earnings_refreshed_today": False,
        "positions": {},              # ticker -> position dict
        "evaluated_today": {},        # ticker -> bool (already evaluated for entry today)
        "last_heartbeat": None,
        "startup_done": False,
    }


# ============================================================
# ALPACA HELPERS
# ============================================================

def alpaca_get(path, params=None, base=None):
    base = base or ALPACA_BASE
    url = f"{base}{path}"
    try:
        r = requests.get(url, headers=ALPACA_HEADERS, params=params, timeout=20)
        if r.status_code != 200:
            log(f"Alpaca GET {path} -> {r.status_code}: {r.text[:200]}")
            return None
        return r.json()
    except Exception as e:
        log(f"Alpaca GET {path} exception: {e}")
        return None


def alpaca_post(path, data):
    url = f"{ALPACA_BASE}{path}"
    try:
        r = requests.post(url, headers=ALPACA_HEADERS, json=data, timeout=20)
        if r.status_code not in (200, 201):
            log(f"Alpaca POST {path} -> {r.status_code}: {r.text[:300]}")
            return None
        return r.json()
    except Exception as e:
        log(f"Alpaca POST {path} exception: {e}")
        return None


def alpaca_account():
    return alpaca_get("/account")


def get_open_positions():
    pos = alpaca_get("/positions")
    return pos or []


def get_stock_quote(symbol):
    """Get latest stock price from Alpaca."""
    data = alpaca_get(f"/stocks/{symbol}/snapshot", base=ALPACA_DATA)
    if not data:
        return None
    quote = data.get("latestQuote") or {}
    trade = data.get("latestTrade") or {}
    # Prefer mid quote, fall back to trade price
    bid = quote.get("bp")
    ask = quote.get("ap")
    if bid and ask and bid > 0 and ask > 0:
        return (bid + ask) / 2
    return trade.get("p")


def get_option_contracts(underlying, expiry):
    """Get option contracts for underlying expiring on/around target date.
       expiry: ISO date string YYYY-MM-DD
    """
    params = {
        "underlying_symbols": underlying,
        "expiration_date": expiry,
        "status": "active",
        "limit": 1000,
    }
    data = alpaca_get("/options/contracts", params=params)
    if not data:
        return []
    return data.get("option_contracts", [])


def get_option_quote(option_symbol):
    """Get option quote from Alpaca options data."""
    data = alpaca_get(f"/options/snapshots/{option_symbol}",
                       base=ALPACA_DATA)
    if not data:
        return None, None
    # Snapshot keyed by symbol
    snap = data.get("snapshots", {}).get(option_symbol)
    if not snap:
        # Or sometimes returned at top level
        snap = data
    quote = snap.get("latestQuote") or {}
    bid = quote.get("bp")
    ask = quote.get("ap")
    return bid, ask


def submit_iron_condor(ticker, expiry, p_short, p_long, c_short, c_long, qty):
    """Submit a 4-leg iron condor as a single multi-leg order on Alpaca.
       Sells the inner strikes (p_short, c_short), buys the outer wings.
    """
    # Build option symbols (OCC format): SYMBOL + YYMMDD + C/P + 8-digit strike (price * 1000)
    def occ(sym, exp_iso, strike, opt_type):
        d = date.fromisoformat(exp_iso)
        return f"{sym}{d.strftime('%y%m%d')}{opt_type}{int(round(strike * 1000)):08d}"

    legs = [
        {"symbol": occ(ticker, expiry, p_short, "P"), "ratio_qty": "1",
         "side": "sell", "position_intent": "sell_to_open"},
        {"symbol": occ(ticker, expiry, p_long, "P"), "ratio_qty": "1",
         "side": "buy", "position_intent": "buy_to_open"},
        {"symbol": occ(ticker, expiry, c_short, "C"), "ratio_qty": "1",
         "side": "sell", "position_intent": "sell_to_open"},
        {"symbol": occ(ticker, expiry, c_long, "C"), "ratio_qty": "1",
         "side": "buy", "position_intent": "buy_to_open"},
    ]

    payload = {
        "order_class": "mleg",
        "qty": str(qty),
        "type": "market",
        "time_in_force": "day",
        "legs": legs,
    }
    log(f"Submitting IC: {ticker} {expiry} P{p_long}/{p_short} C{c_short}/{c_long} x{qty}")
    return alpaca_post("/orders", payload)


def submit_iron_condor_close(ticker, expiry, p_short, p_long, c_short, c_long, qty):
    """Close the iron condor by reversing all 4 legs."""
    def occ(sym, exp_iso, strike, opt_type):
        d = date.fromisoformat(exp_iso)
        return f"{sym}{d.strftime('%y%m%d')}{opt_type}{int(round(strike * 1000)):08d}"

    legs = [
        {"symbol": occ(ticker, expiry, p_short, "P"), "ratio_qty": "1",
         "side": "buy", "position_intent": "buy_to_close"},
        {"symbol": occ(ticker, expiry, p_long, "P"), "ratio_qty": "1",
         "side": "sell", "position_intent": "sell_to_close"},
        {"symbol": occ(ticker, expiry, c_short, "C"), "ratio_qty": "1",
         "side": "buy", "position_intent": "buy_to_close"},
        {"symbol": occ(ticker, expiry, c_long, "C"), "ratio_qty": "1",
         "side": "sell", "position_intent": "sell_to_close"},
    ]
    payload = {
        "order_class": "mleg",
        "qty": str(qty),
        "type": "market",
        "time_in_force": "day",
        "legs": legs,
    }
    log(f"Closing IC: {ticker} {expiry} x{qty}")
    return alpaca_post("/orders", payload)


# ============================================================
# EARNINGS CALENDAR (yfinance scan)
# ============================================================

def fetch_next_earnings(ticker):
    """Get next earnings date + timing (AMC/BMO) for a ticker via yfinance.
       Returns (date_iso, "AMC"|"BMO") or None.
    """
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        if cal is None:
            return None
        # Calendar is a dict with "Earnings Date" key
        if isinstance(cal, dict):
            dates = cal.get("Earnings Date", [])
            if not dates:
                return None
            # Use earliest future date
            today = date.today()
            future_dates = [d for d in dates
                              if isinstance(d, (date, pd.Timestamp)) and
                              (d.date() if isinstance(d, pd.Timestamp) else d) >= today]
            if not future_dates:
                return None
            target = future_dates[0]
            d_iso = (target.date() if isinstance(target, pd.Timestamp) else target).isoformat()
        else:
            return None

        # Determine AMC/BMO from earningsCallTimestamp if available
        # Default: AAPL, NVDA = AMC; JPM, WMT = BMO
        amc_default = {"AAPL": "AMC", "NVDA": "AMC", "JPM": "BMO", "WMT": "BMO"}
        timing = amc_default.get(ticker, "AMC")

        return (d_iso, timing)
    except Exception as e:
        log(f"yfinance earnings fetch fail {ticker}: {e}")
        return None


def refresh_earnings_calendar(state):
    """Once-daily scan for next earnings dates."""
    et = now_et()
    today_iso = et.strftime("%Y-%m-%d")

    if state.get("earnings_refreshed_today"):
        return

    if et.hour < EARNINGS_REFRESH_HOUR:
        return  # Wait until 9am ET

    log("Refreshing earnings calendar...")
    cal = state.get("earnings_calendar", {})
    new_detections = []

    for ticker in TICKERS:
        result = fetch_next_earnings(ticker)
        if not result:
            log(f"  {ticker}: no earnings date found")
            continue
        date_iso, timing = result
        old_entry = cal.get(ticker)
        if old_entry is None or old_entry.get("date") != date_iso:
            new_detections.append((ticker, date_iso, timing))
            cal[ticker] = {"date": date_iso, "timing": timing}
            log(f"  {ticker}: {date_iso} ({timing}) — NEW/UPDATED")
        else:
            log(f"  {ticker}: {date_iso} ({timing})")

    state["earnings_calendar"] = cal
    state["earnings_refreshed_today"] = True
    save_state(state)

    if new_detections:
        msg_lines = ["Earnings detected:"]
        for ticker, d, timing in new_detections:
            msg_lines.append(f"  {ticker}: {d} ({timing})")
        telegram("\n".join(msg_lines))


# ============================================================
# IRON CONDOR EXECUTION
# ============================================================

def get_target_expiry(earnings_date_iso):
    """Find next Friday on or after the day after earnings."""
    d = date.fromisoformat(earnings_date_iso)
    target = d + timedelta(days=1)
    while target.weekday() != 4:  # Friday
        target += timedelta(days=1)
    return target.isoformat()


def find_strikes_for_ic(ticker, expiry):
    """Find 4 strikes for iron condor at 5% OTM, $5 wide.
       Returns (p_short, p_long, c_short, c_long, stock_price) or None.
    """
    stock_price = get_stock_quote(ticker)
    if not stock_price:
        log(f"  {ticker}: no stock price")
        return None
    log(f"  {ticker}: stock @ ${stock_price:.2f}")

    contracts = get_option_contracts(ticker, expiry)
    if not contracts:
        log(f"  {ticker}: no contracts found for {expiry}")
        return None

    # Build strike sets
    put_strikes = sorted({float(c["strike_price"]) for c in contracts
                          if c.get("type") == "put"})
    call_strikes = sorted({float(c["strike_price"]) for c in contracts
                           if c.get("type") == "call"})

    # Targets
    target_p = stock_price * (1 - OTM_PCT / 100)
    target_c = stock_price * (1 + OTM_PCT / 100)

    # Short put: closest strike below target_p
    p_candidates = [s for s in put_strikes if s <= target_p]
    if not p_candidates:
        log(f"  {ticker}: no put strikes below ${target_p:.2f}")
        return None
    p_short = max(p_candidates)
    p_long = p_short - WING_WIDTH
    if p_long not in put_strikes:
        below = [s for s in put_strikes if s <= p_long]
        if not below:
            log(f"  {ticker}: no put long wing at ${p_long}")
            return None
        p_long = max(below)

    # Short call: closest above target_c
    c_candidates = [s for s in call_strikes if s >= target_c]
    if not c_candidates:
        log(f"  {ticker}: no call strikes above ${target_c:.2f}")
        return None
    c_short = min(c_candidates)
    c_long = c_short + WING_WIDTH
    if c_long not in call_strikes:
        above = [s for s in call_strikes if s >= c_long]
        if not above:
            log(f"  {ticker}: no call long wing at ${c_long}")
            return None
        c_long = min(above)

    log(f"  {ticker}: P{p_long}/{p_short} | C{c_short}/{c_long}")
    return (p_short, p_long, c_short, c_long, stock_price)


def calculate_position_size(wing_width):
    """How many contracts can we afford with $1K capital?"""
    cap_per_ic = wing_width * 100  # max risk per IC
    available = PAPER_CAPITAL * CAPITAL_USE_PCT
    return max(1, int(available // cap_per_ic))


def evaluate_and_trade(state):
    """For each ticker, check if today is its earnings entry day. If so, enter IC."""
    et = now_et()
    today_iso = et.strftime("%Y-%m-%d")
    cal = state.get("earnings_calendar", {})

    for ticker in TICKERS:
        # Already in position?
        if ticker in state.get("positions", {}):
            continue
        # Already evaluated today?
        if state.get("evaluated_today", {}).get(ticker):
            continue

        entry = cal.get(ticker)
        if not entry:
            continue
        e_date = entry["date"]
        e_timing = entry["timing"]

        # Determine if today is the entry day
        # AMC: enter at close of earnings day
        # BMO: enter at close of day BEFORE earnings
        if e_timing == "AMC":
            entry_date_iso = e_date
        else:
            d = date.fromisoformat(e_date)
            entry_date_iso = (d - timedelta(days=1)).isoformat()
            # Skip back over weekends
            while date.fromisoformat(entry_date_iso).weekday() >= 5:
                entry_date_iso = (date.fromisoformat(entry_date_iso) -
                                    timedelta(days=1)).isoformat()

        if entry_date_iso != today_iso:
            continue

        # Time check: must be at/after 3:55 PM ET
        if et.hour < ENTRY_HOUR or (et.hour == ENTRY_HOUR and et.minute < ENTRY_MIN):
            continue

        log(f"{ticker}: ENTRY DAY ({e_timing}, earnings {e_date})")

        expiry = get_target_expiry(e_date)
        strikes = find_strikes_for_ic(ticker, expiry)
        if not strikes:
            state.setdefault("evaluated_today", {})[ticker] = True
            save_state(state)
            continue

        p_short, p_long, c_short, c_long, stock_price = strikes
        qty = calculate_position_size(WING_WIDTH)

        result = submit_iron_condor(ticker, expiry, p_short, p_long, c_short, c_long, qty)
        if not result:
            log(f"  {ticker}: order failed")
            state.setdefault("evaluated_today", {})[ticker] = True
            save_state(state)
            continue

        order_id = result.get("id")
        state.setdefault("positions", {})[ticker] = {
            "order_id": order_id,
            "expiry": expiry,
            "p_short": p_short, "p_long": p_long,
            "c_short": c_short, "c_long": c_long,
            "qty": qty,
            "stock_at_entry": round(stock_price, 2),
            "entry_date": today_iso,
            "earnings_date": e_date,
            "timing": e_timing,
            "entry_time": et.strftime("%Y-%m-%d %H:%M:%S"),
        }
        state.setdefault("evaluated_today", {})[ticker] = True
        save_state(state)

        msg = (f"IC ENTRY {ticker}\n"
               f"Earnings: {e_date} {e_timing}\n"
               f"Stock: ${stock_price:.2f}\n"
               f"Strikes: P{p_long}/{p_short} | C{c_short}/{c_long}\n"
               f"Qty: {qty} | Expiry: {expiry}")
        telegram(msg)


def monitor_positions(state):
    """At 9:35 AM ET, close any open IC positions."""
    et = now_et()
    if et.hour != EXIT_HOUR or et.minute < EXIT_MIN:
        return

    positions = state.get("positions", {})
    if not positions:
        return

    today_iso = et.strftime("%Y-%m-%d")

    for ticker in list(positions.keys()):
        pos = positions[ticker]
        # Don't exit on the same day we entered
        if pos.get("entry_date") == today_iso:
            continue

        log(f"{ticker}: closing IC position")
        result = submit_iron_condor_close(
            ticker, pos["expiry"],
            pos["p_short"], pos["p_long"],
            pos["c_short"], pos["c_long"],
            pos["qty"]
        )
        if not result:
            log(f"  {ticker}: close order failed (will retry next tick)")
            continue

        msg = (f"IC EXIT {ticker}\n"
               f"Closing P{pos['p_long']}/{pos['p_short']} | "
               f"C{pos['c_short']}/{pos['c_long']}\n"
               f"Qty: {pos['qty']}")
        telegram(msg)
        del positions[ticker]
        save_state(state)


# ============================================================
# DAILY RESET / STARTUP
# ============================================================

def reset_daily(state):
    today = now_et().strftime("%Y-%m-%d")
    if state.get("today") != today:
        log(f"=== New day: {today} ===")
        state["today"] = today
        state["earnings_refreshed_today"] = False
        state["evaluated_today"] = {}
        save_state(state)


def startup_check(state):
    if state.get("startup_done"):
        return
    log("=" * 60)
    log("Pullback Swinger starting")
    log(f"Tickers: {', '.join(TICKERS)}")
    log(f"Capital: ${PAPER_CAPITAL} (90%)")
    log(f"Setup: {OTM_PCT}% OTM, ${WING_WIDTH} wide")
    log(f"Entry: {ENTRY_HOUR}:{ENTRY_MIN} ET | Exit: {EXIT_HOUR}:{EXIT_MIN} ET")
    log("=" * 60)
    acct = alpaca_account()
    if acct:
        log(f"Alpaca paper acct: ${acct.get('portfolio_value', 'N/A')}, "
            f"BP: ${acct.get('buying_power', 'N/A')}")
    pos = get_open_positions()
    log(f"Open positions on Alpaca: {len(pos)}")
    state["startup_done"] = True
    save_state(state)
    telegram(f"Bot started: IV crush IC on {', '.join(TICKERS)}, ${PAPER_CAPITAL} capital")


# ============================================================
# HEARTBEAT
# ============================================================

def heartbeat(state):
    et = now_et()
    last = state.get("last_heartbeat")
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=ET)
            if (et - last_dt).total_seconds() < HEARTBEAT_MINUTES * 60:
                return
        except Exception:
            pass

    cal = state.get("earnings_calendar", {})
    cal_str = ", ".join(f"{t}:{v['date']}" for t, v in cal.items()) or "none"
    pos_count = len(state.get("positions", {}))
    log(f"STATUS: {et.strftime('%H:%M ET')} | positions: {pos_count} | "
        f"calendar: {cal_str}")
    state["last_heartbeat"] = et.isoformat()
    save_state(state)


# ============================================================
# MAIN
# ============================================================

def main_tick():
    state = load_state()
    reset_daily(state)
    startup_check(state)

    if not is_market_hours():
        return

    refresh_earnings_calendar(state)
    evaluate_and_trade(state)
    monitor_positions(state)
    heartbeat(state)


def main():
    log("Pullback Swinger main loop started")
    while True:
        try:
            main_tick()
        except Exception as e:
            log(f"Tick exception: {e}")
            import traceback
            traceback.print_exc()
        time.sleep(60)


if __name__ == "__main__":
    main()
