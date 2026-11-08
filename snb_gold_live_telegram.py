#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SNB GOLD LIVE — H1 structure + M2 immediate confirmation.

Gold-only Telegram signal bot. H1 structure + M2 immediate entries.
This bot sends alerts only; it never places broker orders.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from aiohttp import web

# ---------------- Configuration ----------------
TZ = os.getenv("TIMEZONE", "Asia/Riyadh")
INSTRUMENT_NAME = "INSTRUMENT_FX_METALS_XAU_USD"
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"

DATA_DIR = Path(os.getenv("SNB_DATA_DIR", str(Path.home() / "snb_gold_data")))
STATE_FILE = DATA_DIR / "snb_gold_state.json"
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "30"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))                  # فحص الإشارات الجديدة
LIVE_POLL_SECONDS = int(os.getenv("LIVE_POLL_SECONDS", "3"))        # متابعة السعر اللحظي
LIVE_WINDOW_MINUTES = int(os.getenv("LIVE_WINDOW_MINUTES", "15"))   # نافذة الجلب الخفيف
FULL_REFRESH_SECONDS = int(os.getenv("FULL_REFRESH_SECONDS", "60")) # جلب كامل لتصحيح الكاش
SIGNAL_MAX_AGE_MINUTES = int(os.getenv("SIGNAL_MAX_AGE_MINUTES", "2"))  # تجاهل الإشارات الأقدم (بعد إعادة التشغيل)
SIGNAL_DIAGNOSTICS_SECONDS = int(os.getenv("SIGNAL_DIAGNOSTICS_SECONDS", "300"))
PENDING_EXPIRY_HOURS = int(os.getenv("PENDING_EXPIRY_HOURS", "2"))
LEVEL_EXPIRY_HOURS = int(os.getenv("LEVEL_EXPIRY_HOURS", "4"))
EQUAL_TOLERANCE = float(os.getenv("EQUAL_TOLERANCE", "0.10"))
FIXED_STOP_DOLLARS = float(os.getenv("FIXED_STOP_DOLLARS", "4.0"))
PROGRESS_START_DOLLARS = float(os.getenv("PROGRESS_START_DOLLARS", "3.0"))
PROGRESS_STEP_DOLLARS = float(os.getenv("PROGRESS_STEP_DOLLARS", "2.0"))
NO_NEW_FIRST_HOUR = os.getenv("NO_NEW_FIRST_HOUR", "true").lower() == "true"
NO_NEW_LAST_HOUR = os.getenv("NO_NEW_LAST_HOUR", "true").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snb-gold-h1-m2")
STOP_REQUESTED = False


def stop_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log.info("Stop requested; state will be saved.")


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


# ---------------- Render health server ----------------
async def health(request):
    return web.json_response({"status": "ok", "service": "snb-gold"})


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/healthz", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("Health server listening on port %s", port)


# ---------------- State ----------------
def load_state() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            state.setdefault("started_message_sent", False)
            state.setdefault("trades", {})
            return state
        except Exception:
            log.exception("State file is invalid; starting a new state.")
    return {"started_message_sent": False, "trades": {}}


def save_state(state: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


# ---------------- Dukascopy data ----------------
def get_duka():
    try:
        import dukascopy_python as duka
        from dukascopy_python.instruments import INSTRUMENT_FX_METALS_XAU_USD
        return duka, INSTRUMENT_FX_METALS_XAU_USD
    except Exception as exc:
        raise RuntimeError(f"Dukascopy import failed: {exc!r}") from exc


def fetch_1m(duka, instrument, side: str, hours: float = LOOKBACK_HOURS, min_rows: int = 30) -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    start = now - pd.Timedelta(hours=hours)
    data = duka.fetch(
        instrument=instrument,
        interval=duka.INTERVAL_MIN_1,
        offer_side=getattr(duka, f"OFFER_SIDE_{side}"),
        start=start.to_pydatetime(),
        end=now.to_pydatetime(),
        max_retries=5,
        limit=30000,
        debug=False,
    )
    if data is None or data.empty:
        raise RuntimeError(f"No {side} candles returned by Dukascopy")
    data = data.copy()
    data.columns = [str(c).lower() for c in data.columns]
    idx = pd.DatetimeIndex(pd.to_datetime(data.index))
    data.index = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")
    data = data[~data.index.duplicated(keep="last")].sort_index()
    for col in ("open", "high", "low", "close"):
        data[col] = pd.to_numeric(data[col], errors="coerce")
    data = data.dropna(subset=["open", "high", "low", "close"])
    if len(data) < min_rows:
        raise RuntimeError(f"Only {len(data)} usable {side} candles returned")
    return data


def closed_resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    out = df.resample(f"{minutes}min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    ).dropna()
    now_floor = pd.Timestamp.now(tz="UTC").floor(f"{minutes}min")
    return out[out.index + pd.Timedelta(minutes=minutes) <= now_floor]


def merge_tail(old: Optional[pd.DataFrame], new: pd.DataFrame) -> pd.DataFrame:
    if old is None or old.empty:
        return new
    out = pd.concat([old[~old.index.isin(new.index)], new]).sort_index()
    return out[out.index >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=LOOKBACK_HOURS + 2)]


def local(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.tz_convert(TZ)


def local_day(ts: pd.Timestamp) -> str:
    return local(ts).date().isoformat()


def fmt_time(ts: pd.Timestamp | str) -> str:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.tz_convert(TZ).strftime("%Y-%m-%d %H:%M")


def allow_new(ts: pd.Timestamp) -> bool:
    h = local(ts).hour
    return not ((NO_NEW_FIRST_HOUR and h == 0) or (NO_NEW_LAST_HOUR and h == 23))


# ---------------- Telegram ----------------
class TelegramSender:
    def __init__(self):
        self.client = None

    async def start(self):
        if not TELEGRAM_ENABLED:
            log.warning("Telegram disabled: preview mode.")
            return
        if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_SESSION_STRING or not TELEGRAM_CHANNEL:
            raise RuntimeError("Missing Telegram environment variables")
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        self.client = TelegramClient(
            StringSession(TELEGRAM_SESSION_STRING),
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH,
            connection_retries=None,
            retry_delay=2,
            auto_reconnect=True,
        )
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STRING is invalid or expired")
        log.info("Telegram connected to %s", TELEGRAM_CHANNEL)

    async def send(self, text: str, reply_to: Optional[int] = None) -> Optional[int]:
        if not TELEGRAM_ENABLED:
            log.info("TELEGRAM PREVIEW:\n%s", text)
            return None
        msg = await self.client.send_message(TELEGRAM_CHANNEL, text, reply_to=reply_to)
        return int(msg.id)

    async def edit_and_reply(self, message_id: Optional[int], text: str, reply: str):
        if not message_id or not TELEGRAM_ENABLED:
            return
        try:
            await self.client.edit_message(TELEGRAM_CHANNEL, message_id, text)
            await self.send(reply, reply_to=message_id)
        except Exception:
            log.exception("Could not update Telegram message %s", message_id)

    async def close(self):
        if self.client:
            await self.client.disconnect()


# ---------------- Messages ----------------
def price(x: float) -> str:
    return f"{x:.2f}"


def side_word(side: str) -> str:
    return "BUY" if side == "buy" else "SELL"


def pending_text(tr: dict[str, Any]) -> str:
    arrow = "📈 BUY LIMIT" if tr["side"] == "buy" else "📉 SELL LIMIT"
    return (
        "🔔 **توصية جديدة | صفقة معلقة** 🔔\n\n"
        "🥇 **XAUUSD**\n"
        f"{arrow} ➜ **{price(tr['entry'])}**\n\n"
        f"🛑 وقف الخسارة: **{price(tr['stop'])}**\n\n"
        "🎯 الهدف: **مفتوح**\n\n"
        "⚠️ إدارة رأس المال مسؤوليتك\n"
        "💡 لا تخاطر بأكثر من 1-2%"
    )


def entry_text(tr: dict[str, Any]) -> str:
    arrow = "📈 BUY" if tr["side"] == "buy" else "📉 SELL"
    title = (
        "🔁 **إعادة دخول بعد وقف الخسارة** 🔁"
        if tr.get("attempt", 0) == 1
        else "🔔 **توصية جديدة | دخول فوري** 🔔"
    )
    return (
        f"{title}\n\n"
        "🥇 **XAUUSD**\n"
        f"{arrow} ➜ **{price(tr['entry'])}**\n\n"
        f"🛑 وقف الخسارة: **{price(tr['stop'])}**\n\n"
        "🎯 الهدف: **مفتوح**\n\n"
        "⚠️ إدارة رأس المال مسؤوليتك\n"
        "💡 لا تخاطر بأكثر من 1-2%"
    )


def cancelled_text(tr: dict[str, Any], reason: str) -> str:
    return (
        "❌ **تم إلغاء الأمر المعلق**\n\n"
        "🥇 **XAUUSD** | " + side_word(tr["side"]) + "\n"
        f"📍 المستوى: **{price(tr['entry'])}**\n"
        f"📝 السبب: {reason}\n\n"
        "🗂️ تم الاحتفاظ بسجل الصفقة ولم يُحذف."
    )


def activated_text(tr: dict[str, Any]) -> str:
    return (
        "✅ **تم تفعيل الصفقة** 🚀\n\n"
        f"🥇 XAUUSD | **{side_word(tr['side'])}**\n"
        f"📍 دخول عند **{price(tr['entry'])}**\n\n"
        "🎯 الهدف مفتوح — نتابع الحركة."
    )


def progress_text(tr: dict[str, Any], dollars: float) -> str:
    return f"🔥 **XAUUSD {side_word(tr['side'])}**\n💰 الحركة لصالح الصفقة: **+{dollars:.2f}$** (+{int(round(dollars * 10))} نقطة)"


def reversed_text(tr: dict[str, Any]) -> str:
    return (
        "🔁 **تم إيقاف متابعة الصفقة بسبب إشارة معاكسة**\n\n"
        f"🥇 XAUUSD | {side_word(tr['side'])}\n"
        f"📍 الدخول: **{price(tr['entry'])}**\n"
        "🗂️ تم الاحتفاظ بسجل الصفقة."
    )


def sl_text(tr: dict[str, Any]) -> str:
    return (
        "🛑 **انتهت متابعة الصفقة عند وقف الخسارة**\n\n"
        f"🥇 XAUUSD | {side_word(tr['side'])}\n"
        f"📍 الدخول: **{price(tr['entry'])}**\n"
        "🗂️ تم الاحتفاظ بسجل الصفقة."
    )


# ---------------- Strategy ----------------
def setup_key(ts: pd.Timestamp, side: str) -> str:
    return f"{ts.isoformat()}|{side}"


def detect_equal_levels(bid: pd.DataFrame) -> list[dict[str, Any]]:
    """Build today's H1 structural levels from adjacent same-color equal-point candles."""
    h1 = closed_resample(bid, 60)
    levels: list[dict[str, Any]] = []
    if len(h1) < 2:
        return levels

    now = pd.Timestamp.now(tz="UTC")
    for i in range(1, len(h1)):
        first, second = h1.iloc[i - 1], h1.iloc[i]
        formed = h1.index[i] + pd.Timedelta(minutes=60)
        if formed > now or not allow_new(h1.index[i]):
            continue
        if local_day(formed) != local_day(now):
            continue
        gap = abs(float(first.close) - float(second.open))
        if gap > EQUAL_TOLERANCE:
            continue
        if first.close > first.open and second.close > second.open:
            side = "buy"
        elif first.close < first.open and second.close < second.open:
            side = "sell"
        else:
            continue
        levels.append({
            "key": setup_key(h1.index[i], side),
            "side": side,
            "formed": formed.isoformat(),
            "level": round((float(first.close) + float(second.open)) / 2, 3),
            "expires": (formed + pd.Timedelta(hours=LEVEL_EXPIRY_HOURS)).isoformat(),
        })
    return levels


def find_m2_immediate_entry(
    level: dict[str, Any], bid: pd.DataFrame, ask: pd.DataFrame,
    diagnostics: Optional[dict[str, int]] = None,
) -> Optional[dict[str, Any]]:
    """After the H1 level is touched, enter at the M2 confirmation close (no limit order)."""
    def mark(name: str) -> None:
        if diagnostics is not None:
            diagnostics[name] = diagnostics.get(name, 0) + 1

    side = level["side"]
    formed = pd.Timestamp(level["formed"])
    expires = pd.Timestamp(level["expires"])
    now = pd.Timestamp.now(tz="UTC")

    touch_quote = ask if side == "buy" else bid
    touched = touch_quote[(touch_quote.index >= formed) & (touch_quote.index < expires)]
    touched = touched[touched.low <= level["level"]] if side == "buy" else touched[touched.high >= level["level"]]
    if touched.empty:
        mark("levels_not_touched")
        return None
    mark("levels_touched")
    touch = touched.index[0]

    m2_bid = closed_resample(bid, 2)
    m2_ask = closed_resample(ask, 2)
    first_full_bar = touch.ceil("2min")
    now_min = now.floor("1min")
    bars = m2_bid[
        (m2_bid.index >= first_full_bar)
        & (m2_bid.index < expires)
        & (m2_bid.index + pd.Timedelta(minutes=2) <= now_min)
    ]
    if len(bars) < 2:
        mark("not_enough_closed_m2_bars")
        return None

    # Search newest-to-oldest, but only accept adjacent, uninterrupted M2 candles.
    saw_equal_pair = False
    saw_same_color_pair = False
    for j in range(len(bars) - 1, 0, -1):
        if bars.index[j] - bars.index[j - 1] != pd.Timedelta(minutes=2):
            continue
        first, second = bars.iloc[j - 1], bars.iloc[j]
        if abs(float(first.close) - float(second.open)) > EQUAL_TOLERANCE:
            continue
        saw_equal_pair = True
        mark("m2_equal_point_pairs")
        same_color = (
            first.close > first.open and second.close > second.open
            if side == "buy"
            else first.close < first.open and second.close < second.open
        )
        if not same_color:
            continue
        saw_same_color_pair = True
        mark("m2_same_color_pairs")

        bar_start = bars.index[j]
        entry_time = bar_start + pd.Timedelta(minutes=2)
        # The second M2 candle is confirmed only at its close.
        confirmed_at = entry_time
        if local_day(confirmed_at) != local_day(now):
            mark("confirmation_wrong_local_day")
            continue
        age = now - entry_time
        if age > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES):
            mark("m2_confirmation_too_old")
            break
        if age < pd.Timedelta(0):
            mark("m2_confirmation_in_future")
            continue

        entry_bars = m2_ask if side == "buy" else m2_bid
        entry = round(float(entry_bars.at[bar_start, "close"]), 3)
        equal_point = round((float(first.close) + float(second.open)) / 2, 3)
        return {
            "key": level["key"] + "|m2|" + bar_start.isoformat(),
            "side": side,
            "level": level["level"],
            "formed": level["formed"],
            "touch_time": touch.isoformat(),
            "confirm_time": confirmed_at.isoformat(),
            "equal_point": equal_point,
            "entry": entry,
            "stop": round(
                entry - FIXED_STOP_DOLLARS if side == "buy" else entry + FIXED_STOP_DOLLARS,
                3,
            ),
            "expiry": expires.isoformat(),
            "status": "active",
            "entry_time": entry_time.isoformat(),
            "entry_source": "M2_CLOSE_IMMEDIATE",
            "attempt": 0,
            "message_id": None,
            "best": entry,
            "last_progress": 0.0,
            "created_at": now.isoformat(),
        }
    if not saw_equal_pair:
        mark("no_m2_equal_point_pair")
    elif not saw_same_color_pair:
        mark("no_m2_same_color_pair")
    return None


def detect_signals(
    bid: pd.DataFrame, ask: pd.DataFrame, state: dict[str, Any],
    diagnostics: Optional[dict[str, int]] = None,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    now = pd.Timestamp.now(tz="UTC")
    trades = state.get("trades", {})
    levels = detect_equal_levels(bid)
    if diagnostics is not None:
        diagnostics["h1_levels_today"] = len(levels)
    for level in levels:
        if any(t.get("setup_key") == level["key"] for t in trades.values()):
            if diagnostics is not None:
                diagnostics["setup_already_registered"] = diagnostics.get("setup_already_registered", 0) + 1
            continue
        tr = find_m2_immediate_entry(level, bid, ask, diagnostics)
        if not tr:
            continue
        age = now - pd.Timestamp(tr["entry_time"])
        if age < pd.Timedelta(0) or age > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES):
            if diagnostics is not None:
                diagnostics["signal_too_old_at_registration"] = diagnostics.get("signal_too_old_at_registration", 0) + 1
            continue
        tr["setup_key"] = level["key"]

        # Preserve the $4 nearby-level suppression used in the H1/M2 weekly test.
        blocked = False
        for old in trades.values():
            if old.get("side") != tr["side"] or old.get("setup_key") == tr["setup_key"]:
                continue
            old_formed = pd.Timestamp(old.get("formed", now))
            if old_formed + pd.Timedelta(hours=LEVEL_EXPIRY_HOURS) <= now:
                continue
            if abs(float(old.get("level", 0.0)) - float(tr["level"])) < 4.0:
                blocked = True
                if diagnostics is not None:
                    diagnostics["blocked_by_nearby_same_side_level"] = diagnostics.get("blocked_by_nearby_same_side_level", 0) + 1
                break
        if not blocked:
            signals.append(tr)
    if diagnostics is not None:
        diagnostics["signals_ready_to_send"] = len(signals)
    return signals


# ---------------- Trade engine ----------------
def opposite(side: str) -> str:
    return "sell" if side == "buy" else "buy"


async def cancel_opposite_pending(state, new_tr, tg):
    for tr in state["trades"].values():
        if tr.get("status") == "pending" and tr.get("side") == opposite(new_tr["side"]):
            tr["status"] = "cancelled"
            tr["close_reason"] = "OPPOSITE_PENDING"
            tr["cancelled_at"] = pd.Timestamp.now(tz="UTC").isoformat()
            await tg.edit_and_reply(
                tr.get("message_id"),
                cancelled_text(tr, "ظهور إشارة معاكسة قبل التفعيل"),
                "❌ تم إلغاء الأمر المعلق بسبب ظهور إشارة معاكسة. بقي السجل محفوظاً.",
            )


async def register_signal(state, tr, tg):
    await cancel_opposite_pending(state, tr, tg)
    # An already active opposite trade is no longer counted/tracked after reversal.
    for old in state["trades"].values():
        if old.get("status") == "active" and old.get("side") == opposite(tr["side"]):
            old["status"] = "closed_reversed"
            old["close_reason"] = "OPPOSITE_SIGNAL"
            old["closed_at"] = pd.Timestamp.now(tz="UTC").isoformat()
            # إيقاف صامت: لا رسالة في القناة
    msg = await tg.send(entry_text(tr))
    tr["message_id"] = msg
    state["trades"][tr["key"]] = tr
    save_state(state)
    if msg is None:
        log.warning("Signal registered in preview mode; Telegram message was not sent: %s %s", tr["side"], tr["entry"])
    else:
        log.info("Telegram signal sent: message_id=%s side=%s entry=%s key=%s", msg, tr["side"], tr["entry"], tr["key"])


async def manage_trades(state, bid, ask, prices, tg):
    now = pd.Timestamp.now(tz="UTC")
    for tr in list(state["trades"].values()):
        if tr.get("status") not in ("pending", "active"):
            continue
        side = tr["side"]
        trigger = ask if side == "buy" else bid
        current = prices["ask"] if side == "buy" else prices["bid"]
        expiry = pd.Timestamp(tr["expiry"])
        if tr["status"] == "pending":
            # الأمر يبدأ بعد إغلاق شمعة التأكيد (confirm + دقيقة)، وينتهي عند الصلاحية
            active_start = pd.Timestamp(tr["confirm_time"]) + pd.Timedelta(minutes=1)
            hit = False
            window = trigger[(trigger.index >= active_start) & (trigger.index < expiry)]
            if len(window):
                hit = bool((window.low <= tr["entry"]).any() if side == "buy" else (window.high >= tr["entry"]).any())
            if not hit and active_start <= now < expiry:
                hit = (current <= tr["entry"]) if side == "buy" else (current >= tr["entry"])
            if hit:
                tr["status"] = "active"
                tr["entry_time"] = now.isoformat()
                tr["best"] = tr["entry"]
                await tg.send(activated_text(tr), reply_to=tr.get("message_id"))
                save_state(state)
            elif now >= expiry:
                tr["status"] = "cancelled"
                tr["close_reason"] = "EXPIRED"
                tr["cancelled_at"] = now.isoformat()
                await tg.edit_and_reply(tr.get("message_id"), cancelled_text(tr, "انتهت مدة صلاحية الأمر"), "❌ تم إلغاء الأمر. بقي السجل محفوظاً.")
            save_state(state)
        elif tr["status"] == "active":
            entry_time = pd.Timestamp(tr["entry_time"])
            observed = trigger[trigger.index > entry_time]
            if side == "buy":
                if len(observed):
                    tr["best"] = max(float(tr.get("best", tr["entry"])), float(observed.high.max()), current)
                else:
                    tr["best"] = max(float(tr.get("best", tr["entry"])), current)
                stopped = (len(observed) and float(observed.low.min()) <= tr["stop"]) or current <= tr["stop"]
                move = tr["best"] - tr["entry"]
            else:
                if len(observed):
                    tr["best"] = min(float(tr.get("best", tr["entry"])), float(observed.low.min()), current)
                else:
                    tr["best"] = min(float(tr.get("best", tr["entry"])), current)
                stopped = (len(observed) and float(observed.high.max()) >= tr["stop"]) or current >= tr["stop"]
                move = tr["entry"] - tr["best"]
            if stopped:
                tr["status"] = "closed_sl"
                tr["close_reason"] = "SL"
                tr["closed_at"] = now.isoformat()
                # إغلاق صامت: لا رسالة وقف
                save_state(state)
                continue
            if move >= PROGRESS_START_DOLLARS:
                milestone = PROGRESS_START_DOLLARS + int((move - PROGRESS_START_DOLLARS) // PROGRESS_STEP_DOLLARS) * PROGRESS_STEP_DOLLARS
                if milestone > float(tr.get("last_progress", 0)):
                    tr["last_progress"] = milestone
                    await tg.send(progress_text(tr, milestone), reply_to=tr.get("message_id"))
                    save_state(state)
    save_state(state)


# ---------------- Same-zone re-entry after stop ----------------
def find_m2_retry_entry(base: dict[str, Any], bid: pd.DataFrame, ask: pd.DataFrame) -> Optional[dict[str, Any]]:
    """Find one immediate M2 re-entry after SL in the original H1 zone."""
    now = pd.Timestamp.now(tz="UTC")
    side = base["side"]
    stopped_at = pd.Timestamp(base["closed_at"])
    formed = pd.Timestamp(base["formed"])
    zone_expiry = formed + pd.Timedelta(hours=LEVEL_EXPIRY_HOURS)
    if now >= zone_expiry:
        return None

    m2_bid = closed_resample(bid, 2)
    m2_ask = closed_resample(ask, 2)
    first_full_bar = stopped_at.ceil("2min")
    now_min = now.floor("1min")
    bars = m2_bid[
        (m2_bid.index >= first_full_bar)
        & (m2_bid.index < zone_expiry)
        & (m2_bid.index + pd.Timedelta(minutes=2) <= now_min)
    ]
    if len(bars) < 2:
        return None

    for j in range(1, len(bars)):
        if bars.index[j] - bars.index[j - 1] != pd.Timedelta(minutes=2):
            continue
        first, second = bars.iloc[j - 1], bars.iloc[j]
        if abs(float(first.close) - float(second.open)) > EQUAL_TOLERANCE:
            continue
        same_color = (
            first.close > first.open and second.close > second.open
            if side == "buy"
            else first.close < first.open and second.close < second.open
        )
        if not same_color:
            continue

        bar_start = bars.index[j]
        confirmed_at = bar_start + pd.Timedelta(minutes=1)
        entry_time = bar_start + pd.Timedelta(minutes=2)
        if local_day(confirmed_at) != local_day(now):
            continue
        age = now - entry_time
        if age < pd.Timedelta(0) or age > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES):
            continue

        entry_bars = m2_ask if side == "buy" else m2_bid
        entry = round(float(entry_bars.at[bar_start, "close"]), 3)
        equal_point = round((float(first.close) + float(second.open)) / 2, 3)
        setup_key_value = base.get("setup_key", base["key"])
        return {
            "key": setup_key_value + "|retry|m2|" + bar_start.isoformat(),
            "setup_key": setup_key_value,
            "parent_trade_key": base["key"],
            "attempt": 1,
            "retry_used": True,
            "side": side,
            "level": base["level"],
            "formed": base["formed"],
            "touch_time": base.get("touch_time", stopped_at.isoformat()),
            "confirm_time": confirmed_at.isoformat(),
            "equal_point": equal_point,
            "entry": entry,
            "stop": round(
                entry - FIXED_STOP_DOLLARS if side == "buy" else entry + FIXED_STOP_DOLLARS,
                3,
            ),
            "expiry": zone_expiry.isoformat(),
            "status": "active",
            "entry_time": entry_time.isoformat(),
            "entry_source": "M2_RETRY_CLOSE_IMMEDIATE",
            "message_id": None,
            "best": entry,
            "last_progress": 0.0,
            "created_at": now.isoformat(),
        }
    return None


async def manage_reentries(state, bid, ask, tg):
    """Allow one same-direction retry after SL; block it if the first trade reached +30 points."""
    now = pd.Timestamp.now(tz="UTC")
    bases = [
        tr for tr in list(state.get("trades", {}).values())
        if tr.get("status") == "closed_sl"
        and tr.get("attempt", 0) == 0
        and not tr.get("retry_used", False)
        and not tr.get("retry_blocked", False)
        and tr.get("closed_at")
    ]

    for base in bases:
        mfe = (
            float(base.get("best", base["entry"])) - float(base["entry"])
            if base["side"] == "buy"
            else float(base["entry"]) - float(base.get("best", base["entry"]))
        )
        if mfe >= 3.0:
            base["retry_blocked"] = True
            base["retry_block_reason"] = "BASE_REACHED_30_POINTS"
            save_state(state)
            continue

        stopped_at = pd.Timestamp(base["closed_at"])
        formed = pd.Timestamp(base["formed"])
        zone_expiry = formed + pd.Timedelta(hours=LEVEL_EXPIRY_HOURS)
        if now >= zone_expiry:
            base["retry_blocked"] = True
            base["retry_block_reason"] = "ZONE_EXPIRED"
            save_state(state)
            continue

        # A different H1 level touched after the stop cancels this re-entry opportunity.
        touched_new_level = False
        for level in detect_equal_levels(bid):
            if level["key"] == base.get("setup_key"):
                continue
            level_formed = pd.Timestamp(level["formed"])
            level_expires = pd.Timestamp(level["expires"])
            left = max(stopped_at, level_formed)
            right = min(now, level_expires)
            if left >= right:
                continue
            trigger = ask if level["side"] == "buy" else bid
            window = trigger[(trigger.index >= left) & (trigger.index < right)]
            touched = (
                bool((window.low <= level["level"]).any())
                if level["side"] == "buy"
                else bool((window.high >= level["level"]).any())
            ) if len(window) else False
            if touched:
                touched_new_level = True
                break

        if touched_new_level:
            base["retry_blocked"] = True
            base["retry_block_reason"] = "NEW_H1_LEVEL_TOUCHED"
            base["retry_blocked_at"] = now.isoformat()
            save_state(state)
            continue

        retry = find_m2_retry_entry(base, bid, ask)
        if retry:
            await register_signal(state, retry, tg)
            base["retry_used"] = True
            base["retry_trade_key"] = retry["key"]
            base["retry_at"] = now.isoformat()
            save_state(state)


# ---------------- Main loops ----------------
async def nap(seconds: float) -> None:
    end = time.monotonic() + seconds
    while not STOP_REQUESTED and time.monotonic() < end:
        await asyncio.sleep(min(1.0, max(0.0, end - time.monotonic())))


async def wait_for_market(duka, instrument):
    """ينتظر بيانات سليمة بدل أن ينهار (السوق مغلق أو الشبكة متقطعة) فلا تدخل Render في حلقة إعادة تشغيل."""
    while not STOP_REQUESTED:
        try:
            bid, ask = await asyncio.gather(
                asyncio.to_thread(fetch_1m, duka, instrument, "BID"),
                asyncio.to_thread(fetch_1m, duka, instrument, "ASK"),
            )
            h1 = closed_resample(bid, 60)
            if len(h1) >= 3:
                log.info("Health check passed: BID=%s ASK=%s closed_H1=%s last=%s",
                         len(bid), len(ask), len(h1), fmt_time(bid.index[-1]))
                return bid, ask
            log.info("Waiting for market data: only %s closed H1 candles", len(h1))
        except Exception as exc:
            log.info("Waiting for market data (market closed or network issue): %r", exc)
        await nap(30)
    return None, None


async def main():
    await start_web_server()
    duka, instrument = get_duka()
    tg = TelegramSender()
    await tg.start()
    state = load_state()

    log.info("Telegram connected; waiting for healthy market data before startup announcement.")
    bid, ask = await wait_for_market(duka, instrument)
    if bid is None:
        await tg.close()
        return
    await tg.send("اللهم صلِّ وسلِّم وبارك على نبينا محمد وعلى آله وصحبه أجمعين ❤️")
    log.info("Startup announcement sent; monitoring for trade signals.")

    cache_bid, cache_ask = bid, ask
    last_detect = pd.Timestamp.min.tz_localize("UTC")
    last_diagnostics_log = pd.Timestamp.min.tz_localize("UTC")
    last_full = pd.Timestamp.now(tz="UTC")
    while not STOP_REQUESTED:
        try:
            now = pd.Timestamp.now(tz="UTC")
            if (now - last_full).total_seconds() >= FULL_REFRESH_SECONDS:
                # جلب كامل (30 ساعة) كل دقيقة لتصحيح الكاش
                nb, na = await asyncio.gather(
                    asyncio.to_thread(fetch_1m, duka, instrument, "BID"),
                    asyncio.to_thread(fetch_1m, duka, instrument, "ASK"),
                )
                last_full = now
            else:
                # جلب خفيف (آخر 15 دقيقة) كل 3 ثوانٍ للسعر اللحظي
                nb, na = await asyncio.gather(
                    asyncio.to_thread(fetch_1m, duka, instrument, "BID", LIVE_WINDOW_MINUTES / 60, 1),
                    asyncio.to_thread(fetch_1m, duka, instrument, "ASK", LIVE_WINDOW_MINUTES / 60, 1),
                )
            cache_bid, cache_ask = merge_tail(cache_bid, nb), merge_tail(cache_ask, na)
            prices = {"bid": float(nb.close.iloc[-1]), "ask": float(na.close.iloc[-1])}
            if (now - last_detect).total_seconds() >= POLL_SECONDS:
                scan = {}
                signals = detect_signals(cache_bid, cache_ask, state, diagnostics=scan)
                for tr in signals:
                    await register_signal(state, tr, tg)
                if signals or (now - last_diagnostics_log).total_seconds() >= SIGNAL_DIAGNOSTICS_SECONDS:
                    log.info(
                        "Signal scan: H1_today=%s touched=%s M2_equal=%s M2_same_color=%s "
                        "stale=%s nearby_block=%s already_registered=%s ready=%s",
                        scan.get("h1_levels_today", 0), scan.get("levels_touched", 0),
                        scan.get("m2_equal_point_pairs", 0), scan.get("m2_same_color_pairs", 0),
                        scan.get("m2_confirmation_too_old", 0),
                        scan.get("blocked_by_nearby_same_side_level", 0),
                        scan.get("setup_already_registered", 0), scan.get("signals_ready_to_send", 0),
                    )
                    if not signals:
                        log.info("Signal scan detail: %s", scan)
                    last_diagnostics_log = now
                last_detect = now
            await manage_trades(state, cache_bid, cache_ask, prices, tg)
            await manage_reentries(state, cache_bid, cache_ask, tg)
        except Exception:
            log.exception("Polling cycle failed; retrying")
        await nap(LIVE_POLL_SECONDS)
    save_state(state)
    await tg.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
