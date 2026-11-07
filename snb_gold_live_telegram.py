#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SNB GOLD LIVE — M30 Equal Point + M1 Equal Point.

Gold-only Telegram signal bot for Render.
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
PENDING_EXPIRY_HOURS = int(os.getenv("PENDING_EXPIRY_HOURS", "2"))
LEVEL_EXPIRY_HOURS = int(os.getenv("LEVEL_EXPIRY_HOURS", "4"))
EQUAL_TOLERANCE = float(os.getenv("EQUAL_TOLERANCE", "0.10"))
FIXED_STOP_DOLLARS = float(os.getenv("FIXED_STOP_DOLLARS", "4.0"))
PROGRESS_START_DOLLARS = float(os.getenv("PROGRESS_START_DOLLARS", "3.0"))
PROGRESS_STEP_DOLLARS = float(os.getenv("PROGRESS_STEP_DOLLARS", "2.0"))
NO_NEW_FIRST_HOUR = os.getenv("NO_NEW_FIRST_HOUR", "true").lower() == "true"
NO_NEW_LAST_HOUR = os.getenv("NO_NEW_LAST_HOUR", "true").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snb-gold-m30-m1")
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
    return (
        "🔔 **توصية جديدة | دخول فوري** 🔔\n\n"
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
    """Find same-color M30 equal levels formed today only."""
    m30 = closed_resample(bid, 30)
    levels = []
    if len(m30) < 3:
        return levels
    for i in range(1, len(m30)):
        first, second = m30.iloc[i - 1], m30.iloc[i]
        formed = m30.index[i] + pd.Timedelta(minutes=30)
        if not allow_new(m30.index[i]) or local_day(formed) != local_day(pd.Timestamp.now(tz="UTC")):
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
            "key": setup_key(m30.index[i], side),
            "side": side,
            "formed": formed.isoformat(),
            "level": round((float(first.close) + float(second.open)) / 2, 3),
            "expires": (formed + pd.Timedelta(hours=LEVEL_EXPIRY_HOURS)).isoformat(),
        })
    return levels


def find_m1_equal_entry(
    level: dict[str, Any],
    bid: pd.DataFrame,
    ask: pd.DataFrame,
) -> Optional[dict[str, Any]]:
    side = level["side"]
    formed = pd.Timestamp(level["formed"])
    expires = pd.Timestamp(level["expires"])
    now = pd.Timestamp.now(tz="UTC")

    # سعر اللمس: ASK للشراء وBID للبيع
    trigger = ask if side == "buy" else bid
    touched = trigger[
        (trigger.index >= formed) &
        (trigger.index < expires)
    ]
    touched = (
        touched[touched.low <= level["level"]]
        if side == "buy"
        else touched[touched.high >= level["level"]]
    )
    if touched.empty:
        return None

    touch = touched.index[0]
    now_min = now.floor("1min")

    # شموع M1 المغلقة فقط؛ لا نستخدم الشمعة الجارية
    m1 = bid[
        (bid.index >= touch) &
        (bid.index < expires) &
        (bid.index + pd.Timedelta(minutes=1) <= now_min)
    ]

    # نبحث من الأحدث إلى الأقدم حتى لا يحجب تأكيد قديمٌ إشارةً حديثة
    for j in range(len(m1) - 1, 0, -1):
        a = m1.iloc[j - 1]
        b = m1.iloc[j]

        if abs(float(a.close) - float(b.open)) > EQUAL_TOLERANCE:
            continue

        same_color = (
            a.close > a.open and b.close > b.open
            if side == "buy"
            else a.close < a.open and b.close < b.open
        )
        if not same_color:
            continue

        confirmed = m1.index[j]
        if local_day(confirmed) != local_day(now):
            continue

        entry_time = confirmed + pd.Timedelta(minutes=1)
        age = now - entry_time

        # بما أن الفحص من الأحدث للأقدم، فكل ما بعده أقدم أيضًا
        if age > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES):
            break
        if age < pd.Timedelta(0):
            continue

        equal_point = round(
            (float(a.close) + float(b.open)) / 2,
            3,
        )

        # الدخول عند إغلاق شمعة التأكيد: ASK للشراء وBID للبيع
        entry_src = trigger if confirmed in trigger.index else bid
        entry = round(float(entry_src.loc[confirmed, "close"]), 3)

        return {
            "key": level["key"] + "|m1|" + confirmed.isoformat(),
            "side": side,
            "level": level["level"],
            "formed": level["formed"],
            "touch_time": touch.isoformat(),
            "confirm_time": confirmed.isoformat(),
            "equal_point": equal_point,
            "entry": entry,
            "stop": round(
                entry - FIXED_STOP_DOLLARS
                if side == "buy"
                else entry + FIXED_STOP_DOLLARS,
                3,
            ),
            "expiry": (
                confirmed + pd.Timedelta(hours=PENDING_EXPIRY_HOURS)
            ).isoformat(),
            "status": "active",
            "entry_time": entry_time.isoformat(),
            "entry_source": "M1_CLOSE",
            "message_id": None,
            "best": entry,
            "last_progress": 0.0,
            "created_at": now.isoformat(),
        }

    return None


def detect_signals(bid: pd.DataFrame, ask: pd.DataFrame, state: dict[str, Any]) -> list[dict[str, Any]]:
    signals = []
    for level in detect_equal_levels(bid):
        if any(t.get("setup_key") == level["key"] for t in state["trades"].values()):
            continue
        tr = find_m1_equal_entry(level, bid, ask)
        if tr:
            age = pd.Timestamp.now(tz="UTC") - (pd.Timestamp(tr["confirm_time"]) + pd.Timedelta(minutes=1))
            if age > pd.Timedelta(minutes=SIGNAL_MAX_AGE_MINUTES):
                continue  # إشارة قديمة (مثلاً بعد إعادة تشغيل ضاعت فيها الحالة): لا تُرسل
            tr["setup_key"] = level["key"]
            signals.append(tr)
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
    log.info("Registered %s %s at %s", tr["side"], tr["entry"], tr["key"])


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
            m30 = closed_resample(bid, 30)
            if len(m30) >= 3:
                log.info("Health check passed: BID=%s ASK=%s closed_M30=%s last=%s",
                         len(bid), len(ask), len(m30), fmt_time(bid.index[-1]))
                return bid, ask
            log.info("Waiting for market data: only %s closed M30 candles", len(m30))
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

    # Critical gate: no startup prayer message until Telegram and market candles are healthy.
    bid, ask = await wait_for_market(duka, instrument)
    if bid is None:
        await tg.close()
        return
    if not state.get("started_message_sent"):
        await tg.send("اللهم صلِّ وسلّم وبارك على نبينا محمد وعلى آله وصحبه أجمعين ❤️")
        state["started_message_sent"] = True
        save_state(state)

    cache_bid, cache_ask = bid, ask
    last_detect = pd.Timestamp.min.tz_localize("UTC")
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
                for tr in detect_signals(cache_bid, cache_ask, state):
                    await register_signal(state, tr, tg)
                last_detect = now
            await manage_trades(state, cache_bid, cache_ask, prices, tg)
        except Exception as exc:
            log.warning("Polling cycle failed; retrying: %r", exc)
        await nap(LIVE_POLL_SECONDS)
    save_state(state)
    await tg.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
