"""
Telegram: ТОЛЬКО информирование и управление лимитами. Не на пути покупки.

Возможности:
  - /menu (и /start) — статус + инлайн-меню: режим, WS, watchlist с лимитами.
  - Set · <item>  -> бот просит цену в USD, ответом ставишь лимит (ARMED).
  - Clear · <item> -> снять лимит у предмета.
  - DISARM ALL    -> снять все лимиты, вернуться в чистый SCAN.
  - Уведомления о покупке / неизвестном результате (HTTP 500) / ошибке.

У Telegram отдельная aiohttp-сессия — он НЕ ходит через market rate limiter.
Все вызовы обёрнуты в try/except: проблемы Telegram не роняют бота.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import aiohttp
from loguru import logger

import config
import names
import state

_session: aiohttp.ClientSession | None = None
# chat_id -> name_id, ожидание ввода лимита ПОКУПКИ
_pending_price: dict[str, int] = {}
# chat_id -> name_id, ожидание ввода пола РЕПРАЙСЕРА (продажа)
_pending_floor: dict[str, int] = {}
# id последнего сообщения-меню (чтобы редактировать на месте / удалять старое)
_menu_msg_id: int | None = None
# id сообщения-подсказки «введи цену» (удаляем после ответа)
_prompt_msg_id: int | None = None
# Текущий экран единого меню: "main" (покупка) | "sell" (репрайсер/продажа)
_screen: str = "main"
# name_id -> развёрнутый в экране «Продажа» стакан: {ts, asks, bids}
_book_cache: dict[int, dict] = {}


def _units(value: float) -> int:
    return int(round(value * config.PRICE_UNITS_SCALE))


def _fmt(units: int | None) -> str:
    if units is None:
        return "—"
    return f"${units / config.PRICE_UNITS_SCALE:.3f}"


def _short(label: str, n: int = 18) -> str:
    return label if len(label) <= n else label[: n - 1] + "…"


async def _session_get() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=35),
            headers={"User-Agent": config.USER_AGENT},
        )
    return _session


async def _api(method: str, payload: dict[str, Any], timeout: float = 35) -> dict | None:
    if not config.TELEGRAM_ENABLED:
        return None
    s = await _session_get()
    try:
        async with s.post(f"{config.TELEGRAM_API}/{method}", json=payload,
                          timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return await r.json()
    except Exception as e:
        logger.debug("telegram {} failed: {}", method, e)
        return None


async def send(text: str, reply_markup: dict | None = None) -> None:
    payload: dict[str, Any] = {"chat_id": config.TELEGRAM_CHAT_ID, "text": text,
                               "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    await _api("sendMessage", payload)


# ── Меню / статус ──────────────────────────────────────────────────────────
def _status_text() -> str:
    scan = state.is_scan_mode()
    mode = "🟡 SCAN" if scan else "🟢 ARMED"
    ws = "🟢 connected" if state.stats.ws_connected else "🔴 down"
    lines = [
        f"<b>{mode}</b>   WS: {ws}   items: {len(state.items)}",
    ]
    if scan:
        lines.append("Лимиты сброшены. Покупка включается, когда задан лимит.")
    lines.append("")
    for it in state.items.values():
        if it.limit_units is None:
            lines.append(f"○ {it.label} — <i>no limit</i>")
        else:
            lines.append(f"● <b>{it.label}</b> — ≤ {_fmt(it.limit_units)} · ARMED")
    st = state.stats
    lines.append("")
    lines.append(f"matched: {st.matched}  ·  bought: {st.buys_ok}  ·  "
                 f"fail: {st.buys_fail}  ·  unknown: {st.buys_uncertain}")
    return "\n".join(lines)


def _menu_keyboard() -> dict:
    rows: list[list[dict]] = []
    for it in state.items.values():
        rows.append([
            {"text": f"⚙ Set · {_short(it.label)}", "callback_data": f"set:{it.name_id}"},
            {"text": f"✖ Clear · {_short(it.label)}", "callback_data": f"clr:{it.name_id}"},
        ])
    rows.append([{"text": "🏷 Продажа · репрайсер", "callback_data": "sell"}])
    rows.append([{"text": "🔄 Refresh", "callback_data": "refresh"}])
    rows.append([{"text": "🛑 DISARM ALL → SCAN", "callback_data": "disarm"}])
    return {"inline_keyboard": rows}


# ── Экран «Продажа» (репрайсер) ────────────────────────────────────────────
def _book_lines(nid: int) -> list[str]:
    """Развёрнутый стакан для предмета, если пользователь его запросил."""
    b = _book_cache.get(nid)
    if not b:
        return []
    lines = ["", "<b>Стакан</b> (ask ↑ / bid ↓):"]
    asks = b.get("asks") or []
    bids = b.get("bids") or []
    if asks:
        lines.append("  <i>продажа:</i> " + "  ".join(
            f"{_fmt(p)}×{t}" for p, t in asks) )
    else:
        lines.append("  <i>продажа:</i> —")
    if bids:
        lines.append("  <i>покупка:</i> " + "  ".join(
            f"{_fmt(p)}×{t}" for p, t in bids))
    else:
        lines.append("  <i>покупка:</i> —")
    return lines


def _sell_text() -> str:
    view = state.repricer_view
    lines = ["<b>🏷 Продажа — репрайсер</b>"]
    armed = state.repricer_armed_count()
    if not view:
        lines.append("")
        lines.append("<i>Нет активных лотов на продаже</i> (или ещё не обновилось).")
        lines.append("Нажми Refresh, чтобы подтянуть /items.")
        return "\n".join(lines)
    lines.append(f"вооружено полов: {armed} · лотов: {len(view)}")
    lines.append("")
    for nid, v in view.items():
        floor = v.get("floor_units")
        my = v.get("my_min_units")
        comp = v.get("competitor_units")
        n_lots = len(v.get("lots") or [])
        lots_sfx = f" ×{n_lots}" if n_lots > 1 else ""
        if floor is None:
            lines.append(f"○ {_short(v['label'], 22)}{lots_sfx} — моя {_fmt(my)} · пол — · <i>off</i>")
        else:
            if comp is None:
                top = "🏷 нет конкурентов" if v.get("is_top") else "?"
            else:
                top = "👑 топ" if v.get("is_top") else f"⚠ не топ (конк. {_fmt(comp)})"
            lines.append(f"● <b>{_short(v['label'], 22)}</b>{lots_sfx} — моя {_fmt(my)} · "
                         f"пол {_fmt(floor)} · {top}")
        lines += _book_lines(nid)
    return "\n".join(lines)


def _sell_keyboard() -> dict:
    rows: list[list[dict]] = []
    for nid, v in state.repricer_view.items():
        armed = state.get_floor(nid) is not None
        rows.append([
            {"text": f"⚙ Пол · {_short(v['label'])}", "callback_data": f"rset:{nid}"},
            {"text": ("✖ Снять" if armed else "—"), "callback_data": f"rclr:{nid}"},
        ])
        rows.append([{"text": f"📖 Стакан · {_short(v['label'])}", "callback_data": f"book:{nid}"}])
    rows.append([{"text": "🔙 Назад", "callback_data": "main"},
                 {"text": "🔄 Refresh", "callback_data": "srefresh"}])
    if state.repricer_armed_count():
        rows.append([{"text": "🛑 Снять все полы", "callback_data": "rdisarm"}])
    return {"inline_keyboard": rows}


def _render() -> tuple[str, dict]:
    if _screen == "sell":
        return _sell_text(), _sell_keyboard()
    return _status_text(), _menu_keyboard()


async def show_menu(*, edit_msg_id: int | None = None, force_new: bool = False) -> None:
    """Единое меню без копий.
       - callback (нажатие кнопки) -> редактируем на месте (edit_msg_id);
       - команда / установка лимита -> удаляем старое меню и шлём свежее вниз."""
    global _menu_msg_id
    text, kb = _render()

    if not force_new:
        target = edit_msg_id or _menu_msg_id
        if target is not None:
            res = await _api("editMessageText", {
                "chat_id": config.TELEGRAM_CHAT_ID, "message_id": target,
                "text": text, "parse_mode": "HTML",
                "disable_web_page_preview": True, "reply_markup": kb,
            })
            if res and (res.get("ok")
                        or "not modified" in str(res.get("description", "")).lower()):
                _menu_msg_id = target
                return
            # редактирование не удалось (сообщение удалено и т.п.) -> отправим новое

    if _menu_msg_id is not None:
        await _api("deleteMessage", {"chat_id": config.TELEGRAM_CHAT_ID, "message_id": _menu_msg_id})
        _menu_msg_id = None
    res = await _api("sendMessage", {
        "chat_id": config.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML",
        "disable_web_page_preview": True, "reply_markup": kb,
    })
    if res and res.get("ok"):
        _menu_msg_id = res["result"]["message_id"]


async def _send_prompt(text: str) -> None:
    """Подсказка «введи цену». Старую подсказку убираем, чтобы не копились."""
    global _prompt_msg_id
    if _prompt_msg_id is not None:
        await _api("deleteMessage", {"chat_id": config.TELEGRAM_CHAT_ID, "message_id": _prompt_msg_id})
        _prompt_msg_id = None
    res = await _api("sendMessage", {"chat_id": config.TELEGRAM_CHAT_ID,
                                     "text": text, "parse_mode": "HTML"})
    if res and res.get("ok"):
        _prompt_msg_id = res["result"]["message_id"]


# ── Обработка обновлений ───────────────────────────────────────────────────
async def _answer_callback(cq_id: str, text: str = "") -> None:
    await _api("answerCallbackQuery", {"callback_query_id": cq_id, "text": text})


def _cq_msg_id(cq: dict) -> int | None:
    return (cq.get("message") or {}).get("message_id")


async def _handle_callback(cq: dict) -> None:
    global _screen
    cq_id = cq.get("id", "")
    data = cq.get("data", "")
    msg_id = _cq_msg_id(cq)

    # ── Навигация между экранами ─────────────────────────────────────────
    if data == "sell":
        _screen = "sell"
        await _answer_callback(cq_id)
        await show_menu(edit_msg_id=msg_id)
        return
    if data == "main":
        _screen = "main"
        _book_cache.clear()
        await _answer_callback(cq_id)
        await show_menu(edit_msg_id=msg_id)
        return

    # ── Экран «Продажа» (репрайсер) ──────────────────────────────────────
    if data == "srefresh":
        import repricer  # локальный импорт: избегаем циклической зависимости
        ok = await repricer.refresh_view()
        await _answer_callback(cq_id, "Обновлено" if ok else "Не удалось (/items)")
        await show_menu(edit_msg_id=msg_id)
        return
    if data == "rdisarm":
        n = state.disarm_repricer()
        await _answer_callback(cq_id, f"Снято полов: {n}")
        await show_menu(edit_msg_id=msg_id)
        return
    if data.startswith("rclr:"):
        nid = int(data[5:])
        ok = state.clear_floor(nid)
        v = state.repricer_view.get(nid)
        await _answer_callback(cq_id, f"Пол снят: {v['label']}" if (ok and v) else "Нет пола")
        await show_menu(edit_msg_id=msg_id)
        return
    if data.startswith("rset:"):
        nid = int(data[5:])
        v = state.repricer_view.get(nid)
        if not v:
            await _answer_callback(cq_id, "Лот исчез — Refresh")
            return
        _pending_floor[str(config.TELEGRAM_CHAT_ID)] = nid
        _pending_price.pop(str(config.TELEGRAM_CHAT_ID), None)  # не путаем с покупкой
        await _answer_callback(cq_id)
        await _send_prompt(f"Введи <b>пол</b> в USD для <b>{v['label']}</b> "
                           f"(ниже не опускаемся), напр. 0.25")
        return
    if data.startswith("book:"):
        nid = int(data[5:])
        v = state.repricer_view.get(nid)
        if not v:
            await _answer_callback(cq_id, "Лот исчез — Refresh")
            return
        import market_client
        import repricer  # локальный импорт
        book = await market_client.bid_ask(v["hash_name"])
        asks, bids = repricer.parse_book(book, depth=config.BOOK_DEPTH)
        _book_cache[nid] = {"ts": time.time(), "asks": asks, "bids": bids}
        await _answer_callback(cq_id, "Стакан обновлён" if book is not None else "bid-ask не ответил")
        await show_menu(edit_msg_id=msg_id)
        return

    if data == "refresh":
        await _answer_callback(cq_id, "Обновлено")
        await show_menu(edit_msg_id=msg_id)
        return
    if data == "disarm":
        n = state.disarm_all()
        await _answer_callback(cq_id, f"Снято лимитов: {n} · режим SCAN")
        await show_menu(edit_msg_id=msg_id)
        return
    if data.startswith("clr:"):
        nid = int(data[4:])
        it = state.clear_limit(nid)
        await _answer_callback(cq_id, f"Лимит снят: {it.label}" if it else "Нет такого")
        await show_menu(edit_msg_id=msg_id)
        return
    if data.startswith("set:"):
        nid = int(data[4:])
        it = state.items.get(nid)
        if not it:
            await _answer_callback(cq_id, "Нет такого")
            return
        _pending_price[str(config.TELEGRAM_CHAT_ID)] = nid
        await _answer_callback(cq_id)
        await _send_prompt(f"Введи цену в USD для <b>{it.label}</b>, например 0.29")
        return
    await _answer_callback(cq_id)


async def _handle_message(msg: dict) -> None:
    global _prompt_msg_id, _screen
    chat_id = str(msg.get("chat", {}).get("id", ""))
    if chat_id != str(config.TELEGRAM_CHAT_ID):
        return
    text = (msg.get("text") or "").strip()
    if not text:
        return

    if text.startswith("/menu") or text.startswith("/start") or text.startswith("/status"):
        _screen = "main"
        _book_cache.clear()
        await show_menu(force_new=True)
        return
    if text.startswith("/disarm"):
        state.disarm_all()
        await show_menu(force_new=True)
        return

    # Ввод ПОЛА репрайсера (продажа), если ждём его
    fnid = _pending_floor.get(chat_id)
    if fnid is not None:
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            await _send_prompt("Не похоже на число. Пример: 0.25")
            return
        if value <= 0:
            await _send_prompt("Пол должен быть больше 0.")
            return
        state.set_floor(fnid, _units(value))
        _pending_floor.pop(chat_id, None)
        if _prompt_msg_id is not None:
            await _api("deleteMessage", {"chat_id": config.TELEGRAM_CHAT_ID, "message_id": _prompt_msg_id})
            _prompt_msg_id = None
        # остаёмся на экране «Продажа», редактируем меню на месте
        await show_menu()
        return

    # Ввод лимита ПОКУПКИ для предмета, по которому ждём ответ
    nid = _pending_price.get(chat_id)
    if nid is not None:
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            await _send_prompt("Не похоже на число. Пример: 0.29")
            return
        if value <= 0:
            await _send_prompt("Цена должна быть больше 0.")
            return
        state.set_limit(nid, _units(value))
        _pending_price.pop(chat_id, None)
        # убрать подсказку «введи цену»
        if _prompt_msg_id is not None:
            await _api("deleteMessage", {"chat_id": config.TELEGRAM_CHAT_ID, "message_id": _prompt_msg_id})
            _prompt_msg_id = None
        # свежее меню вниз = подтверждение: предмет теперь ARMED с точным hash_name
        await show_menu(force_new=True)
        return


async def poll_loop() -> None:
    if not config.TELEGRAM_ENABLED:
        logger.info("Telegram disabled (no token/chat). Skipping poller.")
        return
    offset = 0
    logger.info("Telegram poller started")
    while True:
        resp = await _api("getUpdates",
                          {"offset": offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]},
                          timeout=35)
        if not resp or not resp.get("ok"):
            await asyncio.sleep(2)
            continue
        for upd in resp.get("result", []):
            offset = upd["update_id"] + 1
            try:
                if "callback_query" in upd:
                    await _handle_callback(upd["callback_query"])
                elif "message" in upd:
                    await _handle_message(upd["message"])
            except Exception:
                logger.exception("telegram update handling failed")


# ── Уведомления ────────────────────────────────────────────────────────────
async def notify_purchase(*, label: str, limit_units: int, offer_id,
                          latency_ms: float, signal_ms: float) -> None:
    await send(
        f"✅ <b>BOUGHT</b> · {label}\n"
        f"limit {_fmt(limit_units)} · offer id {offer_id}\n"
        f"WS→buy {signal_ms:.0f} ms · buy {latency_ms:.0f} ms"
    )


async def notify_uncertain(*, label: str, status: int, reason: str) -> None:
    await send(
        f"⚠️ <b>RESULT UNKNOWN</b> · {label}\n"
        f"HTTP {status}. Покупка НЕ повторялась автоматически.\n"
        f"Проверь вручную, не купился ли предмет.\n<code>{reason}</code>"
    )


async def notify_fail(*, label: str, status: int, reason: str) -> None:
    await send(f"❌ Buy failed · {label}\nHTTP {status} · <code>{reason}</code>")


async def notify_reprice(*, label: str, old_units: int, new_units: int,
                         competitor_units: int, floor_units: int) -> None:
    await send(
        f"🔽 <b>Repriced</b>: {label}\n"
        f"{_fmt(old_units)} → {_fmt(new_units)} "
        f"(конкурент {_fmt(competitor_units)}, пол {_fmt(floor_units)})"
    )


async def activity_reporter_loop() -> None:
    """Периодическая сводка активности в Telegram (вместо спама поштучно)."""
    if not config.TELEGRAM_ENABLED or not config.NOTIFY_ACTIVITY:
        return
    interval = config.NOTIFY_ACTIVITY_INTERVAL_SEC
    while True:
        await asyncio.sleep(interval)
        snap = state.snapshot_activity()
        if not snap:
            continue
        lines = [f"📊 Активность за {int(interval)}с"]
        for label, d in snap.items():
            parts: list[str] = []
            if d.get("matched"):
                parts.append(f"matched {d['matched']}")
            if d.get("bought"):
                parts.append(f"✅ bought {d['bought']}")
            if d.get("race"):
                parts.append(f"гонка {d['race']}")
            if d.get("fail"):
                parts.append(f"fail {d['fail']}")
            if d.get("uncertain"):
                parts.append(f"⚠ unknown {d['uncertain']}")
            if parts:
                lines.append(f"<b>{label}</b>: " + " · ".join(parts))
        await send("\n".join(lines))


async def startup() -> None:
    if not config.TELEGRAM_ENABLED:
        return
    await send("🤖 Bot started — режим SCAN, лимитов нет. Задай лимиты в меню.")
    await show_menu(force_new=True)


async def close() -> None:
    if _session and not _session.closed:
        await _session.close()
