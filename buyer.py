"""
Покупатель. Читает очередь и мгновенно отправляет запрос покупки.
Сканер от него никак не зависит (развязка через asyncio.Queue).

Ключевые правила безопасности:
  - В /buy уходит price = ЛИМИТ пользователя (а не наблюдаемая цена).
    Биржа покупает «по этой цене или ниже» -> дороже лимита купить нельзя.
  - HTTP 500: НЕ паникуем, НЕ ретраим вслепую (риск двойной покупки).
    Логируем, помечаем как «неизвестный результат», уведомляем.
  - Анти-дубль по offer_id (короткий TTL), чтобы не бить дважды по одному лоту.
"""
from __future__ import annotations

import asyncio
import time

from loguru import logger

import config
import market_client
import names
import state
import telegram
from scanner import Offer

# offer_id -> ts, анти-дубль
_recent: dict[str, float] = {}
_inflight: set[str] = set()


def _dedupe_key(offer: Offer) -> str:
    if offer.offer_id is not None:
        return f"id:{offer.offer_id}"
    return f"nid:{offer.name_id}"


def _seen_recently(key: str) -> bool:
    now = time.monotonic()
    ts = _recent.get(key)
    if ts is not None and (now - ts) < config.OFFER_DEDUPE_TTL_SEC:
        return True
    return False


def _prune(now: float) -> None:
    if len(_recent) < 256:
        return
    dead = [k for k, t in _recent.items() if now - t > config.OFFER_DEDUPE_TTL_SEC]
    for k in dead:
        _recent.pop(k, None)


async def _handle(offer: Offer) -> None:
    item = state.items.get(offer.name_id)
    if item is None or item.limit_units is None:
        return  # лимит сняли, пока лот ждал в очереди

    key = _dedupe_key(offer)
    if key in _inflight or _seen_recently(key):
        return
    _inflight.add(key)
    label = item.label
    limit = item.limit_units
    try:
        t0 = time.monotonic()
        status, data, raw = await market_client.buy(
            price_units=limit,
            offer_id=offer.offer_id,
            hash_name=names.label_for(offer.name_id),
        )
        latency_ms = (time.monotonic() - t0) * 1000.0
        signal_ms = (time.monotonic() - offer.signal_ts) * 1000.0
        _recent[key] = time.monotonic()
        _prune(time.monotonic())

        # ── Успех ────────────────────────────────────────────────────────
        if status == 200 and data and data.get("success"):
            bought_id = data.get("id")
            state.stats.buys_ok += 1
            state.bump_activity(label, "bought")
            logger.success("Success: bought {} (limit {} units, id={}) "
                           "buy={:.0f}ms ws->buy={:.0f}ms",
                           label, limit, bought_id, latency_ms, signal_ms)
            await telegram.notify_purchase(
                label=label, limit_units=limit, offer_id=bought_id,
                latency_ms=latency_ms, signal_ms=signal_ms,
            )
            return

        # ── HTTP 500 / сетевой сбой -> результат НЕИЗВЕСТЕН ───────────────
        if status == 500 or status == -1:
            state.stats.buys_uncertain += 1
            state.bump_activity(label, "uncertain")
            reason = raw if status == 500 else f"{raw}"
            logger.error("HTTP {} on buy {} — RESULT UNKNOWN, NO auto-retry. {}",
                         status, label, reason[:200])
            await telegram.notify_uncertain(label=label, status=status, reason=reason[:300])
            return

        # ── Прочая неудача (лот ушёл, нет денег, инвентарь полон и т.п.) ──
        state.stats.buys_fail += 1
        err = (data or {}).get("error") if data else raw
        logger.warning("Buy failed {} -> {}", label, str(err)[:200])
        # «не найден по цене или ниже» = гонку проиграли/лот пропал — это норма.
        if _is_race_loss(err):
            state.bump_activity(label, "race")
            if config.NOTIFY_RACE_LOSS:
                await telegram.notify_fail(label=label, status=status, reason=str(err)[:300])
        else:
            state.bump_activity(label, "fail")
            await telegram.notify_fail(label=label, status=status, reason=str(err)[:300])
    except Exception:
        logger.exception("Unexpected error buying {}", label)
    finally:
        _inflight.discard(key)


def _is_race_loss(err) -> bool:
    s = str(err).lower()
    return ("не найден" in s) or ("not found" in s) or ("no item" in s) or ("sold" in s)


async def worker(queue: "asyncio.Queue[Offer]", idx: int) -> None:
    logger.info("Buyer worker #{} started", idx)
    while True:
        offer = await queue.get()
        try:
            await _handle(offer)
        finally:
            queue.task_done()
