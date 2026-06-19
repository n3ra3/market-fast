"""
Сканер. Единственная задача — из WS-пуша достать (name_id, price, offer_id),
проверить WatchList и лимит, и положить в очередь покупателю. Больше ничего.

Горячий путь: никаких await кроме неблокирующего put_nowait, никаких обращений
к диску, никаких вычислений цен/комиссий. Сканер НИКОГДА не ждёт покупку.

Поток:  Centrifugo push -> on_publication() -> Queue -> Buyer
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass

from loguru import logger

import config
import state


@dataclass(slots=True)
class Offer:
    name_id: int
    price_units: int
    offer_id: int | str | None
    signal_ts: float


_queue: "asyncio.Queue[Offer] | None" = None
_discovery_seen = 0


def set_queue(q: "asyncio.Queue[Offer]") -> None:
    global _queue
    _queue = q


def _first(d: dict, keys: tuple[str, ...]):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _price_to_units(raw) -> int | None:
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if config.WS_PRICE_FORMAT == "units":
        return int(round(val))
    return int(round(val * config.PRICE_UNITS_SCALE))


def _iter_records(data):
    """Пуш может быть dict-предметом, или списком предметов, или {'items':[...]}."""
    if isinstance(data, dict):
        inner = data.get("items") or data.get("data")
        if isinstance(inner, list):
            yield from (x for x in inner if isinstance(x, dict))
        else:
            yield data
    elif isinstance(data, list):
        yield from (x for x in data if isinstance(x, dict))


def extract(rec: dict) -> Offer | None:
    # Игнорируем снятие листинга (event=remove и т.п.) — это не покупаемый лот.
    event = _first(rec, config.EVENT_KEYS)
    if event is not None and str(event).lower() in config.EVENT_IGNORE:
        return None
    nid_raw = _first(rec, config.NAME_ID_KEYS)
    price_raw = _first(rec, config.PRICE_KEYS)
    if nid_raw is None or price_raw is None:
        return None
    try:
        nid = int(nid_raw)
    except (TypeError, ValueError):
        return None
    price_units = _price_to_units(price_raw)
    if price_units is None or price_units <= 0:
        return None
    offer_id = _first(rec, config.OFFER_ID_KEYS)
    return Offer(name_id=nid, price_units=price_units, offer_id=offer_id, signal_ts=time.monotonic())


async def on_publication(data) -> None:
    """Колбэк Centrifugo на каждую публикацию канала. Должен быть быстрым."""
    global _discovery_seen

    # Discovery: первые N сырых пушей логируем целиком для калибровки схемы.
    if _discovery_seen < config.DISCOVERY_SAMPLE:
        _discovery_seen += 1
        try:
            logger.info("RAW PUSH #{} :: {}", _discovery_seen, json.dumps(data)[:1000])
        except Exception:
            logger.info("RAW PUSH #{} (repr) :: {}", _discovery_seen, repr(data)[:1000])

    state.stats.ws_last_event_ts = time.time()

    for rec in _iter_records(data):
        offer = extract(rec)
        if offer is None:
            continue

        # O(1) фильтр по WatchList — на высоконагруженном канале отсекает почти всё.
        if offer.name_id not in state.watch_ids:
            continue

        state.stats.received += 1
        item = state.items.get(offer.name_id)
        if item is None:
            continue

        # SCAN-only: лимит не задан -> предмет в WatchList, но не вооружён.
        if item.limit_units is None:
            logger.debug("SCAN sighting: {} @ {} units (no limit set)",
                         item.label, offer.price_units)
            continue

        # Локальный быстрый фильтр по цене. Серверная гарантия — price=limit в buy().
        if offer.price_units > item.limit_units:
            continue

        state.stats.matched += 1
        state.bump_activity(item.label, "matched")
        logger.info("Matched: {} @ {} units (limit {}), offer_id={}",
                    item.label, offer.price_units, item.limit_units, offer.offer_id)

        if config.DISCOVERY_MODE:
            logger.info("DISCOVERY_MODE -> would buy {} (no purchase sent)", item.label)
            continue

        if _queue is None:
            continue
        try:
            _queue.put_nowait(offer)
        except asyncio.QueueFull:
            logger.warning("Buy queue full — dropping offer for {}", item.label)
