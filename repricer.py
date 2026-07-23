"""
Репрайсер: удержание МОИХ лотов на первом месте в продаже через авто-снижение
цены до заданного пола.

Полностью независим от покупки: отдельный модуль и отдельная asyncio-задача, все
запросы к Market идут через общий rate_limiter с НИЗКИМ приоритетом
(priority=False) — переоценка никогда не задерживает покупку кейсов. Горячий путь
(scanner/buyer/очередь) не затрагивается.

Безопасность (как у лимитов покупки):
  - Пол-лимит по каждому предмету (ключ = name_id) живёт ТОЛЬКО в RAM
    (state.repricer_floors) и сбрасывается при рестарте. После старта репрайсер
    по каждому лоту выключен, пока пользователь не задаст пол через Telegram.
  - Цена только СНИЖАЕТСЯ и никогда не опускается ниже пола. Если удержать топ
    можно лишь ниже пола — стоим ровно на полу и больше не двигаем.
  - Идемпотентность по построению: каждый цикл берёт текущую цену из свежего
    /items, поэтому «двойного снижения» не бывает; set-price вызывается только
    если целевая цена ОТЛИЧАЕТСЯ от текущей.
  - HTTP 500 / сеть / success:false — логируем и пропускаем лот до следующего
    цикла, вслепую не повторяем (как в buyer.py).

Цикл (раз в REPRICE_INTERVAL_SEC):
  1. GET /items — мои лоты на продаже (1 запрос). Группируем по name_id.
  2. Для каждого name_id с заданным полом: GET /bid-ask, берём лучшую цену
     конкурента = минимальный ask-уровень, из которого ВЫЧТЕНЫ мои лоты (так
     «конкурент = я вторым лотом» исключается корректно).
  3. Если конкурент дешевле моего лучшего лота — переставляем цену на шаг ниже
     конкурента, зажимая снизу полом.
  4. На каждую фактическую переоценку — уведомление в Telegram.
Даже без единого пола цикл делает 1 лёгкий /items в интервал — чтобы меню
«Продажа» показывало актуальные живые лоты (стакан по каждому — по запросу).
"""
from __future__ import annotations

import asyncio
import json
import time

from loguru import logger

import config
import market_client
import names
import state
import telegram

# Discovery: сколько сырых ответов /items и /bid-ask ещё залогировать целиком.
_items_disc = 0
_book_disc = 0


# ── Парсинг цен ─────────────────────────────────────────────────────────────
def _read_price_to_units(raw) -> int | None:
    """Цена из ответа bid-ask -> units. По докам это десятичная валюта
    ('441.5900'); MARKET_READ_PRICE_FORMAT=units переключает на целые units."""
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if config.MARKET_READ_PRICE_FORMAT == "units":
        return int(round(val))
    return int(round(val * config.PRICE_UNITS_SCALE))


def _items_price_to_units(raw) -> int | None:
    """Цена из /items. Как и WS-канал / bid-ask, market.csgo отдаёт её как
    ДЕСЯТИЧНОЕ значение валюты (напр. 3 == $3), а не целые units. Конвертируем
    так же (× scale). MARKET_READ_PRICE_FORMAT=units переключит на целые units.
    ВНИМАНИЕ: запись (/buy, /set-price) наоборот — в целых units (×1000)."""
    return _read_price_to_units(raw)


def _int(raw, default: int = 0) -> int:
    try:
        return int(round(float(raw)))
    except (TypeError, ValueError):
        return default


# ── Стакан ──────────────────────────────────────────────────────────────────
def parse_book(book: dict | None, depth: int) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """(asks, bids), каждый — список (price_units, total) по depth уровней.
    asks по возрастанию цены, bids по убыванию (лучшие сверху)."""
    if not isinstance(book, dict):
        return [], []

    def side(key: str, reverse: bool) -> list[tuple[int, int]]:
        rows: list[tuple[int, int]] = []
        for lvl in (book.get(key) or []):
            if not isinstance(lvl, dict):
                continue
            p = _read_price_to_units(lvl.get("price"))
            if p is None:
                continue
            rows.append((p, _int(lvl.get("total"))))
        rows.sort(key=lambda x: x[0], reverse=reverse)
        return rows[:depth]

    return side("ask", False), side("bid", True)


def _best_competitor_units(asks: list[tuple[int, int]], mine_counts: dict[int, int]) -> int | None:
    """Минимальная ask-цена (units), из которой вычтены мои лоты. None -> конкурентов нет.
    asks должны быть отсортированы по возрастанию цены."""
    for price, total in asks:
        if total - mine_counts.get(price, 0) > 0:
            return price
    return None


# ── Цикл ────────────────────────────────────────────────────────────────────
# Расшифровка status из /items (по докам market.csgo).
STATUS_LABELS: dict[int, str] = {
    1: "на продаже",
    2: "продан — передать боту",
    3: "ожидает передачи от продавца",
    4: "можно забрать",
    5: "сделка завершена",
    6: "отменено",
    7: "продан — ждёт подтверждения покупателем",
}


_currency_alerted = False


async def _currency_ok(items: list[dict]) -> bool:
    """Сверяем валюту аккаунта (из /items) с MARKET_CURRENCY. При расхождении
    репрайсер ПОЛНОСТЬЮ останавливается: иначе масштаб units не тот и set-price
    выставит цену в разы мимо (напр. 2690.88 RUB -> 2690880 units с cur=USD)."""
    global _currency_alerted
    acc_cur = None
    for it in items:
        c = it.get("currency")
        if c:
            acc_cur = str(c).upper()
            break
    if acc_cur is None or acc_cur == config.CURRENCY.upper():
        return True

    logger.error("ВАЛЮТА НЕ СОВПАДАЕТ: аккаунт={}, MARKET_CURRENCY={}. Репрайсер "
                 "ОСТАНОВЛЕН, чтобы не выставить цены мимо. Задай MARKET_CURRENCY={}",
                 acc_cur, config.CURRENCY, acc_cur)
    if not _currency_alerted:
        _currency_alerted = True
        try:
            await telegram.send(
                f"🛑 <b>Репрайсер остановлен: валюта не совпадает</b>\n"
                f"Аккаунт торгует в <b>{acc_cur}</b>, а в конфиге "
                f"<code>MARKET_CURRENCY={config.CURRENCY}</code>.\n"
                f"Масштаб units разный (RUB=100, USD/EUR=1000) — цены уехали бы мимо. "
                f"Поставь <code>MARKET_CURRENCY={acc_cur}</code> и перезапусти.")
        except Exception:
            logger.debug("currency alert send failed (ignored)")
    return False


async def _build_groups() -> tuple[dict[int, dict], list[dict]] | None:
    """GET /items -> (группы моих АКТИВНЫХ лотов по name_id, список неактивных),
    или None при сбое запроса."""
    global _items_disc
    items = await market_client.get_my_items()
    if items is None:
        return None

    if _items_disc < config.REPRICE_DISCOVERY_SAMPLE and items:
        _items_disc += 1
        try:
            logger.info("RAW /items sample #{} :: {}", _items_disc, json.dumps(items[:3])[:1000])
        except Exception:
            logger.info("RAW /items sample #{} (repr) :: {}", _items_disc, repr(items[:3])[:1000])

    # ЗАЩИТА: валюта аккаунта должна совпадать с MARKET_CURRENCY. Иначе масштаб
    # units другой (RUB=100 vs USD/EUR=1000) и set-price выставит дикую цену.
    if not await _currency_ok(items):
        return None

    groups: dict[int, dict] = {}
    inactive: list[dict] = []
    for it in items:
        hash_name = it.get("market_hash_name") or it.get("hash_name")
        if not hash_name:
            continue
        status = _int(it.get("status"), -1)
        price_units = _items_price_to_units(it.get("price"))

        # Не на продаже (продан/передаётся/забирается) — репрайсить нечего,
        # но показываем в меню, чтобы было видно, почему лот не двигается.
        if status != 1:
            inactive.append({"label": hash_name, "status": status,
                             "price_units": price_units})
            continue

        nid = names.resolve_name(hash_name)
        if nid is None:
            # предмет не в словаре имён -> репрайсить не можем, но не молчим
            inactive.append({"label": hash_name, "status": status,
                             "price_units": price_units, "unresolved": True})
            continue
        item_id = it.get("item_id") or it.get("id")
        if price_units is None or price_units <= 0 or item_id is None:
            continue
        g = groups.get(nid)
        if g is None:
            g = groups[nid] = {"hash_name": hash_name, "lots": [], "counts": {}}
        g["lots"].append({"item_id": item_id, "price_units": price_units,
                          "position": it.get("position")})
        g["counts"][price_units] = g["counts"].get(price_units, 0) + 1
    return groups, inactive


async def _reprice_group(g: dict, floor: int, competitor: int | None) -> None:
    """Снижение цены фронтовых лотов (по минимальной цене) до target, если нужно.
    Двигаем ТОЛЬКО вниз; никогда не поднимаем и никогда не ниже пола."""
    if competitor is None:
        return  # конкурентов нет -> держим цену как есть
    my_min = min(l["price_units"] for l in g["lots"])
    if competitor >= my_min:
        return  # я уже (не хуже) на первом месте
    target = max(competitor - config.REPRICE_STEP_UNITS, floor)
    if target >= my_min:
        # опуститься ниже мешает пол -> стоим ровно на полу, не двигаем
        return
    for lot in g["lots"]:
        if lot["price_units"] != my_min:
            continue  # трогаем только фронтовые лоты (по минимальной цене)
        await _apply(g["hash_name"], lot, target, competitor, floor)


async def _apply(hash_name: str, lot: dict, target: int, competitor: int, floor: int) -> None:
    old = lot["price_units"]
    status, data, raw = await market_client.set_price(item_id=lot["item_id"], price_units=target)

    if status == 200 and data and data.get("success"):
        lot["price_units"] = target
        logger.success("Repriced {} #{}: {} -> {} units (competitor {}, floor {})",
                       hash_name, lot["item_id"], old, target, competitor, floor)
        try:
            await telegram.notify_reprice(label=hash_name, old_units=old, new_units=target,
                                          competitor_units=competitor, floor_units=floor)
        except Exception:
            logger.debug("notify_reprice failed (ignored)")
        return

    # HTTP 500 / сеть -> результат неизвестен, НЕ повторяем вслепую.
    if status == 500 or status == -1:
        logger.error("repricer set-price HTTP {} on {} #{} — skip, no retry. {}",
                     status, hash_name, lot["item_id"], str(raw)[:200])
        return

    err = (data or {}).get("error") if data else raw
    logger.warning("repricer set-price failed {} #{}: {}", hash_name, lot["item_id"], str(err)[:200])


async def _cycle() -> None:
    global _book_disc
    built = await _build_groups()
    if built is None:
        logger.warning("repricer: /items failed — skip cycle")
        return
    groups, inactive = built
    state.repricer_inactive = inactive
    if not groups:
        logger.debug("repricer: активных лотов (status=1) нет — переоценивать нечего "
                     "({} неактивных)", len(inactive))

    view: dict[int, dict] = {}
    for nid, g in groups.items():
        floor = state.get_floor(nid)
        my_min = min(l["price_units"] for l in g["lots"])
        competitor: int | None = None
        is_top = True

        if floor is not None:
            book = await market_client.bid_ask(g["hash_name"])
            if _book_disc < config.REPRICE_DISCOVERY_SAMPLE and book is not None:
                _book_disc += 1
                try:
                    logger.info("RAW /bid-ask sample #{} :: {}", _book_disc, json.dumps(book)[:800])
                except Exception:
                    logger.info("RAW /bid-ask sample #{} (repr) :: {}", _book_disc, repr(book)[:800])
            asks, _bids = parse_book(book, depth=max(len(g["lots"]) + 3, config.BOOK_DEPTH))
            competitor = _best_competitor_units(asks, g["counts"])
            is_top = competitor is None or my_min <= competitor
            await _reprice_group(g, floor, competitor)
            # my_min мог измениться после переоценки — пересчитаем для меню
            my_min = min(l["price_units"] for l in g["lots"])
            is_top = competitor is None or my_min <= competitor

        view[nid] = {
            "label": g["hash_name"], "hash_name": g["hash_name"], "lots": g["lots"],
            "my_min_units": my_min, "competitor_units": competitor,
            "is_top": is_top, "floor_units": floor, "updated_ts": time.time(),
        }

    state.repricer_view = view


async def refresh_view() -> bool:
    """Разовое обновление снапшота из /items без переоценки (для кнопки Refresh
    в меню «Продажа»). True — успех."""
    built = await _build_groups()
    if built is None:
        return False
    groups, inactive = built
    state.repricer_inactive = inactive
    view: dict[int, dict] = {}
    for nid, g in groups.items():
        floor = state.get_floor(nid)
        my_min = min(l["price_units"] for l in g["lots"])
        view[nid] = {
            "label": g["hash_name"], "hash_name": g["hash_name"], "lots": g["lots"],
            "my_min_units": my_min, "competitor_units": None,
            "is_top": True, "floor_units": floor, "updated_ts": time.time(),
        }
    state.repricer_view = view
    return True


async def run_loop() -> None:
    if not config.REPRICER_ENABLED:
        logger.info("Repricer disabled (REPRICER_ENABLED=0)")
        return
    logger.info("Repricer started · interval {}s · step {} units · cur={} scale={} · "
                "alfaskins={} · read_format={} · floors in RAM (empty at start)",
                int(config.REPRICE_INTERVAL_SEC), config.REPRICE_STEP_UNITS,
                config.CURRENCY, config.PRICE_UNITS_SCALE,
                config.BIDASK_WITH_ALFASKINS, config.MARKET_READ_PRICE_FORMAT)
    if config.MARKET_READ_PRICE_FORMAT != "value":
        logger.error("MARKET_READ_PRICE_FORMAT={!r}, а market.csgo отдаёт цены как "
                     "десятичное значение. При 'units' цены схлопываются в одно число "
                     "и репрайсер всегда считает себя топом. Поставь value (или убери "
                     "переменную).", config.MARKET_READ_PRICE_FORMAT)
    if not config.BIDASK_WITH_ALFASKINS:
        logger.warning("BIDASK_WITH_ALFASKINS=0 — лоты alfaskins НЕ учитываются как "
                       "конкуренты, бот не будет подрезать их цены.")
    # дать боту прогреться (names/WS/сессия) перед первым запросом
    await asyncio.sleep(min(config.REPRICE_INTERVAL_SEC, 10.0))
    while True:
        try:
            await _cycle()
        except Exception:
            logger.exception("repricer cycle failed")
        await asyncio.sleep(config.REPRICE_INTERVAL_SEC)
