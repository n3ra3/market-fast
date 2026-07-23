"""
Состояние бота в оперативной памяти.

ВАЖНО (требование безопасности из ТЗ):
  - Лимиты покупки живут ТОЛЬКО в памяти и НИКОГДА не пишутся на диск.
  - После любого рестарта лимитов нет -> покупка выключена -> чистый SCAN.
  - Покупка по предмету включается только когда пользователь задал лимит.

WatchList (какие name_id мы вообще отслеживаем) грузится из файла при старте —
он постоянный. Лимиты к нему не относятся и не сохраняются.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class WatchItem:
    name_id: int
    label: str            # человекочитаемое имя (market_hash_name) для логов/Telegram
    limit_units: int | None = None   # None -> SCAN-only (не покупаем)


# name_id -> WatchItem
items: dict[int, WatchItem] = {}
# быстрый O(1) фильтр в горячем пути сканера
watch_ids: frozenset[int] = frozenset()


@dataclass
class Stats:
    started_at: float = field(default_factory=time.time)
    ws_connected: bool = False
    ws_last_event_ts: float = 0.0
    received: int = 0          # всего пушей с распознанным предметом
    matched: int = 0          # совпало с watchlist и прошло лимит
    buys_ok: int = 0
    buys_fail: int = 0
    buys_uncertain: int = 0


stats = Stats()

# Агрегатор активности для периодической сводки в Telegram (не спамим поштучно).
# label -> {matched, bought, race, fail, uncertain}
activity: dict[str, dict[str, int]] = {}


def bump_activity(label: str, key: str) -> None:
    d = activity.get(label)
    if d is None:
        d = activity[label] = {"matched": 0, "bought": 0, "race": 0, "fail": 0, "uncertain": 0}
    d[key] = d.get(key, 0) + 1


def snapshot_activity() -> dict[str, dict[str, int]]:
    """Вернуть накопленное и обнулить."""
    global activity
    snap = activity
    activity = {}
    return snap


def set_watchlist(new_items: list[WatchItem]) -> None:
    global items, watch_ids
    items = {it.name_id: it for it in new_items}
    watch_ids = frozenset(items.keys())


def armed_count() -> int:
    return sum(1 for it in items.values() if it.limit_units is not None)


def is_scan_mode() -> bool:
    """SCAN, пока ни на один предмет не задан лимит."""
    return armed_count() == 0


def set_limit(name_id: int, limit_units: int) -> WatchItem | None:
    it = items.get(name_id)
    if it is None:
        return None
    it.limit_units = limit_units
    return it


def clear_limit(name_id: int) -> WatchItem | None:
    it = items.get(name_id)
    if it is None:
        return None
    it.limit_units = None
    return it


def disarm_all() -> int:
    """Сбросить все лимиты -> вернуться в SCAN. Возвращает число снятых."""
    n = 0
    for it in items.values():
        if it.limit_units is not None:
            it.limit_units = None
            n += 1
    return n


# ──────────────────────────────────────────────────────────────────────────
# Репрайсер (полностью отдельно от лимитов покупки).
#
# Пол-лимит по каждому предмету (ключ = name_id) живёт ТОЛЬКО в RAM и сбрасыва-
# ется при рестарте — как и лимиты покупки. Пусто при старте -> репрайсер по
# всем лотам выключен, пока пользователь не задаст пол через Telegram.
# ──────────────────────────────────────────────────────────────────────────
# name_id -> пол в units (ниже не опускаемся)
repricer_floors: dict[int, int] = {}

# Снапшот живых лотов для Telegram-меню «Продажа». Репрайсер переписывает его
# каждый цикл из /items; Telegram только читает. Структура на name_id:
#   {label, hash_name, lots:[{item_id, price_units, position}],
#    my_min_units, competitor_units, is_top, floor_units, updated_ts}
repricer_view: dict[int, dict] = {}

# Лоты из /items, которые НЕ на продаже (status != 1) — только для показа в меню,
# чтобы было видно, почему репрайсер их не трогает. Список {label, status, price_units}.
repricer_inactive: list[dict] = []


def set_floor(name_id: int, floor_units: int) -> None:
    repricer_floors[name_id] = floor_units


def get_floor(name_id: int) -> int | None:
    return repricer_floors.get(name_id)


def clear_floor(name_id: int) -> bool:
    return repricer_floors.pop(name_id, None) is not None


def repricer_armed_count() -> int:
    return len(repricer_floors)


def disarm_repricer() -> int:
    """Снять все полы репрайсера. Возвращает число снятых."""
    n = len(repricer_floors)
    repricer_floors.clear()
    return n
