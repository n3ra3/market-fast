"""
Работа со словарём имён.

При старте один раз скачиваем names.json и строим в памяти:
  id_to_name : name_id (int) -> market_hash_name (str)
  name_to_id : normalized(market_hash_name) -> name_id (int)

Повторно names.json НЕ загружаем. Дальше работаем только через словарь в RAM.

WS-канал ради экономии трафика отдаёт name_id, а не имя — поэтому матчинг
WatchList идёт по name_id (O(1)), а имена нужны только для логов/Telegram и
для разворачивания пользовательского WatchList в набор name_id.
"""
from __future__ import annotations

import json

from loguru import logger

import config
import market_client

id_to_name: dict[int, str] = {}
name_to_id: dict[str, int] = {}


def normalize(name: str) -> str:
    return " ".join(name.strip().lower().split())


async def load() -> int:
    """Скачать и распарсить names.json. Возвращает число записей."""
    s = await market_client.init()
    logger.info("Downloading names.json ...")
    async with s.get(config.NAMES_URL, params={"key": config.API_KEY}) as r:
        if r.status != 200:
            raise RuntimeError(f"names.json HTTP {r.status}")
        raw = await r.text()
    data = json.loads(raw)
    items = data.get("items", data if isinstance(data, list) else [])

    id_to_name.clear()
    name_to_id.clear()
    for it in items:
        try:
            nid = int(it["id"])
        except (KeyError, ValueError, TypeError):
            continue
        hn = it.get("hash_name") or it.get("market_hash_name") or ""
        if not hn:
            continue
        id_to_name[nid] = hn
        name_to_id[normalize(hn)] = nid

    logger.info(f"names.json loaded: {len(id_to_name)} entries")
    return len(id_to_name)


def resolve_name(name: str) -> int | None:
    """Имя -> name_id (точное совпадение по нормализованному hash_name)."""
    return name_to_id.get(normalize(name))


def label_for(name_id: int) -> str:
    return id_to_name.get(name_id, f"name_id={name_id}")
