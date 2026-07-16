"""
Единый HTTP-клиент к market.csgo.com.

  - Один общий aiohttp.ClientSession с Keep-Alive (не создаём соединение на
    каждый запрос).
  - get_ws_token() — для Centrifugo (клиент дёргает его сам при коннекте и
    при истечении 10-мин токена).
  - buy() — единственный запрос на пути покупки, идёт через rate limiter с
    приоритетом.
  - warmup() — периодический лёгкий запрос, держит TLS-соединение тёплым.

Telegram сюда НЕ ходит — у него отдельные вызовы без market-rate-limit.
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from loguru import logger

import config
from rate_limiter import limiter

_session: aiohttp.ClientSession | None = None


async def init() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300,
            keepalive_timeout=75,
            force_close=False,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=config.HTTP_TIMEOUT_SEC)
        _session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": config.USER_AGENT},
        )
        logger.debug("aiohttp session created")
    return _session


def session() -> aiohttp.ClientSession:
    if _session is None or _session.closed:
        raise RuntimeError("session not initialized; call market_client.init() first")
    return _session


async def close() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None


async def get_ws_token() -> str:
    """Получить ws-токен (JWT). Поднимает исключение при неудаче —
    centrifuge-python сам повторит с бэкоффом."""
    s = await init()
    await limiter.acquire(priority=False)
    async with s.get(config.GET_WS_TOKEN_URL, params={"key": config.API_KEY}) as r:
        text = await r.text()
        if r.status != 200:
            raise RuntimeError(f"get-ws-token HTTP {r.status}: {text[:200]}")
        import json
        data = json.loads(text)
    token = data.get("token") or data.get("ws_token")
    if not token:
        raise RuntimeError(f"get-ws-token: no token in response: {data}")
    logger.debug("ws token obtained")
    return token


async def buy(*, price_units: int, offer_id: int | str | None, hash_name: str | None
              ) -> tuple[int, dict[str, Any] | None, str]:
    """
    Отправить запрос покупки. price_units — ЛИМИТ пользователя (серверная
    гарантия «по этой цене или ниже»). Возвращает (http_status, json|None, raw_text).
    Минимум работы перед сетевым вызовом — это горячий путь.
    """
    s = session()
    params: dict[str, Any] = {"key": config.API_KEY, "price": price_units}
    if config.BUY_BY == "id" and offer_id is not None:
        params["id"] = offer_id
    elif hash_name is not None:
        params["hash_name"] = hash_name
    elif offer_id is not None:
        params["id"] = offer_id
    else:
        return 0, None, "no offer_id and no hash_name"

    await limiter.acquire(priority=True)
    try:
        async with s.get(config.BUY_URL, params=params,
                         timeout=aiohttp.ClientTimeout(total=config.BUY_TIMEOUT_SEC)) as r:
            text = await r.text()
            status = r.status
            ctype = r.headers.get("Content-Type", "")
    except asyncio.TimeoutError:
        return -1, None, "timeout"
    except aiohttp.ClientError as e:
        return -1, None, f"network: {e}"

    data: dict[str, Any] | None = None
    if "json" in ctype or text[:1] in ("{", "["):
        try:
            import json
            data = json.loads(text)
        except Exception:
            data = None
    return status, data, text


async def get_money() -> dict[str, Any] | None:
    s = await init()
    await limiter.acquire(priority=False)
    try:
        async with s.get(config.GET_MONEY_URL, params={"key": config.API_KEY}) as r:
            if r.status != 200:
                return None
            import json
            return json.loads(await r.text())
    except Exception:
        return None


# ── Репрайсер (продажа) ─────────────────────────────────────────────────────
# Все запросы ниже идут с priority=False: переоценка НИКОГДА не задерживает
# покупку (у покупки priority=True всегда проходит вперёд в rate_limiter).

async def get_my_items() -> list[dict[str, Any]] | None:
    """Мои лоты, выставленные на продажу (/items). None -> запрос не удался
    (цикл репрайсера просто пропустит итерацию)."""
    s = await init()
    await limiter.acquire(priority=False)
    try:
        async with s.get(config.ITEMS_URL, params={"key": config.API_KEY}) as r:
            if r.status != 200:
                return None
            import json
            data = json.loads(await r.text())
    except Exception:
        return None
    if isinstance(data, dict):
        items = data.get("items")
        return items if isinstance(items, list) else []
    if isinstance(data, list):
        return data
    return None


async def bid_ask(hash_name: str) -> dict[str, Any] | None:
    """Стакан по предмету (/bid-ask): {'ask':[{price,total}], 'bid':[...], ...}.
    Цена в ask/bid — строка в валюте (напр. '441.5900'), не units."""
    s = await init()
    params: dict[str, Any] = {
        "key": config.API_KEY, "hash_name": hash_name,
        "with_alfaskins": config.BIDASK_WITH_ALFASKINS,
    }
    await limiter.acquire(priority=False)
    try:
        async with s.get(config.BID_ASK_URL, params=params) as r:
            if r.status != 200:
                return None
            import json
            return json.loads(await r.text())
    except Exception:
        return None


async def set_price(*, item_id: int | str, price_units: int
                    ) -> tuple[int, dict[str, Any] | None, str]:
    """Переставить цену лота (/set-price). price_units=0 -> снять с продажи.
    Возвращает (http_status, json|None, raw_text). Осторожно, как в /buy:
    при HTTP 500 / сети вызывающий НЕ повторяет вслепую."""
    s = await init()
    params: dict[str, Any] = {
        "key": config.API_KEY, "item_id": item_id,
        "price": price_units, "cur": config.SELL_CURRENCY,
    }
    await limiter.acquire(priority=False)
    try:
        async with s.get(config.SET_PRICE_URL, params=params) as r:
            text = await r.text()
            status = r.status
    except asyncio.TimeoutError:
        return -1, None, "timeout"
    except aiohttp.ClientError as e:
        return -1, None, f"network: {e}"
    data: dict[str, Any] | None = None
    if text[:1] in ("{", "["):
        try:
            import json
            data = json.loads(text)
        except Exception:
            data = None
    return status, data, text


async def warmup_loop() -> None:
    """Держим соединение тёплым: лёгкий get-money раз в WARMUP_INTERVAL_SEC.
    Низкий приоритет — никогда не мешает покупке."""
    if not config.WARMUP_ENABLED:
        return
    await asyncio.sleep(config.WARMUP_INTERVAL_SEC)
    while True:
        try:
            await get_money()
            logger.debug("warmup ping ok")
        except Exception:
            logger.debug("warmup ping failed (ignored)")
        await asyncio.sleep(config.WARMUP_INTERVAL_SEC)
