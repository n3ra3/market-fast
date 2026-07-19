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
import time
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


# ── Keep-alive продаж (ping-new + автотокен по куке) ─────────────────────────
# Кеш Steam webapi_token. Значения токена/куки НИКОГДА не логируем.
_steam_token: str | None = None
_steam_token_ts: float = -1e18


async def get_steam_token(*, force: bool = False) -> str | None:
    """Свежий Steam webapi_token по куке steamLoginSecure. Кешируем на
    STEAM_TOKEN_TTL_SEC (токен живёт 24ч). Ходит на steamcommunity.com — НЕ через
    market rate limiter (другой хост). None -> кука невалидна/протухла."""
    global _steam_token, _steam_token_ts
    if not config.STEAM_LOGIN_SECURE:
        return None
    now = time.monotonic()
    if not force and _steam_token and (now - _steam_token_ts) < config.STEAM_TOKEN_TTL_SEC:
        return _steam_token
    s = await init()
    try:
        async with s.get(
            config.STEAM_TOKEN_URL,
            headers={"Cookie": f"steamLoginSecure={config.STEAM_LOGIN_SECURE}"},
        ) as r:
            if r.status != 200:
                logger.warning("steam token fetch HTTP {}", r.status)
                return None
            import json
            data = json.loads(await r.text())
    except Exception as e:
        logger.warning("steam token fetch failed: {}", e)
        return None
    token = (data.get("data") or {}).get("webapi_token") if isinstance(data, dict) else None
    if not token:
        logger.warning("steam token: no webapi_token in response (cookie expired?)")
        return None
    _steam_token = token
    _steam_token_ts = now
    logger.debug("steam webapi_token refreshed")
    return token


async def ping_new(token: str) -> dict[str, Any]:
    """POST /ping-new — «онлайн»-статус (v2), держит лоты на продаже. priority=False.
    Ответ: {'success':true,'ping':'pong','online':true,'p2p':..,'steamApiKey':..}.
    При не-200 возвращаем реальное тело/статус (для диагностики 502 и т.п.)."""
    s = await init()
    body: dict[str, Any] = {"access_token": token}
    if config.STEAM_PROXY:
        body["proxy"] = config.STEAM_PROXY
    await limiter.acquire(priority=False)
    try:
        async with s.post(config.PING_NEW_URL, params={"key": config.API_KEY}, json=body) as r:
            status = r.status
            text = await r.text()
    except Exception as e:
        return {"success": False, "status": -1, "message": f"network: {e}"}
    if text[:1] in ("{", "["):
        try:
            import json
            data = json.loads(text)
            if isinstance(data, dict):
                data.setdefault("status", status)
                return data
        except Exception:
            pass
    # не-JSON (напр. HTML-страница 502 от Cloudflare) — отдаём статус и кусок тела
    return {"success": False, "status": status, "message": f"HTTP {status}", "raw": text[:200]}


async def _sale_alert(text: str) -> None:
    """Разовое уведомление в Telegram (ленивый импорт — избегаем цикла)."""
    try:
        import telegram
        await telegram.send(text)
    except Exception:
        logger.debug("sale alert send failed (ignored)")


async def _keepalive_attempt() -> tuple[bool, dict[str, Any]]:
    """Одна попытка держать «онлайн» с повторами при транзиентных 502/сетевых
    сбоях. Возвращает (успех, последний_ответ)."""
    last: dict[str, Any] = {"message": "no_token"}
    for attempt in range(1, config.PING_RETRIES + 1):
        token = await get_steam_token()
        if token is None:
            return False, {"message": "no_token"}
        data = await ping_new(token)
        last = data
        ok = bool(data.get("success"))
        online = bool(data.get("online", True)) if ok else False
        if ok and online:
            logger.debug("sale ping-new ok (p2p={}, steamApiKey={})",
                         data.get("p2p"), data.get("steamApiKey"))
            return True, data
        # Явно невалидный токен -> форсим свежий перед повтором.
        if "token" in str(data.get("message", "")).lower():
            await get_steam_token(force=True)
        if attempt < config.PING_RETRIES:
            logger.debug("ping-new attempt {}/{} failed ({}), retry in {}s",
                         attempt, config.PING_RETRIES, data.get("message") or data.get("status"),
                         config.PING_RETRY_DELAY_SEC)
            await asyncio.sleep(config.PING_RETRY_DELAY_SEC)
    return False, last


async def sale_keepalive_loop() -> None:
    """Периодический ping-new, чтобы маркет не уводил аккаунт в офлайн и не снимал
    лоты с продажи. Токен обновляется автоматически по куке steamLoginSecure.
    При устойчивом сбое — разовый алерт в Telegram (лоты под угрозой), без спама."""
    if not config.SALE_PING_ENABLED:
        logger.info("Sale keep-alive disabled (SALE_PING_ENABLED=0)")
        return
    if not config.STEAM_LOGIN_SECURE:
        logger.warning("Sale keep-alive: STEAM_LOGIN_SECURE не задан — ping-new не "
                       "работает, лоты будут уходить в офлайн")
        await _sale_alert("⚠️ Keep-alive продаж выключен: не задан STEAM_LOGIN_SECURE. "
                          "Лоты будут сниматься с продажи, пока не добавишь куку в env.")
        return
    logger.info("Sale keep-alive: POST /ping-new every {}s (retries={}, token via steamLoginSecure)",
                int(config.PING_INTERVAL_SEC), config.PING_RETRIES)
    await asyncio.sleep(5.0)  # дать сессии подняться
    prev_ok = True
    while True:
        try:
            ok, data = await _keepalive_attempt()
            if ok:
                if not prev_ok:
                    await _sale_alert("🟢 Продажи снова онлайн — ping-new восстановлен.")
                prev_ok = True
            else:
                logger.warning("sale ping-new failed after {} attempts: {}",
                               config.PING_RETRIES, data)
                if prev_ok:
                    await _sale_alert(_keepalive_fail_text(data))
                prev_ok = False
        except Exception:
            logger.exception("sale keep-alive iteration failed")
        await asyncio.sleep(config.PING_INTERVAL_SEC)


def _keepalive_fail_text(data: dict[str, Any]) -> str:
    """Понятный текст алерта в зависимости от причины сбоя."""
    if data.get("message") == "no_token":
        return ("🔴 <b>Не удалось получить Steam-токен</b> — кука steamLoginSecure "
                "протухла или неверна. Обнови <code>STEAM_LOGIN_SECURE</code> в env. "
                "Лоты могут уйти в офлайн.")
    status = data.get("status")
    body = data.get("message") or data.get("raw") or str(data)
    tail = ""
    if status == 502:
        tail = ("\n\nЭто шлюзовая ошибка: Market не смог провалидировать токен через "
                "Steam. Если держится — вероятно, нужен residential "
                "<code>STEAM_PROXY</code> (IP дата-центра Steam не пускает).")
    return ("🔴 <b>Ping продаж не проходит</b> — лоты могут уйти в офлайн.\n"
            f"Ответ: <code>{str(body)[:200]}</code>" + tail)
