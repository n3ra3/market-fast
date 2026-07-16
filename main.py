"""
Точка входа.

Старт (всегда так, по требованию безопасности):
  SCAN MODE · WatchList загружен · лимитов нет · покупка выключена ·
  бот уже слушает WebSocket, но ничего не покупает, пока ты не задашь лимит.

Поднимает: HTTP сессию -> names.json -> WatchList (с подтверждением) ->
очередь -> воркеры покупателя -> Centrifugo -> Telegram -> /health -> warmup.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys

from aiohttp import web
from loguru import logger

import buyer
import centrifugo_client
import config
import market_client
import names
import repricer
import scanner
import state
import telegram


# ── WatchList ──────────────────────────────────────────────────────────────
def _read_watchlist_file() -> list:
    path = config.WATCHLIST_PATH
    if not os.path.exists(path):
        logger.warning("WatchList file not found: {} — стартуем с пустым списком", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = data.get("items", [])
    return data if isinstance(data, list) else []


def _resolve_watchlist(entries: list) -> list[state.WatchItem]:
    resolved: list[state.WatchItem] = []
    seen: set[int] = set()
    table: list[tuple[str, str]] = []
    for entry in entries:
        name = None
        nid = None
        if isinstance(entry, str):
            name = entry
        elif isinstance(entry, dict):
            name = entry.get("name")
            nid = entry.get("name_id")

        if nid is None and name is not None:
            nid = names.resolve_name(name)

        if nid is None:
            table.append((name or str(entry), "❌ НЕ НАЙДЕН в names.json"))
            continue
        nid = int(nid)
        if nid in seen:
            continue
        seen.add(nid)
        label = names.label_for(nid)
        resolved.append(state.WatchItem(name_id=nid, label=label))
        table.append((name or label, f"✓ name_id={nid} → «{label}»"))

    logger.info("─" * 60)
    logger.info("WatchList resolution (подтверди, что предметы верные):")
    for req, res in table:
        logger.info("  {:<32} {}", req, res)
    logger.info("─" * 60)
    return resolved


# ── Health server (UptimeRobot) ────────────────────────────────────────────
async def _health(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def _start_health() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/health", _health)
    app.router.add_get("/", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.HEALTH_HOST, config.HEALTH_PORT)
    await site.start()
    logger.info("Health endpoint on :{}/health", config.HEALTH_PORT)
    return runner


# ── main ───────────────────────────────────────────────────────────────────
async def main() -> None:
    logger.remove()
    logger.add(sys.stderr, level=config.LOG_LEVEL,
               format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | {message}")
    config.validate()

    logger.info("Starting market-fastbuy | currency={} channel={} mode={}",
                config.CURRENCY, config.WS_CHANNEL,
                "DISCOVERY" if config.DISCOVERY_MODE else "LIVE")

    await market_client.init()
    await names.load()

    entries = _read_watchlist_file()
    state.set_watchlist(_resolve_watchlist(entries))
    logger.info("WatchList active: {} items · SCAN mode (лимитов нет)", len(state.items))

    queue: asyncio.Queue = asyncio.Queue(maxsize=config.BUY_QUEUE_MAXSIZE)
    scanner.set_queue(queue)

    tasks: list[asyncio.Task] = []
    for i in range(config.BUYER_WORKERS):
        tasks.append(asyncio.create_task(buyer.worker(queue, i + 1)))
    tasks.append(asyncio.create_task(market_client.warmup_loop()))
    tasks.append(asyncio.create_task(telegram.poll_loop()))
    tasks.append(asyncio.create_task(telegram.activity_reporter_loop()))
    # Репрайсер — отдельная задача, развязана с покупкой (priority=False запросы).
    tasks.append(asyncio.create_task(repricer.run_loop()))
    runner = await _start_health()
    await telegram.startup()
    tasks.append(asyncio.create_task(centrifugo_client.run()))

    stop = asyncio.Event()

    def _signal() -> None:
        logger.info("Shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal)
        except NotImplementedError:
            pass

    await stop.wait()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await telegram.close()
    await market_client.close()
    await runner.cleanup()
    logger.info("Stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
