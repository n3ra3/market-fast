"""
Клиент Centrifugo (правильный протокол через официальный centrifuge-python).

Старый проект пытался парсить SockJS-фреймы руками и подключался не по тому
протоколу — отсюда постоянные обрывы и откат на REST-polling. Здесь:

  - официальный async-клиент Centrifugo;
  - get_token = market_client.get_ws_token: клиент сам берёт свежий токен при
    коннекте и при истечении 10-мин токена -> авто-reconnect без участия юзера;
  - подписка на public:items:730:<cur>;
  - публикации уходят в scanner.on_publication.

Восстановление подписки, ping/pong и реконнект с бэкоффом — на стороне клиента.
"""
from __future__ import annotations

import asyncio

from centrifuge import (
    Client,
    ClientEventHandler,
    ConnectedContext,
    ConnectingContext,
    DisconnectedContext,
    ErrorContext,
    PublicationContext,
    SubscribedContext,
    SubscriptionErrorContext,
    SubscriptionEventHandler,
)
from loguru import logger

import config
import market_client
import scanner
import state


class _ClientEvents(ClientEventHandler):
    async def on_connecting(self, ctx: ConnectingContext) -> None:
        logger.info("WebSocket connecting... ({})", ctx.reason)

    async def on_connected(self, ctx: ConnectedContext) -> None:
        state.stats.ws_connected = True
        logger.success("WebSocket Connected (client={})", ctx.client)

    async def on_disconnected(self, ctx: DisconnectedContext) -> None:
        state.stats.ws_connected = False
        logger.warning("WebSocket Disconnected: {} ({})", ctx.code, ctx.reason)

    async def on_error(self, ctx: ErrorContext) -> None:
        logger.error("WebSocket client error: {}", ctx.error)


class _SubEvents(SubscriptionEventHandler):
    async def on_subscribed(self, ctx: SubscribedContext) -> None:
        logger.success("Subscribed: {}", ctx.channel)

    async def on_subscribing(self, ctx) -> None:
        logger.info("Subscribing to {} ...", config.WS_CHANNEL)

    async def on_error(self, ctx: SubscriptionErrorContext) -> None:
        logger.error("Subscription error: {}", ctx.error)

    async def on_publication(self, ctx: PublicationContext) -> None:
        try:
            await scanner.on_publication(ctx.pub.data)
        except Exception:
            logger.exception("scanner.on_publication failed")


async def run() -> None:
    """Подключиться и держать соединение вечно (reconnect внутри клиента)."""
    async def _token() -> str:
        return await market_client.get_ws_token()

    client = Client(
        config.WS_URL,
        events=_ClientEvents(),
        get_token=_token,
        headers={"Origin": config.WS_ORIGIN, "User-Agent": config.USER_AGENT},
        name="market-fastbuy",
    )
    sub = client.new_subscription(config.WS_CHANNEL, events=_SubEvents())

    await client.connect()
    await sub.subscribe()

    # держим задачу живой; клиент сам реконнектится
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Shutting down WebSocket client")
        try:
            await sub.unsubscribe()
            await client.disconnect()
        except Exception:
            pass
        raise
