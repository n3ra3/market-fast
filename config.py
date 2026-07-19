"""
Конфигурация бота. Только чтение окружения и константы — никакой логики.

Все значения читаются из переменных окружения (.env на Render / в Docker).
Единственный обязательный параметр — MARKET_API_KEY.
"""
from __future__ import annotations

import os


def _load_dotenv(path: str = ".env") -> None:
    """Минимальный загрузчик .env (для локального запуска). В Docker/Render
    переменные приходят из окружения и .env не нужен."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()


def _get(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _get_int(name: str, default: int) -> int:
    try:
        return int(_get(name) or default)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return float(_get(name) or default)
    except ValueError:
        return default


def _get_bool(name: str, default: bool = False) -> bool:
    val = _get(name).lower()
    if not val:
        return default
    return val in ("1", "true", "yes", "on", "y")


# ──────────────────────────────────────────────────────────────────────────
# Market.CSGO
# ──────────────────────────────────────────────────────────────────────────
API_KEY: str = _get("MARKET_API_KEY")

# Валюта и канал WebSocket. Канал: public:items:730:<rub|usd|eur>
CURRENCY: str = (_get("MARKET_CURRENCY", "USD")).upper()
GAME_ID: int = _get_int("MARKET_GAME_ID", 730)

# Масштаб «units». Эмпирически (подтверждено старым проектом):
#   USD/EUR -> 1000 (т.е. $0.150 == 150 units),  RUB -> 100.
# Цена в API /buy передаётся именно в этих units.
_DEFAULT_SCALE = 100 if CURRENCY == "RUB" else 1000
PRICE_UNITS_SCALE: int = _get_int("PRICE_UNITS_SCALE", _DEFAULT_SCALE)

API_BASE: str = "https://market.csgo.com/api/v2"
GET_WS_TOKEN_URL: str = f"{API_BASE}/get-ws-token"
BUY_URL: str = f"{API_BASE}/buy"
GET_MONEY_URL: str = f"{API_BASE}/get-money"
PING_URL: str = f"{API_BASE}/ping"       # keep-alive «онлайн», держит лоты на продаже
NAMES_URL: str = f"{API_BASE}/dictionary/names.json"
# Репрайсер (продажа): мои лоты, стакан по предмету, перестановка цены.
ITEMS_URL: str = f"{API_BASE}/items"          # мои лоты, выставленные на продажу
BID_ASK_URL: str = f"{API_BASE}/bid-ask"      # стакан по hash_name (ask+bid)
SET_PRICE_URL: str = f"{API_BASE}/set-price"  # переставить цену / снять (price=0)

# ──────────────────────────────────────────────────────────────────────────
# WebSocket (Centrifugo)
# ──────────────────────────────────────────────────────────────────────────
WS_URL: str = _get("WS_URL", "wss://wsprice.csgo.com/connection/websocket")
WS_CHANNEL: str = f"public:items:{GAME_ID}:{CURRENCY.lower()}"

# ──────────────────────────────────────────────────────────────────────────
# Rate limiter
# ──────────────────────────────────────────────────────────────────────────
# Market.CSGO снимает API-ключ при >5 req/s. Держим строго ≤ 4.
MARKET_MAX_RPS: float = _get_float("MARKET_MAX_RPS", 4.0)
# Резерв «полосы» под покупку: сканер/служебка не съедают последний токен,
# чтобы запрос покупки всегда уходил без ожидания.
RATE_BUY_RESERVE: float = _get_float("RATE_BUY_RESERVE", 1.0)

# ──────────────────────────────────────────────────────────────────────────
# HTTP / сессия
# ──────────────────────────────────────────────────────────────────────────
HTTP_TIMEOUT_SEC: float = _get_float("HTTP_TIMEOUT_SEC", 8.0)
BUY_TIMEOUT_SEC: float = _get_float("BUY_TIMEOUT_SEC", 6.0)
# Прогрев соединения с market.csgo (чтобы первый /buy не платил за TLS-handshake).
WARMUP_ENABLED: bool = _get_bool("WARMUP_ENABLED", True)
WARMUP_INTERVAL_SEC: float = _get_float("WARMUP_INTERVAL_SEC", 60.0)

# ──────────────────────────────────────────────────────────────────────────
# Keep-alive продаж («онлайн»-статус)
# ──────────────────────────────────────────────────────────────────────────
# Market.CSGO снимает лоты с продажи, если аккаунт не пингует «онлайн» чаще, чем
# раз в 3 минуты. Держим лоты выставленными периодическим /ping?v=2.
# Параметр v=2 обязателен — без него метод отключён на стороне маркета.
SALE_PING_ENABLED: bool = _get_bool("SALE_PING_ENABLED", True)
# Интервал (сек). Безопасно держать заметно ниже лимита в 180с.
PING_INTERVAL_SEC: float = _get_float("PING_INTERVAL_SEC", 150.0)

USER_AGENT: str = _get("USER_AGENT", "market-fastbuy/2.0")
WS_ORIGIN: str = _get("WS_ORIGIN", "https://market.csgo.com")

# ──────────────────────────────────────────────────────────────────────────
# Покупка
# ──────────────────────────────────────────────────────────────────────────
# "id"        -> покупать конкретный оффер из WS-пуша (быстрее, точнее),
#                если в пуше есть offer id; иначе откат на hash_name.
# "hash_name" -> всегда покупать самый дешёвый ≤ лимита по hash_name.
BUY_BY: str = (_get("BUY_BY", "id")).lower()
# Сколько секунд держать offer_id в анти-дубль кеше.
OFFER_DEDUPE_TTL_SEC: float = _get_float("OFFER_DEDUPE_TTL_SEC", 30.0)
# Размер очереди сканер -> покупатель. Переполнение = дроп с предупреждением.
BUY_QUEUE_MAXSIZE: int = _get_int("BUY_QUEUE_MAXSIZE", 1000)
# Сколько воркеров-покупателей параллельно читают очередь.
BUYER_WORKERS: int = _get_int("BUYER_WORKERS", 2)

# ──────────────────────────────────────────────────────────────────────────
# Репрайсер (удержание моих лотов на первом месте в продаже)
# ──────────────────────────────────────────────────────────────────────────
# Полностью независимая подсистема, развязанная с покупкой. Отдельная asyncio-
# задача, все запросы к Market идут через общий rate_limiter с НИЗКИМ приоритетом
# (priority=False) -> переоценка никогда не задерживает покупку.
#
# ВАЖНО (безопасность, как у лимитов покупки): пол-лимит по каждому лоту живёт
# только в RAM и сбрасывается при рестарте. После старта репрайсер по каждому
# лоту выключен, пока пользователь вручную не задаст пол через Telegram.
REPRICER_ENABLED: bool = _get_bool("REPRICER_ENABLED", True)
# Как часто прогонять цикл переоценки (сек).
REPRICE_INTERVAL_SEC: float = _get_float("REPRICE_INTERVAL_SEC", 60.0)
# Минимальный шаг: на сколько units ставить НИЖЕ конкурента, чтобы держать топ.
REPRICE_STEP_UNITS: int = _get_int("REPRICE_STEP_UNITS", 1)
# Валюта для set-price (по умолчанию = валюте аккаунта/канала).
SELL_CURRENCY: str = (_get("SELL_CURRENCY", CURRENCY)).upper()
# Стакан bid-ask: включать ли alfaskins (0 = исключать, как в API по умолчанию).
BIDASK_WITH_ALFASKINS: int = 1 if _get_bool("BIDASK_WITH_ALFASKINS", False) else 0
# Формат цены в ответах рыночного чтения (bid-ask): "value" (десятичная валюта,
# напр. "441.5900") или "units" (целые units). По докам bid-ask отдаёт "value".
MARKET_READ_PRICE_FORMAT: str = (_get("MARKET_READ_PRICE_FORMAT", "value")).lower()
# Сколько первых сырых ответов /items и /bid-ask логировать целиком (калибровка).
REPRICE_DISCOVERY_SAMPLE: int = _get_int("REPRICE_DISCOVERY_SAMPLE", 5)
# Сколько уровней стакана показывать в Telegram по каждой стороне (ask/bid).
BOOK_DEPTH: int = _get_int("BOOK_DEPTH", 3)

# ──────────────────────────────────────────────────────────────────────────
# Discovery / парсинг WS-пуша
# ──────────────────────────────────────────────────────────────────────────
# DISCOVERY_MODE=1 -> бот только слушает и логирует сырые пуши, НЕ покупает.
DISCOVERY_MODE: bool = _get_bool("DISCOVERY_MODE", False)
# Сколько первых сырых публикаций залогировать целиком (для калибровки схемы).
DISCOVERY_SAMPLE: int = _get_int("DISCOVERY_SAMPLE", 20)

# Кандидаты ключей в пуше (правятся через env без изменения кода, когда увидим
# реальную схему канала). Перечисление через запятую, порядок = приоритет.
def _keys(name: str, default: str) -> tuple[str, ...]:
    raw = _get(name) or default
    return tuple(k.strip() for k in raw.split(",") if k.strip())


NAME_ID_KEYS: tuple[str, ...] = _keys("WS_NAME_ID_KEYS", "name_id,nameId,i,n")
PRICE_KEYS: tuple[str, ...] = _keys("WS_PRICE_KEYS", "price,value,p,c")
OFFER_ID_KEYS: tuple[str, ...] = _keys("WS_OFFER_ID_KEYS", "id,offer_id,offerId,o")
EVENT_KEYS: tuple[str, ...] = _keys("WS_EVENT_KEYS", "event,type,action")
# Значения event, которые НЕ означают доступный к покупке лот (снятие листинга).
EVENT_IGNORE: frozenset[str] = frozenset(
    k.strip().lower() for k in (_get("WS_EVENT_IGNORE", "remove,delete,deleted,sold").split(","))
    if k.strip()
)
# Цена в пуше: "value" (валюта, напр. 0.142) или "units" (целые units, напр. 142).
WS_PRICE_FORMAT: str = (_get("WS_PRICE_FORMAT", "value")).lower()

# ──────────────────────────────────────────────────────────────────────────
# WatchList
# ──────────────────────────────────────────────────────────────────────────
WATCHLIST_PATH: str = _get("WATCHLIST_PATH", "watchlist.json")

# ──────────────────────────────────────────────────────────────────────────
# Telegram (только информирование и управление лимитами; не на пути покупки)
# ──────────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN: str = _get("MARKET_TELEGRAM_TOKEN") or _get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str = _get("MARKET_TELEGRAM_CHAT_ID") or _get("TELEGRAM_CHAT_ID")
TELEGRAM_ENABLED: bool = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)
TELEGRAM_API: str = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else ""

# Периодическая сводка активности (matched/bought/гонка) — чтобы видеть, что
# происходит, но не спамить поштучно (Telegram режет при >~20 msg/s).
NOTIFY_ACTIVITY: bool = _get_bool("NOTIFY_ACTIVITY", True)
NOTIFY_ACTIVITY_INTERVAL_SEC: float = _get_float("NOTIFY_ACTIVITY_INTERVAL_SEC", 20.0)
# Поштучные уведомления о проигранных гонках. ОСТОРОЖНО: при активном рынке это
# десятки сообщений в секунду -> Telegram временно забанит бота. Держи выключенным.
NOTIFY_RACE_LOSS: bool = _get_bool("NOTIFY_RACE_LOSS", False)

# ──────────────────────────────────────────────────────────────────────────
# Сервер здоровья (UptimeRobot не даёт Render заснуть)
# ──────────────────────────────────────────────────────────────────────────
HEALTH_HOST: str = _get("HEALTH_HOST", "0.0.0.0")
HEALTH_PORT: int = _get_int("PORT", 8000)  # Render передаёт PORT

# ──────────────────────────────────────────────────────────────────────────
# Логи
# ──────────────────────────────────────────────────────────────────────────
LOG_LEVEL: str = (_get("LOG_LEVEL", "INFO")).upper()


def validate() -> None:
    if not API_KEY:
        raise SystemExit("FATAL: MARKET_API_KEY is not set. See .env.example.")
    if CURRENCY.lower() not in ("rub", "usd", "eur"):
        raise SystemExit(f"FATAL: unsupported MARKET_CURRENCY={CURRENCY!r}")
