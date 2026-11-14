"""P2P-сканер спредов + закрытая OTC-доска. FastAPI backend для Telegram Mini App."""
import asyncio
import logging
import os
import random
import secrets
import sqlite3
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any, Literal, Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("p2p")

# ============================== КОНФИГ ==============================
DB_PATH = os.getenv("DB_PATH", "p2p_otc.db")
_env_admin_token = os.getenv("ADMIN_TOKEN")
ADMIN_TOKEN = _env_admin_token or secrets.token_urlsafe(24)
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()] or ["*"]

ASSETS = ("USDT", "BTC")
FIATS = ("UAH", "RUB", "BYN", "PLN")
TIERS = ("free", "pro", "vip")

CACHE_TTL = 15.0          # сек, общий TTL кэша стаканов
VIP_MAX_AGE = 5.0         # сек, VIP получают данные не старше 5 сек (почти real-time, без бана API)
HTTP_TIMEOUT = 6.0
MIN_SPREAD = 0.1          # % — ниже не показываем
FREE_MAX_SPREAD = 0.5
FREE_LIMIT = 3
PRO_MAX_SPREAD = 2.0
MAX_QUOTES_PER_SIDE = 15  # лучших платёжных методов на биржу и сторону
MAX_PRICE_DEVIATION = 0.10  # отсев ловушек: цена дальше 10% от медианы стакана

PRO_DEAL_LIMIT = 3000.0
PRO_FEE = 0.01
VIP_FEE = 0.002

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

# ============================== БАЗА ДАННЫХ ==============================
_db_lock = threading.RLock()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
_conn.row_factory = sqlite3.Row
_conn.execute("PRAGMA journal_mode=WAL")
_conn.execute("PRAGMA busy_timeout=5000")
_conn.execute("PRAGMA foreign_keys=ON")
_conn.executescript(
    """
    CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        username    TEXT    NOT NULL DEFAULT '',
        tier        TEXT    NOT NULL DEFAULT 'free' CHECK (tier IN ('free','pro','vip')),
        rating      REAL    NOT NULL DEFAULT 5.0,
        deals_count INTEGER NOT NULL DEFAULT 0,
        is_verified INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS otc_orders (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_id     INTEGER NOT NULL REFERENCES users(telegram_id),
        type           TEXT    NOT NULL CHECK (type IN ('BUY','SELL')),
        asset          TEXT    NOT NULL CHECK (asset IN ('USDT','BTC')),
        fiat           TEXT    NOT NULL CHECK (fiat IN ('UAH','RUB','BYN','PLN')),
        price          REAL    NOT NULL,
        amount_usd     REAL    NOT NULL,
        payment_method TEXT    NOT NULL,
        status         TEXT    NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','IN_PROGRESS','COMPLETED')),
        taker_id       INTEGER REFERENCES users(telegram_id),
        fee_amount     REAL    NOT NULL DEFAULT 0,
        created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_orders_status ON otc_orders(status, asset, fiat);
    """
)


@contextmanager
def db_tx():
    """Атомарная транзакция (BEGIN IMMEDIATE) — защита от гонок при accept/complete."""
    with _db_lock:
        _conn.execute("BEGIN IMMEDIATE")
        try:
            yield _conn
        except BaseException:
            _conn.execute("ROLLBACK")
            raise
        else:
            _conn.execute("COMMIT")


def db_all(sql: str, params: tuple = ()) -> list:
    with _db_lock:
        return _conn.execute(sql, params).fetchall()


def db_one(sql: str, params: tuple = ()):
    with _db_lock:
        return _conn.execute(sql, params).fetchone()


def user_dict(row) -> dict:
    return {
        "telegram_id": row["telegram_id"],
        "username": row["username"],
        "tier": row["tier"],
        "rating": row["rating"],
        "deals_count": row["deals_count"],
        "is_verified": bool(row["is_verified"]),
    }


def order_dict(row, mask: bool = False) -> dict:
    return {
        "id": row["id"],
        "creator_id": None if mask else row["creator_id"],  # id тоже скрываем, иначе маска бессмысленна
        "username": "*** (Доступно в PRO)" if mask else row["creator_username"],
        "creator_tier": row["creator_tier"],
        "creator_rating": row["creator_rating"],
        "creator_deals_count": row["creator_deals_count"],
        "creator_is_verified": bool(row["creator_is_verified"]),
        "type": row["type"],
        "asset": row["asset"],
        "fiat": row["fiat"],
        "price": row["price"],
        "amount_usd": row["amount_usd"],
        "payment_method": row["payment_method"],
        "status": row["status"],
        "taker_id": row["taker_id"],
        "fee_amount": row["fee_amount"],
        "created_at": row["created_at"],
    }


ORDER_SELECT = """
    SELECT o.*, u.username AS creator_username, u.tier AS creator_tier, u.rating AS creator_rating,
           u.deals_count AS creator_deals_count, u.is_verified AS creator_is_verified
    FROM otc_orders o JOIN users u ON u.telegram_id = o.creator_id
"""


def is_admin(token: Optional[str]) -> bool:
    return bool(token) and secrets.compare_digest(token.encode(), ADMIN_TOKEN.encode())


# ============================== P2P-ПАРСЕРЫ ==============================
@dataclass(frozen=True)
class Offer:
    exchange: str
    payment: str
    price: float
    mock: bool = False


# side: "buy"  — пользователь ПОКУПАЕТ крипту (объявления мерчантов на продажу, ищем минимум)
#       "sell" — пользователь ПРОДАЁТ крипту (объявления мерчантов на покупку, ищем максимум)
_http: Optional[httpx.AsyncClient] = None


def get_http() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=HEADERS, follow_redirects=True)
    return _http


def _f(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


BYBIT_URL = "https://api2.bybit.com/fiat/otc/item/online"
BYBIT_PM_URL = "https://api2.bybit.com/fiat/otc/configuration/queryAllPaymentList"
_bybit_pm: dict = {}
_bybit_pm_ts = float("-inf")


async def bybit_payment_names(client: httpx.AsyncClient) -> dict:
    """Справочник ID платёжных методов Bybit (кэш 1 час). При ошибке — имена вида 'Bybit #ID'."""
    global _bybit_pm, _bybit_pm_ts
    if time.monotonic() - _bybit_pm_ts < 3600:
        return _bybit_pm
    try:
        r = await client.post(BYBIT_PM_URL, json={})
        r.raise_for_status()
        items = ((r.json().get("result") or {}).get("paymentConfigVo")) or []
        names = {str(i["paymentType"]): str(i["paymentName"]) for i in items if "paymentType" in i and "paymentName" in i}
        if names:
            _bybit_pm = names
            _bybit_pm_ts = time.monotonic()
            return _bybit_pm
        raise RuntimeError("пустой справочник платёжных методов")
    except Exception as e:
        log.warning("Bybit payment list недоступен: %s", e)
        _bybit_pm_ts = time.monotonic() - 3540  # повторная попытка через ~60 сек
        return _bybit_pm


async def fetch_bybit(client: httpx.AsyncClient, asset: str, fiat: str, side: str) -> list:
    body = {
        "userId": "", "tokenId": asset, "currencyId": fiat, "payment": [],
        "side": "1" if side == "buy" else "0",
        "size": "30", "page": "1", "amount": "", "authMaker": False, "canTrade": False,
    }
    r = await client.post(BYBIT_URL, json=body)
    r.raise_for_status()
    data = r.json()
    if data.get("ret_code") not in (0, "0", None):
        raise RuntimeError(f"Bybit ret_code={data.get('ret_code')}")
    items = (data.get("result") or {}).get("items") or []
    names = await bybit_payment_names(client)
    out = []
    for it in items:
        price = _f(it.get("price"))
        if price <= 0:
            continue
        for pid in it.get("payments") or ["0"]:
            out.append(Offer("Bybit", names.get(str(pid), f"Bybit #{pid}"), price))
    return out


OKX_URL = "https://www.okx.com/v3/c2c/tradingOrders/books"


async def fetch_okx(client: httpx.AsyncClient, asset: str, fiat: str, side: str) -> list:
    okx_side = "sell" if side == "buy" else "buy"  # у OKX side — это сторона объявления мерчанта
    params = {
        "quoteCurrency": fiat, "baseCurrency": asset, "side": okx_side, "paymentMethod": "all",
        "userType": "all", "showTrade": "false", "showFollow": "false", "showAlreadyTraded": "false",
        "isAbleFilter": "false", "receivingAds": "false", "t": int(time.time() * 1000),
    }
    r = await client.get(OKX_URL, params=params)
    r.raise_for_status()
    data = r.json()
    if str(data.get("code")) != "0":
        raise RuntimeError(f"OKX code={data.get('code')}")
    items = (data.get("data") or {}).get(okx_side) or []
    out = []
    for it in items:
        price = _f(it.get("price"))
        if price <= 0:
            continue
        for pm in it.get("paymentMethods") or ["bank"]:
            pm = str(pm).strip()
            out.append(Offer("OKX", (pm[:1].upper() + pm[1:]) or "Bank", price))
    return out


EXCHANGES = {"Bybit": fetch_bybit, "OKX": fetch_okx}

# ============================== МОК-ГЕНЕРАТОР ==============================
BASE_USDT = {"UAH": 41.6, "RUB": 92.5, "BYN": 3.25, "PLN": 3.65}
BTC_USD = 95000.0
MOCK_METHODS = {
    "UAH": ["Monobank", "PrivatBank", "A-Bank", "PUMB", "Bank Transfer"],
    "RUB": ["Tinkoff", "Sberbank", "Raiffeisenbank", "SBP", "Bank Transfer"],
    "BYN": ["Priorbank", "BelVEB", "MTBank", "Alfa-Bank", "Bank Transfer"],
    "PLN": ["Revolut", "BLIK", "mBank", "PKO BP", "Bank Transfer"],
}


def mock_book(exchange: str, asset: str, fiat: str, ref_mid: Optional[float] = None) -> dict:
    """Реалистичный стакан: единая «рыночная» цена + индивидуальный сдвиг каждого платёжного метода.
    Значения стабильны внутри окна CACHE_TTL. Если одна биржа живая — мок подстраивается под её цену."""
    bucket = int(time.time() // CACHE_TTL)
    mid = ref_mid
    if mid is None:
        base = BASE_USDT[fiat] * (BTC_USD if asset == "BTC" else 1.0)
        mid = base * (1 + random.Random(f"mid|{asset}|{fiat}|{bucket}").uniform(-0.004, 0.004))
    rng = random.Random(f"{exchange}|{asset}|{fiat}|{bucket}")
    methods = rng.sample(MOCK_METHODS[fiat], k=rng.randint(3, 5))
    book = {"buy": [], "sell": []}
    for pm in methods:
        shift = max(-0.025, min(0.025, rng.gauss(0, 0.009)))
        for i in range(3):
            step = i * 0.0006
            book["buy"].append(Offer(exchange, pm, round(mid * (1 + shift + 0.0025 + step), 2), True))
            book["sell"].append(Offer(exchange, pm, round(mid * (1 + shift - 0.0025 - step), 2), True))
    return book


# ============================== SPREAD ENGINE ==============================
def _sanitize(offers: list) -> list:
    if len(offers) < 5:
        return offers
    prices = sorted(o.price for o in offers)
    med = prices[len(prices) // 2]
    return [o for o in offers if abs(o.price - med) / med <= MAX_PRICE_DEVIATION]


async def load_book(client: httpx.AsyncClient, fetcher, asset: str, fiat: str) -> dict:
    buy, sell = await asyncio.gather(fetcher(client, asset, fiat, "buy"), fetcher(client, asset, fiat, "sell"))
    buy, sell = _sanitize(buy), _sanitize(sell)
    if not buy or not sell:
        raise RuntimeError("пустой стакан")
    return {"buy": buy, "sell": sell}


def _top(quotes, lowest_best: bool) -> list:
    by_ex: dict = {}
    for q in quotes:
        by_ex.setdefault(q.exchange, []).append(q)
    res = []
    for lst in by_ex.values():
        lst.sort(key=lambda q: q.price, reverse=not lowest_best)
        res.extend(lst[:MAX_QUOTES_PER_SIDE])
    return res


def compute_spreads(asset: str, fiat: str, books: dict) -> list:
    """Лучшая цена покупки/продажи для каждой пары (биржа, платёжный метод) и все связки между ними:
    внутри одной биржи (разные методы) и между биржами."""
    buys: dict = {}
    sells: dict = {}
    for book in books.values():
        for o in book["buy"]:
            k = (o.exchange, o.payment)
            if k not in buys or o.price < buys[k].price:
                buys[k] = o
        for o in book["sell"]:
            k = (o.exchange, o.payment)
            if k not in sells or o.price > sells[k].price:
                sells[k] = o
    result = []
    for b in _top(buys.values(), True):
        for s in _top(sells.values(), False):
            if (b.exchange, b.payment) == (s.exchange, s.payment):
                continue
            spread = (s.price - b.price) / b.price * 100
            if spread <= MIN_SPREAD:
                continue
            result.append({
                "id": f"{b.exchange}:{b.payment}>{s.exchange}:{s.payment}",
                "asset": asset,
                "fiat": fiat,
                "buy_exchange": b.exchange,
                "buy_payment_method": b.payment,
                "buy_price": b.price,
                "sell_exchange": s.exchange,
                "sell_payment_method": s.payment,
                "sell_price": s.price,
                "spread_percent": round(spread, 3),
                "type": "intra_exchange" if b.exchange == s.exchange else "cross_exchange",
                "is_mock": b.mock or s.mock,
                "locked": False,
            })
    result.sort(key=lambda x: x["spread_percent"], reverse=True)
    return result


@dataclass
class CacheEntry:
    ts: float
    updated_at: int
    spreads: list
    source: str  # live | mixed | mock


_cache: dict = {}
_locks: dict = {}


async def build_entry(asset: str, fiat: str) -> CacheEntry:
    client = get_http()
    names = list(EXCHANGES)
    results = await asyncio.gather(
        *(load_book(client, EXCHANGES[n], asset, fiat) for n in names), return_exceptions=True
    )
    books: dict = {}
    failed: list = []
    for name, res in zip(names, results):
        if isinstance(res, Exception):
            log.warning("%s %s/%s недоступна (%s) — используем мок", name, asset, fiat, res)
            failed.append(name)
        else:
            books[name] = res
    ref_mid = None
    if books:
        mids = [(min(o.price for o in b["buy"]) + max(o.price for o in b["sell"])) / 2 for b in books.values()]
        ref_mid = sum(mids) / len(mids)
    for name in failed:
        books[name] = mock_book(name, asset, fiat, ref_mid)
    source = "live" if not failed else ("mock" if len(failed) == len(names) else "mixed")
    return CacheEntry(time.monotonic(), int(time.time()), compute_spreads(asset, fiat, books), source)


async def get_entry(asset: str, fiat: str, max_age: float) -> CacheEntry:
    key = (asset, fiat)
    entry = _cache.get(key)
    if entry and time.monotonic() - entry.ts <= max_age:
        return entry
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:  # один запрос к биржам на ключ, остальные ждут и берут свежий кэш
        entry = _cache.get(key)
        if entry and time.monotonic() - entry.ts <= max_age:
            return entry
        try:
            entry = await build_entry(asset, fiat)
        except Exception:
            log.exception("Сбой построения спредов")
            if entry:
                return entry
            raise HTTPException(503, "Сервис спредов временно недоступен")
        _cache[key] = entry
        return entry


def hide_spread(s: dict) -> dict:
    return {
        "id": None, "asset": s["asset"], "fiat": s["fiat"],
        "buy_exchange": "VIP ONLY", "buy_payment_method": "VIP ONLY", "buy_price": None,
        "sell_exchange": "VIP ONLY", "sell_payment_method": "VIP ONLY", "sell_price": None,
        "spread_percent": "> 2.0", "type": None, "is_mock": s["is_mock"], "locked": True,
    }


def apply_tier(spreads: list, tier: str) -> list:
    if tier == "free":
        return [s for s in spreads if s["spread_percent"] <= FREE_MAX_SPREAD][:FREE_LIMIT]
    if tier == "pro":
        return [s if s["spread_percent"] <= PRO_MAX_SPREAD else hide_spread(s) for s in spreads]
    return spreads


def norm(value: Optional[str], allowed: tuple, name: str, lower: bool = False) -> str:
    v = (value or "").strip()
    v = v.lower() if lower else v.upper()
    if v not in allowed:
        raise HTTPException(422, f"Недопустимое значение {name}. Допустимо: {', '.join(allowed)}")
    return v


# ============================== МОДЕЛИ ==============================
class AuthIn(BaseModel):
    telegram_id: int = Field(gt=0)
    username: str = Field("", max_length=64)


class OrderCreateIn(BaseModel):
    creator_id: int
    type: Literal["BUY", "SELL"]
    asset: Literal["USDT", "BTC"]
    fiat: Literal["UAH", "RUB", "BYN", "PLN"]
    price: float = Field(gt=0, allow_inf_nan=False)
    amount_usd: float = Field(gt=0, allow_inf_nan=False)
    payment_method: str = Field(min_length=1, max_length=64)


class AcceptIn(BaseModel):
    order_id: int
    taker_id: int


class CompleteIn(BaseModel):
    order_id: int
    creator_id: Optional[int] = None


class SetTierIn(BaseModel):
    telegram_id: int
    tier: Literal["free", "pro", "vip"]
    is_verified: Optional[bool] = None


# ============================== ПРИЛОЖЕНИЕ ==============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    get_http()
    if not _env_admin_token:
        log.warning("ADMIN_TOKEN не задан, сгенерирован на этот запуск: %s", ADMIN_TOKEN)
    try:
        yield
    finally:
        if _http is not None:
            await _http.aclose()
        with _db_lock:
            _conn.close()


app = FastAPI(title="P2P Scanner & OTC Board", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=CORS_ORIGINS != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- ЧАСТЬ 1: сканер спредов ----------
@app.get("/api/spreads")
async def api_spreads(
    asset: str = Query("USDT"),
    fiat: str = Query("UAH"),
    tier: str = Query("free"),
):
    asset = norm(asset, ASSETS, "asset")
    fiat = norm(fiat, FIATS, "fiat")
    tier = norm(tier, TIERS, "tier", lower=True)
    entry = await get_entry(asset, fiat, VIP_MAX_AGE if tier == "vip" else CACHE_TTL)
    items = apply_tier(entry.spreads, tier)
    return {
        "asset": asset,
        "fiat": fiat,
        "tier": tier,
        "source": entry.source,
        "updated_at": entry.updated_at,
        "total_found": len(entry.spreads),
        "count": len(items),
        "spreads": items,
    }


# ---------- ЧАСТЬ 2: пользователи и OTC-доска ----------
@app.post("/api/auth/telegram")
def api_auth(body: AuthIn):
    with db_tx() as c:
        c.execute(
            """INSERT INTO users (telegram_id, username) VALUES (?, ?)
               ON CONFLICT(telegram_id) DO UPDATE SET
               username = CASE WHEN excluded.username <> '' THEN excluded.username ELSE users.username END""",
            (body.telegram_id, body.username.strip()),
        )
        row = c.execute("SELECT * FROM users WHERE telegram_id = ?", (body.telegram_id,)).fetchone()
        return user_dict(row)


@app.post("/api/admin/set-tier")
def api_set_tier(body: SetTierIn, x_admin_token: Optional[str] = Header(None)):
    """Выдача тарифа/верификации (заголовок X-Admin-Token)."""
    if not is_admin(x_admin_token):
        raise HTTPException(403, "Доступ только для администратора")
    with db_tx() as c:
        if not c.execute("SELECT 1 FROM users WHERE telegram_id = ?", (body.telegram_id,)).fetchone():
            raise HTTPException(404, "Пользователь не найден")
        c.execute("UPDATE users SET tier = ? WHERE telegram_id = ?", (body.tier, body.telegram_id))
        if body.is_verified is not None:
            c.execute("UPDATE users SET is_verified = ? WHERE telegram_id = ?", (int(body.is_verified), body.telegram_id))
        return user_dict(c.execute("SELECT * FROM users WHERE telegram_id = ?", (body.telegram_id,)).fetchone())


@app.post("/api/otc/create", status_code=201)
def api_otc_create(body: OrderCreateIn):
    with db_tx() as c:
        user = c.execute("SELECT * FROM users WHERE telegram_id = ?", (body.creator_id,)).fetchone()
        if not user:
            raise HTTPException(404, "Пользователь не найден")
        if user["tier"] != "vip":
            raise HTTPException(403, "Выкладывать заявки могут только VIP-мерчанты")
        cur = c.execute(
            """INSERT INTO otc_orders (creator_id, type, asset, fiat, price, amount_usd, payment_method)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (body.creator_id, body.type, body.asset, body.fiat, body.price, body.amount_usd,
             body.payment_method.strip()),
        )
        row = c.execute(ORDER_SELECT + " WHERE o.id = ?", (cur.lastrowid,)).fetchone()
        return order_dict(row)


@app.get("/api/otc/orders")
def api_otc_orders(
    asset: Optional[str] = Query(None),
    fiat: Optional[str] = Query(None),
    telegram_id: Optional[int] = Query(None),
    tier: Optional[str] = Query(None),
):
    """Тариф зрителя: из БД по telegram_id (приоритет), иначе из параметра tier, иначе free."""
    sql = ORDER_SELECT + " WHERE o.status = 'OPEN'"
    params: list = []
    if asset:
        sql += " AND o.asset = ?"
        params.append(norm(asset, ASSETS, "asset"))
    if fiat:
        sql += " AND o.fiat = ?"
        params.append(norm(fiat, FIATS, "fiat"))
    sql += """ ORDER BY CASE u.tier WHEN 'vip' THEN 0 WHEN 'pro' THEN 1 ELSE 2 END,
               u.is_verified DESC, o.created_at DESC, o.id DESC LIMIT 500"""
    viewer_tier = "free"
    if telegram_id is not None:
        viewer = db_one("SELECT tier FROM users WHERE telegram_id = ?", (telegram_id,))
        if viewer:
            viewer_tier = viewer["tier"]
    elif tier:
        viewer_tier = norm(tier, TIERS, "tier", lower=True)
    mask = viewer_tier == "free"
    orders = [order_dict(r, mask) for r in db_all(sql, tuple(params))]
    return {"tier": viewer_tier, "count": len(orders), "orders": orders}


@app.post("/api/otc/accept")
def api_otc_accept(body: AcceptIn):
    with db_tx() as c:
        taker = c.execute("SELECT * FROM users WHERE telegram_id = ?", (body.taker_id,)).fetchone()
        if not taker:
            raise HTTPException(404, "Пользователь не найден")
        if taker["tier"] == "free":
            raise HTTPException(403, "Принимать заявки могут только пользователи PRO и VIP")
        order = c.execute("SELECT * FROM otc_orders WHERE id = ?", (body.order_id,)).fetchone()
        if not order:
            raise HTTPException(404, "Заявка не найдена")
        if order["creator_id"] == body.taker_id:
            raise HTTPException(403, "Нельзя принять собственную заявку")
        if order["status"] != "OPEN":
            raise HTTPException(409, "Заявка уже недоступна")
        if taker["tier"] == "pro":
            if order["amount_usd"] > PRO_DEAL_LIMIT:
                raise HTTPException(403, "На тарифе PRO лимит одной сделки до $3,000. Для крупных сумм перейдите на VIP")
            rate = PRO_FEE
        else:
            rate = VIP_FEE
        fee = round(order["amount_usd"] * rate, 4)
        cur = c.execute(
            "UPDATE otc_orders SET status = 'IN_PROGRESS', taker_id = ?, fee_amount = ? WHERE id = ? AND status = 'OPEN'",
            (body.taker_id, fee, body.order_id),
        )
        if cur.rowcount != 1:
            raise HTTPException(409, "Заявка уже недоступна")
        row = c.execute(ORDER_SELECT + " WHERE o.id = ?", (body.order_id,)).fetchone()
        return order_dict(row)


@app.post("/api/otc/complete")
def api_otc_complete(body: CompleteIn, x_admin_token: Optional[str] = Header(None)):
    """Завершение сделки: создатель заявки (creator_id) или админ (заголовок X-Admin-Token)."""
    admin = is_admin(x_admin_token)
    with db_tx() as c:
        order = c.execute("SELECT * FROM otc_orders WHERE id = ?", (body.order_id,)).fetchone()
        if not order:
            raise HTTPException(404, "Заявка не найдена")
        if not admin and (body.creator_id is None or body.creator_id != order["creator_id"]):
            raise HTTPException(403, "Подтвердить сделку может только создатель заявки или админ")
        if order["status"] != "IN_PROGRESS" or order["taker_id"] is None:
            raise HTTPException(409, "Сделка не находится в статусе IN_PROGRESS")
        cur = c.execute(
            "UPDATE otc_orders SET status = 'COMPLETED' WHERE id = ? AND status = 'IN_PROGRESS'", (body.order_id,)
        )
        if cur.rowcount != 1:
            raise HTTPException(409, "Сделка не находится в статусе IN_PROGRESS")
        c.execute(
            "UPDATE users SET deals_count = deals_count + 1 WHERE telegram_id IN (?, ?)",
            (order["creator_id"], order["taker_id"]),
        )
        row = c.execute(ORDER_SELECT + " WHERE o.id = ?", (body.order_id,)).fetchone()
        return order_dict(row)
