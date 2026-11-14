"""Telegram-бот P2P Terminal: запуск Mini App, оплата подписки, админ-команды (aiogram 3.x)."""
import asyncio
import logging
import os
from html import escape
from typing import Optional

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    Message,
    WebAppInfo,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bot")

# ============================== КОНФИГ ==============================
BOT_TOKEN = os.getenv9(""9088393155:AAF3IoVZgqJ8mt-VPJPl6ArmCHihpb9sa4E)
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://oksana1971pa-dot.github.io/Valen-P2P/")
API_URL = os.getenv("API_URL", "https://valen-p2p-2.onrender.com").rstrip("/")
ADMIN_TOKEN = os.getenv("ABSURDISTRUE456654", "")
PAYMENT_DETAILS = os.getenv("PAYMENT_DETAILS", "").strip()  # реквизиты для оплаты (необязательно)
try:
    ADMIN_ID = int(os.getenv("7066723368", "0"))
except ValueError:
    ADMIN_ID = 0

TIERS = ("free", "pro", "vip")
http = httpx.AsyncClient(timeout=10.0)

user_router = Router()
admin_router = Router()
admin_router.message.filter(F.from_user.id == ADMIN_ID)  # админ-команды только для ADMIN_ID

# ============================== ТЕКСТЫ И КЛАВИАТУРЫ ==============================
START_TEXT = (
    "<b>P2P Terminal</b>\n\n"
    "Сканер P2P-спредов Bybit и OKX и закрытая OTC-доска для проверенных мерчантов.\n\n"
    "• Спреды USDT и BTC в UAH, RUB, BYN и PLN\n"
    "• Закрытые сделки с комиссией от 0.2%\n"
    "• Тарифы FREE, PRO и VIP\n\n"
    "Нажмите кнопку ниже, чтобы открыть терминал."
)

ADMIN_HELP = (
    "<b>Админ-команды</b>\n\n"
    "<code>/set_tier &lt;telegram_id&gt; &lt;free|pro|vip&gt; [0|1]</code>\n"
    "Выдать тариф. Третий параметр: 1 включает верификацию, 0 снимает.\n\n"
    "<code>/complete &lt;order_id&gt;</code>\n"
    "Закрыть сделку в статусе IN_PROGRESS и засчитать её обоим участникам.\n\n"
    "<code>/stats</code>\n"
    "Эта справка."
)


def open_kb(with_tariffs: bool = True) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="Открыть терминал", web_app=WebAppInfo(url=WEBAPP_URL))]]
    if with_tariffs:
        rows.append([InlineKeyboardButton(text="Тарифы и оплата", callback_data="tariffs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tariffs_text(user_id: int) -> str:
    details = escape(PAYMENT_DETAILS) if PAYMENT_DETAILS else "Реквизиты присылает администратор: отправьте любое сообщение с чеком или вопросом, и он ответит."
    return (
        "<b>Тарифы</b>\n"
        "FREE, $0: спреды до 0.5%, до 3 связок.\n"
        "PRO, $50/мес: спреды до 2.0%, сделки до $3,000, комиссия 1.0%.\n"
        "VIP, $500/мес: все спреды в реальном времени, создание заявок, сделки без лимита, комиссия 0.2%.\n\n"
        f"<b>Оплата</b>\n{details}\n\n"
        "<b>Как отправить чек</b>\n"
        "Пришлите скриншот или файл чека в этот чат, в подписи укажите тариф. "
        "Бот передаст его администратору, тариф включат после проверки.\n\n"
        f"Ваш ID для сверки: <code>{user_id}</code>"
    )


# ============================== БЭКЕНД ==============================
async def call_api(path: str, payload: dict) -> tuple:
    """POST к FastAPI с админ-токеном. Возвращает (данные, None) или (None, текст ошибки)."""
    try:
        r = await http.post(f"{API_URL}{path}", json=payload, headers={"X-Admin-Token": ADMIN_TOKEN})
    except httpx.HTTPError as e:
        log.warning("Бэкенд недоступен: %s", e)
        return None, f"Бэкенд недоступен ({e.__class__.__name__}). Проверьте API_URL: {API_URL}"
    try:
        data = r.json()
    except ValueError:
        data = None
    if r.is_success:
        return data, None
    detail: Optional[object] = data.get("detail") if isinstance(data, dict) else None
    if isinstance(detail, list):
        detail = "; ".join(str(d.get("msg", d)) if isinstance(d, dict) else str(d) for d in detail)
    return None, f"{r.status_code}: {detail or r.reason_phrase}"


# ============================== ПОЛЬЗОВАТЕЛИ ==============================
@user_router.message(CommandStart())
async def cmd_start(m: Message) -> None:
    await m.answer(START_TEXT, reply_markup=open_kb())


@user_router.callback_query(F.data == "tariffs")
async def cb_tariffs(cb: CallbackQuery) -> None:
    if cb.message:
        await cb.message.answer(tariffs_text(cb.from_user.id), reply_markup=open_kb(with_tariffs=False))
    await cb.answer()


@user_router.message(F.photo | F.document, F.chat.type == "private", F.from_user.id != ADMIN_ID)
async def on_receipt(m: Message) -> None:
    """Чек об оплате уходит администратору вместе с ID и готовой командой для выдачи тарифа."""
    u = m.from_user
    if not ADMIN_ID:
        await m.answer("Приём чеков пока не настроен. Напишите администратору напрямую.")
        return
    try:
        await m.bot.send_message(
            ADMIN_ID,
            f"Чек от {escape(u.full_name)}"
            f"{' (@' + escape(u.username) + ')' if u.username else ''}, ID <code>{u.id}</code>.\n"
            f"Выдать тариф: <code>/set_tier {u.id} pro</code> или <code>/set_tier {u.id} vip</code>",
        )
        await m.copy_to(ADMIN_ID)
    except TelegramAPIError as e:
        log.warning("Не удалось передать чек админу: %s", e)
        await m.answer("Не удалось передать чек администратору. Напишите ему напрямую.")
        return
    await m.answer("Чек получен и передан администратору. Мы сообщим, когда тариф будет включён.")


# ============================== АДМИН-КОМАНДЫ ==============================
@admin_router.message(Command("set_tier"))
async def cmd_set_tier(m: Message, command: CommandObject) -> None:
    args = (command.args or "").split()
    if (
        len(args) not in (2, 3)
        or not args[0].isdigit()
        or args[1].lower() not in TIERS
        or (len(args) == 3 and args[2] not in ("0", "1"))
    ):
        await m.answer("Формат: <code>/set_tier &lt;telegram_id&gt; &lt;free|pro|vip&gt; [0|1]</code>")
        return
    payload: dict = {"telegram_id": int(args[0]), "tier": args[1].lower()}
    if len(args) == 3:
        payload["is_verified"] = args[2] == "1"
    data, error = await call_api("/api/admin/set-tier", payload)
    if error:
        hint = "\nПользователь должен хотя бы раз открыть Mini App." if error.startswith("404") else ""
        await m.answer(f"Ошибка {escape(error)}{hint}")
        return
    verified = bool(data.get("is_verified"))
    note = ""
    try:
        await m.bot.send_message(
            data["telegram_id"],
            f"Ваш тариф обновлён: <b>{escape(str(data['tier']).upper())}</b>."
            + (" Статус верифицированного мерчанта включён." if verified else ""),
            reply_markup=open_kb(with_tariffs=False),
        )
        note = "\nПользователь уведомлён."
    except TelegramAPIError:
        note = "\nУведомить не удалось: пользователь не запускал бота."
    await m.answer(
        f"Готово. ID <code>{data['telegram_id']}</code> ({escape(data.get('username') or 'без username')})\n"
        f"Тариф: <b>{escape(str(data['tier']).upper())}</b>\n"
        f"Верификация: {'да' if verified else 'нет'}\n"
        f"Рейтинг: {float(data.get('rating', 0)):.1f}, сделок: {data.get('deals_count', 0)}{note}"
    )


@admin_router.message(Command("complete"))
async def cmd_complete(m: Message, command: CommandObject) -> None:
    args = (command.args or "").split()
    if len(args) != 1 or not args[0].isdigit():
        await m.answer("Формат: <code>/complete &lt;order_id&gt;</code>")
        return
    data, error = await call_api("/api/otc/complete", {"order_id": int(args[0])})
    if error:
        await m.answer(f"Ошибка {escape(error)}")
        return
    await m.answer(
        f"Сделка №{data['id']} закрыта, статус <b>{escape(str(data['status']))}</b>.\n"
        f"Сумма: ${float(data['amount_usd']):,.2f}, комиссия: ${float(data['fee_amount']):,.2f}.\n"
        f"Создатель: <code>{data['creator_id']}</code>, исполнитель: <code>{data['taker_id']}</code>.\n"
        "Счётчик сделок увеличен у обоих участников."
    )


@admin_router.message(Command("stats"))
async def cmd_stats(m: Message) -> None:
    await m.answer(ADMIN_HELP)


# ============================== ЗАПУСК ==============================
async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Задайте переменную окружения BOT_TOKEN")
    if not ADMIN_ID:
        log.warning("ADMIN_ID не задан: админ-команды и приём чеков отключены")
    if not ADMIN_TOKEN:
        log.warning("ADMIN_TOKEN не задан: запросы к /api/admin/* будут отклонены бэкендом")
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_routers(admin_router, user_router)
    try:
        await bot.delete_webhook()
        try:
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(text="Терминал", web_app=WebAppInfo(url=WEBAPP_URL))
            )
        except TelegramAPIError as e:
            log.warning("Не удалось поставить кнопку меню: %s", e)
        await dp.start_polling(bot)
    finally:
        await http.aclose()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
