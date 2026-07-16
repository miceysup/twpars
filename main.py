import json
import re
import asyncio
import time
import os
import sys
import logging
# pyrefly: ignore [missing-import]
import aiosqlite
import aiohttp
from datetime import datetime, date, timedelta, timezone

# pyrefly: ignore [missing-import]
from twitchio.ext import commands
from aiogram import Bot as AioBot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove,
)
from aiogram.exceptions import TelegramBadRequest

# ══════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════
CONFIG_FILE   = 'config.json'
CHANNELS_FILE = 'channels.txt'

# IRC-мониторинг: паттерн для отлова ссылок BetBoom в чате
BETBOOM_PATTERN   = r'(https?://(?:www\.)?betboom\.(?:ru|com)/freestream/[a-zA-Z0-9_-]+)'
ANTISPAM_COOLDOWN = 600   # 10 минут

# Fault-tolerant перезапуск
MAX_FAST_RETRIES = 3
FAST_RETRY_DELAY = 15    # секунд
LONG_RETRY_DELAY = 600   # 10 минут

# Поиск амбассадоров
GAME_NAMES       = ['Dota 2', 'Counter-Strike 2']
VIEWER_THRESHOLD = 500      # минимум зрителей для проверки
PANEL_DELAY      = 2.5      # секунд между запросами панелей
CHECK_DAYS       = 14       # дней до повторной проверки стримера

# Паттерн для панелей (ищет упоминания BetBoom-доменов)
BETBOOM_PANEL_RE = re.compile(
    r'betboom\.(?:ru|com|kz|by)|bb\.live',
    re.IGNORECASE
)

try:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as _f:
        _cfg = json.load(_f)
    TG_BOT_TOKEN     = _cfg.get('telegram_bot_token', '')
    TG_CHAT_ID       = str(_cfg.get('telegram_chat_id', ''))
    TWITCH_TOKEN     = _cfg.get('twitch_token', '')
    TWITCH_CLIENT_ID = _cfg.get('twitch_client_id', '')   # для Helix API
    ADMIN_IDS        = [int(x) for x in _cfg.get('admin_ids', [])]
    TG_PROXY         = _cfg.get('proxy', '') or None       # None = прямое подключение
except FileNotFoundError:
    logger.critical(f"Файл {CONFIG_FILE} не найден.")
    sys.exit(1)
except (ValueError, KeyError) as e:
    logger.critical(f"Ошибка в {CONFIG_FILE}: {e}")
    sys.exit(1)

if not TG_BOT_TOKEN or not TWITCH_TOKEN:
    logger.critical("Укажите telegram_bot_token и twitch_token в config.json!")
    sys.exit(1)

# ══════════════════════════════════════════════════════════════════
#  ПУТЬ К БАЗЕ ДАННЫХ
#  /data/bot.db  — для Amvera (Linux)
#  ./data/bot.db — для Windows / локальной разработки
# ══════════════════════════════════════════════════════════════════
if sys.platform.startswith('win'):
    _data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
else:
    _data_dir = '/data'
os.makedirs(_data_dir, exist_ok=True)
DB_PATH = os.path.join(_data_dir, 'bot.db')

# ══════════════════════════════════════════════════════════════════
#  РАБОТА С ФАЙЛОМ КАНАЛОВ
# ══════════════════════════════════════════════════════════════════
def load_channels() -> list[str]:
    try:
        with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
            result = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if 'twitch.tv/' in line:
                    line = line.split('twitch.tv/')[-1].split('/')[0]
                result.append(line.lower())
            return result
    except FileNotFoundError:
        logger.warning(f"Файл {CHANNELS_FILE} не найден — начинаем с пустого списка.")
        return []

def save_channels(channels: list[str]) -> None:
    with open(CHANNELS_FILE, 'w', encoding='utf-8') as f:
        for c in channels:
            f.write(c + '\n')

# ══════════════════════════════════════════════════════════════════
#  СЛОЙ БАЗЫ ДАННЫХ (async SQLite)
# ══════════════════════════════════════════════════════════════════
async def init_db() -> None:
    """Создаёт все таблицы при первом запуске."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица пойманных ссылок из IRC-чатов
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                channel   TEXT    NOT NULL,
                link      TEXT    NOT NULL,
                caught_at TEXT    NOT NULL
            )
        """)
        # Таблица проверенных поиском стримеров
        await db.execute("""
            CREATE TABLE IF NOT EXISTS checked_channels (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                login      TEXT    NOT NULL UNIQUE,
                checked_at TEXT    NOT NULL,
                status     TEXT    NOT NULL   -- 'ambassador' | 'empty'
            )
        """)
        await db.commit()
    logger.info(f"База данных готова: {DB_PATH}")


async def record_link(channel: str, link: str) -> None:
    """Записывает пойманную ссылку из IRC в БД."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO events (channel, link, caught_at) VALUES (?, ?, ?)",
            (channel, link, datetime.now(timezone.utc).isoformat())
        )
        await db.commit()


async def get_today_stats() -> dict[str, int]:
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT channel, COUNT(*) FROM events WHERE caught_at >= ? GROUP BY channel",
            (today,)
        )
        rows = await cur.fetchall()
    return {row[0]: row[1] for row in rows}


async def get_weekly_report() -> tuple[int, list[tuple[str, int]]]:
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur_total = await db.execute(
            "SELECT COUNT(*) FROM events WHERE caught_at >= ?", (week_ago,)
        )
        total = (await cur_total.fetchone())[0]
        cur_top = await db.execute(
            """SELECT channel, COUNT(*) AS cnt FROM events
               WHERE caught_at >= ?
               GROUP BY channel ORDER BY cnt DESC LIMIT 3""",
            (week_ago,)
        )
        top3 = await cur_top.fetchall()
    return total, top3


async def is_recently_checked(login: str) -> bool:
    """Возвращает True, если стример проверялся за последние CHECK_DAYS дней."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=CHECK_DAYS)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM checked_channels WHERE login = ? AND checked_at >= ?",
            (login, cutoff)
        )
        return (await cur.fetchone()) is not None


async def record_checked_channel(login: str, status: str) -> None:
    """Сохраняет результат проверки стримера ('ambassador' или 'empty')."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO checked_channels (login, checked_at, status)
               VALUES (?, ?, ?)
               ON CONFLICT(login)
               DO UPDATE SET checked_at = excluded.checked_at,
                             status     = excluded.status""",
            (login, datetime.now(timezone.utc).isoformat(), status)
        )
        await db.commit()


async def get_search_added_channels() -> list[str]:
    """Возвращает каналы, добавленные поиском (status='ambassador')."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT login FROM checked_channels WHERE status = 'ambassador' ORDER BY checked_at DESC"
        )
        rows = await cur.fetchall()
    return [row[0] for row in rows]

# ══════════════════════════════════════════════════════════════════
#  HELIX API — вспомогательные функции
# ══════════════════════════════════════════════════════════════════
def _helix_headers() -> dict:
    return {
        'Client-Id': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {TWITCH_TOKEN}',
    }

def _helix_session() -> aiohttp.ClientSession:
    """Создаёт aiohttp-сессию для Helix API (с прокси если настроен)."""
    return aiohttp.ClientSession()


async def helix_get_game_ids(session: aiohttp.ClientSession, names: list[str]) -> dict[str, str]:
    """Получает {game_name: game_id} для списка игр."""
    params = [('name', n) for n in names]
    proxy = TG_PROXY
    try:
        async with session.get(
            'https://api.twitch.tv/helix/games',
            headers=_helix_headers(),
            params=params,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
            return {g['name']: g['id'] for g in data.get('data', [])}
    except Exception as e:
        logger.error(f"[Helix] Ошибка получения ID игр: {e}")
        return {}


async def helix_get_live_streams(
    session: aiohttp.ClientSession, game_ids: list[str]
) -> list[dict]:
    """Возвращает все русскоязычные стримы с >VIEWER_THRESHOLD зрителей."""
    all_streams: list[dict] = []
    cursor = None
    proxy = TG_PROXY

    while True:
        params: list[tuple] = [('game_id', gid) for gid in game_ids]
        params += [('language', 'ru'), ('first', '100')]
        if cursor:
            params.append(('after', cursor))

        try:
            async with session.get(
                'https://api.twitch.tv/helix/streams',
                headers=_helix_headers(),
                params=params,
                proxy=proxy,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                data = await resp.json()
        except Exception as e:
            logger.error(f"[Helix] Ошибка получения стримов: {e}")
            break

        streams = data.get('data', [])
        if not streams:
            break

        for s in streams:
            if s.get('viewer_count', 0) <= VIEWER_THRESHOLD:
                # Список отсортирован по убыванию зрителей — можно остановиться
                return all_streams
            all_streams.append(s)

        cursor = data.get('pagination', {}).get('cursor')
        if not cursor:
            break

    return all_streams


async def helix_get_channel_panels_gql(
    session: aiohttp.ClientSession, login: str
) -> list[dict]:
    """Получает панели стримера через GraphQL (неофициальный API Twitch)."""
    url = 'https://gql.twitch.tv/gql'
    headers = {
        'Client-Id': 'kimne78kx3ncx6brgo4mv6wki5h1ko',
    }
    query = '''query($login: String!) { user(login: $login) { panels { ... on DefaultPanel { id title description linkURL } } } }'''
    try:
        async with session.post(
            url, headers=headers,
            json=[{'query': query, 'variables': {'login': login}}],
            proxy=TG_PROXY,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and isinstance(data, list):
                    user_data = data[0].get('data', {}).get('user') or {}
                    return user_data.get('panels', []) or []
    except Exception as e:
        logger.debug(f"[GQL Panels] Ошибка для login={login}: {e}")
    return []


async def helix_get_channel_info(
    session: aiohttp.ClientSession, broadcaster_id: str
) -> dict:
    """Возвращает информацию о канале (description, tags) из Helix."""
    try:
        async with session.get(
            'https://api.twitch.tv/helix/channels',
            headers=_helix_headers(),
            params=[('broadcaster_id', broadcaster_id)],
            proxy=TG_PROXY,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()
            items = data.get('data', [])
            return items[0] if items else {}
    except Exception as e:
        logger.debug(f"[Helix] Ошибка получения info для {broadcaster_id}: {e}")
        return {}


def has_betboom_link(text: str) -> bool:
    """Проверяет наличие BetBoom-домена в произвольном тексте."""
    return bool(text and BETBOOM_PANEL_RE.search(text))

# ══════════════════════════════════════════════════════════════════
#  FSM СОСТОЯНИЯ
# ══════════════════════════════════════════════════════════════════
class Form(StatesGroup):
    add_channel = State()   # ожидаем ввод ника стримера

# ══════════════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════════════
def make_main_kb() -> ReplyKeyboardMarkup:
    """Постоянная нижняя клавиатура для администраторов."""
    b = ReplyKeyboardBuilder()
    b.row(
        KeyboardButton(text="📊 Статус"),
        KeyboardButton(text="📈 Аналитика"),
    )
    b.row(
        KeyboardButton(text="📡 Каналы"),
        KeyboardButton(text="🏓 Пинг"),
    )
    b.row(
        KeyboardButton(text="➕ Добавить канал"),
        KeyboardButton(text="➖ Удалить канал"),
    )
    b.row(
        KeyboardButton(text="🔍 Поиск амбассадоров"),
        KeyboardButton(text="↩️ Отменить поиск"),
    )
    return b.as_markup(resize_keyboard=True, one_time_keyboard=False)

def make_cancel_kb() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой Отмена (показывается во время FSM)."""
    b = ReplyKeyboardBuilder()
    b.row(KeyboardButton(text="🔙 Отмена"))
    return b.as_markup(resize_keyboard=True, one_time_keyboard=True)

def make_refresh_kb(action: str):
    """Inline-кнопка 🔄 Обновить под сообщением."""
    b = InlineKeyboardBuilder()
    b.button(text="🔄 Обновить", callback_data=f"refresh:{action}")
    return b.as_markup()

def make_remove_kb(channels: list[str]):
    """Inline-кнопки с именами каналов для удаления."""
    b = InlineKeyboardBuilder()
    for ch in channels:
        b.button(text=f"❌  {ch}", callback_data=f"rm:{ch}")
    b.button(text="🔙 Отмена", callback_data="rm:__cancel__")
    b.adjust(2)
    return b.as_markup()

def make_undo_kb(channels: list[str]):
    """Inline-кнопки для отмены добавленных поиском каналов."""
    b = InlineKeyboardBuilder()
    for ch in channels:
        b.button(text=f"↩️ {ch}", callback_data=f"undo:{ch}")
    b.button(text="↩️ Удалить все", callback_data="undo:__all__")
    b.button(text="🔙 Закрыть", callback_data="undo:__close__")
    b.adjust(2)
    return b.as_markup()

# ══════════════════════════════════════════════════════════════════
#  TELEGRAM BOT + DISPATCHER
# ══════════════════════════════════════════════════════════════════
_session = AiohttpSession(proxy=TG_PROXY)
if TG_PROXY:
    logger.info(f"Telegram: используется прокси {TG_PROXY}")
tg_bot = AioBot(
    token=TG_BOT_TOKEN,
    session=_session,
    default=DefaultBotProperties(parse_mode='HTML'),
)
dp = Dispatcher(storage=MemoryStorage())

twitch_bot: "TwitchParser | None" = None   # заполняется в main()
_search_task: asyncio.Task | None = None    # текущая задача поиска

# ─── Проверка прав ───────────────────────────────────────────────
async def check_admin(message: types.Message) -> bool:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("🚫 У вас нет доступа.")
        return False
    return True

async def check_admin_cb(callback: types.CallbackQuery) -> bool:
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("🚫 У вас нет доступа.", show_alert=True)
        return False
    return True

# ─── Вспомогательные функции ─────────────────────────────────────
async def _do_add(message: types.Message, raw: str) -> None:
    """Общая логика добавления канала (команда + FSM + поиск)."""
    channel = (
        raw.strip().lower()
        .replace('https://www.twitch.tv/', '')
        .replace('https://twitch.tv/', '')
        .split('/')[0]
    )
    if not channel:
        await message.answer("⚠️ Некорректное имя канала.", reply_markup=make_main_kb())
        return
    if twitch_bot is None:
        await message.answer("⚠️ Twitch-бот ещё не запущен.", reply_markup=make_main_kb())
        return
    if channel in twitch_bot.channels_list:
        await message.answer(f"Канал <b>{channel}</b> уже в списке!", reply_markup=make_main_kb())
        return
    twitch_bot.channels_list.append(channel)
    save_channels(twitch_bot.channels_list)
    await twitch_bot.join_channels([channel])
    await message.answer(f"✅ Канал <b>{channel}</b> добавлен и подключён!", reply_markup=make_main_kb())

async def _build_status_text() -> str:
    today_stats = await get_today_stats()
    if twitch_bot is None or not twitch_bot.channels_list:
        return "Список каналов пуст."
    total_today = sum(today_stats.values())
    lines = []
    for ch in twitch_bot.channels_list:
        count = today_stats.get(ch, 0)
        count_str = f"{count} ссыл." if count else "нет ссылок"
        lines.append(f"🟢 <code>{ch:<20}</code> — {count_str} сегодня")
    return (
        "📊 <b>Статус мониторинга</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(lines) + "\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Каналов: <b>{len(twitch_bot.channels_list)}</b>  |  "
        f"Поймано сегодня: <b>{total_today}</b>"
    )

async def _build_count_text() -> str:
    total, top3 = await get_weekly_report()
    medals = ["🥇", "🥈", "🥉"]
    top_lines = [
        f"{medals[i]} <b>{ch}</b> — {cnt} ссыл."
        for i, (ch, cnt) in enumerate(top3)
    ] or ["Данных пока нет."]
    return (
        "📈 <b>Аналитика за последние 7 дней</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Поймано фрибетов: <b>{total}</b>\n\n"
        "🏆 <b>Топ стримеров:</b>\n"
        + "\n".join(top_lines)
    )

# ══════════════════════════════════════════════════════════════════
#  ПОИСК АМБАССАДОРОВ — фоновая задача
# ══════════════════════════════════════════════════════════════════
async def _safe_edit(msg: types.Message, text: str, **kwargs) -> None:
    """Редактирует сообщение с подавлением ошибок (сообщение могло быть удалено)."""
    try:
        await msg.edit_text(text, **kwargs)
    except Exception:
        pass


async def _do_search(status_msg: types.Message, force: bool = False) -> None:
    """
    Основная логика поиска амбассадоров BetBoom на Twitch.
    Запускается как asyncio.Task, корректно отменяется через task.cancel().
    """
    added: list[tuple[str, int, str]] = []  # (login, viewers, game)
    checked = 0

    try:
        async with _helix_session() as session:

            # ── Шаг 1: получаем ID игр ───────────────────────────
            await _safe_edit(
                status_msg,
                "🔍 <b>Поиск амбассадоров...</b>\n"
                f"Шаг 1/3: Получаю ID игр ({', '.join(GAME_NAMES)})..."
            )
            game_ids_map = await helix_get_game_ids(session, GAME_NAMES)
            if not game_ids_map:
                await _safe_edit(
                    status_msg,
                    "❌ <b>Ошибка:</b> Не удалось получить ID игр от Twitch API.\n"
                    "Проверь <code>twitch_client_id</code> в config.json."
                )
                return

            game_ids = list(game_ids_map.values())
            logger.info(f"[Search] Игры: {game_ids_map}")

            # ── Шаг 2: получаем список стримов ───────────────────
            await _safe_edit(
                status_msg,
                "🔍 <b>Поиск амбассадоров...</b>\n"
                f"Шаг 2/3: Запрашиваю стримы 🇷🇺 с >{VIEWER_THRESHOLD} зрителей..."
            )
            streams = await helix_get_live_streams(session, game_ids)

            if not streams:
                await _safe_edit(
                    status_msg,
                    "🏁 <b>Поиск завершён.</b>\n"
                    f"Стримов с >{VIEWER_THRESHOLD} зрителями не найдено."
                )
                return

            # Фильтруем: убираем уже отслеживаемых и недавно проверенных
            current_channels = set(twitch_bot.channels_list if twitch_bot else [])
            to_check = []
            for s in streams:
                login = s['user_login'].lower()
                if login in current_channels:
                    continue
                if not force and await is_recently_checked(login):
                    continue
                to_check.append(s)

            await _safe_edit(
                status_msg,
                "🔍 <b>Поиск амбассадоров...</b>\n"
                f"Шаг 3/3: Пробиваю панели стримеров...\n\n"
                f"Стримов с >{VIEWER_THRESHOLD} зр.: <b>{len(streams)}</b>\n"
                f"Уже в мониторинге / проверены: "
                f"<b>{len(streams) - len(to_check)}</b>\n"
                f"Новых для проверки: <b>{len(to_check)}</b>"
            )

            if not to_check:
                await _safe_edit(
                    status_msg,
                    "✅ <b>Поиск завершён.</b>\n"
                    f"Все {len(streams)} стримеров уже в мониторинге или\n"
                    f"проверялись за последние {CHECK_DAYS} дней."
                )
                return

            # ── Шаг 3: пробиваем панели каждого нового стримера ─
            for i, stream in enumerate(to_check, 1):
                login     = stream['user_login'].lower()
                user_id   = stream['user_id']
                viewers   = stream.get('viewer_count', 0)
                game_name = stream.get('game_name', '?')
                display   = stream.get('user_name', login)

                try:
                    # Обновляем прогресс каждые 3 стримера
                    if i % 3 == 1:
                        await _safe_edit(
                            status_msg,
                            "🔍 <b>Пробиваю панели...</b>\n"
                            f"Прогресс: <b>{i - 1}/{len(to_check)}</b>\n"
                            f"Добавлено: <b>{len(added)}</b>\n\n"
                            f"Сейчас: <code>{login}</code> ({viewers:,} зр.)"
                        )

                    found = False

                    # 1. Панели через GraphQL API
                    panels = await helix_get_channel_panels_gql(session, login)
                    for panel in panels:
                        if not panel: continue
                        link_text = panel.get('linkURL', '') or ''
                        desc_text = panel.get('description', '') or ''
                        title_text = panel.get('title', '') or ''
                        if (has_betboom_link(link_text)
                                or has_betboom_link(desc_text)
                                or has_betboom_link(title_text)):
                            found = True
                            logger.info(f"[Search] Панель BetBoom у {login}")
                            break

                    # 2. Описание канала из Helix (fallback)
                    if not found:
                        ch_info = await helix_get_channel_info(session, user_id)
                        desc = ch_info.get('description', '') or ''
                        if has_betboom_link(desc):
                            found = True
                            logger.info(f"[Search] BetBoom в описании канала {login}")

                    # 3. Заголовок стрима (последний шанс)
                    if not found:
                        title = stream.get('title', '') or ''
                        if has_betboom_link(title):
                            found = True
                            logger.info(f"[Search] BetBoom в заголовке стрима {login}")

                    # ── Результат проверки ────────────────────────
                    if found:
                        # Добавляем в мониторинг
                        if twitch_bot and login not in twitch_bot.channels_list:
                            twitch_bot.channels_list.append(login)
                            save_channels(twitch_bot.channels_list)
                            await twitch_bot.join_channels([login])

                        await record_checked_channel(login, 'ambassador')
                        added.append((login, viewers, game_name))
                        logger.info(f"[Search] 🔥 Новый амбассадор: {login} ({viewers} зр.)")

                        # Немедленное уведомление админам
                        for admin_id in ADMIN_IDS:
                            try:
                                await tg_bot.send_message(
                                    chat_id=admin_id,
                                    text=(
                                        f"🔥 <b>Найден новый амбассадор BetBoom!</b>\n\n"
                                        f"Канал: <b>{display}</b>\n"
                                        f"Игра: {game_name}\n"
                                        f"Зрителей: <b>{viewers:,}</b>\n\n"
                                        f"✅ Добавлен в мониторинг автоматически!\n"
                                        f"Ссылка: twitch.tv/{login}"
                                    )
                                )
                            except Exception as notify_err:
                                logger.error(f"Ошибка уведомления: {notify_err}")
                    else:
                        await record_checked_channel(login, 'empty')
                        logger.info(f"[Search] Пустышка: {login}")

                    checked += 1

                except asyncio.CancelledError:
                    raise  # пробрасываем для корректной отмены
                except Exception as err:
                    logger.error(f"[Search] Ошибка при проверке {login}: {err}")
                    checked += 1

                # Пауза между запросами (защита от rate limit)
                await asyncio.sleep(PANEL_DELAY)

        # ── Итоговое сообщение ────────────────────────────────────
        summary = (
            "✅ <b>Поиск амбассадоров завершён!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Проверено стримеров: <b>{checked}</b>\n"
            f"Добавлено в мониторинг: <b>{len(added)}</b>\n"
        )
        if added:
            summary += "\n🔥 <b>Новые амбассадоры:</b>\n"
            for a_login, a_viewers, a_game in added:
                summary += f"• <b>{a_login}</b> — {a_viewers:,} зр. ({a_game})\n"
            summary += "\nДля отмены используй /undosearch"
        else:
            summary += "\nЛиний с BetBoom не обнаружено."

        await _safe_edit(status_msg, summary)

    except asyncio.CancelledError:
        await _safe_edit(
            status_msg,
            "⛔ <b>Поиск отменён.</b>\n"
            f"Успел проверить: <b>{checked}</b> стримеров.\n"
            f"Добавлено: <b>{len(added)}</b> каналов."
        )
    except Exception as fatal_err:
        logger.error(f"[Search] Критическая ошибка: {fatal_err}")
        await _safe_edit(status_msg, f"❌ Критическая ошибка поиска:\n<code>{fatal_err}</code>")

# ══════════════════════════════════════════════════════════════════
#  ОБРАБОТЧИКИ КОМАНД И КНОПОК
#  Порядок важен: команды регистрируются ДО FSM-хэндлера,
#  чтобы /cancel и кнопки меню работали даже в середине диалога.
# ══════════════════════════════════════════════════════════════════

# ─── /start ──────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    is_admin = message.from_user.id in ADMIN_IDS
    kb = make_main_kb() if is_admin else ReplyKeyboardRemove()
    admin_hint = "\n\n<b>Панель управления доступна — используй кнопки ниже 👇</b>" if is_admin else ""
    await message.answer(
        f"Привет, {message.from_user.first_name}! 👋\n\n"
        "Я мониторю Twitch-чаты и нахожу ссылки на фрибеты BetBoom.\n"
        f"Твой User ID: <code>{message.from_user.id}</code>"
        + admin_hint,
        reply_markup=kb,
    )

# ─── /cancel и кнопка Отмена (для выхода из FSM) ────────────────
@dp.message(Command("cancel"))
@dp.message(F.text == "🔙 Отмена")
async def cmd_cancel(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Отменено.", reply_markup=make_main_kb())

# ─── /status и кнопка 📊 Статус ──────────────────────────────────
@dp.message(Command("status"))
@dp.message(F.text == "📊 Статус")
async def cmd_status(message: types.Message) -> None:
    if not await check_admin(message): return
    await message.answer(await _build_status_text(), reply_markup=make_refresh_kb("status"))

# ─── /count и кнопка 📈 Аналитика ────────────────────────────────
@dp.message(Command("count"))
@dp.message(F.text == "📈 Аналитика")
async def cmd_count(message: types.Message) -> None:
    if not await check_admin(message): return
    await message.answer(await _build_count_text(), reply_markup=make_refresh_kb("count"))

# ─── /channels и кнопка 📡 Каналы ────────────────────────────────
@dp.message(Command("channels"))
@dp.message(F.text == "📡 Каналы")
async def cmd_channels(message: types.Message) -> None:
    if not await check_admin(message): return
    if twitch_bot is None or not twitch_bot.channels_list:
        await message.answer("Список каналов пуст.")
        return
    lines = [f"• <code>{c}</code>" for c in twitch_bot.channels_list]
    await message.answer(
        f"📡 <b>Отслеживаемые каналы ({len(twitch_bot.channels_list)}):</b>\n"
        + "\n".join(lines)
    )

# ─── /ping и кнопка 🏓 Пинг ──────────────────────────────────────
@dp.message(Command("ping"))
@dp.message(F.text == "🏓 Пинг")
async def cmd_ping(message: types.Message) -> None:
    if not await check_admin(message): return

    tg_ok, tg_ms = True, -1
    try:
        t0 = time.monotonic()
        await tg_bot.get_me()
        tg_ms = round((time.monotonic() - t0) * 1000)
    except Exception as e:
        tg_ok = False
        logger.error(f"Ping Telegram error: {e}")

    tw_ok, tw_ms = True, -1
    try:
        t0 = time.monotonic()
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                "https://tmi.twitch.tv",
                proxy=TG_PROXY,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                tw_ms = round((time.monotonic() - t0) * 1000)
                if resp.status >= 500:
                    tw_ok = False
    except Exception as e:
        tw_ok = False
        logger.error(f"Ping Twitch error: {e}")

    all_ok = tg_ok and tw_ok
    tg_str = f"<b>{tg_ms} мс</b>" if tg_ok else "❌ недоступен"
    tw_str = f"<b>{tw_ms} мс</b>" if tw_ok else "❌ недоступен"
    verdict = "✅ Все системы работают штатно." if all_ok else "⚠️ Обнаружены проблемы!"

    b = InlineKeyboardBuilder()
    b.button(text="🔄 Проверить снова", callback_data="refresh:ping")

    await message.answer(
        f"{'🟢' if all_ok else '🔴'} <b>Диагностика систем</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Telegram API : {tg_str}\n"
        f"🎮 Twitch API   : {tw_str}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        + verdict,
        reply_markup=b.as_markup()
    )

# ─── /add и кнопка ➕ Добавить канал ─────────────────────────────
@dp.message(Command("add"))
async def cmd_add_direct(message: types.Message, state: FSMContext) -> None:
    if not await check_admin(message): return
    parts = message.text.split(maxsplit=1)
    if len(parts) >= 2:
        await _do_add(message, parts[1])
    else:
        await state.set_state(Form.add_channel)
        await message.answer(
            "Введи ник стримера или ссылку на канал:",
            reply_markup=make_cancel_kb()
        )

@dp.message(F.text == "➕ Добавить канал")
async def btn_add(message: types.Message, state: FSMContext) -> None:
    if not await check_admin(message): return
    await state.set_state(Form.add_channel)
    await message.answer(
        "Введи ник стримера или ссылку на канал:",
        reply_markup=make_cancel_kb()
    )

# ─── /remove и кнопка ➖ Удалить канал ───────────────────────────
@dp.message(Command("remove"))
@dp.message(F.text == "➖ Удалить канал")
async def cmd_remove(message: types.Message) -> None:
    if not await check_admin(message): return
    if twitch_bot is None or not twitch_bot.channels_list:
        await message.answer("Список каналов пуст — нечего удалять.")
        return
    await message.answer(
        "Выбери канал для удаления:",
        reply_markup=make_remove_kb(twitch_bot.channels_list)
    )

# ─── /search и кнопка 🔍 Поиск амбассадоров ─────────────────────
@dp.message(Command("search"))
@dp.message(F.text == "🔍 Поиск амбассадоров")
async def cmd_search(message: types.Message) -> None:
    global _search_task
    if not await check_admin(message): return

    if not TWITCH_CLIENT_ID:
        await message.answer(
            "⚠️ <b>Для поиска нужен Client ID.</b>\n\n"
            "Добавь в <code>config.json</code>:\n"
            "<code>\"twitch_client_id\": \"твой_client_id\"</code>\n\n"
            "Получить: https://dev.twitch.tv/console → Your Applications"
        )
        return

    if _search_task and not _search_task.done():
        b = InlineKeyboardBuilder()
        b.button(text="⛔ Остановить поиск", callback_data="search:stop")
        await message.answer(
            "🔍 Поиск уже выполняется!\nНажми кнопку чтобы остановить.",
            reply_markup=b.as_markup()
        )
        return

    status_msg = await message.answer(
        "🔍 <b>Инициализация поиска амбассадоров BetBoom...</b>\n"
        f"Категории: {', '.join(GAME_NAMES)}\n"
        f"Фильтр: 🇷🇺 русский язык, >{VIEWER_THRESHOLD} зрителей"
    )
    _search_task = asyncio.create_task(
        _do_search(status_msg),
        name="ambassador_search"
    )

# ─── /research ──────────────────────────────────────────────────
@dp.message(Command("research"))
async def cmd_research(message: types.Message) -> None:
    global _search_task
    if not await check_admin(message): return

    if not TWITCH_CLIENT_ID:
        await message.answer("⚠️ <b>Для поиска нужен Client ID.</b>")
        return

    if _search_task and not _search_task.done():
        b = InlineKeyboardBuilder()
        b.button(text="⛔ Остановить поиск", callback_data="search:stop")
        await message.answer(
            "🔍 Поиск уже выполняется!\nНажми кнопку чтобы остановить.",
            reply_markup=b.as_markup()
        )
        return

    status_msg = await message.answer(
        "🔍 <b>ПРИНУДИТЕЛЬНЫЙ поиск амбассадоров BetBoom...</b>\n"
        f"Игнорируем историю проверок за {CHECK_DAYS} дней.\n"
        f"Категории: {', '.join(GAME_NAMES)}\n"
    )
    _search_task = asyncio.create_task(
        _do_search(status_msg, force=True),
        name="ambassador_research"
    )

# ─── /undosearch и кнопка ↩️ Отменить поиск ─────────────────────
@dp.message(Command("undosearch"))
@dp.message(F.text == "↩️ Отменить поиск")
async def cmd_undosearch(message: types.Message) -> None:
    if not await check_admin(message): return

    added = await get_search_added_channels()
    current = set(twitch_bot.channels_list if twitch_bot else [])
    active = [ch for ch in added if ch in current]

    if not active:
        await message.answer(
            "Нет каналов, добавленных поиском.\n"
            "(Или они уже были удалены вручную.)"
        )
        return

    await message.answer(
        f"↩️ <b>Каналы найденные поиском ({len(active)}):</b>\n"
        + "\n".join(f"• <code>{ch}</code>" for ch in active)
        + "\n\nВыбери что удалить:",
        reply_markup=make_undo_kb(active)
    )

# ── Стоп поиска через inline-кнопку ─────────────────────────────
@dp.callback_query(F.data == "search:stop")
async def cb_stop_search(callback: types.CallbackQuery) -> None:
    global _search_task
    if not await check_admin_cb(callback): return
    if _search_task and not _search_task.done():
        _search_task.cancel()
        await callback.message.edit_text("⛔ Поиск остановлен.")
        await callback.answer("Поиск отменён")
    else:
        await callback.answer("Поиск уже завершён.", show_alert=True)

# ── Отмена каналов, добавленных поиском ─────────────────────────
@dp.callback_query(F.data.startswith("undo:"))
async def cb_undo(callback: types.CallbackQuery) -> None:
    if not await check_admin_cb(callback): return
    target = callback.data[5:]

    if target == "__close__":
        await callback.message.edit_text("Закрыто.")
        await callback.answer()
        return

    if target == "__all__":
        added = await get_search_added_channels()
        current = set(twitch_bot.channels_list if twitch_bot else [])
        to_remove = [ch for ch in added if ch in current]
        for ch in to_remove:
            if twitch_bot and ch in twitch_bot.channels_list:
                twitch_bot.channels_list.remove(ch)
                try:
                    await twitch_bot.part_channels([ch])
                except Exception:
                    pass
        if to_remove:
            save_channels(twitch_bot.channels_list if twitch_bot else [])
        await callback.message.edit_text(
            f"✅ Удалено каналов: <b>{len(to_remove)}</b>\n"
            + ", ".join(f"<code>{ch}</code>" for ch in to_remove)
        )
        await callback.answer(f"Удалено {len(to_remove)}")
        return

    # Удаление одного канала
    channel = target
    if twitch_bot and channel in twitch_bot.channels_list:
        twitch_bot.channels_list.remove(channel)
        save_channels(twitch_bot.channels_list)
        try:
            await twitch_bot.part_channels([channel])
        except Exception:
            pass
        await callback.message.edit_text(
            f"✅ Канал <b>{channel}</b> удалён из мониторинга."
        )
        await callback.answer(f"Удалён: {channel}")
    else:
        await callback.answer(f"Канала {channel} нет в списке.", show_alert=True)

# ══════════════════════════════════════════════════════════════════
#  FSM ОБРАБОТЧИК — ввод имени канала
#  Регистрируется ПОСЛЕ всех обычных хэндлеров!
# ══════════════════════════════════════════════════════════════════
@dp.message(Form.add_channel)
async def fsm_add_channel(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await _do_add(message, message.text or "")

# ══════════════════════════════════════════════════════════════════
#  CALLBACK QUERY — inline-кнопки (Обновить / Удалить)
# ══════════════════════════════════════════════════════════════════
@dp.callback_query(F.data == "refresh:status")
async def cb_refresh_status(callback: types.CallbackQuery) -> None:
    if not await check_admin_cb(callback): return
    try:
        await callback.message.edit_text(
            await _build_status_text(), reply_markup=make_refresh_kb("status")
        )
        await callback.answer("Обновлено ✅")
    except TelegramBadRequest:
        await callback.answer("Обновлено ✅")

@dp.callback_query(F.data == "refresh:count")
async def cb_refresh_count(callback: types.CallbackQuery) -> None:
    if not await check_admin_cb(callback): return
    try:
        await callback.message.edit_text(
            await _build_count_text(), reply_markup=make_refresh_kb("count")
        )
        await callback.answer("Обновлено ✅")
    except TelegramBadRequest:
        await callback.answer("Обновлено ✅")

@dp.callback_query(F.data == "refresh:ping")
async def cb_refresh_ping(callback: types.CallbackQuery) -> None:
    if not await check_admin_cb(callback): return
    await callback.answer("Проверяю...")
    await cmd_ping(callback.message)

@dp.callback_query(F.data.startswith("rm:"))
async def cb_remove_channel(callback: types.CallbackQuery) -> None:
    if not await check_admin_cb(callback): return
    channel = callback.data[3:]

    if channel == "__cancel__":
        await callback.message.edit_text("Удаление отменено.")
        await callback.answer()
        return

    if twitch_bot is None:
        await callback.answer("⚠️ Twitch-бот не запущен.", show_alert=True)
        return
    if channel not in twitch_bot.channels_list:
        await callback.answer(f"Канала {channel} уже нет в списке.", show_alert=True)
        await callback.message.edit_text("Список устарел. Запроси /channels.")
        return

    twitch_bot.channels_list.remove(channel)
    save_channels(twitch_bot.channels_list)
    await twitch_bot.part_channels([channel])
    await callback.message.edit_text(f"❌ Канал <b>{channel}</b> удалён!")
    await callback.answer(f"Канал {channel} удалён")
    logger.info(f"Канал [{channel}] удалён администратором.")

# ══════════════════════════════════════════════════════════════════
#  TWITCH BOT
# ══════════════════════════════════════════════════════════════════
class TwitchParser(commands.Bot):
    def __init__(self) -> None:
        self.channels_list = load_channels()
        self.sent_links: dict[str, float] = {}

        if not self.channels_list:
            logger.warning("Список каналов пуст. Добавьте через кнопку ➕.")
        if not TWITCH_TOKEN:
            logger.critical("Не указан twitch_token в config.json!")
            sys.exit(1)

        logger.info(
            "Инициализация TwitchParser. Каналы: "
            + (', '.join(self.channels_list) or 'нет')
        )
        super().__init__(
            token=TWITCH_TOKEN,
            prefix='?',
            initial_channels=self.channels_list or [],
        )

    async def event_ready(self) -> None:
        logger.info(
            f"Twitch: авторизован как [{self.nick}]. "
            f"Слежу за {len(self.channels_list)} каналами."
        )

    async def event_message(self, message) -> None:
        if message.echo:
            return

        match = re.search(BETBOOM_PATTERN, message.content, re.IGNORECASE)
        if not match:
            return

        link = match.group(1)
        channel_name = message.channel.name

        now = time.time()
        if link in self.sent_links and (now - self.sent_links[link]) < ANTISPAM_COOLDOWN:
            return
        self.sent_links[link] = now

        logger.info(f"[{channel_name}] Найдена ссылка: {link}")
        await record_link(channel_name, link)

        if TG_CHAT_ID:
            text = f"🚨 Фрибет у стримера [<b>{channel_name}</b>]:\n{link}"
            try:
                await tg_bot.send_message(
                    chat_id=TG_CHAT_ID,
                    text=text,
                    link_preview_options=types.LinkPreviewOptions(is_disabled=True),
                )
                logger.info(f"[{channel_name}] Уведомление отправлено.")
            except Exception as e:
                logger.error(f"Ошибка отправки в Telegram: {e}")

# ══════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════
async def send_admin_alert(text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await tg_bot.send_message(chat_id=admin_id, text=text)
        except Exception as e:
            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")

# ══════════════════════════════════════════════════════════════════
#  ГЛАВНАЯ ФУНКЦИЯ — Fault-Tolerant Loop
# ══════════════════════════════════════════════════════════════════
async def main() -> None:
    global twitch_bot, _search_task

    await init_db()

    logger.info("Запуск Telegram polling...")
    tg_task = asyncio.create_task(
        dp.start_polling(tg_bot, handle_signals=False),
        name="telegram_polling",
    )

    def _on_tg_done(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception():
            logger.error(f"Telegram polling упал: {task.exception()}")

    tg_task.add_done_callback(_on_tg_done)

    fast_failures = 0
    logger.info("Запуск основного цикла мониторинга Twitch...")

    while True:
        try:
            logger.info("Создаём новый экземпляр TwitchParser...")
            twitch_bot = TwitchParser()
            await twitch_bot.start()
            logger.info("Twitch-бот завершил работу штатно.")
            break

        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("Получен сигнал остановки.")
            break

        except Exception as e:
            fast_failures += 1
            logger.error(
                f"Сбой Twitch #{fast_failures}/{MAX_FAST_RETRIES}: "
                f"{type(e).__name__}: {e}"
            )

            if twitch_bot is not None:
                try:
                    await twitch_bot.close()
                except Exception:
                    pass
                twitch_bot = None

            if fast_failures <= MAX_FAST_RETRIES:
                logger.info(
                    f"Быстрый перезапуск через {FAST_RETRY_DELAY}с "
                    f"(попытка {fast_failures}/{MAX_FAST_RETRIES})..."
                )
                await asyncio.sleep(FAST_RETRY_DELAY)
            else:
                logger.warning("Превышен лимит быстрых попыток.")
                await send_admin_alert(
                    "⚠️ <b>Проблемы со связью!</b>\n"
                    f"Сбоев подряд: {fast_failures}\n"
                    f"Ошибка: <code>{type(e).__name__}: {e}</code>\n\n"
                    "Ухожу в режим ожидания на 10 минут..."
                )
                fast_failures = 0
                await asyncio.sleep(LONG_RETRY_DELAY)
                logger.info("Возобновляю работу после паузы.")

    # Финальная очистка
    if _search_task and not _search_task.done():
        _search_task.cancel()
    if not tg_task.done():
        tg_task.cancel()
        try:
            await tg_task
        except asyncio.CancelledError:
            pass
    logger.info("Бот полностью остановлен.")

# ══════════════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановка по Ctrl+C.")
