import asyncio
import logging
import json
import sqlite3
import random
import os
import base64
from datetime import datetime, timezone, timedelta
from pathlib import Path
from telethon import TelegramClient, events
from telethon.tl.types import Message
from telethon.errors import FloodWaitError, ChatWriteForbiddenError


API_ID   = int(os.getenv("API_ID", "12345678"))
API_HASH = os.getenv("API_HASH", "your_api_hash")
SESSION_NAME = "parser"

# Чаты для мониторинга
CHATS_TO_MONITOR = [
    -1003728873984,
    -1001240936626,
    -1003895398768,
    -1002299792440,
    -1003409179740,
    -1003403934736,
    -1003336409274,
    -1003898518501,
    -1003336180098,
    -1003216812306,
    -1003532237255,
]

# Ключевые слова
KEYWORDS = [
    # Боты
    "нужен бот",
    "сделайте бота",
    "хочу бота",
    "ищу бота",
    "заказать бота",
    "разработка бота",
    "телеграм бот",
    "tg бот",
    "бот под",
    "бот для",
    "нужен телеграм",
    # Сайты
    "нужен сайт",
    "сделайте сайт",
    "хочу сайт",
    "заказать сайт",
    "разработка сайта",
    "лендинг",
    "визитка сайт",
    "интернет магазин",
    "нужен лендинг",
    "сделать сайт",
    # Поиск разработчика
    "ищу разработчика",
    "нужен программист",
    "ищу программиста",
    "кто делает сайты",
    "кто делает ботов",
    "посоветуйте разработчика",
    "нужна автоматизация",
    "фриланс разработка",
]

# Куда пересылать
FORWARD_TO_CHAT = "me"

# За сколько часов парсить историю при старте
HISTORY_HOURS = 3

# Файл кэша
CACHE_FILE = "chats_cache.json"

# Задержки (секунды)
DELAY_BETWEEN_FORWARDS = (8, 15)
DELAY_BETWEEN_CHATS    = (3, 7)
DELAY_BETWEEN_MESSAGES = (0.4, 1.0)
# ====================================================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("parser.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ==================== БАЗА ДАННЫХ ====================

_db_conn: sqlite3.Connection | None = None


def get_db() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect("messages.db", check_same_thread=False)
        _db_conn.execute("PRAGMA journal_mode=WAL")
        _db_conn.row_factory = sqlite3.Row
    return _db_conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id     INTEGER,
            chat_id        INTEGER,
            chat_title     TEXT,
            sender_id      INTEGER,
            sender_name    TEXT,
            text           TEXT,
            date           TEXT,
            keywords_found TEXT,
            forwarded      INTEGER DEFAULT 0,
            UNIQUE(message_id, chat_id)
        )
    """)
    conn.commit()
    log.info("✅ БД инициализирована")


def is_already_saved(message_id: int, chat_id: int) -> bool:
    row = get_db().execute(
        "SELECT id FROM messages WHERE message_id=? AND chat_id=?",
        (message_id, chat_id)
    ).fetchone()
    return row is not None


def save_message(msg_data: dict) -> bool:
    if is_already_saved(msg_data["message_id"], msg_data["chat_id"]):
        return False
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO messages
            (message_id, chat_id, chat_title, sender_id, sender_name,
             text, date, keywords_found, forwarded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            msg_data["message_id"],
            msg_data["chat_id"],
            msg_data["chat_title"],
            msg_data["sender_id"],
            msg_data["sender_name"],
            msg_data["text"],
            msg_data["date"],
            json.dumps(msg_data["keywords_found"], ensure_ascii=False),
            msg_data["forwarded"],
        ))
        conn.commit()
        return True
    except Exception as e:
        log.warning(f"Ошибка сохранения в БД: {e}")
        return False


def mark_forwarded(message_id: int, chat_id: int):
    conn = get_db()
    conn.execute(
        "UPDATE messages SET forwarded=1 WHERE message_id=? AND chat_id=?",
        (message_id, chat_id)
    )
    conn.commit()


# ==================== КЭШ ЧАТОВ ====================

def load_cache() -> dict:
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                log.info(f"📦 Кэш загружен: {len(data)} чатов")
                return data
        except Exception:
            pass
    return {}


def save_cache(cache: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ==================== РЕЗОЛВ ЧАТОВ ====================

async def resolve_chat(client, chat, cache):
    if str(chat) in cache:
        try:
            entity = await client.get_entity(cache[str(chat)])
            log.info(f"⚡ Из кэша: {getattr(entity, 'title', chat)}")
            return entity
        except Exception:
            pass
    try:
        entity = await client.get_entity(chat)
        cache[str(chat)] = entity.id
        log.info(f"✅ Подключён: {getattr(entity, 'title', chat)}")
        return entity
    except Exception as e:
        log.error(f"❌ Не удалось подключиться к {chat}: {e}")
        return None


async def resolve_all_chats(client) -> list:
    cache = load_cache()
    log.info(f"🔄 Резолвим {len(CHATS_TO_MONITOR)} чатов...")
    entities = []
    for chat in CHATS_TO_MONITOR:
        entity = await resolve_chat(client, chat, cache)
        if entity:
            entities.append(entity)
        await asyncio.sleep(random.uniform(0.8, 2.0))
    save_cache(cache)
    log.info(f"✅ Подключено {len(entities)}/{len(CHATS_TO_MONITOR)} чатов")
    return entities


# ==================== ОТПРАВКА ====================

async def safe_send(client, text: str, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            await client.send_message(FORWARD_TO_CHAT, text, parse_mode="html")
            return True
        except FloodWaitError as e:
            wait = e.seconds + random.randint(3, 10)
            log.warning(f"⏳ FloodWait — жду {wait}с...")
            await asyncio.sleep(wait)
        except ChatWriteForbiddenError:
            log.error("❌ Нет прав на запись в целевой чат")
            return False
        except Exception as e:
            log.error(f"Ошибка отправки (попытка {attempt + 1}/{retries}): {e}")
            await asyncio.sleep(2 ** attempt)
    log.error("❌ Все попытки отправки исчерпаны")
    return False


# ==================== ОБРАБОТКА СООБЩЕНИЙ ====================

def find_keywords(text: str) -> list:
    if not text:
        return []
    text_lower = text.lower()
    return [kw for kw in KEYWORDS if kw.lower() in text_lower]


async def process_message(client, message: Message, chat_title: str, source: str = "live"):
    text = message.text or ""
    found_keywords = find_keywords(text)
    if not found_keywords:
        return

    try:
        sender = await message.get_sender()
        sender_name = (
            getattr(sender, "first_name", None)
            or getattr(sender, "title", None)
            or "Unknown"
        )
        sender_id = sender.id if sender else 0
    except Exception:
        sender_name = "Unknown"
        sender_id = 0

    try:
        chat = await message.get_chat()
        chat_id = chat.id
    except Exception:
        chat_id = 0

    msg_data = {
        "message_id":     message.id,
        "chat_id":        chat_id,
        "chat_title":     chat_title,
        "sender_id":      sender_id,
        "sender_name":    sender_name,
        "text":           text,
        "date":           str(message.date),
        "keywords_found": found_keywords,
        "forwarded":      0,
    }

    is_new = save_message(msg_data)
    if not is_new:
        return

    icon = "🕐" if source == "history" else "🔴"
    log.info(f"{icon} [{chat_title}] {sender_name}: {found_keywords}")

    keywords_str = ", ".join(found_keywords)
    label = "📜 Из истории" if source == "history" else "🔴 Новое сообщение"
    forward_text = (
        f"🎯 <b>Потенциальный клиент!</b> {label}\n\n"
        f"📨 Чат: <b>{chat_title}</b>\n"
        f"👤 Автор: {sender_name}\n"
        f"🔑 Ключевые слова: {keywords_str}\n"
        f"📅 {message.date.strftime('%d.%m.%Y %H:%M')}\n\n"
        f"💬 <i>{text[:1000]}</i>"
    )

    await asyncio.sleep(random.uniform(*DELAY_BETWEEN_FORWARDS))

    sent = await safe_send(client, forward_text)
    if sent:
        mark_forwarded(message.id, chat_id)


# ==================== ПАРСИНГ ИСТОРИИ ====================

async def parse_history(client, entities: list):
    since = datetime.now(timezone.utc) - timedelta(hours=HISTORY_HOURS)
    log.info(f"📜 Парсим историю с {since.strftime('%d.%m.%Y %H:%M')} UTC...")

    total_checked = 0

    for entity in entities:
        chat_title = getattr(entity, "title", getattr(entity, "first_name", "Unknown"))
        count = 0

        try:
            async for message in client.iter_messages(entity, reverse=False):
                msg_date = message.date
                if msg_date.tzinfo is None:
                    msg_date = msg_date.replace(tzinfo=timezone.utc)

                if msg_date < since:
                    break

                count += 1
                if message.text:
                    await process_message(client, message, chat_title, source="history")

                await asyncio.sleep(random.uniform(*DELAY_BETWEEN_MESSAGES))

        except FloodWaitError as e:
            wait = e.seconds + random.randint(5, 15)
            log.warning(f"⏳ FloodWait в [{chat_title}] — жду {wait}с...")
            await asyncio.sleep(wait)
        except Exception as e:
            log.error(f"Ошибка при парсинге истории [{chat_title}]: {e}")

        log.info(f"📜 [{chat_title}] — проверено {count} сообщений")
        total_checked += count

        await asyncio.sleep(random.uniform(*DELAY_BETWEEN_CHATS))

    log.info(f"✅ История готова — проверено {total_checked} сообщений")


# ==================== MAIN ====================

async def main():
    init_db()

    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        request_retries=5,
        connection_retries=5,
        retry_delay=2,
        flood_sleep_threshold=60,
    )

    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Сессия не авторизована! Проверь переменную SESSION_BASE64")
    log.info("🚀 Клиент запущен")

    chat_entities = await resolve_all_chats(client)

    if not chat_entities:
        log.error("❌ Не удалось подключиться ни к одному чату!")
        return

    await parse_history(client, chat_entities)

    log.info("👂 Переключаюсь в режим реального времени...")

    @client.on(events.NewMessage(chats=chat_entities))
    async def handler(event: events.NewMessage.Event):
        message: Message = event.message
        try:
            chat = await message.get_chat()
            chat_title = getattr(chat, "title", getattr(chat, "first_name", "Unknown"))
            await process_message(client, message, chat_title, source="live")
        except Exception as e:
            log.error(f"Ошибка в обработчике живых сообщений: {e}")

    log.info("✅ Парсер полностью запущен — слежу за новыми сообщениями")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
