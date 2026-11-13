import asyncio
import html
import logging
import os
import re
import sqlite3
import threading
import time
from urllib.parse import quote
from datetime import datetime, timedelta, timezone

WIB = timezone(timedelta(hours=7))

from aiohttp import web

# Telethon: monitor public source channel using the user's Telegram account
from telethon import TelegramClient, events, utils
from telethon.sessions import StringSession

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

CITY_NAME = os.getenv("CITY_NAME", "SURABAYA").strip().upper()
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "").strip()

# Force Subscribe
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL", CHANNEL_USERNAME).strip()
# Force join hanya channel; grup komunitas tidak diwajibkan.

TRAKTEER_USERNAME = os.getenv("TRAKTEER_USERNAME", "").strip()

TRAKTEER_WEBHOOK_TOKEN = os.getenv("TRAKTEER_WEBHOOK_TOKEN", "").strip()

WEBHOOK_PORT = int(os.getenv("PORT", "8080"))

DB_FILE = os.getenv("DB_FILE", "/data/fwb_surabaya.db")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
REGISTRATION_PRICE = int(os.getenv("REGISTRATION_PRICE", "10000"))
REGISTRATION_BONUS_COINS = int(os.getenv("REGISTRATION_BONUS_COINS", "300"))
REFERRAL_BONUS_COINS = int(os.getenv("REFERRAL_BONUS_COINS", "100"))
DAILY_REWARD_DEFAULT = int(os.getenv("DAILY_REWARD", "10"))

# Telethon auto-copy configuration
API_ID = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
TELETHON_SESSION = os.getenv("TELETHON_SESSION", "").strip()
SOURCE_CHANNEL = os.getenv("SOURCE_CHANNEL", "fwbess_id").strip()
DEST_CHANNEL = os.getenv("DEST_CHANNEL", CHANNEL_USERNAME).strip()
FORCE_SUB_LINK = os.getenv("FORCE_SUB_LINK", "").strip()
if CHANNEL_USERNAME and not CHANNEL_USERNAME.startswith("@") and not CHANNEL_USERNAME.startswith("-"):
    CHANNEL_USERNAME = "@" + CHANNEL_USERNAME
if FORCE_SUB_CHANNEL and not FORCE_SUB_CHANNEL.startswith("@") and not FORCE_SUB_CHANNEL.startswith("-"):
    FORCE_SUB_CHANNEL = "@" + FORCE_SUB_CHANNEL
if DEST_CHANNEL and not DEST_CHANNEL.startswith("@") and not DEST_CHANNEL.startswith("-"):
    DEST_CHANNEL = "@" + DEST_CHANNEL
AUTO_COPY_HASHTAG = "#fwbgirl"
# Rp10 = 1 coin
RUPIAH_PER_COIN = 10

# Harga 1 menfess setelah free habis
MENFESS_COST = 100

# Free menfess per hari
FREE_MENFESS = 1

# Zona waktu Indonesia Barat
datetime.now(WIB)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger("FWB-SURABAYA")


# ============================================================
# GLOBAL
# ============================================================

BOT_APP = None
BOT_USERNAME = ""

# Event loop milik Telegram
BOT_LOOP = None

# Lock database
db_lock = threading.Lock()

# Force-sub performance cache. Menu biasa memakai cache singkat;
# tombol "CEK LAGI" selalu meminta pengecekan terbaru.
FORCE_SUB_CACHE_TTL = 20.0
force_sub_cache = {}
force_sub_inflight = {}


# ============================================================
# TIME
# ============================================================

def now_wib():
    return datetime.now(WIB)


def today_key():
    return now_wib().strftime("%Y-%m-%d")


def now_text():
    return now_wib().strftime("%Y-%m-%d %H:%M:%S")


def reset_time_text():
    return "00:01 WIB"


# ============================================================
# DATABASE
# ============================================================

def get_db():
    # Buat folder database bila DB_FILE memakai path volume Railway.
    db_parent = os.path.dirname(os.path.abspath(DB_FILE))
    if db_parent:
        os.makedirs(db_parent, exist_ok=True)

    conn = sqlite3.connect(
        DB_FILE,
        check_same_thread=False,
        timeout=30
    )

    conn.row_factory = sqlite3.Row

    return conn


def init_db():

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        # ----------------------------------------------------
        # USERS
        # ----------------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT DEFAULT '',
                username TEXT DEFAULT '',
                balance INTEGER DEFAULT 0,
                total_menfess INTEGER DEFAULT 0,
                free_used INTEGER DEFAULT 0,
                joined_at TEXT,
                referral_code TEXT,
                referred_by INTEGER DEFAULT NULL,
                is_banned INTEGER DEFAULT 0,
                free_date TEXT DEFAULT '',
                is_registered INTEGER DEFAULT 0,
                registered_at TEXT DEFAULT NULL
            )
        """)

        # Kolom status registrasi untuk database yang sudah ada.
        cur.execute("PRAGMA table_info(users)")
        user_columns = {row[1] for row in cur.fetchall()}
        if "is_registered" not in user_columns:
            cur.execute("ALTER TABLE users ADD COLUMN is_registered INTEGER DEFAULT 0")
        if "registered_at" not in user_columns:
            cur.execute("ALTER TABLE users ADD COLUMN registered_at TEXT DEFAULT NULL")

        # ----------------------------------------------------
        # TRANSACTIONS
        # ----------------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                user_id INTEGER,
                amount_rupiah INTEGER,
                coins INTEGER,
                created_at TEXT
            )
        """)

        # ----------------------------------------------------
        # MENFESS
        # ----------------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS menfess (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                gender TEXT,
                message_id INTEGER,
                text TEXT,
                created_at TEXT
            )
        """)

    # ========================================================
    # MIGRASI DATABASE LAMA
    # ========================================================

    cur.execute("PRAGMA table_info(users)")
    columns = [row[1] for row in cur.fetchall()]

    if "free_reset_date" not in columns:
        cur.execute("""
            ALTER TABLE users
            ADD COLUMN free_reset_date TEXT DEFAULT NULL
        """)

        logger.info(
            "Kolom free_reset_date berhasil ditambahkan."
        )

    # Surabaya feature tables. Semua dibuat tanpa menghapus data lama.
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY, added_at TEXT NOT NULL, added_by INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hashtags (
            tag TEXT PRIMARY KEY, category TEXT DEFAULT '', enabled INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS daily_rewards (
            user_id INTEGER NOT NULL, reward_date TEXT NOT NULL, coins INTEGER NOT NULL,
            PRIMARY KEY(user_id, reward_date)
        );
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            admin_id INTEGER, message TEXT NOT NULL, status TEXT DEFAULT 'open',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS admin_promotions (
            admin_id INTEGER PRIMARY KEY, code TEXT UNIQUE NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS promotion_attributions (
            user_id INTEGER PRIMARY KEY, admin_id INTEGER NOT NULL, created_at TEXT NOT NULL,
            registered INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS autocopy_sources (
            source TEXT PRIMARY KEY, destination TEXT NOT NULL, filter_mode TEXT DEFAULT 'all',
            filter_terms TEXT DEFAULT '', enabled INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS autocopy_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT DEFAULT '*', destination TEXT DEFAULT '*',
            template TEXT NOT NULL, position TEXT DEFAULT 'before', enabled INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS autocopy_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, source_message_id INTEGER,
            destination TEXT, status TEXT, detail TEXT, created_at TEXT,
            UNIQUE(source, source_message_id, destination)
        );
        CREATE TABLE IF NOT EXISTS scheduled_announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT, creator_id INTEGER NOT NULL,
            publish_at TEXT NOT NULL, message TEXT NOT NULL, status TEXT DEFAULT 'pending',
            created_at TEXT NOT NULL
        );
    """)
    cur.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cur.fetchall()}
    if "trial_used" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN trial_used INTEGER DEFAULT 0")
    if "daily_streak" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN daily_streak INTEGER DEFAULT 0")
    if "last_reward_date" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_reward_date TEXT DEFAULT NULL")
    cur.execute("INSERT OR IGNORE INTO hashtags(tag, category, enabled) VALUES('#fwbgirl','girl',1)")
    cur.execute("INSERT OR IGNORE INTO hashtags(tag, category, enabled) VALUES('#fwbboy','boy',1)")
    cur.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('daily_reward',?)", (str(DAILY_REWARD_DEFAULT),))
    conn.commit()
    conn.close()

    logger.info("SQLite database siap: %s", DB_FILE)


# ============================================================
# USER
# ============================================================

def create_user(user):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT user_id
            FROM users
            WHERE user_id = ?
            """,
            (user.id,)
        )

        exists = cur.fetchone()

        if not exists:

            cur.execute(
                """
                INSERT INTO users (
                    user_id,
                    first_name,
                    username,
                    balance,
                    total_menfess,
                    free_used,
                    joined_at,
                    referral_code,
                    referred_by,
                    is_banned,
                    free_date
                )
                VALUES (?, ?, ?, 0, 0, 0, ?, ?, NULL, 0, ?)
                """,
                (
                    user.id,
                    user.first_name or "",
                    user.username or "",
                    now_text(),
                    f"fwb{user.id}",
                    today_key()
                )
            )

        else:

            cur.execute(
                """
                UPDATE users
                SET first_name = ?,
                    username = ?
                WHERE user_id = ?
                """,
                (
                    user.first_name or "",
                    user.username or "",
                    user.id
                )
            )

        conn.commit()
        conn.close()


def get_user(user_id):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT *
            FROM users
            WHERE user_id = ?
            """,
            (user_id,)
        )

        row = cur.fetchone()

        conn.close()

        return row


# ============================================================
# OWNER / ADMIN / REWARD / REFERRAL HELPERS
# ============================================================

def get_setting(key, default=""):
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("SELECT value FROM app_settings WHERE key=?",(key,)); row=cur.fetchone(); conn.close()
    return row["value"] if row else default


def set_setting(key, value):
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("INSERT INTO app_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,value)); conn.commit(); conn.close()


def is_owner(user_id):
    return bool(OWNER_ID and int(user_id) == OWNER_ID)


def is_admin(user_id):
    if is_owner(user_id):
        return True
    with db_lock:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT 1 FROM admins WHERE user_id=?", (int(user_id),))
        admin_row = cur.fetchone()
        cur.execute("SELECT is_banned FROM users WHERE user_id=?", (int(user_id),))
        user_row = cur.fetchone()
        conn.close()
    return bool(admin_row and not (user_row and user_row["is_banned"]))


def add_admin(admin_id, added_by):
    if not OWNER_ID or int(added_by) != OWNER_ID or int(admin_id) == OWNER_ID:
        return False
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("INSERT OR IGNORE INTO admins(user_id,added_at,added_by) VALUES(?,?,?)", (int(admin_id),now_text(),int(added_by)))
        changed=cur.rowcount > 0
        conn.commit(); conn.close()
    return changed


def remove_admin(admin_id, removed_by):
    if not OWNER_ID or int(removed_by) != OWNER_ID or int(admin_id) == OWNER_ID:
        return False
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("DELETE FROM admins WHERE user_id=?", (int(admin_id),))
        changed=cur.rowcount > 0
        conn.commit(); conn.close()
    return changed


def set_user_ban(target_id, banned=True):
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("UPDATE users SET is_banned=? WHERE user_id=?", (1 if banned else 0, int(target_id)))
        changed=cur.rowcount > 0
        conn.commit(); conn.close()
    return changed


def get_daily_reward_amount():
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key='daily_reward'")
        row=cur.fetchone(); conn.close()
    try: return max(0, int(row['value'])) if row else DAILY_REWARD_DEFAULT
    except Exception: return DAILY_REWARD_DEFAULT


def set_daily_reward_amount(amount):
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("INSERT INTO app_settings(key,value) VALUES('daily_reward',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(max(0,int(amount))),))
        conn.commit(); conn.close()


def claim_daily_reward(user_id):
    if not is_user_registered(user_id):
        return False, 0
    today=today_key(); amount=get_daily_reward_amount()
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("INSERT OR IGNORE INTO daily_rewards(user_id,reward_date,coins) VALUES(?,?,?)", (int(user_id),today,amount))
        if cur.rowcount:
            cur.execute("UPDATE users SET balance=balance+?, daily_streak=CASE WHEN last_reward_date=? THEN daily_streak+1 ELSE 1 END, last_reward_date=? WHERE user_id=?", (amount, (now_wib()-timedelta(days=1)).strftime('%Y-%m-%d'), today, int(user_id)))
            ok=True
        else: ok=False
        conn.commit(); conn.close()
    return ok, amount


def attribute_referral(user_id, referrer_id):
    if not referrer_id or int(user_id)==int(referrer_id): return False
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("SELECT referred_by FROM users WHERE user_id=?", (int(user_id),)); row=cur.fetchone()
        if not row or row['referred_by'] is not None:
            conn.close(); return False
        cur.execute("SELECT user_id FROM users WHERE user_id=?", (int(referrer_id),)); ref=cur.fetchone()
        if not ref:
            conn.close(); return False
        cur.execute("UPDATE users SET referred_by=? WHERE user_id=? AND referred_by IS NULL", (int(referrer_id),int(user_id)))
        ok=cur.rowcount>0; conn.commit(); conn.close()
    return ok


def pay_referral_bonus(referred_user_id):
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("SELECT referred_by FROM users WHERE user_id=?", (int(referred_user_id),)); row=cur.fetchone()
        if not row or not row['referred_by']:
            conn.close(); return None
        referrer=int(row['referred_by'])
        cur.execute("CREATE TABLE IF NOT EXISTS referral_rewards(referred_user_id INTEGER PRIMARY KEY, referrer_id INTEGER, coins INTEGER, created_at TEXT)")
        cur.execute("INSERT OR IGNORE INTO referral_rewards VALUES(?,?,?,?)", (int(referred_user_id),referrer,REFERRAL_BONUS_COINS,now_text()))
        if cur.rowcount:
            cur.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (REFERRAL_BONUS_COINS,referrer))
            conn.commit(); conn.close(); return referrer
        conn.close(); return None


def user_is_staff(user_id):
    return is_admin(user_id)


# ============================================================
# REGISTRATION
# ============================================================
def is_user_registered(user_id):
    row = get_user(user_id)
    return bool(row and row["is_registered"])


def registration_keyboard(user_id):
    supporter_message = quote(f"fwbreg {user_id}")
    url = (
        f"https://trakteer.id/{TRAKTEER_USERNAME}/tip"
        f"?quantity=10&step=2&supporter_message={supporter_message}"
    )
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 BAYAR REGISTRASI Rp10.000", url=url)],
        [InlineKeyboardButton("🔄 Cek status registrasi", callback_data="check_registration")]
    ])


async def send_registration_prompt(target, user):
    await target.reply_text(
        "💳 <b>MAU LANJUT KIRIM MENFESS?</b>\n\n"
        f"Registrasi hanya <b>Rp{REGISTRATION_PRICE:,}</b> dan kamu langsung mendapat "
        f"bonus <b>{REGISTRATION_BONUS_COINS} coin</b>.\n\n"
        "🎁 Setelah terdaftar:\n"
        "• 1x menfess gratis setiap hari.\n"
        "• Bisa kirim menfess tambahan menggunakan coin.\n\n"
        "📝 <b>CARA REGISTRASI</b>\n"
        "1. Tekan tombol BAYAR REGISTRASI.\n"
        "2. Ikuti instruksi pembayaran di Trakteer.\n"
        "3. Kode <code>fwbreg ID_TELEGRAM</code> sudah otomatis terisi di pesan pembayaran, jadi kamu tidak perlu mengetik ID sendiri.\n"
        "4. Selesaikan pembayaran, lalu tekan Cek status registrasi jika akun belum otomatis aktif.\n\n"
        "Setelah pembayaran terverifikasi, akun aktif otomatis dan bonus coin masuk. ❤️",
        parse_mode=ParseMode.HTML,
        reply_markup=registration_keyboard(user.id)
    )


def mark_user_registered(user_id):
    with db_lock:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET is_registered=1, registered_at=? WHERE user_id=? AND is_registered=0",
            (now_text(), user_id)
        )
        changed = cur.rowcount > 0
        if changed:
            cur.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (REGISTRATION_BONUS_COINS, user_id))
        conn.commit()
        conn.close()
        return changed


# ============================================================
# FORCE SUBSCRIBE
# ============================================================

async def check_membership(bot, user_id, chat_id):
    """Return True when the user is a member/admin/creator of chat_id."""
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.warning(
            "Force sub check gagal | chat=%s | user=%s | error=%s",
            chat_id, user_id, e
        )
        return False


async def _perform_force_sub_check(bot, user_id):
    """Force join hanya mewajibkan channel FWB Surabaya."""
    result = await check_membership(bot, user_id, FORCE_SUB_CHANNEL)
    force_sub_cache[user_id] = (time.monotonic(), bool(result))
    return bool(result)


async def check_force_sub(bot, user_id, force_refresh=False):
    """
    Cek membership channel + grup.

    Menu biasa menggunakan cache 20 detik. Beberapa update untuk user
    yang sama pada saat bersamaan akan berbagi satu pengecekan Telegram.
    """
    now = time.monotonic()

    if not force_refresh:
        cached = force_sub_cache.get(user_id)
        if cached:
            cached_at, cached_result = cached
            if now - cached_at < FORCE_SUB_CACHE_TTL:
                return cached_result

    task = force_sub_inflight.get(user_id)

    if task is None or task.done():
        task = asyncio.create_task(
            _perform_force_sub_check(bot, user_id)
        )
        force_sub_inflight[user_id] = task

    try:
        return await asyncio.shield(task)
    finally:
        if task.done() and force_sub_inflight.get(user_id) is task:
            force_sub_inflight.pop(user_id, None)


def force_sub_keyboard():
    channel = FORCE_SUB_CHANNEL.lstrip("@")
    join_url = FORCE_SUB_LINK or (f"https://t.me/{channel}" if not channel.startswith("-100") else "https://t.me/")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 JOIN CHANNEL", url=join_url)],
        [InlineKeyboardButton("✅ SUDAH JOIN / CEK LAGI", callback_data="check_force_sub")]
    ])


def force_sub_text():
    return (f"🔒 <b>AKSES TERKUNCI</b>\n\n"
            f"Sebelum menggunakan <b>FWB {html.escape(CITY_NAME)}</b>, "
            "kamu wajib bergabung ke channel kami.\n\n"
            "Setelah join, tekan <b>SUDAH JOIN / CEK LAGI</b>.")


async def force_sub_guard(update, context, force_refresh=False):
    """Gate terpusat: False berarti akses ditahan."""
    user = update.effective_user
    if not user:
        return False

    if await check_force_sub(context.bot, user.id, force_refresh=force_refresh):
        return True

    if update.callback_query:
        await update.callback_query.answer(
            "❌ Kamu belum join channel.",
            show_alert=True
        )
        # Pesan force-sub yang sama tidak perlu diedit ulang.
        # Ini mencegah 400 Bad Request saat isi pesan tidak berubah.
    elif update.message:
        await update.message.reply_text(
            force_sub_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=force_sub_keyboard()
        )

    return False


# ============================================================
# DAILY FREE MENFESS
# ============================================================

def reset_daily_free_if_needed(user_id):
    conn = get_db()
    cur = conn.cursor()

    today = today_key()

    cur.execute("""
        SELECT free_reset_date
        FROM users
        WHERE user_id = ?
    """, (user_id,))

    row = cur.fetchone()

    if row:
        last_reset = row["free_reset_date"]

        if last_reset != today:
            cur.execute("""
                UPDATE users
                SET free_used = 0,
                    free_reset_date = ?
                WHERE user_id = ?
            """, (today, user_id))

    conn.commit()
    conn.close()


def get_free_remaining(user_id):

    reset_daily_free_if_needed(user_id)

    row = get_user(user_id)

    if not row:
        return FREE_MENFESS

    remaining = FREE_MENFESS - row["free_used"]

    return max(0, remaining)


def use_free_menfess(user_id):

    reset_daily_free_if_needed(user_id)

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE users
            SET free_used = free_used + 1
            WHERE user_id = ?
            AND free_used < ?
            """,
            (
                user_id,
                FREE_MENFESS
            )
        )

        changed = cur.rowcount

        conn.commit()
        conn.close()

        return changed > 0


def refund_free_menfess(user_id):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE users
            SET free_used =
                CASE
                    WHEN free_used > 0
                    THEN free_used - 1
                    ELSE 0
                END
            WHERE user_id = ?
            """,
            (user_id,)
        )

        conn.commit()
        conn.close()


# ============================================================
# BALANCE
# ============================================================

def add_balance(user_id, coins):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE users
            SET balance = balance + ?
            WHERE user_id = ?
            """,
            (
                coins,
                user_id
            )
        )

        conn.commit()
        conn.close()


def subtract_balance(user_id, coins):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE users
            SET balance = balance - ?
            WHERE user_id = ?
            AND balance >= ?
            """,
            (
                coins,
                user_id,
                coins
            )
        )

        changed = cur.rowcount

        conn.commit()
        conn.close()

        return changed > 0


# ============================================================
# MENFESS DATABASE
# ============================================================

def increase_menfess(user_id):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE users
            SET total_menfess = total_menfess + 1
            WHERE user_id = ?
            """,
            (user_id,)
        )

        conn.commit()
        conn.close()


def save_menfess(
    user_id,
    gender,
    message_id,
    text
):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO menfess (
                user_id,
                gender,
                message_id,
                text,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                user_id,
                gender,
                message_id,
                text,
                now_text()
            )
        )

        conn.commit()
        conn.close()


# ============================================================
# TRANSACTIONS
# ============================================================

def transaction_exists(transaction_id):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT transaction_id
            FROM transactions
            WHERE transaction_id = ?
            """,
            (transaction_id,)
        )

        exists = cur.fetchone()

        conn.close()

        return exists is not None


def save_transaction(
    transaction_id,
    user_id,
    amount_rupiah,
    coins
):

    with db_lock:

        conn = get_db()
        cur = conn.cursor()

        try:

            cur.execute(
                """
                INSERT INTO transactions (
                    transaction_id,
                    user_id,
                    amount_rupiah,
                    coins,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    user_id,
                    amount_rupiah,
                    coins,
                    now_text()
                )
            )

            conn.commit()

            return True

        except sqlite3.IntegrityError:

            return False

        finally:

            conn.close()


# ============================================================
# HTML HELPER
# ============================================================


def activate_registration_atomic(transaction_id, user_id, amount_rupiah):
    """Idempotent registration credit: transaction, status and bonus in one SQLite transaction."""
    conn=get_db()
    try:
        cur=conn.cursor(); cur.execute("BEGIN IMMEDIATE")
        cur.execute("INSERT OR IGNORE INTO transactions(transaction_id,user_id,amount_rupiah,coins,created_at) VALUES(?,?,?,?,?)",(str(transaction_id),int(user_id),int(amount_rupiah),REGISTRATION_BONUS_COINS,now_text()))
        if cur.rowcount == 0:
            conn.rollback(); return False
        cur.execute("UPDATE users SET is_registered=1,registered_at=?,balance=balance+? WHERE user_id=? AND is_registered=0",(now_text(),REGISTRATION_BONUS_COINS,int(user_id)))
        if cur.rowcount == 0:
            conn.rollback(); return False
        conn.commit(); return True
    except Exception:
        conn.rollback(); raise
    finally:
        conn.close()


def credit_topup_atomic(transaction_id, user_id, amount_rupiah, coins):
    """Idempotent top-up: transaction record and balance credit commit together."""
    conn=get_db()
    try:
        cur=conn.cursor(); cur.execute("BEGIN IMMEDIATE")
        cur.execute("INSERT OR IGNORE INTO transactions(transaction_id,user_id,amount_rupiah,coins,created_at) VALUES(?,?,?,?,?)",(str(transaction_id),int(user_id),int(amount_rupiah),int(coins),now_text()))
        if cur.rowcount == 0:
            conn.rollback(); return False
        cur.execute("UPDATE users SET balance=balance+? WHERE user_id=?",(int(coins),int(user_id)))
        if cur.rowcount == 0:
            conn.rollback(); return False
        conn.commit(); return True
    except Exception:
        conn.rollback(); raise
    finally:
        conn.close()

def esc(text):

    if text is None:
        return ""

    return html.escape(
        str(text),
        quote=False
    )


# ============================================================
# TEXT
# ============================================================

START_TEXT = """
👋 <b>Hai, {name}!</b>

🤖 <b>FWB SURABAYA FESS</b> adalah bot auto post pesan yang kamu kirim akan masuk ke channel <b>@FWBSURABAYA</b>.

Silakan baca Rules dan Help terlebih dahulu.

Pilih menu di bawah untuk melihat informasi.
"""


RULES_TEXT = """
📚 <b>RULES @FWBSURABAYA</b>

1. DILARANG SARA / RUSUH / SPAM / MENGANCAM YANG MERUGIKAN ORANG LAIN.

2. DILARANG MENGEMIS DANA / PULSA / REFERRAL DLL.

3. DILARANG PROMOSI GROUP / LPM / BOT LAIN MENGGUNAKAN FITUR BASE UNTUK MENDAPATKAN KEUNTUNGAN MATERIL.

4. DILARANG MENYEBARKAN AIB ORANG LAIN ATAU FAKE PROMOTE MENGGUNAKAN USERNAME ORANG LAIN ATAU SHARE NOMOR WHATSAPP.

5. HARUS BERUSIA MINIMAL 18 TAHUN.

6. DILARANG MENANYAKAN ATAU MENGIRIM MENFESS TENTANG PROSTITUSI SEPERTI OPEN BO / CC / LC / VCS / JUAL KONTEN VIP/VVIP / SEXTAPE / FFM / MMF / 3SOME DAN SEJENISNYA.

7. OWNER SELALU BENAR.

━━━━━━━━━━━━━━

⚠️ <b>MELANGGAR = AUTO BANNED</b>

💸 <b>BIAYA UNBAN = 30RB</b>
"""


COMMANDS_TEXT = """
💡 <b>COMMANDS FWB SURABAYA</b>

/start
Menampilkan menu utama.

/status
Melihat profile, saldo dan statistik.

/topup
Melihat cara top up coin.

━━━━━━━━━━━━━━

📨 <b>CARA KIRIM MENFESS</b>

Kirim <b>satu pesan</b> yang di dalamnya terdapat:

<code>#fwbgirl</code>

atau

<code>#fwbboy</code>

Hashtag boleh berada di depan, tengah, atau belakang pesan.

Contoh:

<code>#fwbgirl ada yang mau nongkrong malam ini?</code>

atau:

<code>ada yang mau nongkrong #fwbgirl</code>

atau:

<code>ada yang mau nongkrong malam ini? #fwbgirl</code>

🎁 Kamu mendapatkan <b>1x menfess gratis setiap hari</b>.

⏰ Reset gratis setiap hari pukul <b>00:01 WIB</b>.

💰 Setelah jatah gratis habis, pengiriman berikutnya menggunakan coin.
"""


HELP_TEXT = """
❓ <b>HELP FWB SURABAYA</b>

Untuk mengirim menfess, cukup kirim satu pesan yang mengandung:

<code>#fwbgirl</code>

atau

<code>#fwbboy</code>

Hashtag boleh diletakkan di mana saja.

Bot akan otomatis mendeteksi hashtag tersebut dan menghapus hashtag dari isi pesan sebelum diposting.

🎁 <b>1x gratis setiap hari.</b>

⏰ Reset setiap hari pukul <b>00:01 WIB</b>.

💰 Setelah gratis habis, harga pengiriman adalah <b>100 coin</b>.

⚠️ Pastikan membaca Rules sebelum mengirim.
"""


def topup_text(user_id):

    return f"""
💰 <b>TOP UP COIN FWB SURABAYA</b>

1 coin = Rp10

Contoh:

100 coin = Rp1.000
500 coin = Rp5.000
1.000 coin = Rp10.000

━━━━━━━━━━━━━━

📌 <b>Code Top Up kamu:</b>

<code>fwb {user_id}</code>

Saat melakukan pembayaran melalui Trakteer, masukkan code tersebut pada pesan dukungan.

Setelah pembayaran terdeteksi oleh webhook, coin akan otomatis masuk ke akun Telegram kamu.

⚠️ Jangan menghapus code tersebut dari pesan dukungan.
"""


# ============================================================
# KEYBOARDS
# ============================================================

def main_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "👤 Profile",
                callback_data="profile"
            ),
            InlineKeyboardButton(
                "📚 Rules",
                callback_data="rules"
            )
        ],
        [
            InlineKeyboardButton(
                "💡 Commands",
                callback_data="commands"
            ),
            InlineKeyboardButton(
                "❓ Help",
                callback_data="help"
            )
        ],
        [
            InlineKeyboardButton(
                "🔗 Referral",
                callback_data="referral"
            ),
            InlineKeyboardButton(
                "💰 Top Up",
                callback_data="topup"
            )
        ],
        [
            InlineKeyboardButton(
                "❌ Close",
                callback_data="close"
            )
        ]
    ])


def back_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🔙 Kembali",
                callback_data="home"
            ),
            InlineKeyboardButton(
                "❌ Tutup",
                callback_data="close"
            )
        ]
    ])


def profile_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "💰 Cara Top Up Coin",
                callback_data="topup"
            )
        ],
        [
            InlineKeyboardButton(
                "🔙 Kembali",
                callback_data="home"
            ),
            InlineKeyboardButton(
                "❌ Tutup",
                callback_data="close"
            )
        ]
    ])


def topup_keyboard(user_id):
    """Buat link Trakteer personal yang membawa Telegram user ID."""
    supporter_message = quote(f"fwb {user_id}")

    topup_url = (
        f"https://trakteer.id/{TRAKTEER_USERNAME}/tip"
        f"?quantity=10&step=2&supporter_message={supporter_message}"
    )

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "💰 Buka Trakteer",
                url=topup_url
            )
        ],
        [
            InlineKeyboardButton(
                "🔙 Kembali",
                callback_data="home"
            ),
            InlineKeyboardButton(
                "❌ Tutup",
                callback_data="close"
            )
        ]
    ])


# ============================================================
# START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user = update.effective_user

    create_user(user)
    if context.args:
        arg=context.args[0]
        if arg.startswith("ref_"):
            try: attribute_referral(user.id, int(arg[4:]))
            except Exception: pass
        elif arg.startswith("promo_"):
            try:
                with db_lock:
                    conn=get_db(); cur=conn.cursor(); cur.execute("SELECT admin_id FROM admin_promotions WHERE code=?",(arg,)); prow=cur.fetchone()
                    if prow:
                        cur.execute("INSERT OR IGNORE INTO promotion_attributions(user_id,admin_id,created_at) VALUES(?,?,?)",(user.id,prow['admin_id'],now_text()))
                    conn.commit(); conn.close()
            except Exception: logger.exception("Gagal mencatat atribusi promosi")

    if not await force_sub_guard(update, context):
        return

    if not is_user_registered(user.id):
        row = get_user(user.id)
        if row and not row["trial_used"]:
            await update.message.reply_text(
                "👋 <b>SELAMAT DATANG DI FWB SURABAYA!</b>\n\n"
                "💌 <b>CARA KIRIM MENFESS</b>\n"
                "Ketik pesan kamu dan sertakan salah satu hashtag ini:\n"
                "👧 <code>#fwbgirl</code>\n"
                "👦 <code>#fwbboy</code>\n\n"
                "Contoh:\n"
                "<code>#fwbgirl Ada yang mau kenalan?</code>\n\n"
                "🎁 <b>COBAIN TRIAL GRATIS!</b>\n"
                "Kamu punya 1x kesempatan kirim menfess gratis seumur hidup.\n"
                "Langsung kirim pesan dengan hashtag di atas ke chat bot ini.\n\n"
                "Setelah trial dipakai, kamu bisa registrasi Rp"
                f"{REGISTRATION_PRICE:,} dan dapat bonus {REGISTRATION_BONUS_COINS} coin.\n\n"
                "Yuk, cobain dulu! ❤️",
                parse_mode=ParseMode.HTML
            )
            return
        await send_registration_prompt(update.message, user)
        return

    welcome_template = get_setting("welcome", START_TEXT)
    text = welcome_template.format(name=esc(user.first_name or "User")).replace("@FWBSURABAYA", html.escape(CHANNEL_USERNAME))
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())


# ============================================================
# STATUS / PROFILE
# ============================================================

def build_profile_text(user_id):

    reset_daily_free_if_needed(user_id)

    row = get_user(user_id)

    if not row:
        return "❌ User tidak ditemukan."

    username = (
        f"@{esc(row['username'])}"
        if row["username"]
        else "-"
    )

    free_remaining = get_free_remaining(user_id)

    return f"""
👤 <b>Profile User</b>

📛 Nama: {esc(row['first_name'])}
🆔 User ID: <code>{row['user_id']}</code>
🌐 Username: {username}
💰 Saldo: {row['balance']} coin
🔰 Status: terdaftar

📊 <b>User Stats</b>

• Menfess Gratis: {FREE_MENFESS - free_remaining}/{FREE_MENFESS}
• Sisa Gratis: {free_remaining}
• Semua Menfess: {row['total_menfess']}
• Bergabung: {esc(row['joined_at'])}

━━━━━━━━━━━━━━

🔑 <b>Code Top Up:</b>

<code>fwb {row['user_id']}</code>
"""


async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user = update.effective_user

    create_user(user)

    if not await force_sub_guard(update, context):
        return
    if not is_user_registered(user.id):
        await send_registration_prompt(update.message, user)
        return

    await update.message.reply_text(
        build_profile_text(user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=profile_keyboard()
    )


# ============================================================
# TOPUP
# ============================================================

async def topup_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user = update.effective_user

    create_user(user)

    if not await force_sub_guard(update, context):
        return
    if not is_user_registered(user.id):
        await send_registration_prompt(update.message, user)
        return

    await update.message.reply_text(
        topup_text(user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=topup_keyboard(user.id)
    )


# ============================================================
# TOPUP
# ============================================================

# ============================================================
# CALLBACK
# ============================================================

async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    query = update.callback_query

    await query.answer()

    user = query.from_user

    create_user(user)

    data = query.data

    if data == "check_registration":
        if is_user_registered(user.id):
            row = get_user(user.id)
            await query.edit_message_text(
                f"✅ Registrasi berhasil.\nBonus saldo: {REGISTRATION_BONUS_COINS} coin.\nSaldo sekarang: {row['balance']} coin.",
                reply_markup=main_keyboard()
            )
        else:
            await query.answer("Pembayaran belum terdeteksi. Coba lagi setelah webhook Trakteer masuk.", show_alert=True)
        return

    # Semua menu utama dikunci sampai registrasi lunas.
    if data in ("home", "profile", "rules", "commands", "help", "referral", "topup") and not is_user_registered(user.id):
        await send_registration_prompt(query.message, user)
        return

    # --------------------------------------------------------
    # FORCE SUB CHECK BUTTON
    # --------------------------------------------------------

    if data == "check_force_sub":

        if not await force_sub_guard(update, context, force_refresh=True):
            return

        await query.answer("✅ Verifikasi berhasil!")

        if not is_user_registered(user.id):
            await query.edit_message_text(
                "Silakan selesaikan registrasi terlebih dahulu.",
                reply_markup=registration_keyboard(user.id)
            )
            return

        text = get_setting("welcome", START_TEXT).format(name=esc(user.first_name or "User")).replace("@FWBSURABAYA", html.escape(CHANNEL_USERNAME))
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        return

    # --------------------------------------------------------
    # FORCE SUB GUARD FOR MENU ACTIONS
    # --------------------------------------------------------

    if data in (
        "home",
        "profile",
        "rules",
        "commands",
        "help",
        "referral",
        "topup"
    ):
        if not await force_sub_guard(update, context):
            return

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":

        text = get_setting("welcome", START_TEXT).format(name=esc(user.first_name or "User")).replace("@FWBSURABAYA", html.escape(CHANNEL_USERNAME))

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    # --------------------------------------------------------
    # PROFILE
    # --------------------------------------------------------

    elif data == "profile":

        await query.edit_message_text(
            build_profile_text(user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=profile_keyboard()
        )

    # --------------------------------------------------------
    # RULES
    # --------------------------------------------------------

    elif data == "rules":

        await query.edit_message_text(
            get_setting("rules", RULES_TEXT).replace("@FWBSURABAYA", html.escape(CHANNEL_USERNAME)),
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard()
        )

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

    elif data == "commands":

        await query.edit_message_text(
            get_setting("commands", COMMANDS_TEXT).replace("@FWBSURABAYA", html.escape(CHANNEL_USERNAME)),
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard()
        )

    # --------------------------------------------------------
    # HELP
    # --------------------------------------------------------

    elif data == "help":

        await query.edit_message_text(
            get_setting("help", HELP_TEXT).replace("@FWBSURABAYA", html.escape(CHANNEL_USERNAME)),
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard()
        )

    # --------------------------------------------------------
    # TOPUP
    # --------------------------------------------------------

    elif data == "topup":

        await query.edit_message_text(
            topup_text(user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=topup_keyboard(user.id)
        )

    # --------------------------------------------------------
    # REFERRAL
    # --------------------------------------------------------

    elif data == "referral":

        referral_link = (
            f"https://t.me/{BOT_USERNAME}"
            f"?start=ref_{user.id}"
        )

        text = f"""
🔗 <b>REFERRAL</b>

Kode referral kamu:

<code>fwbref{user.id}</code>

Link referral:

{esc(referral_link)}

Kirim link tersebut kepada teman kamu.
"""

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=back_keyboard()
        )

    # --------------------------------------------------------
    # DELETE MENFESS
    # --------------------------------------------------------

    elif data.startswith("delete_menfess:"):

        try:

            message_id = int(
                data.split(":", 1)[1]
            )

        except Exception:

            await query.answer(
                "ID pesan tidak valid.",
                show_alert=True
            )

            return

        try:

            with db_lock:
                conn = get_db()
                cur = conn.cursor()
                cur.execute(
                    "SELECT user_id FROM menfess WHERE message_id = ?",
                    (message_id,)
                )
                owner_row = cur.fetchone()
                conn.close()

            if not owner_row:
                await query.answer(
                    "Menfess tidak ditemukan di database.",
                    show_alert=True
                )
                return

            if owner_row["user_id"] != query.from_user.id:
                await query.answer(
                    "Kamu hanya bisa menghapus menfess milikmu sendiri.",
                    show_alert=True
                )
                return

            await context.bot.delete_message(
                chat_id=CHANNEL_USERNAME,
                message_id=message_id
            )

            await query.edit_message_text(
                "🗑 <b>Menfess berhasil dihapus.</b>",
                parse_mode=ParseMode.HTML
            )

        except Exception as e:

            logger.exception(
                "Gagal menghapus menfess"
            )

            await query.answer(
                "Gagal menghapus. Pastikan bot admin di channel.",
                show_alert=True
            )

    # --------------------------------------------------------
    # CLOSE
    # --------------------------------------------------------

    elif data == "close":

        try:

            await query.delete_message()

        except Exception:

            pass


# ============================================================
# DETECT MENFESS HASHTAG
# ============================================================

def get_active_hashtags():
    try:
        with db_lock:
            conn=get_db(); cur=conn.cursor()
            cur.execute("SELECT tag, category FROM hashtags WHERE enabled=1")
            rows=cur.fetchall(); conn.close()
        return [(r['tag'], (r['category'] or '').lower()) for r in rows]
    except Exception:
        return [("#fwbgirl","girl"),("#fwbboy","boy")]


def hashtag_regex(active_only=True):
    tags = get_active_hashtags() if active_only else [("#fwbgirl","girl"),("#fwbboy","boy")]
    if not tags: return re.compile(r"(?!)")
    pattern = r"(?<!\w)(?:" + "|".join(re.escape(t.lstrip('#')) for t,_ in tags) + r")\b"
    return re.compile(pattern, re.IGNORECASE)


def detect_menfess(text):
    found=[]
    for tag, category in get_active_hashtags():
        if re.search(r"(?<!\w)"+re.escape(tag)+r"\b", text, re.IGNORECASE):
            found.append((tag,category))
    if not found: return None
    categories={cat for _,cat in found}
    if len(categories)>1: return "both"
    return next(iter(categories)) if categories else "other"


def remove_menfess_tag(text):
    for tag,_ in get_active_hashtags():
        text=re.sub(r"(?<!\w)"+re.escape(tag)+r"\b", "", text, flags=re.IGNORECASE)
    cleaned = text

    # Rapikan spasi
    cleaned = re.sub(
        r"[ \t]+",
        " ",
        cleaned
    )

    cleaned = re.sub(
        r"\n[ \t]+",
        "\n",
        cleaned
    )

    return cleaned.strip()


# ============================================================
# USERNAME VALIDATION
# ============================================================

def contains_other_username(text, user):

    usernames = re.findall(
        r"(?<!\w)@([A-Za-z0-9_]{5,32})",
        text
    )

    if not usernames:
        return False, None

    own_username = (
        user.username.lower()
        if user.username
        else None
    )

    for username in usernames:

        if not own_username:

            return True, username

        if username.lower() != own_username:

            return True, username

    return False, None


# ============================================================
# PROCESS MENFESS
# ============================================================

async def process_menfess(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user = update.effective_user

    create_user(user)

    if not await force_sub_guard(update, context):
        return
    row = get_user(user.id)
    if not is_user_registered(user.id) and not user_is_staff(user.id) and not (row and not row["trial_used"]):
        await send_registration_prompt(update.message, user)
        return

    row = get_user(user.id)

    # --------------------------------------------------------
    # BAN
    # --------------------------------------------------------

    if row["is_banned"]:

        await update.message.reply_text(
            "🚫 Akun kamu telah dibanned."
        )

        return

    original_text = update.message.text or ""

    # --------------------------------------------------------
    # DETECT TAG
    # --------------------------------------------------------

    gender = detect_menfess(
        original_text
    )

    if gender is None:

        return

    if gender == "both":

        await update.message.reply_text(
            "⚠️ Gunakan hanya satu hashtag aktif. Jangan gabungkan dua kategori hashtag dalam satu menfess."
        )

        return

    # --------------------------------------------------------
    # HAPUS HASHTAG
    # --------------------------------------------------------

    text = remove_menfess_tag(
        original_text
    )

    if not text:

        await update.message.reply_text(
            "⚠️ Isi menfess tidak boleh kosong."
        )

        return

    # --------------------------------------------------------
    # USERNAME VALIDATION
    # --------------------------------------------------------

    bad_username, username = contains_other_username(
        text,
        user
    )

    if bad_username:

        await update.message.reply_text(
            "🚫 Menfess ditolak.\n\n"
            "Kalau ingin mencantumkan @username, "
            "kamu hanya boleh mencantumkan username "
            "Telegram milik kamu sendiri."
        )

        return

    # --------------------------------------------------------
    # PAYMENT
    # --------------------------------------------------------

    free_remaining = get_free_remaining(user.id) if is_user_registered(user.id) else 0
    is_staff = user_is_staff(user.id)
    is_trial = (not is_staff and not is_user_registered(user.id) and row and not row["trial_used"])

    paid_with_coin = False
    used_free = False

    # --------------------------------------------------------
    # FREE
    # --------------------------------------------------------

    if is_trial:
        with db_lock:
            conn=get_db(); cur=conn.cursor()
            cur.execute("UPDATE users SET trial_used=1 WHERE user_id=? AND trial_used=0", (user.id,))
            trial_claimed=cur.rowcount>0
            conn.commit(); conn.close()
        if not trial_claimed:
            await update.message.reply_text("Trial sudah pernah digunakan. Silakan registrasi.")
            return
        used_free = True
    elif is_staff:
        pass  # Admin/owner bebas biaya dan batas harian.
    elif free_remaining > 0:
        used = use_free_menfess(user.id)
        if not used:
            await update.message.reply_text("⚠️ Gagal menggunakan free menfess. Coba lagi.")
            return
        used_free = True
    else:

        if not subtract_balance(
            user.id,
            MENFESS_COST
        ):

            current = get_user(
                user.id
            )

            await update.message.reply_text(
                f"""
💰 <b>Saldo coin tidak cukup.</b>

Harga menfess: <b>{MENFESS_COST} coin</b>
Saldo kamu: <b>{current['balance']} coin</b>

Silakan top up terlebih dahulu.
""",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "💰 Top Up",
                            callback_data="topup"
                        )
                    ]
                ])
            )

            return

        paid_with_coin = True

    # --------------------------------------------------------
    # FORMAT CHANNEL
    # --------------------------------------------------------

    active_tags = get_active_hashtags()
    tag = next((t for t,cat in active_tags if cat == gender), "#" + str(gender))

    # Hanya tag + isi pesan
    channel_text = (
        f"{tag} {text}"
    )

    # --------------------------------------------------------
    # SEND CHANNEL
    # --------------------------------------------------------

    try:

        sent = await context.bot.send_message(
            chat_id=CHANNEL_USERNAME,
            text=channel_text,
            parse_mode=None
        )

    except Exception as e:

        logger.exception(
            "Gagal mengirim menfess ke channel"
        )

        # Refund coin
        if paid_with_coin:

            add_balance(
                user.id,
                MENFESS_COST
            )

        # Refund free
        elif used_free:
            if is_trial:
                with db_lock:
                    conn=get_db(); cur=conn.cursor()
                    cur.execute("UPDATE users SET trial_used=0 WHERE user_id=?", (user.id,))
                    conn.commit(); conn.close()
            elif not is_trial and not is_staff:
                refund_free_menfess(user.id)

        await update.message.reply_text(
            "❌ Menfess gagal dikirim ke channel.\n"
            "Jatah/coin kamu sudah dikembalikan.\n"
            "Silakan coba lagi."
        )

        return

    # --------------------------------------------------------
    # SAVE DATABASE
    # --------------------------------------------------------

    increase_menfess(
        user.id
    )

    save_menfess(
        user.id,
        gender,
        sent.message_id,
        text
    )

    # --------------------------------------------------------
    # MESSAGE LINK
    # --------------------------------------------------------

    clean_channel = (
        CHANNEL_USERNAME
        .replace("@", "")
    )

    message_link = (
        f"https://t.me/"
        f"{clean_channel}/"
        f"{sent.message_id}"
    )

    # --------------------------------------------------------
    # DAILY COUNTER
    # --------------------------------------------------------

    row = get_user(
        user.id
    )

    free_remaining_after = (
        get_free_remaining(
            user.id
        )
    )

    free_used_today = (
        FREE_MENFESS
        - free_remaining_after
    )

    # --------------------------------------------------------
    # SUCCESS MESSAGE
    # --------------------------------------------------------

    if is_trial:
        success_text = (
            "🎉 <b>TRIAL BERHASIL! MENFESS KAMU SUDAH TERKIRIM.</b>\n\n"
            "Trial gratis seumur hidup kamu sudah digunakan.\n\n"
            f"💳 <b>MAU KIRIM MENFESS LAGI?</b>\n"
            f"Registrasi hanya <b>Rp{REGISTRATION_PRICE:,}</b> dan dapat bonus "
            f"<b>{REGISTRATION_BONUS_COINS} coin</b>.\n\n"
            "🎁 Setelah registrasi:\n"
            "• 1x menfess gratis setiap hari.\n"
            "• Menfess tambahan bisa menggunakan coin.\n\n"
            "📝 <b>CARA REGISTRASI</b>\n"
            "1. Tekan tombol BAYAR REGISTRASI.\n"
            "2. Ikuti instruksi pembayaran Trakteer.\n"
            "3. Kode <code>fwbreg ID_TELEGRAM</code> otomatis terisi di pesan pembayaran. Kamu tidak perlu mengetik ID sendiri.\n"
            "4. Selesaikan pembayaran. Akun aktif otomatis setelah pembayaran terverifikasi.\n\n"
            "Terima kasih sudah mencoba FWB Surabaya! ❤️"
        )
        registration_buttons = registration_keyboard(user.id).inline_keyboard
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👀 LIHAT MENFESS", url=message_link),
             InlineKeyboardButton("🗑 HAPUS", callback_data=f"delete_menfess:{sent.message_id}")],
            *registration_buttons
        ])
    else:
        success_text = f"""
<b>pesan anda berhasil terkirim.</b>

hari ini kamu telah mengirim pesan
sebanyak <b>{free_used_today}/{FREE_MENFESS}</b>. kamu dapat mengirim
pesan sebanyak <b>{FREE_MENFESS - free_used_today}</b> kali dalam sehari

waktu reset setiap hari <b>00:01 WIB</b>
"""
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("👀 LIHAT", url=message_link),
                InlineKeyboardButton("🗑 HAPUS", callback_data=f"delete_menfess:{sent.message_id}")
            ]
        ])

    await update.message.reply_text(
        success_text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )


# ============================================================
# MENFESS HANDLER
# ============================================================

async def menfess_message_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    text = update.message.text or ""

    if detect_menfess(text) is None:
        return

    await process_menfess(
        update,
        context
    )


# ============================================================
# TRAKTEER
# ============================================================

def extract_registration_id(message):
    if not message:
        return None
    match = re.search(r"\bfwbreg\s+(\d{5,15})\b", str(message), re.IGNORECASE)
    return int(match.group(1)) if match else None


def extract_telegram_id(message):

    if not message:
        return None

    patterns = [
        r"\bfwb\s+(\d{5,15})\b",
        r"\bfwb(\d{5,15})\b"
    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            str(message),
            re.IGNORECASE
        )

        if match:

            try:

                return int(
                    match.group(1)
                )

            except Exception:

                return None

    return None


# ============================================================
# SEND TOPUP NOTIFICATION
# ============================================================

async def send_topup_notification(
    user_id,
    amount_rupiah,
    coins
):

    try:

        row = get_user(
            user_id
        )

        if not row:
            return

        await BOT_APP.bot.send_message(
            chat_id=user_id,
            text=f"""
✅ <b>TOP UP BERHASIL</b>

💰 Pembayaran: Rp{amount_rupiah:,}
🪙 Coin masuk: +{coins}
💳 Saldo sekarang: {row['balance']} coin

Terima kasih telah melakukan top up! 🎉
""".replace(",", "."),
            parse_mode=ParseMode.HTML
        )

    except Exception as e:

        logger.warning(
            "Notif topup gagal: %s",
            e
        )


# ============================================================
# TRAKTEER WEBHOOK
# ============================================================

async def trakteer_webhook(
    request
):

    try:

        # ----------------------------------------------------
        # TOKEN
        # ----------------------------------------------------

        received_token = request.headers.get(
            "X-Webhook-Token"
        )

        if (
            TRAKTEER_WEBHOOK_TOKEN
            and received_token != TRAKTEER_WEBHOOK_TOKEN
        ):

            logger.warning(
                "Webhook ditolak: token salah"
            )

            return web.json_response(
                {
                    "ok": False,
                    "error": "invalid token"
                },
                status=401
            )

        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------

        data = await request.json()

        logger.info(
            "Webhook Trakteer diterima: %s",
            data
        )

        # ----------------------------------------------------
        # EVENT
        # ----------------------------------------------------

        event_type = data.get(
            "type"
        )

        if event_type != "tip":

            return web.json_response({
                "ok": True,
                "message": "event ignored"
            })

        # ----------------------------------------------------
        # TRANSACTION ID
        # ----------------------------------------------------

        transaction_id = str(
            data.get(
                "transaction_id",
                ""
            )
        )

        if not transaction_id:

            return web.json_response(
                {
                    "ok": False,
                    "error": "missing transaction_id"
                },
                status=400
            )

        # ----------------------------------------------------
        # ANTI DOUBLE
        # ----------------------------------------------------

        if transaction_exists(
            transaction_id
        ):

            return web.json_response({
                "ok": True,
                "message": "transaction already processed"
            })

        # ----------------------------------------------------
        # SUPPORTER MESSAGE
        # ----------------------------------------------------

        supporter_message = (
            data.get(
                "supporter_message"
            )
            or ""
        )

        registration_user_id = extract_registration_id(supporter_message)
        if registration_user_id:
            row = get_user(registration_user_id)
            if not row:
                return web.json_response({"ok": True, "message": "registration user not found"})
            try:
                registration_amount = int(float(data.get("price", 0)))
            except Exception:
                registration_amount = 0
            if registration_amount < REGISTRATION_PRICE:
                return web.json_response({"ok": True, "message": "registration amount below Rp10000"})
            try:
                activated = activate_registration_atomic(transaction_id, registration_user_id, registration_amount)
            except Exception:
                logger.exception("Gagal memproses registrasi secara atomik")
                return web.json_response({"ok": False, "error": "registration processing failed"}, status=500)
            if not activated:
                return web.json_response({"ok": True, "message": "transaction already processed or user already registered"})
            if activated:
                with db_lock:
                    conn=get_db(); cur=conn.cursor(); cur.execute("UPDATE promotion_attributions SET registered=1 WHERE user_id=?",(registration_user_id,)); conn.commit(); conn.close()
            referral_owner = pay_referral_bonus(registration_user_id) if activated else None
            if referral_owner and BOT_LOOP is not None:
                try:
                    asyncio.run_coroutine_threadsafe(BOT_APP.bot.send_message(chat_id=referral_owner, text=f"🎉 Referral berhasil registrasi! Bonus +{REFERRAL_BONUS_COINS} koin."), BOT_LOOP)
                except Exception as notify_error:
                    logger.warning("Notif referral gagal: %s", notify_error)
            if activated and BOT_LOOP is not None:
                try:
                    asyncio.run_coroutine_threadsafe(
                        BOT_APP.bot.send_message(
                            chat_id=registration_user_id,
                            text=("✅ <b>REGISTRASI BERHASIL</b>\n\n"
                                  f"Pembayaran: Rp{registration_amount:,}.\n"
                                  f"Bonus registrasi: +{REGISTRATION_BONUS_COINS} coin.\n"
                                  "Akun kamu sekarang aktif dan bisa mengirim menfess."),
                            parse_mode=ParseMode.HTML
                        ), BOT_LOOP
                    )
                except Exception as notify_error:
                    logger.warning("Notif registrasi gagal: %s", notify_error)
            return web.json_response({"ok": True, "message": "registration activated" if activated else "already registered"})

        telegram_id = extract_telegram_id(
            supporter_message
        )

        if not telegram_id:

            logger.warning(
                "ID Telegram tidak ditemukan: %s",
                supporter_message
            )

            return web.json_response({
                "ok": True,
                "message": "telegram id not found"
            })

        # ----------------------------------------------------
        # USER
        # ----------------------------------------------------

        row = get_user(
            telegram_id
        )

        if not row:

            logger.warning(
                "User belum terdaftar: %s",
                telegram_id
            )

            return web.json_response({
                "ok": True,
                "message": "user not registered"
            })

        # ----------------------------------------------------
        # AMOUNT
        # ----------------------------------------------------

        price = data.get(
            "price",
            0
        )

        try:

            amount_rupiah = int(
                float(price)
            )

        except Exception:

            amount_rupiah = 0

        coins = (
            amount_rupiah
            // RUPIAH_PER_COIN
        )

        if coins <= 0:

            return web.json_response({
                "ok": True,
                "message": "amount too small"
            })

        # ----------------------------------------------------
        # SAVE TRANSACTION
        # ----------------------------------------------------

        try:
            saved = credit_topup_atomic(transaction_id, telegram_id, amount_rupiah, coins)
        except Exception:
            logger.exception("Gagal memproses top-up secara atomik")
            return web.json_response({"ok": False, "error": "topup processing failed"}, status=500)

        if not saved:
            return web.json_response({"ok": True, "message": "already processed"})

        new_balance = get_user(
            telegram_id
        )["balance"]

        logger.info(
            "TOPUP BERHASIL | user=%s | Rp%s | +%s coin",
            telegram_id,
            amount_rupiah,
            coins
        )

        # ----------------------------------------------------
        # NOTIFICATION
        # ----------------------------------------------------

        if BOT_LOOP is not None:

            try:

                future = asyncio.run_coroutine_threadsafe(
                    send_topup_notification(
                        telegram_id,
                        amount_rupiah,
                        coins
                    ),
                    BOT_LOOP
                )

                # Tidak perlu menunggu
                logger.info(
                    "Notifikasi topup dijadwalkan."
                )

            except Exception as e:

                logger.warning(
                    "Gagal menjadwalkan notif topup: %s",
                    e
                )

        # ----------------------------------------------------
        # RESPONSE
        # ----------------------------------------------------

        return web.json_response({
            "ok": True,
            "message": "topup success",
            "user_id": telegram_id,
            "coins": coins,
            "balance": new_balance
        })

    except Exception as e:

        logger.exception(
            "ERROR WEBHOOK"
        )

        return web.json_response(
            {
                "ok": False,
                "error": "internal server error"
            },
            status=500
        )


async def hashtag_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    if len(context.args)<2:
        await update.message.reply_text("Format: /hashtag_add #hashtag girl|boy"); return
    tag=context.args[0].lower(); category=context.args[1].lower()
    if not re.fullmatch(r"#[a-z0-9_]{2,32}",tag) or category not in ("girl","boy"):
        await update.message.reply_text("Hashtag tidak valid. Kategori hanya girl atau boy."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("INSERT INTO hashtags(tag,category,enabled) VALUES(?,?,1) ON CONFLICT(tag) DO UPDATE SET category=excluded.category,enabled=1",(tag,category)); conn.commit(); conn.close()
    await update.message.reply_text(f"Hashtag {tag} aktif.")


async def hashtag_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    if not context.args:
        await update.message.reply_text("Format: /hashtag_off #hashtag"); return
    tag=context.args[0].lower()
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("UPDATE hashtags SET enabled=0 WHERE tag=?",(tag,)); changed=cur.rowcount; conn.commit(); conn.close()
    await update.message.reply_text("Hashtag dinonaktifkan." if changed else "Hashtag tidak ditemukan.")


async def hashtag_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("SELECT tag,category,enabled FROM hashtags ORDER BY tag"); rows=cur.fetchall(); conn.close()
    body="\n".join(f"{r['tag']} — {r['category']} — {'aktif' if r['enabled'] else 'nonaktif'}" for r in rows) or "Belum ada hashtag."
    await update.message.reply_text(body)


async def promo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("Khusus admin."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("SELECT code FROM admin_promotions WHERE admin_id=?",(uid,)); row=cur.fetchone()
        if not row:
            code=f"promo_{uid}"
            cur.execute("INSERT OR IGNORE INTO admin_promotions(admin_id,code,created_at) VALUES(?,?,?)",(uid,code,now_text()))
            conn.commit(); row={'code':code}
        code=row['code']
        cur.execute("SELECT COUNT(*) n FROM promotion_attributions WHERE admin_id=?",(uid,)); clicks=cur.fetchone()['n']
        cur.execute("SELECT COUNT(*) n FROM promotion_attributions WHERE admin_id=? AND registered=1",(uid,)); regs=cur.fetchone()['n']
        conn.close()
    link=f"https://t.me/{BOT_USERNAME}?start={code}"
    await update.message.reply_text(f"Link promosi kamu:\n{link}\nPengguna teratribusi: {clicks}\nRegistrasi berhasil: {regs}")


async def promostats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("SELECT a.admin_id,a.code,COUNT(p.user_id) users,SUM(CASE WHEN p.registered=1 THEN 1 ELSE 0 END) regs FROM admin_promotions a LEFT JOIN promotion_attributions p ON p.admin_id=a.admin_id GROUP BY a.admin_id ORDER BY regs DESC")
        rows=cur.fetchall(); conn.close()
    out="\n".join(f"{r['admin_id']} | {r['code']} | {r['users']} pengguna | {r['regs'] or 0} registrasi" for r in rows) or "Belum ada data promosi."
    await update.message.reply_text(out)


async def settext_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Akses ditolak."); return
    if len(context.args)<2 or context.args[0] not in ("welcome","rules","help","commands"):
        await update.message.reply_text("Format: /settext welcome|rules|help|commands teks_baru"); return
    key=context.args[0]; value=" ".join(context.args[1:])
    set_setting(key,value)
    await update.message.reply_text(f"Teks {key} berhasil diperbarui.")


async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Akses ditolak."); return
    if len(context.args)<3:
        await update.message.reply_text("Format: /schedule YYYY-MM-DD HH:MM pesan pengumuman"); return
    try:
        dt=datetime.strptime(context.args[0]+" "+context.args[1],"%Y-%m-%d %H:%M").replace(tzinfo=WIB)
        if dt <= now_wib(): raise ValueError("waktu harus di masa depan")
    except Exception:
        await update.message.reply_text("Tanggal/jam tidak valid atau sudah lewat. Gunakan WIB."); return
    message=" ".join(context.args[2:])
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("INSERT INTO scheduled_announcements(creator_id,publish_at,message,status,created_at) VALUES(?,?,?,'pending',?)",(update.effective_user.id,dt.strftime('%Y-%m-%d %H:%M:%S'),message,now_text())); ticket=cur.lastrowid; conn.commit(); conn.close()
    await update.message.reply_text(f"Pengumuman terjadwal #{ticket} pada {dt.strftime('%d-%m-%Y %H:%M')} WIB.")


async def announcement_worker(application):
    while True:
        try:
            with db_lock:
                conn=get_db(); cur=conn.cursor(); cur.execute("SELECT id,message FROM scheduled_announcements WHERE status='pending' AND publish_at<=? ORDER BY id",(now_text(),)); rows=cur.fetchall(); conn.close()
            for row in rows:
                try:
                    await application.bot.send_message(chat_id=CHANNEL_USERNAME,text=row['message'])
                    status='sent'
                except Exception as exc:
                    logger.exception("Pengumuman terjadwal gagal #%s",row['id']); status='failed'
                with db_lock:
                    conn=get_db(); cur=conn.cursor(); cur.execute("UPDATE scheduled_announcements SET status=? WHERE id=? AND status='pending'",(status,row['id'])); conn.commit(); conn.close()
        except Exception:
            logger.exception("Announcement scheduler error")
        await asyncio.sleep(10)


async def autocopy_switch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Auto-Copy hanya bisa diakses owner."); return
    if not context.args or context.args[0].lower() not in ("on","off"):
        await update.message.reply_text("Format: /autocopy_switch on|off"); return
    set_setting("autocopy_enabled", "1" if context.args[0].lower()=="on" else "0")
    await update.message.reply_text("Auto-Copy " + ("diaktifkan." if context.args[0].lower()=="on" else "dimatikan."))


async def autocopy_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Auto-Copy hanya bisa diakses owner."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("SELECT COUNT(*) n FROM autocopy_sources WHERE enabled=1"); sources=cur.fetchone()['n']
        cur.execute("SELECT COUNT(*) n FROM autocopy_log WHERE status='success'"); ok=cur.fetchone()['n']
        cur.execute("SELECT COUNT(*) n FROM autocopy_log WHERE status='failed'"); fail=cur.fetchone()['n']
        conn.close()
    await update.message.reply_text(f"Auto-Copy: {'ON' if get_setting('autocopy_enabled','1')=='1' else 'OFF'}\nSumber aktif: {sources}\nBerhasil: {ok}\nGagal: {fail}")


async def autocopy_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Auto-Copy hanya bisa diakses owner."); return
    if len(context.args)<3:
        await update.message.reply_text("Format: /autocopy_add SOURCE DESTINATION all|hashtag|keyword|both [term1,term2]"); return
    source,dest,mode=context.args[:3]
    terms=" ".join(context.args[3:]).strip()
    if mode not in ("all","hashtag","keyword","both"):
        await update.message.reply_text("Mode filter harus all, hashtag, keyword, atau both."); return
    if mode!="all" and not terms:
        await update.message.reply_text("Mode filter ini membutuhkan daftar kata/hashtag."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("INSERT INTO autocopy_sources(source,destination,filter_mode,filter_terms,enabled) VALUES(?,?,?,?,1) ON CONFLICT(source) DO UPDATE SET destination=excluded.destination,filter_mode=excluded.filter_mode,filter_terms=excluded.filter_terms,enabled=1",(source,dest,mode,terms))
        conn.commit(); conn.close()
    await update.message.reply_text("Sumber Auto-Copy disimpan. Perubahan akan dimuat otomatis oleh worker Telethon.")


async def autocopy_remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Auto-Copy hanya bisa diakses owner."); return
    if not context.args:
        await update.message.reply_text("Format: /autocopy_remove SOURCE"); return
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("DELETE FROM autocopy_sources WHERE source=?",(context.args[0],)); changed=cur.rowcount; conn.commit(); conn.close()
    await update.message.reply_text("Sumber dihapus." if changed else "Sumber tidak ditemukan.")


async def autocopy_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Auto-Copy hanya bisa diakses owner."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("SELECT source,destination,filter_mode,filter_terms,enabled FROM autocopy_sources ORDER BY source"); rows=cur.fetchall(); conn.close()
    text="\n".join(f"{r['source']} → {r['destination']} | {r['filter_mode']} | {r['filter_terms']} | {'ON' if r['enabled'] else 'OFF'}" for r in rows) or "Belum ada sumber Auto-Copy."
    await update.message.reply_text(text)


async def autocopy_enable_source_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Auto-Copy hanya bisa diakses owner."); return
    if len(context.args)<2 or context.args[1].lower() not in ("on","off"):
        await update.message.reply_text("Format: /autocopy_enable SOURCE on|off"); return
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("UPDATE autocopy_sources SET enabled=? WHERE source=?",(1 if context.args[1].lower()=="on" else 0,context.args[0])); changed=cur.rowcount; conn.commit(); conn.close()
    await update.message.reply_text("Status sumber diperbarui." if changed else "Sumber tidak ditemukan.")


async def autocopy_template_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Auto-Copy hanya bisa diakses owner."); return
    if len(context.args)<2:
        await update.message.reply_text("Format: /autocopy_template SOURCE|* before|after TEKS_TEMPLATE"); return
    scope,position=context.args[:2]
    template=" ".join(context.args[2:]).strip()
    if position not in ("before","after") or not template:
        await update.message.reply_text("Posisi before/after dan isi template wajib diisi."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("INSERT INTO autocopy_templates(source,destination,template,position,enabled) VALUES(?,?,?, ?,1)",(scope,"*",template,position)); conn.commit(); conn.close()
    await update.message.reply_text("Template Auto-Copy disimpan.")


# ============================================================
# ADMIN COMMANDS / SUPPORT
# ============================================================

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("Akses ditolak."); return
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("SELECT COUNT(*) n FROM users"); total=cur.fetchone()['n']
        cur.execute("SELECT COUNT(*) n FROM users WHERE is_registered=1"); registered=cur.fetchone()['n']
        cur.execute("SELECT COUNT(*) n FROM menfess WHERE substr(created_at,1,10)=?",(today_key(),)); menfess_today=cur.fetchone()['n']
        cur.execute("SELECT COALESCE(SUM(amount_rupiah),0) n FROM transactions"); revenue=cur.fetchone()['n']
        cur.execute("SELECT COALESCE(SUM(coins),0) n FROM transactions"); credited=cur.fetchone()['n']
        conn.close()
    text=("👑 <b>OWNER DASHBOARD FWB SURABAYA</b>\n\n" if is_owner(uid) else "🛡 <b>ADMIN DASHBOARD FWB SURABAYA</b>\n\n")
    text += f"Pengguna: {total}\nTerdaftar: {registered}\nMenfess hari ini: {menfess_today}"
    if is_owner(uid):
        text += f"\nPendapatan tercatat: Rp{revenue:,}\nKoin dari pembayaran: {credited}\n\nAuto-Copy: khusus owner.\nKelola admin: /addadmin ID atau /removeadmin ID\nAtur reward: /setreward JUMLAH\nCari user: /find kata_kunci\nAtur hashtag: /hashtag_list"
    else:
        text += "\n\nHak admin: promosi, statistik, ubah teks, jadwalkan pengumuman, patroli/ban, dan menfess tanpa batas.\nAuto-Copy, keuangan, saldo, unban, dan pengelolaan admin hanya owner."
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def addadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    try: target=int(context.args[0])
    except Exception:
        await update.message.reply_text("Format: /addadmin TELEGRAM_ID"); return
    ok=add_admin(target, update.effective_user.id)
    await update.message.reply_text("Admin berhasil ditambahkan." if ok else "Gagal/telah menjadi admin.")


async def removeadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    try: target=int(context.args[0])
    except Exception:
        await update.message.reply_text("Format: /removeadmin TELEGRAM_ID"); return
    ok=remove_admin(target, update.effective_user.id)
    await update.message.reply_text("Akses admin dicabut." if ok else "Admin tidak ditemukan.")


async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Akses ditolak."); return
    try: target=int(context.args[0])
    except Exception:
        await update.message.reply_text("Format: /ban TELEGRAM_ID"); return
    if not is_owner(update.effective_user.id) and (is_owner(target) or is_admin(target)):
        await update.message.reply_text("Admin biasa tidak bisa memban owner atau admin lain."); return
    ok=set_user_ban(target, True)
    await update.message.reply_text("Pengguna berhasil diban." if ok else "Pengguna tidak ditemukan.")


async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Unban hanya owner."); return
    try: target=int(context.args[0])
    except Exception:
        await update.message.reply_text("Format: /unban TELEGRAM_ID"); return
    ok=set_user_ban(target, False)
    await update.message.reply_text("Ban dicabut." if ok else "Pengguna tidak ditemukan.")


async def reward_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    try: amount=int(context.args[0]); assert amount >= 0
    except Exception:
        await update.message.reply_text("Format: /setreward JUMLAH_KOIN"); return
    set_daily_reward_amount(amount)
    await update.message.reply_text(f"Daily reward diatur menjadi {amount} koin.")


async def daily_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    if not await force_sub_guard(update, context): return
    ok, amount=claim_daily_reward(uid)
    if ok:
        row=get_user(uid)
        await update.message.reply_text(f"🎁 Daily reward +{amount} koin. Saldo: {row['balance']} koin.")
    else:
        await update.message.reply_text("Daily reward hanya untuk pengguna terdaftar dan hanya bisa diklaim sekali sehari.")


async def find_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Akses ditolak."); return
    query=" ".join(context.args).strip()
    if not query:
        await update.message.reply_text("Format: /find ID_ATAU_USERNAME_ATAU_NAMA"); return
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        if query.isdigit():
            cur.execute("SELECT * FROM users WHERE user_id=? LIMIT 10",(int(query),))
        else:
            q="%"+query.lstrip('@')+"%"
            cur.execute("SELECT * FROM users WHERE username LIKE ? OR first_name LIKE ? LIMIT 10",(q,q))
        rows=cur.fetchall(); conn.close()
    if not rows:
        await update.message.reply_text("Pengguna tidak ditemukan."); return
    lines=[]
    for row in rows:
        line=f"ID: <code>{row['user_id']}</code> | {esc(row['first_name'])} | @{esc(row['username'])} | {'Banned' if row['is_banned'] else 'Aktif'} | Menfess: {row['total_menfess']}"
        if is_owner(update.effective_user.id): line += f" | Saldo: {row['balance']} | Registrasi: {bool(row['is_registered'])}"
        lines.append(line)
    await update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.HTML)


async def support_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg=" ".join(context.args).strip()
    if not msg:
        await update.message.reply_text("Format: /support isi_laporan"); return
    with db_lock:
        conn=get_db(); cur=conn.cursor()
        cur.execute("INSERT INTO support_messages(user_id,message,created_at) VALUES(?,?,?)", (update.effective_user.id,msg,now_text()))
        ticket=cur.lastrowid; conn.commit(); conn.close()
    if OWNER_ID and BOT_APP:
        try:
            await context.bot.send_message(OWNER_ID, f"📩 Bantuan #{ticket} dari {update.effective_user.id}:\n{msg}\nBalas dengan /reply {ticket} pesan")
        except Exception: logger.exception("Gagal kirim tiket bantuan ke owner")
    await update.message.reply_text("Laporan terkirim ke owner. Terima kasih.")


async def reply_support_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Khusus owner."); return
    try: ticket=int(context.args[0]); message=" ".join(context.args[1:]).strip()
    except Exception: ticket=0; message=""
    if not ticket or not message:
        await update.message.reply_text("Format: /reply ID_TIKET pesan"); return
    with db_lock:
        conn=get_db(); cur=conn.cursor(); cur.execute("SELECT user_id FROM support_messages WHERE id=?",(ticket,)); row=cur.fetchone(); conn.close()
    if not row:
        await update.message.reply_text("Tiket tidak ditemukan."); return
    await context.bot.send_message(row['user_id'], f"💬 Balasan bantuan FWB Surabaya:\n{message}")
    await update.message.reply_text("Balasan terkirim.")


# ============================================================
# HEALTH CHECK
# ============================================================

async def health(
    request
):

    return web.json_response({
        "status": "online",
        "bot": "FWB SURABAYA",
        "database": DB_FILE,
        "time": now_text()
    })


# ============================================================
# WEBHOOK SERVER
# ============================================================

def run_webhook_server():

    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(
        loop
    )

    app = web.Application()

    app.router.add_get(
        "/",
        health
    )

    app.router.add_post(
        "/trakteer/webhook",
        trakteer_webhook
    )

    runner = web.AppRunner(
        app
    )

    async def start_server():

        await runner.setup()

        site = web.TCPSite(
            runner,
            "0.0.0.0",
            WEBHOOK_PORT
        )

        await site.start()

        logger.info(
            "Webhook server aktif di port %s",
            WEBHOOK_PORT
        )

    loop.run_until_complete(
        start_server()
    )

    loop.run_forever()


# ============================================================
# POST INIT
# ============================================================

async def post_init(
    application
):

    global BOT_LOOP, BOT_USERNAME

    BOT_LOOP = asyncio.get_running_loop()
    application.create_task(announcement_worker(application))

    me = await application.bot.get_me()
    BOT_USERNAME = me.username or ""

    # Command menu hanya tersedia di private chat.
    private_commands = [
        BotCommand("start", "🚀 Mulai bot"),
        BotCommand("status", "👤 Profile / status akun"),
        BotCommand("topup", "💰 Top up coin"),
    ]

    await application.bot.set_my_commands(
        private_commands,
        scope=BotCommandScopeAllPrivateChats()
    )

    # Hapus command menu dari semua grup.
    await application.bot.set_my_commands(
        [],
        scope=BotCommandScopeAllGroupChats()
    )

    logger.info(
        "Telegram event loop siap. Bot username: @%s",
        BOT_USERNAME
    )


# ============================================================
# TELETHON AUTO-COPY WORKER
# ============================================================

def run_telethon_worker():
    """Dynamic multi-source Telethon copier. Sources and templates are read from SQLite."""
    if not (API_ID and API_HASH and TELETHON_SESSION):
        logger.warning("Telethon belum aktif: isi API_ID, API_HASH, TELETHON_SESSION di Railway Variables.")
        return

    async def telethon_main():
        client=TelegramClient(StringSession(TELETHON_SESSION),int(API_ID),API_HASH)
        await client.start()
        resolved={}

        def read_sources():
            with db_lock:
                conn=get_db(); cur=conn.cursor(); cur.execute("SELECT source,destination,filter_mode,filter_terms FROM autocopy_sources WHERE enabled=1"); rows=cur.fetchall(); conn.close()
            return [dict(r) for r in rows]

        async def refresh_sources():
            while True:
                for row in read_sources():
                    try:
                        entity=await client.get_entity(row['source'])
                        resolved[int(utils.get_peer_id(entity))]=row
                    except Exception as exc:
                        logger.warning("Auto-copy source tidak bisa diakses %s: %s",row['source'],exc)
                # Drop removed sources from in-memory map.
                active={r['source'] for r in read_sources()}
                for peer_id,row in list(resolved.items()):
                    if row['source'] not in active: resolved.pop(peer_id,None)
                await asyncio.sleep(15)

        def passes_filter(text, mode, terms):
            if mode=='all': return True
            values=[x.strip() for x in (terms or '').split(',') if x.strip()]
            if not values: return False
            has_hash=any(re.search(r'(?<!\w)'+re.escape(v if v.startswith('#') else '#'+v)+r'\b',text,re.I) for v in values)
            has_word=any(v.lower() in text.lower() for v in values)
            if mode=='hashtag': return has_hash
            if mode=='keyword': return has_word
            if mode=='both': return has_hash or has_word
            return False

        def apply_template(text, source, destination):
            with db_lock:
                conn=get_db(); cur=conn.cursor(); cur.execute("SELECT template,position FROM autocopy_templates WHERE enabled=1 AND (source=? OR source='*') AND (destination=? OR destination='*') ORDER BY id DESC",(source,destination)); rows=cur.fetchall(); conn.close()
            result=text or ''
            # One latest applicable template only, avoid accidental stacking.
            if rows:
                template,pos=rows[0]['template'],rows[0]['position']
                result=(template+'\n\n'+result) if pos=='before' else (result+'\n\n'+template)
            return result

        async def log_copy(source, message_id, destination, status, detail=''):
            try:
                with db_lock:
                    conn=get_db(); cur=conn.cursor()
                    cur.execute("INSERT INTO autocopy_log(source,source_message_id,destination,status,detail,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT(source,source_message_id,destination) DO UPDATE SET status=excluded.status,detail=excluded.detail,created_at=excluded.created_at",(source,message_id,destination,status,str(detail)[:500],now_text()))
                    conn.commit(); conn.close()
            except Exception: logger.exception("Gagal mencatat log auto-copy")

        async def publish(messages, peer_id):
            if get_setting("autocopy_enabled", "1") != "1": return
            row=resolved.get(int(peer_id))
            if not row: return
            source=row['source']; destinations=[x.strip() for x in row['destination'].split(',') if x.strip()]; text='\n'.join(m.message or '' for m in messages).strip()
            if not text and not any(m.media for m in messages): return
            if not passes_filter(text,row['filter_mode'],row['filter_terms']): return
            for dest in destinations:
                with db_lock:
                    conn=get_db(); cur=conn.cursor(); cur.execute("SELECT 1 FROM autocopy_log WHERE source=? AND source_message_id=? AND destination=? AND status='success'",(source,messages[0].id,dest)); exists=cur.fetchone(); conn.close()
                if exists: continue
                caption=apply_template(text,source,dest)
                try:
                    destination=await client.get_entity(dest)
                    media=[m.media for m in messages if m.media]
                    if media:
                        await client.send_file(destination,media,caption=caption or None)
                    elif caption:
                        await client.send_message(destination,caption,link_preview=False)
                    await log_copy(source,messages[0].id,dest,'success','')
                    logger.info("Auto-copy berhasil %s/%s -> %s",source,messages[0].id,dest)
                except Exception as exc:
                    await log_copy(source,messages[0].id,dest,'failed',exc)
                    logger.exception("Auto-copy gagal %s/%s -> %s",source,messages[0].id,dest)

        @client.on(events.Album())
        async def album_handler(event):
            if event.messages:
                await publish(event.messages,event.chat_id)

        @client.on(events.NewMessage())
        async def message_handler(event):
            if event.message.grouped_id:
                return  # ditangani Album event
            await publish([event.message],event.chat_id)

        asyncio.create_task(refresh_sources())
        logger.info("Telethon multi-source worker aktif; konfigurasi dibaca dari SQLite.")
        await client.run_until_disconnected()

    try:
        asyncio.run(telethon_main())
    except Exception:
        logger.exception("Telethon worker berhenti karena error.")


# ============================================================
# MAIN
# ============================================================

def main():

    global BOT_APP

    if not OWNER_ID:
        raise RuntimeError("OWNER_ID wajib diisi di Railway Variables.")
    if not CHANNEL_USERNAME or not FORCE_SUB_CHANNEL:
        raise RuntimeError("CHANNEL_USERNAME dan FORCE_SUB_CHANNEL wajib diisi.")
    if not TRAKTEER_USERNAME:
        raise RuntimeError("TRAKTEER_USERNAME wajib diisi di Railway Variables.")
    if not BOT_TOKEN or BOT_TOKEN == "PASTE_NEW_BOT_TOKEN_HERE":
        raise RuntimeError(
            "BOT_TOKEN belum diset. Isi BOT_TOKEN baru atau environment variable BOT_TOKEN."
        )

    # --------------------------------------------------------
    # DATABASE
    # --------------------------------------------------------

    init_db()

    # --------------------------------------------------------
    # WEBHOOK THREAD
    # --------------------------------------------------------

    webhook_thread = threading.Thread(
        target=run_webhook_server,
        daemon=True
    )

    webhook_thread.start()

    # --------------------------------------------------------
    # TELETHON AUTO-COPY (separate thread/event loop)
    # --------------------------------------------------------

    telethon_thread = threading.Thread(
        target=run_telethon_worker,
        name="telethon-auto-copy",
        daemon=True,
    )
    telethon_thread.start()

    # --------------------------------------------------------
    # BOT
    # --------------------------------------------------------

    BOT_APP = (
        Application
        .builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .concurrent_updates(64)
        .build()
    )

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

    BOT_APP.add_handler(
        CommandHandler(
            "start",
            start_command,
            filters=filters.ChatType.PRIVATE
        )
    )

    BOT_APP.add_handler(
        CommandHandler(
            "status",
            status_command,
            filters=filters.ChatType.PRIVATE
        )
    )

    BOT_APP.add_handler(
        CommandHandler(
            "topup",
            topup_command,
            filters=filters.ChatType.PRIVATE
        )
    )
    BOT_APP.add_handler(CommandHandler("admin", admin_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("addadmin", addadmin_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("removeadmin", removeadmin_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("ban", ban_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("unban", unban_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("setreward", reward_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("daily", daily_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("find", find_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("support", support_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("reply", reply_support_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("hashtag_add", hashtag_add_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("hashtag_off", hashtag_off_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("hashtag_list", hashtag_list_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("promo", promo_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("promostats", promostats_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("autocopy_add", autocopy_add_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("autocopy_remove", autocopy_remove_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("autocopy_list", autocopy_list_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("autocopy_template", autocopy_template_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("autocopy_switch", autocopy_switch_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("autocopy_status", autocopy_status_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("autocopy_enable", autocopy_enable_source_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("settext", settext_command, filters=filters.ChatType.PRIVATE))
    BOT_APP.add_handler(CommandHandler("schedule", schedule_command, filters=filters.ChatType.PRIVATE))

    # --------------------------------------------------------
    # CALLBACK
    # --------------------------------------------------------

    BOT_APP.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    # --------------------------------------------------------
    # MENFESS
    #
    # Satu pesan langsung.
    # #fwbgirl / #fwbboy boleh di mana saja.
    # --------------------------------------------------------

    BOT_APP.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND
            & filters.ChatType.PRIVATE,
            menfess_message_handler
        )
    )

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------

    logger.info(
        "========================================"
    )

    logger.info(
        "FWB SURABAYA BOT ONLINE"
    )

    logger.info(
        "Database: %s",
        DB_FILE
    )

    logger.info(
        "Free menfess: %s / hari",
        FREE_MENFESS
    )

    logger.info(
        "Harga setelah free: %s coin",
        MENFESS_COST
    )

    logger.info(
        "Reset free: 00:01 WIB"
    )

    logger.info(
        "Webhook port: %s",
        WEBHOOK_PORT
    )

    logger.info(
        "Auto-copy legacy source=%s hashtag=%s destination=%s",
        SOURCE_CHANNEL, AUTO_COPY_HASHTAG, DEST_CHANNEL
    )

    logger.info(
        "========================================"
    )

    BOT_APP.run_polling(
        drop_pending_updates=False
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()