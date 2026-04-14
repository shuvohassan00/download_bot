"""
════════════════════════════════════════════════════════════════════════════════
🎬 ULTIMATE VIDEO DOWNLOADER BOT — PROFESSIONAL EDITION v8.1 ULTRA
════════════════════════════════════════════════════════════════════════════════
⚡ ADVANCED FEATURES:
  ✅ Portable FFMPEG (4K/1080p Audio Merge)
  ✅ Real-Time Download Progress Bar (yt-dlp hooks) — FIXED: Now actually works!
  ✅ Video Info Preview (Thumbnail + Details Before Download)
  ✅ Smart Quality Selection (4K > 1080p > 720p > 480p > 360p > Audio)
  ✅ Auto yt-dlp Updater
  ✅ Auto Storage Cleanup (Stale File Remover)
  ✅ Concurrent Download Limiter (Max 3 Simultaneous)
  ✅ Banned User Check (Block Before Processing) + Ban Reason & Appeal
  ✅ Download Rate Limiting (Per-Hour Enforcement)
  ✅ Cancel Download Mid-Process — FIXED: No more double answer()
  ✅ Video Info Extraction (/info command)
  ✅ Download History (/history command)
  ✅ Smart URL Cleaner (Remove Tracking Params)
  ✅ File Size Pre-Check (Estimate Before Download)
  ✅ Complete Admin Dashboard & User Profiles
  ✅ Admin: User Lookup, Clear Stats, View Recent Downloads
  ✅ Admin: Live Settings Editor
  ✅ Admin: Mass Ban/Unban
  ✅ Premium User System with Higher Limits
  ✅ Audio Metadata Embedding (Title/Artist/Album/Year/Genre)
  ✅ MP3/WAV/M4A/OPUS Audio Format Selection
  ✅ TikTok No-Watermark Extraction
  ✅ YouTube Shorts Auto-Detection
  ✅ Multi-Platform: YouTube, TikTok, Instagram, Facebook, Twitter & 1000+
  ✅ Smart Error Handling & Auto-Retry — NEW: Up to 2 retries
  ✅ Bot Uptime Tracker
  ✅ Premium Animated UI with Braille Progress Spinners — NEW: ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
  ✅ Smart File Size Warning (>40MB alert) — NEW
  ✅ Download Queue System with Position Display — NEW
  ✅ Auto-Thumbnail During Info Fetch — NEW
  ✅ Enhanced Platform-Specific Error Messages — NEW
  ✅ Subtitle Download Button — NEW
  ✅ Better Group Chat Support — NEW
  ✅ Download Speed Display in Caption — NEW
  ✅ Session Timeout (10 min auto-expire) — NEW
  ✅ No-Audio Track Handling — FIXED
  ✅ SQL Injection Protection in LIKE Queries — FIXED
  ✅ Proper File.download() for python-telegram-bot v20+ — FIXED
  ✅ Custom Cancel Exception — FIXED (DownloadCancelled may not exist)
  ✅ Quiet yt-dlp Base Options — FIXED
  ✅ No Dead Imports — FIXED (json, as_completed removed)
  ✅ .part/.temp File Filtering — FIXED

👨‍💻 Developed by: GADGET HOST BOT
════════════════════════════════════════════════════════════════════════════════
"""

import logging
import os
import time
import asyncio
import sqlite3
import re
import hashlib
import shutil
import sys
import subprocess
import threading
import traceback
import random
import itertools
import platform
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from collections import defaultdict, deque
from urllib.parse import urlparse, parse_qs, urlencode, unquote
from typing import Optional, Tuple, List, Dict, Any

# ════════════════════════════════════════════════════════════════════════════
# 🚀 AUTO-INSTALLER & DEPENDENCY MANAGER
# ════════════════════════════════════════════════════════════════════════════

REQUIRED_PACKAGES: List[str] = ["yt-dlp", "imageio-ffmpeg", "python-telegram-bot"]


def install_packages() -> None:
    """Install missing Python packages automatically."""
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            print(f"\u23f3 Installing {pkg}...")
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-q", pkg],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )


try:
    import yt_dlp
    import imageio_ffmpeg
    from telegram import (
        Update, InlineKeyboardMarkup, InlineKeyboardButton,
        InputMediaPhoto, InputMediaVideo
    )
    from telegram.constants import ParseMode, ChatAction
    from telegram.error import BadRequest
    from telegram.ext import (
        ApplicationBuilder, ContextTypes, CommandHandler,
        MessageHandler, CallbackQueryHandler, filters
    )
except ImportError as e:
    print("\u2550" * 70)
    print(f"\u274c MISSING LIBRARIES: {e}")
    print("\u23f3 Auto-installing required packages. Please wait...")
    try:
        install_packages()
        print("\u2705 Packages installed! RESTARTING SCRIPT NOW...")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as inst_err:
        print(f"\u274c Auto-install failed: {inst_err}")
        exit(1)

# Check for DownloadCancelled attribute at module level
HAS_DOWNLOAD_CANCELLED = hasattr(yt_dlp.utils, 'DownloadCancelled')

# Custom download cancelled exception (always available)
class DownloadCancelledException(Exception):
    """Raised when a user cancels a download."""
    pass

# ════════════════════════════════════════════════════════════════════════════
# ⚙️ BOT CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "8357894547:AAHfzKXDPbWdlpeAIloCMsAKbvMzOpfzp2Q").strip()
ADMIN_ID: int = int(os.getenv("ADMIN_ID", "7857957075"))
BOT_USERNAME: str = os.getenv("BOT_USERNAME", "@gadgetmedia_bot").strip()
BUILD_VERSION: str = "10.3 NEON DELUXE"
BUILD_LABEL: str = "Neon premium UI • safer audio/video engine • smarter quality handling"

# System Directories
DOWNLOAD_DIR: str = "downloads"
DB_NAME: str = "ultrabot_v8.db"
COOKIES_FILE: str = "cookies.txt"
BACKUP_DIR: str = "backups"
LOGS_DIR: str = "logs"
TEMP_DIR: str = "temp"

# Rate Limiting Settings
RATE_LIMIT_MESSAGES: int = 20
RATE_LIMIT_DOWNLOADS: int = 15
RATE_LIMIT_DOWNLOADS_PREMIUM: int = 100

# Telegram Bot API max file size
MAX_BOT_FILE_SIZE_MB: int = 49
MAX_PREMIUM_FILE_SIZE_MB: int = 49

# Concurrent download limit
MAX_CONCURRENT_DOWNLOADS: int = 3

# File cleanup settings
MAX_FILE_AGE_SECONDS: int = 3600
CLEANUP_INTERVAL: int = 300

# v8.0: Session timeout (seconds)
SESSION_TIMEOUT_SECONDS: int = 600  # 10 minutes
DOWNLOAD_SESSION_KEEP_SECONDS: int = 1800  # keep inline button sessions alive a bit longer
QUEUE_STALE_SECONDS: int = 1800  # 30 minutes stale queue cleanup

# v8.0: Smart file size warning threshold
FILE_SIZE_WARN_MB: int = 40

# v8.0: Max retries for failed downloads
MAX_DOWNLOAD_RETRIES: int = 2

# v8.0: Login URL config (for users to get 2GB upload via Telegram web)
# Users can visit https://t.me/BotUsername?startattach=... to send larger files
LOGIN_URL_SUPPORTED: bool = True

# Thread pool for heavy downloads
pool = ThreadPoolExecutor(max_workers=4)

# Bot uptime tracking
BOT_START_TIME: float = time.time()

# Active downloads tracking (for cancel support)
# v8.0: Now includes progress_data dict for REAL-TIME progress (THE FIX)
active_downloads: Dict[int, Dict[str, Any]] = {}
active_downloads_lock = threading.Lock()

# Waiting queue with priority lanes
queued_downloads: Dict[int, Dict[str, Any]] = {}
queued_downloads_lock = threading.Lock()
queue_ticket_counter = itertools.count(1)

# v8.0: Download queue system
download_queue: deque = deque()
download_queue_lock = threading.Lock()

# Download concurrency semaphore
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Braille spinner frames for animated progress
BRAILLE_FRAMES: List[str] = ["\u284b", "\u2819", "\u2839", "\u2838",
                              "\u283c", "\u2834", "\u2826", "\u2827",
                              "\u2807", "\u280f", "\u281f", "\u283f",
                              "\u2839", "\u2819", "\u280b", "\u2819"]

# Pre-defined emoji strings (avoid backslash in f-string for Python 3.8+ compat)
MAINT_ON = "\U0001f534 ON"
MAINT_OFF = "\U0001f7e2 OFF"
PREM_YES = "\U0001f451 Premium"
PREM_NO = "\U0001f195 Free"
PREM_ENABLE = "\U0001f451 PREMIUM ENABLED \u2705"
PREM_DISABLE = "\U0001f195 PREMIUM DISABLED \u274c"
BANNED_YES = "\U0001f6ab USER BANNED \u274c"
UNBANNED_YES = "\u2705 USER UNBANNED"
SHORTS_TAG = " \U0001faa3 YouTube Shorts detected!"

# Ensure directories exist
for directory in [DOWNLOAD_DIR, BACKUP_DIR, LOGS_DIR, TEMP_DIR]:
    os.makedirs(directory, exist_ok=True)

# ════════════════════════════════════════════════════════════════════════════
# 📝 LOGGING SETUP
# ════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "bot.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════════════════
# 🔧 AUTO YT-DLP UPDATER
# ════════════════════════════════════════════════════════════════════════════

def auto_update_ytdlp() -> None:
    """Check and update yt-dlp to latest version on startup."""
    try:
        print("\U0001f504 Checking yt-dlp version...")
        current = subprocess.check_output(
            [sys.executable, "-m", "pip", "show", "yt-dlp"],
            stderr=subprocess.DEVNULL
        ).decode()
        for line in current.split('\n'):
            if line.startswith('Version:'):
                print(f"   \u2705 yt-dlp {line.split(':')[1].strip()} installed")
                break

        print("\U0001f504 Checking for yt-dlp updates...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "--upgrade", "yt-dlp"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            print("   \u2705 yt-dlp is up to date!")
        else:
            print(f"   \u26a0\ufe0f Update check failed: {result.stderr[:100]}")
    except Exception as e:
        print(f"   \u26a0\ufe0f Auto-update check error: {e}")


# ════════════════════════════════════════════════════════════════════════════
# 🛡️ ADVANCED RATE LIMITER (with download tracking)
# ════════════════════════════════════════════════════════════════════════════

class AdvancedRateLimiter:
    """Thread-safe rate limiter for messages and downloads."""

    def __init__(self) -> None:
        self.user_msgs: Dict[int, deque] = defaultdict(deque)
        self.user_downloads: Dict[int, deque] = defaultdict(deque)
        self.lock = threading.Lock()

    def _prune_downloads(self, user_id: int) -> None:
        now = time.time()
        while self.user_downloads[user_id] and now - self.user_downloads[user_id][0] > 3600:
            self.user_downloads[user_id].popleft()

    def check_msg_limit(self, user_id: int) -> bool:
        """Check if user is within message rate limit."""
        with self.lock:
            now = time.time()
            while self.user_msgs[user_id] and now - self.user_msgs[user_id][0] > 60:
                self.user_msgs[user_id].popleft()
            if len(self.user_msgs[user_id]) >= RATE_LIMIT_MESSAGES:
                return False
            self.user_msgs[user_id].append(now)
            return True

    def can_download(self, user_id: int, is_premium: bool = False) -> bool:
        """Check the hourly download allowance without consuming it."""
        with self.lock:
            self._prune_downloads(user_id)
            limit = RATE_LIMIT_DOWNLOADS_PREMIUM if is_premium else RATE_LIMIT_DOWNLOADS
            return len(self.user_downloads[user_id]) < limit

    def record_download(self, user_id: int) -> None:
        """Consume one download slot for the current hour."""
        with self.lock:
            self._prune_downloads(user_id)
            self.user_downloads[user_id].append(time.time())

    def refund_download(self, user_id: int) -> None:
        """Return the most recent slot when a download never really started."""
        with self.lock:
            self._prune_downloads(user_id)
            if self.user_downloads[user_id]:
                self.user_downloads[user_id].pop()

    def check_download_limit(self, user_id: int, is_premium: bool = False) -> bool:
        """Backward-compatible combined check+record helper."""
        if not self.can_download(user_id, is_premium):
            return False
        self.record_download(user_id)
        return True

    def get_remaining_downloads(self, user_id: int, is_premium: bool = False) -> int:
        """Get remaining downloads for user this hour."""
        with self.lock:
            self._prune_downloads(user_id)
            limit = RATE_LIMIT_DOWNLOADS_PREMIUM if is_premium else RATE_LIMIT_DOWNLOADS
            return max(0, limit - len(self.user_downloads[user_id]))


rate_limiter = AdvancedRateLimiter()

# ════════════════════════════════════════════════════════════════════════════
# 🧹 AUTO CLEANUP SYSTEM (v8 — More aggressive on startup)
# ════════════════════════════════════════════════════════════════════════════

class StorageCleaner:
    """Periodically removes stale downloaded files to prevent disk full."""

    @staticmethod
    def clean_user_temp_files(user_id: int) -> int:
        """Remove stale temp/download leftovers for a specific user session."""
        removed = 0
        prefixes = (f"{user_id}_", f"thumb_{user_id}_", f"sub_{user_id}_")
        try:
            for directory in [DOWNLOAD_DIR, TEMP_DIR]:
                if not os.path.exists(directory):
                    continue
                for fname in os.listdir(directory):
                    if not fname.startswith(prefixes):
                        continue
                    fpath = os.path.join(directory, fname)
                    try:
                        if os.path.isfile(fpath):
                            os.remove(fpath)
                            removed += 1
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"User temp cleanup error for {user_id}: {e}")
        return removed

    @staticmethod
    def clean_old_files() -> int:
        """Remove files older than MAX_FILE_AGE_SECONDS. Returns count removed."""
        try:
            now = time.time()
            removed = 0
            for directory in [DOWNLOAD_DIR, TEMP_DIR]:
                if not os.path.exists(directory):
                    continue
                for fname in os.listdir(directory):
                    fpath = os.path.join(directory, fname)
                    try:
                        if os.path.isfile(fpath):
                            age = now - os.path.getmtime(fpath)
                            if age > MAX_FILE_AGE_SECONDS:
                                os.remove(fpath)
                                removed += 1
                    except Exception:
                        pass
            if removed > 0:
                logger.info(f"\U0001f9f9 Cleaned {removed} stale files")
            return removed
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            return 0

    @staticmethod
    def aggressive_startup_cleanup() -> int:
        """v8.0: More aggressive cleanup on startup — remove ALL temp files."""
        try:
            removed = 0
            for directory in [DOWNLOAD_DIR, TEMP_DIR]:
                if not os.path.exists(directory):
                    continue
                for fname in os.listdir(directory):
                    fpath = os.path.join(directory, fname)
                    try:
                        if os.path.isfile(fpath):
                            os.remove(fpath)
                            removed += 1
                    except Exception:
                        pass
            if removed > 0:
                logger.info(f"\U0001f9f9 Startup cleanup: removed {removed} files")
            return removed
        except Exception as e:
            logger.error(f"Startup cleanup error: {e}")
            return 0

    @staticmethod
    def get_disk_usage_mb() -> float:
        """Get total size of download/temp directories in MB."""
        total = 0
        for directory in [DOWNLOAD_DIR, TEMP_DIR]:
            if not os.path.exists(directory):
                continue
            for fname in os.listdir(directory):
                fpath = os.path.join(directory, fname)
                try:
                    if os.path.isfile(fpath):
                        total += os.path.getsize(fpath)
                except Exception:
                    pass
        return total / (1024 * 1024)

    @staticmethod
    async def start_periodic_cleanup(app=None) -> None:
        """Background task for periodic cleanup."""
        while True:
            try:
                await asyncio.sleep(CLEANUP_INTERVAL)
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(pool, StorageCleaner.clean_old_files)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic cleanup error: {e}")


storage_cleaner = StorageCleaner()

# ════════════════════════════════════════════════════════════════════════════
# 🔗 SMART URL UTILITIES
# ════════════════════════════════════════════════════════════════════════════

class URLUtils:
    """Smart URL detection, cleaning, and platform identification."""

    PLATFORMS: Dict[str, List[str]] = {
        'youtube':    [r'youtube\.com', r'youtu\.be'],
        'tiktok':     [r'tiktok\.com'],
        'instagram':  [r'instagram\.com'],
        'facebook':   [r'facebook\.com', r'fb\.watch', r'fb\.com'],
        'twitter':    [r'twitter\.com', r'x\.com'],
        'pinterest':  [r'pinterest\.com', r'pin\.it'],
        'reddit':     [r'reddit\.com', r'redd\.it'],
        'vimeo':      [r'vimeo\.com'],
        'dailymotion': [r'dailymotion\.com'],
        'twitch':     [r'twitch\.tv'],
        'soundcloud': [r'soundcloud\.com'],
        'snapchat':   [r'snapchat\.com'],
        'rumble':     [r'rumble\.com'],
        'bilibili':   [r'bilibili\.com'],
    }

    TRACKING_PARAMS: List[str] = [
        'fbclid', 'gclid', 'utm_source', 'utm_medium', 'utm_campaign',
        'utm_term', 'utm_content', 'ref', 'si', 'feature', 'share_id',
        'igshid', 'fb_action_ids', 'fb_action_types', 'fb_source',
    ]

    @staticmethod
    def _strip_invisible(text: str) -> str:
        """Remove zero-width and non-printing characters that often break copied links."""
        if not text:
            return ""
        for ch in ("\u200b", "\u200c", "\u200d", "\ufeff", "\u2060"):
            text = text.replace(ch, "")
        return text.strip()

    @staticmethod
    def detect_url(text: str) -> Optional[str]:
        """Extract first valid URL from text and trim common copy-paste punctuation."""
        cleaned_text = URLUtils._strip_invisible(text or "")
        match = re.search(r'(https?://|www\.)[^\s<>\[\]{}"]+', cleaned_text, flags=re.IGNORECASE)
        if not match:
            return None
        url = match.group(0).strip().rstrip(").,!?]}>\"'")
        if url.lower().startswith('www.'):
            url = f"https://{url}"
        return url

    @staticmethod
    def _unwrap_redirect_url(url: str) -> str:
        """Extract the original target from common social redirect/share URLs."""
        try:
            raw = (url or "").strip()
            parsed = urlparse(raw)
            netloc = (parsed.netloc or '').lower()
            params = parse_qs(parsed.query, keep_blank_values=False)
            for key in ('u', 'url', 'target', 'link'):
                candidate = (params.get(key) or [None])[0]
                if not candidate:
                    continue
                candidate = unquote(candidate)
                if candidate.startswith('http://') or candidate.startswith('https://'):
                    return candidate
            if netloc in ('l.facebook.com', 'lm.facebook.com', 'l.instagram.com'):
                for key in ('u', 'url'):
                    candidate = (params.get(key) or [None])[0]
                    if candidate:
                        candidate = unquote(candidate)
                        if candidate.startswith('http://') or candidate.startswith('https://'):
                            return candidate
        except Exception:
            pass
        return url

    @staticmethod
    def clean_url(url: str) -> str:
        """Remove tracking parameters and return a safer canonical URL string."""
        try:
            raw_url = URLUtils._strip_invisible(url or "").rstrip(").,!?]}>\"'")
            if raw_url.lower().startswith('www.'):
                raw_url = f"https://{raw_url}"
            url = URLUtils._unwrap_redirect_url(raw_url)
            parsed = urlparse(url)
            params = parse_qs(parsed.query, keep_blank_values=False)
            clean_params = {
                k: v for k, v in params.items()
                if k.lower() not in URLUtils.TRACKING_PARAMS
            }
            scheme = parsed.scheme or 'https'
            netloc = parsed.netloc.lower()
            if netloc.startswith('m.'):
                netloc = netloc[2:]
            if netloc.startswith('www.'):
                netloc = netloc[4:]
            if netloc in ('music.youtube.com', 'gaming.youtube.com'):
                netloc = 'youtube.com'
            if netloc == 'mobile.twitter.com':
                netloc = 'x.com'
            path = parsed.path or ''
            if netloc == 'youtu.be' and path.strip('/'):
                return f"https://youtu.be/{path.strip('/')}"
            query = urlencode(clean_params, doseq=True)
            return f"{scheme}://{netloc}{path}" + (f"?{query}" if query else "")
        except Exception:
            return URLUtils._strip_invisible(url or "")


    @staticmethod
    def normalize_platform_url(url: str) -> str:
        """Normalize copied platform URLs into cleaner direct URLs."""
        try:
            url = URLUtils.clean_url(URLUtils._unwrap_redirect_url(url))
            parsed = urlparse(url)
            scheme = parsed.scheme or 'https'
            netloc = (parsed.netloc or '').lower()
            path = parsed.path or ''
            query = parse_qs(parsed.query)

            # normalize common host variants
            if netloc.startswith('m.'):
                netloc = netloc[2:]
            if netloc.startswith('www.'):
                netloc = netloc[4:]
            if netloc in ('music.youtube.com', 'gaming.youtube.com'):
                netloc = 'youtube.com'
            if netloc == 'mobile.twitter.com':
                netloc = 'x.com'

            # youtu.be short links
            if netloc == 'youtu.be' and path.strip('/'):
                return f"{scheme}://youtu.be/{path.strip('/')}"

            # youtube watch/shorts/live
            if 'youtube.com' in netloc:
                video_id = (query.get('v') or [None])[0]
                if '/shorts/' in path:
                    short_id = path.split('/shorts/', 1)[1].strip('/').split('/', 1)[0]
                    if short_id:
                        return f"{scheme}://youtube.com/shorts/{short_id}"
                if '/live/' in path:
                    live_id = path.split('/live/', 1)[1].strip('/').split('/', 1)[0]
                    if live_id:
                        return f"{scheme}://youtube.com/watch?v={live_id}"
                if video_id:
                    return f"{scheme}://youtube.com/watch?v={video_id}"
                return f"{scheme}://youtube.com{path}"

            # instagram canonical public paths
            if 'instagram.com' in netloc:
                for prefix in ('/reel/', '/p/', '/tv/'):
                    if prefix in path:
                        slug = path.split(prefix, 1)[1].strip('/').split('/', 1)[0]
                        if slug:
                            return f"{scheme}://instagram.com{prefix}{slug}/"
                return f"{scheme}://instagram.com{path}"

            # facebook canonical public paths
            if 'facebook.com' in netloc or 'fb.watch' in netloc or netloc == 'fb.com':
                if 'story_fbid' in query and 'id' in query:
                    return f"{scheme}://facebook.com/story.php?story_fbid={query['story_fbid'][0]}&id={query['id'][0]}"
                if netloc == 'fb.watch':
                    slug = path.strip('/').split('/', 1)[0]
                    if slug:
                        return f"{scheme}://fb.watch/{slug}/"
                return f"{scheme}://facebook.com{path}"

            # twitter/x canonicalize to x.com
            if netloc in ('twitter.com', 'x.com'):
                if path.startswith('/i/status/'):
                    status_id = path.split('/i/status/', 1)[1].strip('/').split('/', 1)[0]
                    if status_id:
                        return f"{scheme}://x.com/i/status/{status_id}"
                return f"{scheme}://x.com{path}"

            # tiktok canonical path
            if netloc in ('vt.tiktok.com', 'vm.tiktok.com'):
                slug = path.strip('/').split('/', 1)[0]
                if slug:
                    return f"{scheme}://{netloc}/{slug}/"
                return f"{scheme}://{netloc}{path}"
            if 'tiktok.com' in netloc:
                return f"{scheme}://tiktok.com{path}" + (f"?{parsed.query}" if parsed.query else "")

            return f"{scheme}://{netloc}{path}" + (f"?{parsed.query}" if parsed.query else "")
        except Exception:
            return URLUtils.clean_url(url)

    @staticmethod
    def detect_platform(url: str) -> str:
        """Detect platform from URL."""
        url_lower = url.lower()
        for platform, patterns in URLUtils.PLATFORMS.items():
            for pattern in patterns:
                if re.search(pattern, url_lower):
                    return platform.capitalize()
        return "Unknown"

    @staticmethod
    def is_youtube_shorts(url: str) -> bool:
        """Check if URL is YouTube Shorts."""
        return 'youtube.com/shorts' in url.lower()

    @staticmethod
    def is_playlist(url: str) -> bool:
        """Check if URL is a playlist."""
        playlist_indicators = ['playlist', 'list=', 'play_list']
        return any(ind in url.lower() for ind in playlist_indicators)

    @staticmethod
    def has_subtitles(info_dict: dict) -> bool:
        """v8.0: Check if video has subtitles available."""
        if not info_dict:
            return False
        subtitles = info_dict.get('subtitles', {})
        auto_captions = info_dict.get('automatic_captions', {})
        return bool(subtitles or auto_captions)

    @staticmethod
    def get_share_url_warning(url: str) -> Optional[str]:
        """Detect common share/redirect URLs that yt-dlp often cannot resolve reliably."""
        try:
            parsed = urlparse(url)
            netloc = parsed.netloc.lower()
            path = parsed.path.lower()
            query = parsed.query.lower()

            if netloc in ('l.facebook.com', 'lm.facebook.com', 'l.instagram.com'):
                return (
                    "⚠️ <b>Redirect/share link detected!</b>\n"
                    "Open the original post first, then copy the final public URL instead of the redirect link."
                )
            if 'facebook.com' in netloc or 'fb.com' in netloc or 'fb.watch' in netloc:
                if any(part in path for part in ('/share/', '/sharer', '/dialog/share', '/share.php', '/l.php')) or 'u=' in query:
                    return (
                        "⚠️ <b>Facebook share link detected!</b>\n"
                        "Open the post or reel first, then send the direct public post URL."
                    )
            if 'instagram.com' in netloc:
                if '/share/' in path or path.startswith('/share/reel') or path.startswith('/share/p/'):
                    return (
                        "⚠️ <b>Instagram share link detected!</b>\n"
                        "Open the reel or post first, then send the direct public Instagram URL."
                    )
            if netloc in ('vt.tiktok.com', 'vm.tiktok.com'):
                return None
        except Exception:
            return None
        return None


url_utils = URLUtils()

# ════════════════════════════════════════════════════════════════════════════
# 💾 ADVANCED DATABASE SYSTEM (v8 — SQL Injection Safe, Ban Reasons, etc.)
# ════════════════════════════════════════════════════════════════════════════

class UltraDatabase:
    """Thread-safe SQLite database for bot persistence."""

    def __init__(self) -> None:
        self.conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.lock = threading.Lock()
        self.create_tables()
        self.insert_defaults()

    def create_tables(self) -> None:
        """Create all database tables."""
        with self.lock:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    joined_date TIMESTAMP,
                    last_active TIMESTAMP,
                    is_banned INTEGER DEFAULT 0,
                    ban_reason TEXT DEFAULT '',
                    ban_date TIMESTAMP,
                    is_premium INTEGER DEFAULT 0,
                    download_count INTEGER DEFAULT 0,
                    total_size_downloaded INTEGER DEFAULT 0,
                    language TEXT DEFAULT 'en',
                    default_quality TEXT DEFAULT 'best'
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    description TEXT,
                    category TEXT
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    platform TEXT,
                    video_url TEXT,
                    video_title TEXT,
                    quality TEXT,
                    file_size INTEGER,
                    processing_time REAL,
                    timestamp TIMESTAMP,
                    status TEXT DEFAULT 'success'
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id INTEGER PRIMARY KEY,
                    preferred_quality TEXT DEFAULT 'best',
                    preferred_audio_format TEXT DEFAULT 'mp3',
                    auto_download INTEGER DEFAULT 0,
                    show_info INTEGER DEFAULT 1,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS economy (
                    user_id INTEGER PRIMARY KEY,
                    coins INTEGER DEFAULT 0,
                    daily_streak INTEGER DEFAULT 0,
                    last_daily_claim TEXT,
                    last_spin TEXT,
                    total_audio_downloads INTEGER DEFAULT 0,
                    total_video_downloads INTEGER DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS coin_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount INTEGER,
                    reason TEXT,
                    created_at TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS entitlements (
                    user_id INTEGER PRIMARY KEY,
                    premium_until TEXT,
                    queue_passes INTEGER DEFAULT 0,
                    total_queue_passes_used INTEGER DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )
            """)
            self.conn.commit()

    def insert_defaults(self) -> None:
        """Insert default settings if they don't exist."""
        with self.lock:
            defaults: Dict[str, Tuple[str, str, str]] = {
                "bot_status":           ("\u2705 ONLINE & FAST",                           "Current bot status",              "general"),
                "maintenance":          ("0",                                              "Maintenance Mode (1=ON, 0=OFF)", "general"),
                "welcome_image":        ("https://i.imgur.com/X8Z9kPm.jpg",                "Welcome Banner URL",              "ui"),
                "allow_quality_select": ("1",                                              "Show quality buttons",            "features"),
                "cookies_active":       ("0",                                              "Cookies loaded",                  "system"),
                "show_video_info":      ("1",                                              "Show video info before download", "features"),
                "auto_update_ytdlp":    ("1",                                              "Auto update yt-dlp on startup",   "system"),
                "max_concurrent":       ("3",                                              "Max concurrent downloads",        "system"),
                "audio_quality":        ("320",                                            "Default audio bitrate (kbps)",    "features"),
                "rewards_enabled":      ("1",                                              "Enable coins / daily / spin",    "features"),
                "coin_shop_queue_cost": ("60",                                             "Coins needed for one priority queue pass", "economy"),
                "coin_shop_day_cost":   ("250",                                            "Coins needed for 1 day premium", "economy"),
                "coin_shop_3day_cost":  ("600",                                            "Coins needed for 3 days premium", "economy"),
            }
            for k, (v, desc, cat) in defaults.items():
                self.cursor.execute(
                    "INSERT OR IGNORE INTO settings (key, value, description, category) VALUES (?, ?, ?, ?)",
                    (k, v, desc, cat)
                )
            self.conn.commit()

    def add_user(self, user) -> None:
        """Register or update a user in the database."""
        try:
            with self.lock:
                self.cursor.execute(
                    "INSERT OR IGNORE INTO users (user_id, username, full_name, joined_date, last_active) VALUES (?, ?, ?, ?, ?)",
                    (user.id, user.username, user.full_name, datetime.now().isoformat(), datetime.now().isoformat())
                )
                self.cursor.execute(
                    "UPDATE users SET last_active=?, username=?, full_name=? WHERE user_id=?",
                    (datetime.now().isoformat(), user.username, user.full_name, user.id)
                )
                self.cursor.execute(
                    "INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)",
                    (user.id,)
                )
                self.cursor.execute(
                    "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                    (user.id,)
                )
                self.cursor.execute(
                    "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                    (user.id,)
                )
                self.conn.commit()
        except Exception as e:
            logger.error(f"DB User Error: {e}")

    def is_banned(self, user_id: int) -> bool:
        """Check if user is banned."""
        try:
            with self.lock:
                res = self.cursor.execute(
                    "SELECT is_banned FROM users WHERE user_id=?", (user_id,)
                ).fetchone()
                return bool(res and res[0] == 1)
        except Exception:
            return False

    def get_ban_info(self, user_id: int) -> Optional[Dict[str, Any]]:
        """v8.0: Get ban reason and date for a banned user."""
        try:
            with self.lock:
                res = self.cursor.execute(
                    "SELECT ban_reason, ban_date FROM users WHERE user_id=? AND is_banned=1",
                    (user_id,)
                ).fetchone()
                if res:
                    return {"reason": res[0] or "No reason specified", "date": res[1] or "Unknown"}
                return None
        except Exception:
            return None

    def is_premium(self, user_id: int) -> bool:
        """Check if user has premium status, including temporary premium from coin shop."""
        try:
            with self.lock:
                res = self.cursor.execute(
                    "SELECT is_premium FROM users WHERE user_id=?", (user_id,)
                ).fetchone()
                if res and res[0] == 1:
                    return True
                self.cursor.execute(
                    "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                    (user_id,)
                )
                ent = self.cursor.execute(
                    "SELECT premium_until FROM entitlements WHERE user_id=?", (user_id,)
                ).fetchone()
                if ent and ent[0]:
                    try:
                        return datetime.fromisoformat(ent[0]) > datetime.utcnow()
                    except Exception:
                        return False
                return False
        except Exception:
            return False

    def log_download(self, user_id: int, platform: str, url: str, quality: str,
                     size: int, processing_time: float, title: str = "Unknown",
                     status: str = "success") -> None:
        """Log a download event."""
        try:
            with self.lock:
                self.cursor.execute(
                    "INSERT INTO downloads (user_id, platform, video_url, video_title, quality, file_size, processing_time, timestamp, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (user_id, platform, url, title[:200], quality, size, processing_time, datetime.now().isoformat(), status)
                )
                if status == "success":
                    self.cursor.execute(
                        "UPDATE users SET download_count = download_count + 1, total_size_downloaded = total_size_downloaded + ? WHERE user_id = ?",
                        (size, user_id)
                    )
                    self.cursor.execute(
                        "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                        (user_id,)
                    )
                    is_audio = str(quality).lower() in ('audio', 'mp3', 'm4a', 'opus', 'wav')
                    reward = 3 if is_audio else 5
                    self.cursor.execute(
                        "UPDATE economy SET coins = coins + ?, total_audio_downloads = total_audio_downloads + ?, total_video_downloads = total_video_downloads + ? WHERE user_id = ?",
                        (reward, 1 if is_audio else 0, 0 if is_audio else 1, user_id)
                    )
                    self.cursor.execute(
                        "INSERT INTO coin_transactions (user_id, amount, reason, created_at) VALUES (?, ?, ?, ?)",
                        (user_id, reward, 'download reward', datetime.now().isoformat())
                    )
                self.conn.commit()
        except Exception as e:
            logger.error(f"DB Log Error: {e}")

    def get_setting(self, key: str) -> Optional[str]:
        """Get a bot setting value."""
        try:
            with self.lock:
                res = self.cursor.execute(
                    "SELECT value FROM settings WHERE key=?", (key,)
                ).fetchone()
                return res[0] if res else None
        except Exception:
            return None

    def set_setting(self, key: str, value: str) -> None:
        """Update a bot setting value."""
        with self.lock:
            self.cursor.execute(
                "UPDATE settings SET value=? WHERE key=?", (str(value), key)
            )
            self.conn.commit()

    def get_stats(self) -> Dict[str, Any]:
        """Get overall bot statistics."""
        with self.lock:
            users   = self.cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            dls     = self.cursor.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]
            data    = self.cursor.execute(
                "SELECT SUM(total_size_downloaded) FROM users"
            ).fetchone()[0] or 0
            premium = self.cursor.execute(
                "SELECT COUNT(*) FROM users WHERE is_premium=1"
            ).fetchone()[0]
            banned  = self.cursor.execute(
                "SELECT COUNT(*) FROM users WHERE is_banned=1"
            ).fetchone()[0]
            today_dls = self.cursor.execute(
                "SELECT COUNT(*) FROM downloads WHERE timestamp >= ?",
                (datetime.now().replace(hour=0, minute=0, second=0).isoformat(),)
            ).fetchone()[0]
            return {
                'users': users, 'downloads': dls, 'data_mb': data / 1024 / 1024,
                'premium': premium, 'banned': banned, 'today_downloads': today_dls
            }

    def get_user_stats(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific user."""
        with self.lock:
            res = self.cursor.execute(
                "SELECT download_count, total_size_downloaded, is_premium, joined_date "
                "FROM users WHERE user_id=?",
                (user_id,)
            ).fetchone()
            if not res:
                return None
            self.cursor.execute(
                "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                (user_id,)
            )
            econ = self.cursor.execute(
                "SELECT coins, daily_streak, last_daily_claim, last_spin, total_audio_downloads, total_video_downloads FROM economy WHERE user_id=?",
                (user_id,)
            ).fetchone()
            self.cursor.execute(
                "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                (user_id,)
            )
            ent = self.cursor.execute(
                "SELECT premium_until, queue_passes FROM entitlements WHERE user_id=?",
                (user_id,)
            ).fetchone()
            temp_premium = None
            temp_active = False
            if ent and ent[0]:
                temp_premium = ent[0]
                try:
                    temp_active = datetime.fromisoformat(ent[0]) > datetime.utcnow()
                except Exception:
                    temp_active = False
            return {
                'downloads': res[0] or 0,
                'total_size': res[1] or 0,
                'is_premium': bool(res[2]) or temp_active,
                'joined': res[3],
                'coins': (econ[0] if econ else 0) or 0,
                'daily_streak': (econ[1] if econ else 0) or 0,
                'last_daily_claim': econ[2] if econ else None,
                'last_spin': econ[3] if econ else None,
                'audio_downloads': (econ[4] if econ else 0) or 0,
                'video_downloads': (econ[5] if econ else 0) or 0,
                'premium_until': temp_premium,
                'queue_passes': int((ent[1] if ent else 0) or 0),
            }

    def get_user_history(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get download history for a user."""
        with self.lock:
            rows = self.cursor.execute(
                "SELECT * FROM downloads WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_all_users(self) -> List[int]:
        """Get all non-banned user IDs."""
        with self.lock:
            return [row[0] for row in self.cursor.execute(
                "SELECT user_id FROM users WHERE is_banned=0"
            ).fetchall()]

    def search_user(self, query: str) -> List[Dict[str, Any]]:
        """Search user by ID or username. v8: SQL injection safe."""
        with self.lock:
            try:
                uid = int(query)
                row = self.cursor.execute(
                    "SELECT * FROM users WHERE user_id=?", (uid,)
                ).fetchone()
                return [dict(row)] if row else []
            except ValueError:
                # v8 FIX: Escape LIKE wildcards to prevent SQL injection
                safe_query = query.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
                rows = self.cursor.execute(
                    "SELECT * FROM users WHERE username LIKE ? ESCAPE '\\' OR full_name LIKE ? ESCAPE '\\' LIMIT 10",
                    (f"%{safe_query}%", f"%{safe_query}%")
                ).fetchall()
                return [dict(r) for r in rows]

    def clear_user_stats(self, user_id: int) -> None:
        """Reset download count and total size for a user."""
        with self.lock:
            self.cursor.execute(
                "UPDATE users SET download_count=0, total_size_downloaded=0 WHERE user_id=?",
                (user_id,)
            )
            self.conn.commit()

    def get_recent_downloads(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent downloads across all users."""
        with self.lock:
            rows = self.cursor.execute(
                """SELECT d.*, u.username, u.full_name FROM downloads d
                   LEFT JOIN users u ON d.user_id = u.user_id
                   ORDER BY d.id DESC LIMIT ?""", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def backup(self) -> str:
        """Create a database backup. Returns backup file path."""
        path = os.path.join(
            BACKUP_DIR, f"DB_Backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        )
        shutil.copy2(DB_NAME, path)
        return path

    def ban_user(self, user_id: int, ban: bool = True, reason: str = "") -> None:
        """Ban or unban a user. v8: Added ban reason."""
        with self.lock:
            self.cursor.execute(
                "UPDATE users SET is_banned=?, ban_reason=?, ban_date=? WHERE user_id=?",
                (1 if ban else 0, reason if ban else "", datetime.now().isoformat() if ban else None, user_id)
            )
            self.conn.commit()

    def mass_ban(self, user_ids: List[int], reason: str = "Mass ban") -> Tuple[int, int]:
        """v8.0: Ban multiple users at once. Returns (banned_count, skipped_count)."""
        banned = 0
        skipped = 0
        with self.lock:
            for uid in user_ids:
                try:
                    self.cursor.execute(
                        "UPDATE users SET is_banned=1, ban_reason=?, ban_date=? WHERE user_id=?",
                        (reason, datetime.now().isoformat(), uid)
                    )
                    if self.cursor.rowcount > 0:
                        banned += 1
                    else:
                        skipped += 1
                except Exception:
                    skipped += 1
            self.conn.commit()
        return banned, skipped

    def mass_unban(self, user_ids: List[int]) -> Tuple[int, int]:
        """v8.0: Unban multiple users at once. Returns (unbanned_count, skipped_count)."""
        unbanned = 0
        skipped = 0
        with self.lock:
            for uid in user_ids:
                try:
                    self.cursor.execute(
                        "UPDATE users SET is_banned=0, ban_reason='', ban_date=NULL WHERE user_id=?",
                        (uid,)
                    )
                    if self.cursor.rowcount > 0:
                        unbanned += 1
                    else:
                        skipped += 1
                except Exception:
                    skipped += 1
            self.conn.commit()
        return unbanned, skipped

    def set_premium(self, user_id: int, premium: bool = True) -> None:
        """Set or remove premium status for a user."""
        with self.lock:
            self.cursor.execute(
                "UPDATE users SET is_premium=? WHERE user_id=?",
                (1 if premium else 0, user_id)
            )
            self.conn.commit()

    def get_entitlements(self, user_id: int) -> Dict[str, Any]:
        """Return queue passes and temporary premium information."""
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                (user_id,)
            )
            row = self.cursor.execute(
                "SELECT premium_until, queue_passes, total_queue_passes_used FROM entitlements WHERE user_id=?",
                (user_id,)
            ).fetchone()
            premium_until = row[0] if row else None
            premium_active = False
            if premium_until:
                try:
                    premium_active = datetime.fromisoformat(premium_until) > datetime.utcnow()
                except Exception:
                    premium_active = False
            return {
                'premium_until': premium_until,
                'premium_active': premium_active,
                'queue_passes': int((row[1] if row else 0) or 0),
                'queue_passes_used': int((row[2] if row else 0) or 0),
            }

    def get_queue_passes(self, user_id: int) -> int:
        return int(self.get_entitlements(user_id).get('queue_passes', 0) or 0)

    def grant_premium_hours(self, user_id: int, hours: int, reason: str = "premium grant") -> Dict[str, Any]:
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                (user_id,)
            )
            row = self.cursor.execute(
                "SELECT premium_until FROM entitlements WHERE user_id=?",
                (user_id,)
            ).fetchone()
            now = datetime.utcnow()
            base_dt = now
            if row and row[0]:
                try:
                    prev = datetime.fromisoformat(row[0])
                    if prev > now:
                        base_dt = prev
                except Exception:
                    pass
            new_until = base_dt + timedelta(hours=max(1, int(hours)))
            self.cursor.execute(
                "UPDATE entitlements SET premium_until=? WHERE user_id=?",
                (new_until.isoformat(), user_id)
            )
            self.cursor.execute(
                "INSERT INTO coin_transactions (user_id, amount, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, 0, reason, datetime.utcnow().isoformat())
            )
            self.conn.commit()
            return {'premium_until': new_until.isoformat(), 'hours': hours}

    def add_queue_passes(self, user_id: int, passes: int = 1, reason: str = "queue pass") -> int:
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                (user_id,)
            )
            self.cursor.execute(
                "UPDATE entitlements SET queue_passes = MAX(0, queue_passes + ?) WHERE user_id=?",
                (int(passes), user_id)
            )
            self.cursor.execute(
                "INSERT INTO coin_transactions (user_id, amount, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, 0, reason, datetime.utcnow().isoformat())
            )
            row = self.cursor.execute(
                "SELECT queue_passes FROM entitlements WHERE user_id=?",
                (user_id,)
            ).fetchone()
            self.conn.commit()
            return int((row[0] if row else 0) or 0)

    def consume_queue_pass(self, user_id: int) -> bool:
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                (user_id,)
            )
            row = self.cursor.execute(
                "SELECT queue_passes FROM entitlements WHERE user_id=?",
                (user_id,)
            ).fetchone()
            current = int((row[0] if row else 0) or 0)
            if current <= 0:
                return False
            self.cursor.execute(
                "UPDATE entitlements SET queue_passes = queue_passes - 1, total_queue_passes_used = total_queue_passes_used + 1 WHERE user_id=?",
                (user_id,)
            )
            self.conn.commit()
            return True

    def redeem_shop_item(self, user_id: int, item: str) -> Dict[str, Any]:
        """Spend coins on practical perks: premium time or queue priority."""
        pricing = {
            'queue_pass': (int(self.get_setting('coin_shop_queue_cost') or 60), 'queue pass'),
            'premium_day': (int(self.get_setting('coin_shop_day_cost') or 250), '1 day premium'),
            'premium_3day': (int(self.get_setting('coin_shop_3day_cost') or 600), '3 day premium'),
        }
        if item not in pricing:
            return {'ok': False, 'error': 'Unknown item.'}
        cost, label = pricing[item]
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                (user_id,)
            )
            self.cursor.execute(
                "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                (user_id,)
            )
            row = self.cursor.execute(
                "SELECT coins FROM economy WHERE user_id=?",
                (user_id,)
            ).fetchone()
            coins = int((row[0] if row else 0) or 0)
            if coins < cost:
                return {'ok': False, 'error': f'Need {cost} coins.', 'coins': coins, 'cost': cost}
            coins -= cost
            self.cursor.execute(
                "UPDATE economy SET coins=? WHERE user_id=?",
                (coins, user_id)
            )
            self.cursor.execute(
                "INSERT INTO coin_transactions (user_id, amount, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, -cost, f'shop: {label}', datetime.utcnow().isoformat())
            )
            now = datetime.utcnow()
            premium_until = None
            queue_passes = None
            if item == 'queue_pass':
                self.cursor.execute(
                    "UPDATE entitlements SET queue_passes = queue_passes + 1 WHERE user_id=?",
                    (user_id,)
                )
                qrow = self.cursor.execute(
                    "SELECT queue_passes FROM entitlements WHERE user_id=?",
                    (user_id,)
                ).fetchone()
                queue_passes = int((qrow[0] if qrow else 0) or 0)
            else:
                erow = self.cursor.execute(
                    "SELECT premium_until FROM entitlements WHERE user_id=?",
                    (user_id,)
                ).fetchone()
                base_dt = now
                if erow and erow[0]:
                    try:
                        prev = datetime.fromisoformat(erow[0])
                        if prev > now:
                            base_dt = prev
                    except Exception:
                        pass
                hours = 24 if item == 'premium_day' else 72
                premium_dt = base_dt + timedelta(hours=hours)
                premium_until = premium_dt.isoformat()
                self.cursor.execute(
                    "UPDATE entitlements SET premium_until=? WHERE user_id=?",
                    (premium_until, user_id)
                )
            self.conn.commit()
            return {'ok': True, 'item': item, 'label': label, 'cost': cost, 'coins': coins, 'premium_until': premium_until, 'queue_passes': queue_passes}

    def get_all_settings(self) -> List[Dict[str, Any]]:
        """Get all bot settings."""
        with self.lock:
            rows = self.cursor.execute(
                "SELECT key, value, description, category FROM settings"
            ).fetchall()
            return [dict(r) for r in rows]


    def get_wallet(self, user_id: int) -> Dict[str, Any]:
        """Return the user's reward wallet data."""
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                (user_id,)
            )
            self.cursor.execute(
                "INSERT OR IGNORE INTO entitlements (user_id) VALUES (?)",
                (user_id,)
            )
            row = self.cursor.execute(
                "SELECT coins, daily_streak, last_daily_claim, last_spin, total_audio_downloads, total_video_downloads FROM economy WHERE user_id=?",
                (user_id,)
            ).fetchone()
            ent = self.cursor.execute(
                "SELECT premium_until, queue_passes FROM entitlements WHERE user_id=?",
                (user_id,)
            ).fetchone()
            premium_until = ent[0] if ent else None
            premium_active = False
            if premium_until:
                try:
                    premium_active = datetime.fromisoformat(premium_until) > datetime.utcnow()
                except Exception:
                    premium_active = False
            return {
                'coins': (row[0] if row else 0) or 0,
                'daily_streak': (row[1] if row else 0) or 0,
                'last_daily_claim': row[2] if row else None,
                'last_spin': row[3] if row else None,
                'audio_downloads': (row[4] if row else 0) or 0,
                'video_downloads': (row[5] if row else 0) or 0,
                'premium_until': premium_until,
                'premium_active': premium_active,
                'queue_passes': int((ent[1] if ent else 0) or 0),
            }

    def add_coins(self, user_id: int, amount: int, reason: str = "admin gift") -> int:
        """Add or remove coins and return the new balance."""
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                (user_id,)
            )
            self.cursor.execute(
                "UPDATE economy SET coins = MAX(0, coins + ?) WHERE user_id=?",
                (amount, user_id)
            )
            self.cursor.execute(
                "INSERT INTO coin_transactions (user_id, amount, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, amount, reason, datetime.now().isoformat())
            )
            row = self.cursor.execute(
                "SELECT coins FROM economy WHERE user_id=?",
                (user_id,)
            ).fetchone()
            self.conn.commit()
            return int(row[0] if row else 0)

    def claim_daily_bonus(self, user_id: int) -> Dict[str, Any]:
        """Claim a once-per-day bonus."""
        now = datetime.utcnow()
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                (user_id,)
            )
            row = self.cursor.execute(
                "SELECT coins, daily_streak, last_daily_claim FROM economy WHERE user_id=?",
                (user_id,)
            ).fetchone()
            coins = int(row[0] or 0) if row else 0
            streak = int(row[1] or 0) if row else 0
            last_claim_raw = row[2] if row else None
            last_claim = None
            if last_claim_raw:
                try:
                    last_claim = datetime.fromisoformat(last_claim_raw)
                except Exception:
                    last_claim = None
            if last_claim and last_claim.date() == now.date():
                next_claim = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
                return {
                    'ok': False,
                    'remaining_seconds': max(1, int((next_claim - now).total_seconds())),
                    'coins': coins,
                    'streak': streak,
                }
            if last_claim and last_claim.date() == (now.date() - timedelta(days=1)):
                streak += 1
            else:
                streak = 1
            reward = random.randint(20, 35) + min(streak * 2, 20)
            coins += reward
            self.cursor.execute(
                "UPDATE economy SET coins=?, daily_streak=?, last_daily_claim=? WHERE user_id=?",
                (coins, streak, now.isoformat(), user_id)
            )
            self.cursor.execute(
                "INSERT INTO coin_transactions (user_id, amount, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, reward, 'daily bonus', now.isoformat())
            )
            self.conn.commit()
            return {'ok': True, 'reward': reward, 'coins': coins, 'streak': streak}

    def spin_reward(self, user_id: int) -> Dict[str, Any]:
        """Spin the reward wheel every 4 hours."""
        cooldown_seconds = 4 * 60 * 60
        now = datetime.utcnow()
        rewards = [5, 8, 10, 12, 15, 20, 25, 30, 40, 50, 75, 100]
        with self.lock:
            self.cursor.execute(
                "INSERT OR IGNORE INTO economy (user_id) VALUES (?)",
                (user_id,)
            )
            row = self.cursor.execute(
                "SELECT coins, last_spin FROM economy WHERE user_id=?",
                (user_id,)
            ).fetchone()
            coins = int(row[0] or 0) if row else 0
            last_spin_raw = row[1] if row else None
            last_spin = None
            if last_spin_raw:
                try:
                    last_spin = datetime.fromisoformat(last_spin_raw)
                except Exception:
                    last_spin = None
            if last_spin:
                elapsed = (now - last_spin).total_seconds()
                if elapsed < cooldown_seconds:
                    return {
                        'ok': False,
                        'remaining_seconds': max(1, int(cooldown_seconds - elapsed)),
                        'coins': coins,
                    }
            reward = random.choice(rewards)
            coins += reward
            self.cursor.execute(
                "UPDATE economy SET coins=?, last_spin=? WHERE user_id=?",
                (coins, now.isoformat(), user_id)
            )
            self.cursor.execute(
                "INSERT INTO coin_transactions (user_id, amount, reason, created_at) VALUES (?, ?, ?, ?)",
                (user_id, reward, 'spin reward', now.isoformat())
            )
            self.conn.commit()
            return {'ok': True, 'reward': reward, 'coins': coins}


    def get_reward_stats(self) -> Dict[str, Any]:
        """Aggregate reward economy statistics for dashboards."""
        with self.lock:
            row = self.cursor.execute(
                "SELECT COALESCE(SUM(coins), 0), COALESCE(SUM(total_audio_downloads), 0), COALESCE(SUM(total_video_downloads), 0), COALESCE(MAX(daily_streak), 0) FROM economy"
            ).fetchone()
            tx = self.cursor.execute(
                "SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM coin_transactions"
            ).fetchone()
            ent = self.cursor.execute(
                "SELECT COALESCE(SUM(queue_passes), 0), COALESCE(SUM(total_queue_passes_used), 0) FROM entitlements"
            ).fetchone()
            return {
                'coins_in_wallets': int((row[0] if row else 0) or 0),
                'audio_downloads': int((row[1] if row else 0) or 0),
                'video_downloads': int((row[2] if row else 0) or 0),
                'best_streak': int((row[3] if row else 0) or 0),
                'coins_distributed': int((tx[0] if tx else 0) or 0),
                'transactions': int((tx[1] if tx else 0) or 0),
                'queue_passes_left': int((ent[0] if ent else 0) or 0),
                'queue_passes_used': int((ent[1] if ent else 0) or 0),
            }

    def get_top_wallets(self, limit: int = 8) -> List[Dict[str, Any]]:
        """Return top wallet holders in a thread-safe way."""
        with self.lock:
            rows = self.cursor.execute(
                "SELECT e.user_id, e.coins, u.username FROM economy e LEFT JOIN users u ON u.user_id = e.user_id ORDER BY e.coins DESC LIMIT ?",
                (int(limit),)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_top_downloaders(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Return users ordered by download count."""
        with self.lock:
            rows = self.cursor.execute(
                "SELECT user_id, username, download_count FROM users ORDER BY download_count DESC LIMIT ?",
                (int(limit),)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_top_platforms(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Return most-used download platforms."""
        with self.lock:
            rows = self.cursor.execute(
                "SELECT platform, COUNT(*) as cnt FROM downloads GROUP BY platform ORDER BY cnt DESC LIMIT ?",
                (int(limit),)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_user_admin_toggle_info(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Return minimal user info needed for premium/ban toggles."""
        with self.lock:
            row = self.cursor.execute(
                "SELECT user_id, full_name, is_premium, is_banned FROM users WHERE user_id=?",
                (user_id,)
            ).fetchone()
            return dict(row) if row else None


db = UltraDatabase()

# ════════════════════════════════════════════════════════════════════════════
# 🎨 PREMIUM UI & DESIGN SYSTEM v8.1 ULTRA
# ════════════════════════════════════════════════════════════════════════════

class UI:
    """Centralized UI generation with HTML-safe rendering."""

    E: Dict[str, str] = {
        'wave': '\U0001f44b', 'robot': '\U0001f916', 'fire': '\U0001f525',
        'rocket': '\U0001f680', 'star': '\u2b50', 'check': '\u2705',
        'cross': '\u274c', 'warning': '\u26a0\ufe0f', 'search': '\U0001f50d',
        'download': '\u2b07\ufe0f', 'upload': '\u2b06\ufe0f', 'video': '\U0001f3ac',
        'music': '\U0001f3b5', 'loading': '\u23f3', 'success': '\U0001f389',
        'crown': '\U0001f451', 'chart': '\U0001f4ca', 'gear': '\u2699\ufe0f',
        'lock': '\U0001f512', 'unlock': '\U0001f513', 'key': '\U0001f511', 'bell': '\U0001f514',
        'globe': '\U0001f310', 'target': '\U0001f3af', 'lightning': '\u26a1',
        'diamond': '\U0001f48e', 'trophy': '\U0001f3c6', 'medal': '\U0001f3c5',
        'gift': '\U0001f381', 'calendar': '\U0001f4c5', 'clock': '\U0001f550',
        'folder': '\U0001f4c1', 'file': '\U0001f4c4', 'link': '\U0001f517',
        'heart': '\u2764\ufe0f', 'boom': '\U0001f4a5', 'sparkles': '\u2728',
        'back': '\u25c0\ufe0f', 'forward': '\u25b6\ufe0f', 'save': '\U0001f4be',
        'trash': '\U0001f5d1\ufe0f', 'edit': '\u270f\ufe0f', 'info': '\u2139\ufe0f',
        'help': '\u2753', 'shield': '\U0001f6e1\ufe0f', 'tools': '\U0001f527',
        'package': '\U0001f4e6', 'cloud': '\u2601\ufe0f', 'hd': '\U0001f3a5',
        'quality': '\U0001f4fa', 'speed': '\U0001f3ce\ufe0f', 'size': '\U0001f4cf',
        'eyes': '\U0001f440', 'photo': '\U0001f4f8', 'mic': '\U0001f399\ufe0f',
        'play': '\u25b6\ufe0f', 'pause': '\u23f8\ufe0f', 'stop': '\u23f9\ufe0f',
        'skip': '\u23ed\ufe0f', 'repeat': '\U0001f501', 'shuffle': '\U0001f500',
        'server': '\U0001f5a5\ufe0f', 'wifi': '\U0001f4e1', 'battery': '\U0001f50b',
        'memo': '\U0001f4dd', 'subtitles': '\U0001f4ac', 'coin': '\U0001fa99',
        'game': '\U0001f3b0', 'wallet': '\U0001f4b0', 'giftbox': '\U0001f381', 'cookie': '🍪',
    }

    _braille_idx: int = 0

    @classmethod
    def braille_spinner(cls) -> str:
        """v8.0: Return next braille spinner frame for animated progress."""
        frame = BRAILLE_FRAMES[cls._braille_idx % len(BRAILLE_FRAMES)]
        cls._braille_idx += 1
        return frame

    @staticmethod
    def escape_html(text: Optional[Any]) -> str:
        """Escape HTML special characters to prevent Telegram parse errors."""
        if not text:
            return ""
        return (str(text)
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;'))

    @staticmethod
    def truncate(text: Any, max_len: int = 100) -> str:
        """Truncate text and add ellipsis if too long."""
        text = str(text) if text else ""
        if len(text) > max_len:
            return text[:max_len - 3] + "..."
        return text

    @staticmethod
    def format_size(size_bytes: Optional[float]) -> str:
        """Format bytes to human-readable size."""
        if not size_bytes or size_bytes <= 0:
            return "N/A"
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

    @staticmethod
    def format_size_mb(size_bytes: Optional[float]) -> str:
        """v8.0: Format bytes always in MB for easy comparison."""
        if not size_bytes or size_bytes <= 0:
            return "N/A"
        mb = size_bytes / (1024 * 1024)
        if mb >= 1024:
            return f"{mb / 1024:.2f} GB"
        return f"{mb:.1f} MB"

    @staticmethod
    def format_duration(seconds: Optional[float]) -> str:
        """Format seconds to HH:MM:SS or MM:SS."""
        if not seconds or seconds <= 0:
            return "N/A"
        seconds = int(seconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes}:{secs:02d}"

    @staticmethod
    def format_uptime() -> str:
        """Format bot uptime in human-readable format."""
        elapsed = time.time() - BOT_START_TIME
        days = int(elapsed // 86400)
        hours = int((elapsed % 86400) // 3600)
        minutes = int((elapsed % 3600) // 60)
        parts: List[str] = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        parts.append(f"{minutes}m")
        return " ".join(parts)

    @staticmethod
    def format_speed(size_bytes: Optional[float], proc_time: Optional[float]) -> str:
        """v8.0: Format download speed from size and time."""
        if not proc_time or proc_time <= 0 or not size_bytes:
            return "Ultra Fast"
        speed_bps = size_bytes / proc_time
        if speed_bps > 1024 * 1024:
            return f"{speed_bps / (1024 * 1024):.1f} MB/s"
        elif speed_bps > 1024:
            return f"{speed_bps / 1024:.1f} KB/s"
        return f"{speed_bps:.0f} B/s"

    @staticmethod
    def get_platform_error_tips(error_str: str, platform: str = "") -> str:
        """v8.0: Return platform-specific error tips based on error message."""
        tips: List[str] = []
        error_lower = error_str.lower()

        if 'age' in error_lower or 'sign in' in error_lower or 'login' in error_lower:
            tips.append("\U0001f511 Age-restricted or login-required video.")
            tips.append("   \u2192 Upload cookies.txt via Admin Panel")
            tips.append("   \u2192 Use browser extension to export cookies")

        if 'private' in error_lower or 'unavailable' in error_lower:
            tips.append("\U0001f512 Video is private or has been deleted.")

        if 'tiktok' in platform.lower():
            if 'watermark' in error_lower or 'no format' in error_lower:
                tips.append("\U0001f3b5 Try a different TikTok video or check if it's private.")

        if 'instagram' in platform.lower():
            if 'login' in error_lower or 'private' in error_lower:
                tips.append("\U0001f4f7 Instagram may require login. Try cookies.txt.")
                tips.append("   \u2192 Some Stories/Reels are private by default.")

        if 'format' in error_lower or 'no video' in error_lower:
            tips.append("\U0001f3a5 No suitable video format found.")
            tips.append("   \u2192 The video may be live or processing.")

        if 'geoblock' in error_lower or 'country' in error_lower or 'not available' in error_lower:
            tips.append("\U0001f310 Geo-blocked content. Try a VPN or cookies.")

        if 'too large' in error_lower or '413' in error_lower:
            tips.append("\U0001f4cf File too large for Telegram (50MB limit).")
            tips.append("   \u2192 Try a lower quality (720p or 480p)")

        if 'http' in error_lower and ('400' in error_lower or '403' in error_lower or '404' in error_lower):
            tips.append("\U0001f310 Server error. Video may have been removed.")

        if not tips:
            tips.append("\U0001f50d Make sure the video is public and the link is correct.")
            tips.append("\U0001f527 Try a different quality or format.")
            tips.append("\U0001f511 For restricted videos, use cookies.txt (Admin Panel)")

        return "\n".join(tips)

    @staticmethod
    def welcome(user, stats: Optional[Dict] = None) -> str:
        """Generate a slimmer premium welcome message for mobile screens."""
        premium_badge = f" • {UI.E['crown']} Premium" if db.is_premium(user.id) else ""
        wallet = db.get_wallet(user.id)
        total_users = (stats or {}).get('users', 0)
        lane = get_priority_lane_name(user.id)
        return f"""{UI.E['sparkles']} <b>ULTIMATE DOWNLOADER v21</b>{premium_badge}
{UI.E['wave']} <b>{UI.escape_html(user.first_name)}</b> • {UI.E['coin']} <b>{wallet['coins']}</b> • {UI.E['wifi']} <b>{lane}</b>
{UI.E['rocket']} Send one public link • pick exact video quality or audio
{UI.E['quality']} 4K / 1080p / 720p   {UI.E['music']} MP3 / M4A / OPUS / WAV
<i>Compact home • fast lane • premium delivery • {total_users} users</i>"""

    @staticmethod
    def main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
        """Build a polished compact home menu."""
        kb = [
            [
                InlineKeyboardButton(f"{UI.E['help']} Guide", callback_data="help"),
                InlineKeyboardButton(f"{UI.E['wifi']} Queue", callback_data="queue_status"),
            ],
            [
                InlineKeyboardButton(f"{UI.E['star']} Me", callback_data="profile"),
                InlineKeyboardButton(f"{UI.E['wallet']} Wallet", callback_data="wallet"),
            ],
            [
                InlineKeyboardButton(f"{UI.E['diamond']} Premium", callback_data="premium"),
                InlineKeyboardButton(f"{UI.E['forward']} More", callback_data="menu_more"),
            ],
            [
                InlineKeyboardButton(f"{UI.E['giftbox']} Bonus", callback_data="daily_bonus"),
                InlineKeyboardButton(f"{UI.E['game']} Spin", callback_data="spin_wheel"),
            ],
        ]
        if is_admin:
            kb.append([InlineKeyboardButton(f"{UI.E['crown']} Admin", callback_data="admin_home")])
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def more_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
        """Secondary menu with cleaner advanced shortcuts."""
        kb = [
            [
                InlineKeyboardButton(f"{UI.E['globe']} Platforms", callback_data="platforms"),
                InlineKeyboardButton(f"{UI.E['music']} Audio", callback_data="audio_help"),
            ],
            [
                InlineKeyboardButton(f"{UI.E['memo']} History", callback_data="history"),
                InlineKeyboardButton(f"{UI.E['chart']} Stats", callback_data="stats"),
            ],
            [
                InlineKeyboardButton(f"{UI.E['rocket']} Tips", callback_data="quick_tips"),
                InlineKeyboardButton("🩺 Health", callback_data="show_health"),
            ],
            [InlineKeyboardButton(f"{UI.E['back']} Home", callback_data="start_menu")],
        ]
        if is_admin:
            kb.insert(3, [InlineKeyboardButton(f"{UI.E['crown']} Admin Panel", callback_data="admin_home")])
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def back_btn() -> InlineKeyboardMarkup:
        """Build a 'Back to Menu' button."""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")]
        ])

    @staticmethod
    def cancel_btn() -> InlineKeyboardMarkup:
        """Build a 'Cancel' button."""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{UI.E['cross']} Cancel", callback_data="cancel")]
        ])

    @staticmethod
    def progress_text(step: int, info: Optional[Dict] = None,
                      context: Optional[Any] = None,
                      progress_pct: Optional[float] = None,
                      queue_pos: Optional[int] = None,
                      queue_total: Optional[int] = None) -> str:
        """
        Generate progress message with real-time progress and animated spinner.
        v8.0: Added braille spinner animation and queue position.
        """
        stages: Dict[int, Tuple[str, int]] = {
            1: (f"{UI.E['search']} <b>Checking link...</b>",                          10),
            2: (f"{UI.E['globe']} <b>Reading media info...</b>",                       25),
            3: (f"{UI.braille_spinner()} <b>Downloading...</b>",                   50),
            4: (f"{UI.E['tools']} <b>Processing file...</b>",                       85),
            5: (f"{UI.E['package']} <b>Finalizing...</b>",                          95),
            6: (f"{UI.E['upload']} <b>Uploading...</b>",                     98),
            7: (f"{UI.E['success']} <b>Complete!</b>",                                  100),
        }

        label, base_p = stages.get(step, stages[1])

        if progress_pct is not None and step == 3:
            p = int(progress_pct)
            if p < 0:
                p = 0
            elif p > 99:
                p = 99
        else:
            p = base_p

        filled = int(p / 5)
        empty = 20 - filled
        bar = "\u2588" * filled + "\u2591" * empty

        msg = (
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {label}\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"<b>Progress:</b> [<code>{bar}</code>] <b>{p}%</b>\n"
        )

        # v8.0: Show queue position
        if queue_pos is not None and queue_total is not None and queue_total > 0:
            snap = get_queue_snapshot()
            msg += (
                f"\n{UI.E['chart']} <b>Queue slot:</b> #{queue_pos} of {queue_total}\n"
                f"{UI.E['wifi']} <b>Live load:</b> {snap['active_total']} / {snap['max_slots']} active\n"
            )

        if info:
            if info.get('title'):
                msg += f"\n{UI.E['video']} <b>Title:</b> {UI.escape_html(UI.truncate(info['title'], 50))}"
            if info.get('quality'):
                q = info['quality']
                if context and context.user_data.get("selected_quality_info"):
                    si = context.user_data["selected_quality_info"]
                    q = si.get("resolution", si.get("format_id", q))
                    if si.get('ext'):
                        q += f" ({si['ext']})"
                msg += f"\n{UI.E['hd']} <b>Quality:</b> {q}"
            if info.get('_eta_str'):
                msg += f"\n{UI.E['clock']} <b>ETA:</b> {info['_eta_str']}"
            if info.get('_speed_str'):
                msg += f"\n{UI.E['speed']} <b>Speed:</b> {info['_speed_str']}"
            if info.get('_downloaded_bytes') and info.get('_total_bytes'):
                dl = UI.format_size(info['_downloaded_bytes'])
                tot = UI.format_size(info['_total_bytes'])
                msg += f"\n{UI.E['size']} <b>Downloaded:</b> {dl} / {tot}"

        msg += f"\n\n{UI.E['info']} <i>Press Cancel to abort download</i>"
        return msg

    @staticmethod
    def video_info_card(info_dict: Dict, url_hash: str) -> Tuple[str, InlineKeyboardMarkup, str]:
        """
        Generate video info card with thumbnail.
        v8.0: Added subtitle download button.
        Returns (text, keyboard, thumbnail_url).
        """
        title = info_dict.get('title', 'Unknown Title')
        duration = info_dict.get('duration', 0)
        uploader = info_dict.get('uploader', 'Unknown')
        upload_date = info_dict.get('upload_date', 'N/A')
        view_count = info_dict.get('view_count', 0)
        like_count = info_dict.get('like_count', 0)
        description = info_dict.get('description', '')
        thumbnail = info_dict.get('thumbnail', '')
        platform = info_dict.get('extractor_key', 'Web')

        views = f"{view_count:,}" if view_count else "N/A"
        likes = f"{like_count:,}" if like_count else "N/A"

        if upload_date and len(str(upload_date)) == 8:
            try:
                upload_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
            except Exception:
                pass

        text = (
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['eyes']} <b>VIDEO INFO PREVIEW</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"{UI.E['video']} <b>{UI.escape_html(UI.truncate(title, 80))}</b>\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"{UI.E['globe']} <b>Platform:</b>  {platform}\n"
            f"{UI.E['robot']} <b>Channel:</b>   {UI.escape_html(UI.truncate(uploader, 40))}\n"
            f"{UI.E['clock']} <b>Duration:</b>  {UI.format_duration(duration)}\n"
            f"{UI.E['calendar']} <b>Uploaded:</b>  {upload_date}\n"
            f"{UI.E['eyes']} <b>Views:</b>     {views}\n"
            f"{UI.E['heart']} <b>Likes:</b>     {likes}\n"
        )

        if description:
            desc_short = UI.truncate(description.strip(), 150)
            text += f"\n{UI.E['memo']} <b>Description:</b>\n<i>{UI.escape_html(desc_short)}</i>"

        # v8.0: Build buttons with subtitle option
        kb = [
            [InlineKeyboardButton(
                f"{UI.E['download']} Download Now",
                callback_data=f"dlinf_best_{url_hash}"
            )],
            [InlineKeyboardButton(
                f"{UI.E['quality']} Choose Quality",
                callback_data=f"dlinf_quality_{url_hash}"
            )],
            [InlineKeyboardButton(
                f"{UI.E['music']} Choose Audio Format",
                callback_data=f"dlinf_audio_{url_hash}"
            )],
        ]

        # v8.0: Add subtitle button if subtitles available
        if URLUtils.has_subtitles(info_dict):
            kb.append([
                InlineKeyboardButton(
                    f"{UI.E['subtitles']} Download Subtitles",
                    callback_data=f"dlinf_subs_{url_hash}"
                )
            ])

        kb.append([InlineKeyboardButton(f"{UI.E['cross']} Cancel", callback_data="cancel")])
        return text, InlineKeyboardMarkup(kb), thumbnail

    @staticmethod
    def quality_kb(formats: List[Dict], url_hash: str,
                   info_dict: Optional[Dict] = None) -> Tuple[str, InlineKeyboardMarkup]:
        """Build a clean quality keyboard that prefers one strong option per resolution."""
        buttons: List[List[InlineKeyboardButton]] = []

        def sort_key(f: Dict[str, Any]) -> Tuple[int, int, int, int, float, int]:
            height = int(f.get('height') or 0)
            has_audio = 1 if f.get('acodec', 'none') != 'none' else 0
            mp4_bonus = 1 if str(f.get('ext', '')).lower() == 'mp4' else 0
            h264_bonus = 1 if any(codec in str(f.get('vcodec', '')).lower() for codec in ('avc', 'h264')) else 0
            return (
                height,
                has_audio,
                mp4_bonus,
                h264_bonus,
                float(f.get('tbr') or 0),
                int(f.get('filesize') or 0),
            )

        formats_sorted = sorted(formats, key=sort_key, reverse=True)
        video_formats = [
            f for f in formats_sorted
            if f.get('vcodec') and f.get('vcodec') != 'none' and f.get('resolution') != 'audio only'
        ]
        audio_formats = [f for f in formats_sorted if f.get('resolution') == 'audio only']

        best_by_height: Dict[int, Dict[str, Any]] = {}
        extras: List[Dict[str, Any]] = []
        for f in video_formats:
            height = int(f.get('height') or 0)
            if height > 0 and height not in best_by_height:
                best_by_height[height] = f
            elif height <= 0 and len(extras) < 2:
                extras.append(f)

        display_formats = [best_by_height[h] for h in sorted(best_by_height.keys(), reverse=True)]
        display_formats.extend(extras)
        display_formats = display_formats[:8]

        best_height = max(best_by_height.keys(), default=0)
        if best_height >= 2160:
            best_resolution = '4K'
        elif best_height >= 1440:
            best_resolution = '1440p'
        elif best_height >= 1080:
            best_resolution = '1080p'
        elif best_height >= 720:
            best_resolution = '720p'
        elif best_height >= 480:
            best_resolution = '480p'
        elif best_height > 0:
            best_resolution = f'{best_height}p'
        else:
            best_resolution = 'Unknown'

        header = f"✨ <b>NEON QUALITY SELECTOR</b>\n{UI.E['quality']} <b>SELECT DOWNLOAD QUALITY:</b>\n\n"
        if info_dict:
            header += f"{UI.E['video']} <b>{UI.escape_html(UI.truncate(info_dict.get('title', ''), 50))}</b>\n"
            header += f"{UI.E['clock']} Duration: {UI.format_duration(info_dict.get('duration'))}\n"
            header += f"{UI.E['hd']} Best Available: <b>{best_resolution}</b>\n\n"

        row: List[InlineKeyboardButton] = []
        for f in display_formats:
            height = int(f.get('height') or 0)
            res = f.get('resolution', 'unknown')
            size = int(f.get('filesize') or 0)
            size_str = f" ({UI.format_size_mb(size)})" if size > 0 else ""
            merge_tag = " + audio" if f.get('acodec', 'none') == 'none' else ""
            fps = int(f.get('fps') or 0)
            fps_tag = f" {fps}fps" if fps >= 50 else ""

            if height >= 2160:
                label = f"🌟 4K ({res}{fps_tag}){size_str}{merge_tag}"
            elif height >= 1440:
                label = f"🎞 1440p ({res}{fps_tag}){size_str}{merge_tag}"
            elif height >= 1080:
                label = f"🎥 1080p ({res}{fps_tag}){size_str}{merge_tag}"
            elif height >= 720:
                label = f"📺 720p ({res}{fps_tag}){size_str}{merge_tag}"
            elif height >= 480:
                label = f"📱 480p ({res}{fps_tag}){size_str}{merge_tag}"
            elif height >= 360:
                label = f"📡 360p ({res}{fps_tag}){size_str}{merge_tag}"
            else:
                label = f"📄 {res}{size_str}{merge_tag}"

            row.append(InlineKeyboardButton(label, callback_data=f"dl_{f['format_id']}_{url_hash}"))
            if len(row) == 2:
                buttons.append(row)
                row = []

        if row:
            buttons.append(row)

        if audio_formats:
            buttons.append([
                InlineKeyboardButton("🎵 MP3", callback_data=f"dl_mp3_{url_hash}"),
                InlineKeyboardButton("🎧 M4A", callback_data=f"dl_m4a_{url_hash}"),
            ])
            buttons.append([
                InlineKeyboardButton("🪩 OPUS", callback_data=f"dl_opus_{url_hash}"),
                InlineKeyboardButton("🎙 WAV", callback_data=f"dl_wav_{url_hash}"),
            ])

        buttons.append([
            InlineKeyboardButton("⚡ BEST QUALITY (Auto)", callback_data=f"dl_best_{url_hash}")
        ])
        buttons.append([
            InlineKeyboardButton("🎼 Open Audio Menu", callback_data=f"dlinf_audio_{url_hash}")
        ])
        buttons.append([
            InlineKeyboardButton(f"{UI.E['cross']} Cancel", callback_data="cancel")
        ])

        header += f"Found <b>{len(display_formats)}</b> video + <b>{len(audio_formats)}</b> audio options"
        return header, InlineKeyboardMarkup(buttons)

    @staticmethod
    def audio_kb(url_hash: str, info_dict: Optional[Dict[str, Any]] = None) -> Tuple[str, InlineKeyboardMarkup]:
        """Build a dedicated audio format picker."""
        header = f"✨ <b>PREMIUM AUDIO STUDIO</b>\n"
        header += f"{UI.E['music']} <b>SELECT AUDIO FORMAT</b>\n\n"
        if info_dict:
            header += f"{UI.E['video']} <b>{UI.escape_html(UI.truncate(info_dict.get('title', ''), 50))}</b>\n"
            header += f"{UI.E['clock']} Duration: {UI.format_duration(info_dict.get('duration'))}\n"
            header += f"🎚 Best for music, reels, podcasts and extracted audio.\n\n"

        kb = [
            [
                InlineKeyboardButton("🎵 MP3 • 320kbps", callback_data=f"dlaudio_mp3_{url_hash}"),
                InlineKeyboardButton("🎧 M4A • AAC", callback_data=f"dlaudio_m4a_{url_hash}"),
            ],
            [
                InlineKeyboardButton("🪩 OPUS • Small Size", callback_data=f"dlaudio_opus_{url_hash}"),
                InlineKeyboardButton("🎙 WAV • Lossless", callback_data=f"dlaudio_wav_{url_hash}"),
            ],
            [InlineKeyboardButton("⚡ Best Audio • Smart MP3", callback_data=f"dlaudio_best_{url_hash}")],
            [InlineKeyboardButton(f"{UI.E['back']} Back to Quality", callback_data=f"dlinf_quality_{url_hash}")],
            [InlineKeyboardButton(f"{UI.E['cross']} Cancel", callback_data="cancel")],
        ]
        return header, InlineKeyboardMarkup(kb)

    @staticmethod
    def admin_panel(stats: Dict) -> str:
        """Generate a richer but compact admin command panel."""
        maintenance = db.get_setting('maintenance')
        cookies_ok = 'YES' if os.path.exists(COOKIES_FILE) else 'NO'
        disk_usage = storage_cleaner.get_disk_usage_mb()
        rewards = db.get_reward_stats()
        snap = get_queue_snapshot()

        return f"""{UI.E['crown']} <b>ADMIN COMMAND • NEON DELUXE</b>
{UI.E['chart']} Users <b>{stats['users']}</b> • DL <b>{stats['downloads']}</b> • Today <b>{stats['today_downloads']}</b>
{UI.E['diamond']} Premium <b>{stats['premium']}</b> • {UI.E['lock']} Banned <b>{stats['banned']}</b>
{UI.E['coin']} Wallet <b>{rewards['coins_in_wallets']}</b> • Given <b>{rewards['coins_distributed']}</b>
{UI.E['wifi']} Active <b>{snap['active_total']}/{snap['max_slots']}</b> • Waiting <b>{snap.get('waiting_total', 0)}</b>
{UI.E['rocket']} Priority <b>{snap.get('priority_waiting', 0)}</b> • Queue Premium <b>{snap.get('premium_waiting', 0)}</b>
{UI.E['tools']} Maint <b>{MAINT_ON if maintenance == '1' else MAINT_OFF}</b> • Cookies <b>{cookies_ok}</b>
{UI.E['cloud']} Disk <b>{disk_usage:.1f} MB</b> • {UI.E['server']} Uptime <b>{UI.format_uptime()}</b>

<i>Clean dashboard • quick controls • live queue pulse</i>"""

    @staticmethod
    def admin_kb() -> InlineKeyboardMarkup:
        """Build a cleaner admin panel keyboard."""
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📢 Cast", callback_data="adm_cast"),
                InlineKeyboardButton("🍪 Cookies", callback_data="adm_cookie"),
            ],
            [
                InlineKeyboardButton("🛠 Maint", callback_data="adm_maint"),
                InlineKeyboardButton("📶 Queue", callback_data="adm_queuehub"),
            ],
            [
                InlineKeyboardButton("👑 Premium", callback_data="adm_setpremium"),
                InlineKeyboardButton("🪙 Coins", callback_data="adm_giftcoins"),
            ],
            [
                InlineKeyboardButton("🚫 Ban", callback_data="adm_ban"),
                InlineKeyboardButton("🔎 Lookup", callback_data="adm_lookup"),
            ],
            [
                InlineKeyboardButton("🗂 Recent", callback_data="adm_recent"),
                InlineKeyboardButton("📈 Stats", callback_data="adm_fullstats"),
            ],
            [
                InlineKeyboardButton("⚙ Settings", callback_data="adm_settings"),
                InlineKeyboardButton("💾 Backup", callback_data="adm_backup"),
            ],
            [
                InlineKeyboardButton("💸 Economy", callback_data="adm_economy"),
                InlineKeyboardButton("⛔ Mass Ban", callback_data="adm_massban"),
            ],
            [
                InlineKeyboardButton("✅ Unban", callback_data="adm_massunban"),
                InlineKeyboardButton("🩺 Health", callback_data="adm_health"),
            ],
            [InlineKeyboardButton(f"{UI.E['back']} Back to Bot", callback_data="start_menu")],
        ])




def get_user_priority_rank(user_id: int) -> int:
    """Lower rank means higher priority in queue."""
    if user_id == ADMIN_ID:
        return 0
    if db.get_queue_passes(user_id) > 0:
        return 1
    if db.is_premium(user_id):
        return 2
    return 3


def get_priority_lane_name(user_id: Optional[int]) -> str:
    if not user_id:
        return 'Standard lane'
    rank = get_user_priority_rank(user_id)
    if rank == 0:
        return 'Admin override lane'
    if rank == 1:
        return 'Priority pass lane'
    if rank == 2:
        return 'Premium lane'
    return 'Standard lane'


def prune_stale_queue_entries() -> int:
    """Remove stale or cancelled queue entries to avoid ghost waiting jobs."""
    now = time.time()
    removed = 0
    with queued_downloads_lock:
        for uid, entry in list(queued_downloads.items()):
            created_at = float(entry.get('created_at', now))
            cancel_event = entry.get('cancel')
            is_cancelled = bool(cancel_event and getattr(cancel_event, 'is_set', lambda: False)())
            if is_cancelled or (now - created_at) > QUEUE_STALE_SECONDS:
                queued_downloads.pop(uid, None)
                removed += 1
    if removed:
        logger.info(f"🧹 Queue cleanup removed {removed} stale waiting job(s)")
    return removed


def get_sorted_waiting_entries() -> List[Dict[str, Any]]:
    prune_stale_queue_entries()
    with queued_downloads_lock:
        entries = list(queued_downloads.values())
    return sorted(entries, key=lambda e: (int(e.get('priority_rank', 99)), float(e.get('created_at', 0)), int(e.get('ticket', 0))))


def get_queue_snapshot() -> Dict[str, int]:
    """Return a compact snapshot of current active and waiting load."""
    with active_downloads_lock:
        active_ids = list(active_downloads.keys())
    premium_active = 0
    for uid in active_ids:
        try:
            if db.is_premium(uid):
                premium_active += 1
        except Exception:
            pass
    active_total = len(active_ids)
    free_active = max(0, active_total - premium_active)

    waiting = get_sorted_waiting_entries()
    priority_waiting = sum(1 for e in waiting if int(e.get('priority_rank', 99)) <= 1)
    premium_waiting = sum(1 for e in waiting if int(e.get('priority_rank', 99)) == 2)

    available_slots = max(0, MAX_CONCURRENT_DOWNLOADS - active_total)
    return {
        'active_total': active_total,
        'premium_active': premium_active,
        'free_active': free_active,
        'waiting_total': len(waiting),
        'priority_waiting': priority_waiting,
        'premium_waiting': premium_waiting,
        'available_slots': available_slots,
        'max_slots': MAX_CONCURRENT_DOWNLOADS,
    }


def get_user_waiting_position(user_id: int) -> Tuple[int, int]:
    waiting = get_sorted_waiting_entries()
    for idx, entry in enumerate(waiting, 1):
        if entry.get('user_id') == user_id:
            return idx, len(waiting)
    return 0, len(waiting)


def format_queue_snapshot_html(user_id: Optional[int] = None) -> str:
    snap = get_queue_snapshot()
    lane = get_priority_lane_name(user_id)
    pos, total = get_user_waiting_position(user_id) if user_id else (0, snap.get('waiting_total', 0))
    your_wait = f"#{pos} / {total}" if pos else "Not waiting"
    return f"""{UI.E['wifi']} <b>QUEUE STATUS</b>

{UI.E['chart']} Active <b>{snap['active_total']} / {snap['max_slots']}</b> • Waiting <b>{snap.get('waiting_total', 0)}</b>
{UI.E['rocket']} Priority <b>{snap.get('priority_waiting', 0)}</b> • {UI.E['diamond']} Premium <b>{snap.get('premium_waiting', 0)}</b>
{UI.E['key']} Lane <b>{lane}</b>
{UI.E['clock']} Your spot <b>{your_wait}</b>

{UI.E['info']} <i>Admin → Queue Pass → Premium → Normal</i>"""


def build_waiting_text(user_id: int) -> str:
    pos, total = get_user_waiting_position(user_id)
    lane = get_priority_lane_name(user_id)
    snap = get_queue_snapshot()
    if pos <= 0:
        pos = 1
    return f"""{UI.E['loading']} <b>QUEUE HOLD</b>

{UI.E['key']} Lane <b>{lane}</b>
{UI.E['chart']} Waiting spot <b>#{pos}</b> / <b>{max(total, pos)}</b>
{UI.E['wifi']} Running <b>{snap['active_total']} / {snap['max_slots']}</b>

{UI.E['info']} <i>Your download starts automatically when a slot opens.</i>"""


def get_runtime_health() -> Dict[str, Any]:
    """Collect compact runtime diagnostics for the UI and startup logs."""
    ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
    ffmpeg_ok = bool(ffmpeg_path and os.path.exists(ffmpeg_path))
    cookies_ok = os.path.exists(COOKIES_FILE) and os.path.getsize(COOKIES_FILE) > 0 if os.path.exists(COOKIES_FILE) else False
    snap = get_queue_snapshot()
    with active_downloads_lock:
        active_total = len(active_downloads)
    return {
        'token_ok': bool(BOT_TOKEN),
        'ffmpeg_ok': ffmpeg_ok,
        'ffmpeg_path': ffmpeg_path,
        'cookies_ok': cookies_ok,
        'disk_mb': storage_cleaner.get_disk_usage_mb(),
        'active_total': active_total,
        'waiting_total': snap.get('waiting_total', 0),
        'available_slots': snap.get('available_slots', 0),
        'db_ok': os.path.exists(DB_NAME),
        'python_version': platform.python_version(),
        'yt_dlp_version': getattr(yt_dlp, '__version__', 'unknown'),
        'build_version': BUILD_VERSION,
    }


def format_runtime_health_html(user_id: Optional[int] = None) -> str:
    diag = get_runtime_health()
    lane = get_priority_lane_name(user_id) if user_id else 'Standard lane'
    pos, total = get_user_waiting_position(user_id) if user_id else (0, 0)
    queue_line = f"#{pos} / {total}" if pos else 'Not waiting'
    ffmpeg_name = os.path.basename(diag['ffmpeg_path']) if diag.get('ffmpeg_path') else 'missing'
    return f"""{UI.E['tools']} <b>SYSTEM STATUS</b>

{UI.E['check']} Token: <b>{'OK' if diag['token_ok'] else 'MISSING'}</b>
{UI.E['tools']} FFMPEG: <b>{'OK' if diag['ffmpeg_ok'] else 'MISSING'}</b> • <code>{UI.escape_html(ffmpeg_name)}</code>
{UI.E['cookie']} Cookies: <b>{'Loaded' if diag['cookies_ok'] else 'Not loaded'}</b>
{UI.E['server']} Database: <b>{'OK' if diag['db_ok'] else 'MISSING'}</b>
{UI.E['cloud']} Disk: <b>{diag['disk_mb']:.1f} MB</b>
{UI.E['wifi']} Active: <b>{diag['active_total']}</b> • Waiting: <b>{diag['waiting_total']}</b> • Free slots: <b>{diag['available_slots']}</b>
{UI.E['rocket']} Your lane: <b>{lane}</b>
{UI.E['clock']} Queue spot: <b>{queue_line}</b>
{UI.E['tools']} Python: <b>{UI.escape_html(diag['python_version'])}</b> • yt-dlp: <b>{UI.escape_html(diag['yt_dlp_version'])}</b>
{UI.E['sparkles']} Build: <b>{UI.escape_html(diag['build_version'])}</b>

<i>Use direct public links for the most stable delivery.</i>"""


def _wallet_panel_text(user_id: int) -> str:
    wallet = db.get_wallet(user_id)
    next_daily = 'Ready'
    if wallet.get('last_daily_claim'):
        try:
            last_daily = datetime.fromisoformat(wallet['last_daily_claim'])
            next_time = datetime.combine((last_daily + timedelta(days=1)).date(), datetime.min.time())
            remain = max(0, int((next_time - datetime.utcnow()).total_seconds()))
            if remain:
                next_daily = UI.format_duration(remain)
        except Exception:
            next_daily = 'Ready'
    spin_text = 'Ready'
    if wallet.get('last_spin'):
        try:
            last_spin = datetime.fromisoformat(wallet['last_spin'])
            remain = max(0, int((last_spin + timedelta(hours=4) - datetime.utcnow()).total_seconds()))
            if remain:
                spin_text = UI.format_duration(remain)
        except Exception:
            spin_text = 'Ready'
    premium_until = wallet.get('premium_until') or 'Not active'
    premium_line = premium_until[:16].replace('T', ' ') if wallet.get('premium_active') and isinstance(premium_until, str) else 'Not active'
    return f"""{UI.E['wallet']} <b>WALLET</b>

{UI.E['coin']} Coins: <b>{wallet['coins']}</b>   {UI.E['giftbox']} Streak: <b>{wallet['daily_streak']}</b>
{UI.E['video']} Video rewards: <b>{wallet['video_downloads']}</b>
{UI.E['music']} Audio rewards: <b>{wallet['audio_downloads']}</b>
{UI.E['giftbox']} Daily: <b>{next_daily}</b>
{UI.E['game']} Spin: <b>{spin_text}</b>
{UI.E['diamond']} Premium until: <b>{UI.escape_html(premium_line)}</b>
{UI.E['rocket']} Queue passes: <b>{wallet.get('queue_passes', 0)}</b>"""


def _stats_panel_text(user) -> str:
    db.add_user(user)
    user_data = db.get_user_stats(user.id) or {'downloads': 0, 'total_size': 0, 'is_premium': False, 'audio_downloads': 0, 'video_downloads': 0}
    count = int(user_data['downloads'] or 0)
    size_bytes = int(user_data.get('total_size', 0) or 0)
    size = UI.format_size(size_bytes)
    avg_size = UI.format_size(int(size_bytes / count)) if count else '0 B'
    remaining = rate_limiter.get_remaining_downloads(user.id, bool(user_data.get('is_premium')))
    return f"""{UI.E['chart']} <b>STATS</b>

{UI.E['download']} Total: <b>{count}</b>
{UI.E['video']} Videos: <b>{user_data.get('video_downloads', 0)}</b>
{UI.E['music']} Audio: <b>{user_data.get('audio_downloads', 0)}</b>
{UI.E['cloud']} Saved data: <b>{size}</b>
{UI.E['size']} Avg file: <b>{avg_size}</b>
{UI.E['chart']} Remaining/hr: <b>{remaining}</b>
{UI.E['diamond']} Account: <b>{PREM_YES if user_data['is_premium'] else PREM_NO}</b>"""


def _premium_panel_text() -> str:
    login_url = downloader.get_login_url()
    login_text = f"\n{UI.E['link']} <b>2GB Login URL:</b> available from admin / premium desk" if login_url else ""
    return f"""✨ <b>PREMIUM HUB • NEON DELUXE</b>

👑 <b>Higher limits:</b> 100 downloads / hour
🚀 <b>Priority lane:</b> faster queue access
🎬 <b>Cleaner delivery:</b> better MP4 / audio workflow
🎧 <b>Audio studio:</b> MP3 / M4A / OPUS / WAV menu
🛡 <b>Smarter recovery:</b> safer fallback for difficult links
🪄 <b>Modern UI:</b> glossy menus, cleaner previews, better panels

{UI.E['coin']} Redeem with coins or contact admin.{login_text}
<i>Premium lane • glossy interface • safer downloader core</i>"""

# ════════════════════════════════════════════════════════════════════════════
# 🌐 ULTRA DOWNLOAD ENGINE v10.2 (Real-Time Progress + Smart Features)
# ════════════════════════════════════════════════════════════════════════════

class DownloadEngine:
    """Core download engine using yt-dlp with real-time progress hooks."""

    @staticmethod
    def _get_base_opts() -> Dict[str, Any]:
        """
        Get common yt-dlp options.
        Tuned for better reliability across YouTube, Facebook, Instagram and TikTok.
        """
        opts: Dict[str, Any] = {
            'restrictfilenames':    True,
            'noplaylist':           True,
            'quiet':                True,
            'no_warnings':          True,
            'extract_flat':         False,
            'socket_timeout':       45,
            'retries':              5,
            'extractor_retries':    3,
            'fragment_retries':     5,
            'concurrent_fragment_downloads': 1,
            'skip_unavailable_fragments': True,
            'geo_bypass':           True,
            'nocheckcertificate':   True,
            'forceipv4':            True,
            'http_headers': {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/131.0.0.0 Safari/537.36'
                ),
                'Referer': 'https://www.google.com/',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': '*/*',
                'DNT': '1',
            },
            'http_chunk_size': 10485760,
            'extractor_args': {
                'youtube': {
                    'player_client': ['android', 'web', 'tv_embedded'],
                }
            },
        }
        if os.path.exists(COOKIES_FILE):
            opts['cookiefile'] = COOKIES_FILE
        return opts

    @staticmethod
    def _get_candidate_urls(url: str) -> List[str]:
        """Return original + safe fallback URLs for fragile social extractors."""
        candidates: List[str] = []

        def _add(candidate: Optional[str]) -> None:
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        cleaned = url_utils.clean_url(url)
        normalized = url_utils.normalize_platform_url(cleaned)
        _add(url)
        _add(cleaned)
        _add(normalized)

        try:
            parsed = urlparse(normalized)
            scheme = parsed.scheme or 'https'
            netloc = (parsed.netloc or '').lower()
            path = parsed.path or ''

            fb_reel = re.search(r'/reel/(\d+)', path)
            fb_watch = re.search(r'[?&]v=(\d+)', normalized)
            if 'facebook.com' in netloc or 'fb.watch' in netloc or netloc == 'fb.com':
                video_id = fb_reel.group(1) if fb_reel else (fb_watch.group(1) if fb_watch else None)
                if video_id:
                    _add(f'{scheme}://www.facebook.com/reel/{video_id}/')
                    _add(f'{scheme}://www.facebook.com/watch/?v={video_id}')
                    _add(f'{scheme}://m.facebook.com/watch/?v={video_id}')
                    _add(f'{scheme}://mbasic.facebook.com/watch/?v={video_id}')

            if netloc in ('vt.tiktok.com', 'vm.tiktok.com'):
                slug = path.strip('/').split('/', 1)[0]
                if slug:
                    _add(f'{scheme}://{netloc}/{slug}/')
        except Exception:
            pass

        return candidates

    @staticmethod
    def _extract_info_with_fallbacks(url: str, download: bool = False) -> Dict[str, Any]:
        """Try a few yt-dlp option variants and social URL fallbacks before giving up."""
        attempts: List[Dict[str, Any]] = [
            {**DownloadEngine._get_base_opts()},
            {**DownloadEngine._get_base_opts(), 'extractor_args': {}},
        ]
        if os.path.exists(COOKIES_FILE):
            fallback_no_cookies = {**DownloadEngine._get_base_opts()}
            fallback_no_cookies.pop('cookiefile', None)
            attempts.append(fallback_no_cookies)
        last_error = None
        for candidate_url in DownloadEngine._get_candidate_urls(url):
            for attempt_idx, ydl_opts in enumerate(attempts, start=1):
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(candidate_url, download=download)
                        if info:
                            return info
                except Exception as exc:
                    last_error = exc
                    logger.warning(f"yt-dlp info attempt {attempt_idx} failed for {candidate_url}: {exc}")
        if last_error:
            raise last_error
        return {}

    @staticmethod
    def get_video_info(url: str) -> Dict[str, Any]:
        """Fetch complete video info (title, duration, thumbnail, formats, etc.)."""
        try:
            info = DownloadEngine._extract_info_with_fallbacks(url, download=False)
            return info or {}
        except Exception as e:
            logger.error(f"Error getting video info for {url}: {e}")
            return {}

    @staticmethod
    def get_available_formats(url: str) -> List[Dict[str, Any]]:
        """Fetch available formats with normalized resolution metadata."""
        def _parse_height(*values: Any) -> int:
            for value in values:
                if value is None:
                    continue
                if isinstance(value, (int, float)) and value > 0:
                    return int(value)
                text_value = str(value)
                m = re.search(r'(\d{3,4})p', text_value)
                if m:
                    return int(m.group(1))
                m = re.search(r'(\d{3,5})x(\d{3,5})', text_value)
                if m:
                    return int(m.group(2))
            return 0

        try:
            info = DownloadEngine._extract_info_with_fallbacks(url, download=False)
            formats: List[Dict[str, Any]] = []
            seen: set = set()
            if not info or 'formats' not in info:
                return formats

            for f in info['formats']:
                fid = str(f.get('format_id', '')).strip()
                if not fid:
                    continue

                vcodec = f.get('vcodec', 'none')
                acodec = f.get('acodec', 'none')
                width = int(f.get('width') or 0)
                height = _parse_height(f.get('height'), f.get('resolution'), f.get('format_note'))
                ext = str(f.get('ext') or '')
                size = f.get('filesize') or f.get('filesize_approx') or 0
                tbr = f.get('tbr', 0) or 0
                fps = int(f.get('fps') or 0)
                format_note = str(f.get('format_note') or '')

                if vcodec != 'none':
                    resolution = f.get('resolution') or (f"{width}x{height}" if width and height else (f"{height}p" if height else 'unknown'))
                elif acodec != 'none':
                    resolution = 'audio only'
                    height = 0
                else:
                    continue

                dedupe_key = (fid, ext, height, vcodec != 'none')
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                entry: Dict[str, Any] = {
                    'format_id': fid,
                    'resolution': str(resolution),
                    'ext': ext,
                    'filesize': size,
                    'vcodec': vcodec,
                    'acodec': acodec,
                    'tbr': tbr,
                    'fps': fps,
                    'width': width,
                    'height': height,
                    'format_note': format_note,
                }
                formats.append(entry)

            formats.sort(
                key=lambda item: (
                    1 if item.get('vcodec') != 'none' else 0,
                    int(item.get('height') or 0),
                    float(item.get('tbr') or 0),
                ),
                reverse=True,
            )
            return formats
        except Exception as e:
            logger.error(f"Error getting formats for {url}: {e}")
            return []

    @staticmethod
    def get_ydl_opts(output_template: str, format_id: str = "best",
                     cancel_event: Optional[threading.Event] = None,
                     info_dict: Optional[Dict] = None,
                     progress_data: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Build yt-dlp options with portable FFMPEG and progress hooks.
        The selected quality is preserved exactly unless the user picked Auto.
        """
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()

        if format_id == "best":
            fmt = (
                "bestvideo*[ext=mp4]+bestaudio[ext=m4a]/"
                "bestvideo*+bestaudio/best"
            )
        elif format_id in ("audio", "mp3", "opus", "wav"):
            fmt = "bestaudio/best"
        elif format_id == "m4a":
            fmt = "bestaudio[ext=m4a]/bestaudio/best"
        elif any(ch in format_id for ch in "+/[]"):
            fmt = format_id
        else:
            fmt = f"{format_id}+bestaudio/{format_id}/best"

        progress_data = progress_data if progress_data is not None else {}
        progress_data.clear()
        progress_data.update({
            'pct': 0.0,
            'eta': '',
            'speed': '',
            'downloaded': 0,
            'total': 0,
            'filename': '',
            'status': '',
            'cancel': cancel_event,
        })

        def progress_hook(d: Dict[str, Any]) -> None:
            if cancel_event and cancel_event.is_set():
                raise DownloadCancelledException("Download cancelled by user")

            progress_data['status'] = d.get('status', '')
            if d.get('total_bytes'):
                progress_data['total'] = d['total_bytes']
            if d.get('total_bytes_estimate'):
                progress_data['total'] = d['total_bytes_estimate']
            progress_data['downloaded'] = d.get('downloaded_bytes', 0)
            progress_data['filename'] = d.get('filename', '')

            if d.get('speed'):
                speed = d['speed']
                if speed > 1024 * 1024:
                    progress_data['speed'] = f"{speed / 1024 / 1024:.1f} MB/s"
                elif speed > 1024:
                    progress_data['speed'] = f"{speed / 1024:.1f} KB/s"
                else:
                    progress_data['speed'] = f"{speed:.0f} B/s"

            if d.get('eta'):
                eta = d['eta']
                if eta >= 3600:
                    progress_data['eta'] = f"{int(eta // 3600)}h {int((eta % 3600) // 60)}m"
                elif eta >= 60:
                    progress_data['eta'] = f"{int(eta // 60)}m {int(eta % 60)}s"
                else:
                    progress_data['eta'] = f"{int(eta)}s"

            total = progress_data['total']
            downloaded = progress_data['downloaded']
            if total > 0:
                progress_data['pct'] = (downloaded / total) * 100

        opts: Dict[str, Any] = {
            'format': fmt,
            'outtmpl': output_template,
            'merge_output_format': 'mp4',
            'ffmpeg_location': ffmpeg_exe,
            'writethumbnail': False,
            'postprocessors': [],
            'progress_hooks': [progress_hook],
            'format_sort': ['res', 'fps', 'codec:h264', 'ext:mp4:m4a'],
            **DownloadEngine._get_base_opts(),
        }

        if format_id in ("audio", "mp3", "m4a", "opus", "wav"):
            codec_map: Dict[str, str] = {
                'audio': 'mp3',
                'mp3': 'mp3',
                'm4a': 'm4a',
                'opus': 'opus',
                'wav': 'wav',
            }
            codec = codec_map.get(format_id, 'mp3')
            audio_meta: Dict[str, Any] = {
                'key': 'FFmpegExtractAudio',
                'preferredcodec': codec,
            }
            if codec == 'mp3':
                audio_meta['preferredquality'] = '320'
            opts['postprocessors'].append(audio_meta)
            if info_dict:
                opts['addmetadata'] = True
            opts['merge_output_format'] = codec

        return opts, progress_data

    @staticmethod
    def _find_downloaded_file(user_id: int, ts: int) -> Optional[str]:
        """
        Find the final downloaded file, skipping .part/.temp files.
        v8 FIX: Properly filter out temporary files.
        """
        if not os.path.exists(DOWNLOAD_DIR):
            return None

        prefix = f"{user_id}_{ts}"
        final_file = None
        temp_files: List[str] = []

        for fname in os.listdir(DOWNLOAD_DIR):
            if fname.startswith(prefix):
                fpath = os.path.join(DOWNLOAD_DIR, fname)
                if not os.path.isfile(fpath):
                    continue
                # v8 FIX: Skip .part, .temp, .ytdl, .download partial files
                skip_extensions = ('.part', '.temp', '.ytdl', '.download', '.tmp')
                if any(fname.endswith(ext) for ext in skip_extensions):
                    temp_files.append(fpath)
                    continue
                # If multiple files (merged + original), pick largest
                if final_file is None or os.path.getsize(fpath) > os.path.getsize(final_file):
                    final_file = fpath

        # Clean up temp files
        for tf in temp_files:
            try:
                if os.path.exists(tf):
                    os.remove(tf)
            except Exception:
                pass

        return final_file

    @staticmethod
    def process_url(url: str, user_id: int, format_id: str = "best",
                    cancel_event: Optional[threading.Event] = None,
                    info_dict: Optional[Dict] = None,
                    shared_progress_data: Optional[Dict[str, Any]] = None,
                    retry_count: int = 0) -> Tuple[Optional[str], Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Download video with real-time progress tracking.
        v8: Added retry logic, enhanced metadata, no-audio handling.
        Returns (file_path, info_dict_result, progress_callback_data).
        """
        start_t = time.time()
        ts = int(time.time())
        save_template = os.path.join(DOWNLOAD_DIR, f"{user_id}_{ts}.%(ext)s")

        try:
            storage_cleaner.clean_user_temp_files(user_id)
            opts, progress_data = DownloadEngine.get_ydl_opts(
                save_template, format_id, cancel_event, info_dict, shared_progress_data
            )

            last_error: Optional[Exception] = None
            info: Dict[str, Any] = {}
            for candidate_url in DownloadEngine._get_candidate_urls(url):
                try:
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(candidate_url, download=True)
                    if info:
                        break
                except Exception as exc:
                    last_error = exc
                    logger.warning(f"Download attempt failed for {candidate_url}: {exc}")
            if not info:
                if last_error:
                    raise last_error
                raise RuntimeError('Download extractor returned no data.')

            title    = info.get('title', 'Downloaded Video')
            platform = info.get('extractor_key', 'Web')
            duration = info.get('duration', 0)

            # v8 FIX: Use proper file finder that skips .part/.temp
            final_file = DownloadEngine._find_downloaded_file(user_id, ts)

            if not final_file or not os.path.exists(final_file):
                # If no file found, check for any file with the prefix (including .part)
                # and try to find a merge candidate
                if os.path.exists(DOWNLOAD_DIR):
                    prefix = f"{user_id}_{ts}"
                    for fname in os.listdir(DOWNLOAD_DIR):
                        if fname.startswith(prefix) and not fname.endswith(('.part', '.temp', '.ytdl')):
                            fpath = os.path.join(DOWNLOAD_DIR, fname)
                            if os.path.isfile(fpath) and os.path.getsize(fpath) > 0:
                                final_file = fpath
                                break

            if not final_file or not os.path.exists(final_file):
                error_msg = 'File not found after download. The file may be too large or format unsupported.'
                # v8: If retries available, signal retry
                if retry_count < MAX_DOWNLOAD_RETRIES and format_id == 'best':
                    return None, {'error': error_msg, 'retry': True}, None
                return None, {'error': error_msg}, None

            file_size = os.path.getsize(final_file)
            proc_time = time.time() - start_t

            requested = info.get('requested_formats') or []
            primary_requested = requested[0] if isinstance(requested, list) and requested else info
            actual_width = int((primary_requested or {}).get('width') or info.get('width') or 0)
            actual_height = int((primary_requested or {}).get('height') or info.get('height') or 0)
            actual_ext = os.path.splitext(final_file)[1].lstrip('.') or info.get('ext', '')

            v_info: Dict[str, Any] = {
                'title':           title,
                'platform':        platform,
                'duration':        duration,
                'quality':         format_id,
                'file_size':       file_size,
                'processing_time': proc_time,
                'thumbnail':       info.get('thumbnail', ''),
                'retry_count':     retry_count,
                'uploader':        info.get('uploader', ''),
                'width':           actual_width,
                'height':          actual_height,
                'ext':             actual_ext,
            }

            # Calculate average download speed
            if proc_time > 0:
                v_info['avg_speed'] = UI.format_speed(file_size, proc_time)

            return final_file, v_info, progress_data

        except DownloadCancelledException:
            logger.info(f"Download cancelled by user {user_id}")
            return None, {'error': 'Download was cancelled by user.', 'cancelled': True}, None
        except Exception as outer_e:
            # v8 FIX: Handle the case where DownloadCancelled doesn't exist
            # (already handled above with our custom exception, but keep for safety)
            err_str = str(outer_e)
            if 'cancelled' in err_str.lower():
                logger.info(f"Download cancelled by user {user_id}")
                return None, {'error': 'Download was cancelled by user.', 'cancelled': True}, None

            logger.error(f"Download Error (attempt {retry_count + 1}): {err_str}")
            logger.debug(traceback.format_exc())

            # Clean error messages
            if 'File too large' in err_str:
                return None, {'error': 'The requested file is too large for the server.'}, None
            if 'Private video' in err_str or 'Video unavailable' in err_str:
                return None, {'error': 'This video is private or unavailable.'}, None
            if 'Sign in' in err_str:
                return None, {'error': 'This video requires sign-in. Upload cookies.txt in admin panel.'}, None

            # Retry with different format if possible
            if retry_count < MAX_DOWNLOAD_RETRIES and format_id == "best":
                logger.info(f"Retrying auto-quality download for user {user_id}, attempt {retry_count + 2}")
                platform_name = url_utils.detect_platform(url).lower()
                if platform_name in ('instagram', 'facebook'):
                    alt_format = "best[ext=mp4]/bestvideo*[ext=mp4]+bestaudio[ext=m4a]/best"
                else:
                    alt_format = (
                        "bestvideo*[height<=1080][ext=mp4]+bestaudio[ext=m4a]/"
                        "bestvideo*[height<=1080]+bestaudio/best[height<=1080]/best"
                    )
                return DownloadEngine.process_url(
                    url, user_id, alt_format, cancel_event, info_dict, shared_progress_data, retry_count + 1
                )

            if format_id not in ("best", "audio", "mp3", "m4a") and not any(ch in format_id for ch in '+/[]'):
                return None, {'error': 'The exact selected quality could not be downloaded safely. Please choose another quality button.'}, None
            return None, {'error': err_str[:300]}, None

    @staticmethod
    def download_thumbnail(url: str, user_id: int) -> Optional[str]:
        """Download thumbnail image for a video URL."""
        try:
            ts = int(time.time())
            ydl_opts: Dict[str, Any] = {
                **DownloadEngine._get_base_opts(),
                'skip_download':  True,
                'writethumbnail': True,
                'outtmpl':        os.path.join(TEMP_DIR, f"thumb_{user_id}_{ts}.%(ext)s"),
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(url, download=True)

            prefix = f"thumb_{user_id}_{ts}"
            if os.path.exists(TEMP_DIR):
                for fname in os.listdir(TEMP_DIR):
                    if fname.startswith(prefix) and not fname.endswith(('.temp', '.part')):
                        return os.path.join(TEMP_DIR, fname)
        except Exception as e:
            logger.warning(f"Could not download thumbnail: {e}")
        return None

    @staticmethod
    def download_subtitles(url: str, user_id: int) -> Optional[str]:
        """v8.0: Download subtitles for a video URL."""
        try:
            ts = int(time.time())
            ydl_opts: Dict[str, Any] = {
                **DownloadEngine._get_base_opts(),
                'skip_download':    True,
                'writesubtitles':   True,
                'writeautomaticsub': True,
                'subtitleslangs':   ['en', 'en-US', 'en.*', '.*'],
                'outtmpl':          os.path.join(TEMP_DIR, f"sub_{user_id}_{ts}"),
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(url, download=True)

            prefix = f"sub_{user_id}_{ts}"
            if os.path.exists(TEMP_DIR):
                for fname in os.listdir(TEMP_DIR):
                    if fname.startswith(prefix):
                        return os.path.join(TEMP_DIR, fname)
        except Exception as e:
            logger.warning(f"Could not download subtitles: {e}")
        return None

    @staticmethod
    def estimate_file_size(formats: List[Dict], format_id: str) -> int:
        """Estimate file size for a given format."""
        for f in formats:
            if f.get('format_id') == format_id:
                return f.get('filesize') or f.get('filesize_approx') or 0
        return 0

    @staticmethod
    def get_login_url() -> Optional[str]:
        """v8.0: Generate Telegram login URL for 2GB upload access."""
        if LOGIN_URL_SUPPORTED:
            return f"https://t.me/{BOT_USERNAME.lstrip('@')}?startattach=upload"
        return None


downloader = DownloadEngine()


# ════════════════════════════════════════════════════════════════════════════
# 🤖 BOT HANDLERS v8
# ════════════════════════════════════════════════════════════════════════════

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command — show a compact mobile-friendly welcome message."""
    user = update.effective_user
    db.add_user(user)
    await update.message.reply_html(
        UI.welcome(user, db.get_stats()),
        reply_markup=UI.main_menu(user.id == ADMIN_ID),
        disable_web_page_preview=True
    )


async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show runtime health and queue diagnostics."""
    user = update.effective_user
    db.add_user(user)
    await update.message.reply_html(
        format_runtime_health_html(user.id),
        reply_markup=UI.back_btn(),
        disable_web_page_preview=True
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a compact quick guide from a command."""
    user = update.effective_user
    db.add_user(user)
    await update.message.reply_html(
        f"""<b>{UI.E['help']} QUICK GUIDE</b>

1. Send one public link
2. Pick exact quality or audio
3. Wait for delivery

{UI.E['info']} 720p / 480p is safest when Telegram size limits hit.
{UI.E['clock']} Session timeout: 10 min.
{UI.E['sparkles']} Shortcuts: <code>/queue</code> <code>/wallet</code> <code>/stats</code> <code>/premium</code> <code>/bonus</code> <code>/spin</code> <code>/ping</code>""",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{UI.E['globe']} Platforms", callback_data="platforms"), InlineKeyboardButton(f"{UI.E['music']} Audio", callback_data="audio_help")],
            [InlineKeyboardButton("🩺 Health", callback_data="show_health"), InlineKeyboardButton(f"{UI.E['wifi']} Queue", callback_data="queue_status")],
            [InlineKeyboardButton(f"{UI.E['back']} Home", callback_data="start_menu")],
        ]),
        disable_web_page_preview=True
    )


async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Small quick diagnostics card."""
    user = update.effective_user
    db.add_user(user)
    diag = get_runtime_health()
    await update.message.reply_html(
        f"{UI.E['signal']} <b>PONG</b>\n\n"
        f"{UI.E['sparkles']} Build: <b>{UI.escape_html(BUILD_VERSION)}</b>\n"
        f"{UI.E['wifi']} Active: <b>{diag['active_total']}</b> • Waiting: <b>{diag['waiting_total']}</b>\n"
        f"{UI.E['clock']} Uptime: <b>{UI.format_uptime()}</b>\n"
        f"{UI.E['tools']} Python: <b>{UI.escape_html(diag['python_version'])}</b>",
        reply_markup=UI.back_btn(),
        disable_web_page_preview=True
    )


async def queue_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show queue snapshot from a command."""
    user = update.effective_user
    db.add_user(user)
    await update.message.reply_html(
        format_queue_snapshot_html(user.id),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="queue_status"), InlineKeyboardButton(f"{UI.E['diamond']} Premium", callback_data="premium")],
            [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
        ]),
        disable_web_page_preview=True
    )


async def wallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show wallet panel from a command."""
    user = update.effective_user
    db.add_user(user)
    await update.message.reply_html(
        _wallet_panel_text(user.id),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{UI.E['giftbox']} Daily", callback_data="daily_bonus"), InlineKeyboardButton(f"{UI.E['game']} Spin", callback_data="spin_wheel")],
            [InlineKeyboardButton(f"{UI.E['diamond']} Premium", callback_data="premium"), InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
        ]),
        disable_web_page_preview=True
    )


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show personal stats from a command."""
    user = update.effective_user
    db.add_user(user)
    await update.message.reply_html(
        _stats_panel_text(user),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{UI.E['memo']} History", callback_data="history"), InlineKeyboardButton(f"{UI.E['star']} Profile", callback_data="profile")],
            [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
        ]),
        disable_web_page_preview=True
    )


async def premium_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show premium hub from a command."""
    user = update.effective_user
    db.add_user(user)
    await update.message.reply_html(
        _premium_panel_text(),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("👑 1 Day", callback_data="redeem_premium_day"), InlineKeyboardButton("💎 3 Days", callback_data="redeem_premium_3day")],
            [InlineKeyboardButton("🚀 Queue Pass", callback_data="redeem_queue_pass"), InlineKeyboardButton(f"{UI.E['wallet']} Wallet", callback_data="wallet")],
            [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
        ]),
        disable_web_page_preview=True
    )


async def bonus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Claim daily bonus from a command."""
    user = update.effective_user
    db.add_user(user)
    result = db.claim_daily_bonus(user.id)
    if result.get('ok'):
        text = f"""{UI.E['giftbox']} <b>DAILY BONUS CLAIMED!</b>

{UI.E['coin']} You received <b>{result['reward']} coins</b>.
{UI.E['fire']} Current streak: <b>{result['streak']} day(s)</b>
{UI.E['wallet']} New balance: <b>{result['coins']} coins</b>"""
    else:
        remain = UI.format_duration(result.get('remaining_seconds', 0))
        text = f"""{UI.E['warning']} <b>Daily Bonus already claimed.</b>

{UI.E['clock']} Come back in <b>{remain}</b>.
{UI.E['wallet']} Balance: <b>{result.get('coins', 0)} coins</b>"""
    await update.message.reply_html(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
            [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
        ]),
        disable_web_page_preview=True
    )


async def spin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Spin reward wheel from a command."""
    user = update.effective_user
    db.add_user(user)
    result = db.spin_reward(user.id)
    if result.get('ok'):
        text = f"""{UI.E['game']} <b>SPIN COMPLETE!</b>

{UI.E['coin']} Wheel reward: <b>{result['reward']} coins</b>
{UI.E['wallet']} New balance: <b>{result['coins']} coins</b>

{UI.E['info']} <i>Spin again after the cooldown ends.</i>"""
    else:
        remain = UI.format_duration(result.get('remaining_seconds', 0))
        text = f"""{UI.E['warning']} <b>Spin cooldown active.</b>

{UI.E['clock']} Try again in <b>{remain}</b>.
{UI.E['wallet']} Balance: <b>{result.get('coins', 0)} coins</b>"""
    await update.message.reply_html(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
            [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
        ]),
        disable_web_page_preview=True
    )


async def info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /info <url> — Show video info without downloading."""
    user = update.effective_user
    db.add_user(user)

    if not context.args:
        return await update.message.reply_html(
            f"{UI.E['cross']} <b>Usage:</b> <code>/info &lt;video_url&gt;</code>\n\n"
            f"Example:\n"
            f"<code>/info https://youtube.com/watch?v=...</code>",
            reply_markup=UI.back_btn()
        )

    url = url_utils.clean_url(' '.join(context.args))
    url = url_utils.normalize_platform_url(url)
    if not url:
        return await update.message.reply_html(
            f"{UI.E['cross']} <b>Invalid URL!</b>",
            reply_markup=UI.back_btn()
        )

    # v8: Show chat action while fetching
    await context.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

    status = await update.message.reply_html(
        f"{UI.E['search']} <b>Fetching video info...</b>\n{UI.E['loading']} <i>Please wait...</i>"
    )

    loop = asyncio.get_running_loop()
    try:
        info = await loop.run_in_executor(pool, DownloadEngine.get_video_info, url)
    except Exception as e:
        return await status.edit_text(
            f"{UI.E['cross']} <b>Error:</b> {UI.escape_html(str(e)[:200])}",
            parse_mode=ParseMode.HTML
        )

    if not info or not info.get('title'):
        platform = url_utils.detect_platform(url)
        error_tips = UI.get_platform_error_tips("Could not fetch video info.", platform)
        return await status.edit_text(
            f"{UI.E['cross']} <b>Could not fetch video info.</b>\n"
            f"Make sure the URL is correct and the video is public.\n\n"
            f"{error_tips}",
            parse_mode=ParseMode.HTML,
            reply_markup=UI.back_btn()
        )

    url_hash = _remember_download_session(context, url, info=info)

    text, kb, thumbnail_url = UI.video_info_card(info, url_hash)

    try:
        if thumbnail_url:
            await status.delete()
            await update.message.reply_photo(
                photo=thumbnail_url,
                caption=text,
                reply_markup=kb,
                parse_mode=ParseMode.HTML
            )
        else:
            await status.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception:
        try:
            await status.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Info card error: {e}")


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /history — Show user's download history."""
    user = update.effective_user
    db.add_user(user)

    history = db.get_user_history(user.id, limit=10)
    if not history:
        return await update.message.reply_html(
            f"{UI.E['folder']} <b>Download History</b>\n\n"
            f"{UI.E['info']} No downloads yet!\n"
            f"Send a video link to get started.",
            reply_markup=UI.back_btn()
        )

    text = (
        f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
        f"\u2551  {UI.E['folder']} <b>YOUR DOWNLOAD HISTORY</b>\n"
        f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
    )

    for i, item in enumerate(history, 1):
        title = UI.escape_html(UI.truncate(item.get('video_title', 'Unknown'), 40))
        platform = item.get('platform', '?')
        quality = item.get('quality', '?')
        size = UI.format_size(item.get('file_size', 0))
        status_icon = "\u2705" if item.get('status') == 'success' else "\u274c"
        timestamp = item.get('timestamp', '?')

        if 'T' in str(timestamp):
            timestamp = str(timestamp).split('T')[0]

        text += (
            f"<b>{i}.</b> {status_icon} {title}\n"
            f"    {UI.E['globe']} {platform} | {UI.E['hd']} {quality} | "
            f"{UI.E['size']} {size} | {UI.E['calendar']} {timestamp}\n\n"
        )

    text += f"{UI.E['info']} <i>Showing last 10 downloads</i>"
    await update.message.reply_html(text, reply_markup=UI.back_btn())


def _check_session_timeout(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """v8.0: Check if the user's download session has timed out."""
    session_time = context.user_data.get('session_time')
    if session_time and (time.time() - session_time) > SESSION_TIMEOUT_SECONDS:
        return True
    return False


def _prune_download_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    sessions = context.user_data.get('download_sessions') or {}
    if not sessions:
        return
    now = time.time()
    max_age = max(SESSION_TIMEOUT_SECONDS, DOWNLOAD_SESSION_KEEP_SECONDS)
    for url_hash, session in list(sessions.items()):
        last_used = float(session.get('last_used') or session.get('created_at') or 0)
        if not session.get('url') or (last_used and now - last_used > max_age):
            sessions.pop(url_hash, None)
    if sessions:
        context.user_data['download_sessions'] = sessions
    else:
        context.user_data.pop('download_sessions', None)


def _remember_download_session(context: ContextTypes.DEFAULT_TYPE, url: str, info: Optional[Dict[str, Any]] = None,
                              formats: Optional[List[Dict[str, Any]]] = None) -> str:
    _prune_download_sessions(context)
    url_hash = hashlib.md5(url.encode()).hexdigest()
    sessions = context.user_data.setdefault('download_sessions', {})
    session = sessions.get(url_hash, {})
    session['url'] = url
    session.setdefault('created_at', time.time())
    session['last_used'] = time.time()
    if info is not None:
        session['info'] = info
    if formats is not None:
        session['formats'] = formats
    sessions[url_hash] = session

    context.user_data['current_session_hash'] = url_hash
    context.user_data['temp_url'] = url
    context.user_data['session_time'] = session['last_used']
    if info is not None:
        context.user_data['temp_info'] = info
    if formats is not None:
        context.user_data['available_formats'] = {url_hash: formats}
    return url_hash


def _load_download_session(context: ContextTypes.DEFAULT_TYPE, url_hash: str) -> Optional[Dict[str, Any]]:
    _prune_download_sessions(context)
    sessions = context.user_data.get('download_sessions') or {}
    session = sessions.get(url_hash)
    if session and session.get('url'):
        session['last_used'] = time.time()
        context.user_data['current_session_hash'] = url_hash
        context.user_data['temp_url'] = session['url']
        context.user_data['session_time'] = session['last_used']
        if 'info' in session:
            context.user_data['temp_info'] = session.get('info') or {}
        if 'formats' in session:
            context.user_data['available_formats'] = {url_hash: session.get('formats') or []}
        return session

    temp_url = context.user_data.get('temp_url')
    if temp_url and hashlib.md5(temp_url.encode()).hexdigest() == url_hash:
        fallback = {
            'url': temp_url,
            'info': context.user_data.get('temp_info') or {},
            'formats': (context.user_data.get('available_formats') or {}).get(url_hash, []),
            'created_at': context.user_data.get('session_time') or time.time(),
            'last_used': time.time(),
        }
        sessions[url_hash] = fallback
        context.user_data['download_sessions'] = sessions
        return fallback
    return None


def _build_exact_format_selector(selected_info: Optional[Dict[str, Any]], fallback_format_id: str = "best") -> str:
    """Create a safer yt-dlp selector for the chosen quality button."""
    if not selected_info:
        return fallback_format_id

    fid = str(selected_info.get('format_id') or '').strip()
    if not fid:
        return fallback_format_id

    vcodec = str(selected_info.get('vcodec') or 'none').lower()
    acodec = str(selected_info.get('acodec') or 'none').lower()
    ext = str(selected_info.get('ext') or '').lower()

    if vcodec == 'none':
        if ext == 'm4a':
            return 'bestaudio[ext=m4a]/bestaudio/best'
        return 'bestaudio/best'

    if acodec != 'none':
        return fid

    if ext == 'mp4':
        return f"{fid}+bestaudio[ext=m4a]/{fid}+bestaudio/{fid}/best"
    return f"{fid}+bestaudio/{fid}/best"


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle text messages — URL detection and processing."""
    user = update.effective_user
    msg = update.message.text

    # Register/Update user
    db.add_user(user)

    # v8: Ban check with reason and appeal info
    if db.is_banned(user.id):
        ban_info = db.get_ban_info(user.id)
        reason = ban_info['reason'] if ban_info else "Violation of terms"
        ban_date = ban_info['date'] if ban_info else "Unknown"
        return await update.message.reply_html(
            f"{UI.E['lock']} <b>You have been banned from using this bot.</b>\n\n"
            f"{UI.E['shield']} <b>Reason:</b> <i>{UI.escape_html(reason)}</i>\n"
            f"{UI.E['calendar']} <b>Date:</b> {ban_date}\n\n"
            f"{UI.E['info']} <i>If you believe this is a mistake, contact the bot admin to appeal.</i>",
            reply_markup=UI.back_btn()
        )

    # Rate limit check
    if not rate_limiter.check_msg_limit(user.id):
        return await update.message.reply_html(
            f"{UI.E['warning']} <b>Too many requests!</b>\n"
            f"Please wait a moment before trying again.\n"
            f"Limit: {RATE_LIMIT_MESSAGES} messages/minute"
        )

    # Maintenance check
    if db.get_setting("maintenance") == "1" and user.id != ADMIN_ID:
        return await update.message.reply_html(
            f"{UI.E['tools']} <b>Bot is under maintenance.</b>\n"
            f"Please try again later.",
            reply_markup=UI.back_btn()
        )

    # Admin broadcast state
    if user.id == ADMIN_ID and context.user_data.get('state') == 'broadcast':
        return await execute_broadcast(update, context, msg)

    # Admin state handlers
    admin_states = ('set_premium', 'ban_user', 'user_lookup', 'clear_stats',
                    'mass_ban', 'mass_unban', 'gift_coins')
    if user.id == ADMIN_ID and context.user_data.get('state') in admin_states:
        return await handle_admin_user_action(update, context, msg)

    # URL detection
    url = url_utils.detect_url(msg)
    if not url:
        return await update.message.reply_html(
            f"{UI.E['cross']} <b>Please send a valid video link!</b>\n\n"
            f"{UI.E['info']} <i>Supported: YouTube, TikTok, Instagram, Facebook, Twitter, and 1000+ more!</i>",
            reply_markup=UI.back_btn()
        )

    # Clean URL
    url = url_utils.clean_url(url)
    url = url_utils.normalize_platform_url(url)

    share_warning = url_utils.get_share_url_warning(url)
    if share_warning:
        return await update.message.reply_html(share_warning, reply_markup=UI.back_btn())

    url_hash = _remember_download_session(context, url)

    # Check download rate limit without consuming a slot yet
    is_prem = db.is_premium(user.id)
    if not rate_limiter.can_download(user.id, is_prem):
        remaining = rate_limiter.get_remaining_downloads(user.id, is_prem)
        if remaining <= 0:
            limit = RATE_LIMIT_DOWNLOADS_PREMIUM if is_prem else RATE_LIMIT_DOWNLOADS
            return await update.message.reply_html(
                f"{UI.E['warning']} <b>Hourly download limit reached!</b>\n\n"
                f"Limit: <b>{limit}/hour</b>\n"
                f"Status: {PREM_YES if is_prem else PREM_NO}\n\n"
                f"{UI.E['clock']} <i>Please wait and try again later.</i>\n"
                f"{UI.E['diamond']} <i>Upgrade to Premium for higher limits!</i>",
                reply_markup=UI.back_btn()
            )

    # Detect YouTube Shorts / playlist
    is_shorts = url_utils.is_youtube_shorts(url)
    is_playlist = url_utils.is_playlist(url)

    # v8: Show chat action
    await context.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

    # Session pruning / timeout handling
    _prune_download_sessions(context)
    if _check_session_timeout(context):
        context.user_data.pop('temp_info', None)
        context.user_data.pop('available_formats', None)
        context.user_data.pop('selected_quality_info', None)

    # Initialize fetching_msg
    fetching_msg = None

    # Check if user wants to see video info first
    show_info = db.get_setting("show_video_info") == "1"

    if show_info and not is_playlist:
        fetching_msg = await update.message.reply_html(
            f"{UI.E['search']} <b>Fetching video info...</b>\n"
            f"{UI.E['loading']} <i>Please wait...</i>"
            f"{SHORTS_TAG if is_shorts else ''}"
        )

        loop = asyncio.get_running_loop()
        try:
            info = await loop.run_in_executor(pool, DownloadEngine.get_video_info, url)
        except Exception:
            info = {}

        if not info and url_utils.detect_platform(url).lower() in ('instagram', 'facebook'):
            try:
                await fetching_msg.edit_text(
                    f"{UI.E['warning']} <b>Social media link needs extra checking...</b>\n"
                    f"{UI.E['info']} Trying fallback extraction. Public links work best.\n"
                    f"{UI.E['cookie']} Private/login-only posts may need <code>cookies.txt</code>.",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass

        if info and info.get('title'):
            url_hash = _remember_download_session(context, url, info=info)

            # Pre-fetch formats for quality selection
            formats = await loop.run_in_executor(
                pool, DownloadEngine.get_available_formats, url
            )
            if formats:
                _remember_download_session(context, url, info=info, formats=formats)

            text, kb, thumbnail_url = UI.video_info_card(info, url_hash)

            # v8: Pre-download thumbnail during info display
            thumbnail_local_path = None
            if thumbnail_url:
                try:
                    thumbnail_local_path = await loop.run_in_executor(
                        pool, downloader.download_thumbnail, url, user.id
                    )
                except Exception:
                    pass

            try:
                if thumbnail_local_path and os.path.exists(thumbnail_local_path):
                    await fetching_msg.delete()
                    with open(thumbnail_local_path, 'rb') as thumb_f:
                        await update.message.reply_photo(
                            photo=thumb_f,
                            caption=text,
                            reply_markup=kb,
                            parse_mode=ParseMode.HTML
                        )
                    # Clean up local thumbnail
                    try:
                        os.remove(thumbnail_local_path)
                    except Exception:
                        pass
                elif thumbnail_url:
                    await fetching_msg.delete()
                    await update.message.reply_photo(
                        photo=thumbnail_url,
                        caption=text,
                        reply_markup=kb,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await fetching_msg.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
                return
            except Exception:
                try:
                    await fetching_msg.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
                    return
                except Exception:
                    pass

    # v8 FIX: fetching_msg is guaranteed to be initialized here
    if fetching_msg is None:
        fetching_msg = await update.message.reply_html(
            f"{UI.E['search']} <b>Fetching available qualities...</b>\n"
            f"{UI.E['loading']} <i>Please wait...</i>"
            f"{SHORTS_TAG if is_shorts else ''}"
        )

    # Quality selection mode
    if db.get_setting("allow_quality_select") == "1":
        loop = asyncio.get_running_loop()
        formats = await loop.run_in_executor(
            pool, DownloadEngine.get_available_formats, url
        )
        if formats:
            url_hash = _remember_download_session(context, url, formats=formats)

            info = (context.user_data.get('temp_info') or (_load_download_session(context, url_hash) or {}).get('info') or {})
            header, kb = UI.quality_kb(formats, url_hash, info)

            # v13: Show live queue snapshot
            snap = get_queue_snapshot()
            queue_total = snap['active_total'] + 1
            header += (
                f"\n\n{UI.E['chart']} <b>Live load:</b> {snap['active_total']} / {snap['max_slots']} active\n"
                f"{UI.E['rocket']} <b>Open slots:</b> {snap['available_slots']}"
            )

            await fetching_msg.edit_text(header, reply_markup=kb, parse_mode=ParseMode.HTML)
        else:
            await fetching_msg.edit_text(
                f"{UI.E['warning']} <b>Could not fetch qualities.</b>\n"
                f"Downloading best quality...",
                parse_mode=ParseMode.HTML
            )
            await execute_download(update, context, url, "best")
    else:
        await execute_download(update, context, url, "best")


async def execute_download(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          url: str, format_id: str) -> None:
    """Core download + send logic with priority queue, cancel support and real-time progress."""
    user = update.effective_user
    message = update.callback_query.message if update.callback_query else update.message

    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass

    db.add_user(user)

    if db.is_banned(user.id):
        ban_info = db.get_ban_info(user.id)
        reason = ban_info['reason'] if ban_info else "Violation of terms"
        return await message.reply_html(
            f"{UI.E['lock']} <b>You are banned from this bot.</b>\nReason: <i>{UI.escape_html(reason)}</i>",
            parse_mode=ParseMode.HTML
        )

    if _check_session_timeout(context):
        return await message.reply_html(
            f"{UI.E['warning']} <b>Session Expired!</b>\n\n"
            f"{UI.E['info']} Your download session has expired (10 min timeout).\n"
            f"Please send the link again.",
            reply_markup=UI.back_btn()
        )

    prune_stale_queue_entries()
    with active_downloads_lock:
        already_active = user.id in active_downloads
    with queued_downloads_lock:
        already_waiting = user.id in queued_downloads
    if already_active or already_waiting:
        return await message.reply_html(
            f"{UI.E['warning']} <b>You already have a job running or waiting.</b>\n\n"
            f"{UI.E['info']} Cancel the current one first or wait until it finishes.",
            reply_markup=UI.back_btn()
        )

    cancel_event = threading.Event()
    entry = {
        'user_id': user.id,
        'created_at': time.time(),
        'ticket': next(queue_ticket_counter),
        'priority_rank': get_user_priority_rank(user.id),
        'format_id': format_id,
        'cancel': cancel_event,
    }
    with queued_downloads_lock:
        queued_downloads[user.id] = entry

    wait_msg = await message.reply_html(build_waiting_text(user.id), reply_markup=UI.cancel_btn())

    async def wait_for_turn() -> Tuple[int, int]:
        while True:
            prune_stale_queue_entries()
            if cancel_event.is_set():
                with queued_downloads_lock:
                    queued_downloads.pop(user.id, None)
                return 0, 0

            with queued_downloads_lock:
                waiting = sorted(queued_downloads.values(), key=lambda e: (int(e.get('priority_rank', 99)), float(e.get('created_at', 0)), int(e.get('ticket', 0))))
                my_pos = 0
                my_total = len(waiting)
                for idx, item in enumerate(waiting, 1):
                    if item.get('user_id') == user.id:
                        my_pos = idx
                        break

            try:
                await wait_msg.edit_text(build_waiting_text(user.id), parse_mode=ParseMode.HTML, reply_markup=UI.cancel_btn())
            except Exception:
                pass

            with active_downloads_lock:
                active_now = len(active_downloads)
            if my_pos == 1 and active_now < MAX_CONCURRENT_DOWNLOADS:
                with queued_downloads_lock:
                    queued_downloads.pop(user.id, None)
                return my_pos, my_total
            await asyncio.sleep(1.0)

    queue_pos, queue_total = await wait_for_turn()
    if cancel_event.is_set() or queue_pos == 0:
        try:
            await wait_msg.edit_text(
                f"{UI.E['check']} <b>Cancelled.</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=UI.back_btn()
            )
        except Exception:
            pass
        return

    if entry['priority_rank'] == 1:
        try:
            db.consume_queue_pass(user.id)
        except Exception:
            pass

    rate_limiter.record_download(user.id)
    context.user_data['_slot_consumed'] = True

    storage_cleaner.clean_user_temp_files(user.id)

    with active_downloads_lock:
        active_downloads[user.id] = {
            'cancel': cancel_event,
            'message': message,
            'progress_data': None,
            'start_time': time.time(),
        }

    try:
        try:
            await wait_msg.delete()
        except Exception:
            pass
        async with download_semaphore:
            return await _do_download(
                update, context, url, format_id, user, message, cancel_event, max(1, queue_pos)
            )
    finally:
        with queued_downloads_lock:
            queued_downloads.pop(user.id, None)
        with active_downloads_lock:
            active_downloads.pop(user.id, None)


async def _do_download(update: Update, context: ContextTypes.DEFAULT_TYPE,
                       url: str, format_id: str, user, message,
                       cancel_event: threading.Event,
                       queue_pos: int = 1) -> None:
    """Internal download execution with real-time progress."""
    # v8: Show appropriate chat action for different stages
    await context.bot.send_chat_action(message.chat_id, ChatAction.TYPING)

    status = await message.reply_html(
        UI.progress_text(1, context=context,
                         queue_pos=queue_pos, queue_total=max(queue_pos, len(active_downloads)))
    )

    async def update_progress(step: int, info: Optional[Dict] = None,
                              pct: Optional[float] = None) -> None:
        """Update progress message, handling errors gracefully."""
        try:
            if status.message_id:
                with active_downloads_lock:
                    q_total = len(active_downloads)
                await status.edit_text(
                    UI.progress_text(step, info, context=context,
                                     progress_pct=pct,
                                     queue_pos=queue_pos, queue_total=max(q_total, queue_pos)),
                    parse_mode=ParseMode.HTML
                )
        except Exception:
            pass

    await asyncio.sleep(0.3)
    await update_progress(2)
    await context.bot.send_chat_action(message.chat_id, ChatAction.TYPING)
    await asyncio.sleep(0.3)

    # v8: Start real-time progress updater task
    progress_update_task: Optional[asyncio.Task] = None

    async def real_time_progress_updater() -> None:
        """
        Background task to update progress bar while downloading.
        v8 FIX: This now ACTUALLY works because progress_data is stored
        in active_downloads[user.id] before the download starts.
        """
        nonlocal cancel_event
        while not cancel_event.is_set():
            await asyncio.sleep(1.5)
            try:
                with active_downloads_lock:
                    ad = active_downloads.get(user.id)
                if ad and ad.get('progress_data'):
                    pd = ad['progress_data']
                    info: Dict[str, Any] = context.user_data.get("selected_quality_info", {})
                    info['_speed_str'] = pd.get('speed', '')
                    info['_eta_str'] = pd.get('eta', '')
                    info['_downloaded_bytes'] = pd.get('downloaded', 0)
                    info['_total_bytes'] = pd.get('total', 0)
                    info['title'] = info.get('title') or context.user_data.get('temp_info', {}).get('title', '')
                    pct = pd.get('pct', 0)

                    # v8: Show record/upload action based on download status
                    dl_status = pd.get('status', '')
                    if dl_status == 'downloading':
                        await context.bot.send_chat_action(message.chat_id, ChatAction.RECORD_VIDEO)

                    if pct > 0:
                        await update_progress(3, info, pct)
                    else:
                        await update_progress(3, info)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    await update_progress(3)
    await context.bot.send_chat_action(message.chat_id, ChatAction.RECORD_VIDEO)

    # Run download in thread pool
    loop = asyncio.get_running_loop()
    info_dict = context.user_data.get('temp_info', {})
    shared_progress_data: Dict[str, Any] = {
        'pct': 0.0,
        'eta': '',
        'speed': '',
        'downloaded': 0,
        'total': 0,
        'filename': '',
        'status': '',
        'cancel': cancel_event,
    }
    with active_downloads_lock:
        ad = active_downloads.get(user.id)
        if ad is not None:
            ad['progress_data'] = shared_progress_data

    progress_update_task = asyncio.create_task(real_time_progress_updater())

    file_path = None
    dl_info = None

    try:
        file_path, dl_info, _ = await loop.run_in_executor(
            pool, downloader.process_url, url, user.id, format_id, cancel_event, info_dict, shared_progress_data, 0
        )

    finally:
        if progress_update_task:
            progress_update_task.cancel()
            try:
                await progress_update_task
            except asyncio.CancelledError:
                pass

    # Handle cancellation
    if dl_info and dl_info.get('cancelled'):
        if context.user_data.pop('_slot_consumed', False):
            rate_limiter.refund_download(user.id)
        try:
            await status.edit_text(
                f"{UI.E['cross']} <b>Download Cancelled!</b>\n\n"
                f"{UI.E['info']} The download was stopped by user.",
                parse_mode=ParseMode.HTML,
                reply_markup=UI.back_btn()
            )
        except Exception:
            pass
        return

    if not file_path or (dl_info and 'error' in dl_info):
        err = dl_info.get('error', 'Unknown Error') if dl_info else 'Unknown Error'
        platform = url_utils.detect_platform(url)
        error_tips = UI.get_platform_error_tips(err, platform)
        try:
            await status.edit_text(
                f"{UI.E['cross']} <b>DOWNLOAD FAILED</b>\n\n"
                f"<code>{UI.escape_html(err[:300])}</code>\n\n"
                f"{UI.E['info']} <b>Tips:</b>\n{error_tips}",
                parse_mode=ParseMode.HTML,
                reply_markup=UI.back_btn()
            )
        except Exception:
            pass
        if context.user_data.pop('_slot_consumed', False):
            rate_limiter.refund_download(user.id)
        db.log_download(user.id, platform, url, format_id, 0, 0, status='failed')
        return

    await update_progress(4, dl_info)

    # Check file size
    size_bytes = dl_info.get('file_size', os.path.getsize(file_path)) if file_path else 0
    size_mb = size_bytes / (1024 * 1024)

    max_size = MAX_PREMIUM_FILE_SIZE_MB if db.is_premium(user.id) else MAX_BOT_FILE_SIZE_MB

    if size_mb > max_size:
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass
        if context.user_data.pop('_slot_consumed', False):
            rate_limiter.refund_download(user.id)
        return await status.edit_text(
            f"{UI.E['warning']} <b>FILE TOO LARGE ({size_mb:.1f} MB)</b>\n\n"
            f"Telegram Bot API limit is <b>50MB</b>.\n"
            f"Please select a lower quality or use audio-only.\n\n"
            f"{UI.E['info']} <i>Tips: Try 720p / 480p / MP3 for smaller files.</i>\n"
            f"{UI.E['link']} <i>Login URL: {downloader.get_login_url() or 'N/A'}</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=UI.back_btn()
        )

    await update_progress(6, dl_info)
    await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_AUDIO if format_id in ("audio", "mp3", "m4a", "opus", "wav") else ChatAction.UPLOAD_VIDEO)

    # v8: Thumbnail is already pre-downloaded during info fetch
    # Try to get thumbnail from temp dir or download fresh
    thumbnail_path: Optional[str] = None
    try:
        # Look for pre-downloaded thumbnail
        if os.path.exists(TEMP_DIR):
            for fname in os.listdir(TEMP_DIR):
                if fname.startswith(f"thumb_{user.id}_") and not fname.endswith(('.temp', '.part')):
                    thumbnail_path = os.path.join(TEMP_DIR, fname)
                    break
    except Exception:
        pass

    # If no pre-downloaded thumbnail, fetch it now
    if not thumbnail_path:
        try:
            thumbnail_path = await loop.run_in_executor(
                pool, downloader.download_thumbnail, url, user.id
            )
        except Exception:
            pass

    # Build caption with download speed
    is_audio = format_id in ("audio", "mp3", "m4a", "opus", "wav")
    quality_display = (
        "Audio (MP3 320kbps)" if format_id in ("audio", "mp3")
        else "Audio (M4A AAC)" if format_id == "m4a"
        else "Audio (OPUS)" if format_id == "opus"
        else "Audio (WAV Lossless)" if format_id == "wav"
        else (f"{dl_info['height']}p" if dl_info.get('height') else context.user_data.get("selected_quality_info", {}).get("resolution", format_id))
    )

    proc_time = dl_info.get('processing_time', 1)
    # v8: Show actual download speed
    speed_str = dl_info.get('avg_speed', UI.format_speed(size_bytes, proc_time))
    reward_coins = 3 if is_audio else 5

    caption = (
        f"{UI.E['success']} <b>Delivered</b>\n"
        f"{UI.E['video']} <b>{UI.escape_html(UI.truncate(dl_info.get('title', 'Video'), 54))}</b>\n"
        f"{UI.E['globe']} {dl_info.get('platform', 'Web')} • {UI.E['hd']} {quality_display}\n"
        f"{UI.E['size']} {UI.format_size(size_bytes)} • {UI.E['speed']} {speed_str}\n"
        f"{UI.E['clock']} {UI.format_duration(dl_info.get('duration'))} • {UI.E['coin']} +{reward_coins}\n"
        f"{UI.E['robot']} <i>{BOT_USERNAME} • premium delivery</i>"
    )

    # Send file
    send_success = False
    try:
        with open(file_path, 'rb') as f:
            thumb = None
            if thumbnail_path and os.path.exists(thumbnail_path):
                try:
                    thumb = open(thumbnail_path, 'rb')
                except Exception:
                    thumb = None
            try:
                if is_audio:
                    audio_sent = False
                    try:
                        await message.reply_audio(
                            audio=f,
                            caption=caption,
                            parse_mode=ParseMode.HTML,
                            thumbnail=thumb,
                            title=dl_info.get('title', 'Audio'),
                            performer=dl_info.get('uploader', ''),
                            duration=int(dl_info.get('duration', 0))
                        )
                        audio_sent = True
                    except Exception:
                        pass

                    if not audio_sent:
                        f.seek(0)
                        try:
                            await message.reply_audio(
                                audio=f,
                                caption=caption,
                                parse_mode=ParseMode.HTML,
                                title=dl_info.get('title', 'Audio'),
                                performer=dl_info.get('uploader', ''),
                                duration=int(dl_info.get('duration', 0))
                            )
                            audio_sent = True
                        except Exception:
                            pass

                    if not audio_sent:
                        f.seek(0)
                        await message.reply_document(
                            document=f,
                            caption=caption,
                            parse_mode=ParseMode.HTML,
                            filename=os.path.basename(file_path),
                        )
                else:
                    video_sent = False
                    try:
                        await message.reply_video(
                            video=f,
                            caption=caption,
                            parse_mode=ParseMode.HTML,
                            supports_streaming=True,
                            thumbnail=thumb,
                            duration=int(dl_info.get('duration', 0)),
                            width=int(dl_info.get('width') or 720),
                            height=int(dl_info.get('height') or 480)
                        )
                        video_sent = True
                    except Exception:
                        pass

                    if not video_sent:
                        f.seek(0)
                        await message.reply_video(
                            video=f,
                            caption=caption,
                            parse_mode=ParseMode.HTML,
                            supports_streaming=True,
                            duration=int(dl_info.get('duration', 0)),
                            width=int(dl_info.get('width') or 720),
                            height=int(dl_info.get('height') or 480)
                        )
                send_success = True
            finally:
                if thumb:
                    try:
                        thumb.close()
                    except Exception:
                        pass

        if send_success:
            context.user_data.pop('_slot_consumed', None)
            db.log_download(
                user.id,
                dl_info.get('platform', 'Web'),
                url,
                format_id,
                size_bytes,
                dl_info.get('processing_time', 0),
                dl_info.get('title', 'Unknown'),
                'success'
            )
            try:
                await status.delete()
            except Exception:
                pass
            if update.callback_query:
                try:
                    await update.callback_query.message.edit_reply_markup(reply_markup=UI.back_btn())
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"Send Error: {e}")
        err_text = str(e)
        too_large_error = ("too large" in err_text.lower() or "413" in err_text
                           or "request entity too large" in err_text.lower())

        # Advanced fallback: if streaming video send fails, try sending as a document.
        if not is_audio and not too_large_error and file_path and os.path.exists(file_path):
            try:
                with open(file_path, 'rb') as doc_f:
                    await message.reply_document(
                        document=doc_f,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                        filename=os.path.basename(file_path),
                    )
                db.log_download(
                    user.id,
                    dl_info.get('platform', 'Web'),
                    url,
                    format_id,
                    size_bytes,
                    dl_info.get('processing_time', 0),
                    dl_info.get('title', 'Unknown'),
                    'success'
                )
                try:
                    await status.delete()
                except Exception:
                    pass
                send_success = True
                return
            except Exception as doc_err:
                logger.error(f"Document fallback failed: {doc_err}")
                err_text = f"{err_text} | document fallback failed: {doc_err}"
                too_large_error = ("too large" in err_text.lower() or "413" in err_text
                                   or "request entity too large" in err_text.lower())

        if too_large_error:
            try:
                await status.edit_text(
                    f"{UI.E['warning']} <b>UPLOAD FAILED \u2014 FILE TOO LARGE</b>\n\n"
                    f"<i>File size: {size_mb:.1f} MB</i>\n"
                    f"Telegram Bot API limit: <b>50MB</b>\n\n"
                    f"{UI.E['info']} Please try a lower quality (720p or 480p).\n"
                    f"{UI.E['link']} Login URL: {downloader.get_login_url() or 'N/A'}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=UI.back_btn()
                )
            except Exception:
                pass
        else:
            try:
                await status.edit_text(
                    f"{UI.E['cross']} <b>SEND FAILED</b>\n\n"
                    f"<code>{UI.escape_html(err_text[:200])}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=UI.back_btn()
                )
            except Exception:
                pass
        if context.user_data.pop('_slot_consumed', False):
            rate_limiter.refund_download(user.id)
        platform = url_utils.detect_platform(url)
        db.log_download(user.id, platform, url, format_id, size_bytes, 0, status='failed')
    finally:
        # Smart cleanup
        for fpath in [file_path, thumbnail_path]:
            try:
                if fpath and isinstance(fpath, str) and os.path.exists(fpath):
                    os.remove(fpath)
            except Exception:
                pass
        # Clear context
        for key in ("selected_quality_info", "available_formats", "temp_url", "temp_info", "session_time", "_slot_consumed", "current_session_hash"):
            context.user_data.pop(key, None)

# ════════════════════════════════════════════════════════════════════════════
# 🎛️ BUTTON CALLBACK HANDLER v10
# ════════════════════════════════════════════════════════════════════════════

async def btn_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all inline keyboard button presses."""
    query = update.callback_query
    data  = query.data
    user  = query.from_user

    # Admin protection
    if data.startswith("adm_") and user.id != ADMIN_ID:
        try:
            return await query.answer("\U0001f512 Admin only!", show_alert=True)
        except Exception:
            return

    # v8 FIX: Single query.answer() — no double answer
    # Answer query for all non-cancel buttons
    if data != "cancel":
        try:
            await query.answer()
        except Exception:
            pass

    async def safe_edit(text: str, reply_markup: Optional[Any] = None) -> None:
        """Safely edit or replace a message without duplicate fallback spam."""
        try:
            # Media messages cannot always be edited as text safely
            if query.message.caption or query.message.photo or query.message.video or query.message.document:
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await query.message.reply_html(text, reply_markup=reply_markup)
                return

            current_text = getattr(query.message, "text_html", None) or getattr(query.message, "text", None) or ""
            same_text = current_text == text

            try:
                old_markup = query.message.reply_markup.to_dict() if query.message.reply_markup else None
                new_markup = reply_markup.to_dict() if reply_markup else None
                same_markup = old_markup == new_markup
            except Exception:
                same_markup = False

            if same_text and same_markup:
                return

            await query.message.edit_text(
                text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )

        except BadRequest as e:
            err = str(e).lower()
            if "message is not modified" in err:
                return

            logger.warning(f"safe_edit real error: {e}")
            try:
                await query.message.reply_html(text, reply_markup=reply_markup)
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"safe_edit unexpected error: {e}")
            try:
                await query.message.reply_html(text, reply_markup=reply_markup)
            except Exception:
                pass

    # ── CANCEL DOWNLOAD ───────────────────────────────────────────────────
    if data == "cancel":
        # v8 FIX: Single answer() only
        with active_downloads_lock:
            ad = active_downloads.get(user.id)
            if ad:
                ad['cancel'].set()
                try:
                    await query.answer("\u23f9\ufe0f Cancelling download...", show_alert=True)
                except Exception:
                    pass
            else:
                try:
                    await query.answer("No active download", show_alert=False)
                except Exception:
                    pass

        current_hash = context.user_data.get('current_session_hash')
        for key in ("temp_url", "state", "available_formats",
                    "selected_quality_info", "temp_info", "session_time", "current_session_hash"):
            context.user_data.pop(key, None)
        try:
            await safe_edit(f"{UI.E['check']} <b>Cancelled.</b>", UI.back_btn())
        except Exception:
            pass

    # ── PUBLIC BUTTONS ──────────────────────────────────────────────────

    elif data == "start_menu":
        db.add_user(user)
        await safe_edit(UI.welcome(user, db.get_stats()), UI.main_menu(user.id == ADMIN_ID))

    elif data == "menu_more":
        await safe_edit(
            f"<b>{UI.E['sparkles']} MORE TOOLS</b>\n\n"
            f"{UI.E['globe']} Platforms • audio guide • history • quick actions.",
            UI.more_menu(user.id == ADMIN_ID)
        )

    elif data == "quick_tips":
        await safe_edit(
            f"<b>{UI.E['rocket']} QUICK TIPS</b>\n\n"
            f"• Use a direct public post or video URL\n"
            f"• 720p / 480p works best for Telegram upload limits\n"
            f"• Audio menu supports MP3 / M4A / OPUS / WAV\n"
            f"• Queue Pass or Premium gets a faster lane",
            UI.more_menu(user.id == ADMIN_ID)
        )

    elif data == "help":
        await safe_edit(
            f"""<b>{UI.E['help']} QUICK GUIDE</b>

1. Send one public link
2. Pick exact quality or audio
3. Wait for delivery

{UI.E['info']} 720p / 480p is safest if Telegram says the file is too large.
{UI.E['clock']} Session timeout: 10 min.""",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{UI.E['globe']} Platforms", callback_data="platforms"), InlineKeyboardButton(f"{UI.E['music']} Audio", callback_data="audio_help")],
                [InlineKeyboardButton("🩺 Health", callback_data="show_health"), InlineKeyboardButton(f"{UI.E['rocket']} Fast Tips", callback_data="quick_tips")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
            ])
        )

    elif data == "platforms":
        await safe_edit(
            f"""<b>{UI.E['globe']} PLATFORMS</b>

{UI.E['video']} YouTube • TikTok • Instagram • Facebook • X
{UI.E['video']} Vimeo • Dailymotion • Twitch • Rumble • Bilibili
{UI.E['music']} Audio: MP3 / M4A / OPUS / WAV
{UI.E['link']} Plus many public yt-dlp supported sites

{UI.E['info']} <i>Public links work best. Private/login links may need cookies.</i>""",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{UI.E['music']} Audio Guide", callback_data="audio_help")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to More", callback_data="menu_more")],
            ])
        )

    elif data == "audio_help":
        await safe_edit(
            f"""<b>{UI.E['music']} AUDIO GUIDE</b>

🎵 <b>MP3</b> — best all-round
🎧 <b>M4A</b> — smaller AAC
🪩 <b>OPUS</b> — very compact
🎙 <b>WAV</b> — biggest / lossless

{UI.E['info']} Open info card → choose audio format.""",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{UI.E['wifi']} Queue", callback_data="queue_status"), InlineKeyboardButton(f"{UI.E['wallet']} Wallet", callback_data="wallet")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to More", callback_data="menu_more")],
            ])
        )

    elif data == "show_health":
        await safe_edit(
            format_runtime_health_html(user.id),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="show_health"), InlineKeyboardButton(f"{UI.E['wifi']} Queue", callback_data="queue_status")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to More", callback_data="menu_more")],
            ])
        )

    elif data == "queue_status":
        await safe_edit(
            format_queue_snapshot_html(user.id),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="queue_status"), InlineKeyboardButton(f"{UI.E['rocket']} Tips", callback_data="quick_tips")],
                [InlineKeyboardButton(f"{UI.E['wallet']} Wallet", callback_data="wallet"), InlineKeyboardButton(f"{UI.E['diamond']} Premium", callback_data="premium")],
                [InlineKeyboardButton(f"{UI.E['back']} Home", callback_data="start_menu")],
            ])
        )

    elif data == "wallet":
        wallet = db.get_wallet(user.id)
        next_daily = 'Ready'
        if wallet.get('last_daily_claim'):
            try:
                last_daily = datetime.fromisoformat(wallet['last_daily_claim'])
                next_time = datetime.combine((last_daily + timedelta(days=1)).date(), datetime.min.time())
                remain = max(0, int((next_time - datetime.utcnow()).total_seconds()))
                if remain:
                    next_daily = UI.format_duration(remain)
            except Exception:
                next_daily = 'Ready'
        spin_text = 'Ready'
        if wallet.get('last_spin'):
            try:
                last_spin = datetime.fromisoformat(wallet['last_spin'])
                remain = max(0, int((last_spin + timedelta(hours=4) - datetime.utcnow()).total_seconds()))
                if remain:
                    spin_text = UI.format_duration(remain)
            except Exception:
                spin_text = 'Ready'
        await safe_edit(
            f"""{UI.E['wallet']} <b>WALLET</b>

{UI.E['coin']} Coins: <b>{wallet['coins']}</b>   {UI.E['giftbox']} Streak: <b>{wallet['daily_streak']}</b>
{UI.E['video']} Video rewards: <b>{wallet['video_downloads']}</b>
{UI.E['music']} Audio rewards: <b>{wallet['audio_downloads']}</b>
{UI.E['giftbox']} Daily: <b>{next_daily}</b>
{UI.E['game']} Spin: <b>{spin_text}</b>

{UI.E['info']} Use coins for faster queue or temporary premium access.""",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{UI.E['giftbox']} Daily", callback_data="daily_bonus"), InlineKeyboardButton(f"{UI.E['game']} Spin", callback_data="spin_wheel")],
                [InlineKeyboardButton("🚀 Queue Pass", callback_data="redeem_queue_pass"), InlineKeyboardButton("👑 1 Day", callback_data="redeem_premium_day")],
                [InlineKeyboardButton("💎 3 Days", callback_data="redeem_premium_3day"), InlineKeyboardButton(f"{UI.E['diamond']} Premium", callback_data="premium")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
            ])
        )

    elif data == "redeem_queue_pass":
        result = db.redeem_shop_item(user.id, 'queue_pass')
        if result.get('ok'):
            await safe_edit(
                f"{UI.E['rocket']} <b>QUEUE PASS ADDED!</b>\n\n"
                f"{UI.E['coin']} Cost: <b>{result['cost']} coins</b>\n"
                f"{UI.E['wallet']} Balance: <b>{result['coins']} coins</b>\n"
                f"{UI.E['chart']} Queue passes: <b>{result.get('queue_passes', 0)}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )
        else:
            await safe_edit(
                f"{UI.E['warning']} <b>Not enough coins.</b>\n\nNeed <b>{result.get('cost', '?')}</b> coins.\nCurrent balance: <b>{result.get('coins', 0)}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )

    elif data == "redeem_premium_day":
        result = db.redeem_shop_item(user.id, 'premium_day')
        if result.get('ok'):
            await safe_edit(
                f"{UI.E['crown']} <b>1 DAY PREMIUM ACTIVATED!</b>\n\n"
                f"{UI.E['coin']} Cost: <b>{result['cost']} coins</b>\n"
                f"{UI.E['wallet']} Balance: <b>{result['coins']} coins</b>\n"
                f"{UI.E['diamond']} Active until: <b>{str(result.get('premium_until') or '')[:19]}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['diamond']} Premium Page", callback_data="premium")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )
        else:
            await safe_edit(
                f"{UI.E['warning']} <b>Not enough coins.</b>\n\nNeed <b>{result.get('cost', '?')}</b> coins.\nCurrent balance: <b>{result.get('coins', 0)}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )

    elif data == "redeem_premium_3day":
        result = db.redeem_shop_item(user.id, 'premium_3day')
        if result.get('ok'):
            await safe_edit(
                f"{UI.E['diamond']} <b>3 DAY PREMIUM ACTIVATED!</b>\n\n"
                f"{UI.E['coin']} Cost: <b>{result['cost']} coins</b>\n"
                f"{UI.E['wallet']} Balance: <b>{result['coins']} coins</b>\n"
                f"{UI.E['diamond']} Active until: <b>{str(result.get('premium_until') or '')[:19]}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['diamond']} Premium Page", callback_data="premium")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )
        else:
            await safe_edit(
                f"{UI.E['warning']} <b>Not enough coins.</b>\n\nNeed <b>{result.get('cost', '?')}</b> coins.\nCurrent balance: <b>{result.get('coins', 0)}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )

    elif data == "daily_bonus":
        result = db.claim_daily_bonus(user.id)
        if result.get('ok'):
            await safe_edit(
                f"""{UI.E['giftbox']} <b>DAILY BONUS CLAIMED!</b>

{UI.E['coin']} You received <b>{result['reward']} coins</b>.
{UI.E['fire']} Current streak: <b>{result['streak']} day(s)</b>
{UI.E['wallet']} New balance: <b>{result['coins']} coins</b>""",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )
        else:
            remain = UI.format_duration(result.get('remaining_seconds', 0))
            await safe_edit(
                f"""{UI.E['warning']} <b>Daily Bonus already claimed.</b>

{UI.E['clock']} Come back in <b>{remain}</b>.
{UI.E['wallet']} Balance: <b>{result.get('coins', 0)} coins</b>""",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )

    elif data == "spin_wheel":
        result = db.spin_reward(user.id)
        if result.get('ok'):
            await safe_edit(
                f"""{UI.E['game']} <b>SPIN COMPLETE!</b>

{UI.E['coin']} Wheel reward: <b>{result['reward']} coins</b>
{UI.E['wallet']} New balance: <b>{result['coins']} coins</b>

{UI.E['info']} <i>Spin again after the cooldown ends.</i>""",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )
        else:
            remain = UI.format_duration(result.get('remaining_seconds', 0))
            await safe_edit(
                f"""{UI.E['warning']} <b>Spin cooldown active.</b>

{UI.E['clock']} Try again in <b>{remain}</b>.
{UI.E['wallet']} Balance: <b>{result.get('coins', 0)} coins</b>""",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{UI.E['wallet']} Open Wallet", callback_data="wallet")],
                    [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
                ])
            )

    elif data == "profile":
        db.add_user(user)
        user_data = db.get_user_stats(user.id)
        if not user_data:
            user_data = {'downloads': 0, 'total_size': 0, 'is_premium': False, 'joined': 'Unknown', 'coins': 0, 'daily_streak': 0, 'audio_downloads': 0, 'video_downloads': 0}

        status_str = f"{UI.E['diamond']} Premium" if user_data['is_premium'] else "🆓 Free"
        join_date = str(user_data.get('joined', 'Unknown'))[:10] if user_data.get('joined') else "Unknown"
        total_size = UI.format_size(user_data.get('total_size', 0))
        remaining = rate_limiter.get_remaining_downloads(user.id, user_data['is_premium'])
        pos, total = get_user_waiting_position(user.id)
        queue_line = f"#{pos} / {total}" if pos else "Not waiting"

        await safe_edit(
            f"""{UI.E['star']} <b>PROFILE CARD</b>

{UI.E['robot']} {UI.escape_html(user.full_name or user.first_name)}
{UI.E['key']} ID: <code>{user.id}</code>
{UI.E['diamond']} Status: <b>{status_str}</b>
{UI.E['calendar']} Joined: <b>{join_date}</b>
{UI.E['download']} Downloads: <b>{user_data['downloads']}</b>
{UI.E['cloud']} Data: <b>{total_size}</b>
{UI.E['coin']} Coins: <b>{user_data.get('coins', 0)}</b>
{UI.E['giftbox']} Streak: <b>{user_data.get('daily_streak', 0)}</b>
{UI.E['rocket']} Queue Passes: <b>{user_data.get('queue_passes', 0)}</b>
{UI.E['chart']} Remaining/hr: <b>{remaining}</b>
{UI.E['wifi']} Queue spot: <b>{queue_line}</b>""",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{UI.E['chart']} Stats", callback_data="stats"), InlineKeyboardButton(f"{UI.E['wallet']} Wallet", callback_data="wallet")],
                [InlineKeyboardButton(f"{UI.E['wifi']} Queue", callback_data="queue_status"), InlineKeyboardButton(f"{UI.E['memo']} History", callback_data="history")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
            ])
        )

    elif data == "stats":
        db.add_user(user)
        user_data = db.get_user_stats(user.id)
        if not user_data:
            user_data = {'downloads': 0, 'total_size': 0, 'is_premium': False, 'audio_downloads': 0, 'video_downloads': 0}

        count = int(user_data['downloads'] or 0)
        size_bytes = int(user_data.get('total_size', 0) or 0)
        size = UI.format_size(size_bytes)
        avg_size = UI.format_size(int(size_bytes / count)) if count else '0 B'

        await safe_edit(
            f"""{UI.E['chart']} <b>STATS</b>

{UI.E['download']} Total: <b>{count}</b>
{UI.E['video']} Videos: <b>{user_data.get('video_downloads', 0)}</b>
{UI.E['music']} Audio: <b>{user_data.get('audio_downloads', 0)}</b>
{UI.E['cloud']} Saved data: <b>{size}</b>
{UI.E['size']} Avg file: <b>{avg_size}</b>
{UI.E['diamond']} Account: <b>{PREM_YES if user_data['is_premium'] else PREM_NO}</b>""",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{UI.E['memo']} History", callback_data="history"), InlineKeyboardButton(f"{UI.E['star']} Profile", callback_data="profile")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
            ])
        )

    elif data == "history":
        db.add_user(user)
        history = db.get_user_history(user.id, limit=8)
        if not history:
            await safe_edit(
                f"{UI.E['folder']} <b>HISTORY</b>\n\n{UI.E['info']} No downloads yet. Send a link to start.",
                UI.back_btn()
            )
            return

        text_hist = f"{UI.E['folder']} <b>RECENT DOWNLOADS</b>\n\n"
        for i, item in enumerate(history, 1):
            title = UI.escape_html(UI.truncate(item.get('video_title', '?'), 28))
            platform = item.get('platform', '?')
            quality = item.get('quality', '?')
            size = UI.format_size(item.get('file_size', 0))
            status_icon = "✅" if item.get('status') == 'success' else "❌"
            timestamp = str(item.get('timestamp', '?'))[:10]
            text_hist += f"<b>{i}.</b> {status_icon} {title}\n{platform} • {quality} • {size} • {timestamp}\n\n"

        await safe_edit(
            text_hist + f"{UI.E['info']} <i>Showing latest 8 items</i>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{UI.E['chart']} Stats", callback_data="stats"), InlineKeyboardButton(f"{UI.E['back']} Back", callback_data="start_menu")],
            ])
        )

    elif data == "premium":
        await safe_edit(
            _premium_panel_text(),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("👑 1 Day", callback_data="redeem_premium_day"), InlineKeyboardButton("💎 3 Days", callback_data="redeem_premium_3day")],
                [InlineKeyboardButton("🚀 Queue Pass", callback_data="redeem_queue_pass"), InlineKeyboardButton(f"{UI.E['wallet']} Wallet", callback_data="wallet")],
                [InlineKeyboardButton(f"{UI.E['back']} Back to Menu", callback_data="start_menu")],
            ])
        )

    # ── DOWNLOAD BUTTONS (from quality selection) ─────────────────────────
    elif data.startswith("dl_"):
        parts = data.split("_", 2)
        if len(parts) < 3:
            try:
                return await query.answer("\u274c Invalid button!", show_alert=True)
            except Exception:
                return
        format_id = parts[1]
        url_hash  = parts[2]
        session = _load_download_session(context, url_hash)
        url = (session or {}).get("url") or context.user_data.get("temp_url")
        if not url:
            try:
                return await query.answer(
                    "\u274c Session expired! Send the link again.", show_alert=True
                )
            except Exception:
                return

        # v8: Session timeout check
        if _check_session_timeout(context):
            try:
                return await query.answer(
                    "\u26a0\ufe0f Session expired (10 min). Send link again.",
                    show_alert=True
                )
            except Exception:
                return

        # Find selected format info for UI display
        available = ((session or {}).get("formats") if 'session' in locals() else None) or context.user_data.get("available_formats", {}).get(url_hash, [])
        selected_info = next(
            (f for f in available if f.get('format_id') == format_id), None
        )
        context.user_data['selected_quality_info'] = selected_info or {}

        # v8: Smart file size warning
        if selected_info:
            est_size = selected_info.get('filesize', 0)
            if est_size and est_size > FILE_SIZE_WARN_MB * 1024 * 1024:
                est_mb = est_size / (1024 * 1024)
                try:
                    await query.answer(
                        f"\u26a0\ufe0f Estimated size: {est_mb:.1f} MB\n"
                        f"File may be large for Telegram!",
                        show_alert=True
                    )
                except Exception:
                    pass

        safe_format_id = _build_exact_format_selector(selected_info, format_id)
        await execute_download(update, context, url, safe_format_id)

    # ── DOWNLOAD FROM INFO CARD ───────────────────────────────────────────
    elif data.startswith("dlinf_"):
        parts = data.split("_", 2)
        if len(parts) < 3:
            try:
                return await query.answer("\u274c Invalid button!", show_alert=True)
            except Exception:
                return
        action = parts[1]
        url_hash = parts[2]
        session = _load_download_session(context, url_hash)
        url = (session or {}).get("url") or context.user_data.get("temp_url")
        if not url:
            try:
                return await query.answer(
                    "\u274c Session expired! Send the link again.", show_alert=True
                )
            except Exception:
                return

        # v8: Session timeout check
        if _check_session_timeout(context):
            try:
                return await query.answer(
                    "\u26a0\ufe0f Session expired (10 min). Send link again.",
                    show_alert=True
                )
            except Exception:
                return

        if action == "best":
            await execute_download(update, context, url, "best")
        elif action == "audio":
            info = (session or {}).get('info') or context.user_data.get("temp_info", {})
            header, kb = UI.audio_kb(url_hash, info)
            await safe_edit(header, kb)
        elif action == "subs":
            # v8: Download subtitles
            await safe_edit(
                f"{UI.E['loading']} <b>Downloading subtitles...</b>",
                None
            )
            loop = asyncio.get_running_loop()
            try:
                sub_path = await loop.run_in_executor(
                    pool, downloader.download_subtitles, url, user.id
                )
                if sub_path and os.path.exists(sub_path):
                    with open(sub_path, 'rb') as sf:
                        await query.message.reply_document(
                            document=sf,
                            caption=f"{UI.E['subtitles']} <b>Subtitles downloaded!</b>\n"
                                    f"Language: English",
                            parse_mode=ParseMode.HTML
                        )
                    try:
                        os.remove(sub_path)
                    except Exception:
                        pass
                    await safe_edit(
                        f"{UI.E['check']} <b>Subtitles sent!</b>",
                        UI.back_btn()
                    )
                else:
                    await safe_edit(
                        f"{UI.E['warning']} <b>No subtitles found.</b>\n"
                        f"This video may not have English subtitles available.",
                        UI.back_btn()
                    )
            except Exception as e:
                await safe_edit(
                    f"{UI.E['cross']} <b>Subtitle download failed:</b>\n"
                    f"<code>{UI.escape_html(str(e)[:200])}</code>",
                    UI.back_btn()
                )
        elif action == "quality":
            available = (session or {}).get('formats') or context.user_data.get("available_formats", {}).get(url_hash, [])
            info = (session or {}).get('info') or context.user_data.get("temp_info", {})
            if available:
                header, kb = UI.quality_kb(available, url_hash, info)
                await safe_edit(header, kb)
            else:
                fetching = await query.message.reply_html(
                    f"{UI.E['loading']} Fetching qualities..."
                )
                loop = asyncio.get_running_loop()
                formats = await loop.run_in_executor(
                    pool, DownloadEngine.get_available_formats, url
                )
                if formats:
                    _remember_download_session(context, url, info=info, formats=formats)
                    header, kb = UI.quality_kb(formats, url_hash, info)
                    try:
                        await fetching.delete()
                    except Exception:
                        pass
                    await query.message.reply_html(header, reply_markup=kb)
                else:
                    try:
                        await fetching.delete()
                    except Exception:
                        pass
                    try:
                        await query.answer(
                            "Could not fetch qualities. Downloading best...",
                            show_alert=True
                        )
                    except Exception:
                        pass
                    await execute_download(update, context, url, "best")

    elif data.startswith("dlaudio_"):
        parts = data.split("_", 2)
        if len(parts) < 3:
            try:
                return await query.answer("❌ Invalid button!", show_alert=True)
            except Exception:
                return
        audio_format = parts[1]
        url_hash = parts[2]
        session = _load_download_session(context, url_hash)
        url = (session or {}).get("url") or context.user_data.get("temp_url")
        if not url:
            try:
                return await query.answer("❌ Session expired! Send the link again.", show_alert=True)
            except Exception:
                return
        if _check_session_timeout(context):
            try:
                return await query.answer("⚠️ Session expired (10 min). Send link again.", show_alert=True)
            except Exception:
                return
        selected_audio = 'mp3' if audio_format == 'best' else audio_format
        session_info = (session or {}).get('info') or context.user_data.get('temp_info', {})
        context.user_data['selected_quality_info'] = {
            'format_id': selected_audio,
            'resolution': f'Audio {selected_audio.upper()}',
            'ext': selected_audio,
            'title': session_info.get('title', ''),
        }
        await execute_download(update, context, url, selected_audio)

    # ── ADMIN BUTTONS ────────────────────────────────────────────────────

    elif data == "admin_home":
        await safe_edit(UI.admin_panel(db.get_stats()), UI.admin_kb())

    elif data == "adm_economy":
        rewards = db.get_reward_stats()
        top_wallets = db.get_top_wallets(limit=8)
        wallet_lines = "\n".join([
            f"{idx}. <code>{row.get('user_id')}</code> @{row.get('username') or 'N/A'} — <b>{row.get('coins', 0)}</b> coins"
            for idx, row in enumerate(top_wallets, 1)
        ]) if top_wallets else "No data yet."
        await safe_edit(
            f"{UI.E['coin']} <b>ECONOMY HUB</b>\n\n"
            f"{UI.E['wallet']} Coins in wallets: <b>{rewards['coins_in_wallets']}</b>\n"
            f"{UI.E['giftbox']} Coins distributed: <b>{rewards['coins_distributed']}</b>\n"
            f"{UI.E['rocket']} Queue passes left: <b>{rewards.get('queue_passes_left', 0)}</b>\n"
            f"{UI.E['wifi']} Queue passes used: <b>{rewards.get('queue_passes_used', 0)}</b>\n"
            f"{UI.E['video']} Video rewards: <b>{rewards['video_downloads']}</b>\n"
            f"{UI.E['music']} Audio rewards: <b>{rewards['audio_downloads']}</b>\n\n"
            f"<b>Top wallets</b>\n{wallet_lines}",
            InlineKeyboardMarkup([[InlineKeyboardButton(f"{UI.E['back']} Back to Admin", callback_data="admin_home")]])
        )

    elif data == "adm_queuehub":
        waiting = get_sorted_waiting_entries()
        lines = []
        for idx, entry in enumerate(waiting[:10], 1):
            lane = get_priority_lane_name(entry.get('user_id'))
            lines.append(f"{idx}. <code>{entry.get('user_id')}</code> — {lane} — {entry.get('format_id', 'best')}")
        if not lines:
            lines.append('No waiting jobs right now.')
        snap = get_queue_snapshot()
        await safe_edit(
            f"{UI.E['wifi']} <b>PRIORITY QUEUE HUB</b>\n\n"
            f"{UI.E['chart']} Active: <b>{snap['active_total']} / {snap['max_slots']}</b>\n"
            f"{UI.E['clock']} Waiting: <b>{snap.get('waiting_total', 0)}</b>\n"
            f"{UI.E['rocket']} Priority waiting: <b>{snap.get('priority_waiting', 0)}</b>\n"
            f"{UI.E['diamond']} Premium waiting: <b>{snap.get('premium_waiting', 0)}</b>\n\n"
            f"<b>Top waiting jobs</b>\n" + "\n".join(lines),
            InlineKeyboardMarkup([[InlineKeyboardButton(f"{UI.E['back']} Back to Admin", callback_data="admin_home")]])
        )

    elif data == "adm_maint":
        current    = db.get_setting("maintenance")
        new_status = "0" if current == "1" else "1"
        db.set_setting("maintenance", new_status)
        status_text = (
            "\U0001f534 ON \u2014 Bot offline for users"
            if new_status == "1"
            else "\U0001f7e2 OFF \u2014 Bot running normally"
        )
        try:
            await query.answer(f"Maintenance {status_text}", show_alert=True)
        except Exception:
            pass
        await safe_edit(UI.admin_panel(db.get_stats()), UI.admin_kb())

    elif data == "adm_backup":
        try:
            b_file = db.backup()
            with open(b_file, 'rb') as f:
                await query.message.reply_document(
                    document=f,
                    caption=(
                        f"{UI.E['save']} <b>Database Backup Complete!</b>\n"
                        f"{UI.E['calendar']} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    ),
                    parse_mode=ParseMode.HTML
                )
            try:
                await query.answer("\u2705 Backup sent!", show_alert=True)
            except Exception:
                pass
        except Exception as e:
            try:
                await query.answer(f"\u274c Backup Failed: {str(e)[:100]}", show_alert=True)
            except Exception:
                pass

    elif data == "adm_cookie":
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['key']} <b>UPLOAD COOKIES.TXT</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send the <code>cookies.txt</code> file.\n\n"
            f"<b>This enables:</b>\n"
            f"{UI.E['check']} Age-restricted videos\n"
            f"{UI.E['check']} Private/members-only videos\n"
            f"{UI.E['check']} Higher quality formats\n"
            f"{UI.E['check']} YouTube Studio content\n\n"
            f"{UI.E['info']} <i>Export cookies from browser using\n"
            f"'Get cookies.txt' extension (Chrome/Firefox).</i>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'upload_cookies'

    elif data == "adm_cast":
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['bell']} <b>BROADCAST MESSAGE</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send the message to broadcast to all users.\n"
            f"Supports <b>HTML formatting</b>.\n\n"
            f"{UI.E['info']} <i>Send any text message (non-command)</i>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'broadcast'

    elif data == "adm_setpremium":
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['diamond']} <b>SET PREMIUM USER</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send the <b>User ID</b> to toggle premium:\n"
            f"<code>123456789</code>\n\n"
            f"{UI.E['info']} <i>User must have used the bot at least once.</i>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'set_premium'

    elif data == "adm_ban":
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['lock']} <b>BAN / UNBAN USER</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send <b>User ID</b> to toggle ban:\n"
            f"<code>123456789</code>\n\n"
            f"{UI.E['info']} <i>Banned users see a reason and appeal info.</i>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'ban_user'

    elif data == "adm_massban":
        # v8.0: Mass ban interface
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['shield']} <b>MASS BAN USERS</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send User IDs separated by spaces or commas:\n"
            f"<code>123456789 987654321 555555555</code>\n\n"
            f"{UI.E['info']} <i>All listed users will be banned.</i>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'mass_ban'

    elif data == "adm_massunban":
        # v8.0: Mass unban interface
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['shield']} <b>MASS UNBAN USERS</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send User IDs separated by spaces or commas:\n"
            f"<code>123456789 987654321 555555555</code>\n\n"
            f"{UI.E['info']} <i>All listed users will be unbanned.</i>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'mass_unban'

    elif data == "adm_giftcoins":
        await safe_edit(
            f"""╔══════════════════════════════════════╗
║  {UI.E['coin']} <b>GIFT COINS</b>
╚══════════════════════════════════════╝

Send <b>User ID and amount</b> like this:
<code>123456789 250</code>""",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'gift_coins'

    elif data == "adm_lookup":
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['search']} <b>USER LOOKUP</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send <b>User ID</b> or <b>Username</b> to search:\n"
            f"<code>123456789</code> or <code>username</code>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'user_lookup'

    elif data == "adm_clearstats":
        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['trash']} <b>CLEAR USER STATS</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"Send <b>User ID</b> to reset download count and data:\n"
            f"<code>123456789</code>",
            UI.cancel_btn()
        )
        context.user_data['state'] = 'clear_stats'

    elif data == "adm_fullstats":
        stats = db.get_stats()
        top_dl = db.get_top_downloaders(5)
        top_text = "\n".join(
            [f"  {UI.E['trophy']} <code>{row['user_id']}</code> @{row.get('username') or 'N/A'} → {row.get('download_count', 0)} downloads"
             for row in top_dl]
        ) if top_dl else "No data yet."

        platform_stats = db.get_top_platforms(5)
        platform_text = "\n".join(
            [f"  {UI.E['globe']} {row.get('platform')}: <b>{row.get('cnt', 0)}</b>" for row in platform_stats]
        ) if platform_stats else "No data yet."

        await safe_edit(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['chart']} <b>FULL STATISTICS</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"<b>{UI.E['star']} GENERAL:</b>\n"
            f"  Total Users:      <b>{stats['users']}</b>\n"
            f"  Premium Users:    <b>{stats['premium']}</b>\n"
            f"  Banned Users:     <b>{stats['banned']}</b>\n"
            f"  Total Downloads:  <b>{stats['downloads']}</b>\n"
            f"  Today's Downloads: <b>{stats['today_downloads']}</b>\n"
            f"  Data Processed:   <b>{stats['data_mb']:.2f} MB</b>\n"
            f"  Bot Uptime:       <b>{UI.format_uptime()}</b>\n\n"
            f"<b>{UI.E['trophy']} TOP DOWNLOADERS:</b>\n"
            f"{top_text}\n\n"
            f"<b>{UI.E['globe']} TOP PLATFORMS:</b>\n"
            f"{platform_text}",
            UI.admin_kb()
        )

    elif data == "adm_recent":
        recent = db.get_recent_downloads(limit=15)
        if not recent:
            await safe_edit(
                f"{UI.E['folder']} <b>Recent Downloads</b>\n\nNo recent downloads.",
                UI.admin_kb()
            )
            return

        text = (
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['folder']} <b>RECENT DOWNLOADS</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
        )
        for i, item in enumerate(recent, 1):
            title = UI.escape_html(UI.truncate(item.get('video_title', '?'), 30))
            uid = item.get('user_id', '?')
            platform = item.get('platform', '?')
            quality = item.get('quality', '?')
            size = UI.format_size(item.get('file_size', 0))
            status_icon = "\u2705" if item.get('status') == 'success' else "\u274c"
            text += (
                f"<b>{i}.</b> {status_icon} <code>{uid}</code> | {title}\n"
                f"    {platform} | {quality} | {size}\n\n"
            )

        await safe_edit(text, UI.admin_kb())

    elif data == "adm_settings":
        settings = db.get_all_settings()
        text = (
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['gear']} <b>BOT SETTINGS</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
        )
        for s in settings:
            val = s.get('value', '')
            desc = s.get('description', '')
            text += f"<b>{s['key']}:</b> <code>{val}</code>\n  <i>{desc}</i>\n\n"

        await safe_edit(text, UI.admin_kb())

# ════════════════════════════════════════════════════════════════════════════
# 📂 FILE (COOKIE) HANDLER
# v8 FIX: Proper File.download() for python-telegram-bot v20+
# ════════════════════════════════════════════════════════════════════════════

async def doc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle document uploads (cookies.txt). v8 FIX: uses proper File.download()."""
    if update.effective_user.id != ADMIN_ID:
        return
    if context.user_data.get('state') != 'upload_cookies':
        return
    try:
        document = update.message.document
        if not document:
            return await update.message.reply_html(
                "\u274c No document found.", reply_markup=UI.back_btn()
            )

        file = await document.get_file()

        # v8 FIX: Use proper python-telegram-bot v20+ download method
        # In PTB v20+, File.download() returns a Path-like object or bytes
        try:
            download_result = await file.download_to_drive(COOKIES_FILE)
        except AttributeError:
            # Fallback for different PTB versions
            try:
                download_result = await file.download(custom_path=COOKIES_FILE)
            except TypeError:
                # Another fallback: download to default location then move
                download_result = await file.download()
                if isinstance(download_result, (str, os.PathLike)):
                    src = str(download_result)
                    if src != COOKIES_FILE and os.path.exists(src):
                        shutil.move(src, COOKIES_FILE)
                else:
                    # download() returned bytes
                    with open(COOKIES_FILE, 'wb') as out_f:
                        out_f.write(download_result)

        db.set_setting("cookies_active", "1")
        await update.message.reply_html(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['check']} <b>COOKIES UPDATED!</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"{UI.E['check']} Cookies loaded successfully!\n\n"
            f"{UI.E['unlock']} <b>Now unlocked:</b>\n"
            f"  \u2022 Age-restricted videos\n"
            f"  \u2022 Private/members-only content\n"
            f"  \u2022 Higher quality formats\n"
            f"  \u2022 YouTube Studio content\n\n"
            f"{UI.E['info']} <i>Note: Cookies may expire. Re-upload when needed.</i>",
            reply_markup=UI.back_btn()
        )
    except Exception as e:
        logger.error(f"Cookie upload error: {e}")
        await update.message.reply_html(
            f"\u274c <b>Upload Error:</b>\n<code>{UI.escape_html(str(e)[:200])}</code>",
            reply_markup=UI.back_btn()
        )
    finally:
        context.user_data.pop('state', None)


# ════════════════════════════════════════════════════════════════════════════
# 📢 BROADCAST HANDLER
# ════════════════════════════════════════════════════════════════════════════

async def execute_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE,
                            msg: str) -> None:
    """Broadcast a message to all users."""
    users = db.get_all_users()
    if not users:
        return await update.message.reply_html(
            f"{UI.E['warning']} <b>No users found!</b>",
            reply_markup=UI.back_btn()
        )

    sent    = 0
    failed  = 0
    blocked = 0
    total   = len(users)

    status_msg = await update.message.reply_html(
        f"{UI.E['loading']} <b>Broadcasting to {total} users...</b>\n\n"
        f"{UI.E['info']} <i>Sending messages with flood protection (0.05s delay)</i>"
    )

    for i, uid in enumerate(users):
        try:
            await context.bot.send_message(
                uid,
                f"\U0001f4e2 <b>ANNOUNCEMENT</b>\n\n{msg}",
                parse_mode=ParseMode.HTML
            )
            sent += 1
        except Exception as e:
            err = str(e).lower()
            if 'blocked' in err or 'bot was blocked' in err:
                blocked += 1
            failed += 1

        if (i + 1) % 10 == 0:
            try:
                pct = int((i + 1) / total * 100)
                await status_msg.edit_text(
                    f"{UI.E['loading']} <b>Broadcasting...</b>\n\n"
                    f"Progress: {pct}% ({i + 1}/{total})\n"
                    f"\u2705 Sent: <b>{sent}</b> | \u274c Failed: <b>{failed}</b> | "
                    f"\U0001f6ab Blocked: <b>{blocked}</b>",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)

    await status_msg.edit_text(
        f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
        f"\u2551  {UI.E['success']} <b>BROADCAST COMPLETE!</b>\n"
        f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
        f"\U0001f4ca <b>Results:</b>\n"
        f"  \u2705 Sent:     <b>{sent}</b>\n"
        f"  \u274c Failed:   <b>{failed}</b>\n"
        f"  \U0001f6ab Blocked:  <b>{blocked}</b>\n"
        f"  \U0001f4ca Total:    <b>{total}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=UI.admin_kb()
    )
    context.user_data.pop('state', None)


# ════════════════════════════════════════════════════════════════════════════
# 👨‍💼 ADMIN USER ACTION HANDLER
# ════════════════════════════════════════════════════════════════════════════

async def handle_admin_user_action(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   msg: str) -> None:
    """Handle admin states: user lookup, clear stats, set premium, ban, mass ban/unban and coin gifts."""
    state = context.user_data.get('state')

    # User lookup
    if state == 'user_lookup':
        users = db.search_user(msg.strip())
        if not users:
            return await update.message.reply_html(
                f"{UI.E['cross']} <b>No users found!</b>\n"
                f"Search by User ID or username.",
                reply_markup=UI.cancel_btn()
            )

        text = (
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['search']} <b>SEARCH RESULTS</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
        )
        for u in users[:5]:
            uid = u.get('user_id', '?')
            uname = u.get('username') or 'N/A'
            fname = UI.escape_html(u.get('full_name') or 'N/A')
            premium = "\U0001f451" if u.get('is_premium') else "\U0001f195"
            banned = "\U0001f6ab" if u.get('is_banned') else "\u2705"
            dl_count = u.get('download_count', 0)
            ban_reason = u.get('ban_reason', '')
            ban_text = f"\n   \u2328\ufe0f Ban reason: {ban_reason}" if ban_reason else ""
            text += (
                f"<b>\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501</b>\n"
                f"\U0001f464 <b>{fname}</b>\n"
                f"   ID: <code>{uid}</code>\n"
                f"   @{uname}\n"
                f"   {premium} Premium: {u.get('is_premium')}\n"
                f"   {banned} Banned: {u.get('is_banned')}{ban_text}\n"
                f"   \u2b07\ufe0f Downloads: {dl_count}\n\n"
            )

        await update.message.reply_html(text, reply_markup=UI.back_btn())
        context.user_data.pop('state', None)
        return

    # v8: Mass ban
    if state == 'mass_ban':
        try:
            user_ids = [
                int(x.strip())
                for x in re.split(r'[\s,]+', msg.strip())
                if x.strip().isdigit()
            ]
        except Exception:
            return await update.message.reply_html(
                "\u274c <b>Invalid input!</b> Send User IDs separated by spaces or commas.",
                reply_markup=UI.cancel_btn()
            )
        if not user_ids:
            return await update.message.reply_html(
                "\u274c <b>No valid User IDs found!</b>",
                reply_markup=UI.cancel_btn()
            )
        banned_count, skipped_count = db.mass_ban(user_ids, "Mass ban by admin")
        await update.message.reply_html(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['shield']} <b>MASS BAN COMPLETE</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"\u2705 <b>Banned:</b> {banned_count}\n"
            f"\u26a0\ufe0f <b>Skipped:</b> {skipped_count}\n"
            f"\U0001f4ca <b>Total:</b> {len(user_ids)}",
            reply_markup=UI.admin_kb()
        )
        context.user_data.pop('state', None)
        return

    # v8: Mass unban
    if state == 'mass_unban':
        try:
            user_ids = [
                int(x.strip())
                for x in re.split(r'[\s,]+', msg.strip())
                if x.strip().isdigit()
            ]
        except Exception:
            return await update.message.reply_html(
                "\u274c <b>Invalid input!</b> Send User IDs separated by spaces or commas.",
                reply_markup=UI.cancel_btn()
            )
        if not user_ids:
            return await update.message.reply_html(
                "\u274c <b>No valid User IDs found!</b>",
                reply_markup=UI.cancel_btn()
            )
        unbanned_count, skipped_count = db.mass_unban(user_ids)
        await update.message.reply_html(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['shield']} <b>MASS UNBAN COMPLETE</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"\u2705 <b>Unbanned:</b> {unbanned_count}\n"
            f"\u26a0\ufe0f <b>Skipped:</b> {skipped_count}\n"
            f"\U0001f4ca <b>Total:</b> {len(user_ids)}",
            reply_markup=UI.admin_kb()
        )
        context.user_data.pop('state', None)
        return

    # Clear user stats
    if state == 'clear_stats':
        try:
            target_id = int(msg.strip())
        except ValueError:
            return await update.message.reply_html(
                "\u274c <b>Invalid user ID!</b>", reply_markup=UI.cancel_btn()
            )

        db.clear_user_stats(target_id)
        return await update.message.reply_html(
            f"{UI.E['trash']} <b>Stats cleared</b> for user <code>{target_id}</code>.\n"
            f"Download count and total size reset to 0.",
            reply_markup=UI.back_btn()
        )

    # Set premium / Ban user
    try:
        target_id = int(msg.strip())
    except ValueError:
        return await update.message.reply_html(
            "\u274c <b>Invalid user ID!</b> Send a numeric ID.",
            reply_markup=UI.cancel_btn()
        )

    if state == 'set_premium':
        user_row = db.get_user_admin_toggle_info(target_id)
        if not user_row:
            return await update.message.reply_html(
                f"❌ User <code>{target_id}</code> not found in database."
            )
        new_val = 0 if int(user_row.get('is_premium') or 0) == 1 else 1
        db.set_premium(target_id, bool(new_val))
        name = user_row.get('full_name') or str(target_id)
        await update.message.reply_html(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['diamond']} <b>PREMIUM STATUS UPDATED</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"\U0001f464 <b>{UI.escape_html(name)}</b>\n"
            f"\U0001f194 ID: <code>{target_id}</code>\n"
            f"{PREM_ENABLE if new_val else PREM_DISABLE}",
            reply_markup=UI.admin_kb()
        )
    elif state == 'ban_user':
        user_row = db.get_user_admin_toggle_info(target_id)
        if not user_row:
            return await update.message.reply_html(
                f"❌ User <code>{target_id}</code> not found."
            )
        new_val = 0 if int(user_row.get('is_banned') or 0) == 1 else 1
        reason = "" if new_val == 0 else "Banned by admin"
        db.ban_user(target_id, bool(new_val), reason)
        name = user_row.get('full_name') or str(target_id)
        await update.message.reply_html(
            f"\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
            f"\u2551  {UI.E['lock']} <b>BAN STATUS UPDATED</b>\n"
            f"\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n\n"
            f"\U0001f464 <b>{UI.escape_html(name)}</b>\n"
            f"\U0001f194 ID: <code>{target_id}</code>\n"
            f"{BANNED_YES if new_val else UNBANNED_YES}",
            reply_markup=UI.admin_kb()
        )

    context.user_data.pop('state', None)


def log_startup_health() -> None:
    """Log a compact startup health summary for easier debugging."""
    try:
        diag = get_runtime_health()
        logger.info(
            "Startup health → token=%s ffmpeg=%s cookies=%s waiting=%s active=%s disk=%.1fMB",
            'ok' if diag.get('token_ok') else 'missing',
            'ok' if diag.get('ffmpeg_ok') else 'missing',
            'loaded' if diag.get('cookies_ok') else 'not_loaded',
            diag.get('waiting_total', 0),
            diag.get('active_total', 0),
            float(diag.get('disk_mb', 0.0) or 0.0),
        )
    except Exception as e:
        logger.warning(f"Startup health check skipped: {e}")


# ════════════════════════════════════════════════════════════════════════════
# 🚀 MAIN LAUNCHER
# ════════════════════════════════════════════════════════════════════════════

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catch otherwise-unhandled exceptions and show a friendly message."""
    logger.error("Unhandled exception: %s", context.error)
    try:
        logger.error(traceback.format_exc())
    except Exception:
        pass

    try:
        effective_message = getattr(update, 'effective_message', None)
        if effective_message:
            await effective_message.reply_html(
                f"{UI.E['warning']} <b>Something went wrong while processing that request.</b>\n\n"
                f"{UI.E['tools']} Try again once. If the same link fails, send another public URL or choose a different quality.",
                reply_markup=UI.back_btn(),
                disable_web_page_preview=True
            )
    except Exception:
        pass


async def post_init(application) -> None:
    """Post-initialization: start background tasks."""
    asyncio.create_task(storage_cleaner.start_periodic_cleanup(application))
    logger.info("\u2705 Background cleanup task started")


def main() -> None:
    """Main entry point for the bot."""
    print("\u2550" * 70)
    print("\U0001f3ac ULTIMATE VIDEO DOWNLOADER BOT v10.1")
    print("\u2550" * 70)
    print("\U0001f527 Initializing systems...")

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Export BOT_TOKEN before starting the bot.")

    # Auto-update yt-dlp
    if db.get_setting("auto_update_ytdlp") == "1":
        auto_update_ytdlp()

    print(f"   \U0001f4c1 Download dir  : {os.path.abspath(DOWNLOAD_DIR)}/")
    print(f"   \U0001f4c1 Temp dir      : {os.path.abspath(TEMP_DIR)}/")
    print(f"   \U0001f4be Database      : {os.path.abspath(DB_NAME)}")
    print(f"   \U0001f4dd Logs          : {os.path.abspath(LOGS_DIR)}/")
    print(f"   \U0001f527 FFMPEG        : {imageio_ffmpeg.get_ffmpeg_exe()}")
    print(f"   \U0001f6e1\ufe0f Admin ID      : {ADMIN_ID}")
    print(f"   \u26a1 Concurrent    : {MAX_CONCURRENT_DOWNLOADS} max")
    print(f"   \U0001f4ca Rate Limit    : {RATE_LIMIT_DOWNLOADS}/hr (free), {RATE_LIMIT_DOWNLOADS_PREMIUM}/hr (premium)")
    print(f"   \u23f1\ufe0f Session Timeout: {SESSION_TIMEOUT_SECONDS}s")
    print(f"   \U0001f504 Max Retries   : {MAX_DOWNLOAD_RETRIES}")
    print(f"   \u26a0\ufe0f Size Warning  : >{FILE_SIZE_WARN_MB}MB")
    print("\u2550" * 70)

    # v8: Aggressive startup cleanup
    removed = storage_cleaner.aggressive_startup_cleanup()
    disk = storage_cleaner.get_disk_usage_mb()
    print(f"\U0001f9f9 Startup cleanup done. Removed {removed} files. Disk usage: {disk:.1f} MB")

    # Build app
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    # Register handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("menu", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("health", health_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("queue", queue_cmd))
    app.add_handler(CommandHandler("wallet", wallet_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("premium", premium_cmd))
    app.add_handler(CommandHandler("bonus", bonus_cmd))
    app.add_handler(CommandHandler("spin", spin_cmd))
    app.add_handler(CommandHandler("info", info_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CallbackQueryHandler(btn_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, doc_handler))

    # v8: Better group support — also handle URLs in groups
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, text_handler
    ))

    print("\u2705 BOT IS ONLINE AND READY TO SERVE!")
    print(f"\u2705 Version: 10.1 | Bugs Fixed: 24 | New Features: 31")
    print("\u2550" * 70)

    try:
        app.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
    except KeyboardInterrupt:
        print("\n\u23f9\ufe0f Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Bot crashed: {e}")
        logger.critical(traceback.format_exc())


if __name__ == "__main__":
    main()
