import logging
import os
import json
import asyncio
import unicodedata
from datetime import datetime
from asyncio import to_thread
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import quote_plus, urlparse

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InlineQueryResultCachedVideo,
    InputTextMessageContent,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)
from yt_dlp import YoutubeDL
from shared_db import load_saved_videos as db_load_saved_videos
from shared_db import load_users as db_load_users
from shared_db import load_video_catalog as db_load_video_catalog
from shared_db import save_saved_videos as db_save_saved_videos
from shared_db import save_users as db_save_users
from shared_db import save_video_catalog as db_save_video_catalog

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
SHARE_BOT_URL = "https://t.me/Hidop_bot"
SHARE_TEXT = "Hidop botni sinab ko'ring"
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
LOADING_STICKER_FILE_ID = os.getenv("LOADING_STICKER_FILE_ID", "").strip()
NOTIFIED_REFERRALS = set()
NOTIFIED_NEW_USERS = set()
LAST_VIDEO_FILE_ID_BY_USER: dict[int, str] = {}
MAX_VIDEO_BYTES = 49 * 1024 * 1024
FAST_VIDEO_FORMAT = "b[ext=mp4][height<=480]/b[height<=480]/b[ext=mp4]/b"
USERS_FILE = Path("users.json")
USERS: dict[int, dict[str, str]] = {}
VIDEO_CATALOG_FILE = Path("videos.json")
VIDEO_CATALOG: dict[str, object] = {"next_id": 1, "items": []}
VIDEO_ADMIN_ID = 8239140931
UPLOADERS_FILE = Path("uploaders.json")
VIDEO_UPLOADERS: set[int] = set()
SAVED_VIDEOS_FILE = Path("saved_videos.json")
SAVED_VIDEOS: dict[int, list[dict[str, object]]] = {}
VIDEO_REACTIONS_FILE = Path("video_reactions.json")
VIDEO_REACTIONS: dict[int, dict[str, int]] = {}
USER_REACTIONS_FILE = Path("user_reactions.json")
USER_REACTIONS: dict[int, dict[int, str]] = {}
MONTHLY_REACTIONS_FILE = Path("monthly_reactions.json")
MONTHLY_REACTIONS: dict[str, dict[str, dict[str, int]]] = {}
# Search optimization constants
SEARCH_CACHE: dict[str, list[dict]] = {}
SEARCH_CACHE_MAX_SIZE = 200  # Increased cache size
TITLE_INDEX: dict[str, list[dict]] = {}  # Pre-built index for first letters
THANKS_TEXT = "Bizdan foydalanganingiz uchun rahmat"
SHARE_BROADCAST_TEXT = "BIZNI QO'LAB QUVATLASH UCHUN SHARE TUGMASINI BOSING"
SMS_SELECT_MODE_KEY = "sms_select_mode"
SMS_SELECT_OPTIONS_KEY = "sms_select_options"
SMS_SELECT_ACTION_KEY = "sms_select_action"
SELECTED_CHAT_USER_ID_KEY = "selected_chat_user_id"
ACTIVE_CHAT_USER_IDS_KEY = "active_chat_user_ids"
GENERAL_BROADCAST_STATE_KEY = "general_broadcast_state"
GENERAL_BROADCAST_TEXT_KEY = "general_broadcast_text"
GENERAL_BROADCAST_CONTENT_KEY = "general_broadcast_content"
SAVE_VIDEO_STATE_KEY = "save_video_state"
DELETE_SAVED_VIDEOS_STATE_KEY = "delete_saved_videos_state"
DELETE_CATALOG_VIDEOS_STATE_KEY = "delete_catalog_videos_state"
UPLOADER_SELECT_MODE_KEY = "uploader_select_mode"
UPLOADER_SELECT_ACTION_KEY = "uploader_select_action"
UPLOADER_SELECT_OPTIONS_KEY = "uploader_select_options"
UPLOAD_VIDEO_STATE_KEY = "upload_video_state"

LANGUAGE_MESSAGES = {
    "lang_uz_lat": (
        "👋 Assalomu alaykum! Botimizga xush kelibsiz\n"
        "botimzda siz qidirgan kinoni topasiz 😉\n"
        "🔗 qo'llab quvatlash uchun pastagi tugmani bosing"
    ),
}

DEFAULT_LANG = "lang_uz_lat"
LANG_KEY = "lang"

I18N = {
    "lang_uz_lat": {
        "share_button": "Botni ulashish",
        "message_not_found": "Xabar topilmadi.",
        "server_http_client_missing": "Server xatoligi: HTTP client topilmadi.",
        "unknown_track": "Noma'lum",
        "unknown_artist": "Noma'lum",
        "inline_no_video_title": "Video topilmadi",
        "inline_no_video_text": "Avval botga video link yuboring, keyin jo'natish tugmasini bosing.",
        "inline_no_video_description": "Avval video yuklab oling",
        "inline_send_video_title": "Videoni jo'natish",
        "inline_send_video_description": "Chat tanlab videoni yuboring",
        "video_file_not_found": "Video fayl topilmadi.",
        "video_too_large": "Video hajmi katta. Telegram limitidan oshib ketdi.",
        "action_send": "📤 Jo'natish",
        "video_caption": "📥 @Hidop_bot | Hech qanday obuna va reklamalarsiz ✅",
        "youtube_result": "▶️ YouTube: {url}",
        "no_results": "iltimos kino codini yoki nomini yozing 🔗",
        "video_saved": "saqlandi 📥",
        "video_list_header": "Video ro'yxati:",
        "video_list_empty": "Hozircha video yo'q.",
        "video_number_not_found": "Bunday mediaya topilmadi 😞",
        "video_save_prompt": "Videoga nom qo'ying.",
        "video_saved_list_header": "Saqlangan videolar:",
        "saved_videos_clear": "Tozalash",
        "saved_videos_delete_prompt": "O'chirmoqchi bo'lgan videoni raqamini yozing ✅",
        "saved_videos_delete_invalid": "Raqamlarni to'g'ri yuboring. Masalan: 1,3,5",
        "saved_videos_delete_not_found": "Bunday raqam yo'q. Qayta yuboring.",
        "saved_videos_deleted": "Tanlangan videolar o'chirildi.",
        "catalog_delete_prompt": "O'chirmoqchi bo'lgan videoni raqamini yozing ✅",
        "catalog_deleted": "Tanlangan videolar katalogdan o'chirildi.",
        "uploader_list_header_add": "Video yuklovchini tanlang",
        "uploader_list_header_remove": "Olib tashlanadigan yuklovchini tanlang",
        "uploader_list_empty": "Hozircha video yuklovchilar yo'q.",
        "uploader_added": "Video yuklovchi qo'shildi.",
        "uploader_removed": "Video yuklovchi olib tashlandi.",
        "upload_video_name_prompt": "Video uchun nom yuboring.",
        "upload_video_id_choice_prompt": "Video IDI tanlaysizmi?",
        "upload_video_id_prompt": "Video IDI bering.",
        "upload_video_id_invalid": "ID raqam bo'lishi kerak.",
        "upload_video_id_exists": "Bu ID allaqachon mavjud.",
        "upload_video_comment_confirm_prompt": "Comment qo'shasizmi?",
        "upload_video_comment_prompt": "Comment kiting 📨",
        "upload_video_comment_added": "Comment kiritildi 📨 ✅",
        "save_button": "Saqlash 📥",
    },
}


def get_user_lang(context: ContextTypes.DEFAULT_TYPE) -> str:
    lang = context.user_data.get(LANG_KEY, DEFAULT_LANG)
    if lang not in I18N:
        return DEFAULT_LANG
    return lang


def t(context: ContextTypes.DEFAULT_TYPE, key: str, **kwargs: str) -> str:
    lang = get_user_lang(context)
    value = I18N.get(lang, I18N[DEFAULT_LANG]).get(key, key)
    return value.format(**kwargs)


def build_share_button_url(referrer_id: int) -> str:
    referral_link = f"{SHARE_BOT_URL}?start=ref_{referrer_id}"
    return f"tg://msg_url?url={quote_plus(referral_link)}&text={quote_plus(SHARE_TEXT)}"


def parse_referrer_id(args: list[str]) -> int | None:
    if not args:
        return None
    first_arg = args[0]
    if not first_arg.startswith("ref_"):
        return None
    ref_value = first_arg.replace("ref_", "", 1)
    if not ref_value.isdigit():
        return None
    return int(ref_value)


def parse_video_share(args: list[str]) -> int | None:
    if not args:
        return None
    first_arg = args[0]
    if not first_arg.startswith("video_"):
        return None
    video_value = first_arg.replace("video_", "", 1)
    if not video_value.isdigit():
        return None
    return int(video_value)


def parse_send_target_video(args: list[str]) -> tuple[int, int] | None:
    if not args:
        return None
    first_arg = args[0]
    if not first_arg.startswith("send_"):
        return None
    raw_value = first_arg.replace("send_", "", 1)
    parts = raw_value.split("_", 1)
    if len(parts) != 2:
        return None
    target_user_id, video_id = parts
    if not target_user_id.isdigit() or not video_id.isdigit():
        return None
    return int(target_user_id), int(video_id)


def load_users() -> None:
    try:
        USERS.clear()
        USERS.update(db_load_users())
    except Exception as exc:
        logger.warning("users bazadan o'qilmadi: %s", exc)


def save_users() -> None:
    try:
        db_save_users(USERS)
    except Exception as exc:
        logger.warning("users baza ga yozilmadi: %s", exc)


def load_video_catalog() -> None:
    try:
        raw = db_load_video_catalog()
        VIDEO_CATALOG["next_id"] = int(raw.get("next_id", 1))
        VIDEO_CATALOG["items"] = raw.get("items", [])
    except Exception as exc:
        logger.warning("videos baza dan o'qilmadi: %s", exc)


def save_video_catalog() -> None:
    try:
        db_save_video_catalog(VIDEO_CATALOG)
    except Exception as exc:
        logger.warning("videos baza ga yozilmadi: %s", exc)


def video_catalog_id_exists(number: int) -> bool:
    items = VIDEO_CATALOG.get("items", [])
    if not isinstance(items, list):
        return False
    for item in items:
        if isinstance(item, dict) and item.get("id") == number:
            return True
    return False


def next_available_catalog_id(start_id: int) -> int:
    candidate = max(1, int(start_id))
    while video_catalog_id_exists(candidate):
        candidate += 1
    return candidate


def format_duration(seconds: int) -> str:
    """Format duration in seconds to MM:SS format"""
    if seconds <= 0:
        return "00:00"
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    return f"{minutes:02d}:{remaining_seconds:02d}"


def build_search_index() -> None:
    """Build a fast search index for first letter lookups"""
    global TITLE_INDEX
    TITLE_INDEX.clear()
    
    items = VIDEO_CATALOG.get("items", [])
    if not isinstance(items, list):
        return
    
    for item in items:
        if not isinstance(item, dict):
            continue
        
        title = str(item.get("title", ""))
        if not title:
            continue
        
        normalized_title = normalize_text(title)
        if not normalized_title:
            continue
        
        # Index by first letter
        first_letter = normalized_title[0]
        if first_letter not in TITLE_INDEX:
            TITLE_INDEX[first_letter] = []
        TITLE_INDEX[first_letter].append(item)


def clear_search_cache() -> None:
    """Clear the search cache when catalog is updated"""
    global SEARCH_CACHE, TITLE_INDEX
    SEARCH_CACHE.clear()
    TITLE_INDEX.clear()
    # Rebuild index after clearing
    build_search_index()


def add_video_to_catalog(
    file_id: str, title: str, added_by: int, custom_id: int | None = None, comment: str = "", duration: int = 0
) -> int:
    items = VIDEO_CATALOG.get("items", [])
    if not isinstance(items, list):
        items = []
    next_id = int(VIDEO_CATALOG.get("next_id", 1))
    if next_id < 1:
        next_id = 1

    if custom_id is not None:
        new_id = custom_id
        if video_catalog_id_exists(new_id):
            raise ValueError("catalog id exists")
    else:
        new_id = next_available_catalog_id(next_id)

    entry = {
        "id": new_id,
        "file_id": file_id,
        "title": title,
        "added_by": added_by,
        "added_at": datetime.now().isoformat(timespec="seconds"),
        "comment": comment,
        "duration": duration,
    }
    items.append(entry)
    VIDEO_CATALOG["items"] = items
    VIDEO_CATALOG["next_id"] = max(next_id, new_id + 1)
    save_video_catalog()
    
    # Clear search cache since catalog was updated
    clear_search_cache()
    
    return new_id


def get_video_by_number(number: int) -> dict | None:
    items = VIDEO_CATALOG.get("items", [])
    if not isinstance(items, list):
        return None
    for item in items:
        if isinstance(item, dict) and item.get("id") == number:
            return item
    return None


def normalize_text(text: str) -> str:
    """Normalize text for better search matching"""
    # Convert to NFKD form and remove combining characters
    normalized = unicodedata.normalize('NFKD', text.lower().strip())
    # Remove diacritics and other combining characters
    result = ''.join(c for c in normalized if not unicodedata.combining(c))
    # Replace common punctuation and special characters with spaces for better matching
    result = result.replace("'", " ").replace('"', " ").replace("-", " ").replace("_", " ")
    # Remove extra spaces
    result = " ".join(result.split())
    return result


def get_videos_by_name(search_query: str) -> list[dict]:
    # Check cache first - fastest path
    cache_key = search_query.lower().strip()
    if cache_key in SEARCH_CACHE:
        return SEARCH_CACHE[cache_key]
    
    if not search_query.strip():
        return []
    
    # Normalize search query for better matching
    normalized_search = normalize_text(search_query)
    search_words = normalized_search.split()
    
    # Special handling for single letter searches - use index for maximum speed
    is_single_letter = len(normalized_search) == 1 and len(search_words) == 1
    
    results = []
    
    if is_single_letter:
        # Use pre-built index for single letter searches - much faster!
        first_letter = normalized_search[0]
        indexed_items = TITLE_INDEX.get(first_letter, [])
        
        for item in indexed_items:
            if not isinstance(item, dict):
                continue
                
            title = str(item.get("title", ""))
            if not title:
                continue
            
            normalized_title = normalize_text(title)
            
            # Check for exact match first (highest priority)
            if normalized_title == normalized_search:
                results.insert(0, item)
                continue
            
            # Already filtered by first letter, just add it
            results.append(item)
            if len(results) >= 100:
                break
    else:
        # For multi-word searches, use optimized scanning
        items = VIDEO_CATALOG.get("items", [])
        if not isinstance(items, list):
            return []
        
        # Pre-filter items that have titles to avoid repeated checks
        valid_items = []
        for item in items:
            if isinstance(item, dict) and item.get("title"):
                valid_items.append(item)
        
        for item in valid_items:
            title = str(item.get("title", ""))
            normalized_title = normalize_text(title)
            
            # Check for exact match first (highest priority)
            if normalized_title == normalized_search:
                results.insert(0, item)
                continue
            
            # Check if all search words are contained in title
            if all(word in normalized_title for word in search_words):
                results.append(item)
                if len(results) >= 100:
                    break
    
    # Cache the results (with increased size limit)
    if len(SEARCH_CACHE) >= SEARCH_CACHE_MAX_SIZE:
        # Remove multiple oldest entries for better cache management
        for _ in range(min(20, len(SEARCH_CACHE))):
            oldest_key = next(iter(SEARCH_CACHE))
            del SEARCH_CACHE[oldest_key]
    
    SEARCH_CACHE[cache_key] = results
    return results


def load_monthly_reactions() -> None:
    if not MONTHLY_REACTIONS_FILE.exists():
        return
    try:
        raw = json.loads(MONTHLY_REACTIONS_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
        MONTHLY_REACTIONS.clear()
        for year_month, user_data in raw.items():
            if not isinstance(user_data, dict):
                continue
            MONTHLY_REACTIONS[year_month] = {}
            for user_id, reactions in user_data.items():
                if not str(user_id).lstrip("-").isdigit():
                    continue
                if isinstance(reactions, dict):
                    MONTHLY_REACTIONS[year_month][user_id] = {
                        "likes": int(reactions.get("likes", 0)),
                        "dislikes": int(reactions.get("dislikes", 0))
                    }
    except Exception as exc:
        logger.warning("monthly_reactions.json o'qilmadi: %s", exc)


def save_monthly_reactions() -> None:
    try:
        MONTHLY_REACTIONS_FILE.write_text(
            json.dumps(MONTHLY_REACTIONS, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("monthly_reactions.json yozilmadi: %s", exc)


def update_monthly_reaction(user_id: int, reaction_type: str) -> None:
    from datetime import datetime
    now = datetime.now()
    year_month = f"{now.year}-{now.month:02d}"
    
    if year_month not in MONTHLY_REACTIONS:
        MONTHLY_REACTIONS[year_month] = {}
    
    if user_id not in MONTHLY_REACTIONS[year_month]:
        MONTHLY_REACTIONS[year_month][user_id] = {"likes": 0, "dislikes": 0}
    
    MONTHLY_REACTIONS[year_month][user_id][reaction_type] += 1
    save_monthly_reactions()


def get_monthly_reaction_count(user_id: int, reaction_type: str, year_month: str = None) -> int:
    if year_month is None:
        from datetime import datetime
        now = datetime.now()
        year_month = f"{now.year}-{now.month:02d}"
    
    if year_month not in MONTHLY_REACTIONS:
        return 0
    if user_id not in MONTHLY_REACTIONS[year_month]:
        return 0
    return MONTHLY_REACTIONS[year_month][user_id].get(reaction_type, 0)


def load_video_reactions() -> None:
    if not VIDEO_REACTIONS_FILE.exists():
        return
    try:
        raw = json.loads(VIDEO_REACTIONS_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
        VIDEO_REACTIONS.clear()
        for key, value in raw.items():
            if not str(key).lstrip("-").isdigit():
                continue
            video_id = int(key)
            if isinstance(value, dict):
                VIDEO_REACTIONS[video_id] = {
                    "likes": int(value.get("likes", 0)),
                    "dislikes": int(value.get("dislikes", 0))
                }
    except Exception as exc:
        logger.warning("video_reactions.json o'qilmadi: %s", exc)


def save_video_reactions() -> None:
    try:
        VIDEO_REACTIONS_FILE.write_text(
            json.dumps(VIDEO_REACTIONS, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("video_reactions.json yozilmadi: %s", exc)


def load_user_reactions() -> None:
    if not USER_REACTIONS_FILE.exists():
        return
    try:
        raw = json.loads(USER_REACTIONS_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
        USER_REACTIONS.clear()
        for user_id_key, videos in raw.items():
            if not str(user_id_key).lstrip("-").isdigit():
                continue
            user_id = int(user_id_key)
            if isinstance(videos, dict):
                USER_REACTIONS[user_id] = {
                    int(video_id): reaction_type
                    for video_id, reaction_type in videos.items()
                    if str(video_id).lstrip("-").isdigit() and isinstance(reaction_type, str)
                }
    except Exception as exc:
        logger.warning("user_reactions.json o'qilmadi: %s", exc)


def save_user_reactions() -> None:
    try:
        serialized = {
            str(user_id): {str(video_id): reaction for video_id, reaction in videos.items()}
            for user_id, videos in USER_REACTIONS.items()
        }
        USER_REACTIONS_FILE.write_text(
            json.dumps(serialized, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("user_reactions.json yozilmadi: %s", exc)


def get_video_reaction_users(video_id: int, reaction_type: str) -> list[str]:
    """Get list of users who gave specific reaction to a video"""
    try:
        data = json.loads(USER_REACTIONS_FILE.read_text(encoding="utf-8"))
        video_key = str(video_id)
        if video_key in data and reaction_type in data[video_key]:
            return data[video_key][reaction_type]
        return []
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return []


def add_video_reaction(video_id: int, user_id: int, reaction_type: str) -> None:
    """Add or update user reaction for a video"""
    try:
        # Load existing data
        try:
            data = json.loads(USER_REACTIONS_FILE.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}
        
        video_key = str(video_id)
        if video_key not in data:
            data[video_key] = {"likes": [], "dislikes": []}
        
        # Remove user from other reaction type if exists
        other_reaction = "dislikes" if reaction_type == "likes" else "likes"
        if str(user_id) in data[video_key][other_reaction]:
            data[video_key][other_reaction].remove(str(user_id))
        
        # Add user to new reaction type if not already there
        if str(user_id) not in data[video_key][reaction_type]:
            data[video_key][reaction_type].append(str(user_id))
        
        # Save updated data
        serialized = {str(k): v for k, v in data.items()}
        USER_REACTIONS_FILE.write_text(
            json.dumps(serialized, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("user_reactions.json yozilmadi: %s", exc)


def remove_video_reaction(video_id: int, user_id: int) -> None:
    """Remove user reaction for a video"""
    try:
        data = json.loads(USER_REACTIONS_FILE.read_text(encoding="utf-8"))
        video_key = str(video_id)
        user_str = str(user_id)
        
        if video_key in data:
            for reaction_type in ["likes", "dislikes"]:
                if user_str in data[video_key][reaction_type]:
                    data[video_key][reaction_type].remove(user_str)
            
            # Clean up empty video entries
            if not data[video_key]["likes"] and not data[video_key]["dislikes"]:
                del data[video_key]
            
            # Save updated data
            serialized = {str(k): v for k, v in data.items()}
            USER_REACTIONS_FILE.write_text(
                json.dumps(serialized, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as exc:
        logger.warning("Reaksiya o'chirishda xatolik: %s", exc)


def create_video_buttons_with_users(video_id: int, context: ContextTypes.DEFAULT_TYPE, user_id: int = None) -> InlineKeyboardMarkup:
    """Create video buttons with reaction counts and user list option"""
    likes = get_video_reaction_count(video_id, "likes")
    dislikes = get_video_reaction_count(video_id, "dislikes")
    
    # Base buttons for all users
    buttons = [
        [
            InlineKeyboardButton(f"👎🏻 {dislikes}", callback_data=f"dislike_video_{video_id}"),
            InlineKeyboardButton(f"👍🏻 {likes}", callback_data=f"like_video_{video_id}")
        ],
        [InlineKeyboardButton(
            t(context, "save_button"),
            callback_data=f"save_video_{video_id}"
        )]
    ]
    
    # Add delete button for admin
    if user_id and user_id == VIDEO_ADMIN_ID:
        buttons.append([InlineKeyboardButton("🗑️", callback_data=f"confirm_delete_video_{video_id}")])
    
    return InlineKeyboardMarkup(buttons)


async def on_show_reactions_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle showing reaction history for a video"""
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("show_reactions_"):
        return
    
    raw_video_id = query.data.replace("show_reactions_", "", 1)
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    video = get_video_by_number(video_id)
    if not video:
        await query.answer("Video topilmadi", show_alert=False)
        return
    
    likes = get_video_reaction_count(video_id, "likes")
    dislikes = get_video_reaction_count(video_id, "dislikes")
    
    # Get user lists for reactions
    like_users = get_video_reaction_users(video_id, "likes")
    dislike_users = get_video_reaction_users(video_id, "dislikes")
    
    # Create reaction info message
    title = video.get("title", "Video")
    reaction_text = f"📹 *{title}*\n\n"
    reaction_text += f"👍🏻 {likes} ta yoqtirdi\n"
    reaction_text += f"👎🏻 {dislikes} ta yoqtirdi\n\n"
    
    # Show users who liked (limit to first 5)
    if like_users:
        reaction_text += "👍🏻 *Yoqtirganlar:*\n"
        for i, user_id in enumerate(like_users[:5]):
            try:
                user = await context.bot.get_chat(user_id)
                name = user.full_name or f"User {user_id}"
                reaction_text += f"• {name}\n"
            except Exception:
                reaction_text += f"• User {user_id}\n"
        if len(like_users) > 5:
            reaction_text += f"... va {len(like_users) - 5} ta kishi\n"
        reaction_text += "\n"
    
    # Show users who disliked (limit to first 5)
    if dislike_users:
        reaction_text += "👎🏻 *Yoqtirmaganlar:*\n"
        for i, user_id in enumerate(dislike_users[:5]):
            try:
                user = await context.bot.get_chat(user_id)
                name = user.full_name or f"User {user_id}"
                reaction_text += f"• {name}\n"
            except Exception:
                reaction_text += f"• User {user_id}\n"
        if len(dislike_users) > 5:
            reaction_text += f"... va {len(dislike_users) - 5} ta kishi\n"
    
    if likes == 0 and dislikes == 0:
        reaction_text += "Hali hech kim reaksiya qilmadi."
    
    await query.answer()
    
    # Send reaction info as separate message
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=reaction_text,
        parse_mode="Markdown"
    )


def create_video_buttons(video_id: int, context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    likes = get_video_reaction_count(video_id, "likes")
    dislikes = get_video_reaction_count(video_id, "dislikes")
    
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"👎🏻 {dislikes}", callback_data=f"dislike_video_{video_id}"),
            InlineKeyboardButton(f"👍🏻 {likes}", callback_data=f"like_video_{video_id}")
        ],
        [InlineKeyboardButton(
            t(context, "save_button"),
            callback_data=f"save_video_{video_id}"
        )]
    ])


def get_video_reaction_count(video_id: int, reaction_type: str) -> int:
    """Get count of users who gave specific reaction to a video"""
    try:
        data = json.loads(USER_REACTIONS_FILE.read_text(encoding="utf-8"))
        video_key = str(video_id)
        if video_key in data and reaction_type in data[video_key]:
            return len(data[video_key][reaction_type])
        return 0
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return 0


def get_user_reaction(user_id: int, video_id: int) -> str | None:
    """Get user's reaction for a specific video"""
    if user_id not in USER_REACTIONS:
        return None
    return USER_REACTIONS[user_id].get(video_id)




def load_saved_videos() -> None:
    try:
        SAVED_VIDEOS.clear()
        raw = db_load_saved_videos()
        for user_id, items in raw.items():
            cleaned: list[dict[str, object]] = []
            for item in items:
                if isinstance(item, dict) and isinstance(item.get("name"), str):
                    cleaned.append(item)
            SAVED_VIDEOS[int(user_id)] = cleaned
        changed = False
        for items in SAVED_VIDEOS.values():
            if ensure_saved_ids(items):
                changed = True
        if changed:
            save_saved_videos()
    except Exception as exc:
        logger.warning("saved_videos baza dan o'qilmadi: %s", exc)


def save_saved_videos() -> None:
    try:
        db_save_saved_videos(SAVED_VIDEOS)
    except Exception as exc:
        logger.warning("saved_videos baza ga yozilmadi: %s", exc)


def next_available_saved_id(used_ids: set[int]) -> int | None:
    for candidate in range(1, 1000):
        if candidate not in used_ids:
            return candidate
    return None


def ensure_saved_ids(items: list[dict[str, object]]) -> bool:
    used_ids: set[int] = set()
    changed = False
    for item in items:
        saved_id = item.get("saved_id")
        if isinstance(saved_id, int) and 0 < saved_id < 1000 and saved_id not in used_ids:
            used_ids.add(saved_id)
            continue
        item["saved_id"] = None
        changed = True
    for item in items:
        saved_id = item.get("saved_id")
        if isinstance(saved_id, int) and 0 < saved_id < 1000:
            continue
        new_id = next_available_saved_id(used_ids)
        if new_id is None:
            break
        item["saved_id"] = new_id
        used_ids.add(new_id)
        changed = True
    return changed


def add_saved_video(
    user_id: int, video_id: int, name: str
) -> tuple[list[dict[str, object]], int | None]:
    items = SAVED_VIDEOS.get(user_id, [])
    if not isinstance(items, list):
        items = []
    ensure_saved_ids(items)
    
    # Check if this video is already saved
    existing_ids = {
        item.get("video_id")
        for item in items
        if isinstance(item, dict) and isinstance(item.get("video_id"), int)
    }
    if video_id in existing_ids:
        return items, None  # Video already saved
    
    # Use original video_id as saved_id
    items.append(
        {
            "video_id": video_id,
            "name": name,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "saved_id": video_id,
        }
    )
    SAVED_VIDEOS[user_id] = items
    save_saved_videos()
    return items, video_id


def load_video_uploaders() -> None:
    VIDEO_UPLOADERS.clear()
    VIDEO_UPLOADERS.add(VIDEO_ADMIN_ID)
    if not UPLOADERS_FILE.exists():
        return
    try:
        raw = json.loads(UPLOADERS_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            for item in raw:
                if str(item).lstrip("-").isdigit():
                    VIDEO_UPLOADERS.add(int(item))
    except Exception as exc:
        logger.warning("uploaders.json o'qilmadi: %s", exc)


def save_video_uploaders() -> None:
    try:
        serialized = sorted(VIDEO_UPLOADERS)
        UPLOADERS_FILE.write_text(
            json.dumps(serialized, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("uploaders.json yozilmadi: %s", exc)


def is_video_uploader(user_id: int) -> bool:
    return user_id in VIDEO_UPLOADERS


def register_user(user) -> tuple[bool, int]:
    user_id = user.id
    username = user.username or ""
    full_name = " ".join(
        part for part in [user.first_name or "", user.last_name or ""] if part
    ).strip()
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    is_new_user = user_id not in USERS
    if is_new_user:
        USERS[user_id] = {
            "first_join_date": today_str,
            "first_joined_at": now.isoformat(timespec="seconds"),
            "username": username,
            "full_name": full_name,
        }
        save_users()
        return True, len(USERS)

    existing = USERS[user_id]
    changed = False
    if not existing.get("first_join_date"):
        existing["first_join_date"] = today_str
        existing["first_joined_at"] = now.isoformat(timespec="seconds")
        changed = True
    if existing.get("username", "") != username:
        existing["username"] = username
        changed = True
    if existing.get("full_name", "") != full_name:
        existing["full_name"] = full_name
        changed = True
    if changed:
        save_users()

    return False, len(USERS)


def get_user_position(user_id: int) -> int:
    ordered_users = sorted(
        USERS.items(),
        key=lambda item: (item[1].get("first_joined_at", ""), item[0]),
    )
    for index, (uid, _) in enumerate(ordered_users, start=1):
        if uid == user_id:
            return index
    return len(ordered_users)


def get_admin_id() -> int | None:
    if not ADMIN_ID_RAW:
        return None
    if not ADMIN_ID_RAW.isdigit():
        return None
    return int(ADMIN_ID_RAW)


def set_thanks_targets(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    report_key: str,
    user_ids: list[int],
) -> None:
    storage = context.application.bot_data.setdefault("thanks_targets", {})
    if not isinstance(storage, dict):
        storage = {}
        context.application.bot_data["thanks_targets"] = storage
    storage[f"{chat_id}:{report_key}"] = user_ids


def get_thanks_targets(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, report_key: str
) -> list[int]:
    storage = context.application.bot_data.get("thanks_targets", {})
    if not isinstance(storage, dict):
        return []
    targets = storage.get(f"{chat_id}:{report_key}", [])
    if not isinstance(targets, list):
        return []
    return [uid for uid in targets if isinstance(uid, int)]


def build_thanks_markup(report_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Rahmat", callback_data=f"thanks_{report_key}")],
            [InlineKeyboardButton("Share", callback_data="share_broadcast")],
            [InlineKeyboardButton("SMS", callback_data="sms_broadcast")],
            [InlineKeyboardButton("umumiy habar", callback_data="general_broadcast")],
        ]
    )


def build_share_markup() -> InlineKeyboardMarkup:
    share_url = (
        f"tg://msg_url?url={quote_plus(SHARE_BOT_URL)}&text={quote_plus(SHARE_TEXT)}"
    )
    return InlineKeyboardMarkup([[InlineKeyboardButton("Share", url=share_url)]])


def build_close_chat_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("chatni yopish", callback_data="close_active_chat")],
            [InlineKeyboardButton("chat qo'sh", callback_data="add_active_chat")],
            [InlineKeyboardButton("chatdan uzish", callback_data="remove_active_chat")],
        ]
    )


def build_upload_video_id_choice_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Ha", callback_data="upload_video_id_yes"),
                InlineKeyboardButton("Yo'q", callback_data="upload_video_id_no"),
            ]
        ]
    )


def build_upload_video_comment_choice_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Ha", callback_data="upload_video_comment_yes"),
            InlineKeyboardButton("Yo'q", callback_data="upload_video_comment_no"),
        ]
    ])


def build_general_broadcast_confirm_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Ha", callback_data="general_broadcast_yes"),
                InlineKeyboardButton("Yo'q", callback_data="general_broadcast_no"),
            ]
        ]
    )


def build_chat_request_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("chatni ulash so'rovi", callback_data="request_chat_link")]]
    )


def get_active_chat_user_ids(context: ContextTypes.DEFAULT_TYPE) -> list[int]:
    raw = context.application.bot_data.get(ACTIVE_CHAT_USER_IDS_KEY, [])
    if not isinstance(raw, list):
        return []
    return [uid for uid in raw if isinstance(uid, int)]


def set_active_chat_user_ids(
    context: ContextTypes.DEFAULT_TYPE, user_ids: list[int]
) -> None:
    context.application.bot_data[ACTIVE_CHAT_USER_IDS_KEY] = [
        uid for uid in user_ids if isinstance(uid, int)
    ]


async def format_user_name_for_report(
    context: ContextTypes.DEFAULT_TYPE, user_id: int, user_data: dict[str, str]
) -> str:
    # First try to get from stored data (no API call)
    stored_username = user_data.get("username", "")
    if stored_username:
        return f"@{stored_username}"
    stored_full_name = user_data.get("full_name", "")
    if stored_full_name:
        return stored_full_name
    
    # Only make API call if we don't have stored data
    try:
        chat = await asyncio.wait_for(context.bot.get_chat(chat_id=user_id), timeout=3.0)
        if chat.username:
            return f"@{chat.username}"
        full_name = " ".join(
            part for part in [chat.first_name or "", chat.last_name or ""] if part
        ).strip()
        if full_name:
            return full_name
    except asyncio.TimeoutError:
        logger.warning("Report uchun user nomi olinmadi (%s): Timed out", user_id)
    except Exception as exc:
        logger.warning("Report uchun user nomi olinmadi (%s): %s", user_id, exc)
    
    return str(user_id)


def is_user_active(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is still active in bot by trying to get chat info"""
    try:
        # Try to get basic chat info to check if user still exists
        asyncio.create_task(context.bot.get_chat(user_id))
        return True
    except Exception:
        return False


async def filter_active_users(all_users: list, context: ContextTypes.DEFAULT_TYPE) -> list:
    """Filter out inactive users from the list"""
    async def check_user(user_pair):
        user_id, user_data = user_pair
        try:
            # Try to get chat info with short timeout
            chat = await asyncio.wait_for(context.bot.get_chat(user_id), timeout=1.5)
            
            # Additional check: verify user can receive messages by checking chat permissions
            if chat and hasattr(chat, 'id'):
                # Check if user has blocked the bot by attempting to get chat member info
                try:
                    member = await asyncio.wait_for(
                        context.bot.get_chat_member(chat_id=user_id, user_id=user_id), 
                        timeout=1.5
                    )
                    # If we can get member info, user is still active
                    return (user_id, user_data)
                except Exception:
                    # Can't get member info, likely blocked or left
                    return None
            else:
                return None
        except asyncio.TimeoutError:
            # User check timed out
            return None
        except Exception:
            # User is no longer active or has blocked the bot
            return None
    
    # Check users concurrently to speed up the process
    tasks = [check_user(user_pair) for user_pair in all_users]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out None results and exceptions
    active_users = []
    for result in results:
        if isinstance(result, tuple) and result is not None:
            active_users.append(result)
    
    return active_users


async def handle_admin_today_joined_report(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat:
        return False
    if normalize_query(message.text or "") != "8900":
        return False

    admin_id = get_admin_id()
    if admin_id is None or update.effective_chat.id != admin_id:
        return False

    today_str = datetime.now().strftime("%Y-%m-%d")
    today_users = [
        (user_id, user_data)
        for user_id, user_data in USERS.items()
        if user_data.get("first_join_date") == today_str
    ]
    today_users.sort(key=lambda item: item[1].get("first_joined_at", ""))
    if not today_users:
        await message.reply_text("Bugun hali yangi foydalanuvchi qo'shilmadi.")
        return True

    # Filter out inactive users
    active_today_users = await filter_active_users(today_users, context)
    
    lines = ["Bugun shu insonlar qo'shildi:"]
    # Filter out video uploaders from target users list
    target_user_ids = [user_id for user_id, _ in active_today_users if not is_video_uploader(user_id)]
    for idx, (user_id, user_data) in enumerate(active_today_users, start=1):
        # Filter out video uploaders from the list
        if is_video_uploader(user_id):
            continue
        user_name = await format_user_name_for_report(context, user_id, user_data)
        lines.append(f"{idx}. {user_name} 🎉")
    set_thanks_targets(context, update.effective_chat.id, "today", target_user_ids)
    await message.reply_text(
        "\n".join(lines),
        reply_markup=build_thanks_markup("today"),
    )
    return True


async def handle_admin_all_users_report(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat:
        return False
    if normalize_query(message.text or "") != "89000":
        return False

    admin_id = get_admin_id()
    if admin_id is None or update.effective_chat.id != admin_id:
        return False

    all_users = list(USERS.items())
    all_users.sort(key=lambda item: item[1].get("first_joined_at", ""))
    if not all_users:
        await message.reply_text("Botda hali foydalanuvchilar yo'q.")
        return True

    # Filter out inactive users and video uploaders
    active_users = await filter_active_users(all_users, context)
    
    lines = [f"Botda shuncha inson bor ({len(active_users)} ta):"]
    # Filter out video uploaders from target users list
    target_user_ids = [user_id for user_id, _ in active_users if not is_video_uploader(user_id)]
    for idx, (user_id, user_data) in enumerate(active_users, start=1):
        # Filter out video uploaders from the list
        if is_video_uploader(user_id):
            continue
        user_name = await format_user_name_for_report(context, user_id, user_data)
        lines.append(f"{idx}. {user_name} 🎉")
    set_thanks_targets(context, update.effective_chat.id, "all", target_user_ids)
    await message.reply_text(
        "\n".join(lines),
        reply_markup=build_thanks_markup("all"),
    )
    return True


async def on_thanks_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    if query.data not in {"thanks_today", "thanks_all"}:
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    report_key = query.data.replace("thanks_", "", 1)
    if not query.message:
        await query.answer("Xabar topilmadi.", show_alert=False)
        return
    chat_id = query.message.chat_id
    target_user_ids = get_thanks_targets(context, chat_id, report_key)
    if not target_user_ids:
        await query.answer("Yuborish uchun ro'yxat topilmadi.", show_alert=False)
        return

    await query.answer("Yuborilmoqda...")
    sent_count = 0
    failed_count = 0
    for user_id in target_user_ids:
        try:
            await context.bot.send_message(chat_id=user_id, text=THANKS_TEXT)
            sent_count += 1
        except Exception:
            failed_count += 1

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"Rahmat xabari yuborildi.\n"
            f"Yuborildi: {sent_count}\n"
            f"Yuborilmadi: {failed_count}"
        ),
    )


async def on_share_broadcast_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "share_broadcast":
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    await query.answer("Yuborilmoqda...")
    share_markup = build_share_markup()
    sent_count = 0
    failed_count = 0
    for user_id in USERS.keys():
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=SHARE_BROADCAST_TEXT,
                reply_markup=share_markup,
            )
            sent_count += 1
        except Exception:
            failed_count += 1

    if query.message:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                "Share xabari yuborildi.\n"
                f"Yuborildi: {sent_count}\n"
                f"Yuborilmadi: {failed_count}"
            ),
        )


async def on_sms_broadcast_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "sms_broadcast":
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    await query.answer()
    if query.message:
        await send_user_selection_list(
            context=context,
            chat_id=query.message.chat_id,
            action="replace",
        )


async def on_upload_video_id_choice(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data not in {"upload_video_id_yes", "upload_video_id_no"}:
        return
    if not query.from_user or not is_video_uploader(query.from_user.id):
        await query.answer("Bu tugma faqat video yuklovchi uchun.", show_alert=False)
        return

    state = context.user_data.get(UPLOAD_VIDEO_STATE_KEY)
    if not isinstance(state, dict) or state.get("stage") != "await_id_choice":
        await query.answer("So'rov muddati tugagan.", show_alert=False)
        return

    file_id = str(state.get("file_id", "")).strip()
    title = str(state.get("title", "")).strip()
    if not file_id or not title:
        context.user_data.pop(UPLOAD_VIDEO_STATE_KEY, None)
        await query.answer("Xatolik. Qayta yuklang.", show_alert=False)
        return

    if query.data == "upload_video_id_yes":
        state["stage"] = "await_custom_id"
        await query.answer()
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=t(context, "upload_video_id_prompt"),
        )
        return

    if query.data == "upload_video_id_no":
        # Ask for comment confirmation
        state["stage"] = "await_comment_choice"
        await query.answer()
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=t(context, "upload_video_comment_confirm_prompt"),
            reply_markup=build_upload_video_comment_choice_markup(),
        )
        return

    try:
        number = add_video_to_catalog(
            file_id=file_id,
            title=title,
            added_by=query.from_user.id,
        )
    except ValueError:
        await query.answer("Xatolik. Qayta urinib ko'ring.", show_alert=False)
        return
    context.user_data.pop(UPLOAD_VIDEO_STATE_KEY, None)
    await query.answer()
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=f"Video muvaffaqiyatli saqlandi! 📥\nVideo ID: {number}\nNomi: {title}",
    )


async def on_upload_video_comment_choice(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data not in {"upload_video_comment_yes", "upload_video_comment_no"}:
        return
    
    state = context.user_data.get(UPLOAD_VIDEO_STATE_KEY)
    if not isinstance(state, dict) or state.get("stage") != "await_comment_choice":
        await query.answer("So'rov muddati tugagan.", show_alert=False)
        return

    file_id = str(state.get("file_id", "")).strip()
    title = str(state.get("title", "")).strip()
    duration = int(state.get("duration", 0))
    custom_id = state.get("custom_id")
    
    if not file_id or not title:
        context.user_data.pop(UPLOAD_VIDEO_STATE_KEY, None)
        await query.answer("Xatolik. Qayta yuklang.", show_alert=False)
        return

    await query.answer()
    
    if query.data == "upload_video_comment_no":
        # Save without comment
        try:
            if custom_id:
                number = add_video_to_catalog(
                    file_id=file_id,
                    title=title,
                    added_by=query.from_user.id,
                    custom_id=custom_id,
                    duration=duration
                )
            else:
                number = add_video_to_catalog(
                    file_id=file_id,
                    title=title,
                    added_by=query.from_user.id,
                    duration=duration
                )
            context.user_data.pop(UPLOAD_VIDEO_STATE_KEY, None)
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text=f"Video muvaffaqiyatli saqlandi! 📥\nVideo ID: {number}\nNomi: {title}",
            )
        except ValueError:
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text="Xatolik. Qayta urinib ko'ring.",
            )
        return

    if query.data == "upload_video_comment_yes":
        # Ask for comment
        state["stage"] = "await_comment"
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=t(context, "upload_video_comment_prompt"),
        )
        return


async def on_general_broadcast_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "general_broadcast":
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    context.user_data[GENERAL_BROADCAST_STATE_KEY] = "await_content"
    context.user_data.pop(GENERAL_BROADCAST_TEXT_KEY, None)
    context.user_data.pop(GENERAL_BROADCAST_CONTENT_KEY, None)
    await query.answer()
    if query.message:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="Habar yozib qoldiring 🖊️",
        )


async def on_general_broadcast_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data not in {"general_broadcast_yes", "general_broadcast_no"}:
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    state = context.user_data.get(GENERAL_BROADCAST_STATE_KEY)
    message_text = context.user_data.get(GENERAL_BROADCAST_TEXT_KEY, "")
    message_content = context.user_data.get(GENERAL_BROADCAST_CONTENT_KEY)
    context.user_data.pop(GENERAL_BROADCAST_STATE_KEY, None)
    context.user_data.pop(GENERAL_BROADCAST_TEXT_KEY, None)
    context.user_data.pop(GENERAL_BROADCAST_CONTENT_KEY, None)

    if query.data == "general_broadcast_no":
        await query.answer("Bekor qilindi.", show_alert=False)
        if query.message:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Habar yuborilmadi.",
            )
        return

    if state != "await_confirm" or (not message_text and not message_content):
        await query.answer("Habar topilmadi.", show_alert=False)
        return

    await query.answer("Yuborilmoqda...")
    sent_count = 0
    failed_count = 0
    failed_user_ids: list[int] = []
    
    for user_id in USERS.keys():
        try:
            if message_content:
                # Send content (photo, video, document, audio, voice)
                content_type = message_content["type"]
                
                if content_type == "photo":
                    kwargs = {"chat_id": user_id, "photo": message_content["file_id"]}
                    if message_text:
                        kwargs["caption"] = message_text
                    await context.bot.send_photo(**kwargs)
                elif content_type == "video":
                    kwargs = {"chat_id": user_id, "video": message_content["file_id"]}
                    if message_text:
                        kwargs["caption"] = message_text
                    await context.bot.send_video(**kwargs)
                elif content_type == "animation":
                    kwargs = {"chat_id": user_id, "animation": message_content["file_id"]}
                    if message_text:
                        kwargs["caption"] = message_text
                    await context.bot.send_animation(**kwargs)
                elif content_type == "document":
                    kwargs = {"chat_id": user_id, "document": message_content["file_id"]}
                    if message_text:
                        kwargs["caption"] = message_text
                    await context.bot.send_document(**kwargs)
                elif content_type == "audio":
                    kwargs = {"chat_id": user_id, "audio": message_content["file_id"]}
                    if message_text:
                        kwargs["caption"] = message_text
                    await context.bot.send_audio(**kwargs)
                elif content_type == "voice":
                    kwargs = {"chat_id": user_id, "voice": message_content["file_id"]}
                    if message_text:
                        kwargs["caption"] = message_text
                    await context.bot.send_voice(**kwargs)
            else:
                # Send text only
                await context.bot.send_message(chat_id=user_id, text=message_text)
            sent_count += 1
        except Exception:
            failed_count += 1
            failed_user_ids.append(user_id)

    if query.message:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                "Umumiy habar yuborildi.\n"
                f"Yuborildi: {sent_count}\n"
                f"Yuborilmadi: {failed_count}"
            ),
        )
        if failed_user_ids:
            failed_lines: list[str] = []
            for failed_user_id in failed_user_ids:
                user_name = await format_user_name_for_report(
                    context,
                    failed_user_id,
                    USERS.get(failed_user_id, {}),
                )
                failed_lines.append(f"- {user_name} ({failed_user_id})")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Yuborilmaganlar:\n" + "\n".join(failed_lines),
            )


async def send_user_selection_list(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, action: str
) -> None:
    if action == "remove":
        remove_users = [(uid, USERS.get(uid, {})) for uid in get_active_chat_user_ids(context)]
        if not remove_users:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Hozircha ulangan chat yo'q.",
            )
            return
        options = remove_users
        header_text = "Uziladigan foydalanuvchini tanlang"
    else:
        all_users = list(USERS.items())
        all_users.sort(key=lambda item: item[1].get("first_joined_at", ""))
        if not all_users:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Botda hali foydalanuvchilar yo'q.",
            )
            return
        options = all_users
        header_text = "Foydalanuvchini tanlang"

    context.user_data[SMS_SELECT_MODE_KEY] = True
    context.user_data[SMS_SELECT_OPTIONS_KEY] = [user_id for user_id, _ in options]
    context.user_data[SMS_SELECT_ACTION_KEY] = action

    lines = [header_text]
    for idx, (user_id, user_data) in enumerate(options, start=1):
        user_name = await format_user_name_for_report(context, user_id, user_data)
        lines.append(f"{idx}. {user_name} 🎉")

    await context.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
    )


async def handle_admin_sms_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat:
        return False

    admin_id = get_admin_id()
    if admin_id is None or update.effective_chat.id != admin_id:
        return False
    if not context.user_data.get(SMS_SELECT_MODE_KEY):
        return False

    selected_text = normalize_query(message.text or "")

    options = context.user_data.get(SMS_SELECT_OPTIONS_KEY, [])
    if not isinstance(options, list) or not options:
        context.user_data[SMS_SELECT_MODE_KEY] = False
        context.user_data.pop(SMS_SELECT_ACTION_KEY, None)
        await message.reply_text("Ro'yxat topilmadi. SMS tugmasini qayta bosing.")
        return True

    select_action = str(context.user_data.get(SMS_SELECT_ACTION_KEY, "replace"))
    raw_indexes = [part.strip() for part in selected_text.split(",") if part.strip()]
    if not raw_indexes:
        await message.reply_text("Iltimos, ro'yxat raqamini yuboring.")
        return True

    if select_action != "add" and len(raw_indexes) > 1:
        await message.reply_text("Bu rejimda faqat bitta raqam yuboring.")
        return True

    selected_user_ids: list[int] = []
    for raw_index in raw_indexes:
        if not raw_index.isdigit():
            await message.reply_text("Raqamlarni to'g'ri yuboring. Masalan: 1,3,7")
            return True
        selected_index = int(raw_index) - 1
        if selected_index < 0 or selected_index >= len(options):
            await message.reply_text("Bunday raqam yo'q. Qayta yuboring.")
            return True
        selected_user_id = options[selected_index]
        if not isinstance(selected_user_id, int):
            await message.reply_text("Tanlovda xatolik. Qayta urinib ko'ring.")
            return True
        if selected_user_id not in selected_user_ids:
            selected_user_ids.append(selected_user_id)

    if not selected_user_ids:
        await message.reply_text("Tanlov bo'sh. Qayta yuboring.")
        return True

    context.user_data[SELECTED_CHAT_USER_ID_KEY] = selected_user_ids[0]
    active_user_ids = get_active_chat_user_ids(context)
    if select_action == "add":
        for selected_user_id in selected_user_ids:
            if selected_user_id not in active_user_ids:
                active_user_ids.append(selected_user_id)
        status_text = "Chatga foydalanuvchilar qo'shildi"
    elif select_action == "remove":
        selected_user_id = selected_user_ids[0]
        active_user_ids = [uid for uid in active_user_ids if uid != selected_user_id]
        status_text = "Foydalanuvchi chatdan uzildi"
    else:
        selected_user_id = selected_user_ids[0]
        active_user_ids = [selected_user_id]
        status_text = "Chat ulandi"
    set_active_chat_user_ids(context, active_user_ids)
    active_count = len(active_user_ids)
    context.user_data[SMS_SELECT_MODE_KEY] = False
    context.user_data.pop(SMS_SELECT_ACTION_KEY, None)
    await message.reply_text(
        f"{status_text}\nJami: {active_count} ta odam",
        reply_markup=build_close_chat_markup(),
    )
    try:
        for selected_user_id in selected_user_ids:
            await context.bot.send_message(
                chat_id=selected_user_id,
                text="chat ulandi adminga yozishingiz mumkin",
            )
    except Exception as exc:
        logger.warning("Tanlangan userga chat ulandi xabari yuborilmadi: %s", exc)
        await message.reply_text("Foydalanuvchiga xabar yuborilmadi.")
    return True


async def handle_admin_general_broadcast_content(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat:
        return False

    admin_id = get_admin_id()
    if admin_id is None or update.effective_chat.id != admin_id:
        return False

    state = context.user_data.get(GENERAL_BROADCAST_STATE_KEY)
    if state is None:
        return False

    if state == "await_confirm":
        await message.reply_text("Iltimos, Ha/Yo'q tugmalaridan foydalaning.")
        return True

    if state != "await_content":
        context.user_data.pop(GENERAL_BROADCAST_STATE_KEY, None)
        context.user_data.pop(GENERAL_BROADCAST_TEXT_KEY, None)
        context.user_data.pop(GENERAL_BROADCAST_CONTENT_KEY, None)
        return False

    content_data = None
    text_content = None

    # Check for content types
    if message.photo:
        content_data = {"type": "photo", "file_id": message.photo[-1].file_id}
        text_content = message.caption or ""
    elif message.video:
        content_data = {"type": "video", "file_id": message.video.file_id}
        text_content = message.caption or ""
    elif message.animation:
        content_data = {"type": "animation", "file_id": message.animation.file_id}
        text_content = message.caption or ""
    elif message.document:
        content_data = {"type": "document", "file_id": message.document.file_id}
        text_content = message.caption or ""
    elif message.audio:
        content_data = {"type": "audio", "file_id": message.audio.file_id}
        text_content = message.caption or ""
    elif message.voice:
        content_data = {"type": "voice", "file_id": message.voice.file_id}
        text_content = message.caption or ""
    elif message.text and message.text.strip():
        text_content = message.text.strip()
    else:
        await message.reply_text("Iltimos, matn, rasm, video, GIF, fayl, audio yoki voice yuboring.")
        return True

    context.user_data[GENERAL_BROADCAST_TEXT_KEY] = text_content
    if content_data:
        context.user_data[GENERAL_BROADCAST_CONTENT_KEY] = content_data
    context.user_data[GENERAL_BROADCAST_STATE_KEY] = "await_confirm"
    
    preview_text = f"Yuboriladigan habar:\n\n{text_content}"
    if content_data:
        preview_text += f"\n\n📎 {content_data['type'].upper()} qo'shilgan"
    
    await message.reply_text(
        preview_text,
        reply_markup=build_general_broadcast_confirm_markup(),
    )
    return True


async def relay_link_video_in_active_chat(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    active_user_ids: list[int],
    admin_id: int,
) -> bool:
    message = update.message
    if not message or not update.effective_user or not update.effective_chat:
        return False
    if not message.text:
        return False

    query_text = normalize_query(message.text)
    if not looks_like_url(query_text):
        return False

    sender_id = update.effective_user.id
    sender_chat_id = update.effective_chat.id
    if sender_id == admin_id and sender_chat_id == admin_id:
        target_chat_ids = active_user_ids
        sender_label = "Admin"
    elif sender_id in active_user_ids:
        target_chat_ids = [admin_id] + [uid for uid in active_user_ids if uid != sender_id]
        sender_label = await format_user_name_for_report(
            context,
            sender_id,
            USERS.get(sender_id, {}),
        )
    else:
        return False

    if LOADING_STICKER_FILE_ID:
        status_msg = await message.reply_sticker(LOADING_STICKER_FILE_ID)
    else:
        status_msg = await message.reply_text("⏳")

    try:
        with TemporaryDirectory() as temp_dir:
            file_path, _ = await to_thread(download_video_sync, query_text, temp_dir)
            if not os.path.exists(file_path):
                await message.reply_text(t(context, "video_file_not_found"))
                return True

            file_size = os.path.getsize(file_path)
            if file_size > MAX_VIDEO_BYTES:
                await message.reply_text(t(context, "video_too_large"))
                return True

            for target_chat_id in target_chat_ids:
                with open(file_path, "rb") as video_file:
                    await context.bot.send_video(
                        chat_id=target_chat_id,
                        video=video_file,
                        caption=f"{sender_label} yuborgan video",
                        supports_streaming=True,
                    )
    except Exception as exc:
        logger.exception("Active chat link video relay xatosi: %s", exc)
        await message.reply_text("Video yuklab bo'lmadi. Iltimos qayta urinib ko'ring.")
        return True
    finally:
        try:
            await status_msg.delete()
        except Exception:
            pass
        try:
            await message.delete()
        except Exception:
            pass

    return True


async def relay_active_chat_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat or not update.effective_user:
        return False

    admin_id = get_admin_id()
    if admin_id is None:
        return False

    active_user_ids = get_active_chat_user_ids(context)
    if not active_user_ids:
        return False

    if await relay_link_video_in_active_chat(update, context, active_user_ids, admin_id):
        return True

    sender_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if sender_id == admin_id and chat_id == admin_id:
        if normalize_query(message.text or "") in {"8900", "89000"}:
            return False
        for active_user_id in active_user_ids:
            await context.bot.send_message(
                chat_id=active_user_id,
                text=f"Admin:\n{message.text or ''}",
            )
        return True

    if sender_id in active_user_ids:
        user_name = await format_user_name_for_report(
            context,
            sender_id,
            USERS.get(sender_id, {}),
        )
        await context.bot.send_message(
            chat_id=admin_id,
            text=f"{user_name}:\n{message.text or ''}",
        )
        for peer_user_id in active_user_ids:
            if peer_user_id == sender_id:
                continue
            await context.bot.send_message(
                chat_id=peer_user_id,
                text=f"{user_name}:\n{message.text or ''}",
            )
        return True

    return False


async def relay_active_chat_non_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat or not update.effective_user:
        return False

    admin_id = get_admin_id()
    if admin_id is None:
        return False

    active_user_ids = get_active_chat_user_ids(context)
    if not active_user_ids:
        return False

    sender_id = update.effective_user.id
    source_chat_id = update.effective_chat.id

    if sender_id == admin_id and source_chat_id == admin_id:
        for active_user_id in active_user_ids:
            await context.bot.send_message(chat_id=active_user_id, text="Admin media yubordi")
            await context.bot.copy_message(
                chat_id=active_user_id,
                from_chat_id=source_chat_id,
                message_id=message.message_id,
            )
        return True

    if sender_id in active_user_ids:
        user_name = await format_user_name_for_report(
            context,
            sender_id,
            USERS.get(sender_id, {}),
        )
        await context.bot.send_message(chat_id=admin_id, text=f"{user_name} media yubordi")
        await context.bot.copy_message(
            chat_id=admin_id,
            from_chat_id=source_chat_id,
            message_id=message.message_id,
        )
        for peer_user_id in active_user_ids:
            if peer_user_id == sender_id:
                continue
            await context.bot.send_message(
                chat_id=peer_user_id,
                text=f"{user_name} media yubordi",
            )
            await context.bot.copy_message(
                chat_id=peer_user_id,
                from_chat_id=source_chat_id,
                message_id=message.message_id,
            )
        return True

    return False


async def handle_admin_video_upload(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not message.video or not update.effective_user or not update.effective_chat:
        return False

    if not is_video_uploader(update.effective_user.id):
        return False
    if update.effective_chat.id != update.effective_user.id:
        return False

    active_user_ids = get_active_chat_user_ids(context)
    if active_user_ids:
        return False

    context.user_data[UPLOAD_VIDEO_STATE_KEY] = {
        "file_id": message.video.file_id,
        "duration": message.video.duration,
        "stage": "await_title",
    }
    await message.reply_text(t(context, "upload_video_name_prompt"))
    return True


async def handle_user_non_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if await handle_admin_general_broadcast_content(update, context):
        return
    if await handle_admin_video_upload(update, context):
        return
    if await relay_active_chat_non_text(update, context):
        return


async def on_close_active_chat_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "close_active_chat":
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    active_user_ids = get_active_chat_user_ids(context)
    context.application.bot_data.pop(ACTIVE_CHAT_USER_IDS_KEY, None)
    context.user_data.pop(SELECTED_CHAT_USER_ID_KEY, None)
    context.user_data[SMS_SELECT_MODE_KEY] = False
    context.user_data.pop(SMS_SELECT_OPTIONS_KEY, None)
    context.user_data.pop(SMS_SELECT_ACTION_KEY, None)

    if not active_user_ids:
        await query.answer("Aktiv chat topilmadi.", show_alert=False)
        return

    await query.answer("Chat yopildi")
    if query.message:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="Chat to'xtatildi.",
        )


async def on_change_active_chat_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "change_active_chat":
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    await query.answer()
    if query.message:
        await send_user_selection_list(
            context=context,
            chat_id=query.message.chat_id,
            action="replace",
        )


async def on_add_active_chat_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "add_active_chat":
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    await query.answer()
    if query.message:
        await send_user_selection_list(
            context=context,
            chat_id=query.message.chat_id,
            action="add",
        )


async def on_remove_active_chat_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "remove_active_chat":
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    await query.answer()
    if query.message:
        await send_user_selection_list(
            context=context,
            chat_id=query.message.chat_id,
            action="remove",
        )


async def on_user_chat_request_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "request_chat_link" or not query.from_user:
        return

    await query.answer("So'rov yuborildi")
    admin_id = get_admin_id()
    if admin_id is None:
        if query.message:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Hozircha admin bilan ulanish mavjud emas.",
            )
        return

    user_id = query.from_user.id
    user_nick = f"@{query.from_user.username}" if query.from_user.username else query.from_user.full_name
    request_text = (
        f"{user_nick} ({user_id}) sizdan chatni ulashni so'rayapti."
    )
    admin_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("chatni ulash", callback_data=f"connect_request_{user_id}")]]
    )
    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=request_text,
            reply_markup=admin_markup,
        )
    except Exception as exc:
        logger.warning("Admin'ga ulash so'rovi yuborilmadi: %s", exc)
        return

    if query.message:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="So'rovingiz adminga yuborildi.",
        )


async def on_admin_connect_request_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("connect_request_"):
        return

    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return

    raw_user_id = query.data.replace("connect_request_", "", 1)
    if not raw_user_id.isdigit():
        await query.answer("User ID xato.", show_alert=False)
        return

    user_id = int(raw_user_id)
    set_active_chat_user_ids(context, [user_id])
    active_count = len(get_active_chat_user_ids(context))
    context.user_data[SELECTED_CHAT_USER_ID_KEY] = user_id
    context.user_data[SMS_SELECT_MODE_KEY] = False
    context.user_data.pop(SMS_SELECT_OPTIONS_KEY, None)
    context.user_data.pop(SMS_SELECT_ACTION_KEY, None)

    await query.answer("Chat ulandi")
    if query.message:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"Chat ulandi\nJami: {active_count} ta odam",
            reply_markup=build_close_chat_markup(),
        )
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text="chat ulandi adminga yozishingiz mumkin",
        )
    except Exception as exc:
        logger.warning("So'rovdan keyin userga chat ulash xabari yuborilmadi: %s", exc)


async def handle_admin_chat_panel_shortcut(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat:
        return False

    admin_id = get_admin_id()
    if admin_id is None or update.effective_chat.id != admin_id:
        return False

    if normalize_query(message.text or "").upper() != "UZU":
        return False

    active_count = len(get_active_chat_user_ids(context))
    await message.reply_text(
        f"Chat boshqaruvi (ulangan: {active_count} ta)",
        reply_markup=build_close_chat_markup(),
    )
    return True


async def handle_admin_video_uploaders_report(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat:
        return False
    if normalize_query(message.text or "").lower() != "zuz":
        return False
    admin_id = get_admin_id()
    if admin_id is None or update.effective_chat.id != admin_id:
        return False

    # Use current VIDEO_UPLOADERS set instead of extracting from catalog
    uploader_ids = list(VIDEO_UPLOADERS)
    
    if not uploader_ids:
        await message.reply_text(t(context, "uploader_list_empty"))
        return True

    lines = ["Video yuklovchilar:"]
    # Always show Umidjon_299 first (user ID: 8239140931)
    umidjon_id = 8239140931
    sorted_uploader_ids = sorted(uploader_ids, key=lambda x: (0 if x == umidjon_id else 1, x))
    
    for idx, uploader_id in enumerate(sorted_uploader_ids, start=1):
        user_name = await format_user_name_for_report(
            context,
            uploader_id,
            USERS.get(uploader_id, {}),
        )
        lines.append(f"{idx}. {user_name}")
    actions = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Yuklovchi qo'shish", callback_data="uploader_add"),
                InlineKeyboardButton("Yuklovchini olib tashlash", callback_data="uploader_remove"),
            ]
        ]
    )
    await message.reply_text("\n".join(lines), reply_markup=actions)
    return True


async def send_uploader_selection_list(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, action: str
) -> None:
    if action == "remove":
        options = [
            (uid, USERS.get(uid, {}))
            for uid in sorted(VIDEO_UPLOADERS)
            if isinstance(uid, int)
        ]
        options = [item for item in options if item[0] != VIDEO_ADMIN_ID]
        header_text = t(context, "uploader_list_header_remove")
    else:
        all_users = list(USERS.items())
        all_users.sort(key=lambda item: item[1].get("first_joined_at", ""))
        options = all_users
        header_text = t(context, "uploader_list_header_add")

    if not options:
        await context.bot.send_message(chat_id=chat_id, text=t(context, "uploader_list_empty"))
        return

    context.user_data[UPLOADER_SELECT_MODE_KEY] = True
    context.user_data[UPLOADER_SELECT_ACTION_KEY] = action
    context.user_data[UPLOADER_SELECT_OPTIONS_KEY] = [user_id for user_id, _ in options]

    lines = [header_text]
    for idx, (user_id, user_data) in enumerate(options, start=1):
        user_name = await format_user_name_for_report(context, user_id, user_data)
        lines.append(f"{idx}. {user_name} 🎉")

    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))


async def handle_admin_uploader_selection(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_chat:
        return False
    admin_id = get_admin_id()
    if admin_id is None or update.effective_chat.id != admin_id:
        return False
    if not context.user_data.get(UPLOADER_SELECT_MODE_KEY):
        return False

    selected_text = normalize_query(message.text or "")
    options = context.user_data.get(UPLOADER_SELECT_OPTIONS_KEY, [])
    if not isinstance(options, list) or not options:
        context.user_data[UPLOADER_SELECT_MODE_KEY] = False
        context.user_data.pop(UPLOADER_SELECT_ACTION_KEY, None)
        await message.reply_text("Ro'yxat topilmadi. Qayta urinib ko'ring.")
        return True

    raw_indexes = [part.strip() for part in selected_text.split(",") if part.strip()]
    if not raw_indexes:
        await message.reply_text(t(context, "saved_videos_delete_invalid"))
        return True

    selected_user_ids: list[int] = []
    for raw_index in raw_indexes:
        if not raw_index.isdigit():
            await message.reply_text(t(context, "saved_videos_delete_invalid"))
            return True
        selected_index = int(raw_index) - 1
        if selected_index < 0 or selected_index >= len(options):
            await message.reply_text(t(context, "saved_videos_delete_not_found"))
            return True
        selected_user_id = options[selected_index]
        if not isinstance(selected_user_id, int):
            await message.reply_text("Tanlovda xatolik. Qayta urinib ko'ring.")
            return True
        if selected_user_id not in selected_user_ids:
            selected_user_ids.append(selected_user_id)

    if not selected_user_ids:
        await message.reply_text("Tanlov bo'sh. Qayta yuboring.")
        return True

    action = str(context.user_data.get(UPLOADER_SELECT_ACTION_KEY, "add"))
    if action == "remove":
        for user_id in selected_user_ids:
            if user_id != VIDEO_ADMIN_ID and user_id in VIDEO_UPLOADERS:
                VIDEO_UPLOADERS.discard(user_id)
                # Send notification to removed uploader
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="siz endi video yuklay olmaysiz 🥲"
                    )
                except Exception as e:
                    # If user hasn't started the bot or blocked it, continue with others
                    pass
        save_video_uploaders()
        reply_text = t(context, "uploader_removed")
    else:
        for user_id in selected_user_ids:
            VIDEO_UPLOADERS.add(user_id)
            # Send notification to newly added uploader
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="siz endi video yuklashingiz mumkin 🎉"
                )
            except Exception as e:
                # If user hasn't started the bot or blocked it, continue with others
                pass
        save_video_uploaders()
        reply_text = t(context, "uploader_added")

    context.user_data[UPLOADER_SELECT_MODE_KEY] = False
    context.user_data.pop(UPLOADER_SELECT_ACTION_KEY, None)
    context.user_data.pop(UPLOADER_SELECT_OPTIONS_KEY, None)
    await message.reply_text(reply_text)
    return True


def normalize_query(raw_text: str) -> str:
    return " ".join(raw_text.strip().split())


def looks_like_url(text: str) -> bool:
    parsed = urlparse(text.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def download_video_sync(url: str, download_dir: str) -> tuple[str, str]:
    output_template = str(Path(download_dir) / "%(id)s.%(ext)s")
    options = {
        "format": FAST_VIDEO_FORMAT,
        "outtmpl": output_template,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 12,
        "retries": 2,
        "fragment_retries": 2,
        "concurrent_fragment_downloads": 4,
        "noprogress": True,
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(url, download=True)
        file_path = ydl.prepare_filename(info)
        if not os.path.exists(file_path):
            requested = info.get("requested_downloads") or []
            if requested and requested[0].get("filepath"):
                file_path = requested[0]["filepath"]
        title = info.get("title") or "video"
    return file_path, title


async def notify_admin_about_referral(
    context: ContextTypes.DEFAULT_TYPE, referrer_id: int, new_user_id: int
) -> None:
    if not ADMIN_ID_RAW:
        return
    try:
        admin_id = int(ADMIN_ID_RAW)
    except ValueError:
        logger.warning("ADMIN_ID noto'g'ri formatda: %s", ADMIN_ID_RAW)
        return

    referral_pair = (referrer_id, new_user_id)
    if referral_pair in NOTIFIED_REFERRALS:
        return

    async def format_user_label(user_id: int) -> str:
        try:
            chat = await context.bot.get_chat(chat_id=user_id)
            if chat.username:
                return f"@{chat.username}"
            full_name = " ".join(
                part for part in [chat.first_name, chat.last_name] if part
            ).strip()
            if full_name:
                return full_name
        except Exception as exc:
            logger.warning("User info olinmadi (%s): %s", user_id, exc)
        return f"user_id={user_id}"

    referrer_label = await format_user_label(referrer_id)
    new_user_label = await format_user_label(new_user_id)
    text = (
        f"{referrer_label} ({referrer_id}) tomonidan\n"
        f"{new_user_label} ({new_user_id}) qo'shildi."
    )
    try:
        await context.bot.send_message(chat_id=admin_id, text=text)
        NOTIFIED_REFERRALS.add(referral_pair)
    except Exception as exc:
        logger.warning("Admin xabar yuborilmadi: %s", exc)


async def notify_admin_about_new_user(
    context: ContextTypes.DEFAULT_TYPE, new_user_id: int
) -> None:
    admin_id = get_admin_id()
    if admin_id is None:
        return
    if new_user_id == admin_id:
        return
    if new_user_id in NOTIFIED_NEW_USERS:
        return

    try:
        chat = await context.bot.get_chat(chat_id=new_user_id)
        if chat.username:
            user_label = f"@{chat.username}"
        else:
            full_name = " ".join(
                part for part in [chat.first_name, chat.last_name] if part
            ).strip()
            user_label = full_name or f"user_id={new_user_id}"
    except Exception as exc:
        logger.warning("Yangi user info olinmadi (%s): %s", new_user_id, exc)
        user_label = f"user_id={new_user_id}"

    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=f"Yangi foydalanuvchi qo'shildi: {user_label} ({new_user_id})",
        )
        NOTIFIED_NEW_USERS.add(new_user_id)
    except Exception as exc:
        logger.warning("Admin'ga yangi user xabari yuborilmadi: %s", exc)


async def malumot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    await update.message.reply_text(
        "Admin bilan bog'lanish uchun pastdagi tugmani bosing.",
        reply_markup=build_chat_request_markup(),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    is_new_user, _ = register_user(update.effective_user)
    if is_new_user:
        await notify_admin_about_new_user(
            context=context,
            new_user_id=update.effective_user.id,
        )
    user_position = get_user_position(update.effective_user.id)

    referrer_id = parse_referrer_id(context.args)
    if referrer_id and referrer_id != update.effective_user.id:
        await notify_admin_about_referral(
            context=context,
            referrer_id=referrer_id,
            new_user_id=update.effective_user.id,
        )

    shared_video_id = parse_video_share(context.args)
    if shared_video_id is not None:
        video = get_video_by_number(shared_video_id)
        if not video:
            await update.message.reply_text("Video topilmadi.")
            return

        file_id = str(video.get("file_id", "")).strip()
        title = str(video.get("title", "")).strip()
        comment = str(video.get("comment", "")).strip()
        duration = int(video.get("duration", 0))
        duration_str = format_duration(duration)

        if comment:
            caption = f"🎥filim nomi: {title}\n🗝️film kodi : {shared_video_id}\n🧭film avqti: {duration_str}\n✉️commant:{comment}\n\n@Hidop_bot tomonidan yuklab olindi"
        else:
            caption = f"🎥filim nomi: {title}\n🗝️film kodi : {shared_video_id}\n🧭film avqti: {duration_str}\n\n@Hidop_bot tomonidan yuklab olindi"

        await update.message.reply_video(
            video=file_id,
            caption=caption,
            reply_markup=create_video_buttons_with_users(
                shared_video_id,
                context,
                update.effective_user.id,
            ),
            supports_streaming=True,
        )
        return

    send_target_video = parse_send_target_video(context.args)
    if send_target_video is not None:
        target_user_id, video_id = send_target_video
        video = get_video_by_number(video_id)
        if not video:
            await update.message.reply_text("Video topilmadi.")
            return

        file_id = str(video.get("file_id", "")).strip()
        title = str(video.get("title", "")).strip()
        comment = str(video.get("comment", "")).strip()
        duration = int(video.get("duration", 0))
        duration_str = format_duration(duration)

        if comment:
            caption = f"🎥filim nomi: {title}\n🗝️film kodi : {video_id}\n🧭film avqti: {duration_str}\n✉️commant:{comment}\n\n@Hidop_bot tomonidan yuklab olindi"
        else:
            caption = f"🎥filim nomi: {title}\n🗝️film kodi : {video_id}\n🧭film avqti: {duration_str}\n\n@Hidop_bot tomonidan yuklab olindi"

        try:
            await context.bot.send_video(
                chat_id=target_user_id,
                video=file_id,
                caption=caption,
                reply_markup=create_video_buttons_with_users(
                    video_id,
                    context,
                    target_user_id,
                ),
                supports_streaming=True,
            )
            await update.message.reply_text(
                f"Video `{target_user_id}` userga yuborildi ✅",
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Start payload orqali video yuborilmadi (%s -> %s): %s", video_id, target_user_id, exc)
            await update.message.reply_text("Video yuborishda xatolik bo'ldi.")
        return

    await update.message.reply_text(f"Siz {user_position}-foydalanuvchisiz 🎉")

    # Set Uzbek Latin as default language
    context.user_data[LANG_KEY] = "lang_uz_lat"
    
    # Send welcome message
    text = (
        "👋 Assalomu alaykum! Botimizga xush kelibsiz\n"
        "botimzda siz qidirgan kinoni topasiz 😉\n"
        "🔗 qo'llab quvatlash uchun pastagi tugmani bosing"
    )
    keyboard = [
        [
            InlineKeyboardButton("📤 Botni ulashish", url=build_share_button_url(update.effective_user.id)),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(text, reply_markup=reply_markup)
    
    # Wait 2 seconds then send second message
    await asyncio.sleep(2)
    
    second_text = "BIZ BILAN ZERIKMAYSIZ 😉\nBOSHLASH UCHUN START  NI BOSING ✅"
    second_keyboard = [
        [
            InlineKeyboardButton("🚀 Boshlash", callback_data="start_using_bot"),
        ]
    ]
    second_reply_markup = InlineKeyboardMarkup(second_keyboard)
    await update.message.reply_text(second_text, reply_markup=second_reply_markup)


async def on_start_using_bot_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or query.data != "start_using_bot":
        return
    await query.answer()
    
    # Send help message about how to use the bot
    help_text = (
        "🎬 Botdan foydalanish uchun:\n\n"
        "📝 Kino nomini yoki raqamini yuboring\n"
        "🔗 Video havolasini yuboring\n"
        "📥 Videoni saqlab olishingiz mumkin\n\n"
        "👉 Endi kino nomini yuboring!"
    )
    
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=help_text
    )
    
    # Wait 2 seconds then send video count message
    await asyncio.sleep(2)
    
    # Send video count message
    video_count = len(VIDEO_CATALOG.get("items", []))
    count_message = f"Bizda hozirda {video_count} ta kino bor 😅"
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=count_message
    )


async def on_language_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query:
        await query.answer()
        selected_message = LANGUAGE_MESSAGES.get(query.data)
        if selected_message:
            context.user_data[LANG_KEY] = query.data
            share_button_url = build_share_button_url(query.from_user.id)
            share_markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton(t(context, "share_button"), url=share_button_url)]]
            )
            await query.edit_message_text(selected_message, reply_markup=share_markup)


async def inline_share_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    inline_query = update.inline_query
    if not inline_query:
        return
    file_id = LAST_VIDEO_FILE_ID_BY_USER.get(inline_query.from_user.id)
    if not file_id:
        await inline_query.answer(
            [
                InlineQueryResultArticle(
                    id="no_video",
                    title=t(context, "inline_no_video_title"),
                    input_message_content=InputTextMessageContent(
                        t(context, "inline_no_video_text")
                    ),
                    description=t(context, "inline_no_video_description"),
                )
            ],
            cache_time=1,
            is_personal=True,
        )
        return

    # Create buttons for inline video
    video_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👎🏻", callback_data=f"dislike_video_inline_{inline_query.from_user.id}"),
            InlineKeyboardButton("👍🏻", callback_data=f"like_video_inline_{inline_query.from_user.id}")
        ],
        [InlineKeyboardButton(
            t(context, "save_button"),
            callback_data=f"save_video_inline_{inline_query.from_user.id}"
        )],
        [InlineKeyboardButton(
            "Kimlar reaksiya qildi?",
            callback_data=f"show_reactions_inline_{inline_query.from_user.id}"
        )]
    ])

    await inline_query.answer(
        [
            InlineQueryResultCachedVideo(
                id="share_last_video",
                video_file_id=file_id,
                title=t(context, "inline_send_video_title"),
                description=t(context, "inline_send_video_description"),
                reply_markup=video_markup,
            )
        ],
        cache_time=1,
        is_personal=True,
    )


async def videos_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    items = VIDEO_CATALOG.get("items", [])
    if not isinstance(items, list) or not items:
        await update.message.reply_text(t(context, "video_list_empty"))
        return
    
    # Show all videos
    buttons = []
    
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        number = item.get("id")
        title = str(item.get("title", "")).strip()
        if not title:
            title = "Video"
        if isinstance(number, int):
            # Preserve line breaks in title by replacing them with a visible separator
            formatted_title = title.replace('\n', ' | ')
            # Use only title as button text (without number)
            button_text = formatted_title
            button = InlineKeyboardButton(button_text, callback_data=f"send_video_{number}")
            
            # Add buttons in pairs (2 per row)
            if i % 2 == 0:
                buttons.append([button])
            else:
                buttons[-1].append(button)
    
    # Handle odd number of videos
    if len(items) % 2 == 1:
        # Last row has only one button, that's fine
        pass
    
    if buttons:
        reply_markup = InlineKeyboardMarkup(buttons)
        total_count = len(items)
        await update.message.reply_text(f"Topilgan videolar (1-{total_count} {total_count} dan):", reply_markup=reply_markup)
    else:
        await update.message.reply_text(t(context, "video_list_empty"))
    
    # Send admin controls separately if admin
    if update.effective_user and update.effective_user.id == VIDEO_ADMIN_ID:
        admin_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(t(context, "saved_videos_clear"), callback_data="catalog_videos_clear")]]
        )
        await update.message.reply_text("Admin boshqaruvi:", reply_markup=admin_markup)


async def handle_video_number_request(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not message.text:
        return False
    text = normalize_query(message.text)
    if not text.isdigit():
        return False
    if text in {"8900", "89000"}:
        return False
    
    number = int(text)
    video = get_video_by_number(number)
    if not video:
        await message.reply_text(t(context, "video_number_not_found"))
        return True
    
    file_id = str(video.get("file_id", ""))
    title = str(video.get("title", "")).strip()
    comment = str(video.get("comment", "")).strip()
    duration = int(video.get("duration", 0))
    duration_str = format_duration(duration)
    
    # Format caption with film name, ID, duration, and comment
    if comment:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {number}\n🧭film avqti: {duration_str}\n✉️commant:{comment}\n\n@Hidop_bot tomonidan yuklab olindi"
    else:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {number}\n🧭film avqti: {duration_str}\n\n@Hidop_bot tomonidan yuklab olindi"
    
    save_markup = create_video_buttons_with_users(number, context, update.effective_user.id if update.effective_user else None)
    await message.reply_video(
        video=file_id,
        caption=caption,
        reply_markup=save_markup,
        supports_streaming=True,
    )
    return True


async def handle_video_name_request(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not message.text:
        return False
    text = normalize_query(message.text)
    if not text or text.isdigit():
        return False
    if text in {"8900", "89000"}:
        return False
    # Don't process URLs - let them be handled by the URL download logic
    if looks_like_url(text):
        return False
    
    videos = get_videos_by_name(text)
    if not videos:
        await message.reply_text(t(context, "video_number_not_found"))
        return True
    
    if len(videos) == 1:
        # If only one video found, send it directly
        video = videos[0]
        file_id = str(video.get("file_id", "")).strip()
        if not file_id:
            await message.reply_text(t(context, "video_number_not_found"))
            return True
        title = str(video.get("title", "")).strip()
        number = int(video.get("id", 0))
        comment = str(video.get("comment", "")).strip()
        duration = int(video.get("duration", 0))
        duration_str = format_duration(duration)
        
        # Format caption with film name, ID, duration, and comment
        if comment:
            caption = f"🎥filim nomi: {title}\n🗝️film kodi : {number}\n🧭film avqti: {duration_str}\n✉️commant:{comment}\n\n@Hidop_bot tomonidan yuklab olindi"
        else:
            caption = f"🎥filim nomi: {title}\n🗝️film kodi : {number}\n🧭film avqti: {duration_str}\n\n@Hidop_bot tomonidan yuklab olindi"
        
        save_markup = create_video_buttons_with_users(number, context, update.effective_user.id if update.effective_user else None)
        await message.reply_video(
            video=file_id,
            caption=caption,
            reply_markup=save_markup,
            supports_streaming=True,
        )
        return True
    else:
        # If multiple videos found, show buttons
        buttons = []
        for i, video in enumerate(videos[:10]):  # Limit to 10 results
            title = str(video.get("title", "")).strip()
            number = int(video.get("id", 0))
            # Preserve line breaks in title by replacing them with a visible separator
            formatted_title = title.replace('\n', ' | ')
            # Use only title as button text (without number)
            button_text = formatted_title
            button = InlineKeyboardButton(button_text, callback_data=f"send_video_{number}")
            
            # Add buttons in pairs (2 per row)
            if i % 2 == 0:
                buttons.append([button])
            else:
                buttons[-1].append(button)
        
        # Handle odd number of videos
        if len(videos[:10]) % 2 == 1:
            # Last row has only one button, that's fine
            pass
        
        if len(videos) > 10:
            # Add info message about more results (full width)
            buttons.append([
                InlineKeyboardButton("Davomi 🎥", callback_data="more_videos_10")
            ])
        
        # Store search results in user data for "Davomi 🎥" functionality
        context.user_data['search_results'] = videos
        context.user_data['search_query'] = text
        
        reply_markup = InlineKeyboardMarkup(buttons)
        total_count = len(videos)
        shown_count = min(10, total_count)
        await message.reply_text(f"Topilgan videolar (1-{shown_count} {total_count} dan):", reply_markup=reply_markup)
        return True


async def on_no_action_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle no_action callback (do nothing)"""
    query = update.callback_query
    if not query or not query.data or query.data != "no_action":
        return
    await query.answer()


async def on_more_videos_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle showing more videos from search results"""
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("more_videos_"):
        return
    
    await query.answer()
    
    # Extract offset from callback_data
    try:
        offset = int(query.data.split("_")[2])
    except (IndexError, ValueError):
        offset = 10
    
    # Get search results from user data
    videos = context.user_data.get('search_results', [])
    search_query = context.user_data.get('search_query', '')
    
    if not videos:
        await query.edit_message_text("Qidiruv natijalari topilmadi. Iltimos qayta urinib ko'ring.")
        return
    
    # Show videos from offset to offset+10 (or remaining if less)
    start_index = offset
    end_index = min(offset + 10, len(videos))
    
    buttons = []
    for i in range(start_index, end_index):
        video = videos[i]
        title = str(video.get("title", "")).strip()
        number = int(video.get("id", 0))
        # Preserve line breaks in title by replacing them with a visible separator
        formatted_title = title.replace('\n', ' | ')
        button_text = formatted_title
        button = InlineKeyboardButton(button_text, callback_data=f"send_video_{number}")
        
        # Add buttons in pairs (2 per row)
        button_index = i - start_index
        if button_index % 2 == 0:
            buttons.append([button])
        else:
            buttons[-1].append(button)
    
    # Handle odd number of videos in this batch
    if (end_index - start_index) % 2 == 1:
        # Last row has only one button, that's fine
        pass
    
    # Add navigation buttons if needed
    nav_buttons = []
    if offset > 0:
        # Add "Orqaga 🎥" button
        prev_offset = max(0, offset - 10)
        nav_buttons.append(
            InlineKeyboardButton("Orqaga 🎥", callback_data=f"more_videos_{prev_offset}")
        )
    
    if len(videos) > end_index:
        # Add "Davomi 🎥" button
        next_offset = end_index
        nav_buttons.append(
            InlineKeyboardButton("Davomi 🎥", callback_data=f"more_videos_{next_offset}")
        )
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    reply_markup = InlineKeyboardMarkup(buttons)
    await query.edit_message_text(
        text=f"Topilgan videolar ({start_index+1}-{end_index} {len(videos)} dan):",
        reply_markup=reply_markup
    )


async def on_send_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("send_video_"):
        return
    
    raw_video_id = query.data.replace("send_video_", "", 1)
    if not raw_video_id.isdigit():
        return
    
    number = int(raw_video_id)
    video = get_video_by_number(number)
    if not video:
        await query.answer("Video topilmadi", show_alert=False)
        return
    
    file_id = video["file_id"]
    title = str(video.get("title", "")).strip()
    comment = str(video.get("comment", "")).strip()
    duration = int(video.get("duration", 0))
    duration_str = format_duration(duration)
    
    # Format caption with film name, ID, duration, and comment
    if comment:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {number}\n🧭film avqti: {duration_str}\n✉️commant:{comment}\n\n@Hidop_bot tomonidan yuklab olindi"
    else:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {number}\n🧭film avqti: {duration_str}\n\n@Hidop_bot tomonidan yuklab olindi"
    
    save_markup = create_video_buttons_with_users(number, context, query.from_user.id if query.from_user else None)
    
    await context.bot.send_video(
        chat_id=query.from_user.id,
        video=file_id,
        caption=caption,
        reply_markup=save_markup,
    )


async def on_share_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("share_video_"):
        return
    await query.answer()
    raw_number = query.data.replace("share_video_", "", 1)
    if not raw_number.isdigit():
        return
    number = int(raw_number)
    if not get_video_by_number(number):
        await query.answer("Video topilmadi", show_alert=False)
        return
    
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=f"📤 Videoni ulashish uchun ushbu habarni yuboring:\n\n@Hidop_bot botga kirib start bosing va shuni yuboring:\n\n`{number}`",
        parse_mode="Markdown",
    )


async def handle_save_video_name(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not message.text or not update.effective_user:
        return False
    state = context.user_data.get(SAVE_VIDEO_STATE_KEY)
    if not isinstance(state, dict):
        return False
    raw_name = normalize_query(message.text)
    if not raw_name:
        await message.reply_text(t(context, "video_save_prompt"))
        return True
    video_id = state.get("video_id")
    if not isinstance(video_id, int):
        context.user_data.pop(SAVE_VIDEO_STATE_KEY, None)
        return False
    saved_items, saved_id = add_saved_video(update.effective_user.id, video_id, raw_name)
    context.user_data.pop(SAVE_VIDEO_STATE_KEY, None)
    if saved_id is None:
        await message.reply_text("video allaqachon saqlangan ❤️")
        return True
    
    # Send folder message first
    folder_msg = await message.reply_text("📁")
    
    # Wait 2 seconds and delete folder message
    await asyncio.sleep(2)
    await folder_msg.delete()
    
    # Send success message
    await message.reply_text("video saqlandi ✅")
    
    # Send warehouse button message
    await message.reply_text(
        "omborni ko'rish uchun bosing ⬇️",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("ombor 📦", callback_data="view_warehouse")]
        ])
    )
    return True


async def handle_saved_videos_request(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not update.effective_user:
        return False

    user_id = update.effective_user.id
    reply_markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Pleylist 📁",
                    url="https://hidop-bot-app.onrender.com",
                )
            ]
        ]
    )
    await message.reply_text(
        f"saqlangan videolar Pleylist 📁 da saqlanayapti\n"
        f"kirish uchun IDi `{user_id}`\n"
        f"IDi ni saytga kiriting ✅",
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )
    return True


async def on_warehouse_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or query.data != "view_warehouse":
        return
    
    await query.answer()
    
    # Get user's saved videos
    items = SAVED_VIDEOS.get(query.from_user.id, [])
    if not isinstance(items, list) or not items:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="Saqlangan videolar yo'q."
        )
        return
    
    # Create buttons for each saved video
    buttons = []
    for index, item in enumerate(items, 1):
        name = str(item.get("name", ""))
        saved_id = item.get("saved_id")
        if isinstance(saved_id, int):
            # Use only video name as button text
            button_text = name if name else f"Video {index}"
            buttons.append([
                InlineKeyboardButton(button_text, callback_data=f"send_saved_video_{saved_id}"),
                InlineKeyboardButton("🗑️", callback_data=f"delete_saved_video_{saved_id}")
            ])
    
    if buttons:
        reply_markup = InlineKeyboardMarkup(buttons)
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="Saqlangan videolar:",
            reply_markup=reply_markup
        )


async def on_send_catalog_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("send_catalog_video_"):
        return
    
    await query.answer()
    raw_video_id = query.data.replace("send_catalog_video_", "", 1)
    
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    video = get_video_by_number(video_id)
    if not video:
        await query.answer("Video topilmadi", show_alert=False)
        return
    
    save_markup = create_video_buttons_with_users(video_id, context)
    
    title = str(video.get('title', '')).strip()
    comment = str(video.get('comment', '')).strip()
    duration = int(video.get('duration', 0))
    duration_str = format_duration(duration)
    
    # Format caption with film name, ID, duration, and comment
    if comment:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {video_id}\n🧭film avqti: {duration_str}\n✉️commant:{comment}\n\n@Hidop_bot tomonidan yuklab olindi"
    else:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {video_id}\n🧭film avqti: {duration_str}\n\n@Hidop_bot tomonidan yuklab olindi"
    
    await context.bot.send_video(
        chat_id=query.from_user.id,
        video=video["file_id"],
        caption=caption,
        reply_markup=save_markup
    )


def cleanup_orphaned_saved_videos():
    """Remove videos from users' saved videos that no longer exist in catalog"""
    catalog_video_ids = set()
    catalog_items = VIDEO_CATALOG.get("items", [])
    if isinstance(catalog_items, list):
        for item in catalog_items:
            if isinstance(item, dict):
                video_id = item.get("id")
                if isinstance(video_id, int):
                    catalog_video_ids.add(video_id)
    
    users_updated = 0
    videos_removed = 0
    for user_id in list(SAVED_VIDEOS.keys()):
        user_videos = SAVED_VIDEOS.get(user_id, [])
        if isinstance(user_videos, list):
            updated_user_videos = []
            for saved_video in user_videos:
                if isinstance(saved_video, dict):
                    saved_id = saved_video.get("saved_id")
                    if isinstance(saved_id, int) and saved_id not in catalog_video_ids:
                        videos_removed += 1
                        continue  # Skip this video
                updated_user_videos.append(saved_video)
            if len(updated_user_videos) != len(user_videos):
                SAVED_VIDEOS[user_id] = updated_user_videos
                users_updated += 1
    
    if users_updated > 0:
        save_users()
    return users_updated, videos_removed


async def on_confirm_delete_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle delete confirmation for admin videos"""
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("confirm_delete_video_"):
        return
    
    await query.answer()
    raw_video_id = query.data.replace("confirm_delete_video_", "", 1)
    
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    
    # Send confirmation message
    confirmation_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Ha", callback_data=f"delete_catalog_video_{video_id}"),
            InlineKeyboardButton("Yo'q", callback_data=f"cancel_delete_video_{video_id}")
        ]
    ])
    
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=f"shu videoni o'chirasizmi 🗑️",
        reply_markup=confirmation_markup
    )


async def on_cancel_delete_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle cancel delete for admin videos"""
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("cancel_delete_video_"):
        return
    
    await query.answer()
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text="Video o'chirish bekor qilindi."
    )


async def on_delete_catalog_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("delete_catalog_video_"):
        return
    
    await query.answer()
    raw_video_id = query.data.replace("delete_catalog_video_", "", 1)
    
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    
    # Find and remove video from catalog
    catalog_items = VIDEO_CATALOG.get("items", [])
    if not isinstance(catalog_items, list):
        return
    
    removed_video = None
    updated_items = []
    for item in catalog_items:
        if isinstance(item, dict) and item.get("id") == video_id:
            removed_video = item
        else:
            updated_items.append(item)
    
    if removed_video:
        # Update catalog
        VIDEO_CATALOG["items"] = updated_items
        save_video_catalog()
        
        # Remove from all users' saved videos
        users_updated = 0
        videos_removed = 0
        for user_id in list(SAVED_VIDEOS.keys()):
            user_videos = SAVED_VIDEOS.get(user_id, [])
            if isinstance(user_videos, list):
                updated_user_videos = []
                for saved_video in user_videos:
                    if isinstance(saved_video, dict) and saved_video.get("saved_id") == video_id:
                        videos_removed += 1
                        continue  # Skip this video
                    updated_user_videos.append(saved_video)
                if len(updated_user_videos) != len(user_videos):
                    SAVED_VIDEOS[user_id] = updated_user_videos
                    users_updated += 1
        
        save_users()
        
        # Send confirmation message
        video_title = str(removed_video.get("title", ""))
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=f"{video_title} katalogdan olib tashlandi 🗑️."
        )
    else:
        await query.answer("Video topilmadi", show_alert=False)


async def on_delete_saved_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("delete_saved_video_"):
        return
    
    await query.answer()
    raw_video_id = query.data.replace("delete_saved_video_", "", 1)
    
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    user_id = query.from_user.id
    
    # Get user's saved videos
    saved_videos = SAVED_VIDEOS.get(user_id, [])
    if not isinstance(saved_videos, list):
        return
    
    # Find and remove the video
    removed_video = None
    updated_videos = []
    for video in saved_videos:
        if isinstance(video, dict) and video.get("saved_id") == video_id:
            removed_video = video
        else:
            updated_videos.append(video)
    
    if removed_video:
        # Update saved videos
        SAVED_VIDEOS[user_id] = updated_videos
        save_users()
        
        # Send confirmation message
        video_name = str(removed_video.get("name", ""))
        await context.bot.send_message(
            chat_id=user_id,
            text=f"{video_name} saqlanganlardan olib tashlandi 🗑️."
        )
    else:
        await query.answer("Video topilmadi", show_alert=False)


async def on_save_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("save_video_"):
        return
    
    await query.answer()
    raw_video_id = query.data.replace("save_video_", "", 1)
    
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    video = get_video_by_number(video_id)
    
    if not video:
        await query.answer("Video topilmadi", show_alert=False)
        return
    
    # Get video title and use it as the save name
    video_title = str(video.get("title", "")).strip()
    if not video_title:
        video_title = f"Video {video_id}"
    
    # Save the video with its original title
    saved_items, saved_id = add_saved_video(query.from_user.id, video_id, video_title)
    
    if saved_id is None:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="Video allaqachon saqlangan ❤️"
        )
    else:
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=t(context, "video_saved", number=saved_id)
        )


async def on_like_video_inline_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("like_video_inline_"):
        return
    
    await query.answer("👍🏻")


async def on_dislike_video_inline_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("dislike_video_inline_"):
        return
    
    await query.answer("👎🏻")


async def on_save_video_inline_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("save_video_inline_"):
        return
    
    await query.answer("saqlandi 📥")


async def on_show_reactions_inline_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle showing reaction history for inline video"""
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("show_reactions_inline_"):
        return
    
    # For inline videos, we don't have a specific video_id, so show general message
    await query.answer("Bu video uchun reaksiya ma'lumotlari mavjud emas.", show_alert=False)


async def on_like_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("like_video_"):
        return
    
    raw_video_id = query.data.replace("like_video_", "", 1)
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    user_id = query.from_user.id
    
    # Check if user already reacted to this video
    existing_reaction = get_user_reaction(user_id, video_id)
    
    if existing_reaction == "likes":
        # User already liked this video
        await query.answer("Siz allaqachon yoqtirgan ekansiz!")
        return
    elif existing_reaction == "dislikes":
        # User is changing from dislike to like
        await query.answer("Reaksiyangiz o'zgartirildi!")
    else:
        # First time reacting
        await query.answer("Yoqtirildi!")
    
    # Save reaction to user reactions file
    add_video_reaction(video_id, user_id, "likes")
    
    likes = get_video_reaction_count(video_id, "likes")
    dislikes = get_video_reaction_count(video_id, "dislikes")
    
    # Get user info for display
    user_name = query.from_user.first_name or query.from_user.username or "Foydalanuvchi"
    
    # Update the message with new counts
    try:
        await query.edit_message_reply_markup(
            reply_markup=create_video_buttons_with_users(video_id, context)
        )
    except Exception:
        pass  # Message might be too old to edit


async def on_dislike_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("dislike_video_"):
        return
    
    raw_video_id = query.data.replace("dislike_video_", "", 1)
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    user_id = query.from_user.id
    
    # Check if user already reacted to this video
    existing_reaction = get_user_reaction(user_id, video_id)
    
    if existing_reaction == "dislikes":
        # User already disliked this video
        await query.answer("Siz allaqachon yoqmagan ekansiz!")
        return
    elif existing_reaction == "likes":
        # User is changing from like to dislike
        await query.answer("Reaksiyangiz o'zgartirildi!")
    else:
        # First time reacting
        await query.answer("Yoqmadi!")
    
    # Save reaction to user reactions file
    add_video_reaction(video_id, user_id, "dislikes")
    
    likes = get_video_reaction_count(video_id, "likes")
    dislikes = get_video_reaction_count(video_id, "dislikes")
    
    # Get user info for display
    user_name = query.from_user.first_name or query.from_user.username or "Foydalanuvchi"
    
    # Update the message with new counts
    try:
        await query.edit_message_reply_markup(
            reply_markup=create_video_buttons_with_users(video_id, context)
        )
    except Exception:
        pass  # Message might be too old to edit


async def on_send_saved_video_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.data.startswith("send_saved_video_"):
        return
    
    await query.answer()
    raw_video_id = query.data.replace("send_saved_video_", "", 1)
    
    if not raw_video_id.isdigit():
        return
    
    video_id = int(raw_video_id)
    video = get_video_by_number(video_id)
    
    if not video:
        await query.answer("Video topilmadi", show_alert=False)
        return
    
    save_markup = create_video_buttons_with_users(video_id, context)
    
    title = str(video.get('title', '')).strip()
    comment = str(video.get('comment', '')).strip()
    duration = int(video.get('duration', 0))
    duration_str = format_duration(duration)
    
    # Format caption with film name, ID, duration, and comment
    if comment:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {video_id}\n🧭film avqti: {duration_str}\n✉️commant:{comment}\n\n@Hidop_bot tomonidan yuklab olindi"
    else:
        caption = f"🎥filim nomi: {title}\n🗝️film kodi : {video_id}\n🧭film avqti: {duration_str}\n\n@Hidop_bot tomonidan yuklab olindi"
    
    # Send the video to user
    try:
        await context.bot.send_video(
            chat_id=query.from_user.id,
            video=video["file_id"],
            caption=caption,
            reply_markup=save_markup
        )
    except Exception as e:
        await query.answer("Video yuborishda xatolik", show_alert=False)


async def on_saved_videos_clear_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "saved_videos_clear":
        return
    await query.answer()
    context.user_data[DELETE_SAVED_VIDEOS_STATE_KEY] = True
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=t(context, "saved_videos_delete_prompt"),
    )


async def on_catalog_videos_clear_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "catalog_videos_clear":
        return
    if not query.from_user or query.from_user.id != VIDEO_ADMIN_ID:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return
    await query.answer()
    context.user_data[DELETE_CATALOG_VIDEOS_STATE_KEY] = True
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=t(context, "catalog_delete_prompt"),
    )


async def on_uploader_add_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "uploader_add":
        return
    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return
    await query.answer()
    if query.message:
        await send_uploader_selection_list(
            context=context,
            chat_id=query.message.chat_id,
            action="add",
        )


async def on_uploader_remove_click(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or query.data != "uploader_remove":
        return
    admin_id = get_admin_id()
    if admin_id is None or not query.from_user or query.from_user.id != admin_id:
        await query.answer("Bu tugma faqat admin uchun.", show_alert=False)
        return
    await query.answer()
    if query.message:
        await send_uploader_selection_list(
            context=context,
            chat_id=query.message.chat_id,
            action="remove",
        )


async def handle_delete_saved_videos(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not message.text or not update.effective_user:
        return False
    if not context.user_data.get(DELETE_SAVED_VIDEOS_STATE_KEY):
        return False
    raw_input = normalize_query(message.text)
    raw_parts = [part.strip() for part in raw_input.split(",") if part.strip()]
    if not raw_parts:
        await message.reply_text(t(context, "saved_videos_delete_invalid"))
        return True
    indexes: list[int] = []
    for part in raw_parts:
        if not part.isdigit():
            await message.reply_text(t(context, "saved_videos_delete_invalid"))
            return True
        idx = int(part)
        if idx <= 0:
            await message.reply_text(t(context, "saved_videos_delete_invalid"))
            return True
        if idx not in indexes:
            indexes.append(idx)

    items = SAVED_VIDEOS.get(update.effective_user.id, [])
    if not isinstance(items, list) or not items:
        context.user_data.pop(DELETE_SAVED_VIDEOS_STATE_KEY, None)
        await message.reply_text(t(context, "video_list_empty"))
        return True

    existing_ids = {item.get("saved_id") for item in items if isinstance(item, dict)}
    for idx in indexes:
        if idx not in existing_ids:
            await message.reply_text(t(context, "saved_videos_delete_not_found"))
            return True

    remove_set = set(indexes)
    keep = [
        item
        for item in items
        if not (isinstance(item, dict) and item.get("saved_id") in remove_set)
    ]
    SAVED_VIDEOS[update.effective_user.id] = keep
    save_saved_videos()
    context.user_data.pop(DELETE_SAVED_VIDEOS_STATE_KEY, None)
    await message.reply_text(t(context, "saved_videos_deleted"))
    return True


async def handle_delete_catalog_videos(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not message.text or not update.effective_user:
        return False
    if update.effective_user.id != VIDEO_ADMIN_ID:
        return False
    if not context.user_data.get(DELETE_CATALOG_VIDEOS_STATE_KEY):
        return False
    raw_input = normalize_query(message.text)
    raw_parts = [part.strip() for part in raw_input.split(",") if part.strip()]
    if not raw_parts:
        await message.reply_text(t(context, "saved_videos_delete_invalid"))
        return True
    ids: list[int] = []
    for part in raw_parts:
        if not part.isdigit():
            await message.reply_text(t(context, "saved_videos_delete_invalid"))
            return True
        idx = int(part)
        if idx <= 0:
            await message.reply_text(t(context, "saved_videos_delete_invalid"))
            return True
        if idx not in ids:
            ids.append(idx)

    items = VIDEO_CATALOG.get("items", [])
    if not isinstance(items, list) or not items:
        context.user_data.pop(DELETE_CATALOG_VIDEOS_STATE_KEY, None)
        await message.reply_text(t(context, "video_list_empty"))
        return True

    existing_ids = {item.get("id") for item in items if isinstance(item, dict)}
    for idx in ids:
        if idx not in existing_ids:
            await message.reply_text(t(context, "saved_videos_delete_not_found"))
            return True

    remove_set = set(ids)
    keep = [item for item in items if not (isinstance(item, dict) and item.get("id") in remove_set)]
    VIDEO_CATALOG["items"] = keep
    save_video_catalog()
    
    # Remove deleted videos from all users' saved videos
    removed_count = 0
    for user_id, user_videos in SAVED_VIDEOS.items():
        if isinstance(user_videos, list):
            # Keep only videos that are not in the removed set
            original_count = len(user_videos)
            filtered_videos = [video for video in user_videos if isinstance(video, dict) and video.get("saved_id") not in remove_set]
            SAVED_VIDEOS[user_id] = filtered_videos
            removed_count += original_count - len(filtered_videos)
    
    # Save users data after removal
    save_users()
    
    context.user_data.pop(DELETE_CATALOG_VIDEOS_STATE_KEY, None)
    await message.reply_text(t(context, "catalog_deleted"))
    return True


async def handle_upload_video_name(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    message = update.message
    if not message or not message.text or not update.effective_user:
        return False
    if not is_video_uploader(update.effective_user.id):
        return False
    state = context.user_data.get(UPLOAD_VIDEO_STATE_KEY)
    if not isinstance(state, dict):
        return False
    stage = state.get("stage")
    file_id = str(state.get("file_id", "")).strip()
    if not file_id:
        context.user_data.pop(UPLOAD_VIDEO_STATE_KEY, None)
        return False

    if stage == "await_title":
        title = normalize_query(message.text)
        if not title:
            await message.reply_text(t(context, "upload_video_name_prompt"))
            return True
        state["title"] = title
        state["stage"] = "await_id_choice"
        await message.reply_text(
            t(context, "upload_video_id_choice_prompt"),
            reply_markup=build_upload_video_id_choice_markup(),
        )
        return True

    if stage == "await_comment_choice":
        await message.reply_text("Iltimos, Ha/Yo'q tugmalaridan foydalaning.")
        return True

    if stage == "await_comment":
        comment = normalize_query(message.text)
        state["comment"] = comment
        state["stage"] = "ready_to_save"
        await message.reply_text(t(context, "upload_video_comment_added"))
        # Auto-save the video with comment
        title = str(state.get("title", "")).strip()
        file_id = str(state.get("file_id", "")).strip()
        comment = str(state.get("comment", "")).strip()
        duration = int(state.get("duration", 0))
        custom_id = state.get("custom_id")  # May be None for auto-generated ID
        try:
            if custom_id:
                number = add_video_to_catalog(
                    file_id=file_id,
                    title=title,
                    added_by=update.effective_user.id,
                    custom_id=custom_id,
                    comment=comment,
                    duration=duration
                )
            else:
                number = add_video_to_catalog(
                    file_id=file_id,
                    title=title,
                    added_by=update.effective_user.id,
                    comment=comment,
                    duration=duration
                )
            context.user_data.pop(UPLOAD_VIDEO_STATE_KEY, None)
            await message.reply_text(f"Video muvaffaqiyatli saqlandi! 📥\nVideo ID: {number}\nNomi: {title}\nComment: {comment}")
        except ValueError:
            await message.reply_text(t(context, "upload_video_id_exists"))
        return True

    if stage == "await_custom_id":
        raw_id = normalize_query(message.text)
        if not raw_id.isdigit():
            await message.reply_text(t(context, "upload_video_id_invalid"))
            return True
        custom_id = int(raw_id)
        if custom_id <= 0:
            await message.reply_text(t(context, "upload_video_id_invalid"))
            return True
        if video_catalog_id_exists(custom_id):
            await message.reply_text(t(context, "upload_video_id_exists"))
            return True
        title = str(state.get("title", "")).strip()
        if not title:
            state["stage"] = "await_title"
            await message.reply_text(t(context, "upload_video_name_prompt"))
            return True
        # Save the custom ID and ask for comment confirmation
        state["custom_id"] = custom_id
        state["stage"] = "await_comment_choice"
        await message.reply_text(t(context, "upload_video_comment_confirm_prompt"), reply_markup=build_upload_video_comment_choice_markup())
        return True

    if stage == "await_id_choice":
        await message.reply_text(t(context, "upload_video_id_choice_prompt"))
        return True

    return False


async def handle_idi_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.message
    if not message or not message.text or not update.effective_user:
        return False
    
    # Faqat adminlar va video yuklovchilar uchun
    if not is_video_uploader(update.effective_user.id):
        return False
    
    query_text = normalize_query(message.text)
    if query_text != "idi":
        return False
    
    # Video katalogidan barcha videolarni olish
    items = VIDEO_CATALOG.get("items", [])
    if not isinstance(items, list) or len(items) == 0:
        await message.reply_text("Hozircha video yo'q.")
        return True
    
    # Videolarni ID bo'yicha saralash
    sorted_items = sorted(items, key=lambda x: x.get("id", 0))
    
    # Xabar matnini yaratish
    text = "📹 **Video katalogi**\n\n"
    
    for item in sorted_items:
        video_id = item.get("id", "Noma'lum")
        title = item.get("title", "Noma'lum")
        comment = item.get("comment", "")
        duration = int(item.get("duration", 0))
        duration_str = format_duration(duration) if duration > 0 else ""
        
        line = f"🎬 ID: {video_id} - {title}"
        if duration_str:
            line += f" ⏱️{duration_str}"
        if comment:
            line += f"\n💬 {comment}"
        line += "\n"
        
        text += line
    
    # Agar xabar juda uzun bo'lsa, bo'laklarga bo'lish
    if len(text) > 4000:
        parts = []
        current_part = "📹 **Video katalogi**\n\n"
        for item in sorted_items:
            video_id = item.get("id", "Noma'lum")
            title = item.get("title", "Noma'lum")
            comment = item.get("comment", "")
            duration = int(item.get("duration", 0))
            duration_str = format_duration(duration) if duration > 0 else ""
            
            line = f"🎬 ID: {video_id} - {title}"
            if duration_str:
                line += f" ⏱️{duration_str}"
            if comment:
                line += f"\n💬 {comment}"
            line += "\n"
            if len(current_part) + len(line) > 4000:
                parts.append(current_part)
                current_part = "📹 **Video katalogi (davomi)**\n\n"
            current_part += line
        parts.append(current_part)
        
        for part in parts:
            await message.reply_text(part, parse_mode="Markdown")
    else:
        await message.reply_text(text, parse_mode="Markdown")
    
    return True


async def handle_user_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    if await handle_admin_general_broadcast_content(update, context):
        return
    if await handle_admin_today_joined_report(update, context):
        return
    if await handle_admin_all_users_report(update, context):
        return
    if await handle_admin_sms_selection(update, context):
        return
    if await handle_admin_uploader_selection(update, context):
        return
    if await handle_admin_chat_panel_shortcut(update, context):
        return
    if await handle_admin_video_uploaders_report(update, context):
        return
    if await handle_upload_video_name(update, context):
        return
    if await relay_active_chat_message(update, context):
        return
    if await handle_delete_saved_videos(update, context):
        return
    if await handle_delete_catalog_videos(update, context):
        return
    if await handle_save_video_name(update, context):
        return
    if await handle_video_number_request(update, context):
        return
    if await handle_video_name_request(update, context):
        return
    if await handle_idi_command(update, context):
        return
    query_text = normalize_query(update.message.text)
    if not query_text:
        return

    if looks_like_url(query_text):
        if LOADING_STICKER_FILE_ID:
            status_msg = await update.message.reply_sticker(LOADING_STICKER_FILE_ID)
        else:
            status_msg = await update.message.reply_text("⏳")
        try:
            with TemporaryDirectory() as temp_dir:
                file_path, title = await to_thread(download_video_sync, query_text, temp_dir)
                if not os.path.exists(file_path):
                    await update.message.reply_text(t(context, "video_file_not_found"))
                    await status_msg.delete()
                    return

                file_size = os.path.getsize(file_path)
                if file_size > MAX_VIDEO_BYTES:
                    await update.message.reply_text(t(context, "video_too_large"))
                    await status_msg.delete()
                    return

                video_actions = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                t(context, "action_send"),
                                switch_inline_query="send_last_video",
                            )
                        ],
                    ]
                )
                with open(file_path, "rb") as video_file:
                    sent_video_msg = await update.message.reply_video(
                        video=video_file,
                        caption=t(context, "video_caption"),
                        reply_markup=video_actions,
                        supports_streaming=True,
                    )
                if sent_video_msg.video:
                    if update.effective_user:
                        LAST_VIDEO_FILE_ID_BY_USER[
                            update.effective_user.id
                        ] = sent_video_msg.video.file_id
                await status_msg.delete()
        except Exception as exc:
            logger.exception("Video yuklashda xato: %s", exc)
            try:
                await status_msg.delete()
            except Exception:
                pass
            await update.message.reply_text(
                "Video yuklab bo'lmadi. Iltimos qayta urinib ko'ring."
            )
        return

    await update.message.reply_text(t(context, "no_results"))


async def on_startup(app: Application) -> None:
    load_users()
    load_video_catalog()
    load_saved_videos()
    load_video_reactions()
    load_monthly_reactions()
    load_video_uploaders()
    # Build search index for maximum performance
    build_search_index()
    
    # Clean up orphaned videos from users' saved videos
    users_updated, videos_removed = cleanup_orphaned_saved_videos()
    if users_updated > 0:
        print(f"🧹 {videos_removed} ta eski video {users_updated} ta foydalanuvchidan o'chirildi")
    
    me = await app.bot.get_me()
    logger.info("Bot ishga tushdi: @%s", me.username)
    print(f"✅ Bot ishga tushdi: @{me.username}")


async def on_shutdown(app: Application) -> None:
    return


def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN topilmadi. .env fayliga token qo'shing.")

    app = (
        Application.builder()
        .token(token)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .build()
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("malumot", malumot_command))
    app.add_handler(CommandHandler("videos", videos_command))
    app.add_handler(CommandHandler("ombor", handle_saved_videos_request))
    app.add_handler(CallbackQueryHandler(on_thanks_click, pattern=r"^thanks_"))
    app.add_handler(CallbackQueryHandler(on_start_using_bot_click, pattern=r"^start_using_bot$"))
    app.add_handler(CallbackQueryHandler(on_share_broadcast_click, pattern=r"^share_broadcast$"))
    app.add_handler(CallbackQueryHandler(on_sms_broadcast_click, pattern=r"^sms_broadcast$"))
    app.add_handler(CallbackQueryHandler(on_upload_video_id_choice, pattern=r"^upload_video_id_(yes|no)$"))
    app.add_handler(CallbackQueryHandler(on_upload_video_comment_choice, pattern=r"^upload_video_comment_(yes|no)$"))
    app.add_handler(CallbackQueryHandler(on_general_broadcast_click, pattern=r"^general_broadcast$"))
    app.add_handler(CallbackQueryHandler(on_general_broadcast_confirm, pattern=r"^general_broadcast_(yes|no)$"))
    app.add_handler(CallbackQueryHandler(on_close_active_chat_click, pattern=r"^close_active_chat$"))
    app.add_handler(CallbackQueryHandler(on_add_active_chat_click, pattern=r"^add_active_chat$"))
    app.add_handler(CallbackQueryHandler(on_remove_active_chat_click, pattern=r"^remove_active_chat$"))
    app.add_handler(CallbackQueryHandler(on_user_chat_request_click, pattern=r"^request_chat_link$"))
    app.add_handler(CallbackQueryHandler(on_admin_connect_request_click, pattern=r"^connect_request_"))
    app.add_handler(CallbackQueryHandler(on_saved_videos_clear_click, pattern=r"^saved_videos_clear$"))
    app.add_handler(CallbackQueryHandler(on_catalog_videos_clear_click, pattern=r"^catalog_videos_clear$"))
    app.add_handler(CallbackQueryHandler(on_uploader_add_click, pattern=r"^uploader_add$"))
    app.add_handler(CallbackQueryHandler(on_uploader_remove_click, pattern=r"^uploader_remove$"))
    app.add_handler(CallbackQueryHandler(on_save_video_click, pattern=r"^save_video_"))
    app.add_handler(CallbackQueryHandler(on_save_video_inline_click, pattern=r"^save_video_inline_"))
    app.add_handler(CallbackQueryHandler(on_like_video_click, pattern=r"^like_video_"))
    app.add_handler(CallbackQueryHandler(on_like_video_inline_click, pattern=r"^like_video_inline_"))
    app.add_handler(CallbackQueryHandler(on_show_reactions_click, pattern=r"^show_reactions_"))
    app.add_handler(CallbackQueryHandler(on_show_reactions_inline_click, pattern=r"^show_reactions_inline_"))
    app.add_handler(CallbackQueryHandler(on_dislike_video_click, pattern=r"^dislike_video_"))
    app.add_handler(CallbackQueryHandler(on_dislike_video_inline_click, pattern=r"^dislike_video_inline_"))
    app.add_handler(CallbackQueryHandler(on_send_video_click, pattern=r"^send_video_"))
    app.add_handler(CallbackQueryHandler(on_send_saved_video_click, pattern=r"^send_saved_video_"))
    app.add_handler(CallbackQueryHandler(on_delete_saved_video_click, pattern=r"^delete_saved_video_"))
    app.add_handler(CallbackQueryHandler(on_send_catalog_video_click, pattern=r"^send_catalog_video_"))
    app.add_handler(CallbackQueryHandler(on_confirm_delete_video_click, pattern=r"^confirm_delete_video_"))
    app.add_handler(CallbackQueryHandler(on_cancel_delete_video_click, pattern=r"^cancel_delete_video_"))
    app.add_handler(CallbackQueryHandler(on_delete_catalog_video_click, pattern=r"^delete_catalog_video_"))
    app.add_handler(CallbackQueryHandler(on_warehouse_button_click, pattern=r"^view_warehouse$"))
    app.add_handler(CallbackQueryHandler(on_no_action_click, pattern=r"^no_action$"))
    app.add_handler(CallbackQueryHandler(on_more_videos_click, pattern=r"^more_videos_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_text))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, handle_user_non_text))

    logger.info("🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️🔥✈️⏳🎥🧭⏰✉️🗝️✅🖊️❤️")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        timeout=30
    )


if __name__ == "__main__":
    main()
