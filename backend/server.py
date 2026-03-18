import json
import logging
import os
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from shared_db import load_saved_videos as db_load_saved_videos
from shared_db import load_video_catalog as db_load_video_catalog

WEBAPP_DIR = ROOT_DIR / "webapp"
HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
PORT = int(os.getenv("BACKEND_PORT", "8080"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def format_duration(seconds: int) -> str:
    minutes = max(0, seconds) // 60
    remaining_seconds = max(0, seconds) % 60
    return f"{minutes}:{remaining_seconds:02d}"


def build_video_caption(video_id: int, item: dict) -> str:
    title = str(item.get("title", "")).strip()
    comment = str(item.get("comment", "")).strip()
    duration = int(item.get("duration", 0))
    duration_str = format_duration(duration)
    if comment:
        return (
            f"🎥filim nomi: {title}\n"
            f"🗝️film kodi : {video_id}\n"
            f"🧭film avqti: {duration_str}\n"
            f"✉️commant:{comment}\n\n"
            "@Hidop_bot tomonidan yuklab olindi"
        )
    return (
        f"🎥filim nomi: {title}\n"
        f"🗝️film kodi : {video_id}\n"
        f"🧭film avqti: {duration_str}\n\n"
        "@Hidop_bot tomonidan yuklab olindi"
    )


def detect_category(item: dict) -> str:
    haystack = f"{item.get('title', '')} {item.get('comment', '')}".lower()
    if any(keyword in haystack for keyword in ("tiktok", "instagram", "youtube", "youtu", "ombor")):
        return "Ombor"
    return "HOME"


def normalize_catalog_item(item: dict) -> dict:
    video_id = int(item.get("id", 0))
    return {
        "id": video_id,
        "title": str(item.get("title", "Sarlavha topilmadi")),
        "comment": str(item.get("comment", "")),
        "category": detect_category(item),
        "duration": int(item.get("duration", 0)),
        "ageLabel": "Kutubxonada",
        "preview_url": f"/api/video-file?video_id={video_id}",
    }


def get_catalog_items() -> list[dict]:
    payload = db_load_video_catalog()
    items = payload.get("items", []) if isinstance(payload, dict) else []
    return [normalize_catalog_item(item) for item in items if isinstance(item, dict)]


def get_saved_items(owner_id: str) -> list[dict]:
    saved_payload = db_load_saved_videos()
    raw_saved_items = saved_payload.get(int(owner_id), []) if owner_id.isdigit() else []
    catalog_by_id = {item["id"]: item for item in get_catalog_items()}
    results = []
    for saved_item in raw_saved_items:
        if not isinstance(saved_item, dict):
            continue
        video_id = int(saved_item.get("video_id", saved_item.get("saved_id", 0)) or 0)
        catalog_item = catalog_by_id.get(video_id)
        if not catalog_item:
            continue
        results.append(
            {
                **catalog_item,
                "saved_name": str(saved_item.get("name", catalog_item["title"])),
                "category": "Ombor",
                "ageLabel": "Saqlangan",
            }
        )
    return results


def delete_saved_video(owner_id: str, video_id: int) -> tuple[bool, str]:
    saved_payload = db_load_saved_videos()
    if not owner_id.isdigit():
        return False, "Owner ID noto'g'ri."
    owner_key = int(owner_id)
    user_items = saved_payload.get(owner_key, [])
    if not isinstance(user_items, list):
        return False, "User saqlangan videolari topilmadi."

    original_count = len(user_items)
    filtered_items = [
        item
        for item in user_items
        if not (
            isinstance(item, dict)
            and int(item.get("video_id", item.get("saved_id", 0)) or 0) == video_id
        )
    ]

    if len(filtered_items) == original_count:
        return False, "Video omborda topilmadi."

    saved_payload[owner_key] = filtered_items
    from shared_db import save_saved_videos as db_save_saved_videos
    db_save_saved_videos(saved_payload)
    return True, "Video ombordan olib tashlandi."


def send_video_via_bot(target_user_id: int, video_id: int) -> tuple[bool, str]:
    if not BOT_API_URL:
        return False, "BOT_TOKEN topilmadi."

    payload = db_load_video_catalog()
    items = payload.get("items", []) if isinstance(payload, dict) else []
    target_item = next(
        (item for item in items if isinstance(item, dict) and int(item.get("id", 0)) == video_id),
        None,
    )
    if not target_item:
        return False, "Video topilmadi."

    file_id = str(target_item.get("file_id", "")).strip()
    if not file_id:
        return False, "Video file_id topilmadi."

    body = {
        "chat_id": target_user_id,
        "video": file_id,
        "caption": build_video_caption(video_id, target_item),
        "supports_streaming": True,
    }
    request = Request(
        f"{BOT_API_URL}/sendVideo",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as response:
            raw = response.read().decode("utf-8")
        payload = json.loads(raw)
        if payload.get("ok"):
            return True, "Video yuborildi."
        return False, str(payload.get("description", "Telegram API xatoligi"))
    except Exception as exc:
        logger.warning("Telegram API sendVideo xatosi: %s", exc)
        return False, str(exc)


def get_video_file_path(video_id: int) -> str | None:
    if not BOT_API_URL:
        return None

    payload = db_load_video_catalog()
    items = payload.get("items", []) if isinstance(payload, dict) else []
    target_item = next(
        (item for item in items if isinstance(item, dict) and int(item.get("id", 0)) == video_id),
        None,
    )
    if not target_item:
        return None

    file_id = str(target_item.get("file_id", "")).strip()
    if not file_id:
        return None

    request = Request(
        f"{BOT_API_URL}/getFile",
        data=json.dumps({"file_id": file_id}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as response:
            raw = response.read().decode("utf-8")
        payload = json.loads(raw)
        if not payload.get("ok"):
            return None
        return str(payload.get("result", {}).get("file_path", "")).strip() or None
    except Exception as exc:
        logger.warning("Telegram API getFile xatosi (%s): %s", video_id, exc)
        return None


class AppHandler(BaseHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/catalog":
            return self._send_json({"items": get_catalog_items()})

        if parsed.path == "/api/saved-videos":
            params = parse_qs(parsed.query)
            owner_id = str(params.get("owner_id", [""])[0]).strip()
            if not owner_id:
                return self._send_json({"items": []})
            return self._send_json({"items": get_saved_items(owner_id)})

        if parsed.path == "/api/video-file":
            params = parse_qs(parsed.query)
            video_id = str(params.get("video_id", [""])[0]).strip()
            if not video_id.isdigit():
                self.send_error(HTTPStatus.BAD_REQUEST)
                return
            file_path = get_video_file_path(int(video_id))
            if not file_path:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}")
            self.end_headers()
            return

        if parsed.path in {"/", "/index.html"}:
            return self._serve_static("index.html", "text/html; charset=utf-8")

        if parsed.path == "/app.js":
            return self._serve_static("app.js", "text/javascript; charset=utf-8")

        if parsed.path == "/styles.css":
            return self._serve_static("styles.css", "text/css; charset=utf-8")

        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            return self._send_json({"ok": False, "error": "JSON xato"}, HTTPStatus.BAD_REQUEST)

        if parsed.path == "/api/send-video":
            target_user_id = payload.get("target_user_id")
            video_id = payload.get("video_id")
            if not str(target_user_id).isdigit() or not str(video_id).isdigit():
                return self._send_json({"ok": False, "error": "ID lar noto'g'ri"}, HTTPStatus.BAD_REQUEST)

            ok, message = send_video_via_bot(int(target_user_id), int(video_id))
            status = HTTPStatus.OK if ok else HTTPStatus.BAD_REQUEST
            return self._send_json({"ok": ok, "message": message}, status)

        if parsed.path == "/api/delete-saved-video":
            owner_id = str(payload.get("owner_id", "")).strip()
            video_id = payload.get("video_id")
            if not owner_id or not str(video_id).isdigit():
                return self._send_json({"ok": False, "error": "Owner ID yoki video ID noto'g'ri"}, HTTPStatus.BAD_REQUEST)

            ok, message = delete_saved_video(owner_id, int(video_id))
            status = HTTPStatus.OK if ok else HTTPStatus.BAD_REQUEST
            return self._send_json({"ok": ok, "message": message}, status)

        self.send_error(HTTPStatus.NOT_FOUND)

    def _serve_static(self, filename: str, content_type: str) -> None:
        path = WEBAPP_DIR / filename
        if not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), AppHandler)
    logger.info("Backend running on http://%s:%s", HOST, PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
