import json
import sqlite3
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
DB_FILE = ROOT_DIR / "app.db"
VIDEOS_JSON_FILE = ROOT_DIR / "videos.json"
SAVED_VIDEOS_JSON_FILE = ROOT_DIR / "saved_videos.json"
USERS_JSON_FILE = ROOT_DIR / "users.json"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                first_join_date TEXT NOT NULL DEFAULT '',
                first_joined_at TEXT NOT NULL DEFAULT '',
                username TEXT NOT NULL DEFAULT '',
                full_name TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY,
                file_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                added_by INTEGER NOT NULL DEFAULT 0,
                added_at TEXT NOT NULL DEFAULT '',
                comment TEXT NOT NULL DEFAULT '',
                duration INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS saved_videos (
                owner_user_id INTEGER NOT NULL,
                saved_id INTEGER NOT NULL,
                video_id INTEGER NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                saved_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (owner_user_id, saved_id)
            );
            """
        )
    migrate_json_to_db()


def read_json(path: Path, fallback):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def migrate_json_to_db() -> None:
    with get_connection() as conn:
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        videos_count = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        saved_count = conn.execute("SELECT COUNT(*) FROM saved_videos").fetchone()[0]

        if users_count == 0 and USERS_JSON_FILE.exists():
            raw_users = read_json(USERS_JSON_FILE, {})
            if isinstance(raw_users, dict):
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO users (user_id, first_join_date, first_joined_at, username, full_name)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            int(user_id),
                            str(record.get("first_join_date", "")),
                            str(record.get("first_joined_at", "")),
                            str(record.get("username", "")),
                            str(record.get("full_name", "")),
                        )
                        for user_id, record in raw_users.items()
                        if str(user_id).lstrip("-").isdigit() and isinstance(record, dict)
                    ],
                )

        if videos_count == 0 and VIDEOS_JSON_FILE.exists():
            raw_videos = read_json(VIDEOS_JSON_FILE, {"items": []})
            items = raw_videos.get("items", []) if isinstance(raw_videos, dict) else []
            conn.executemany(
                """
                INSERT OR REPLACE INTO videos (id, file_id, title, added_by, added_at, comment, duration)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        int(item.get("id", 0)),
                        str(item.get("file_id", "")),
                        str(item.get("title", "")),
                        int(item.get("added_by", 0) or 0),
                        str(item.get("added_at", "")),
                        str(item.get("comment", "")),
                        int(item.get("duration", 0) or 0),
                    )
                    for item in items
                    if isinstance(item, dict) and int(item.get("id", 0) or 0) > 0
                ],
            )

        if saved_count == 0 and SAVED_VIDEOS_JSON_FILE.exists():
            raw_saved = read_json(SAVED_VIDEOS_JSON_FILE, {})
            rows = []
            if isinstance(raw_saved, dict):
                for owner_user_id, items in raw_saved.items():
                    if not str(owner_user_id).isdigit() or not isinstance(items, list):
                        continue
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        rows.append(
                            (
                                int(owner_user_id),
                                int(item.get("saved_id", item.get("video_id", 0)) or 0),
                                int(item.get("video_id", 0) or 0),
                                str(item.get("name", "")),
                                str(item.get("saved_at", "")),
                            )
                        )
            conn.executemany(
                """
                INSERT OR REPLACE INTO saved_videos (owner_user_id, saved_id, video_id, name, saved_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                [row for row in rows if row[1] > 0 and row[2] > 0],
            )


def load_users() -> dict[int, dict[str, str]]:
    init_db()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT user_id, first_join_date, first_joined_at, username, full_name
            FROM users
            ORDER BY user_id
            """
        ).fetchall()
    return {
        int(row["user_id"]): {
            "first_join_date": str(row["first_join_date"]),
            "first_joined_at": str(row["first_joined_at"]),
            "username": str(row["username"]),
            "full_name": str(row["full_name"]),
        }
        for row in rows
    }


def save_users(users: dict[int, dict[str, str]]) -> None:
    init_db()
    with get_connection() as conn:
        conn.execute("DELETE FROM users")
        conn.executemany(
            """
            INSERT INTO users (user_id, first_join_date, first_joined_at, username, full_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    int(user_id),
                    str(record.get("first_join_date", "")),
                    str(record.get("first_joined_at", "")),
                    str(record.get("username", "")),
                    str(record.get("full_name", "")),
                )
                for user_id, record in users.items()
            ],
        )


def load_video_catalog() -> dict[str, object]:
    init_db()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, file_id, title, added_by, added_at, comment, duration
            FROM videos
            ORDER BY id
            """
        ).fetchall()
    items = [
        {
            "id": int(row["id"]),
            "file_id": str(row["file_id"]),
            "title": str(row["title"]),
            "added_by": int(row["added_by"]),
            "added_at": str(row["added_at"]),
            "comment": str(row["comment"]),
            "duration": int(row["duration"]),
        }
        for row in rows
    ]
    next_id = (max((item["id"] for item in items), default=0) + 1) if items else 1
    return {"next_id": next_id, "items": items}


def save_video_catalog(catalog: dict[str, object]) -> None:
    init_db()
    items = catalog.get("items", [])
    if not isinstance(items, list):
        items = []
    with get_connection() as conn:
        conn.execute("DELETE FROM videos")
        conn.executemany(
            """
            INSERT INTO videos (id, file_id, title, added_by, added_at, comment, duration)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    int(item.get("id", 0)),
                    str(item.get("file_id", "")),
                    str(item.get("title", "")),
                    int(item.get("added_by", 0) or 0),
                    str(item.get("added_at", "")),
                    str(item.get("comment", "")),
                    int(item.get("duration", 0) or 0),
                )
                for item in items
                if isinstance(item, dict) and int(item.get("id", 0) or 0) > 0
            ],
        )


def load_saved_videos() -> dict[int, list[dict[str, object]]]:
    init_db()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT owner_user_id, saved_id, video_id, name, saved_at
            FROM saved_videos
            ORDER BY owner_user_id, saved_id
            """
        ).fetchall()
    result: dict[int, list[dict[str, object]]] = {}
    for row in rows:
        owner_user_id = int(row["owner_user_id"])
        result.setdefault(owner_user_id, []).append(
            {
                "saved_id": int(row["saved_id"]),
                "video_id": int(row["video_id"]),
                "name": str(row["name"]),
                "saved_at": str(row["saved_at"]),
            }
        )
    return result


def save_saved_videos(saved_videos: dict[int, list[dict[str, object]]]) -> None:
    init_db()
    rows = []
    for owner_user_id, items in saved_videos.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            rows.append(
                (
                    int(owner_user_id),
                    int(item.get("saved_id", item.get("video_id", 0)) or 0),
                    int(item.get("video_id", 0) or 0),
                    str(item.get("name", "")),
                    str(item.get("saved_at", "")),
                )
            )
    with get_connection() as conn:
        conn.execute("DELETE FROM saved_videos")
        conn.executemany(
            """
            INSERT INTO saved_videos (owner_user_id, saved_id, video_id, name, saved_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [row for row in rows if row[1] > 0 and row[2] > 0],
        )
