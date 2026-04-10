
import asyncio
import csv
import json
import math
import os
import re
import shutil
import uuid
from datetime import datetime, timedelta
from html import escape
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote, quote_plus, unquote, urlencode, urljoin, urlparse

from aiohttp import ClientSession, ClientTimeout, web
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader, select_autoescape

from downloaders.jm import manual_login_jm, manual_logout_jm, sync_jm_favorites
from downloaders.toonily import Chapter, DownloadReport, ToonilyAsyncDownloader, normalize_url
from core.provider_base import SiteProvider
from core.provider_loader import load_provider_plugins


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(
    os.getenv("DATA_DIR", "").strip()
    or str(BASE_DIR / "data")
).expanduser().resolve()
BOOKSHELF_FILE = DATA_DIR / "bookshelf.json"
SETTINGS_FILE = DATA_DIR / "webui_settings.json"
LEGACY_BOOKSHELF_FILE = BASE_DIR / "bookshelf.json"
LEGACY_SETTINGS_FILE = BASE_DIR / "webui_settings.json"
TEMPLATES_DIR = BASE_DIR / "templates"
DEFAULT_PROVIDER_ID = "toonily"
AUTO_PROVIDER_ID = "__auto__"
_TEMPLATE_ENV: Optional[Environment] = None
FLASH_MSG_COOKIE = "comic_flash_msg"


def ensure_data_dir_ready() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if DATA_DIR == BASE_DIR:
        return

    if not BOOKSHELF_FILE.exists() and LEGACY_BOOKSHELF_FILE.exists():
        shutil.copy2(LEGACY_BOOKSHELF_FILE, BOOKSHELF_FILE)
    if not SETTINGS_FILE.exists() and LEGACY_SETTINGS_FILE.exists():
        shutil.copy2(LEGACY_SETTINGS_FILE, SETTINGS_FILE)


def get_template_env() -> Environment:
    global _TEMPLATE_ENV
    if _TEMPLATE_ENV is not None:
        return _TEMPLATE_ENV

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    _TEMPLATE_ENV = env
    return env


def render_template(name: str, **context: Any) -> str:
    return get_template_env().get_template(name).render(**context)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def fmt_time(value: str) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return value


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, number))


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def form_getall_str(form: Any, key: str) -> list[str]:
    if hasattr(form, "getall"):
        return [str(v).strip() for v in form.getall(key) if str(v).strip()]
    value = str(form.get(key, "") or "").strip() if hasattr(form, "get") else ""
    return [value] if value else []


def format_chapter_number(num: Optional[float]) -> str:
    if num is None:
        return "-"
    if float(num).is_integer():
        return str(int(num))
    return str(num)


PROVIDERS: dict[str, SiteProvider] = {}
_PROVIDERS_LOADED = False


def _provider_loader_log(message: str) -> None:
    print(f"[provider-loader] {message}", flush=True)


def _provider_context() -> dict[str, Any]:
    return {
        "search_toonily": search_toonily,
        "fetch_series_snapshot_toonily": fetch_series_snapshot_toonily,
    }


def ensure_providers_loaded() -> None:
    global _PROVIDERS_LOADED
    if _PROVIDERS_LOADED:
        return

    plugins_dir = BASE_DIR / "providers"
    loaded = load_provider_plugins(plugins_dir, _provider_context(), logger=_provider_loader_log)
    PROVIDERS.clear()
    PROVIDERS.update(loaded)
    _PROVIDERS_LOADED = True


def list_providers() -> list[SiteProvider]:
    ensure_providers_loaded()
    providers = list(PROVIDERS.values())
    providers.sort(key=lambda p: (p.provider_id != DEFAULT_PROVIDER_ID, p.display_name.lower()))
    return providers


def get_provider(provider_id: str) -> SiteProvider:
    ensure_providers_loaded()
    key = (provider_id or "").strip().lower()
    provider = PROVIDERS.get(key)
    if provider is not None:
        return provider
    default_provider = PROVIDERS.get(DEFAULT_PROVIDER_ID)
    if default_provider is not None:
        return default_provider
    if PROVIDERS:
        return next(iter(PROVIDERS.values()))
    raise RuntimeError("未加载到任何站点插件，请检查 providers 目录。")


def provider_name(provider_id: str) -> str:
    ensure_providers_loaded()
    key = (provider_id or "").strip().lower()
    provider = PROVIDERS.get(key)
    if provider is not None:
        return provider.display_name
    return key or DEFAULT_PROVIDER_ID


def provider_disabled_reason(state: "UIState", provider: SiteProvider) -> str:
    if not provider.enabled:
        return provider.disabled_reason or "该站点当前不可用。"
    if not state.is_provider_enabled(provider.provider_id):
        return "该站点已在设置中停用。"
    return ""


def provider_enabled_for_state(state: "UIState", provider: SiteProvider) -> bool:
    return provider_disabled_reason(state, provider) == ""


def provider_icon_svg(provider_id: str) -> str:
    key = (provider_id or "").strip().lower()
    if key == "jmcomic":
        return (
            "<svg class=\"site-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
            "<path d=\"M4 6h16v12H4z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"/>"
            "<path d=\"M8 9h8M8 12h5M8 15h8\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>"
            "</svg>"
        )
    if key == "toonily":
        return (
            "<svg class=\"site-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
            "<circle cx=\"12\" cy=\"12\" r=\"8\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"/>"
            "<path d=\"M8 9h8M9 12h6M10 15h4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>"
            "</svg>"
        )
    return (
        "<svg class=\"site-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18Z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"/>"
        "<path d=\"M3 12h18M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.4\"/>"
        "</svg>"
    )


def render_provider_badge(provider_id: str) -> str:
    key = (provider_id or "").strip().lower()
    title = provider_name(key)
    return (
        f"<span class=\"site-badge {escape(key or 'default')}\">"
        f"{provider_icon_svg(key)}"
        f"<span>{escape(title)}</span>"
        "</span>"
    )


class UIState:
    def __init__(self) -> None:
        self.bookshelf: dict[str, dict[str, Any]] = {}
        self.jobs: dict[str, dict[str, Any]] = {}
        self.job_order_counter = 0
        self.current_job_id: Optional[str] = None
        self.job_history: list[dict[str, Any]] = []
        self.max_job_history = 500
        self.last_search_query = ""
        self.last_search_provider = DEFAULT_PROVIDER_ID
        self.last_search_results: list[dict[str, Any]] = []

        self.output_dir = BASE_DIR / "downloads"
        self.chapter_concurrency = 3
        self.image_concurrency = 10
        self.retries = 3
        self.timeout = 45
        self.max_parallel_jobs = 2
        self.retry_base_delay_seconds = 0.8
        self.retry_recoverable_only = True
        self.enable_chapter_dedupe = True

        self.image_output_format = "original"
        self.image_quality = 85
        self.keep_original_images = False
        self.auto_archive_format = "none"
        self.write_metadata_sidecar = True

        self.manga_dir_template = "{site}/{manga}"
        self.chapter_dir_template = "{chapter_number}-{chapter_title}"
        self.page_name_template = "{page:03}"

        self.bandwidth_day_kbps = 0
        self.bandwidth_night_kbps = 0
        self.night_start_hour = 22
        self.night_end_hour = 7

        self.scheduler_enabled = False
        self.scheduler_interval_minutes = 60
        self.scheduler_auto_download = True
        self.scheduler_last_run_at = ""
        self.scheduler_next_run_at = ""
        self.scheduler_task: Optional[asyncio.Task[Any]] = None
        self._scheduler_running = False

        self.health_stats: dict[str, dict[str, Any]] = {}

        self.redis_host = os.getenv("REDIS_HOST", "").strip()
        self.redis_port = parse_int(os.getenv("REDIS_PORT", "6379"), 6379, minimum=1, maximum=65535)
        self.redis_db = parse_int(os.getenv("REDIS_DB", "0"), 0, minimum=0, maximum=999999)
        self.redis_username = (
            os.getenv("REDIS_USERNAME", "").strip()
        )
        self.redis_password = (
            os.getenv("REDIS_PASSWORD", "").strip()
        )
        self.cache_enabled = True
        self.cache_ttl_seconds = 900
        self.jm_username = os.getenv("JM_USERNAME", "").strip()
        self.jm_password = os.getenv("JM_PASSWORD", "").strip()
        self.jm_manual_logged_in = False
        self.jm_manual_login_user = ""
        self.enabled_provider_ids: set[str] = set(PROVIDERS.keys()) or {DEFAULT_PROVIDER_ID}
        self.webhook_enabled = False
        self.webhook_url = ""
        self.webhook_token = ""
        self.webhook_event_completed = True
        self.webhook_event_failed = True
        self.webhook_event_cancelled = False
        self.webhook_timeout_seconds = 8
        self.compact_mode_enabled = False
        self.manga_view_mode = "poster"

        self.max_job_logs = 600
        self._save_lock = asyncio.Lock()

    def load(self) -> None:
        if SETTINGS_FILE.exists():
            try:
                raw = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
                self.output_dir = Path(raw.get("output_dir", str(self.output_dir))).resolve()
                self.chapter_concurrency = max(1, int(raw.get("chapter_concurrency", self.chapter_concurrency)))
                self.image_concurrency = max(1, int(raw.get("image_concurrency", self.image_concurrency)))
                self.retries = max(1, int(raw.get("retries", self.retries)))
                self.timeout = max(10, int(raw.get("timeout", self.timeout)))
                self.max_parallel_jobs = parse_int(raw.get("max_parallel_jobs", self.max_parallel_jobs), self.max_parallel_jobs, minimum=1, maximum=20)
                self.retry_base_delay_seconds = max(0.2, float(raw.get("retry_base_delay_seconds", self.retry_base_delay_seconds)))
                self.retry_recoverable_only = parse_bool(raw.get("retry_recoverable_only", self.retry_recoverable_only), self.retry_recoverable_only)
                self.enable_chapter_dedupe = parse_bool(raw.get("enable_chapter_dedupe", self.enable_chapter_dedupe), self.enable_chapter_dedupe)
                image_fmt = str(raw.get("image_output_format", self.image_output_format)).strip().lower()
                self.image_output_format = image_fmt if image_fmt in {"original", "jpg", "webp"} else "original"
                self.image_quality = parse_int(raw.get("image_quality", self.image_quality), self.image_quality, minimum=1, maximum=100)
                self.keep_original_images = parse_bool(raw.get("keep_original_images", self.keep_original_images), self.keep_original_images)
                archive_fmt = str(raw.get("auto_archive_format", self.auto_archive_format)).strip().lower()
                self.auto_archive_format = archive_fmt if archive_fmt in {"none", "cbz", "zip"} else "none"
                self.write_metadata_sidecar = parse_bool(raw.get("write_metadata_sidecar", self.write_metadata_sidecar), self.write_metadata_sidecar)
                self.manga_dir_template = str(raw.get("manga_dir_template", self.manga_dir_template) or self.manga_dir_template)
                self.chapter_dir_template = str(raw.get("chapter_dir_template", self.chapter_dir_template) or self.chapter_dir_template)
                self.page_name_template = str(raw.get("page_name_template", self.page_name_template) or self.page_name_template)
                self.bandwidth_day_kbps = max(0, int(raw.get("bandwidth_day_kbps", self.bandwidth_day_kbps)))
                self.bandwidth_night_kbps = max(0, int(raw.get("bandwidth_night_kbps", self.bandwidth_night_kbps)))
                self.night_start_hour = parse_int(raw.get("night_start_hour", self.night_start_hour), self.night_start_hour, minimum=0, maximum=23)
                self.night_end_hour = parse_int(raw.get("night_end_hour", self.night_end_hour), self.night_end_hour, minimum=0, maximum=23)
                self.scheduler_enabled = parse_bool(raw.get("scheduler_enabled", self.scheduler_enabled), self.scheduler_enabled)
                self.scheduler_interval_minutes = parse_int(raw.get("scheduler_interval_minutes", self.scheduler_interval_minutes), self.scheduler_interval_minutes, minimum=5, maximum=1440)
                self.scheduler_auto_download = parse_bool(raw.get("scheduler_auto_download", self.scheduler_auto_download), self.scheduler_auto_download)
                self.scheduler_last_run_at = str(raw.get("scheduler_last_run_at", self.scheduler_last_run_at) or "")
                self.scheduler_next_run_at = str(raw.get("scheduler_next_run_at", self.scheduler_next_run_at) or "")
                self.redis_host = str(raw.get("redis_host", self.redis_host)).strip()
                self.redis_port = parse_int(raw.get("redis_port", self.redis_port), self.redis_port, minimum=1, maximum=65535)
                self.redis_db = parse_int(raw.get("redis_db", self.redis_db), self.redis_db, minimum=0, maximum=999999)
                self.redis_username = str(raw.get("redis_username", self.redis_username)).strip()
                self.redis_password = str(raw.get("redis_password", self.redis_password)).strip()
                self.cache_enabled = bool(raw.get("cache_enabled", self.cache_enabled))
                self.cache_ttl_seconds = max(30, int(raw.get("cache_ttl_seconds", self.cache_ttl_seconds)))
                self.jm_username = str(raw.get("jm_username", self.jm_username)).strip()
                self.jm_password = str(raw.get("jm_password", self.jm_password)).strip()
                self.webhook_enabled = parse_bool(raw.get("webhook_enabled", self.webhook_enabled), self.webhook_enabled)
                self.webhook_url = str(raw.get("webhook_url", self.webhook_url)).strip()
                self.webhook_token = str(raw.get("webhook_token", self.webhook_token)).strip()
                self.webhook_event_completed = parse_bool(raw.get("webhook_event_completed", self.webhook_event_completed), self.webhook_event_completed)
                self.webhook_event_failed = parse_bool(raw.get("webhook_event_failed", self.webhook_event_failed), self.webhook_event_failed)
                self.webhook_event_cancelled = parse_bool(raw.get("webhook_event_cancelled", self.webhook_event_cancelled), self.webhook_event_cancelled)
                self.webhook_timeout_seconds = parse_int(
                    raw.get("webhook_timeout_seconds", self.webhook_timeout_seconds),
                    self.webhook_timeout_seconds,
                    minimum=3,
                    maximum=30,
                )
                self.compact_mode_enabled = parse_bool(raw.get("compact_mode_enabled", self.compact_mode_enabled), self.compact_mode_enabled)
                raw_view_mode = str(raw.get("manga_view_mode", self.manga_view_mode)).strip().lower()
                self.manga_view_mode = raw_view_mode if raw_view_mode in {"poster", "list"} else "poster"
                if not self.redis_host:
                    legacy_redis_url = str(raw.get("redis_url", "")).strip()
                    if legacy_redis_url:
                        parsed = urlparse(legacy_redis_url)
                        self.redis_host = (parsed.hostname or "").strip()
                        if parsed.port:
                            self.redis_port = parse_int(parsed.port, self.redis_port, minimum=1, maximum=65535)
                        db_text = parsed.path.lstrip("/").strip()
                        if db_text:
                            self.redis_db = parse_int(db_text, self.redis_db, minimum=0, maximum=999999)
                enabled_providers = raw.get("enabled_providers")
                if isinstance(enabled_providers, list):
                    self.enabled_provider_ids = {
                        str(pid).strip().lower() for pid in enabled_providers if str(pid).strip()
                    }
            except Exception:
                pass

        if "REDIS_HOST" in os.environ:
            self.redis_host = os.getenv("REDIS_HOST", "").strip()
        if "REDIS_PORT" in os.environ:
            self.redis_port = parse_int(os.getenv("REDIS_PORT", self.redis_port), self.redis_port, minimum=1, maximum=65535)
        if "REDIS_DB" in os.environ:
            self.redis_db = parse_int(os.getenv("REDIS_DB", self.redis_db), self.redis_db, minimum=0, maximum=999999)

        if "REDIS_USERNAME" in os.environ:
            self.redis_username = os.getenv("REDIS_USERNAME", "").strip()

        if "REDIS_PASSWORD" in os.environ:
            self.redis_password = os.getenv("REDIS_PASSWORD", "").strip()
        if not self.redis_host:
            self.cache_enabled = False

        self.normalize_enabled_providers()

        if BOOKSHELF_FILE.exists():
            try:
                raw = json.loads(BOOKSHELF_FILE.read_text(encoding="utf-8"))
                items: list[dict[str, Any]] = raw if isinstance(raw, list) else []
                for item in items:
                    book = self._normalize_book_item(item)
                    self.bookshelf[book["id"]] = book
            except Exception:
                self.bookshelf = {}

        if self.scheduler_enabled and not self.scheduler_next_run_at:
            self.schedule_next_run(immediate=False)

    async def save_settings(self) -> None:
        payload = {
            "output_dir": str(self.output_dir),
            "chapter_concurrency": self.chapter_concurrency,
            "image_concurrency": self.image_concurrency,
            "retries": self.retries,
            "timeout": self.timeout,
            "max_parallel_jobs": self.max_parallel_jobs,
            "retry_base_delay_seconds": self.retry_base_delay_seconds,
            "retry_recoverable_only": self.retry_recoverable_only,
            "enable_chapter_dedupe": self.enable_chapter_dedupe,
            "image_output_format": self.image_output_format,
            "image_quality": self.image_quality,
            "keep_original_images": self.keep_original_images,
            "auto_archive_format": self.auto_archive_format,
            "write_metadata_sidecar": self.write_metadata_sidecar,
            "manga_dir_template": self.manga_dir_template,
            "chapter_dir_template": self.chapter_dir_template,
            "page_name_template": self.page_name_template,
            "bandwidth_day_kbps": self.bandwidth_day_kbps,
            "bandwidth_night_kbps": self.bandwidth_night_kbps,
            "night_start_hour": self.night_start_hour,
            "night_end_hour": self.night_end_hour,
            "scheduler_enabled": self.scheduler_enabled,
            "scheduler_interval_minutes": self.scheduler_interval_minutes,
            "scheduler_auto_download": self.scheduler_auto_download,
            "scheduler_last_run_at": self.scheduler_last_run_at,
            "scheduler_next_run_at": self.scheduler_next_run_at,
            "redis_host": self.redis_host,
            "redis_port": self.redis_port,
            "redis_db": self.redis_db,
            "redis_username": self.redis_username,
            "redis_password": self.redis_password,
            "cache_enabled": self.cache_enabled,
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "jm_username": self.jm_username,
            "jm_password": self.jm_password,
            "webhook_enabled": self.webhook_enabled,
            "webhook_url": self.webhook_url,
            "webhook_token": self.webhook_token,
            "webhook_event_completed": self.webhook_event_completed,
            "webhook_event_failed": self.webhook_event_failed,
            "webhook_event_cancelled": self.webhook_event_cancelled,
            "webhook_timeout_seconds": self.webhook_timeout_seconds,
            "compact_mode_enabled": self.compact_mode_enabled,
            "manga_view_mode": self.manga_view_mode,
            "enabled_providers": sorted(self.enabled_provider_ids),
        }
        async with self._save_lock:
            SETTINGS_FILE.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def normalize_enabled_providers(self) -> None:
        ensure_providers_loaded()
        known = {str(pid).strip().lower() for pid in PROVIDERS.keys() if str(pid).strip()}
        selected = {pid for pid in self.enabled_provider_ids if pid in known}
        if not selected:
            if DEFAULT_PROVIDER_ID in known:
                selected = {DEFAULT_PROVIDER_ID}
            elif known:
                selected = {sorted(known)[0]}
        self.enabled_provider_ids = selected

        if self.enabled_provider_ids and self.last_search_provider not in self.enabled_provider_ids:
            self.last_search_provider = sorted(self.enabled_provider_ids)[0]

    def is_provider_enabled(self, provider_id: str) -> bool:
        key = (provider_id or "").strip().lower()
        return key in self.enabled_provider_ids

    def set_enabled_providers(self, provider_ids: set[str]) -> None:
        self.enabled_provider_ids = {str(pid).strip().lower() for pid in provider_ids if str(pid).strip()}
        self.normalize_enabled_providers()

    def ensure_health_entry(self, provider_id: str) -> dict[str, Any]:
        pid = (provider_id or DEFAULT_PROVIDER_ID).strip().lower() or DEFAULT_PROVIDER_ID
        row = self.health_stats.get(pid)
        if row is not None:
            return row
        row = {
            "provider_id": pid,
            "provider_name": provider_name(pid),
            "available": True,
            "last_check_at": "",
            "last_error": "",
            "total_jobs": 0,
            "success_jobs": 0,
            "failed_jobs": 0,
            "total_downloaded_bytes": 0,
            "total_download_seconds": 0.0,
            "avg_speed_kbps": 0.0,
            "failure_reasons": {},
        }
        self.health_stats[pid] = row
        return row

    def mark_provider_health(self, provider_id: str, *, available: bool, error: str = "") -> None:
        row = self.ensure_health_entry(provider_id)
        row["available"] = bool(available)
        row["last_check_at"] = now_iso()
        row["last_error"] = str(error or "").strip()

    def record_download_report(self, provider_id: str, report: Optional[DownloadReport], status: str, error: str = "") -> None:
        row = self.ensure_health_entry(provider_id)
        row["provider_name"] = provider_name(provider_id)
        row["last_check_at"] = now_iso()
        row["total_jobs"] = int(row.get("total_jobs", 0)) + 1
        if status == "completed":
            row["success_jobs"] = int(row.get("success_jobs", 0)) + 1
            row["available"] = True
            row["last_error"] = ""
        elif status == "cancelled":
            row["last_error"] = str(error or "").strip()
        else:
            row["failed_jobs"] = int(row.get("failed_jobs", 0)) + 1
            row["available"] = False
            row["last_error"] = str(error or row.get("last_error") or "").strip()

        if report is not None:
            downloaded_bytes = max(0, int(report.downloaded_bytes or 0))
            elapsed_seconds = max(
                0.0,
                (report.finished_at - report.started_at).total_seconds(),
            )
            row["total_downloaded_bytes"] = int(row.get("total_downloaded_bytes", 0)) + downloaded_bytes
            row["total_download_seconds"] = float(row.get("total_download_seconds", 0.0)) + elapsed_seconds
            total_seconds = float(row.get("total_download_seconds", 0.0))
            total_bytes = int(row.get("total_downloaded_bytes", 0))
            if total_seconds > 0:
                row["avg_speed_kbps"] = round((total_bytes / total_seconds) / 1024, 2)
            failure_reasons = dict(row.get("failure_reasons") or {})
            for reason, count in (report.failure_reasons or {}).items():
                key = str(reason or "unknown").strip() or "unknown"
                failure_reasons[key] = int(failure_reasons.get(key, 0)) + int(count)
            row["failure_reasons"] = failure_reasons
        elif error and status != "cancelled":
            failure_reasons = dict(row.get("failure_reasons") or {})
            key = str(error).strip() or "unknown"
            failure_reasons[key] = int(failure_reasons.get(key, 0)) + 1
            row["failure_reasons"] = failure_reasons

    def schedule_next_run(self, *, immediate: bool = False) -> None:
        if immediate:
            next_dt = datetime.now()
        else:
            next_dt = datetime.now() + timedelta(minutes=max(5, int(self.scheduler_interval_minutes)))
        self.scheduler_next_run_at = next_dt.isoformat(timespec="seconds")

    async def save_bookshelf(self) -> None:
        items = list(self.bookshelf.values())
        items.sort(key=lambda x: x.get("title", "").lower())
        async with self._save_lock:
            BOOKSHELF_FILE.write_text(
                json.dumps(items, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def _normalize_book_item(self, raw: dict[str, Any]) -> dict[str, Any]:
        series_url = normalize_url(str(raw.get("series_url", "")))
        provider_id = str(raw.get("provider_id") or DEFAULT_PROVIDER_ID).strip().lower() or DEFAULT_PROVIDER_ID
        return {
            "id": str(raw.get("id") or uuid.uuid4().hex[:12]),
            "provider_id": provider_id,
            "title": str(raw.get("title") or "未命名漫画").strip(),
            "series_url": series_url,
            "cover": str(raw.get("cover") or "").strip(),
            "group": str(raw.get("group") or "").strip(),
            "follow_enabled": bool(raw.get("follow_enabled", True)),
            "last_downloaded_chapter_number": parse_float(raw.get("last_downloaded_chapter_number")),
            "last_downloaded_chapter_title": str(raw.get("last_downloaded_chapter_title") or "").strip(),
            "last_downloaded_chapter_url": normalize_url(str(raw.get("last_downloaded_chapter_url") or "")),
            "latest_site_chapter_number": parse_float(raw.get("latest_site_chapter_number")),
            "latest_site_chapter_title": str(raw.get("latest_site_chapter_title") or "").strip(),
            "latest_site_chapter_url": normalize_url(str(raw.get("latest_site_chapter_url") or "")),
            "pending_update_count": int(raw.get("pending_update_count", 0)),
            "last_checked_at": str(raw.get("last_checked_at") or ""),
            "last_update_at": str(raw.get("last_update_at") or ""),
        }

    def list_books(self) -> list[dict[str, Any]]:
        books = list(self.bookshelf.values())
        books.sort(key=lambda x: (x.get("provider_id", DEFAULT_PROVIDER_ID), x.get("title", "").lower()))
        return books

    def get_book(self, book_id: str) -> Optional[dict[str, Any]]:
        return self.bookshelf.get(book_id)

    def upsert_book(
        self,
        *,
        provider_id: str,
        title: str,
        series_url: str,
        cover: str = "",
        group: str = "",
    ) -> tuple[dict[str, Any], bool]:
        pid = (provider_id or DEFAULT_PROVIDER_ID).strip().lower() or DEFAULT_PROVIDER_ID
        normalized = normalize_url(series_url)
        group_name = str(group or "").strip()
        for book in self.bookshelf.values():
            if (
                book.get("provider_id", DEFAULT_PROVIDER_ID) == pid
                and normalize_url(book.get("series_url", "")) == normalized
            ):
                if title:
                    book["title"] = title
                if cover:
                    book["cover"] = cover
                if group_name:
                    book["group"] = group_name
                return book, False

        book = self._normalize_book_item(
            {
                "id": uuid.uuid4().hex[:12],
                "provider_id": pid,
                "title": title or "未命名漫画",
                "series_url": normalized,
                "cover": cover,
                "group": group_name,
                "follow_enabled": True,
            }
        )
        self.bookshelf[book["id"]] = book
        return book, True

    def remove_book(self, book_id: str) -> bool:
        return self.bookshelf.pop(book_id, None) is not None

    def append_job_log(self, job: dict[str, Any], message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{stamp}] {message}"
        logs = job["logs"]
        logs.append(line)
        if len(logs) > self.max_job_logs:
            del logs[0 : len(logs) - self.max_job_logs]
        try:
            job_id = str(job.get("id") or "-")
            print(f"[job:{job_id}] {line}", flush=True)
        except Exception:
            # Console logging must never affect in-memory job logging.
            pass

    def append_job_history(self, item: dict[str, Any]) -> None:
        self.job_history.append(item)
        if len(self.job_history) > self.max_job_history:
            del self.job_history[0 : len(self.job_history) - self.max_job_history]

    def webhook_event_enabled(self, status: str) -> bool:
        if status == "completed":
            return self.webhook_event_completed
        if status == "failed":
            return self.webhook_event_failed
        if status == "cancelled":
            return self.webhook_event_cancelled
        return False

    def create_job(
        self,
        *,
        title: str,
        series_url: str,
        chapter_selector: str = "all",
        chapter_urls: Optional[list[str]] = None,
        mode: str = "download_all",
        book_id: str = "",
        provider_id: str = DEFAULT_PROVIDER_ID,
    ) -> dict[str, Any]:
        job_id = uuid.uuid4().hex[:10]
        pause_event = asyncio.Event()
        pause_event.set()
        self.job_order_counter += 1

        job = {
            "id": job_id,
            "title": title,
            "series_url": normalize_url(series_url),
            "provider_id": (provider_id or DEFAULT_PROVIDER_ID).strip().lower() or DEFAULT_PROVIDER_ID,
            "chapter_selector": chapter_selector,
            "chapter_urls": chapter_urls or [],
            "mode": mode,
            "book_id": book_id,
            "status": "queued",
            "error": "",
            "retry_file": "",
            "created_at": now_iso(),
            "queue_order": self.job_order_counter,
            "started_at": "",
            "finished_at": "",
            "done_chapters": 0,
            "total_chapters": 0,
            "saved_images": 0,
            "total_images": 0,
            "successful_chapters": 0,
            "failed_chapters": 0,
            "cancel_requested": False,
            "pause_event": pause_event,
            "task": None,
            "logs": [],
        }
        self.jobs[job_id] = job
        self.current_job_id = job_id
        self.append_job_log(job, "任务已创建，等待执行。")
        return job


def chapter_percent(done_count: int, total_count: int) -> int:
    if total_count <= 0:
        return 0
    return max(0, min(100, int(done_count * 100 / total_count)))


def is_job_final(status: str) -> bool:
    return status in {"completed", "failed", "cancelled"}


def queue_order_value(job: dict[str, Any]) -> int:
    fallback = parse_int(job.get("created_order", 999999999), 999999999, minimum=0, maximum=999999999)
    return parse_int(job.get("queue_order", fallback), fallback, minimum=0, maximum=999999999)


def queued_jobs_sorted(state: UIState) -> list[dict[str, Any]]:
    jobs = [job for job in state.jobs.values() if job.get("status") == "queued"]
    jobs.sort(key=lambda row: (queue_order_value(row), str(row.get("created_at", ""))))
    return jobs


def normalize_queue_orders(state: UIState) -> None:
    queued = queued_jobs_sorted(state)
    for idx, job in enumerate(queued, start=1):
        job["queue_order"] = idx
    if queued:
        state.job_order_counter = max(state.job_order_counter, len(queued))


def parse_iso_datetime(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def get_app_state(request: web.Request) -> UIState:
    return request.app["state"]


def pop_flash_message(request: web.Request) -> str:
    msg = str(request.get("flash_msg", "") or "").strip()
    if msg:
        return msg
    # Backward compatibility for old links with ?msg=...
    return str(request.query.get("msg", "") or "").strip()


def request_wants_json(request: web.Request, form: Optional[Any] = None) -> bool:
    xrw = str(request.headers.get("X-Requested-With", "") or "").strip().lower()
    if xrw == "xmlhttprequest":
        return True
    accept = str(request.headers.get("Accept", "") or "").lower()
    if "application/json" in accept:
        return True
    if form is not None:
        try:
            flag = str(form.get("ajax", "") or "").strip().lower()
            if flag in {"1", "true", "yes", "on"}:
                return True
        except Exception:
            pass
    qflag = str(request.query.get("ajax", "") or "").strip().lower()
    return qflag in {"1", "true", "yes", "on"}


@web.middleware
async def flash_message_middleware(request: web.Request, handler: Callable[..., Any]) -> web.StreamResponse:
    raw_cookie = str(request.cookies.get(FLASH_MSG_COOKIE, "") or "")
    if raw_cookie:
        try:
            request["flash_msg"] = unquote(raw_cookie).strip()
        except Exception:
            request["flash_msg"] = raw_cookie.strip()

    try:
        response = await handler(request)
    except web.HTTPException as exc:
        response = exc
        if raw_cookie:
            response.del_cookie(FLASH_MSG_COOKIE, path="/")
        raise

    if raw_cookie:
        response.del_cookie(FLASH_MSG_COOKIE, path="/")
    return response


def count_active_jobs(state: UIState) -> int:
    total = 0
    for job in state.jobs.values():
        if job.get("status") in {"running", "paused", "cancelling"}:
            total += 1
    return total


def dispatch_jobs(state: UIState) -> None:
    active = count_active_jobs(state)
    if active >= state.max_parallel_jobs:
        return

    queued_jobs = [job for job in queued_jobs_sorted(state) if job.get("task") is None]
    for job in queued_jobs:
        if active >= state.max_parallel_jobs:
            break
        start_job(state, job)
        active += 1


def redis_cache_enabled_for_state(state: UIState) -> bool:
    return bool(state.cache_enabled and state.redis_host.strip())


async def fetch_html_with_downloader(
    state: UIState,
    url: str,
    logger: Optional[callable] = None,
) -> str:
    downloader = ToonilyAsyncDownloader(
        series_url=url,
        output_dir=state.output_dir,
        chapter_selector="all",
        chapter_concurrency=1,
        image_concurrency=max(2, min(state.image_concurrency, 5)),
        retries=state.retries,
        timeout=state.timeout,
        write_failed_file=False,
        logger=logger,
        cache_enabled=redis_cache_enabled_for_state(state),
        redis_host=state.redis_host,
        redis_port=state.redis_port,
        redis_db=state.redis_db,
        redis_username=state.redis_username,
        redis_password=state.redis_password,
        cache_ttl_seconds=state.cache_ttl_seconds,
        retry_base_delay_seconds=state.retry_base_delay_seconds,
        retry_recoverable_only=state.retry_recoverable_only,
    )
    try:
        return await downloader.fetch_html(url)
    finally:
        await downloader.close()


def normalize_http_url(url: str, *, base_url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    if text.startswith("//"):
        text = f"https:{text}"
    elif text.startswith("/"):
        text = urljoin(base_url, text)
    if text.startswith(("http://", "https://")):
        return text
    return ""


def best_src_from_srcset(srcset: str) -> str:
    best_url = ""
    best_score = -1.0
    for raw in str(srcset or "").split(","):
        item = raw.strip()
        if not item:
            continue
        parts = item.split()
        if not parts:
            continue
        candidate = parts[0].strip()
        score = 0.0
        if len(parts) > 1:
            desc = parts[1].strip().lower()
            match_w = re.match(r"^(\d+(?:\.\d+)?)w$", desc)
            match_x = re.match(r"^(\d+(?:\.\d+)?)x$", desc)
            if match_w:
                score = float(match_w.group(1))
            elif match_x:
                score = float(match_x.group(1)) * 1000.0
        if score >= best_score:
            best_score = score
            best_url = candidate
    return best_url


def extract_img_url(img: Any, *, base_url: str) -> str:
    if img is None:
        return ""
    srcset = str(img.get("data-srcset") or img.get("srcset") or "").strip()
    if srcset:
        url = normalize_http_url(best_src_from_srcset(srcset), base_url=base_url)
        if url:
            return url
    raw = (
        img.get("data-src")
        or img.get("data-lazy-src")
        or img.get("data-original")
        or img.get("src")
        or ""
    )
    return normalize_http_url(str(raw).strip(), base_url=base_url)


def parse_toonily_cover_from_html(html: str, *, base_url: str = "https://toonily.com") -> str:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ".summary_image img.img-responsive",
        ".summary_image img",
        ".profile-manga img.img-responsive",
        ".profile-manga img",
        "img.img-responsive",
    ]
    for selector in selectors:
        img = soup.select_one(selector)
        cover_url = extract_img_url(img, base_url=base_url)
        if cover_url:
            return cover_url

    og = soup.select_one("meta[property='og:image'], meta[name='og:image']")
    if og is not None:
        cover_url = normalize_http_url(str(og.get("content") or "").strip(), base_url=base_url)
        if cover_url:
            return cover_url
    return ""


async def fetch_toonily_cover_url(
    state: UIState,
    series_url: str,
    *,
    logger: Optional[callable] = None,
) -> str:
    html = await fetch_html_with_downloader(state, series_url, logger=logger)
    return parse_toonily_cover_from_html(html, base_url=series_url or "https://toonily.com")


def parse_search_results(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    series_href_re = re.compile(r"/(serie|series)/", re.IGNORECASE)
    selectors = [
        "div.page-item-detail.manga h3.h5 a",
        "div.c-tabs-item__content .post-title h3 a",
        "div.c-tabs-item__content .post-title a",
        "div.post-title h3 a",
        "h3.h5 a[href*='/serie/']",
        "a[href*='/serie/']",
        "a[href*='/series/']",
    ]

    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    for selector in selectors:
        for a in soup.select(selector):
            href = (a.get("href") or "").strip()
            if not series_href_re.search(href):
                continue
            url = normalize_url(urljoin("https://toonily.com", href))
            if url in seen:
                continue
            title = " ".join(a.get_text(" ", strip=True).split())
            if not title:
                continue

            container = a
            for _ in range(6):
                if container is None:
                    break
                classes = " ".join(container.get("class") or [])
                if any(token in classes for token in ("c-tabs-item", "page-item-detail", "manga")):
                    break
                container = container.parent

            latest = ""
            cover = ""
            if container is not None:
                chapter_link = container.select_one("div.chapter a, .chapter-item a, .latest-chap a")
                if chapter_link is not None:
                    latest = " ".join(chapter_link.get_text(" ", strip=True).split())
                img = container.select_one("img")
                cover = extract_img_url(img, base_url="https://toonily.com")

            results.append(
                {
                    "title": title,
                    "url": url,
                    "latest": latest,
                    "cover": cover,
                }
            )
            seen.add(url)
            if len(results) >= 40:
                return results

    # Fallback: toonily occasionally renders search results via script/json rather than anchor cards.
    if len(results) < 40:
        pattern = re.compile(
            r"(https?://(?:www\.)?toonily\.com/(?:serie|series)/[a-z0-9][a-z0-9-]*/?)",
            re.IGNORECASE,
        )
        for match in pattern.finditer(html):
            candidate = normalize_url(match.group(1))
            if not candidate or candidate in seen:
                continue
            slug = candidate.rstrip("/").split("/")[-1]
            slug_without_hash = re.sub(r"-[0-9a-f]{6,}$", "", slug, flags=re.IGNORECASE)
            guessed_title = " ".join(part for part in slug_without_hash.split("-") if part).strip() or slug
            results.append(
                {
                    "title": guessed_title.title(),
                    "url": candidate,
                    "latest": "",
                    "cover": "",
                }
            )
            seen.add(candidate)
            if len(results) >= 40:
                break
    return results


def extract_series_url_hint(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    og_url = soup.select_one("meta[property='og:url']")
    if og_url is not None:
        candidate = normalize_url(str(og_url.get("content") or ""))
        if "/serie/" in candidate or "/series/" in candidate:
            return candidate

    canonical = soup.select_one("link[rel='canonical']")
    if canonical is not None:
        candidate = normalize_url(str(canonical.get("href") or ""))
        if "/serie/" in candidate or "/series/" in candidate:
            return candidate

    match = re.search(r'"base_url"\s*:\s*"([^"]+?/(?:serie|series)/[^"]+?)"', html)
    if match:
        candidate = normalize_url(match.group(1).replace("\\/", "/"))
        if "/serie/" in candidate or "/series/" in candidate:
            return candidate

    return ""


def slugify_keyword(keyword: str) -> str:
    text = keyword.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


async def search_toonily(state: UIState, keyword: str) -> list[dict[str, Any]]:
    keyword = keyword.strip()
    if not keyword:
        return []

    if keyword.startswith(("http://", "https://")) and ("/serie/" in keyword or "/series/" in keyword):
        hinted_url = normalize_url(keyword)
        hinted_title = hinted_url.rstrip("/").split("/")[-1].replace("-", " ")
        try:
            snapshot_title, _ = await fetch_series_snapshot_toonily(state, hinted_url)
            if snapshot_title:
                hinted_title = snapshot_title
        except Exception:
            pass
        hinted_cover = ""
        try:
            hinted_cover = await fetch_toonily_cover_url(state, hinted_url)
        except Exception:
            hinted_cover = ""
        return [
            {
                "title": hinted_title,
                "url": hinted_url,
                "latest": "",
                "cover": hinted_cover,
            }
        ]

    query_urls: list[str] = []
    slug = slugify_keyword(keyword)
    if slug:
        query_urls.append(f"https://toonily.com/search/{slug}")
    query_urls.append(f"https://toonily.com/?s={quote_plus(keyword)}")
    query_urls.append(f"https://toonily.com/?s={quote_plus(keyword)}&post_type=wp-manga")

    merged: list[dict[str, Any]] = []
    seen: set[str] = set()

    for url in query_urls:
        try:
            html = await fetch_html_with_downloader(state, url)
        except Exception:
            continue
        for item in parse_search_results(html):
            key = normalize_url(item.get("url", ""))
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(item)
        if not merged:
            hinted_series_url = extract_series_url_hint(html)
            if hinted_series_url and hinted_series_url not in seen:
                hinted_title = hinted_series_url.rstrip("/").split("/")[-1].replace("-", " ")
                try:
                    snapshot_title, _ = await fetch_series_snapshot_toonily(state, hinted_series_url)
                    if snapshot_title:
                        hinted_title = snapshot_title
                except Exception:
                    pass
                hinted_cover = ""
                try:
                    hinted_cover = await fetch_toonily_cover_url(state, hinted_series_url)
                except Exception:
                    hinted_cover = ""
                merged.append(
                    {
                        "title": hinted_title,
                        "url": hinted_series_url,
                        "latest": "",
                        "cover": hinted_cover,
                    }
                )
                seen.add(hinted_series_url)
        if merged:
            break

    if (
        not merged
        and keyword.startswith(("http://", "https://"))
        and ("/serie/" in keyword or "/series/" in keyword)
    ):
        hinted_url = normalize_url(keyword)
        hinted_cover = ""
        try:
            hinted_cover = await fetch_toonily_cover_url(state, hinted_url)
        except Exception:
            hinted_cover = ""
        merged.append(
            {
                "title": keyword.rstrip("/").split("/")[-1].replace("-", " "),
                "url": hinted_url,
                "latest": "",
                "cover": hinted_cover,
            }
        )

    return merged[:40]


async def fetch_series_snapshot_toonily(
    state: UIState,
    series_url: str,
    logger: Optional[callable] = None,
) -> tuple[str, list[Chapter]]:
    downloader = ToonilyAsyncDownloader(
        series_url=series_url,
        output_dir=state.output_dir,
        chapter_selector="all",
        chapter_concurrency=1,
        image_concurrency=max(2, min(state.image_concurrency, 5)),
        retries=state.retries,
        timeout=state.timeout,
        write_failed_file=False,
        logger=logger,
        cache_enabled=redis_cache_enabled_for_state(state),
        redis_host=state.redis_host,
        redis_port=state.redis_port,
        redis_db=state.redis_db,
        redis_username=state.redis_username,
        redis_password=state.redis_password,
        cache_ttl_seconds=state.cache_ttl_seconds,
        retry_base_delay_seconds=state.retry_base_delay_seconds,
        retry_recoverable_only=state.retry_recoverable_only,
    )
    try:
        return await downloader.get_series_details()
    finally:
        await downloader.close()


async def search_by_provider(state: UIState, provider_id: str, keyword: str) -> list[dict[str, Any]]:
    provider = get_provider(provider_id)
    if not provider_enabled_for_state(state, provider):
        return []
    results = await provider.search(state, keyword)
    normalized_results: list[dict[str, Any]] = []
    for item in results:
        row = dict(item)
        row["provider_id"] = provider.provider_id
        normalized_results.append(row)
    return normalized_results


async def fetch_series_snapshot(
    state: UIState,
    provider_id: str,
    series_url: str,
    logger: Optional[callable] = None,
) -> tuple[str, list[Chapter]]:
    provider = get_provider(provider_id)
    reason = provider_disabled_reason(state, provider)
    if reason:
        raise RuntimeError(reason)
    return await provider.fetch_series_snapshot(state, series_url, logger=logger)


def compute_pending_chapters(book: dict[str, Any], chapters: list[Chapter]) -> list[Chapter]:
    if not chapters:
        return []

    baseline_url = normalize_url(book.get("last_downloaded_chapter_url", ""))
    baseline_num = parse_float(book.get("last_downloaded_chapter_number"))

    if baseline_url:
        for idx, chapter in enumerate(chapters):
            if normalize_url(chapter.url) == baseline_url:
                return chapters[idx + 1 :]

    if baseline_num is not None:
        pending = [chapter for chapter in chapters if chapter.number is not None and chapter.number > baseline_num]
        if pending:
            return pending

    if baseline_url or baseline_num is not None:
        return chapters

    return chapters


def set_site_latest_fields(book: dict[str, Any], chapters: list[Chapter]) -> None:
    if not chapters:
        return
    latest = chapters[-1]
    book["latest_site_chapter_number"] = latest.number
    book["latest_site_chapter_title"] = latest.title
    book["latest_site_chapter_url"] = normalize_url(latest.url)


async def refresh_book_snapshot(
    state: UIState,
    book: dict[str, Any],
    *,
    logger: Optional[callable] = None,
) -> list[Chapter]:
    provider_id = str(book.get("provider_id") or DEFAULT_PROVIDER_ID)
    title, chapters = await fetch_series_snapshot(state, provider_id, book["series_url"], logger=logger)
    if title:
        book["title"] = title
    set_site_latest_fields(book, chapters)
    pending = compute_pending_chapters(book, chapters)
    book["pending_update_count"] = len(pending)
    if provider_id == "toonily" and not str(book.get("cover") or "").strip():
        try:
            cover = await fetch_toonily_cover_url(state, book["series_url"], logger=logger)
            if cover:
                book["cover"] = cover
        except Exception as exc:
            if logger:
                logger(f"[WARN] 获取封面失败：{exc}")
    book["last_checked_at"] = now_iso()
    return pending


def pick_latest_report_chapter(report: DownloadReport) -> tuple[Optional[str], Optional[str], Optional[float]]:
    completed = [item for item in report.chapter_results if item.saved_images > 0]
    if not completed:
        return None, None, None

    by_num = [item for item in completed if item.number is not None]
    if by_num:
        item = max(by_num, key=lambda x: x.number or 0)
    else:
        item = completed[-1]
    return item.url, item.title, item.number


def build_job_history_item(
    *,
    job: dict[str, Any],
    report: Optional[DownloadReport],
) -> dict[str, Any]:
    started = parse_iso_datetime(str(job.get("started_at", "")))
    finished = parse_iso_datetime(str(job.get("finished_at", "")))
    duration_seconds = 0.0
    if started is not None and finished is not None:
        duration_seconds = max(0.0, (finished - started).total_seconds())
    speed_kbps = 0.0
    if report is not None:
        elapsed = max(0.0, (report.finished_at - report.started_at).total_seconds())
        if elapsed > 0:
            speed_kbps = round((max(0, int(report.downloaded_bytes or 0)) / elapsed) / 1024, 2)
    return {
        "job_id": str(job.get("id") or ""),
        "title": str(job.get("title") or ""),
        "provider_id": str(job.get("provider_id") or DEFAULT_PROVIDER_ID),
        "status": str(job.get("status") or ""),
        "status_text": status_text(str(job.get("status") or "")),
        "finished_at": str(job.get("finished_at") or now_iso()),
        "duration_seconds": round(duration_seconds, 2),
        "done_chapters": int(job.get("done_chapters", 0)),
        "successful_chapters": int(job.get("successful_chapters", 0)),
        "failed_chapters": int(job.get("failed_chapters", 0)),
        "saved_images": int(job.get("saved_images", 0)),
        "error": str(job.get("error") or ""),
        "speed_kbps": speed_kbps,
    }


def summarize_recent_history(state: UIState, *, hours: int = 24) -> dict[str, Any]:
    cutoff = datetime.now() - timedelta(hours=max(1, hours))
    recent_items: list[dict[str, Any]] = []
    for item in state.job_history:
        finished = parse_iso_datetime(str(item.get("finished_at", "")))
        if finished is None or finished < cutoff:
            continue
        recent_items.append(item)

    total = len(recent_items)
    success = sum(1 for item in recent_items if str(item.get("status")) == "completed")
    failed = sum(1 for item in recent_items if str(item.get("status")) == "failed")
    cancelled = sum(1 for item in recent_items if str(item.get("status")) == "cancelled")
    avg_speed_values = [float(item.get("speed_kbps", 0.0) or 0.0) for item in recent_items if float(item.get("speed_kbps", 0.0) or 0.0) > 0]
    avg_speed = round(sum(avg_speed_values) / len(avg_speed_values), 2) if avg_speed_values else 0.0
    success_rate = round((success * 100.0 / total), 1) if total else 0.0

    reason_counts: dict[str, int] = {}
    for item in recent_items:
        if str(item.get("status") or "") != "failed":
            continue
        reason = str(item.get("error") or "unknown").strip() or "unknown"
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    top_reasons = sorted(reason_counts.items(), key=lambda pair: pair[1], reverse=True)[:6]

    latest_items = sorted(
        recent_items,
        key=lambda row: str(row.get("finished_at", "")),
        reverse=True,
    )[:20]
    return {
        "hours": max(1, hours),
        "total": total,
        "success": success,
        "failed": failed,
        "cancelled": cancelled,
        "success_rate": success_rate,
        "avg_speed_kbps": avg_speed,
        "top_reasons": top_reasons,
        "latest_items": latest_items,
    }


async def push_job_webhook(state: UIState, job: dict[str, Any], report: Optional[DownloadReport]) -> None:
    status = str(job.get("status") or "")
    if not state.webhook_enabled or not state.webhook_url or not state.webhook_event_enabled(status):
        return

    payload = {
        "event": f"job.{status}",
        "job_id": str(job.get("id") or ""),
        "title": str(job.get("title") or ""),
        "provider_id": str(job.get("provider_id") or DEFAULT_PROVIDER_ID),
        "provider_name": provider_name(str(job.get("provider_id") or DEFAULT_PROVIDER_ID)),
        "status": status,
        "status_text": status_text(status),
        "series_url": str(job.get("series_url") or ""),
        "mode": str(job.get("mode") or ""),
        "book_id": str(job.get("book_id") or ""),
        "finished_at": str(job.get("finished_at") or ""),
        "done_chapters": int(job.get("done_chapters", 0)),
        "successful_chapters": int(job.get("successful_chapters", 0)),
        "failed_chapters": int(job.get("failed_chapters", 0)),
        "saved_images": int(job.get("saved_images", 0)),
        "error": str(job.get("error") or ""),
    }
    if report is not None:
        payload["downloaded_bytes"] = max(0, int(report.downloaded_bytes or 0))
        payload["failure_reasons"] = dict(report.failure_reasons or {})

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "comic-downloader-webhook/1.0",
    }
    token = str(state.webhook_token or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    timeout = ClientTimeout(total=float(state.webhook_timeout_seconds))
    try:
        async with ClientSession(timeout=timeout) as session:
            async with session.post(state.webhook_url, json=payload, headers=headers) as response:
                if response.status >= 400:
                    body = (await response.text())[:180]
                    state.append_job_log(job, f"Webhook 推送失败：HTTP {response.status} {body}")
                else:
                    state.append_job_log(job, f"Webhook 推送成功：HTTP {response.status}")
    except Exception as exc:
        state.append_job_log(job, f"Webhook 推送异常：{exc}")


def build_redirect(path: str, **params: Any) -> web.HTTPSeeOther:
    raw_msg = params.pop("msg", None)
    msg = str(raw_msg).strip() if raw_msg is not None else ""
    query: dict[str, str] = {}
    for key, value in params.items():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            query[key] = text
    location = path
    if query:
        location = f"{path}?{urlencode(query)}"
    response = web.HTTPSeeOther(location=location)
    if msg:
        response.set_cookie(
            FLASH_MSG_COOKIE,
            quote(msg, safe=""),
            max_age=180,
            path="/",
            samesite="Lax",
        )
    return response


def extract_urls_from_text(text: str) -> list[str]:
    found = re.findall(r"https?://[^\s\"'<>]+", text or "", flags=re.IGNORECASE)
    urls: list[str] = []
    seen: set[str] = set()
    for item in found:
        url = normalize_url(item)
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def parse_bulk_import_payload(raw_text: str, filename: str = "") -> list[str]:
    text = str(raw_text or "").strip()
    if not text:
        return []

    lower_name = str(filename or "").strip().lower()
    urls: list[str] = []

    if lower_name.endswith(".json") or text.startswith("[") or text.startswith("{"):
        try:
            data = json.loads(text)
            candidates: list[Any] = []
            if isinstance(data, list):
                candidates = data
            elif isinstance(data, dict):
                for key in ("items", "books", "urls", "data"):
                    val = data.get(key)
                    if isinstance(val, list):
                        candidates.extend(val)
                if not candidates:
                    candidates = [data]
            for row in candidates:
                if isinstance(row, str):
                    urls.extend(extract_urls_from_text(row))
                elif isinstance(row, dict):
                    for key in ("url", "series_url", "link", "href"):
                        val = row.get(key)
                        if val:
                            urls.extend(extract_urls_from_text(str(val)))
            if urls:
                return list(dict.fromkeys(urls))
        except Exception:
            pass

    if lower_name.endswith(".csv"):
        reader = csv.reader(StringIO(text))
        for row in reader:
            for cell in row:
                urls.extend(extract_urls_from_text(str(cell)))
        if urls:
            return list(dict.fromkeys(urls))

    urls = extract_urls_from_text(text)
    if urls:
        return urls

    urls = []
    seen: set[str] = set()
    for line in text.splitlines():
        token = line.strip()
        if not token:
            continue
        if token.startswith(("http://", "https://")):
            url = normalize_url(token)
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def guess_title_from_url(url: str) -> str:
    parsed = urlparse(url)
    tail = parsed.path.strip("/").split("/")[-1] if parsed.path else ""
    tail = tail.replace("-", " ").replace("_", " ").strip()
    return tail or parsed.netloc or "未命名漫画"


def detect_provider_id_by_url(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return ""
    host = (parsed.hostname or "").strip().lower()
    path = (parsed.path or "").strip().lower()
    if not host:
        return ""

    if "toonily" in host:
        return "toonily"

    jm_host_tokens = (
        "18comic",
        "jmcomic",
        "jm365",
        "comic18",
    )
    if any(token in host for token in jm_host_tokens):
        return "jmcomic"
    if path.startswith("/album/") or path.startswith("/photo/"):
        return "jmcomic"

    return ""


def first_enabled_provider_id(state: UIState) -> str:
    for provider in list_providers():
        if provider_enabled_for_state(state, provider):
            return provider.provider_id
    return DEFAULT_PROVIDER_ID


def has_active_job_for_book(state: UIState, book_id: str) -> bool:
    if not book_id:
        return False
    for job in state.jobs.values():
        if str(job.get("book_id") or "") != book_id:
            continue
        if job.get("status") in {"queued", "running", "paused", "cancelling"}:
            return True
    return False


async def run_scheduler_cycle(state: UIState) -> tuple[int, int]:
    scanned = 0
    enqueued = 0
    books = [book for book in state.list_books() if bool(book.get("follow_enabled", True))]
    for book in books:
        provider_id = str(book.get("provider_id") or DEFAULT_PROVIDER_ID)
        provider = get_provider(provider_id)
        reason = provider_disabled_reason(state, provider)
        if reason:
            state.mark_provider_health(provider.provider_id, available=False, error=reason)
            continue
        scanned += 1
        try:
            pending = await refresh_book_snapshot(state, book)
            state.mark_provider_health(provider.provider_id, available=True)
            if state.scheduler_auto_download and pending and not has_active_job_for_book(state, str(book.get("id") or "")):
                chapter_urls = [item.url for item in pending]
                job = state.create_job(
                    title=f"计划任务更新：{book['title']} ({len(chapter_urls)} 章)",
                    series_url=book["series_url"],
                    chapter_selector="all",
                    chapter_urls=chapter_urls,
                    mode="scheduled_updates",
                    book_id=str(book.get("id") or ""),
                    provider_id=provider.provider_id,
                )
                state.append_job_log(job, "由计划任务自动创建。")
                enqueued += 1
        except Exception as exc:
            state.mark_provider_health(provider.provider_id, available=False, error=str(exc))
    await state.save_bookshelf()
    dispatch_jobs(state)
    return scanned, enqueued


async def scheduler_loop(app: web.Application) -> None:
    state: UIState = app["state"]
    while True:
        try:
            await asyncio.sleep(5)
            if not state.scheduler_enabled:
                continue
            if state._scheduler_running:
                continue
            next_run_at = parse_iso_datetime(state.scheduler_next_run_at)
            now = datetime.now()
            if next_run_at is not None and now < next_run_at:
                continue

            state._scheduler_running = True
            scanned, enqueued = await run_scheduler_cycle(state)
            state.scheduler_last_run_at = now_iso()
            state.schedule_next_run(immediate=False)
            await state.save_settings()
            print(f"[SCHEDULER] scanned={scanned}, enqueued={enqueued}, next={state.scheduler_next_run_at}", flush=True)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"[SCHEDULER] cycle failed: {exc}", flush=True)
        finally:
            state._scheduler_running = False

def render_layout(
    *,
    title: str,
    active_nav: str,
    body: str,
    script: str = "",
    compact_mode: bool = False,
    manga_view_mode: str = "poster",
) -> str:
    nav_items = [
        ("dashboard", "主页", "/dashboard"),
        ("progress", "进度", "/progress"),
        ("queue", "队列", "/queue"),
        ("bookshelf", "书架", "/bookshelf"),
        ("follow", "追更", "/follow"),
        ("health", "监控", "/health"),
        ("settings", "设置", "/settings"),
    ]
    nav_html_parts = []
    for key, label, href in nav_items:
        cls = "nav-link active" if key == active_nav else "nav-link"
        nav_html_parts.append(f'<a class="{cls}" href="{href}">{escape(label)}</a>')
    nav_html = "\n".join(nav_html_parts)
    theme_bootstrap_script = (
        "<script>"
        "(function(){"
        "try{"
        "var key='comic-ui-theme';"
        "var saved=localStorage.getItem(key);"
        "var prefersDark=window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches;"
        "var theme=(saved==='dark'||saved==='light')?saved:(prefersDark?'dark':'light');"
        "document.documentElement.setAttribute('data-theme',theme);"
        "}catch(_){document.documentElement.setAttribute('data-theme','light');}"
        "})();"
        "</script>"
    )
    theme_toggle_script = (
        "<script>"
        "(function(){"
        "var key='comic-ui-theme';"
        "var btn=document.getElementById('theme-toggle');"
        "if(!btn){return;}"
        "var root=document.documentElement;"
        "function applyTheme(theme){"
        "root.setAttribute('data-theme',theme);"
        "btn.setAttribute('title',theme==='dark'?'当前深色主题，点击切换浅色':'当前浅色主题，点击切换深色');"
        "btn.setAttribute('aria-label',theme==='dark'?'当前深色主题，点击切换浅色':'当前浅色主题，点击切换深色');"
        "}"
        "var saved='';"
        "try{saved=localStorage.getItem(key)||'';}catch(_){saved='';}"
        "var prefersDark=window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches;"
        "var current=(saved==='dark'||saved==='light')?saved:(prefersDark?'dark':'light');"
        "applyTheme(current);"
        "btn.addEventListener('click',function(){"
        "current=root.getAttribute('data-theme')==='dark'?'light':'dark';"
        "applyTheme(current);"
        "try{localStorage.setItem(key,current);}catch(_){ }"
        "});"
        "})();"
        "</script>"
    )

    return (
        "<!doctype html>\n"
        f"<html lang=\"zh-CN\" data-compact=\"{'1' if compact_mode else '0'}\" "
        f"data-view-mode=\"{escape(manga_view_mode if manga_view_mode in {'poster', 'list'} else 'poster')}\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        f"  <title>{escape(title)}</title>\n"
        "  <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\n"
        "  <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>\n"
        "  <link href=\"https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Noto+Sans+SC:wght@400;500;700;900&display=swap\" rel=\"stylesheet\">\n"
        f"  {theme_bootstrap_script}\n"
        "  <style>\n"
        "    :root {\n"
        "      --primary: #0f6fff;\n"
        "      --primary-light: #287df8;\n"
        "      --secondary: #0ea5a0;\n"
        "      --accent: #0ea5a0;\n"
        "      --warning: #f59e0b;\n"
        "      --danger: #dc2626;\n"
        "      --text: #1f2937;\n"
        "      --muted: #667085;\n"
        "      --bg-dark: #f6f7fb;\n"
        "      --bg-darker: #eef2f8;\n"
        "      --panel: #ffffff;\n"
        "      --panel-border: #dfe4ee;\n"
        "      --glass: #f5f8ff;\n"
        "      --glass-hover: #edf2ff;\n"
        "      --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);\n"
        "    }\n"
        "    * { box-sizing: border-box; }\n"
        "    body {\n"
        "      margin: 0;\n"
        "      color: var(--text);\n"
        "      font-family: 'Manrope', 'Noto Sans SC', 'PingFang SC', 'Microsoft YaHei', sans-serif;\n"
        "      background: radial-gradient(1200px 520px at 8% -10%, #e7efff 0%, transparent 60%), radial-gradient(1000px 460px at 95% -12%, #d8fbf6 0%, transparent 62%), linear-gradient(180deg, var(--bg-dark) 0%, #f9fbff 100%);\n"
        "      min-height: 100vh;\n"
        "      overflow-x: hidden;\n"
        "    }\n"
        "    .bg-effects { display: none; }\n"
        "    .grid-pattern { display: none; }\n"
        "    @keyframes float {\n"
        "      0%,100% { transform: translate(0,0) scale(1); }\n"
        "      25% { transform: translate(36px, -42px) scale(1.05); }\n"
        "      50% { transform: translate(-24px, 34px) scale(0.95); }\n"
        "      75% { transform: translate(18px, 44px) scale(1.02); }\n"
        "    }\n"
        "    .shell { width: min(1320px, calc(100% - 24px)); margin: 14px auto 30px; }\n"
        "    .top {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: space-between;\n"
        "      gap: 12px;\n"
        "      margin-bottom: 14px;\n"
        "      position: sticky;\n"
        "      top: 10px;\n"
        "      z-index: 20;\n"
        "      background: rgba(255,255,255,0.92);\n"
        "      border: 1px solid var(--panel-border);\n"
        "      border-radius: 16px;\n"
        "      backdrop-filter: blur(8px);\n"
        "      box-shadow: var(--shadow);\n"
        "      padding: 12px 14px;\n"
        "    }\n"
        "    .logo { font-size: 28px; font-weight: 900; letter-spacing: 0.4px; color: #0b1324; font-family: 'Noto Sans SC', sans-serif; }\n"
        "    .top-actions {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: flex-end;\n"
        "      gap: 8px;\n"
        "      flex-wrap: wrap;\n"
        "      margin: 0;\n"
        "    }\n"
        "    .nav { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin: 0; padding: 0; }\n"
        "    .nav-link {\n"
        "      text-decoration: none;\n"
        "      color: #334155;\n"
        "      font-size: 14px;\n"
        "      padding: 9px 12px;\n"
        "      border-radius: 11px;\n"
        "      border: 1px solid transparent;\n"
        "      transition: all 0.15s ease;\n"
        "      background: transparent;\n"
        "      font-weight: 700;\n"
        "    }\n"
        "    .nav-link:hover { color: #0b1324; border-color: #d8e2f4; background: #f1f5ff; transform: translateY(-1px); }\n"
        "    .nav-link.active {\n"
        "      color: #fff;\n"
        "      font-weight: 800;\n"
        "      background: linear-gradient(135deg, var(--primary), var(--primary-light));\n"
        "      border-color: var(--primary);\n"
        "      box-shadow: 0 8px 18px rgba(15, 111, 255, 0.35);\n"
        "    }\n"
        "    .theme-toggle {\n"
        "      min-width: 36px;\n"
        "      justify-content: center;\n"
        "      font-weight: 700;\n"
        "      min-height: 36px;\n"
        "      padding: 8px;\n"
        "      line-height: 1.2;\n"
        "      align-self: center;\n"
        "      position: relative;\n"
        "      top: -1px;\n"
        "      margin: 0;\n"
        "    }\n"
        "    .theme-toggle .btn-icon { width: 16px; height: 16px; }\n"
        "    .panel {\n"
        "      background: var(--panel);\n"
        "      border: 1px solid var(--panel-border);\n"
        "      border-radius: 16px;\n"
        "      box-shadow: var(--shadow);\n"
        "      padding: 16px;\n"
        "      margin-bottom: 14px;\n"
        "    }\n"
        "    .title { margin: 0 0 10px; font-size: 20px; font-weight: 900; letter-spacing: 0.2px; }\n"
        "    .subtle { color: var(--muted); font-size: 13px; line-height: 1.5; }\n"
        "    .msg {\n"
        "      position: fixed;\n"
        "      top: 12px;\n"
        "      left: 50%;\n"
        "      transform: translate(-50%, -10px);\n"
        "      z-index: 9999;\n"
        "      width: min(760px, calc(100% - 20px));\n"
        "      border-radius: 12px;\n"
        "      background: #f0f6ff;\n"
        "      border: 1px solid #b4d0ff;\n"
        "      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.16);\n"
        "      color: #0b3f9a;\n"
        "      padding: 10px 12px;\n"
        "      font-size: 14px;\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      gap: 8px;\n"
        "      opacity: 0;\n"
        "      pointer-events: none;\n"
        "      transition: opacity 0.2s ease, transform 0.2s ease;\n"
        "    }\n"
        "    .msg.show {\n"
        "      opacity: 1;\n"
        "      transform: translate(-50%, 0);\n"
        "      pointer-events: auto;\n"
        "    }\n"
        "    .msg.hide {\n"
        "      opacity: 0;\n"
        "      transform: translate(-50%, -10px);\n"
        "      pointer-events: none;\n"
        "    }\n"
        "    .msg-text {\n"
        "      flex: 1;\n"
        "      min-width: 0;\n"
        "    }\n"
        "    .msg-close {\n"
        "      border: 0;\n"
        "      background: transparent;\n"
        "      color: #0b3f9a;\n"
        "      font-size: 18px;\n"
        "      line-height: 1;\n"
        "      cursor: pointer;\n"
        "      padding: 0 2px;\n"
        "      opacity: 0.85;\n"
        "    }\n"
        "    .msg-close:hover { opacity: 1; }\n"
        "    .split-grid { display: grid; grid-template-columns: 1.2fr 1fr; gap: 12px; }\n"
        "    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 10px; }\n"
        "    .result-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));\n"
        "      gap: 10px;\n"
        "      align-items: stretch;\n"
        "    }\n"
        "    .search-form { display: grid; grid-template-columns: minmax(220px, 1fr) 220px 140px auto; gap: 10px; align-items: end; }\n"
        "    .input,\n"
        "    .select {\n"
        "      background: #fff;\n"
        "      color: var(--text);\n"
        "      border: 1px solid #d6deec;\n"
        "      border-radius: 11px;\n"
        "      padding: 9px 11px;\n"
        "      min-height: 40px;\n"
        "      width: 100%;\n"
        "      transition: border-color 0.15s ease, box-shadow 0.15s ease;\n"
        "    }\n"
        "    .input:focus,\n"
        "    .select:focus { outline: none; border-color: #8ab3ff; box-shadow: 0 0 0 3px rgba(15, 111, 255, 0.14); }\n"
        "    .input::placeholder { color: #94a3b8; }\n"
        "    .btn {\n"
        "      border: 1px solid var(--primary);\n"
        "      border-radius: 11px;\n"
        "      padding: 9px 12px;\n"
        "      min-height: 38px;\n"
        "      cursor: pointer;\n"
        "      color: #fff;\n"
        "      font-weight: 800;\n"
        "      font-size: 13px;\n"
        "      background: linear-gradient(135deg, var(--primary), var(--primary-light));\n"
        "      transition: all 0.16s ease;\n"
        "      text-decoration: none;\n"
        "      display: inline-flex;\n"
        "      align-items: center;\n"
        "      justify-content: center;\n"
        "      white-space: nowrap;\n"
        "    }\n"
        "    .btn:hover { transform: translateY(-1px); filter: brightness(1.02); box-shadow: 0 8px 16px rgba(15, 111, 255, 0.26); }\n"
        "    .btn.secondary {\n"
        "      border-color: #0e8f8a;\n"
        "      background: linear-gradient(135deg, #0ea5a0, #139c8e);\n"
        "    }\n"
        "    .btn.ghost {\n"
        "      color: #334155;\n"
        "      border: 1px solid #c8d4ea;\n"
        "      background: #fff;\n"
        "      box-shadow: none;\n"
        "    }\n"
        "    .btn.warn { border-color: var(--danger); background: linear-gradient(135deg, var(--danger), #ef4444); color: #fff; }\n"
        "    .btn[disabled] { opacity: 0.52; cursor: not-allowed; transform: none; }\n"
        "    .icon-btn {\n"
        "      min-height: 34px;\n"
        "      padding: 8px 10px;\n"
        "      border-radius: 10px;\n"
        "      justify-content: center;\n"
        "      gap: 6px;\n"
        "    }\n"
        "    .btn-icon {\n"
        "      width: 14px;\n"
        "      height: 14px;\n"
        "      display: inline-block;\n"
        "      flex: 0 0 auto;\n"
        "    }\n"
        "    .btn-text {\n"
        "      display: inline-block;\n"
        "      line-height: 1;\n"
        "    }\n"
        "    .icon-btn .btn-text {\n"
        "      display: inline-block;\n"
        "      max-width: 96px;\n"
        "      overflow: hidden;\n"
        "      text-overflow: ellipsis;\n"
        "      white-space: nowrap;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    .actions .icon-btn,\n"
        "    .book-actions .icon-btn,\n"
        "    .job-actions .icon-btn,\n"
        "    html[data-view-mode='list'] .book-card > .book-actions .icon-btn,\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions .icon-btn {\n"
        "      min-width: 72px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "      gap: 4px;\n"
        "    }\n"
        "    .actions .icon-btn .btn-text,\n"
        "    .book-actions .icon-btn .btn-text,\n"
        "    .job-actions .icon-btn .btn-text,\n"
        "    html[data-view-mode='list'] .book-card > .book-actions .icon-btn .btn-text,\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions .icon-btn .btn-text {\n"
        "      display: inline-block;\n"
        "      max-width: 74px;\n"
        "    }\n"
        "    html[data-compact='1'] .subtle { font-size: 12px; }\n"
        "    .result-card {\n"
        "      border-radius: 14px;\n"
        "      border: 1px solid #dbe3f0;\n"
        "      background: linear-gradient(180deg, #ffffff 0%, #fbfcff 100%);\n"
        "      padding: 8px;\n"
        "      display: flex;\n"
        "      flex-direction: column;\n"
        "      gap: 6px;\n"
        "      min-height: 360px;\n"
        "      height: 100%;\n"
        "    }\n"
        "    .result-cover-wrap {\n"
        "      width: 88%;\n"
        "      margin: 0 auto;\n"
        "      aspect-ratio: 3 / 4;\n"
        "      border-radius: 10px;\n"
        "      max-height: 240px;\n"
        "      overflow: hidden;\n"
        "      background: #edf1f8;\n"
        "      border: 1px solid #dce4f2;\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: center;\n"
        "    }\n"
        "    .result-cover {\n"
        "      width: 100%;\n"
        "      height: 100%;\n"
        "      object-fit: cover;\n"
        "      display: block;\n"
        "    }\n"
        "    .result-cover-empty {\n"
        "      color: #94a3b8;\n"
        "      font-size: 12px;\n"
        "      letter-spacing: 0.2px;\n"
        "    }\n"
        "    .result-title {\n"
        "      font-size: 14px;\n"
        "      font-weight: 800;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 2);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 2;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .link { color: #0f6fff; text-decoration: none; font-size: 12px; }\n"
        "    .result-link {\n"
        "      word-break: break-all;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 2);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 2;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .result-latest {\n"
        "      white-space: nowrap;\n"
        "      overflow: hidden;\n"
        "      text-overflow: ellipsis;\n"
        "      min-height: 1.4em;\n"
        "    }\n"
        "    .link:hover { text-decoration: underline; }\n"
        "    .actions { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; margin-top: auto; }\n"
        "    .actions form { margin: 0; min-width: 0; }\n"
        "    .actions .btn { width: 100%; }\n"
        "    .job-actions {\n"
        "      grid-template-columns: repeat(3, minmax(0, 1fr));\n"
        "      gap: 6px;\n"
        "      margin-top: 10px;\n"
        "    }\n"
        "    .job-meta { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 10px; color: var(--muted); }\n"
        "    .badge {\n"
        "      display: inline-block;\n"
        "      padding: 3px 10px;\n"
        "      border-radius: 999px;\n"
        "      border: 1px solid #cad6ef;\n"
        "      font-size: 12px;\n"
        "      color: #1555c0;\n"
        "      background: #edf4ff;\n"
        "    }\n"
        "    .progress {\n"
        "      width: 100%;\n"
        "      height: 10px;\n"
        "      border-radius: 999px;\n"
        "      background: #e6edf8;\n"
        "      overflow: hidden;\n"
        "      margin: 6px 0 12px;\n"
        "    }\n"
        "    .bar {\n"
        "      height: 100%;\n"
        "      width: 0%;\n"
        "      background: linear-gradient(90deg, var(--primary), var(--accent));\n"
        "      transition: width 0.3s ease;\n"
        "    }\n"
        "    .log-box {\n"
        "      height: 260px;\n"
        "      border-radius: 12px;\n"
        "      border: 1px solid #dce4f2;\n"
        "      background: #f7f9fd;\n"
        "      color: #1f2937;\n"
        "      padding: 10px;\n"
        "      overflow: auto;\n"
        "      font-family: Consolas, \"Courier New\", monospace;\n"
        "      font-size: 12px;\n"
        "      white-space: pre-wrap;\n"
        "      line-height: 1.45;\n"
        "    }\n"
        "    .book-card {\n"
        "      padding: 8px;\n"
        "      border: 1px solid #dbe3f0;\n"
        "      border-radius: 14px;\n"
        "      background: linear-gradient(180deg, #ffffff 0%, #fbfcff 100%);\n"
        "      display: flex;\n"
        "      flex-direction: column;\n"
        "      gap: 6px;\n"
        "      height: 100%;\n"
        "      min-height: 360px;\n"
        "    }\n"
        "    .bookshelf-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));\n"
        "      gap: 10px;\n"
        "      align-items: stretch;\n"
        "    }\n"
        "    .book-card .result-cover-wrap {\n"
        "      width: 88%;\n"
        "      margin: 0 auto 6px;\n"
        "    }\n"
        "    .book-title {\n"
        "      margin: 0;\n"
        "      font-size: 14px;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 2);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 2;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .book-meta-list { display: flex; flex-direction: column; gap: 4px; }\n"
        "    .book-meta { font-size: 12px; color: var(--muted); margin: 0; line-height: 1.4; }\n"
        "    .book-meta.clamp-1 { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n"
        "    .book-actions {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(3, minmax(0, 1fr));\n"
        "      align-items: center;\n"
        "      gap: 4px;\n"
        "      margin-top: auto;\n"
        "    }\n"
        "    .book-actions form {\n"
        "      margin: 0;\n"
        "      min-width: 0;\n"
        "      width: 100%;\n"
        "    }\n"
        "    .book-actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 72px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    .book-actions .btn-icon {\n"
        "      width: 12px;\n"
        "      height: 12px;\n"
        "    }\n"
        "    .follow-page .follow-toolbar {\n"
        "      display: flex;\n"
        "      justify-content: space-between;\n"
        "      align-items: flex-start;\n"
        "      gap: 8px;\n"
        "      flex-wrap: wrap;\n"
        "      margin: 8px 0 12px;\n"
        "    }\n"
        "    .follow-page .follow-toolbar-left,\n"
        "    .follow-page .follow-toolbar-right {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      gap: 6px;\n"
        "      flex-wrap: wrap;\n"
        "    }\n"
        "    .follow-page .book-actions {\n"
        "      grid-template-columns: repeat(2, minmax(0, 1fr));\n"
        "      gap: 6px;\n"
        "    }\n"
        "    .follow-page .book-actions .btn {\n"
        "      min-width: 0;\n"
        "      font-size: 12px;\n"
        "      padding: 6px;\n"
        "    }\n"
        "    .follow-page .book-actions .btn-text {\n"
        "      max-width: 56px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-grid,\n"
        "    html[data-view-mode='list'] .bookshelf-grid {\n"
        "      grid-template-columns: 1fr;\n"
        "      gap: 10px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card {\n"
        "      display: grid;\n"
        "      grid-template-columns: 78px minmax(0, 1fr) auto;\n"
        "      grid-template-areas:\n"
        "        'cover title actions'\n"
        "        'cover provider actions'\n"
        "        'cover link actions'\n"
        "        'cover latest actions';\n"
        "      column-gap: 12px;\n"
        "      row-gap: 3px;\n"
        "      min-height: 0;\n"
        "      align-items: start;\n"
        "      padding: 10px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-cover-wrap {\n"
        "      grid-area: cover;\n"
        "      width: 78px;\n"
        "      max-height: 104px;\n"
        "      margin: 0;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-title {\n"
        "      grid-area: title;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 2;\n"
        "      font-size: 14px;\n"
        "      margin-top: 1px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > div:not(.result-cover-wrap):not(.result-title):not(.result-link):not(.result-latest):not(.actions) {\n"
        "      grid-area: provider;\n"
        "      margin-top: 1px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-link {\n"
        "      grid-area: link;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 1;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-latest {\n"
        "      grid-area: latest;\n"
        "      min-height: 0;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .actions {\n"
        "      grid-area: actions;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: 1fr;\n"
        "      min-width: 88px;\n"
        "      justify-content: center;\n"
        "      align-content: center;\n"
        "      gap: 5px;\n"
        "      align-self: center;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 88px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card {\n"
        "      display: grid;\n"
        "      grid-template-columns: 78px minmax(0, 1fr) auto;\n"
        "      grid-template-areas:\n"
        "        'cover select actions'\n"
        "        'cover title actions'\n"
        "        'cover group actions'\n"
        "        'cover metas actions';\n"
        "      column-gap: 12px;\n"
        "      row-gap: 3px;\n"
        "      min-height: 0;\n"
        "      align-items: start;\n"
        "      padding: 10px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .result-cover-wrap {\n"
        "      grid-area: cover;\n"
        "      width: 78px;\n"
        "      max-height: 104px;\n"
        "      margin: 0;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > label {\n"
        "      grid-area: select;\n"
        "      margin: 0;\n"
        "      gap: 6px;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-title {\n"
        "      grid-area: title;\n"
        "      margin: 0;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 2;\n"
        "      font-size: 14px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-meta {\n"
        "      grid-area: group;\n"
        "      margin: 0;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-meta-list {\n"
        "      grid-area: metas;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(2, minmax(0, 1fr));\n"
        "      column-gap: 10px;\n"
        "      row-gap: 2px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-actions {\n"
        "      grid-area: actions;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: 1fr;\n"
        "      min-width: 88px;\n"
        "      justify-content: center;\n"
        "      align-content: center;\n"
        "      gap: 5px;\n"
        "      align-self: center;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 88px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card {\n"
        "      grid-template-columns: 78px minmax(0, 1fr) minmax(132px, 34%);\n"
        "      grid-template-areas:\n"
        "        'cover select actions'\n"
        "        'cover title actions'\n"
        "        'cover metas actions';\n"
        "      column-gap: 12px;\n"
        "      row-gap: 4px;\n"
        "      padding: 10px;\n"
        "      min-height: 0;\n"
        "      align-items: start;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .result-cover-wrap {\n"
        "      grid-area: cover;\n"
        "      width: 78px;\n"
        "      max-height: 104px;\n"
        "      margin: 0;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > label {\n"
        "      grid-area: select;\n"
        "      margin: 0;\n"
        "      gap: 6px;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-title {\n"
        "      grid-area: title;\n"
        "      margin: 0;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 2;\n"
        "      font-size: 14px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-meta-list {\n"
        "      grid-area: metas;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: 1fr;\n"
        "      row-gap: 2px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions {\n"
        "      grid-area: actions;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(2, minmax(0, 1fr));\n"
        "      min-width: 132px;\n"
        "      justify-content: center;\n"
        "      align-content: center;\n"
        "      gap: 4px;\n"
        "      align-self: center;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 0;\n"
        "      min-height: 32px;\n"
        "      padding: 6px;\n"
        "    }\n"
        "    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 10px; }\n"
        "    .stat-card {\n"
        "      border: 1px solid #dbe3f0;\n"
        "      background: #fff;\n"
        "      border-radius: 12px;\n"
        "      padding: 10px;\n"
        "    }\n"
        "    .stat-label { color: var(--muted); font-size: 12px; margin-bottom: 4px; }\n"
        "    .stat-value { font-size: 18px; font-weight: 800; }\n"
        "    .settings-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));\n"
        "      gap: 10px;\n"
        "    }\n"
        "    .settings-section {\n"
        "      border: 1px solid #dbe3f0;\n"
        "      border-radius: 12px;\n"
        "      padding: 12px;\n"
        "      margin-bottom: 12px;\n"
        "      background: #f8fafc;\n"
        "    }\n"
        "    .settings-title {\n"
        "      margin: 0 0 8px;\n"
        "      font-size: 15px;\n"
        "      font-weight: 700;\n"
        "    }\n"
        "    .site-badge {\n"
        "      display: inline-flex;\n"
        "      align-items: center;\n"
        "      gap: 6px;\n"
        "      font-size: 12px;\n"
        "      line-height: 1;\n"
        "      padding: 4px 8px;\n"
        "      border-radius: 999px;\n"
        "      border: 1px solid #d5deee;\n"
        "      background: #f7faff;\n"
        "      color: #1e3a8a;\n"
        "    }\n"
        "    .site-badge.toonily { color: #a16207; border-color: #f3ddb0; background: #fff7e7; }\n"
        "    .site-badge.jmcomic { color: #0f766e; border-color: #b5eee6; background: #ecfdf9; }\n"
        "    .site-icon { width: 14px; height: 14px; display: inline-block; }\n"
        "    .pager {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: space-between;\n"
        "      gap: 10px;\n"
        "      flex-wrap: wrap;\n"
        "      margin-bottom: 10px;\n"
        "    }\n"
        "    .pager .subtle { margin: 0; }\n"
        "    .pager-form {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      gap: 8px;\n"
        "      flex-wrap: wrap;\n"
        "      margin: 0;\n"
        "    }\n"
        "    .pager-links { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }\n"
        "    label { display: block; font-size: 13px; margin-bottom: 5px; color: #c5d7ea; }\n"
        "    html[data-theme='dark'] {\n"
        "      color-scheme: dark;\n"
        "      --text: #e3ebf7;\n"
        "      --muted: #9cb0cc;\n"
        "      --bg-dark: #0b1220;\n"
        "      --bg-darker: #0f1a2d;\n"
        "      --panel: #101a2d;\n"
        "      --panel-border: #24364f;\n"
        "      --shadow: 0 12px 28px rgba(0, 0, 0, 0.35);\n"
        "      --primary: #4e9dff;\n"
        "      --primary-light: #3f87df;\n"
        "      --secondary: #13b9a4;\n"
        "      --accent: #13b9a4;\n"
        "    }\n"
        "    html[data-theme='dark'] body {\n"
        "      background: radial-gradient(1200px 520px at 8% -10%, #1a2a45 0%, transparent 60%), radial-gradient(1000px 460px at 95% -12%, #10343a 0%, transparent 62%), linear-gradient(180deg, #0b1220 0%, #0f1a2d 100%);\n"
        "    }\n"
        "    html[data-theme='dark'] .top {\n"
        "      background: rgba(16, 26, 45, 0.92);\n"
        "      border-color: #2a3d59;\n"
        "    }\n"
        "    html[data-theme='dark'] .logo { color: #f0f5ff; }\n"
        "    html[data-theme='dark'] .nav-link {\n"
        "      color: #c0cee3;\n"
        "    }\n"
        "    html[data-theme='dark'] .nav-link:hover {\n"
        "      color: #f0f5ff;\n"
        "      border-color: #324865;\n"
        "      background: #18263d;\n"
        "    }\n"
        "    html[data-theme='dark'] .input,\n"
        "    html[data-theme='dark'] .select {\n"
        "      background: #0f1a2b;\n"
        "      color: #e3ebf7;\n"
        "      border-color: #304463;\n"
        "    }\n"
        "    html[data-theme='dark'] .input::placeholder { color: #7f92ad; }\n"
        "    html[data-theme='dark'] .btn.ghost {\n"
        "      color: #d1deef;\n"
        "      border-color: #39506e;\n"
        "      background: #142239;\n"
        "    }\n"
        "    html[data-theme='dark'] .result-card,\n"
        "    html[data-theme='dark'] .book-card {\n"
        "      border-color: #2a3f5e;\n"
        "      background: linear-gradient(180deg, #111e32 0%, #0f1a2d 100%);\n"
        "    }\n"
        "    html[data-theme='dark'] .result-cover-wrap {\n"
        "      background: #17263d;\n"
        "      border-color: #314969;\n"
        "    }\n"
        "    html[data-theme='dark'] .result-cover-empty { color: #90a3c0; }\n"
        "    html[data-theme='dark'] .badge {\n"
        "      border-color: #334b6b;\n"
        "      color: #a9cbff;\n"
        "      background: #152841;\n"
        "    }\n"
        "    html[data-theme='dark'] .progress { background: #1b2c45; }\n"
        "    html[data-theme='dark'] .log-box {\n"
        "      border-color: #314969;\n"
        "      background: #0d1727;\n"
        "      color: #d6e1f2;\n"
        "    }\n"
        "    html[data-theme='dark'] .stat-card {\n"
        "      border-color: #2a3f5e;\n"
        "      background: #101c2f;\n"
        "    }\n"
        "    html[data-theme='dark'] .settings-section {\n"
        "      border-color: #304463;\n"
        "      background: #0f1a2d;\n"
        "    }\n"
        "    html[data-theme='dark'] .site-badge {\n"
        "      border-color: #324866;\n"
        "      background: #14233a;\n"
        "      color: #b6cef0;\n"
        "    }\n"
        "    html[data-theme='dark'] .site-badge.toonily {\n"
        "      color: #f8d17e;\n"
        "      border-color: #7a6431;\n"
        "      background: #2f2816;\n"
        "    }\n"
        "    html[data-theme='dark'] .site-badge.jmcomic {\n"
        "      color: #6ee0cc;\n"
        "      border-color: #2f6e67;\n"
        "      background: #112a29;\n"
        "    }\n"
        "    @media (max-width: 980px) {\n"
        "      .split-grid { grid-template-columns: 1fr; }\n"
        "      .search-form { grid-template-columns: 1fr 1fr; }\n"
        "    }\n"
        "    @media (max-width: 780px) {\n"
        "      .shell { width: calc(100% - 14px); margin-top: 10px; }\n"
        "      .top { flex-direction: column; align-items: flex-start; }\n"
        "      .top-actions { width: 100%; justify-content: space-between; }\n"
        "      .logo { font-size: 24px; }\n"
        "      .search-form { grid-template-columns: 1fr; }\n"
        "      .result-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; }\n"
        "      .bookshelf-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; }\n"
        "      .result-card,\n"
        "      .book-card { min-height: 330px; }\n"
        "      .actions { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n"
        "      .job-actions { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n"
        "      .follow-page .follow-toolbar { flex-direction: column; align-items: stretch; }\n"
        "      .follow-page .follow-toolbar-left,\n"
        "      .follow-page .follow-toolbar-right { width: 100%; }\n"
        "      .follow-page .book-actions { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n"
        "      .settings-grid { grid-template-columns: 1fr; }\n"
        "    }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        "  <div class=\"shell\">\n"
        "    <div class=\"top\">\n"
        "      <div class=\"logo\">漫画下载</div>\n"
        "      <div class=\"top-actions\">\n"
        f"        <nav class=\"nav\">{nav_html}</nav>\n"
        "        <button class=\"btn ghost icon-btn theme-toggle\" id=\"theme-toggle\" type=\"button\" title=\"切换主题\" aria-label=\"切换主题\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M12 3a1 1 0 0 1 1 1v1a1 1 0 1 1-2 0V4a1 1 0 0 1 1-1zm0 15a4 4 0 1 1 0-8 4 4 0 0 1 0 8zm8-5a1 1 0 0 1 1 1 1 1 0 0 1-1 1h-1a1 1 0 1 1 0-2zm-14 0a1 1 0 1 1 0 2H5a1 1 0 1 1 0-2zm10.66-6.66a1 1 0 0 1 1.41 0l.7.7a1 1 0 1 1-1.41 1.41l-.7-.7a1 1 0 0 1 0-1.41zM7.34 16.66a1 1 0 0 1 1.41 0l.7.7a1 1 0 1 1-1.41 1.41l-.7-.7a1 1 0 0 1 0-1.41zm11.43 0a1 1 0 0 1 0 1.41l-.7.7a1 1 0 1 1-1.41-1.41l.7-.7a1 1 0 0 1 1.41 0zM8.75 6.34a1 1 0 0 1 0 1.41l-.7.7A1 1 0 0 1 6.64 7.04l.7-.7a1 1 0 0 1 1.41 0z\" fill=\"currentColor\"/>"
        "</svg>"
        "<span class=\"btn-text\">主题</span>"
        "</button>\n"
        "      </div>\n"
        "    </div>\n"
        f"    {body}\n"
        "  </div>\n"
        f"{theme_toggle_script}\n"
        f"{script}\n"
        "</body>\n"
        "</html>\n"
    )


def status_text(status: str) -> str:
    mapping = {
        "queued": "排队中",
        "running": "进行中",
        "paused": "已暂停",
        "cancelling": "取消中",
        "cancelled": "已取消",
        "failed": "失败",
        "completed": "完成",
    }
    return mapping.get(status, status)


def render_message(msg: str) -> str:
    if not msg:
        return ""
    return (
        "<div class=\"msg\" id=\"toast-msg\" role=\"status\" aria-live=\"polite\">"
        f"<span class=\"msg-text\">{escape(msg)}</span>"
        "<button type=\"button\" class=\"msg-close\" aria-label=\"关闭提示\" "
        "onclick=\"(function(btn){const el=btn.closest('.msg');if(!el)return;el.classList.remove('show');el.classList.add('hide');setTimeout(function(){if(el.isConnected){el.remove();}},220);})(this)\">×</button>"
        "</div>"
        "<script>(function(){"
        "const el=document.getElementById('toast-msg');"
        "if(!el)return;"
        "requestAnimationFrame(function(){el.classList.add('show');});"
        "setTimeout(function(){"
        "if(!el.isConnected)return;"
        "el.classList.remove('show');"
        "el.classList.add('hide');"
        "setTimeout(function(){if(el.isConnected){el.remove();}},220);"
        "},2800);"
        "})();</script>"
    )


def render_job_panel(job: dict[str, Any], *, heading: str, full_page: bool = False) -> tuple[str, str]:
    chapter_pct = chapter_percent(job["done_chapters"], job["total_chapters"])
    image_pct = chapter_percent(job["saved_images"], job["total_images"])
    logs = "\n".join(job["logs"])
    retry_file = job.get("retry_file", "")

    progress_jump = (
        ""
        if full_page
        else (
            f"<a class=\"btn ghost icon-btn\" href=\"/progress?job={escape(job['id'])}\" "
            "title=\"打开完整进度界面\" aria-label=\"打开完整进度界面\">"
            "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
            "<path d=\"M4 12s3-6 8-6 8 6 8 6-3 6-8 6-8-6-8-6z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"/>"
            "<circle cx=\"12\" cy=\"12\" r=\"2.5\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"/>"
            "</svg>"
            "<span class=\"btn-text\">打开完整进度界面</span>"
            "</a>"
        )
    )
    back_home = (
        "<a class=\"btn ghost icon-btn\" href=\"/dashboard\" title=\"返回主页\" aria-label=\"返回主页\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M4 11.5 12 5l8 6.5V20h-5v-5H9v5H4z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linejoin=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">返回主页</span>"
        "</a>"
        if full_page
        else ""
    )
    retry_text = f"失败重试清单：{retry_file}" if retry_file else ""
    provider_text = provider_name(str(job.get("provider_id") or DEFAULT_PROVIDER_ID))

    stats_html = ""
    if full_page:
        stats_html = (
            "<div class=\"stat-grid\">"
            "<div class=\"stat-card\"><div class=\"stat-label\">任务状态</div>"
            f"<div class=\"stat-value\" id=\"stat-status\">{escape(status_text(job['status']))}</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">成功章节</div>"
            f"<div class=\"stat-value\" id=\"stat-success\">{int(job.get('successful_chapters', 0))}</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">失败章节</div>"
            f"<div class=\"stat-value\" id=\"stat-failed\">{int(job.get('failed_chapters', 0))}</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">章节完成率</div>"
            f"<div class=\"stat-value\" id=\"stat-chapter-pct\">{chapter_pct}%</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">图片完成率</div>"
            f"<div class=\"stat-value\" id=\"stat-image-pct\">{image_pct}%</div></div>"
            "</div>"
        )

    job_html = (
        "<div class=\"panel\" id=\"job-panel\" "
        f"data-job-id=\"{escape(job['id'])}\">"
        f"<h2 class=\"title\">{escape(heading)}</h2>"
        f"{stats_html}"
        f"<div class=\"job-meta\"><span class=\"badge\" id=\"job-status\">{escape(status_text(job['status']))}</span>"
        f"<span>创建：<span id=\"job-created\">{escape(fmt_time(job['created_at']))}</span></span>"
        f"<span>开始：<span id=\"job-started\">{escape(fmt_time(job['started_at']))}</span></span>"
        f"<span>结束：<span id=\"job-finished\">{escape(fmt_time(job['finished_at']))}</span></span></div>"
        f"<div style=\"font-weight:700;margin-bottom:6px;\">{escape(job['title'])}</div>"
        f"<div class=\"subtle\">站点：{escape(provider_text)}</div>"
        f"<div class=\"subtle\" id=\"job-error\">{escape(job.get('error', ''))}</div>"
        f"<div class=\"subtle\" id=\"retry-file\">{escape(retry_text)}</div>"
        "<div style=\"margin-top:12px;\">章节进度：<span id=\"chapter-text\">"
        f"{job['done_chapters']}/{job['total_chapters']}</span></div>"
        "<div class=\"progress\"><div class=\"bar\" id=\"chapter-bar\" "
        f"style=\"width:{chapter_pct}%;\"></div></div>"
        "<div>图片进度：<span id=\"image-text\">"
        f"{job['saved_images']}/{job['total_images']}</span></div>"
        "<div class=\"progress\"><div class=\"bar\" id=\"image-bar\" "
        f"style=\"width:{image_pct}%;\"></div></div>"
        "<div class=\"actions job-actions\">"
        "<button id=\"btn-pause\" class=\"btn ghost icon-btn\" type=\"button\" title=\"暂停任务\" aria-label=\"暂停任务\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M8 6v12M16 6v12\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">暂停</span>"
        "</button>"
        "<button id=\"btn-resume\" class=\"btn secondary icon-btn\" type=\"button\" title=\"继续任务\" aria-label=\"继续任务\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M8 6l10 6-10 6z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linejoin=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">继续</span>"
        "</button>"
        "<button id=\"btn-cancel\" class=\"btn warn icon-btn\" type=\"button\" title=\"取消任务\" aria-label=\"取消任务\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M6 6l12 12M18 6 6 18\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">取消</span>"
        "</button>"
        f"{progress_jump}"
        f"{back_home}"
        "</div>"
        "<div style=\"margin:10px 0 6px;\" class=\"subtle\">实时日志（自动滚动）</div>"
        f"<pre id=\"log-box\" class=\"log-box\">{escape(logs)}</pre>"
        "</div>"
    )

    script = (
        "<script>\n"
        "(function(){\n"
        "  const panel = document.getElementById('job-panel');\n"
        "  if (!panel) return;\n"
        "  const jobId = panel.dataset.jobId;\n"
        "  const statusEl = document.getElementById('job-status');\n"
        "  const chapterText = document.getElementById('chapter-text');\n"
        "  const imageText = document.getElementById('image-text');\n"
        "  const chapterBar = document.getElementById('chapter-bar');\n"
        "  const imageBar = document.getElementById('image-bar');\n"
        "  const logBox = document.getElementById('log-box');\n"
        "  const errorEl = document.getElementById('job-error');\n"
        "  const retryEl = document.getElementById('retry-file');\n"
        "  const createdEl = document.getElementById('job-created');\n"
        "  const startedEl = document.getElementById('job-started');\n"
        "  const finishedEl = document.getElementById('job-finished');\n"
        "  const statStatusEl = document.getElementById('stat-status');\n"
        "  const statSuccessEl = document.getElementById('stat-success');\n"
        "  const statFailedEl = document.getElementById('stat-failed');\n"
        "  const statChapterPctEl = document.getElementById('stat-chapter-pct');\n"
        "  const statImagePctEl = document.getElementById('stat-image-pct');\n"
        "  const btnPause = document.getElementById('btn-pause');\n"
        "  const btnResume = document.getElementById('btn-resume');\n"
        "  const btnCancel = document.getElementById('btn-cancel');\n"
        "  const finalStates = new Set(['completed', 'failed', 'cancelled']);\n"
        "  let poll = null;\n"
        "  function pct(done, total){ if (!total || total <= 0) return 0; return Math.max(0, Math.min(100, Math.floor(done*100/total))); }\n"
        "  function fmt(v){ if(!v) return '-'; return String(v).replace('T',' '); }\n"
        "  function updateButtons(ctrl){\n"
        "    btnPause.disabled = !ctrl.can_pause;\n"
        "    btnResume.disabled = !ctrl.can_resume;\n"
        "    btnCancel.disabled = !ctrl.can_cancel;\n"
        "  }\n"
        "  async function control(action){\n"
        "    try {\n"
        "      await fetch('/job/' + encodeURIComponent(jobId) + '/' + action, {method:'POST'});\n"
        "      await refresh();\n"
        "    } catch (_) {}\n"
        "  }\n"
        "  btnPause.addEventListener('click', ()=>control('pause'));\n"
        "  btnResume.addEventListener('click', ()=>control('resume'));\n"
        "  btnCancel.addEventListener('click', ()=>control('cancel'));\n"
        "  async function refresh(){\n"
        "    try {\n"
        "      const res = await fetch('/job/' + encodeURIComponent(jobId) + '/state');\n"
        "      if (!res.ok) return;\n"
        "      const data = await res.json();\n"
        "      const cPct = pct(data.done_chapters, data.total_chapters);\n"
        "      const iPct = pct(data.saved_images, data.total_images);\n"
        "      statusEl.textContent = data.status_text;\n"
        "      chapterText.textContent = data.done_chapters + '/' + data.total_chapters;\n"
        "      imageText.textContent = data.saved_images + '/' + data.total_images;\n"
        "      chapterBar.style.width = cPct + '%';\n"
        "      imageBar.style.width = iPct + '%';\n"
        "      errorEl.textContent = data.error || '';\n"
        "      if (createdEl) createdEl.textContent = fmt(data.created_at);\n"
        "      if (startedEl) startedEl.textContent = fmt(data.started_at);\n"
        "      if (finishedEl) finishedEl.textContent = fmt(data.finished_at);\n"
        "      if (retryEl) retryEl.textContent = data.retry_file ? ('失败重试清单：' + data.retry_file) : '';\n"
        "      if (statStatusEl) statStatusEl.textContent = data.status_text;\n"
        "      if (statSuccessEl) statSuccessEl.textContent = String(data.successful_chapters || 0);\n"
        "      if (statFailedEl) statFailedEl.textContent = String(data.failed_chapters || 0);\n"
        "      if (statChapterPctEl) statChapterPctEl.textContent = cPct + '%';\n"
        "      if (statImagePctEl) statImagePctEl.textContent = iPct + '%';\n"
        "      const nextLogs = (data.logs || []).join('\\n');\n"
        "      if (logBox.textContent !== nextLogs){\n"
        "        logBox.textContent = nextLogs;\n"
        "        logBox.scrollTop = logBox.scrollHeight;\n"
        "      }\n"
        "      updateButtons(data.controls || {can_pause:false, can_resume:false, can_cancel:false});\n"
        "      if (finalStates.has(data.status) && poll){\n"
        "        clearInterval(poll);\n"
        "        poll = null;\n"
        "      }\n"
        "    } catch (_) {}\n"
        "  }\n"
        "  refresh();\n"
        "  poll = setInterval(refresh, 2000);\n"
        "})();\n"
        "</script>"
    )
    return job_html, script


def render_dashboard(
    state: UIState,
    msg: str,
    *,
    search_page: int,
    search_page_size: int,
) -> str:
    selected_provider = get_provider(state.last_search_provider)
    if not provider_enabled_for_state(state, selected_provider):
        for candidate in list_providers():
            if provider_enabled_for_state(state, candidate):
                state.last_search_provider = candidate.provider_id
                break

    results: Optional[dict[str, Any]] = None
    if state.last_search_results:
        total_results = len(state.last_search_results)
        page_size = max(4, min(40, int(search_page_size)))
        page_count = max(1, math.ceil(total_results / page_size))
        page = max(1, min(int(search_page), page_count))
        start = (page - 1) * page_size
        end = start + page_size
        page_results = state.last_search_results[start:end]

        def dashboard_page_url(target_page: int) -> str:
            params: dict[str, str] = {"sp": str(target_page), "sps": str(page_size)}
            return f"/dashboard?{urlencode(params)}"

        items: list[dict[str, Any]] = []
        for item in page_results:
            title = str(item.get("title", "") or "")
            url = str(item.get("url", "") or "")
            latest = str(item.get("latest", "") or "") or "-"
            cover = item.get("cover", "")
            provider_id = str(item.get("provider_id") or state.last_search_provider or DEFAULT_PROVIDER_ID)
            provider_badge = render_provider_badge(provider_id)
            cover_url = str(cover).strip()
            if cover_url.startswith("//"):
                cover_url = f"https:{cover_url}"
            elif cover_url.startswith("/"):
                cover_url = urljoin("https://toonily.com", cover_url)
            if cover_url and not cover_url.startswith(("http://", "https://")):
                cover_url = ""

            items.append(
                {
                    "title": title,
                    "url": url,
                    "latest": latest,
                    "cover_url": cover_url,
                    "provider_id": provider_id,
                    "provider_badge_html": provider_badge,
                }
            )

        results = {
            "keyword": state.last_search_query,
            "total": total_results,
            "page": page,
            "page_count": page_count,
            "page_size": page_size,
            "has_prev": page > 1,
            "has_next": page < page_count,
            "prev_url": dashboard_page_url(page - 1),
            "next_url": dashboard_page_url(page + 1),
            "page_size_options": [
                {"value": size, "selected": size == page_size} for size in (8, 12, 16, 24, 40)
            ],
            "items": items,
        }

    provider_options: list[dict[str, Any]] = []
    for provider in list_providers():
        reason = provider_disabled_reason(state, provider)
        if reason:
            if provider.enabled:
                label = f"{provider.display_name}（已停用）"
            else:
                label = f"{provider.display_name}（不可用）"
        else:
            label = provider.display_name
        provider_options.append(
            {
                "value": provider.provider_id,
                "label": label,
                "selected": provider.provider_id == state.last_search_provider,
            }
        )
    import_provider_options = [
        {
            "value": AUTO_PROVIDER_ID,
            "label": "自动识别站点（推荐）",
            "selected": True,
        }
    ] + [
        {
            "value": item["value"],
            "label": f"{item['label']}（固定）",
            "selected": False,
        }
        for item in provider_options
    ]

    search_page_size_options = [
        {"value": size, "selected": size == max(4, min(40, int(search_page_size)))}
        for size in (8, 12, 16, 24, 40)
    ]
    body = render_template(
        "dashboard.html",
        message_html=render_message(msg),
        search_query=state.last_search_query,
        provider_options=provider_options,
        import_provider_options=import_provider_options,
        search_page_size_options=search_page_size_options,
        results=results,
    )
    return render_layout(
        title="漫画下载 - 主页",
        active_nav="dashboard",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_progress(state: UIState, msg: str, selected_job_id: str) -> str:
    progress_content_html = ""
    script = ""

    job = state.jobs.get(selected_job_id) if selected_job_id else None
    if job is None and state.current_job_id:
        job = state.jobs.get(state.current_job_id)

    if job is None:
        progress_content_html = (
            "<div class=\"panel\">"
            "<h2 class=\"title\">任务进度</h2>"
            "<div class=\"subtle\">当前没有活跃任务。请前往主页搜索漫画并创建下载任务。</div>"
            "<div style=\"margin-top:10px;\">"
            "<a class=\"btn icon-btn\" href=\"/dashboard\" title=\"去主页创建任务\" aria-label=\"去主页创建任务\">"
            "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
            "<path d=\"M4 11.5 12 5l8 6.5V20h-5v-5H9v5H4z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linejoin=\"round\"/>"
            "</svg>"
            "<span class=\"btn-text\">去主页创建任务</span>"
            "</a>"
            "</div>"
            "</div>"
        )
    else:
        panel_html, script = render_job_panel(job, heading="任务进度", full_page=True)
        progress_content_html = panel_html

    body = render_template(
        "progress.html",
        message_html=render_message(msg),
        progress_content_html=progress_content_html,
    )

    return render_layout(
        title="漫画下载 - 进度",
        active_nav="progress",
        body=body,
        script=script,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_queue(state: UIState, msg: str) -> str:
    queued_rows: list[dict[str, Any]] = []
    queued_jobs = queued_jobs_sorted(state)
    for idx, job in enumerate(queued_jobs, start=1):
        pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
        queued_rows.append(
            {
                "id": str(job.get("id") or ""),
                "index": idx,
                "title": str(job.get("title") or ""),
                "provider_badge_html": render_provider_badge(pid),
                "created_at": fmt_time(str(job.get("created_at") or "")),
                "can_move_up": idx > 1,
                "can_move_down": idx < len(queued_jobs),
            }
        )

    running_rows: list[dict[str, Any]] = []
    for job in state.jobs.values():
        status = str(job.get("status") or "")
        if status not in {"running", "paused", "cancelling"}:
            continue
        pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
        running_rows.append(
            {
                "id": str(job.get("id") or ""),
                "title": str(job.get("title") or ""),
                "provider_badge_html": render_provider_badge(pid),
                "status": status_text(status),
                "done_chapters": int(job.get("done_chapters", 0)),
                "total_chapters": int(job.get("total_chapters", 0)),
                "created_at": fmt_time(str(job.get("created_at") or "")),
                "started_at": fmt_time(str(job.get("started_at") or "")),
            }
        )
    running_rows.sort(key=lambda row: row["created_at"], reverse=True)

    failed_rows: list[dict[str, Any]] = []
    for job in state.jobs.values():
        if str(job.get("status") or "") != "failed":
            continue
        pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
        failed_rows.append(
            {
                "id": str(job.get("id") or ""),
                "title": str(job.get("title") or ""),
                "provider_badge_html": render_provider_badge(pid),
                "error": str(job.get("error") or ""),
                "finished_at": fmt_time(str(job.get("finished_at") or "")),
            }
        )
    failed_rows.sort(key=lambda row: row["finished_at"], reverse=True)

    body = render_template(
        "queue.html",
        message_html=render_message(msg),
        summary={
            "queued": len(queued_rows),
            "running": len(running_rows),
            "failed": len(failed_rows),
            "total": len(state.jobs),
        },
        queued_rows=queued_rows,
        running_rows=running_rows,
        failed_rows=failed_rows,
    )
    return render_layout(
        title="漫画下载 - 队列",
        active_nav="queue",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def normalize_cover_url(value: Any) -> str:
    cover_url = str(value or "").strip()
    if cover_url.startswith("//"):
        cover_url = f"https:{cover_url}"
    elif cover_url.startswith("/"):
        cover_url = urljoin("https://toonily.com", cover_url)
    if cover_url and not cover_url.startswith(("http://", "https://")):
        return ""
    return cover_url


def build_book_card_payload(book: dict[str, Any]) -> dict[str, Any]:
    follow_enabled = bool(book.get("follow_enabled", True))
    follow_text = "开启" if follow_enabled else "关闭"
    pending = int(book.get("pending_update_count", 0))
    provider_id = str(book.get("provider_id") or DEFAULT_PROVIDER_ID)
    provider_badge = render_provider_badge(provider_id)
    group_name = str(book.get("group") or "").strip()
    return {
        "id": str(book["id"]),
        "title": str(book.get("title") or "未命名漫画"),
        "cover_url": normalize_cover_url(book.get("cover")),
        "group": group_name,
        "provider_badge_html": provider_badge,
        "downloaded_text": (
            f"已下载：{book.get('last_downloaded_chapter_title') or '-'} "
            f"/ #{format_chapter_number(book.get('last_downloaded_chapter_number'))}"
        ),
        "latest_text": (
            f"最新：{book.get('latest_site_chapter_title') or '-'} "
            f"/ #{format_chapter_number(book.get('latest_site_chapter_number'))}"
        ),
        "summary_text": (
            f"待更新：{pending} | 追更：{follow_text} | "
            f"分组：{group_name or '未分组'} | 检查：{fmt_time(book.get('last_checked_at', ''))}"
        ),
        "follow_enabled": follow_enabled,
    }


def render_bookshelf(
    state: UIState,
    msg: str,
    *,
    bookshelf_page: int,
    bookshelf_page_size: int,
    bookshelf_group: str = "",
) -> str:
    jm_provider = get_provider("jmcomic")
    has_jm_login = bool(state.jm_username and state.jm_password)
    jm_reason = provider_disabled_reason(state, jm_provider)
    jm_enabled_for_use = not jm_reason

    all_books = state.list_books()
    selected_group = str(bookshelf_group or "").strip()
    grouped_counts: dict[str, int] = {}
    for item in all_books:
        key = str(item.get("group") or "").strip()
        grouped_counts[key] = grouped_counts.get(key, 0) + 1

    if selected_group:
        filtered_books = [book for book in all_books if str(book.get("group") or "").strip() == selected_group]
    else:
        filtered_books = all_books

    total_books = len(filtered_books)
    page_size = max(6, min(60, int(bookshelf_page_size)))
    page_count = max(1, math.ceil(total_books / page_size)) if total_books else 1
    page = max(1, min(int(bookshelf_page), page_count))
    start = (page - 1) * page_size
    end = start + page_size
    page_books = filtered_books[start:end]

    def bookshelf_page_url(target_page: int) -> str:
        params: dict[str, str] = {"bp": str(target_page), "bps": str(page_size)}
        if selected_group:
            params["bg"] = selected_group
        return f"/bookshelf?{urlencode(params)}"

    books = [build_book_card_payload(book) for book in page_books]
    for book in books:
        book["follow_button_text"] = "关闭追更" if book["follow_enabled"] else "开启追更"

    body = render_template(
        "bookshelf.html",
        message_html=render_message(msg),
        sync={
            "jm_enabled": jm_enabled_for_use,
            "has_jm_login": has_jm_login,
            "jm_username": state.jm_username,
            "manual_logged_in": state.jm_manual_logged_in,
            "manual_login_user": state.jm_manual_login_user,
            "jm_disabled_reason": jm_reason or "未知原因",
        },
        total_books=total_books,
        all_books_count=len(all_books),
        follow_count=sum(1 for item in all_books if bool(item.get("follow_enabled", True))),
        group_filter={
            "value": selected_group,
            "options": [
                {
                    "value": key,
                    "label": (key or "未分组"),
                    "count": grouped_counts[key],
                    "selected": key == selected_group,
                }
                for key in sorted(grouped_counts.keys(), key=lambda k: (k == "", k.lower()))
            ],
        },
        pager={
            "page": page,
            "page_count": page_count,
            "page_size": page_size,
            "has_prev": page > 1,
            "has_next": page < page_count,
            "prev_url": bookshelf_page_url(page - 1),
            "next_url": bookshelf_page_url(page + 1),
            "page_size_options": [
                {"value": size, "selected": size == page_size} for size in (12, 24, 36, 60)
            ],
        },
        books=books,
    )
    return render_layout(
        title="漫画下载 - 书架",
        active_nav="bookshelf",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_follow(
    state: UIState,
    msg: str,
    *,
    follow_page: int,
    follow_page_size: int,
) -> str:
    follow_books = [book for book in state.list_books() if bool(book.get("follow_enabled", True))]
    total_books = len(follow_books)
    pending_total = sum(max(0, int(book.get("pending_update_count", 0))) for book in follow_books)
    page_size = max(6, min(60, int(follow_page_size)))
    page_count = max(1, math.ceil(total_books / page_size)) if total_books else 1
    page = max(1, min(int(follow_page), page_count))
    start = (page - 1) * page_size
    end = start + page_size
    page_books = follow_books[start:end]

    def follow_page_url(target_page: int) -> str:
        return f"/follow?{urlencode({'fp': str(target_page), 'fps': str(page_size)})}"

    books = [build_book_card_payload(book) for book in page_books]

    body = render_template(
        "follow.html",
        message_html=render_message(msg),
        total_books=total_books,
        pending_total=pending_total,
        pager={
            "page": page,
            "page_count": page_count,
            "page_size": page_size,
            "has_prev": page > 1,
            "has_next": page < page_count,
            "prev_url": follow_page_url(page - 1),
            "next_url": follow_page_url(page + 1),
            "page_size_options": [
                {"value": size, "selected": size == page_size} for size in (12, 24, 36, 60)
            ],
        },
        books=books,
    )
    return render_layout(
        title="漫画下载 - 追更",
        active_nav="follow",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_health(state: UIState, msg: str) -> str:
    rows: list[dict[str, Any]] = []
    for provider in list_providers():
        row = state.ensure_health_entry(provider.provider_id)
        reasons = dict(row.get("failure_reasons") or {})
        top_reasons = sorted(reasons.items(), key=lambda item: item[1], reverse=True)[:6]
        rows.append(
            {
                "provider_id": provider.provider_id,
                "provider_name": provider.display_name,
                "available": bool(row.get("available", provider.enabled)),
                "last_check_at": fmt_time(str(row.get("last_check_at", ""))),
                "last_error": str(row.get("last_error", "")),
                "avg_speed_kbps": float(row.get("avg_speed_kbps", 0.0)),
                "total_jobs": int(row.get("total_jobs", 0)),
                "success_jobs": int(row.get("success_jobs", 0)),
                "failed_jobs": int(row.get("failed_jobs", 0)),
                "failure_reasons": top_reasons,
            }
        )
    recent_summary = summarize_recent_history(state, hours=24)
    recent_items = []
    for item in recent_summary["latest_items"]:
        row = dict(item)
        row["provider_name"] = provider_name(str(item.get("provider_id") or DEFAULT_PROVIDER_ID))
        row["finished_at_text"] = fmt_time(str(item.get("finished_at") or ""))
        recent_items.append(row)

    body = render_template(
        "health.html",
        message_html=render_message(msg),
        scheduler={
            "enabled": state.scheduler_enabled,
            "interval_minutes": state.scheduler_interval_minutes,
            "auto_download": state.scheduler_auto_download,
            "last_run_at": fmt_time(state.scheduler_last_run_at),
            "next_run_at": fmt_time(state.scheduler_next_run_at),
            "running": state._scheduler_running,
        },
        rows=rows,
        recent_summary=recent_summary,
        recent_items=recent_items,
    )
    return render_layout(
        title="漫画下载 - 监控",
        active_nav="health",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_settings(state: UIState, msg: str) -> str:
    jm_provider = get_provider("jmcomic")
    provider_switches: list[dict[str, Any]] = []
    for provider in list_providers():
        reason = provider_disabled_reason(state, provider)
        provider_switches.append(
            {
                "id": provider.provider_id,
                "name": provider.display_name,
                "checked": state.is_provider_enabled(provider.provider_id),
                "runtime_available": provider.enabled,
                "reason": reason,
            }
        )

    body = render_template(
        "settings.html",
        message_html=render_message(msg),
        settings={
            "output_dir": str(state.output_dir),
            "chapter_concurrency": state.chapter_concurrency,
            "image_concurrency": state.image_concurrency,
            "retries": state.retries,
            "timeout": state.timeout,
            "max_parallel_jobs": state.max_parallel_jobs,
            "retry_base_delay_seconds": state.retry_base_delay_seconds,
            "retry_recoverable_only": state.retry_recoverable_only,
            "enable_chapter_dedupe": state.enable_chapter_dedupe,
            "image_output_format": state.image_output_format,
            "image_quality": state.image_quality,
            "keep_original_images": state.keep_original_images,
            "auto_archive_format": state.auto_archive_format,
            "write_metadata_sidecar": state.write_metadata_sidecar,
            "manga_dir_template": state.manga_dir_template,
            "chapter_dir_template": state.chapter_dir_template,
            "page_name_template": state.page_name_template,
            "bandwidth_day_kbps": state.bandwidth_day_kbps,
            "bandwidth_night_kbps": state.bandwidth_night_kbps,
            "night_start_hour": state.night_start_hour,
            "night_end_hour": state.night_end_hour,
            "scheduler_enabled": state.scheduler_enabled,
            "scheduler_interval_minutes": state.scheduler_interval_minutes,
            "scheduler_auto_download": state.scheduler_auto_download,
            "scheduler_last_run_at": fmt_time(state.scheduler_last_run_at),
            "scheduler_next_run_at": fmt_time(state.scheduler_next_run_at),
            "redis_host": state.redis_host,
            "redis_port": state.redis_port,
            "redis_db": state.redis_db,
            "redis_username": state.redis_username,
            "redis_password": state.redis_password,
            "cache_ttl_seconds": state.cache_ttl_seconds,
            "cache_enabled": state.cache_enabled,
            "jm_username": state.jm_username,
            "jm_password": state.jm_password,
            "webhook_enabled": state.webhook_enabled,
            "webhook_url": state.webhook_url,
            "webhook_token": state.webhook_token,
            "webhook_event_completed": state.webhook_event_completed,
            "webhook_event_failed": state.webhook_event_failed,
            "webhook_event_cancelled": state.webhook_event_cancelled,
            "webhook_timeout_seconds": state.webhook_timeout_seconds,
            "compact_mode_enabled": state.compact_mode_enabled,
            "manga_view_mode": state.manga_view_mode,
            "jm_enabled": provider_enabled_for_state(state, jm_provider),
            "jm_disabled_reason": provider_disabled_reason(state, jm_provider) or "未知原因",
            "provider_switches": provider_switches,
        },
    )
    return render_layout(
        title="漫画下载 - 设置",
        active_nav="settings",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def job_controls(job: dict[str, Any]) -> dict[str, bool]:
    status = job.get("status", "")
    return {
        "can_pause": status == "running",
        "can_resume": status == "paused",
        "can_cancel": status in {"queued", "running", "paused", "cancelling"},
    }


def serialize_job(job: dict[str, Any]) -> dict[str, Any]:
    pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
    return {
        "id": job["id"],
        "title": job["title"],
        "provider_id": pid,
        "provider_name": provider_name(pid),
        "series_url": job["series_url"],
        "status": job["status"],
        "status_text": status_text(job["status"]),
        "error": job.get("error", ""),
        "retry_file": job.get("retry_file", ""),
        "created_at": job.get("created_at", ""),
        "queue_order": queue_order_value(job),
        "started_at": job.get("started_at", ""),
        "finished_at": job.get("finished_at", ""),
        "done_chapters": int(job.get("done_chapters", 0)),
        "total_chapters": int(job.get("total_chapters", 0)),
        "saved_images": int(job.get("saved_images", 0)),
        "total_images": int(job.get("total_images", 0)),
        "successful_chapters": int(job.get("successful_chapters", 0)),
        "failed_chapters": int(job.get("failed_chapters", 0)),
        "logs": job.get("logs", []),
        "controls": job_controls(job),
    }

async def run_download_job(state: UIState, job: dict[str, Any]) -> None:
    job["status"] = "running"
    job["started_at"] = now_iso()
    state.append_job_log(job, "开始下载任务。")
    provider = get_provider(str(job.get("provider_id") or DEFAULT_PROVIDER_ID))
    state.append_job_log(job, f"使用站点：{provider.display_name}")

    reason = provider_disabled_reason(state, provider)
    if reason:
        job["status"] = "failed"
        job["finished_at"] = now_iso()
        job["error"] = reason
        state.append_job_log(job, f"任务失败：{job['error']}")
        state.record_download_report(provider.provider_id, None, "failed", reason)
        return

    async def pause_waiter() -> None:
        while not job["pause_event"].is_set():
            if job["cancel_requested"]:
                raise asyncio.CancelledError("Task cancelled.")
            await asyncio.sleep(0.2)

    def cancel_checker() -> bool:
        return bool(job["cancel_requested"])

    def logger(message: str) -> None:
        state.append_job_log(job, message)

    def progress_callback(payload: dict[str, Any]) -> None:
        event = payload.get("event", "")
        job["done_chapters"] = int(payload.get("done_chapters", job["done_chapters"]))
        job["total_chapters"] = int(payload.get("total_chapters", job["total_chapters"]))
        job["saved_images"] = int(payload.get("saved_images", job["saved_images"]))
        job["total_images"] = int(payload.get("total_images", job["total_images"]))
        if event == "finished":
            job["successful_chapters"] = int(payload.get("successful_chapters", 0))
            job["failed_chapters"] = int(payload.get("failed_chapters", 0))
            job["retry_file"] = str(payload.get("retry_file", "")).strip()

    report: Optional[DownloadReport] = None
    downloader: Optional[Any] = None
    try:
        downloader = provider.create_downloader(
            state,
            series_url=job["series_url"],
            chapter_selector=job["chapter_selector"],
            chapter_urls=job["chapter_urls"],
            logger=logger,
            progress_callback=progress_callback,
            pause_waiter=pause_waiter,
            cancel_checker=cancel_checker,
        )
        report = await downloader.run()
        job["status"] = "completed"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "任务完成。")
        if report.retry_file:
            job["retry_file"] = str(report.retry_file)
        state.record_download_report(provider.provider_id, report, "completed")
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "任务已取消。")
        state.record_download_report(provider.provider_id, report, "cancelled", "cancelled")
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = now_iso()
        state.append_job_log(job, f"任务失败：{exc}")
        state.record_download_report(provider.provider_id, report, "failed", str(exc))
    finally:
        if downloader is not None:
            await downloader.close()

    book_id = str(job.get("book_id") or "")
    if book_id and report is not None and job["status"] == "completed":
        book = state.get_book(book_id)
        if book is not None:
            latest_url, latest_title, latest_num = pick_latest_report_chapter(report)
            if latest_url:
                book["last_downloaded_chapter_url"] = normalize_url(latest_url)
                book["last_downloaded_chapter_title"] = latest_title or ""
                book["last_downloaded_chapter_number"] = latest_num
                book["last_update_at"] = now_iso()
            try:
                await refresh_book_snapshot(state, book, logger=lambda msg: state.append_job_log(job, msg))
            except Exception as exc:
                state.append_job_log(job, f"刷新书架信息失败：{exc}")
            await state.save_bookshelf()

    if is_job_final(str(job.get("status") or "")):
        state.append_job_history(build_job_history_item(job=job, report=report))
        if state.webhook_enabled and state.webhook_url and state.webhook_event_enabled(str(job.get("status") or "")):
            asyncio.create_task(push_job_webhook(state, job, report))


def start_job(state: UIState, job: dict[str, Any]) -> None:
    task = asyncio.create_task(run_download_job(state, job))
    job["task"] = task

    def _finish_callback(fut: asyncio.Task[Any]) -> None:
        try:
            fut.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            state.append_job_log(job, f"后台异常：{exc}")
            job["status"] = "failed"
            job["error"] = str(exc)
            job["finished_at"] = now_iso()
        finally:
            job["task"] = None
            dispatch_jobs(state)

    task.add_done_callback(_finish_callback)


async def handle_root(_: web.Request) -> web.StreamResponse:
    raise build_redirect("/dashboard")


async def handle_dashboard(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    search_page = parse_int(request.query.get("sp", "1"), 1, minimum=1, maximum=999)
    search_page_size = parse_int(request.query.get("sps", "12"), 12, minimum=4, maximum=40)
    html = render_dashboard(
        state,
        msg,
        search_page=search_page,
        search_page_size=search_page_size,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_progress(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    selected_job_id = request.query.get("job", "").strip() or (state.current_job_id or "")
    html = render_progress(state, msg, selected_job_id)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_queue(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    html = render_queue(state, msg)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


def apply_import_book_fields(book: dict[str, Any], raw: dict[str, Any]) -> None:
    if "follow_enabled" in raw:
        book["follow_enabled"] = parse_bool(raw.get("follow_enabled"), bool(book.get("follow_enabled", True)))
    for key in (
        "last_downloaded_chapter_number",
        "last_downloaded_chapter_title",
        "last_downloaded_chapter_url",
        "latest_site_chapter_number",
        "latest_site_chapter_title",
        "latest_site_chapter_url",
        "last_checked_at",
        "last_update_at",
    ):
        if key in raw:
            value = raw.get(key)
            if key.endswith("_number"):
                book[key] = parse_float(value)
            else:
                book[key] = normalize_url(str(value)) if key.endswith("_url") else str(value or "").strip()
    if "pending_update_count" in raw:
        try:
            book["pending_update_count"] = max(0, int(raw.get("pending_update_count") or 0))
        except Exception:
            pass
    if "group" in raw:
        book["group"] = str(raw.get("group") or "").strip()


async def handle_bookshelf_export(request: web.Request) -> web.Response:
    state = get_app_state(request)
    items = state.list_books()
    payload = {
        "exported_at": now_iso(),
        "count": len(items),
        "books": items,
    }
    filename = f"bookshelf-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    return web.Response(
        text=json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
        charset="utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def handle_bookshelf_import(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    payload_text = ""
    upload = form.get("bookshelf_file")
    if hasattr(upload, "file"):
        try:
            payload_text = upload.file.read().decode("utf-8", errors="ignore")
        except Exception:
            payload_text = ""
    if not payload_text:
        payload_text = str(form.get("bookshelf_json", "") or "").strip()
    if not payload_text:
        raise back("请先选择要导入的 JSON 文件。")

    try:
        raw_data = json.loads(payload_text)
    except Exception as exc:
        raise back(f"导入失败，JSON 解析错误：{exc}")

    if isinstance(raw_data, list):
        rows = raw_data
    elif isinstance(raw_data, dict):
        candidate = raw_data.get("books")
        if not isinstance(candidate, list):
            candidate = raw_data.get("items")
        rows = candidate if isinstance(candidate, list) else []
    else:
        rows = []
    if not rows:
        raise back("导入内容为空或格式不正确。")

    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue
        series_url = normalize_url(str(row.get("series_url") or row.get("url") or "").strip())
        if not series_url:
            skipped += 1
            continue
        provider_id = str(row.get("provider_id") or detect_provider_id_by_url(series_url) or DEFAULT_PROVIDER_ID).strip().lower()
        title = str(row.get("title") or guess_title_from_url(series_url)).strip()
        cover = str(row.get("cover") or "").strip()
        group = str(row.get("group") or "").strip()
        book, is_created = state.upsert_book(
            provider_id=provider_id,
            title=title,
            series_url=series_url,
            cover=cover,
            group=group,
        )
        apply_import_book_fields(book, row)
        if is_created:
            created += 1
        else:
            updated += 1

    await state.save_bookshelf()
    raise back(f"导入完成：新增 {created} 本，更新 {updated} 本，跳过 {skipped} 条。")


async def handle_search(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    keyword = str(form.get("query", "")).strip()
    page_size = parse_int(form.get("page_size", "12"), 12, minimum=4, maximum=40)
    provider_id = str(form.get("provider_id", state.last_search_provider or DEFAULT_PROVIDER_ID)).strip().lower()
    provider = get_provider(provider_id)
    if not keyword:
        raise build_redirect("/dashboard", msg="请输入漫画名称。", sp=1, sps=page_size)
    reason = provider_disabled_reason(state, provider)
    if reason:
        raise build_redirect("/dashboard", msg=f"{provider.display_name} 不可用：{reason}", sp=1, sps=page_size)

    state.last_search_query = keyword
    state.last_search_provider = provider.provider_id
    state.last_search_results = await search_by_provider(state, provider.provider_id, keyword)
    if not state.last_search_results:
        raise build_redirect("/dashboard", msg="未搜索到结果，可尝试英文名或更短关键词。", sp=1, sps=page_size)
    raise build_redirect(
        "/dashboard",
        msg=f"搜索完成，共 {len(state.last_search_results)} 条结果。",
        sp=1,
        sps=page_size,
    )


async def handle_search_action(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()

    action = str(form.get("action", "")).strip()
    sp = parse_int(form.get("sp", "1"), 1, minimum=1, maximum=999)
    sps = parse_int(form.get("sps", "12"), 12, minimum=4, maximum=40)
    provider_id = str(form.get("provider_id", state.last_search_provider or DEFAULT_PROVIDER_ID)).strip().lower()
    provider = get_provider(provider_id)
    title = str(form.get("title", "")).strip()
    series_url = str(form.get("url", "")).strip()
    cover = str(form.get("cover", "")).strip()

    if not series_url:
        raise build_redirect("/dashboard", msg="缺少漫画链接。")
    reason = provider_disabled_reason(state, provider)
    if reason:
        raise build_redirect("/dashboard", msg=f"{provider.display_name} 不可用：{reason}")

    if action == "add_bookshelf":
        book, created = state.upsert_book(
            provider_id=provider.provider_id,
            title=title,
            series_url=series_url,
            cover=cover,
        )
        try:
            await refresh_book_snapshot(state, book)
        except Exception:
            pass
        await state.save_bookshelf()
        word = "已加入书架" if created else "书架已存在，已更新信息"
        raise build_redirect("/dashboard", msg=word, sp=sp, sps=sps)

    if action == "download_all":
        job = state.create_job(
            title=f"下载全部：{title or series_url}",
            series_url=series_url,
            chapter_selector="all",
            chapter_urls=[],
            mode="download_all",
            provider_id=provider.provider_id,
        )
        dispatch_jobs(state)
        raise build_redirect("/dashboard", msg="下载任务已创建。", sp=sp, sps=sps)

    if action == "follow_download":
        book, _ = state.upsert_book(
            provider_id=provider.provider_id,
            title=title,
            series_url=series_url,
            cover=cover,
        )
        await state.save_bookshelf()
        job = state.create_job(
            title=f"追更下载：{book['title']}",
            series_url=book["series_url"],
            chapter_selector="all",
            chapter_urls=[],
            mode="follow_download",
            book_id=book["id"],
            provider_id=provider.provider_id,
        )
        dispatch_jobs(state)
        raise build_redirect("/dashboard", msg="已加入书架并创建追更下载任务。", sp=sp, sps=sps)

    raise build_redirect("/dashboard", msg="未知操作。")


async def handle_batch_import(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()

    provider_id = str(form.get("provider_id", AUTO_PROVIDER_ID)).strip().lower()
    auto_detect = provider_id == AUTO_PROVIDER_ID
    fallback_provider_id = first_enabled_provider_id(state)
    if not auto_detect:
        fallback_provider_id = provider_id
        fixed_provider = get_provider(fallback_provider_id)
        fixed_reason = provider_disabled_reason(state, fixed_provider)
        if fixed_reason:
            raise build_redirect("/dashboard", msg=f"{fixed_provider.display_name} 不可用：{fixed_reason}")

    import_mode = str(form.get("import_mode", "queue_download")).strip().lower()
    text_payload = str(form.get("import_text", "") or "")
    filename = ""
    file_field = form.get("import_file")
    if hasattr(file_field, "filename") and hasattr(file_field, "file"):
        filename = str(getattr(file_field, "filename", "") or "")
        try:
            text_payload = file_field.file.read().decode("utf-8", errors="ignore")
        except Exception:
            text_payload = ""

    urls = parse_bulk_import_payload(text_payload, filename=filename)
    if not urls:
        raise build_redirect("/dashboard", msg="未识别到可导入的链接，请检查 txt/csv/json 内容。")

    created_books = 0
    updated_books = 0
    queued_jobs = 0
    skipped_urls = 0
    detected_success = 0
    detected_fallback = 0
    detected_unknown = 0
    provider_count: dict[str, int] = {}

    for url in urls:
        target_provider_id = fallback_provider_id
        if auto_detect:
            detected_id = detect_provider_id_by_url(url)
            if detected_id:
                detected_provider = get_provider(detected_id)
                detected_reason = provider_disabled_reason(state, detected_provider)
                if detected_reason:
                    target_provider_id = fallback_provider_id
                    detected_fallback += 1
                else:
                    target_provider_id = detected_provider.provider_id
                    detected_success += 1
            else:
                detected_unknown += 1

        provider = get_provider(target_provider_id)
        reason = provider_disabled_reason(state, provider)
        if reason:
            skipped_urls += 1
            continue

        title = guess_title_from_url(url)
        book, created = state.upsert_book(
            provider_id=provider.provider_id,
            title=title,
            series_url=url,
            cover="",
        )
        if created:
            created_books += 1
        else:
            updated_books += 1
        provider_count[provider.provider_id] = provider_count.get(provider.provider_id, 0) + 1

        if import_mode == "queue_download":
            job = state.create_job(
                title=f"批量导入下载：{book['title']}",
                series_url=book["series_url"],
                chapter_selector="all",
                chapter_urls=[],
                mode="batch_import",
                book_id=book["id"],
                provider_id=provider.provider_id,
            )
            state.append_job_log(job, "由批量导入创建。")
            queued_jobs += 1

    await state.save_bookshelf()
    dispatch_jobs(state)
    provider_summary = ", ".join(
        f"{provider_name(pid)} {count}"
        for pid, count in sorted(provider_count.items(), key=lambda item: item[0])
    ) or "无"

    if import_mode == "queue_download":
        if auto_detect:
            raise build_redirect(
                "/dashboard",
                msg=(
                    f"批量导入完成：链接 {len(urls)}，识别成功 {detected_success}，回退 {detected_fallback}，"
                    f"未识别 {detected_unknown}，跳过 {skipped_urls}；新增书架 {created_books}，"
                    f"更新 {updated_books}，入队任务 {queued_jobs}。分流：{provider_summary}。"
                ),
            )
        raise build_redirect(
            "/dashboard",
            msg=(
                f"批量导入完成：链接 {len(urls)}，跳过 {skipped_urls}；新增书架 {created_books}，"
                f"更新 {updated_books}，入队任务 {queued_jobs}。站点：{provider_summary}。"
            ),
        )
    if auto_detect:
        raise build_redirect(
            "/dashboard",
            msg=(
                f"批量导入完成：链接 {len(urls)}，识别成功 {detected_success}，回退 {detected_fallback}，"
                f"未识别 {detected_unknown}，跳过 {skipped_urls}；新增书架 {created_books}，"
                f"更新 {updated_books}。分流：{provider_summary}。"
            ),
        )
    raise build_redirect(
        "/dashboard",
        msg=(
            f"批量导入完成：链接 {len(urls)}，跳过 {skipped_urls}；新增书架 {created_books}，"
            f"更新 {updated_books}。站点：{provider_summary}。"
        ),
    )


async def handle_bookshelf(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    bookshelf_page = parse_int(request.query.get("bp", "1"), 1, minimum=1, maximum=999)
    bookshelf_page_size = parse_int(request.query.get("bps", "24"), 24, minimum=6, maximum=60)
    bookshelf_group = str(request.query.get("bg", "") or "").strip()
    html = render_bookshelf(
        state,
        msg,
        bookshelf_page=bookshelf_page,
        bookshelf_page_size=bookshelf_page_size,
        bookshelf_group=bookshelf_group,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_follow(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    follow_page = parse_int(request.query.get("fp", "1"), 1, minimum=1, maximum=999)
    follow_page_size = parse_int(request.query.get("fps", "24"), 24, minimum=6, maximum=60)
    html = render_follow(
        state,
        msg,
        follow_page=follow_page,
        follow_page_size=follow_page_size,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_follow_summary(request: web.Request) -> web.Response:
    state = get_app_state(request)
    follow_books = [book for book in state.list_books() if bool(book.get("follow_enabled", True))]
    pending_total = sum(max(0, int(book.get("pending_update_count", 0))) for book in follow_books)
    return web.json_response(
        {
            "ok": True,
            "total_books": len(follow_books),
            "pending_total": pending_total,
        }
    )


async def handle_follow_bulk(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    ajax = request_wants_json(request, form)
    page = parse_int(form.get("fp", "1"), 1, minimum=1, maximum=999)
    page_size = parse_int(form.get("fps", "24"), 24, minimum=6, maximum=60)
    bulk_action = str(form.get("bulk_action", "")).strip().lower()
    raw_selected_ids: list[str] = []
    if hasattr(form, "getall"):
        try:
            raw_selected_ids = [str(v).strip() for v in form.getall("book_ids") if str(v).strip()]
        except KeyError:
            raw_selected_ids = []
    else:
        value = str(form.get("book_ids", "") or "").strip() if hasattr(form, "get") else ""
        if value:
            raw_selected_ids = [value]
    selected_ids = list(dict.fromkeys(raw_selected_ids))

    def back_redirect(message: str) -> web.HTTPSeeOther:
        return build_redirect("/follow", msg=message, fp=page, fps=page_size)

    follow_books = [book for book in state.list_books() if bool(book.get("follow_enabled", True))]

    if bulk_action == "follow_check_all":
        if not follow_books:
            if ajax:
                return web.json_response({"ok": False, "message": "当前没有开启追更的漫画。"}, status=400)
            raise back_redirect("当前没有开启追更的漫画。")
        checked = 0
        failed = 0
        for book in follow_books:
            try:
                await refresh_book_snapshot(state, book)
                checked += 1
            except Exception:
                failed += 1
        await state.save_bookshelf()
        pending_total = sum(max(0, int(book.get("pending_update_count", 0))) for book in follow_books)
        if failed:
            message = f"一键检查完成：成功 {checked} 本，失败 {failed} 本，待更新合计 {pending_total} 章。"
            if ajax:
                return web.json_response(
                    {
                        "ok": True,
                        "message": message,
                        "checked": checked,
                        "failed": failed,
                        "total_books": len(follow_books),
                        "pending_total": pending_total,
                    }
                )
            raise back_redirect(message)
        message = f"一键检查完成：已检查 {checked} 本，待更新合计 {pending_total} 章。"
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": message,
                    "checked": checked,
                    "failed": 0,
                    "total_books": len(follow_books),
                    "pending_total": pending_total,
                }
            )
        raise back_redirect(message)

    if bulk_action == "follow_update_all":
        if not follow_books:
            if ajax:
                return web.json_response({"ok": False, "message": "当前没有开启追更的漫画。"}, status=400)
            raise back_redirect("当前没有开启追更的漫画。")
        queued = 0
        no_update = 0
        failed = 0
        job_ids: list[str] = []
        for book in follow_books:
            created, detail = await enqueue_book_updates_job(
                state,
                book,
                source_message="由追更页一键更新创建。",
            )
            if created:
                queued += 1
                if detail:
                    job_ids.append(detail)
            elif detail:
                failed += 1
            else:
                no_update += 1
        dispatch_jobs(state)
        await state.save_bookshelf()
        message = f"一键更新完成：已创建 {queued} 个任务，无更新 {no_update} 本，失败 {failed} 本。"
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": message,
                    "queued": queued,
                    "no_update": no_update,
                    "failed": failed,
                    "job_ids": job_ids,
                }
            )
        raise back_redirect(message)

    if not selected_ids:
        if ajax:
            return web.json_response({"ok": False, "message": "请先选择至少一本漫画。"}, status=400)
        raise back_redirect("请先选择至少一本漫画。")

    selected_books: list[dict[str, Any]] = []
    for book_id in selected_ids:
        book = state.get_book(book_id)
        if book is not None:
            selected_books.append(book)
    if not selected_books:
        if ajax:
            return web.json_response({"ok": False, "message": "所选项目不存在或已被移除。"}, status=404)
        raise back_redirect("所选项目不存在或已被移除。")

    if bulk_action == "bulk_disable_follow":
        changed = 0
        for book in selected_books:
            if bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = False
                changed += 1
        await state.save_bookshelf()
        message = f"已取消 {changed} 本漫画的追更。"
        if ajax:
            return web.json_response({"ok": True, "message": message, "changed": changed})
        raise back_redirect(message)

    if ajax:
        return web.json_response({"ok": False, "message": "未知批量操作。"}, status=400)
    raise back_redirect("未知批量操作。")


async def handle_health(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    html = render_health(state, msg)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_bookshelf_jm_login(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    provider = get_provider("jmcomic")
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    reason = provider_disabled_reason(state, provider)
    if reason:
        raise back(f"JM 不可用：{reason}")

    if not state.jm_username or not state.jm_password:
        raise build_redirect("/settings", msg="请先填写 JM 用户名和密码，再手动登录。")

    try:
        login_user = await manual_login_jm(
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
        )
    except Exception as exc:
        state.jm_manual_logged_in = False
        state.jm_manual_login_user = ""
        raise back(f"JM 手动登录失败：{exc}")

    state.jm_manual_logged_in = True
    state.jm_manual_login_user = login_user
    raise back(f"JM 手动登录成功：{login_user}")


async def handle_bookshelf_jm_logout(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    try:
        await manual_logout_jm(
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
        )
    except Exception:
        # Some jmcomic versions may not expose logout, treat local logout as source of truth.
        pass

    state.jm_manual_logged_in = False
    state.jm_manual_login_user = ""
    raise back("JM 已退出手动登录状态。")


async def handle_bookshelf_sync_jm_favorites(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    provider = get_provider("jmcomic")
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    reason = provider_disabled_reason(state, provider)
    if reason:
        raise back(f"JM 不可用：{reason}")

    if not state.jm_username or not state.jm_password:
        raise build_redirect("/settings", msg="请先填写 JM 用户名和密码，再同步收藏。")

    try:
        favorites = await sync_jm_favorites(
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
        )
    except Exception as exc:
        raise back(f"同步 JM 收藏失败：{exc}")

    created = 0
    updated = 0
    created_follow_off = 0
    for item in favorites:
        book, is_created = state.upsert_book(
            provider_id="jmcomic",
            title=str(item.get("title") or "").strip(),
            series_url=str(item.get("url") or "").strip(),
            cover=str(item.get("cover") or "").strip(),
        )
        if is_created:
            if bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = False
                created_follow_off += 1
            created += 1
        else:
            updated += 1

    await state.save_bookshelf()
    raise back(
        f"JM 收藏同步完成：共 {len(favorites)} 条，新增 {created}（默认关闭追更 {created_follow_off}），更新 {updated}。"
    )


async def enqueue_book_updates_job(
    state: UIState,
    book: dict[str, Any],
    *,
    source_message: str = "",
) -> tuple[bool, str]:
    try:
        _, chapters = await fetch_series_snapshot(
            state,
            str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
            str(book.get("series_url") or ""),
        )
    except Exception as exc:
        return False, str(exc)

    pending = compute_pending_chapters(book, chapters)
    set_site_latest_fields(book, chapters)
    book["pending_update_count"] = len(pending)
    book["last_checked_at"] = now_iso()

    if not pending:
        return False, ""

    chapter_urls = [item.url for item in pending]
    title = f"下载更新：{book.get('title') or '未命名漫画'} ({len(chapter_urls)} 章)"
    job = state.create_job(
        title=title,
        series_url=str(book.get("series_url") or ""),
        chapter_selector="all",
        chapter_urls=chapter_urls,
        mode="download_updates",
        book_id=str(book.get("id") or ""),
        provider_id=str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
    )
    if source_message:
        state.append_job_log(job, source_message)
    return True, str(job.get("id") or "")


def create_retry_job_from_failed(state: UIState, failed_job: dict[str, Any]) -> Optional[dict[str, Any]]:
    status = str(failed_job.get("status") or "")
    if status not in {"failed", "cancelled"}:
        return None
    job = state.create_job(
        title=f"重试：{str(failed_job.get('title') or '下载任务')}",
        series_url=str(failed_job.get("series_url") or ""),
        chapter_selector=str(failed_job.get("chapter_selector") or "all"),
        chapter_urls=[str(item) for item in list(failed_job.get("chapter_urls") or []) if str(item).strip()],
        mode=str(failed_job.get("mode") or "download_all"),
        book_id=str(failed_job.get("book_id") or ""),
        provider_id=str(failed_job.get("provider_id") or DEFAULT_PROVIDER_ID),
    )
    state.append_job_log(job, f"由失败任务 {failed_job.get('id')} 重试创建。")
    return job


async def handle_bookshelf_bulk(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    page = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    page_size = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    group_filter = str(form.get("bg", "") or "").strip()
    bulk_action = str(form.get("bulk_action", "")).strip().lower()

    raw_ids: list[str] = []
    if hasattr(form, "getall"):
        raw_ids = [str(v).strip() for v in form.getall("book_ids") if str(v).strip()]
    selected_ids = list(dict.fromkeys(raw_ids))

    def back_redirect(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": page, "bps": page_size}
        if group_filter:
            params["bg"] = group_filter
        return build_redirect("/bookshelf", msg=message, **params)

    if not selected_ids:
        raise back_redirect("请先选择至少一本漫画。")

    selected_books: list[dict[str, Any]] = []
    for book_id in selected_ids:
        book = state.get_book(book_id)
        if book is not None:
            selected_books.append(book)
    if not selected_books:
        raise back_redirect("所选项目不存在或已被移除。")

    if bulk_action == "bulk_enable_follow":
        changed = 0
        for book in selected_books:
            if not bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = True
                changed += 1
        await state.save_bookshelf()
        if changed:
            raise back_redirect(f"已为 {changed} 本漫画开启追更。")
        raise back_redirect("所选漫画已全部开启追更。")

    if bulk_action == "bulk_follow_download":
        enabled_changed = 0
        queued = 0
        unchanged = 0
        failed_titles: list[str] = []
        for book in selected_books:
            if not bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = True
                enabled_changed += 1
            created, detail = await enqueue_book_updates_job(
                state,
                book,
                source_message="由书架批量追更下载创建。",
            )
            if created:
                queued += 1
            elif detail:
                failed_titles.append(f"{book.get('title') or '未命名漫画'}：{detail}")
            else:
                unchanged += 1
        await state.save_bookshelf()
        dispatch_jobs(state)

        msg = f"批量追更完成：入队 {queued} 本，无更新 {unchanged} 本"
        if enabled_changed:
            msg += f"，并开启追更 {enabled_changed} 本"
        if failed_titles:
            msg += f"，失败 {len(failed_titles)} 本"
        msg += "。"

        raise back_redirect(msg)

    if bulk_action == "bulk_download_all":
        queued = 0
        for book in selected_books:
            title = f"下载全部：{book.get('title') or '未命名漫画'}"
            job = state.create_job(
                title=title,
                series_url=str(book.get("series_url") or ""),
                chapter_selector="all",
                chapter_urls=[],
                mode="download_all",
                book_id=str(book.get("id") or ""),
                provider_id=str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
            )
            queued += 1
        dispatch_jobs(state)
        if queued:
            raise back_redirect(f"已为 {queued} 本漫画创建下载任务。")
        raise back_redirect("未能创建下载任务，请稍后重试。")

    if bulk_action == "bulk_set_group":
        group_name = str(form.get("bulk_group_name", "") or "").strip()
        for book in selected_books:
            book["group"] = group_name
        await state.save_bookshelf()
        if group_name:
            raise back_redirect(f"已将 {len(selected_books)} 本漫画设置到分组：{group_name}。")
        raise back_redirect(f"已将 {len(selected_books)} 本漫画设为未分组。")

    raise back_redirect("未知批量操作。")


async def handle_book_action(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    ajax = request_wants_json(request, form)
    src = str(form.get("src", "bookshelf")).strip().lower()
    if src == "follow":
        page = parse_int(form.get("fp", "1"), 1, minimum=1, maximum=999)
        page_size = parse_int(form.get("fps", "24"), 24, minimum=6, maximum=60)
        back_path = "/follow"
        back_params = {"fp": page, "fps": page_size}
    else:
        page = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
        page_size = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
        group_filter = str(form.get("bg", "") or "").strip()
        back_path = "/bookshelf"
        back_params = {"bp": page, "bps": page_size}
        if group_filter:
            back_params["bg"] = group_filter

    def back_redirect(message: str) -> web.HTTPSeeOther:
        return build_redirect(back_path, msg=message, **back_params)

    book_id = request.match_info["book_id"]
    action = request.match_info["action"]
    book = state.get_book(book_id)
    if book is None:
        if ajax:
            return web.json_response({"ok": False, "message": "书架项目不存在。"}, status=404)
        raise back_redirect("书架项目不存在。")

    if action == "remove":
        state.remove_book(book_id)
        await state.save_bookshelf()
        if ajax:
            return web.json_response({"ok": True, "message": "已移除。", "removed": True, "book_id": book_id})
        raise back_redirect("已移除。")

    if action == "toggle_follow":
        book["follow_enabled"] = not bool(book.get("follow_enabled", True))
        await state.save_bookshelf()
        message = f"追更已{'开启' if book['follow_enabled'] else '关闭'}。"
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": message,
                    "book": build_book_card_payload(book),
                    "book_id": str(book.get("id") or ""),
                }
            )
        raise back_redirect(message)

    if action == "check":
        try:
            pending = await refresh_book_snapshot(state, book)
            await state.save_bookshelf()
            message = f"检查完成，待更新 {len(pending)} 章。"
            if ajax:
                return web.json_response(
                    {
                        "ok": True,
                        "message": message,
                        "book": build_book_card_payload(book),
                        "book_id": str(book.get("id") or ""),
                        "pending_count": len(pending),
                    }
                )
            raise back_redirect(message)
        except Exception as exc:
            if ajax:
                return web.json_response({"ok": False, "message": f"检查失败：{exc}"}, status=400)
            raise back_redirect(f"检查失败：{exc}")

    if action in {"download_updates", "download_all"}:
        chapter_urls: list[str] = []
        mode = "download_updates" if action == "download_updates" else "download_all"

        try:
            _, chapters = await fetch_series_snapshot(
                state,
                str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
                book["series_url"],
            )
        except Exception as exc:
            if ajax:
                return web.json_response({"ok": False, "message": f"读取章节失败：{exc}"}, status=400)
            raise back_redirect(f"读取章节失败：{exc}")

        if action == "download_updates":
            pending = compute_pending_chapters(book, chapters)
            chapter_urls = [item.url for item in pending]
            set_site_latest_fields(book, chapters)
            book["pending_update_count"] = len(chapter_urls)
            book["last_checked_at"] = now_iso()
            if not chapter_urls:
                await state.save_bookshelf()
                if ajax:
                    return web.json_response(
                        {
                            "ok": True,
                            "message": "没有新的章节需要下载。",
                            "no_job": True,
                            "book": build_book_card_payload(book),
                            "book_id": str(book.get("id") or ""),
                        }
                    )
                raise back_redirect("没有新的章节需要下载。")
            title = f"下载更新：{book['title']} ({len(chapter_urls)} 章)"
        else:
            title = f"下载全部：{book['title']}"

        await state.save_bookshelf()
        job = state.create_job(
            title=title,
            series_url=book["series_url"],
            chapter_selector="all",
            chapter_urls=chapter_urls,
            mode=mode,
            book_id=book["id"],
            provider_id=str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
        )
        dispatch_jobs(state)
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": "任务已创建。",
                    "job_id": str(job.get("id") or ""),
                    "book_id": str(book.get("id") or ""),
                    "mode": mode,
                }
            )
        raise back_redirect("任务已创建。")

    if ajax:
        return web.json_response({"ok": False, "message": "未知操作。"}, status=400)
    raise back_redirect("未知操作。")


async def handle_book_card(request: web.Request) -> web.Response:
    state = get_app_state(request)
    book_id = str(request.match_info.get("book_id") or "").strip()
    book = state.get_book(book_id)
    if book is None:
        return web.json_response({"ok": False, "message": "book_not_found"}, status=404)
    return web.json_response({"ok": True, "book": build_book_card_payload(book)})


async def handle_settings_get(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    html = render_settings(state, msg)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_settings_post(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    try:
        state.output_dir = Path(str(form.get("output_dir", state.output_dir)).strip() or state.output_dir).resolve()
        state.chapter_concurrency = max(1, int(form.get("chapter_concurrency", state.chapter_concurrency)))
        state.image_concurrency = max(1, int(form.get("image_concurrency", state.image_concurrency)))
        state.retries = max(1, int(form.get("retries", state.retries)))
        state.timeout = max(10, int(form.get("timeout", state.timeout)))
        state.max_parallel_jobs = parse_int(form.get("max_parallel_jobs", state.max_parallel_jobs), state.max_parallel_jobs, minimum=1, maximum=20)
        state.retry_base_delay_seconds = max(0.2, float(form.get("retry_base_delay_seconds", state.retry_base_delay_seconds)))
        state.retry_recoverable_only = str(form.get("retry_recoverable_only", "1")).strip() == "1"
        state.enable_chapter_dedupe = str(form.get("enable_chapter_dedupe", "1")).strip() == "1"
        image_fmt = str(form.get("image_output_format", state.image_output_format)).strip().lower()
        state.image_output_format = image_fmt if image_fmt in {"original", "jpg", "webp"} else "original"
        state.image_quality = parse_int(form.get("image_quality", state.image_quality), state.image_quality, minimum=1, maximum=100)
        state.keep_original_images = str(form.get("keep_original_images", "0")).strip() == "1"
        archive_fmt = str(form.get("auto_archive_format", state.auto_archive_format)).strip().lower()
        state.auto_archive_format = archive_fmt if archive_fmt in {"none", "cbz", "zip"} else "none"
        state.write_metadata_sidecar = str(form.get("write_metadata_sidecar", "1")).strip() == "1"
        state.manga_dir_template = str(form.get("manga_dir_template", state.manga_dir_template)).strip() or "{site}/{manga}"
        state.chapter_dir_template = str(form.get("chapter_dir_template", state.chapter_dir_template)).strip() or "{chapter_number}-{chapter_title}"
        state.page_name_template = str(form.get("page_name_template", state.page_name_template)).strip() or "{page:03}"
        state.bandwidth_day_kbps = max(0, int(form.get("bandwidth_day_kbps", state.bandwidth_day_kbps)))
        state.bandwidth_night_kbps = max(0, int(form.get("bandwidth_night_kbps", state.bandwidth_night_kbps)))
        state.night_start_hour = parse_int(form.get("night_start_hour", state.night_start_hour), state.night_start_hour, minimum=0, maximum=23)
        state.night_end_hour = parse_int(form.get("night_end_hour", state.night_end_hour), state.night_end_hour, minimum=0, maximum=23)
        scheduler_enabled_before = state.scheduler_enabled
        state.scheduler_enabled = str(form.get("scheduler_enabled", "0")).strip() == "1"
        state.scheduler_interval_minutes = parse_int(form.get("scheduler_interval_minutes", state.scheduler_interval_minutes), state.scheduler_interval_minutes, minimum=5, maximum=1440)
        state.scheduler_auto_download = str(form.get("scheduler_auto_download", "1")).strip() == "1"
        if state.scheduler_enabled and (not scheduler_enabled_before or not state.scheduler_next_run_at):
            state.schedule_next_run(immediate=False)
        if not state.scheduler_enabled:
            state.scheduler_next_run_at = ""
        state.redis_host = str(form.get("redis_host", "")).strip()
        state.redis_port = parse_int(form.get("redis_port", state.redis_port), state.redis_port, minimum=1, maximum=65535)
        state.redis_db = parse_int(form.get("redis_db", state.redis_db), state.redis_db, minimum=0, maximum=999999)
        state.redis_username = str(form.get("redis_username", "")).strip()
        state.redis_password = str(form.get("redis_password", "")).strip()
        state.cache_ttl_seconds = max(30, int(form.get("cache_ttl_seconds", state.cache_ttl_seconds)))
        state.cache_enabled = str(form.get("cache_enabled", "1")) == "1"
        state.jm_username = str(form.get("jm_username", state.jm_username)).strip()
        state.jm_password = str(form.get("jm_password", state.jm_password)).strip()
        state.jm_manual_logged_in = False
        state.jm_manual_login_user = ""
        state.webhook_enabled = str(form.get("webhook_enabled", "0")).strip() == "1"
        state.webhook_url = str(form.get("webhook_url", state.webhook_url)).strip()
        state.webhook_token = str(form.get("webhook_token", state.webhook_token)).strip()
        state.webhook_event_completed = str(form.get("webhook_event_completed", "1")).strip() == "1"
        state.webhook_event_failed = str(form.get("webhook_event_failed", "1")).strip() == "1"
        state.webhook_event_cancelled = str(form.get("webhook_event_cancelled", "0")).strip() == "1"
        state.webhook_timeout_seconds = parse_int(
            form.get("webhook_timeout_seconds", state.webhook_timeout_seconds),
            state.webhook_timeout_seconds,
            minimum=3,
            maximum=30,
        )
        state.compact_mode_enabled = str(form.get("compact_mode_enabled", "0")).strip() == "1"
        view_mode = str(form.get("manga_view_mode", state.manga_view_mode)).strip().lower()
        state.manga_view_mode = view_mode if view_mode in {"poster", "list"} else "poster"
        enabled_values = []
        if hasattr(form, "getall"):
            enabled_values = [str(v).strip().lower() for v in form.getall("enabled_providers") if str(v).strip()]
        state.set_enabled_providers(set(enabled_values))
        await state.save_settings()
        dispatch_jobs(state)
    except Exception as exc:
        raise build_redirect("/settings", msg=f"保存失败：{exc}")
    raise build_redirect("/settings", msg="设置已保存。")


def cancel_job(state: UIState, job: dict[str, Any]) -> bool:
    status = str(job.get("status") or "")
    if status not in {"queued", "running", "paused", "cancelling"}:
        return False

    job["cancel_requested"] = True
    job["pause_event"].set()
    task = job.get("task")
    if task is None:
        job["status"] = "cancelled"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "排队任务已取消。")
        state.append_job_history(build_job_history_item(job=job, report=None))
        if state.webhook_enabled and state.webhook_url and state.webhook_event_enabled("cancelled"):
            asyncio.create_task(push_job_webhook(state, job, None))
    else:
        job["status"] = "cancelling"
        state.append_job_log(job, "收到取消请求，正在停止任务。")
        if not task.done():
            task.cancel()
    return True


async def handle_queue_action(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    action = str(form.get("action", "") or "").strip().lower()

    selected_ids = list(dict.fromkeys(form_getall_str(form, "job_ids")))
    move_job_id = str(form.get("job_id", "") or "").strip()

    if action in {"move_up", "move_down"}:
        job = state.jobs.get(move_job_id)
        if job is None or str(job.get("status") or "") != "queued":
            raise build_redirect("/queue", msg="只能调整排队中的任务。")
        queued = queued_jobs_sorted(state)
        idx = next((i for i, row in enumerate(queued) if str(row.get("id") or "") == move_job_id), -1)
        if idx < 0:
            raise build_redirect("/queue", msg="任务不存在。")
        target_idx = idx - 1 if action == "move_up" else idx + 1
        if target_idx < 0 or target_idx >= len(queued):
            raise build_redirect("/queue", msg="已到边界，无法继续移动。")
        current = queued[idx]
        target = queued[target_idx]
        current_order = queue_order_value(current)
        target_order = queue_order_value(target)
        current["queue_order"] = target_order
        target["queue_order"] = current_order
        normalize_queue_orders(state)
        dispatch_jobs(state)
        raise build_redirect("/queue", msg="队列顺序已更新。")

    if action == "cancel_selected":
        if not selected_ids:
            raise build_redirect("/queue", msg="请先勾选任务。")
        cancelled = 0
        for job_id in selected_ids:
            job = state.jobs.get(job_id)
            if job is None:
                continue
            if cancel_job(state, job):
                cancelled += 1
        dispatch_jobs(state)
        raise build_redirect("/queue", msg=f"已处理取消请求 {cancelled} 个任务。")

    if action in {"retry_failed", "retry_all_failed"}:
        target_ids = selected_ids
        if action == "retry_all_failed":
            target_ids = [
                str(job.get("id") or "")
                for job in state.jobs.values()
                if str(job.get("status") or "") in {"failed", "cancelled"}
            ]
        if not target_ids:
            raise build_redirect("/queue", msg="没有可重试的失败任务。")
        retried = 0
        last_job_id = ""
        for job_id in target_ids:
            failed_job = state.jobs.get(job_id)
            if failed_job is None:
                continue
            new_job = create_retry_job_from_failed(state, failed_job)
            if new_job is None:
                continue
            retried += 1
            last_job_id = str(new_job.get("id") or "")
        dispatch_jobs(state)
        if retried and last_job_id:
            raise build_redirect("/queue", msg=f"已重试 {retried} 个任务。")
        raise build_redirect("/queue", msg="没有可重试的失败任务。")

    if action == "remove_finished":
        removed = 0
        target_ids = selected_ids
        if not target_ids:
            target_ids = [
                str(job.get("id") or "")
                for job in state.jobs.values()
                if is_job_final(str(job.get("status") or ""))
            ]
        for job_id in target_ids:
            job = state.jobs.get(job_id)
            if job is None:
                continue
            if is_job_final(str(job.get("status") or "")):
                state.jobs.pop(job_id, None)
                removed += 1
        if state.current_job_id and state.current_job_id not in state.jobs:
            state.current_job_id = ""
        raise build_redirect("/queue", msg=f"已移除 {removed} 个已结束任务。")

    raise build_redirect("/queue", msg="未知队列操作。")


async def handle_job_state(request: web.Request) -> web.Response:
    state = get_app_state(request)
    job_id = request.match_info["job_id"]
    job = state.jobs.get(job_id)
    if job is None:
        return web.json_response({"error": "job_not_found"}, status=404)
    return web.json_response(serialize_job(job))


async def handle_job_action(request: web.Request) -> web.Response:
    state = get_app_state(request)
    job_id = request.match_info["job_id"]
    action = request.match_info["action"]
    job = state.jobs.get(job_id)
    if job is None:
        return web.json_response({"ok": False, "message": "job_not_found"}, status=404)

    status = job["status"]
    if action == "pause" and status == "running":
        job["pause_event"].clear()
        job["status"] = "paused"
        state.append_job_log(job, "任务已暂停。")
    elif action == "resume" and status == "paused":
        job["pause_event"].set()
        job["status"] = "running"
        state.append_job_log(job, "任务继续执行。")
    elif action == "cancel":
        cancel_job(state, job)
    dispatch_jobs(state)

    return web.json_response({"ok": True, "state": serialize_job(job)})


async def handle_scheduler_run(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    if state._scheduler_running:
        raise build_redirect("/health", msg="计划任务正在执行中，请稍后刷新。")

    state._scheduler_running = True
    try:
        scanned, enqueued = await run_scheduler_cycle(state)
        state.scheduler_last_run_at = now_iso()
        if state.scheduler_enabled:
            state.schedule_next_run(immediate=False)
        await state.save_settings()
    except Exception as exc:
        state._scheduler_running = False
        raise build_redirect("/health", msg=f"计划任务执行失败：{exc}")
    finally:
        state._scheduler_running = False

    raise build_redirect("/health", msg=f"计划任务执行完成：检查 {scanned} 本，新增任务 {enqueued} 个。")


async def on_shutdown(app: web.Application) -> None:
    state: UIState = app["state"]
    scheduler_task: Optional[asyncio.Task[Any]] = app.get("scheduler_task")
    if scheduler_task is not None and not scheduler_task.done():
        scheduler_task.cancel()
        await asyncio.gather(scheduler_task, return_exceptions=True)

    waits: list[asyncio.Task[Any]] = []
    for job in state.jobs.values():
        task = job.get("task")
        if task is not None and not task.done():
            job["cancel_requested"] = True
            job["pause_event"].set()
            task.cancel()
            waits.append(task)
    if waits:
        await asyncio.gather(*waits, return_exceptions=True)


async def on_startup(app: web.Application) -> None:
    state: UIState = app["state"]
    dispatch_jobs(state)
    if state.scheduler_enabled and not state.scheduler_next_run_at:
        state.schedule_next_run(immediate=False)
        await state.save_settings()
    app["scheduler_task"] = asyncio.create_task(scheduler_loop(app))


def create_app() -> web.Application:
    ensure_providers_loaded()
    ensure_data_dir_ready()
    state = UIState()
    state.load()
    state.output_dir.mkdir(parents=True, exist_ok=True)

    app = web.Application(middlewares=[flash_message_middleware])
    app["state"] = state

    app.add_routes(
        [
            web.get("/", handle_root),
            web.get("/dashboard", handle_dashboard),
            web.get("/progress", handle_progress),
            web.get("/queue", handle_queue),
            web.post("/queue/action", handle_queue_action),
            web.post("/search", handle_search),
            web.post("/search/action", handle_search_action),
            web.post("/dashboard/import", handle_batch_import),
            web.get("/bookshelf", handle_bookshelf),
            web.get("/bookshelf/export", handle_bookshelf_export),
            web.post("/bookshelf/import", handle_bookshelf_import),
            web.post("/bookshelf/jm-login", handle_bookshelf_jm_login),
            web.post("/bookshelf/jm-logout", handle_bookshelf_jm_logout),
            web.get("/follow", handle_follow),
            web.get("/follow/summary", handle_follow_summary),
            web.post("/follow/bulk", handle_follow_bulk),
            web.get("/health", handle_health),
            web.post("/bookshelf/sync-jm-favorites", handle_bookshelf_sync_jm_favorites),
            web.post("/bookshelf/bulk", handle_bookshelf_bulk),
            web.post("/bookshelf/{book_id}/{action}", handle_book_action),
            web.get("/api/books/{book_id}", handle_book_card),
            web.get("/settings", handle_settings_get),
            web.post("/settings", handle_settings_post),
            web.get("/job/{job_id}/state", handle_job_state),
            web.post("/job/{job_id}/{action}", handle_job_action),
            web.post("/scheduler/run", handle_scheduler_run),
        ]
    )
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app
