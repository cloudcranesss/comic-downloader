
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
from downloaders.toonily import (
    Chapter,
    DownloadReport,
    ToonilyAsyncDownloader,
    normalize_proxy_url,
    normalize_url,
)
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


def mask_proxy_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
        if not parsed.scheme or not parsed.netloc:
            return text
        host = (parsed.hostname or "").strip()
        if parsed.port:
            host = f"{host}:{parsed.port}"
        if not host:
            host = parsed.netloc
        return f"{parsed.scheme}://{host}"
    except Exception:
        return text


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
    from app.webui_services import fetch_series_snapshot_toonily, search_toonily

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


