
import argparse
import asyncio
import json
import math
import os
import re
import uuid
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote_plus, urlencode, urljoin

from aiohttp import web
from bs4 import BeautifulSoup

from jm_async_downloader import (
    JMAsyncDownloader,
    fetch_series_snapshot_jm,
    jm_available,
    jm_unavailable_reason,
    search_jm,
    sync_jm_favorites,
)
from toonily_async_downloader import Chapter, DownloadReport, ToonilyAsyncDownloader, normalize_url


BASE_DIR = Path(__file__).resolve().parent
BOOKSHELF_FILE = BASE_DIR / "bookshelf.json"
SETTINGS_FILE = BASE_DIR / "webui_settings.json"
DEFAULT_PROVIDER_ID = "toonily"
JM_PROVIDER_ENABLED = jm_available()
JM_PROVIDER_DISABLED_REASON = jm_unavailable_reason()


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


def format_chapter_number(num: Optional[float]) -> str:
    if num is None:
        return "-"
    if float(num).is_integer():
        return str(int(num))
    return str(num)


class SiteProvider:
    provider_id = ""
    display_name = ""
    enabled = True
    disabled_reason = ""

    def ui_label(self) -> str:
        if self.enabled:
            return self.display_name
        return f"{self.display_name}（未启用）"

    async def search(self, state: "UIState", keyword: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_series_snapshot(
        self,
        state: "UIState",
        series_url: str,
        logger: Optional[Callable[[str], None]] = None,
    ) -> tuple[str, list[Chapter]]:
        raise NotImplementedError

    def create_downloader(
        self,
        state: "UIState",
        *,
        series_url: str,
        chapter_selector: str,
        chapter_urls: list[str],
        logger: Callable[[str], None],
        progress_callback: Callable[[dict[str, Any]], None],
        pause_waiter: Callable[[], Any],
        cancel_checker: Callable[[], bool],
    ) -> Any:
        raise NotImplementedError


class ToonilyProvider(SiteProvider):
    provider_id = "toonily"
    display_name = "Toonily"
    enabled = True

    async def search(self, state: "UIState", keyword: str) -> list[dict[str, Any]]:
        return await search_toonily(state, keyword)

    async def fetch_series_snapshot(
        self,
        state: "UIState",
        series_url: str,
        logger: Optional[Callable[[str], None]] = None,
    ) -> tuple[str, list[Chapter]]:
        return await fetch_series_snapshot_toonily(state, series_url, logger=logger)

    def create_downloader(
        self,
        state: "UIState",
        *,
        series_url: str,
        chapter_selector: str,
        chapter_urls: list[str],
        logger: Callable[[str], None],
        progress_callback: Callable[[dict[str, Any]], None],
        pause_waiter: Callable[[], Any],
        cancel_checker: Callable[[], bool],
    ) -> ToonilyAsyncDownloader:
        return ToonilyAsyncDownloader(
            series_url=series_url,
            output_dir=state.output_dir,
            chapter_selector=chapter_selector,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            chapter_urls=chapter_urls,
            write_failed_file=True,
            logger=logger,
            progress_callback=progress_callback,
            pause_waiter=pause_waiter,
            cancel_checker=cancel_checker,
            cache_enabled=state.cache_enabled,
            redis_url=state.redis_url,
            redis_username=state.redis_username,
            redis_password=state.redis_password,
            cache_ttl_seconds=state.cache_ttl_seconds,
        )


class JMProvider(SiteProvider):
    provider_id = "jmcomic"
    display_name = "JMComic"
    enabled = JM_PROVIDER_ENABLED
    disabled_reason = JM_PROVIDER_DISABLED_REASON

    async def search(self, state: "UIState", keyword: str) -> list[dict[str, Any]]:
        return await search_jm(
            keyword,
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
        )

    async def fetch_series_snapshot(
        self,
        state: "UIState",
        series_url: str,
        logger: Optional[Callable[[str], None]] = None,
    ) -> tuple[str, list[Chapter]]:
        return await fetch_series_snapshot_jm(
            series_url,
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
        )

    def create_downloader(
        self,
        state: "UIState",
        *,
        series_url: str,
        chapter_selector: str,
        chapter_urls: list[str],
        logger: Callable[[str], None],
        progress_callback: Callable[[dict[str, Any]], None],
        pause_waiter: Callable[[], Any],
        cancel_checker: Callable[[], bool],
    ) -> Any:
        return JMAsyncDownloader(
            series_url=series_url,
            output_dir=state.output_dir,
            chapter_selector=chapter_selector,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            chapter_urls=chapter_urls,
            write_failed_file=True,
            logger=logger,
            progress_callback=progress_callback,
            pause_waiter=pause_waiter,
            cancel_checker=cancel_checker,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
        )


PROVIDERS: dict[str, SiteProvider] = {
    ToonilyProvider.provider_id: ToonilyProvider(),
    JMProvider.provider_id: JMProvider(),
}


def get_provider(provider_id: str) -> SiteProvider:
    key = (provider_id or "").strip().lower()
    return PROVIDERS.get(key) or PROVIDERS[DEFAULT_PROVIDER_ID]


def provider_name(provider_id: str) -> str:
    key = (provider_id or "").strip().lower()
    provider = PROVIDERS.get(key)
    if provider is not None:
        return provider.display_name
    return key or DEFAULT_PROVIDER_ID


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
        self.current_job_id: Optional[str] = None
        self.last_search_query = ""
        self.last_search_provider = DEFAULT_PROVIDER_ID
        self.last_search_results: list[dict[str, Any]] = []

        self.output_dir = BASE_DIR / "downloads"
        self.chapter_concurrency = 3
        self.image_concurrency = 10
        self.retries = 3
        self.timeout = 45

        self.redis_url = os.getenv("TOONILY_REDIS_URL", "").strip()
        self.redis_username = os.getenv("TOONILY_REDIS_USERNAME", "").strip()
        self.redis_password = os.getenv("TOONILY_REDIS_PASSWORD", "").strip()
        self.cache_enabled = True
        self.cache_ttl_seconds = 900
        self.jm_username = os.getenv("JM_USERNAME", "").strip()
        self.jm_password = os.getenv("JM_PASSWORD", "").strip()

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
                self.redis_url = str(raw.get("redis_url", self.redis_url)).strip()
                self.redis_username = str(raw.get("redis_username", self.redis_username)).strip()
                self.redis_password = str(raw.get("redis_password", self.redis_password)).strip()
                self.cache_enabled = bool(raw.get("cache_enabled", self.cache_enabled))
                self.cache_ttl_seconds = max(30, int(raw.get("cache_ttl_seconds", self.cache_ttl_seconds)))
                self.jm_username = str(raw.get("jm_username", self.jm_username)).strip()
                self.jm_password = str(raw.get("jm_password", self.jm_password)).strip()
            except Exception:
                pass

        if BOOKSHELF_FILE.exists():
            try:
                raw = json.loads(BOOKSHELF_FILE.read_text(encoding="utf-8"))
                items: list[dict[str, Any]] = raw if isinstance(raw, list) else []
                for item in items:
                    book = self._normalize_book_item(item)
                    self.bookshelf[book["id"]] = book
            except Exception:
                self.bookshelf = {}

    async def save_settings(self) -> None:
        payload = {
            "output_dir": str(self.output_dir),
            "chapter_concurrency": self.chapter_concurrency,
            "image_concurrency": self.image_concurrency,
            "retries": self.retries,
            "timeout": self.timeout,
            "redis_url": self.redis_url,
            "redis_username": self.redis_username,
            "redis_password": self.redis_password,
            "cache_enabled": self.cache_enabled,
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "jm_username": self.jm_username,
            "jm_password": self.jm_password,
        }
        async with self._save_lock:
            SETTINGS_FILE.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

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
    ) -> tuple[dict[str, Any], bool]:
        pid = (provider_id or DEFAULT_PROVIDER_ID).strip().lower() or DEFAULT_PROVIDER_ID
        normalized = normalize_url(series_url)
        for book in self.bookshelf.values():
            if (
                book.get("provider_id", DEFAULT_PROVIDER_ID) == pid
                and normalize_url(book.get("series_url", "")) == normalized
            ):
                if title:
                    book["title"] = title
                if cover:
                    book["cover"] = cover
                return book, False

        book = self._normalize_book_item(
            {
                "id": uuid.uuid4().hex[:12],
                "provider_id": pid,
                "title": title or "未命名漫画",
                "series_url": normalized,
                "cover": cover,
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


def get_app_state(request: web.Request) -> UIState:
    return request.app["state"]


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
        cache_enabled=state.cache_enabled,
        redis_url=state.redis_url,
        redis_username=state.redis_username,
        redis_password=state.redis_password,
        cache_ttl_seconds=state.cache_ttl_seconds,
    )
    try:
        return await downloader.fetch_html(url)
    finally:
        await downloader.close()


def parse_search_results(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "div.page-item-detail.manga h3.h5 a",
        "div.c-tabs-item__content .post-title h3 a",
        "div.c-tabs-item__content .post-title a",
        "div.post-title h3 a",
        "h3.h5 a[href*='/serie/']",
        "a[href*='/serie/']",
    ]

    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    for selector in selectors:
        for a in soup.select(selector):
            href = (a.get("href") or "").strip()
            if "/serie/" not in href:
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
                if img is not None:
                    cover = (
                        img.get("data-src")
                        or img.get("data-lazy-src")
                        or img.get("src")
                        or ""
                    ).strip()
                    if cover.startswith("//"):
                        cover = f"https:{cover}"
                    elif cover.startswith("/"):
                        cover = urljoin("https://toonily.com", cover)
                    if cover and not cover.startswith(("http://", "https://")):
                        cover = ""

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
    return results


def extract_series_url_hint(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    og_url = soup.select_one("meta[property='og:url']")
    if og_url is not None:
        candidate = normalize_url(str(og_url.get("content") or ""))
        if "/serie/" in candidate:
            return candidate

    match = re.search(r'"base_url"\s*:\s*"([^"]+?/serie/[^"]+?)"', html)
    if match:
        candidate = normalize_url(match.group(1).replace("\\/", "/"))
        if "/serie/" in candidate:
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

    query_urls: list[str] = []
    slug = slugify_keyword(keyword)
    if slug:
        query_urls.append(f"https://toonily.com/search/{slug}")
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
                merged.append(
                    {
                        "title": hinted_title,
                        "url": hinted_series_url,
                        "latest": "",
                        "cover": "",
                    }
                )
                seen.add(hinted_series_url)
        if merged:
            break

    if not merged and keyword.startswith(("http://", "https://")) and "/serie/" in keyword:
        merged.append(
            {
                "title": keyword.rstrip("/").split("/")[-1].replace("-", " "),
                "url": normalize_url(keyword),
                "latest": "",
                "cover": "",
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
        cache_enabled=state.cache_enabled,
        redis_url=state.redis_url,
        redis_username=state.redis_username,
        redis_password=state.redis_password,
        cache_ttl_seconds=state.cache_ttl_seconds,
    )
    try:
        return await downloader.get_series_details()
    finally:
        await downloader.close()


async def search_by_provider(state: UIState, provider_id: str, keyword: str) -> list[dict[str, Any]]:
    provider = get_provider(provider_id)
    if not provider.enabled:
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
    if not provider.enabled:
        raise RuntimeError(provider.disabled_reason or "该站点未启用。")
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


def build_redirect(path: str, **params: Any) -> web.HTTPSeeOther:
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
    return web.HTTPSeeOther(location=location)

def render_layout(*, title: str, active_nav: str, body: str, script: str = "") -> str:
    nav_items = [
        ("dashboard", "主页", "/dashboard"),
        ("progress", "进度", "/progress"),
        ("bookshelf", "书架", "/bookshelf"),
        ("settings", "设置", "/settings"),
    ]
    nav_html_parts = []
    for key, label, href in nav_items:
        cls = "nav-link active" if key == active_nav else "nav-link"
        nav_html_parts.append(f'<a class="{cls}" href="{href}">{escape(label)}</a>')
    nav_html = "\n".join(nav_html_parts)

    return (
        "<!doctype html>\n"
        "<html lang=\"zh-CN\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        f"  <title>{escape(title)}</title>\n"
        "  <style>\n"
        "    :root {\n"
        "      --bg-a: #0f172a;\n"
        "      --bg-b: #0b1f2c;\n"
        "      --accent: #f59e0b;\n"
        "      --accent-2: #14b8a6;\n"
        "      --panel: rgba(13, 25, 38, 0.74);\n"
        "      --panel-border: rgba(255, 255, 255, 0.12);\n"
        "      --text: #e5ecf5;\n"
        "      --muted: #9fb2c8;\n"
        "      --danger: #fb7185;\n"
        "      --ok: #34d399;\n"
        "      --shadow: 0 16px 38px rgba(0, 0, 0, 0.35);\n"
        "    }\n"
        "    * { box-sizing: border-box; }\n"
        "    body {\n"
        "      margin: 0;\n"
        "      color: var(--text);\n"
        "      font-family: \"Noto Sans SC\", \"Source Han Sans SC\", \"PingFang SC\", \"Microsoft YaHei\", sans-serif;\n"
        "      background: radial-gradient(circle at 12% 18%, #0e3344 0%, transparent 36%),\n"
        "                  radial-gradient(circle at 92% 4%, #1f3c2a 0%, transparent 33%),\n"
        "                  linear-gradient(145deg, var(--bg-a), var(--bg-b));\n"
        "      min-height: 100vh;\n"
        "    }\n"
        "    .shell { width: min(1200px, calc(100% - 28px)); margin: 20px auto 32px; }\n"
        "    .top {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: space-between;\n"
        "      gap: 14px;\n"
        "      margin-bottom: 14px;\n"
        "    }\n"
        "    .logo { font-size: 24px; font-weight: 800; letter-spacing: 0.8px; }\n"
        "    .logo span { color: var(--accent); }\n"
        "    .nav { display: flex; gap: 10px; flex-wrap: wrap; }\n"
        "    .nav-link {\n"
        "      text-decoration: none;\n"
        "      color: var(--muted);\n"
        "      padding: 9px 14px;\n"
        "      border-radius: 12px;\n"
        "      border: 1px solid transparent;\n"
        "      transition: 0.2s ease;\n"
        "      background: rgba(255,255,255,0.03);\n"
        "    }\n"
        "    .nav-link:hover { color: #fff; border-color: rgba(255,255,255,0.18); }\n"
        "    .nav-link.active {\n"
        "      color: #111827;\n"
        "      font-weight: 700;\n"
        "      background: linear-gradient(90deg, var(--accent), #fde68a);\n"
        "      border-color: rgba(255,255,255,0.2);\n"
        "    }\n"
        "    .panel {\n"
        "      background: var(--panel);\n"
        "      border: 1px solid var(--panel-border);\n"
        "      border-radius: 16px;\n"
        "      box-shadow: var(--shadow);\n"
        "      backdrop-filter: blur(8px);\n"
        "      padding: 16px;\n"
        "      margin-bottom: 16px;\n"
        "    }\n"
        "    .title { margin: 0 0 10px; font-size: 20px; font-weight: 800; }\n"
        "    .subtle { color: var(--muted); font-size: 14px; }\n"
        "    .msg {\n"
        "      position: fixed;\n"
        "      top: 14px;\n"
        "      left: 50%;\n"
        "      transform: translate(-50%, -10px);\n"
        "      z-index: 9999;\n"
        "      width: min(760px, calc(100% - 20px));\n"
        "      border-radius: 12px;\n"
        "      background: rgba(20, 184, 166, 0.18);\n"
        "      border: 1px solid rgba(20, 184, 166, 0.45);\n"
        "      box-shadow: 0 12px 28px rgba(2, 6, 23, 0.36);\n"
        "      color: #d1fae5;\n"
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
        "      color: #d1fae5;\n"
        "      font-size: 18px;\n"
        "      line-height: 1;\n"
        "      cursor: pointer;\n"
        "      padding: 0 2px;\n"
        "      opacity: 0.85;\n"
        "    }\n"
        "    .msg-close:hover { opacity: 1; }\n"
        "    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }\n"
        "    .result-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fill, minmax(180px, 220px));\n"
        "      gap: 12px;\n"
        "      justify-content: start;\n"
        "      align-items: stretch;\n"
        "    }\n"
        "    .search-form { display: flex; gap: 10px; flex-wrap: wrap; }\n"
        "    .input,\n"
        "    .select {\n"
        "      background: rgba(255,255,255,0.05);\n"
        "      color: var(--text);\n"
        "      border: 1px solid rgba(255,255,255,0.2);\n"
        "      border-radius: 10px;\n"
        "      padding: 10px 12px;\n"
        "      min-height: 42px;\n"
        "      width: 100%;\n"
        "    }\n"
        "    .input::placeholder { color: #86a1b9; }\n"
        "    .btn {\n"
        "      border: 0;\n"
        "      border-radius: 10px;\n"
        "      padding: 10px 14px;\n"
        "      cursor: pointer;\n"
        "      color: #081018;\n"
        "      font-weight: 700;\n"
        "      background: linear-gradient(90deg, var(--accent), #fcd34d);\n"
        "      transition: transform 0.12s ease, filter 0.2s ease;\n"
        "    }\n"
        "    .btn:hover { transform: translateY(-1px); filter: brightness(1.05); }\n"
        "    .btn.secondary {\n"
        "      background: linear-gradient(90deg, var(--accent-2), #5eead4);\n"
        "    }\n"
        "    .btn.ghost {\n"
        "      color: var(--text);\n"
        "      border: 1px solid rgba(255,255,255,0.24);\n"
        "      background: rgba(255,255,255,0.06);\n"
        "    }\n"
        "    .btn.warn { background: linear-gradient(90deg, #ef4444, #fb7185); color: #fff; }\n"
        "    .btn[disabled] { opacity: 0.52; cursor: not-allowed; transform: none; }\n"
        "    .result-card {\n"
        "      border-radius: 14px;\n"
        "      border: 1px solid rgba(255,255,255,0.12);\n"
        "      background: rgba(255,255,255,0.03);\n"
        "      padding: 12px;\n"
        "      display: flex;\n"
        "      flex-direction: column;\n"
        "      gap: 8px;\n"
        "      min-height: 180px;\n"
        "      height: 100%;\n"
        "    }\n"
        "    .result-cover-wrap {\n"
        "      width: 100%;\n"
        "      aspect-ratio: 3 / 4;\n"
        "      border-radius: 10px;\n"
        "      overflow: hidden;\n"
        "      background: linear-gradient(145deg, rgba(148,163,184,0.15), rgba(148,163,184,0.05));\n"
        "      border: 1px solid rgba(255,255,255,0.12);\n"
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
        "      color: var(--muted);\n"
        "      font-size: 13px;\n"
        "      letter-spacing: 0.4px;\n"
        "    }\n"
        "    .result-title {\n"
        "      font-size: 16px;\n"
        "      font-weight: 700;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 4);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 4;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .link { color: #93c5fd; text-decoration: none; }\n"
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
        "    .actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: auto; }\n"
        "    .actions form { margin: 0; }\n"
        "    .job-meta { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 10px; color: var(--muted); }\n"
        "    .badge {\n"
        "      display: inline-block;\n"
        "      padding: 3px 10px;\n"
        "      border-radius: 999px;\n"
        "      border: 1px solid rgba(255,255,255,0.2);\n"
        "      font-size: 12px;\n"
        "      color: #dbeafe;\n"
        "      background: rgba(96,165,250,0.2);\n"
        "    }\n"
        "    .progress {\n"
        "      width: 100%;\n"
        "      height: 10px;\n"
        "      border-radius: 999px;\n"
        "      background: rgba(255,255,255,0.1);\n"
        "      overflow: hidden;\n"
        "      margin: 6px 0 12px;\n"
        "    }\n"
        "    .bar {\n"
        "      height: 100%;\n"
        "      width: 0%;\n"
        "      background: linear-gradient(90deg, #22d3ee, #34d399);\n"
        "      transition: width 0.3s ease;\n"
        "    }\n"
        "    .log-box {\n"
        "      height: 260px;\n"
        "      border-radius: 12px;\n"
        "      border: 1px solid rgba(255,255,255,0.14);\n"
        "      background: rgba(1, 7, 16, 0.7);\n"
        "      color: #d4e4f7;\n"
        "      padding: 10px;\n"
        "      overflow: auto;\n"
        "      font-family: Consolas, \"Courier New\", monospace;\n"
        "      font-size: 12px;\n"
        "      white-space: pre-wrap;\n"
        "      line-height: 1.45;\n"
        "    }\n"
        "    .book-card {\n"
        "      padding: 10px;\n"
        "      border: 1px solid rgba(255,255,255,0.12);\n"
        "      border-radius: 14px;\n"
        "      background: rgba(255,255,255,0.03);\n"
        "      display: flex;\n"
        "      flex-direction: column;\n"
        "      gap: 8px;\n"
        "      height: 100%;\n"
        "    }\n"
        "    .bookshelf-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fill, minmax(220px, 260px));\n"
        "      gap: 12px;\n"
        "      justify-content: start;\n"
        "      align-items: stretch;\n"
        "    }\n"
        "    .book-card .result-cover-wrap {\n"
        "      width: min(122px, 100%);\n"
        "      margin: 0 auto 8px;\n"
        "    }\n"
        "    .book-title {\n"
        "      margin: 0;\n"
        "      font-size: 16px;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 3);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 3;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .book-meta-list { display: flex; flex-direction: column; gap: 4px; }\n"
        "    .book-meta { font-size: 12px; color: var(--muted); margin: 0; line-height: 1.4; }\n"
        "    .book-meta.clamp-1 { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n"
        "    .book-actions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: auto; }\n"
        "    .book-actions form { margin: 0; }\n"
        "    .book-actions .btn { padding: 7px 10px; min-height: 34px; font-size: 12px; }\n"
        "    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 10px; }\n"
        "    .stat-card {\n"
        "      border: 1px solid rgba(255,255,255,0.14);\n"
        "      background: rgba(255,255,255,0.04);\n"
        "      border-radius: 12px;\n"
        "      padding: 10px;\n"
        "    }\n"
        "    .stat-label { color: var(--muted); font-size: 12px; margin-bottom: 4px; }\n"
        "    .stat-value { font-size: 18px; font-weight: 800; }\n"
        "    .settings-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));\n"
        "      gap: 12px;\n"
        "    }\n"
        "    .settings-section {\n"
        "      border: 1px solid rgba(255,255,255,0.14);\n"
        "      border-radius: 12px;\n"
        "      padding: 12px;\n"
        "      margin-bottom: 12px;\n"
        "      background: rgba(255,255,255,0.03);\n"
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
        "      border: 1px solid rgba(255,255,255,0.2);\n"
        "      background: rgba(255,255,255,0.06);\n"
        "      color: #dbeafe;\n"
        "    }\n"
        "    .site-badge.toonily { color: #fde68a; border-color: rgba(245, 158, 11, 0.5); }\n"
        "    .site-badge.jmcomic { color: #99f6e4; border-color: rgba(20, 184, 166, 0.5); }\n"
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
        "    @media (max-width: 780px) {\n"
        "      .shell { width: calc(100% - 16px); margin-top: 12px; }\n"
        "      .top { flex-direction: column; align-items: flex-start; }\n"
        "      .search-form { flex-direction: column; }\n"
        "      .result-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }\n"
        "      .bookshelf-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }\n"
        "      .result-card { max-width: none; }\n"
        "      .book-card .result-cover-wrap { width: min(100px, 100%); }\n"
        "      .settings-grid { grid-template-columns: 1fr; }\n"
        "    }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        "  <div class=\"shell\">\n"
        "    <div class=\"top\">\n"
        "      <div class=\"logo\">漫画下载</div>\n"
        f"      <nav class=\"nav\">{nav_html}</nav>\n"
        "    </div>\n"
        f"    {body}\n"
        "  </div>\n"
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
        else f"<a class=\"btn ghost\" href=\"/progress?job={escape(job['id'])}\">打开完整进度界面</a>"
    )
    back_home = "<a class=\"btn ghost\" href=\"/dashboard\">返回主页</a>" if full_page else ""
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
        "<div class=\"actions\">"
        "<button id=\"btn-pause\" class=\"btn ghost\" type=\"button\">暂停</button>"
        "<button id=\"btn-resume\" class=\"btn secondary\" type=\"button\">继续</button>"
        "<button id=\"btn-cancel\" class=\"btn warn\" type=\"button\">取消</button>"
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
    selected_job_id: str,
    *,
    search_page: int,
    search_page_size: int,
) -> str:
    results_html = ""
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
            if selected_job_id:
                params["job"] = selected_job_id
            return f"/dashboard?{urlencode(params)}"

        cards: list[str] = []
        for item in page_results:
            title = str(item.get("title", "") or "")
            url = str(item.get("url", "") or "")
            latest = str(item.get("latest", "") or "")
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

            cover_html = (
                f"<div class=\"result-cover-wrap\"><img class=\"result-cover\" src=\"{escape(cover_url)}\" alt=\"{escape(title)}\" loading=\"lazy\" /></div>"
                if cover_url
                else "<div class=\"result-cover-wrap\"><div class=\"result-cover-empty\">暂无封面</div></div>"
            )
            card = (
                "<div class=\"result-card\">"
                f"{cover_html}"
                f"<div class=\"result-title\" title=\"{escape(title)}\">{escape(title)}</div>"
                f"<div>{provider_badge}</div>"
                f"<a class=\"link result-link\" href=\"{escape(url)}\" title=\"{escape(url)}\" target=\"_blank\" rel=\"noreferrer\">{escape(url)}</a>"
                f"<div class=\"subtle result-latest\" title=\"最新章节：{escape(latest or '-')}\">最新章节：{escape(latest or '-')}</div>"
                "<div class=\"actions\">"
                "<form method=\"post\" action=\"/search/action\">"
                f"<input type=\"hidden\" name=\"provider_id\" value=\"{escape(provider_id)}\" />"
                f"<input type=\"hidden\" name=\"title\" value=\"{escape(title)}\" />"
                f"<input type=\"hidden\" name=\"url\" value=\"{escape(url)}\" />"
                f"<input type=\"hidden\" name=\"cover\" value=\"{escape(cover_url)}\" />"
                f"<input type=\"hidden\" name=\"sp\" value=\"{page}\" />"
                f"<input type=\"hidden\" name=\"sps\" value=\"{page_size}\" />"
                "<input type=\"hidden\" name=\"action\" value=\"add_bookshelf\" />"
                "<button class=\"btn ghost\" type=\"submit\">加入书架</button>"
                "</form>"
                "<form method=\"post\" action=\"/search/action\">"
                f"<input type=\"hidden\" name=\"provider_id\" value=\"{escape(provider_id)}\" />"
                f"<input type=\"hidden\" name=\"title\" value=\"{escape(title)}\" />"
                f"<input type=\"hidden\" name=\"url\" value=\"{escape(url)}\" />"
                "<input type=\"hidden\" name=\"action\" value=\"download_all\" />"
                "<button class=\"btn secondary\" type=\"submit\">下载全部</button>"
                "</form>"
                "<form method=\"post\" action=\"/search/action\">"
                f"<input type=\"hidden\" name=\"provider_id\" value=\"{escape(provider_id)}\" />"
                f"<input type=\"hidden\" name=\"title\" value=\"{escape(title)}\" />"
                f"<input type=\"hidden\" name=\"url\" value=\"{escape(url)}\" />"
                f"<input type=\"hidden\" name=\"cover\" value=\"{escape(cover_url)}\" />"
                "<input type=\"hidden\" name=\"action\" value=\"follow_download\" />"
                "<button class=\"btn\" type=\"submit\">追更下载</button>"
                "</form>"
                "</div>"
                "</div>"
            )
            cards.append(card)

        page_size_options = [8, 12, 16, 24, 40]
        page_size_html = "".join(
            (
                f"<option value=\"{size}\" selected>{size}</option>"
                if size == page_size
                else f"<option value=\"{size}\">{size}</option>"
            )
            for size in page_size_options
        )

        pager_html = (
            "<div class=\"pager\">"
            "<form class=\"pager-form\" method=\"get\" action=\"/dashboard\">"
            "<label style=\"margin:0;color:var(--muted);\">每页</label>"
            f"<select class=\"select\" style=\"width:96px;min-width:96px;\" name=\"sps\" onchange=\"this.form.submit()\">{page_size_html}</select>"
            "<input type=\"hidden\" name=\"sp\" value=\"1\" />"
            + (f"<input type=\"hidden\" name=\"job\" value=\"{escape(selected_job_id)}\" />" if selected_job_id else "")
            + "</form>"
            + "<div class=\"pager-links\">"
            + (
                f"<a class=\"btn ghost\" href=\"{escape(dashboard_page_url(page - 1))}\">上一页</a>"
                if page > 1
                else "<button class=\"btn ghost\" type=\"button\" disabled>上一页</button>"
            )
            + f"<span class=\"subtle\">第 {page}/{page_count} 页</span>"
            + (
                f"<a class=\"btn ghost\" href=\"{escape(dashboard_page_url(page + 1))}\">下一页</a>"
                if page < page_count
                else "<button class=\"btn ghost\" type=\"button\" disabled>下一页</button>"
            )
            + "</div>"
            + "</div>"
        )

        results_html = (
            "<div class=\"panel\">"
            "<h2 class=\"title\">搜索结果</h2>"
            f"<div class=\"subtle\" style=\"margin-bottom:10px;\">关键词：{escape(state.last_search_query)}，共 {total_results} 条</div>"
            f"{pager_html}"
            f"<div class=\"result-grid\">{''.join(cards)}</div>"
            "</div>"
        )

    job_html = "<div class=\"panel\"><h2 class=\"title\">当前任务</h2><div class=\"subtle\">暂无任务，可在搜索结果中直接创建下载。</div></div>"
    script = ""
    if selected_job_id and selected_job_id in state.jobs:
        job_html, script = render_job_panel(state.jobs[selected_job_id], heading="当前任务", full_page=False)

    provider_options: list[str] = []
    for provider in PROVIDERS.values():
        selected = " selected" if provider.provider_id == state.last_search_provider else ""
        provider_options.append(
            f"<option value=\"{escape(provider.provider_id)}\"{selected}>{escape(provider.ui_label())}</option>"
        )
    provider_select = "".join(provider_options)

    body = (
        render_message(msg)
        + "<div class=\"panel\">"
        + "<h2 class=\"title\">搜索漫画并操作</h2>"
        + "<div class=\"subtle\" style=\"margin-bottom:10px;\">主页已合并：创建下载任务与追更入口都放在搜索结果内。</div>"
        + "<form class=\"search-form\" method=\"post\" action=\"/search\">"
        + f"<input class=\"input\" style=\"flex:1 1 460px;\" name=\"query\" placeholder=\"输入漫画名称（例如 Wireless Onahole）\" value=\"{escape(state.last_search_query)}\" />"
        + f"<select class=\"select\" style=\"flex:0 0 220px;\" name=\"provider_id\">{provider_select}</select>"
        + "<select class=\"select\" style=\"flex:0 0 140px;\" name=\"page_size\">"
        + "".join(
            (
                f"<option value=\"{size}\" selected>每页 {size}</option>"
                if size == max(4, min(40, int(search_page_size)))
                else f"<option value=\"{size}\">每页 {size}</option>"
            )
            for size in (8, 12, 16, 24, 40)
        )
        + "</select>"
        + "<button class=\"btn\" type=\"submit\">搜索</button>"
        + "</form>"
        + "</div>"
        + results_html
        + job_html
    )
    return render_layout(title="漫画下载 - 主页", active_nav="dashboard", body=body, script=script)


def render_progress(state: UIState, msg: str, selected_job_id: str) -> str:
    body = render_message(msg)
    script = ""

    job = state.jobs.get(selected_job_id) if selected_job_id else None
    if job is None and state.current_job_id:
        job = state.jobs.get(state.current_job_id)

    if job is None:
        body += (
            "<div class=\"panel\">"
            "<h2 class=\"title\">任务进度</h2>"
            "<div class=\"subtle\">当前没有活跃任务。请前往主页搜索漫画并创建下载任务。</div>"
            "<div style=\"margin-top:10px;\"><a class=\"btn\" href=\"/dashboard\">去主页创建任务</a></div>"
            "</div>"
        )
    else:
        panel_html, script = render_job_panel(job, heading="任务进度", full_page=True)
        body += panel_html

    return render_layout(title="漫画下载 - 进度", active_nav="progress", body=body, script=script)


def render_bookshelf(
    state: UIState,
    msg: str,
    *,
    bookshelf_page: int,
    bookshelf_page_size: int,
) -> str:
    jm_provider = get_provider("jmcomic")
    has_jm_login = bool(state.jm_username and state.jm_password)

    all_books = state.list_books()
    total_books = len(all_books)
    page_size = max(6, min(60, int(bookshelf_page_size)))
    page_count = max(1, math.ceil(total_books / page_size)) if total_books else 1
    page = max(1, min(int(bookshelf_page), page_count))
    start = (page - 1) * page_size
    end = start + page_size
    page_books = all_books[start:end]

    def bookshelf_page_url(target_page: int) -> str:
        return f"/bookshelf?{urlencode({'bp': str(target_page), 'bps': str(page_size)})}"

    sync_panel_parts: list[str] = []
    if jm_provider.enabled:
        sync_panel_parts.append(
            "<div class=\"subtle\" style=\"margin-bottom:8px;\">"
            + (
                f"JM 登录已配置：{escape(state.jm_username)}"
                if has_jm_login
                else "JM 登录未配置，请先到设置页填写用户名和密码。"
            )
            + "</div>"
        )
        if has_jm_login:
            sync_panel_parts.append(
                "<form method=\"post\" action=\"/bookshelf/sync-jm-favorites\">"
                f"<input type=\"hidden\" name=\"bp\" value=\"{page}\" />"
                f"<input type=\"hidden\" name=\"bps\" value=\"{page_size}\" />"
                "<button class=\"btn secondary\" type=\"submit\">同步 JM 收藏到书架</button>"
                "</form>"
            )
        else:
            sync_panel_parts.append("<a class=\"btn ghost\" href=\"/settings\">去设置 JM 账号</a>")
    else:
        reason = jm_provider.disabled_reason or "未知原因"
        sync_panel_parts.append(
            f"<div class=\"subtle\" style=\"margin-bottom:8px;color:#fecaca;\">JM 功能不可用：{escape(reason)}</div>"
        )

    sync_panel_html = (
        "<div class=\"panel\">"
        "<h2 class=\"title\">JM 收藏同步</h2>"
        + "".join(sync_panel_parts)
        + "</div>"
    )

    page_size_options = [12, 24, 36, 60]
    page_size_html = "".join(
        (
            f"<option value=\"{size}\" selected>{size}</option>"
            if size == page_size
            else f"<option value=\"{size}\">{size}</option>"
        )
        for size in page_size_options
    )

    pager_html = (
        "<div class=\"pager\">"
        "<form class=\"pager-form\" method=\"get\" action=\"/bookshelf\">"
        "<label style=\"margin:0;color:var(--muted);\">每页</label>"
        f"<select class=\"select\" style=\"width:96px;min-width:96px;\" name=\"bps\" onchange=\"this.form.submit()\">{page_size_html}</select>"
        "<input type=\"hidden\" name=\"bp\" value=\"1\" />"
        "</form>"
        "<div class=\"pager-links\">"
        + (
            f"<a class=\"btn ghost\" href=\"{escape(bookshelf_page_url(page - 1))}\">上一页</a>"
            if page > 1
            else "<button class=\"btn ghost\" type=\"button\" disabled>上一页</button>"
        )
        + f"<span class=\"subtle\">第 {page}/{page_count} 页</span>"
        + (
            f"<a class=\"btn ghost\" href=\"{escape(bookshelf_page_url(page + 1))}\">下一页</a>"
            if page < page_count
            else "<button class=\"btn ghost\" type=\"button\" disabled>下一页</button>"
        )
        + "</div>"
        + "</div>"
    )

    cards: list[str] = []
    for book in page_books:
        follow_text = "开启" if book.get("follow_enabled", True) else "关闭"
        pending = int(book.get("pending_update_count", 0))
        provider_id = str(book.get("provider_id") or DEFAULT_PROVIDER_ID)
        provider_badge = render_provider_badge(provider_id)
        book_title = str(book.get("title") or "未命名漫画")
        cover_url = str(book.get("cover") or "").strip()
        if cover_url.startswith("//"):
            cover_url = f"https:{cover_url}"
        elif cover_url.startswith("/"):
            cover_url = urljoin("https://toonily.com", cover_url)
        if cover_url and not cover_url.startswith(("http://", "https://")):
            cover_url = ""

        cover_html = (
            f"<div class=\"result-cover-wrap\"><img class=\"result-cover\" src=\"{escape(cover_url)}\" alt=\"{escape(book['title'])}\" loading=\"lazy\" /></div>"
            if cover_url
            else "<div class=\"result-cover-wrap\"><div class=\"result-cover-empty\">暂无封面</div></div>"
        )
        hidden_inputs = f"<input type=\"hidden\" name=\"bp\" value=\"{page}\" /><input type=\"hidden\" name=\"bps\" value=\"{page_size}\" />"
        downloaded_text = (
            f"已下载：{book.get('last_downloaded_chapter_title') or '-'} "
            f"/ #{format_chapter_number(book.get('last_downloaded_chapter_number'))}"
        )
        latest_text = (
            f"最新：{book.get('latest_site_chapter_title') or '-'} "
            f"/ #{format_chapter_number(book.get('latest_site_chapter_number'))}"
        )
        summary_text = f"待更新：{pending} | 追更：{follow_text} | 检查：{fmt_time(book.get('last_checked_at', ''))}"
        cards.append(
            "<div class=\"book-card\">"
            f"{cover_html}"
            f"<h3 class=\"book-title\" title=\"{escape(book_title)}\">{escape(book_title)}</h3>"
            "<div class=\"book-meta-list\">"
            f"<div class=\"book-meta\">{provider_badge}</div>"
            f"<div class=\"book-meta clamp-1\" title=\"{escape(downloaded_text)}\">{escape(downloaded_text)}</div>"
            f"<div class=\"book-meta clamp-1\" title=\"{escape(latest_text)}\">{escape(latest_text)}</div>"
            f"<div class=\"book-meta clamp-1\" title=\"{escape(summary_text)}\">{escape(summary_text)}</div>"
            "</div>"
            "<div class=\"book-actions\">"
            f"<form method=\"post\" action=\"/bookshelf/{escape(book['id'])}/check\">{hidden_inputs}<button class=\"btn ghost\" type=\"submit\">检查</button></form>"
            f"<form method=\"post\" action=\"/bookshelf/{escape(book['id'])}/download_updates\">{hidden_inputs}<button class=\"btn secondary\" type=\"submit\">更新</button></form>"
            f"<form method=\"post\" action=\"/bookshelf/{escape(book['id'])}/download_all\">{hidden_inputs}<button class=\"btn\" type=\"submit\">全部</button></form>"
            f"<form method=\"post\" action=\"/bookshelf/{escape(book['id'])}/toggle_follow\">{hidden_inputs}<button class=\"btn ghost\" type=\"submit\">追更</button></form>"
            f"<form method=\"post\" action=\"/bookshelf/{escape(book['id'])}/remove\" onsubmit=\"return confirm('确认从书架移除？');\">{hidden_inputs}<button class=\"btn warn\" type=\"submit\">移除</button></form>"
            "</div>"
            "</div>"
        )

    if not cards:
        cards.append("<div class=\"panel\"><div class=\"subtle\">书架为空。请先在主页搜索并加入书架。</div></div>")

    body = (
        render_message(msg)
        + sync_panel_html
        + "<div class=\"panel\"><h2 class=\"title\">书架与追更</h2>"
        + f"<div class=\"subtle\" style=\"margin-bottom:8px;\">共 {total_books} 本漫画</div>"
        + pager_html
        + "<div class=\"bookshelf-grid\">"
        + "".join(cards)
        + "</div>"
        + pager_html
        + "</div>"
    )
    return render_layout(title="漫画下载 - 书架", active_nav="bookshelf", body=body)


def render_settings(state: UIState, msg: str) -> str:
    download_section = (
        "<div class=\"settings-section\">"
        "<h3 class=\"settings-title\">下载配置</h3>"
        "<div class=\"settings-grid\">"
        "<div><label>下载目录</label>"
        f"<input class=\"input\" name=\"output_dir\" value=\"{escape(str(state.output_dir))}\" /></div>"
        "<div><label>章节并发</label>"
        f"<input class=\"input\" name=\"chapter_concurrency\" type=\"number\" min=\"1\" value=\"{state.chapter_concurrency}\" /></div>"
        "<div><label>图片并发</label>"
        f"<input class=\"input\" name=\"image_concurrency\" type=\"number\" min=\"1\" value=\"{state.image_concurrency}\" /></div>"
        "<div><label>重试次数</label>"
        f"<input class=\"input\" name=\"retries\" type=\"number\" min=\"1\" value=\"{state.retries}\" /></div>"
        "<div><label>超时（秒）</label>"
        f"<input class=\"input\" name=\"timeout\" type=\"number\" min=\"10\" value=\"{state.timeout}\" /></div>"
        "</div>"
        "</div>"
    )

    cache_section = (
        "<div class=\"settings-section\">"
        "<h3 class=\"settings-title\">缓存配置（Redis）</h3>"
        "<div class=\"settings-grid\">"
        "<div><label>Redis URL</label>"
        f"<input class=\"input\" name=\"redis_url\" value=\"{escape(state.redis_url)}\" placeholder=\"redis://127.0.0.1:6379/0\" /></div>"
        "<div><label>Redis 用户名（可为空）</label>"
        f"<input class=\"input\" name=\"redis_username\" value=\"{escape(state.redis_username)}\" /></div>"
        "<div><label>Redis 密码（可为空）</label>"
        f"<input class=\"input\" type=\"password\" name=\"redis_password\" value=\"{escape(state.redis_password)}\" autocomplete=\"new-password\" /></div>"
        "<div><label>缓存 TTL（秒）</label>"
        f"<input class=\"input\" name=\"cache_ttl_seconds\" type=\"number\" min=\"30\" value=\"{state.cache_ttl_seconds}\" /></div>"
        "<div><label>缓存开关</label>"
        "<select class=\"select\" name=\"cache_enabled\">"
        + ("<option value=\"1\" selected>启用</option>" if state.cache_enabled else "<option value=\"1\">启用</option>")
        + ("<option value=\"0\">关闭</option>" if state.cache_enabled else "<option value=\"0\" selected>关闭</option>")
        + "</select></div>"
        "</div>"
        "</div>"
    )

    jm_section = (
        "<div class=\"settings-section\">"
        "<h3 class=\"settings-title\">JM 账号</h3>"
        "<div class=\"settings-grid\">"
        "<div><label>JM 用户名</label>"
        f"<input class=\"input\" name=\"jm_username\" value=\"{escape(state.jm_username)}\" autocomplete=\"username\" /></div>"
        "<div><label>JM 密码</label>"
        f"<input class=\"input\" type=\"password\" name=\"jm_password\" value=\"{escape(state.jm_password)}\" autocomplete=\"current-password\" /></div>"
        "</div>"
        + (
            "<div class=\"subtle\" style=\"margin-top:8px;\">JM 状态：可用，支持登录与同步收藏。</div>"
            if JM_PROVIDER_ENABLED
            else f"<div class=\"subtle\" style=\"margin-top:8px;color:#fecaca;\">JM 状态：不可用（{escape(JM_PROVIDER_DISABLED_REASON or '未知原因')}）。</div>"
        )
        + "</div>"
    )

    body = (
        render_message(msg)
        + "<div class=\"panel\">"
        + "<h2 class=\"title\">设置</h2>"
        + "<form method=\"post\" action=\"/settings\">"
        + download_section
        + cache_section
        + jm_section
        + "<div style=\"margin-top:12px;\"><button class=\"btn\" type=\"submit\">保存设置</button></div>"
        + "</form>"
        + "</div>"
    )
    return render_layout(title="漫画下载 - 设置", active_nav="settings", body=body)


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

    if not provider.enabled:
        job["status"] = "failed"
        job["finished_at"] = now_iso()
        job["error"] = provider.disabled_reason or "该站点当前未启用。"
        state.append_job_log(job, f"任务失败：{job['error']}")
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

    report: Optional[DownloadReport] = None
    try:
        report = await downloader.run()
        job["status"] = "completed"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "任务完成。")
        if report.retry_file:
            job["retry_file"] = str(report.retry_file)
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "任务已取消。")
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = now_iso()
        state.append_job_log(job, f"任务失败：{exc}")
    finally:
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

    task.add_done_callback(_finish_callback)


async def handle_root(_: web.Request) -> web.StreamResponse:
    raise build_redirect("/dashboard")


async def handle_dashboard(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = request.query.get("msg", "").strip()
    selected_job_id = request.query.get("job", "").strip() or (state.current_job_id or "")
    search_page = parse_int(request.query.get("sp", "1"), 1, minimum=1, maximum=999)
    search_page_size = parse_int(request.query.get("sps", "12"), 12, minimum=4, maximum=40)
    html = render_dashboard(
        state,
        msg,
        selected_job_id,
        search_page=search_page,
        search_page_size=search_page_size,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_progress(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = request.query.get("msg", "").strip()
    selected_job_id = request.query.get("job", "").strip() or (state.current_job_id or "")
    html = render_progress(state, msg, selected_job_id)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_search(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    keyword = str(form.get("query", "")).strip()
    page_size = parse_int(form.get("page_size", "12"), 12, minimum=4, maximum=40)
    provider_id = str(form.get("provider_id", state.last_search_provider or DEFAULT_PROVIDER_ID)).strip().lower()
    provider = get_provider(provider_id)
    if not keyword:
        raise build_redirect("/dashboard", msg="请输入漫画名称。", sp=1, sps=page_size)
    if not provider.enabled:
        raise build_redirect("/dashboard", msg=f"{provider.display_name} 暂未启用。", sp=1, sps=page_size)

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
    if not provider.enabled:
        raise build_redirect("/dashboard", msg=f"{provider.display_name} 暂未启用。")

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
        start_job(state, job)
        raise build_redirect("/progress", msg="下载任务已创建。", job=job["id"])

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
        start_job(state, job)
        raise build_redirect("/progress", msg="已加入书架并创建追更下载任务。", job=job["id"])

    raise build_redirect("/dashboard", msg="未知操作。")


async def handle_bookshelf(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = request.query.get("msg", "").strip()
    bookshelf_page = parse_int(request.query.get("bp", "1"), 1, minimum=1, maximum=999)
    bookshelf_page_size = parse_int(request.query.get("bps", "24"), 24, minimum=6, maximum=60)
    html = render_bookshelf(
        state,
        msg,
        bookshelf_page=bookshelf_page,
        bookshelf_page_size=bookshelf_page_size,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_bookshelf_sync_jm_favorites(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    provider = get_provider("jmcomic")
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)

    if not provider.enabled:
        raise build_redirect("/bookshelf", msg=f"JM 不可用：{provider.disabled_reason or '未知原因'}", bp=bp, bps=bps)

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
        raise build_redirect("/bookshelf", msg=f"同步 JM 收藏失败：{exc}", bp=bp, bps=bps)

    created = 0
    updated = 0
    for item in favorites:
        _, is_created = state.upsert_book(
            provider_id="jmcomic",
            title=str(item.get("title") or "").strip(),
            series_url=str(item.get("url") or "").strip(),
            cover=str(item.get("cover") or "").strip(),
        )
        if is_created:
            created += 1
        else:
            updated += 1

    await state.save_bookshelf()
    raise build_redirect(
        "/bookshelf",
        msg=f"JM 收藏同步完成：共 {len(favorites)} 条，新增 {created}，更新 {updated}。",
        bp=bp,
        bps=bps,
    )


async def handle_book_action(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    book_id = request.match_info["book_id"]
    action = request.match_info["action"]
    book = state.get_book(book_id)
    if book is None:
        raise build_redirect("/bookshelf", msg="书架项目不存在。", bp=bp, bps=bps)

    if action == "remove":
        state.remove_book(book_id)
        await state.save_bookshelf()
        raise build_redirect("/bookshelf", msg="已移除。", bp=bp, bps=bps)

    if action == "toggle_follow":
        book["follow_enabled"] = not bool(book.get("follow_enabled", True))
        await state.save_bookshelf()
        raise build_redirect("/bookshelf", msg=f"追更已{'开启' if book['follow_enabled'] else '关闭'}。", bp=bp, bps=bps)

    if action == "check":
        try:
            pending = await refresh_book_snapshot(state, book)
            await state.save_bookshelf()
            raise build_redirect("/bookshelf", msg=f"检查完成，待更新 {len(pending)} 章。", bp=bp, bps=bps)
        except Exception as exc:
            raise build_redirect("/bookshelf", msg=f"检查失败：{exc}", bp=bp, bps=bps)

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
            raise build_redirect("/bookshelf", msg=f"读取章节失败：{exc}", bp=bp, bps=bps)

        if action == "download_updates":
            pending = compute_pending_chapters(book, chapters)
            chapter_urls = [item.url for item in pending]
            if not chapter_urls:
                set_site_latest_fields(book, chapters)
                book["pending_update_count"] = 0
                book["last_checked_at"] = now_iso()
                await state.save_bookshelf()
                raise build_redirect("/bookshelf", msg="没有新的章节需要下载。", bp=bp, bps=bps)
            title = f"下载更新：{book['title']} ({len(chapter_urls)} 章)"
        else:
            title = f"下载全部：{book['title']}"

        job = state.create_job(
            title=title,
            series_url=book["series_url"],
            chapter_selector="all",
            chapter_urls=chapter_urls,
            mode=mode,
            book_id=book["id"],
            provider_id=str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
        )
        start_job(state, job)
        raise build_redirect("/progress", msg="任务已创建。", job=job["id"])

    raise build_redirect("/bookshelf", msg="未知操作。")

async def handle_settings_get(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = request.query.get("msg", "").strip()
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
        state.redis_url = str(form.get("redis_url", "")).strip()
        state.redis_username = str(form.get("redis_username", "")).strip()
        state.redis_password = str(form.get("redis_password", "")).strip()
        state.cache_ttl_seconds = max(30, int(form.get("cache_ttl_seconds", state.cache_ttl_seconds)))
        state.cache_enabled = str(form.get("cache_enabled", "1")) == "1"
        state.jm_username = str(form.get("jm_username", state.jm_username)).strip()
        state.jm_password = str(form.get("jm_password", state.jm_password)).strip()
        await state.save_settings()
    except Exception as exc:
        raise build_redirect("/settings", msg=f"保存失败：{exc}")
    raise build_redirect("/settings", msg="设置已保存。")


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
    elif action == "cancel" and status in {"queued", "running", "paused", "cancelling"}:
        job["cancel_requested"] = True
        job["pause_event"].set()
        job["status"] = "cancelling"
        state.append_job_log(job, "收到取消请求，正在停止任务。")
        task = job.get("task")
        if task is not None and not task.done():
            task.cancel()

    return web.json_response({"ok": True, "state": serialize_job(job)})


async def on_shutdown(app: web.Application) -> None:
    state: UIState = app["state"]
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


def create_app() -> web.Application:
    state = UIState()
    state.load()
    state.output_dir.mkdir(parents=True, exist_ok=True)

    app = web.Application()
    app["state"] = state

    app.add_routes(
        [
            web.get("/", handle_root),
            web.get("/dashboard", handle_dashboard),
            web.get("/progress", handle_progress),
            web.post("/search", handle_search),
            web.post("/search/action", handle_search_action),
            web.get("/bookshelf", handle_bookshelf),
            web.post("/bookshelf/sync-jm-favorites", handle_bookshelf_sync_jm_favorites),
            web.post("/bookshelf/{book_id}/{action}", handle_book_action),
            web.get("/settings", handle_settings_get),
            web.post("/settings", handle_settings_post),
            web.get("/job/{job_id}/state", handle_job_state),
            web.post("/job/{job_id}/{action}", handle_job_action),
        ]
    )
    app.on_shutdown.append(on_shutdown)
    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="漫画下载 WebUI")
    parser.add_argument("--host", default="127.0.0.1", help="WebUI host")
    parser.add_argument("--port", type=int, default=8000, help="WebUI port")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app()
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
