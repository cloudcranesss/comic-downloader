import argparse
import asyncio
import hashlib
import io
import json
import mimetypes
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse

import aiohttp
import cloudscraper
from bs4 import BeautifulSoup

try:
    from redis.asyncio import Redis
except Exception:
    Redis = None  # type: ignore[assignment]

try:
    from PIL import Image
except Exception:
    Image = None  # type: ignore[assignment]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class Chapter:
    title: str
    url: str
    number: Optional[float]


@dataclass
class ChapterResult:
    title: str
    url: str
    number: Optional[float]
    total_images: int
    saved_images: int
    downloaded_bytes: int
    status: str
    error: Optional[str] = None


@dataclass
class DownloadReport:
    manga_title: str
    manga_dir: Path
    total_chapters_found: int
    selected_chapters: int
    successful_chapters: int
    failed_chapters: int
    retry_file: Optional[Path]
    chapter_results: list[ChapterResult]
    started_at: datetime
    finished_at: datetime
    downloaded_bytes: int = 0
    archive_file: Optional[Path] = None
    metadata_file: Optional[Path] = None
    failure_reasons: Optional[dict[str, int]] = None


def sanitize_name(name: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".")
    return cleaned or "untitled"


def normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


def parse_int_or_default(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(maximum, number))


def parse_chapter_number(title: str) -> Optional[float]:
    match = re.search(
        r"(?:chapter|chap|ch|episode|ep)\.?\s*(\d+(?:\.\d+)?)",
        title,
        re.IGNORECASE,
    )
    if not match:
        match = re.search(r"\b(\d+(?:\.\d+)?)\b", title)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def parse_retry_file(retry_file: Path) -> list[str]:
    if not retry_file.exists():
        raise FileNotFoundError(f"Retry file not found: {retry_file}")

    urls: list[str] = []
    seen: set[str] = set()

    for raw in retry_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        token = None
        for part in line.split():
            if part.startswith("http://") or part.startswith("https://"):
                token = part
                break
        if not token:
            continue
        key = normalize_url(token)
        if key not in seen:
            seen.add(key)
            urls.append(token)

    return urls


def parse_selector(selector: str, chapters: list[Chapter]) -> list[Chapter]:
    if selector.lower() == "all":
        return chapters

    selected: dict[str, Chapter] = {}
    for raw in selector.split(","):
        token = raw.strip()
        if not token:
            continue

        range_match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*", token)
        if range_match:
            start = float(range_match.group(1))
            end = float(range_match.group(2))
            low, high = (start, end) if start <= end else (end, start)
            for chapter in chapters:
                if chapter.number is not None and low <= chapter.number <= high:
                    selected[chapter.url] = chapter
            continue

        try:
            target = float(token)
            for chapter in chapters:
                if chapter.number == target:
                    selected[chapter.url] = chapter
        except ValueError:
            for chapter in chapters:
                if token.lower() in chapter.title.lower():
                    selected[chapter.url] = chapter

    return [c for c in chapters if c.url in selected]


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def safe_format(template: str, mapping: dict[str, Any], fallback: str) -> str:
    try:
        return str(template).format_map(mapping)
    except Exception:
        return fallback


def sanitize_path_parts(path_text: str) -> list[str]:
    raw_parts = str(path_text).replace("\\", "/").split("/")
    parts: list[str] = []
    for part in raw_parts:
        token = sanitize_name(part)
        if token and token not in {".", ".."}:
            parts.append(token)
    return parts


class ToonilyAsyncDownloader:
    def __init__(
        self,
        series_url: str,
        output_dir: Path,
        chapter_selector: str,
        chapter_concurrency: int,
        image_concurrency: int,
        retries: int,
        timeout: int,
        chapter_urls: Optional[list[str]] = None,
        write_failed_file: bool = True,
        failed_list_file: Optional[Path] = None,
        logger: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[dict[str, Any]], None]] = None,
        pause_waiter: Optional[Callable[[], Awaitable[None]]] = None,
        cancel_checker: Optional[Callable[[], bool]] = None,
        cache_enabled: bool = True,
        redis_host: Optional[str] = None,
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_username: Optional[str] = None,
        redis_password: Optional[str] = None,
        cache_ttl_seconds: int = 900,
        cache_prefix: str = "toonily:html:",
        site_name: str = "toonily",
        manga_dir_template: str = "{site}/{manga}",
        chapter_dir_template: str = "{chapter_number}-{chapter_title}",
        page_name_template: str = "{page:03}",
        image_output_format: str = "original",
        image_quality: int = 85,
        keep_original_images: bool = False,
        auto_archive_format: str = "none",
        write_metadata_sidecar: bool = True,
        enable_chapter_dedupe: bool = True,
        retry_base_delay_seconds: float = 0.8,
        retry_recoverable_only: bool = True,
        bandwidth_day_kbps: int = 0,
        bandwidth_night_kbps: int = 0,
        night_start_hour: int = 22,
        night_end_hour: int = 7,
    ) -> None:
        self.series_url = series_url.strip()
        self.output_dir = output_dir
        self.chapter_selector = chapter_selector
        self.chapter_concurrency = max(1, chapter_concurrency)
        self.image_concurrency = max(1, image_concurrency)
        self.retries = max(1, retries)
        self.timeout = max(10, timeout)
        self.chapter_urls = chapter_urls or []
        self.write_failed_file = write_failed_file
        self.failed_list_file = failed_list_file
        self.logger = logger
        self.progress_callback = progress_callback
        self.pause_waiter = pause_waiter
        self.cancel_checker = cancel_checker
        self.cache_enabled = bool(cache_enabled)
        self.redis_host = (
            (redis_host or "").strip()
            or os.getenv("REDIS_HOST", "").strip()
        )
        env_port = os.getenv("REDIS_PORT", "6379")
        env_db = os.getenv("REDIS_DB", "0")
        self.redis_port = parse_int_or_default(redis_port, parse_int_or_default(env_port, 6379, minimum=1, maximum=65535), minimum=1, maximum=65535)
        self.redis_db = parse_int_or_default(redis_db, parse_int_or_default(env_db, 0, minimum=0, maximum=999999), minimum=0, maximum=999999)
        self.redis_username = (
            (redis_username or "").strip()
            or os.getenv("REDIS_USERNAME", "").strip()
        )
        self.redis_password = (
            (redis_password or "").strip()
            or os.getenv("REDIS_PASSWORD", "").strip()
        )
        self.cache_ttl_seconds = max(30, int(cache_ttl_seconds))
        self.cache_prefix = cache_prefix
        self.site_name = sanitize_name(site_name or "toonily")
        self.manga_dir_template = str(manga_dir_template or "{site}/{manga}")
        self.chapter_dir_template = str(chapter_dir_template or "{chapter_number}-{chapter_title}")
        self.page_name_template = str(page_name_template or "{page:03}")
        fmt = str(image_output_format or "original").strip().lower()
        self.image_output_format = fmt if fmt in {"original", "jpg", "webp"} else "original"
        self.image_quality = max(1, min(100, int(image_quality)))
        self.keep_original_images = bool(keep_original_images)
        archive_fmt = str(auto_archive_format or "none").strip().lower()
        self.auto_archive_format = archive_fmt if archive_fmt in {"none", "cbz", "zip"} else "none"
        self.write_metadata_sidecar = bool(write_metadata_sidecar)
        self.enable_chapter_dedupe = bool(enable_chapter_dedupe)
        self.retry_base_delay_seconds = max(0.2, float(retry_base_delay_seconds))
        self.retry_recoverable_only = bool(retry_recoverable_only)
        self.bandwidth_day_kbps = max(0, int(bandwidth_day_kbps))
        self.bandwidth_night_kbps = max(0, int(bandwidth_night_kbps))
        self.night_start_hour = max(0, min(23, int(night_start_hour)))
        self.night_end_hour = max(0, min(23, int(night_end_hour)))

        self._redis: Optional[Redis] = None  # type: ignore[valid-type]
        self._cache_disabled_reason = ""
        self._download_index: dict[str, dict[str, Any]] = {}

        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self.scraper.headers.update({"User-Agent": UA})

        self.image_semaphore = asyncio.Semaphore(self.image_concurrency)
        self.chapter_semaphore = asyncio.Semaphore(self.chapter_concurrency)

    def log(self, message: str) -> None:
        if self.logger:
            self.logger(message)
            return
        try:
            print(message, flush=True)
        except UnicodeEncodeError:
            safe_message = message.encode("ascii", "backslashreplace").decode("ascii")
            print(safe_message, flush=True)

    def emit_progress(self, **payload: Any) -> None:
        if not self.progress_callback:
            return
        try:
            self.progress_callback(payload)
        except Exception:
            # Progress updates should not break downloading.
            return

    async def wait_if_paused(self) -> None:
        if self.pause_waiter:
            await self.pause_waiter()

    def is_cancelled(self) -> bool:
        return bool(self.cancel_checker and self.cancel_checker())

    async def ensure_not_cancelled(self) -> None:
        if self.is_cancelled():
            raise asyncio.CancelledError("Task cancelled by user.")

    def _is_night_time(self) -> bool:
        hour = datetime.now().hour
        start = self.night_start_hour
        end = self.night_end_hour
        if start == end:
            return False
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def _bandwidth_limit_bps(self) -> int:
        kbps = self.bandwidth_night_kbps if self._is_night_time() else self.bandwidth_day_kbps
        return max(0, int(kbps * 1024))

    async def _apply_bandwidth_limit(self, payload_size: int, elapsed_seconds: float) -> None:
        limit_bps = self._bandwidth_limit_bps()
        if limit_bps <= 0 or payload_size <= 0:
            return
        target_seconds = payload_size / float(limit_bps)
        if target_seconds > elapsed_seconds:
            await asyncio.sleep(target_seconds - elapsed_seconds)

    def _is_recoverable_error(self, exc: Exception) -> bool:
        text = str(exc).lower()
        if "429" in text:
            return True
        if "timeout" in text:
            return True
        if "temporarily unavailable" in text:
            return True
        if "connection reset" in text or "connection aborted" in text:
            return True
        if isinstance(exc, asyncio.TimeoutError):
            return True
        if isinstance(exc, aiohttp.ClientError):
            return True
        return False

    def _chapter_key(self, chapter_url: str) -> str:
        return hashlib.sha1(normalize_url(chapter_url).encode("utf-8", errors="ignore")).hexdigest()

    def _load_download_index(self, manga_dir: Path) -> None:
        index_file = manga_dir / ".download_index.json"
        self._download_index = {}
        if not index_file.exists():
            return
        try:
            data = json.loads(index_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                self._download_index = {
                    str(k): dict(v) for k, v in data.items() if isinstance(k, str) and isinstance(v, dict)
                }
        except Exception:
            self._download_index = {}

    def _save_download_index(self, manga_dir: Path) -> None:
        index_file = manga_dir / ".download_index.json"
        try:
            index_file.write_text(
                json.dumps(self._download_index, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _template_context(
        self,
        *,
        manga_title: str,
        chapter: Optional[Chapter] = None,
        chapter_index: int = 0,
        page: int = 1,
    ) -> dict[str, Any]:
        chapter_title = chapter.title if chapter else ""
        chapter_number = chapter.number if chapter else None
        if chapter_number is None:
            chapter_number_text = str(chapter_index + 1)
        elif float(chapter_number).is_integer():
            chapter_number_text = str(int(chapter_number))
        else:
            chapter_number_text = str(chapter_number)
        return {
            "site": self.site_name,
            "manga": manga_title,
            "chapter_title": chapter_title,
            "chapter_number": chapter_number_text,
            "chapter_index": chapter_index + 1,
            "page": page,
        }

    def _build_manga_dir(self, manga_title: str) -> Path:
        context = self._template_context(manga_title=manga_title)
        rendered = safe_format(self.manga_dir_template, context, f"{self.site_name}/{manga_title}")
        parts = sanitize_path_parts(rendered)
        if not parts:
            parts = [self.site_name, sanitize_name(manga_title)]
        return self.output_dir.joinpath(*parts)

    def _build_chapter_dir(self, manga_dir: Path, manga_title: str, chapter: Chapter, chapter_index: int) -> Path:
        context = self._template_context(manga_title=manga_title, chapter=chapter, chapter_index=chapter_index)
        rendered = safe_format(self.chapter_dir_template, context, chapter.title)
        parts = sanitize_path_parts(rendered)
        if not parts:
            parts = [sanitize_name(chapter.title)]
        return manga_dir.joinpath(*parts)

    def _build_page_basename(self, chapter: Chapter, chapter_index: int, page: int) -> str:
        context = self._template_context(manga_title="", chapter=chapter, chapter_index=chapter_index, page=page)
        rendered = safe_format(self.page_name_template, context, f"{page:03}")
        token = sanitize_name(rendered)
        return token or f"{page:03}"

    def _archive_manga_dir(self, manga_dir: Path) -> Optional[Path]:
        if self.auto_archive_format == "none":
            return None
        try:
            archive_base = manga_dir.parent / manga_dir.name
            zip_path = Path(shutil.make_archive(str(archive_base), "zip", root_dir=str(manga_dir)))
            if self.auto_archive_format == "zip":
                return zip_path
            cbz_path = zip_path.with_suffix(".cbz")
            if cbz_path.exists():
                cbz_path.unlink()
            zip_path.rename(cbz_path)
            return cbz_path
        except Exception as exc:
            self.log(f"[WARN] 归档失败：{exc}")
            return None

    def _failure_reason_counts(self, chapter_results: list[ChapterResult]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in chapter_results:
            if item.status == "success":
                continue
            reason = (item.error or item.status or "unknown").strip()
            if not reason:
                reason = "unknown"
            counts[reason] = counts.get(reason, 0) + 1
        return counts

    def _write_metadata_sidecar(
        self,
        manga_dir: Path,
        *,
        manga_title: str,
        chapters: list[Chapter],
        selected: list[Chapter],
        chapter_results: list[ChapterResult],
        archive_file: Optional[Path],
    ) -> Optional[Path]:
        if not self.write_metadata_sidecar:
            return None

        result_map = {normalize_url(item.url): item for item in chapter_results}
        chapter_rows: list[dict[str, Any]] = []
        for chapter in chapters:
            key = normalize_url(chapter.url)
            result = result_map.get(key)
            chapter_rows.append(
                {
                    "title": chapter.title,
                    "url": chapter.url,
                    "number": chapter.number,
                    "selected": any(normalize_url(s.url) == key for s in selected),
                    "status": result.status if result else "skipped",
                    "saved_images": result.saved_images if result else 0,
                    "total_images": result.total_images if result else 0,
                    "downloaded_bytes": result.downloaded_bytes if result else 0,
                    "error": result.error if result else "",
                }
            )

        payload = {
            "title": manga_title,
            "site": self.site_name,
            "series_url": self.series_url,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "archive_file": str(archive_file) if archive_file else "",
            "chapters": chapter_rows,
        }
        target = manga_dir / "metadata.json"
        try:
            target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return target
        except Exception as exc:
            self.log(f"[WARN] metadata 写入失败：{exc}")
            return None

    def _cache_key(self, url: str) -> str:
        digest = hashlib.sha1(normalize_url(url).encode("utf-8", errors="ignore")).hexdigest()
        return f"{self.cache_prefix}{digest}"

    def _disable_cache(self, reason: str) -> None:
        if self.cache_enabled:
            self.cache_enabled = False
            self._cache_disabled_reason = reason
            self.log(f"[CACHE] disabled: {reason}")

    async def _get_redis(self) -> Optional[Redis]:  # type: ignore[valid-type]
        if not self.cache_enabled:
            return None
        if not self.redis_host:
            self._disable_cache("redis_host not configured")
            return None
        if Redis is None:
            self._disable_cache("redis package not installed")
            return None
        if self._redis is not None:
            return self._redis

        try:
            client = Redis(  # type: ignore[call-arg]
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,
                username=self.redis_username or None,
                password=self.redis_password or None,
            )
            await client.ping()
            self._redis = client
            self.log("[CACHE] redis connected")
            return self._redis
        except Exception as exc:
            self._disable_cache(f"connect failed: {exc}")
            return None

    async def _cache_get_html(self, url: str) -> Optional[str]:
        client = await self._get_redis()
        if client is None:
            return None
        try:
            cached = await client.get(self._cache_key(url))
            if cached:
                self.emit_progress(event="cache_hit", url=url)
                return str(cached)
        except Exception as exc:
            self._disable_cache(f"get failed: {exc}")
        return None

    async def _cache_set_html(self, url: str, html: str) -> None:
        client = await self._get_redis()
        if client is None:
            return
        try:
            await client.set(self._cache_key(url), html, ex=self.cache_ttl_seconds)
        except Exception as exc:
            self._disable_cache(f"set failed: {exc}")

    async def close(self) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.aclose()
        except Exception:
            pass
        finally:
            self._redis = None

    async def fetch_html(self, url: str) -> str:
        cached_html = await self._cache_get_html(url)
        if cached_html is not None:
            return cached_html

        last_error: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                await self.wait_if_paused()
                await self.ensure_not_cancelled()
                response = await asyncio.to_thread(
                    self.scraper.get,
                    url,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                html = response.text
                await self._cache_set_html(url, html)
                return html
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                can_retry = self._is_recoverable_error(exc)
                should_retry = can_retry or (not self.retry_recoverable_only)
                if attempt < self.retries and should_retry:
                    await self.wait_if_paused()
                    await self.ensure_not_cancelled()
                    await asyncio.sleep(self.retry_base_delay_seconds * (2 ** (attempt - 1)))
                else:
                    break
        raise RuntimeError(f"Failed to fetch {url}: {last_error}")

    async def get_series_details(self) -> tuple[str, list[Chapter]]:
        html = await self.fetch_html(self.series_url)
        soup = BeautifulSoup(html, "html.parser")

        title_node = (
            soup.select_one("div.post-title h1")
            or soup.select_one("h1.entry-title")
            or soup.select_one("h1")
        )
        manga_title = ""
        if title_node is not None:
            for span in title_node.select("span"):
                span.decompose()
            manga_title = sanitize_name(title_node.get_text(" ", strip=True))
        if not manga_title:
            og_title = soup.select_one("meta[property='og:title'], meta[name='og:title']")
            if og_title is not None:
                manga_title = sanitize_name(str(og_title.get("content") or "").strip())
        if not manga_title:
            raise RuntimeError("Could not find manga title.")

        chapters: list[Chapter] = []
        chapter_selectors = (
            "li.wp-manga-chapter > a",
            "li.wp-manga-chapter a",
            ".listing-chapters_wrap li a",
            ".main.version-chap li a",
            ".version-chap li a",
            ".wp-manga-chapter a",
        )
        chapter_anchors = []
        for selector in chapter_selectors:
            chapter_anchors.extend(soup.select(selector))

        seen_chapter_urls: set[str] = set()
        for a in chapter_anchors:
            href = (a.get("href") or "").strip()
            if not href:
                continue
            href = normalize_url(href)
            if href in seen_chapter_urls:
                continue
            text = " ".join(a.get_text(" ", strip=True).split())
            if not text:
                continue
            # Skip obvious non-chapter links from broad selectors.
            if not re.search(r"(chapter|chap|ch\.?|episode|ep\.?|\b\d+\b)", text, re.IGNORECASE):
                continue
            seen_chapter_urls.add(href)
            chapters.append(Chapter(title=text, url=href, number=parse_chapter_number(text)))

        if not chapters:
            raise RuntimeError("No chapters found on this series page.")

        chapters = sorted(
            chapters,
            key=lambda c: (float("inf") if c.number is None else c.number, c.title.lower()),
        )
        return manga_title, chapters

    async def get_chapter_images(self, chapter_url: str) -> list[str]:
        html = await self.fetch_html(chapter_url)
        soup = BeautifulSoup(html, "html.parser")
        images: list[str] = []

        for img in soup.select("div.reading-content img"):
            image_url = (img.get("data-src") or img.get("data-lazy-src") or img.get("src") or "").strip()
            if image_url.startswith("http"):
                images.append(image_url)

        deduped: list[str] = []
        seen: set[str] = set()
        for url in images:
            if url not in seen:
                seen.add(url)
                deduped.append(url)

        return deduped

    async def _download_one_image(
        self,
        session: aiohttp.ClientSession,
        image_url: str,
        target_file: Path,
        referer: str,
        *,
        source_ext: str,
        output_ext: str,
        keep_original_target: Optional[Path] = None,
    ) -> tuple[bool, int, str]:
        async with self.image_semaphore:
            for attempt in range(1, self.retries + 1):
                try:
                    await self.wait_if_paused()
                    await self.ensure_not_cancelled()
                    started = time.perf_counter()
                    async with session.get(
                        image_url,
                        headers={"Referer": referer, "User-Agent": UA},
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as response:
                        if response.status >= 400:
                            raise RuntimeError(f"HTTP {response.status}")
                        data = await response.read()
                        if not data:
                            raise RuntimeError("Empty image response")
                        elapsed = max(0.0001, time.perf_counter() - started)
                        await self._apply_bandwidth_limit(len(data), elapsed)

                        final_data = data
                        if self.image_output_format != "original" and output_ext != source_ext:
                            if Image is None:
                                raise RuntimeError("Pillow not installed for image conversion")
                            with Image.open(io.BytesIO(data)) as img:
                                if output_ext == ".jpg":
                                    if img.mode in ("RGBA", "LA", "P"):
                                        img = img.convert("RGB")
                                    buf = io.BytesIO()
                                    img.save(buf, format="JPEG", quality=self.image_quality, optimize=True)
                                    final_data = buf.getvalue()
                                elif output_ext == ".webp":
                                    buf = io.BytesIO()
                                    img.save(buf, format="WEBP", quality=self.image_quality, method=6)
                                    final_data = buf.getvalue()

                        target_file.write_bytes(final_data)
                        if keep_original_target is not None and self.keep_original_images and keep_original_target != target_file:
                            if not keep_original_target.exists():
                                keep_original_target.write_bytes(data)
                        return True, len(final_data), ""
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    recoverable = self._is_recoverable_error(exc)
                    should_retry = recoverable or (not self.retry_recoverable_only)
                    if attempt < self.retries and should_retry:
                        await self.wait_if_paused()
                        await self.ensure_not_cancelled()
                        await asyncio.sleep(self.retry_base_delay_seconds * (2 ** (attempt - 1)))
                    else:
                        return False, 0, str(exc)
            return False, 0, "unknown"

    @staticmethod
    def _guess_extension(image_url: str, content_type: Optional[str] = None) -> str:
        if content_type:
            ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
            if ext:
                return ".jpg" if ext == ".jpe" else ext

        parsed = urlparse(image_url)
        suffix = Path(parsed.path).suffix.lower()
        if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
            return ".jpg" if suffix == ".jpeg" else suffix

        return ".jpg"

    async def download_chapter(
        self,
        session: aiohttp.ClientSession,
        manga_dir: Path,
        manga_title: str,
        chapter: Chapter,
        chapter_index: int,
    ) -> ChapterResult:
        async with self.chapter_semaphore:
            await self.wait_if_paused()
            await self.ensure_not_cancelled()
            chapter_dir = self._build_chapter_dir(manga_dir, manga_title, chapter, chapter_index)
            chapter_dir.mkdir(parents=True, exist_ok=True)

            try:
                images = await self.get_chapter_images(chapter.url)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                msg = f"[FAIL] {chapter.title}: {exc}"
                self.log(msg)
                return ChapterResult(
                    title=chapter.title,
                    url=chapter.url,
                    number=chapter.number,
                    total_images=0,
                    saved_images=0,
                    downloaded_bytes=0,
                    status="failed",
                    error=str(exc),
                )

            if not images:
                msg = f"[FAIL] {chapter.title}: no images found"
                self.log(msg)
                return ChapterResult(
                    title=chapter.title,
                    url=chapter.url,
                    number=chapter.number,
                    total_images=0,
                    saved_images=0,
                    downloaded_bytes=0,
                    status="failed",
                    error="no images found",
                )

            total = len(images)
            tasks: list[asyncio.Task[tuple[bool, int, str]]] = []
            skipped = 0
            skipped_bytes = 0

            for index, image_url in enumerate(images, start=1):
                await self.wait_if_paused()
                await self.ensure_not_cancelled()
                source_ext = self._guess_extension(image_url)
                output_ext = source_ext if self.image_output_format == "original" else f".{self.image_output_format}"
                page_name = self._build_page_basename(chapter, chapter_index, index)
                target_file = chapter_dir / f"{page_name}{output_ext}"
                original_file = None
                if self.keep_original_images and self.image_output_format != "original":
                    original_file = chapter_dir / f"{page_name}.orig{source_ext}"

                if target_file.exists() and target_file.stat().st_size > 0:
                    skipped += 1
                    skipped_bytes += target_file.stat().st_size
                    continue

                tasks.append(
                    asyncio.create_task(
                        self._download_one_image(
                            session,
                            image_url,
                            target_file,
                            chapter.url,
                            source_ext=source_ext,
                            output_ext=output_ext,
                            keep_original_target=original_file,
                        )
                    )
                )

            downloaded = 0
            downloaded_bytes = 0
            failure_reasons: dict[str, int] = {}
            if tasks:
                results = await asyncio.gather(*tasks)
                for ok, size, reason in results:
                    if ok:
                        downloaded += 1
                        downloaded_bytes += max(0, int(size))
                    else:
                        key = (reason or "download failed").strip()
                        failure_reasons[key] = failure_reasons.get(key, 0) + 1

            saved = downloaded + skipped
            total_bytes = downloaded_bytes + skipped_bytes
            if saved == total:
                status = "success"
                error = None
            elif saved > 0:
                status = "partial"
                if failure_reasons:
                    top_reason = max(failure_reasons.items(), key=lambda x: x[1])[0]
                    error = f"saved {saved}/{total}; {top_reason}"
                else:
                    error = f"saved {saved}/{total}"
            else:
                status = "failed"
                if failure_reasons:
                    top_reason = max(failure_reasons.items(), key=lambda x: x[1])[0]
                    error = f"saved {saved}/{total}; {top_reason}"
                else:
                    error = f"saved {saved}/{total}"

            prefix = "OK" if status == "success" else "WARN"
            self.log(
                f"[{prefix}] {chapter.title}: total={total}, downloaded={downloaded}, "
                f"skipped={skipped}, status={status}"
            )

            return ChapterResult(
                title=chapter.title,
                url=chapter.url,
                number=chapter.number,
                total_images=total,
                saved_images=saved,
                downloaded_bytes=total_bytes,
                status=status,
                error=error,
            )

    async def _download_chapter_safe(
        self,
        session: aiohttp.ClientSession,
        manga_dir: Path,
        manga_title: str,
        chapter: Chapter,
        chapter_index: int,
    ) -> ChapterResult:
        try:
            return await self.download_chapter(session, manga_dir, manga_title, chapter, chapter_index)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.log(f"[FAIL] {chapter.title}: unexpected error: {exc}")
            return ChapterResult(
                title=chapter.title,
                url=chapter.url,
                number=chapter.number,
                total_images=0,
                saved_images=0,
                downloaded_bytes=0,
                status="failed",
                error=str(exc),
            )

    def _write_failed_retry_file(
        self,
        manga_dir: Path,
        failed_results: list[ChapterResult],
    ) -> Optional[Path]:
        if not self.write_failed_file:
            return None

        target = self.failed_list_file or (manga_dir / "failed_chapters_retry.txt")
        target = target.resolve()
        target.parent.mkdir(parents=True, exist_ok=True)

        lines = [
            "# Toonily failed chapters retry list",
            f"# generated_at: {datetime.now().isoformat(timespec='seconds')}",
            f"# series_url: {self.series_url}",
            "# usage: python downloaders/toonily.py <series_url> --retry-file <this_file>",
            "",
        ]

        for item in failed_results:
            lines.append(f"# {item.title} | status={item.status} | reason={item.error or '-'}")
            lines.append(item.url)

        target.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.log(f"Retry list written: {target}")
        return target

    def _select_chapters(self, chapters: list[Chapter]) -> list[Chapter]:
        if self.chapter_urls:
            wanted = {normalize_url(url) for url in self.chapter_urls}
            selected = [c for c in chapters if normalize_url(c.url) in wanted]
            return selected
        return parse_selector(self.chapter_selector, chapters)

    async def run(self) -> DownloadReport:
        started_at = datetime.now()

        manga_title, chapters = await self.get_series_details()
        selected = self._select_chapters(chapters)

        if not selected:
            raise RuntimeError("No chapters matched your selection.")

        manga_dir = self._build_manga_dir(manga_title)
        manga_dir.mkdir(parents=True, exist_ok=True)
        if self.enable_chapter_dedupe:
            self._load_download_index(manga_dir)

        self.log(f"Manga: {manga_title}")
        self.log(f"Total chapters found: {len(chapters)}")
        self.log(f"Selected chapters: {len(selected)}")
        self.log(f"Output dir: {manga_dir}")

        chapter_index_map = {normalize_url(ch.url): idx for idx, ch in enumerate(chapters)}
        selected_pairs: list[tuple[int, Chapter]] = [
            (chapter_index_map.get(normalize_url(ch.url), idx), ch)
            for idx, ch in enumerate(selected)
        ]

        pre_done_results: list[ChapterResult] = []
        pending_pairs: list[tuple[int, Chapter]] = []
        if self.enable_chapter_dedupe:
            for chapter_index, chapter in selected_pairs:
                chapter_key = self._chapter_key(chapter.url)
                index_row = self._download_index.get(chapter_key, {})
                if str(index_row.get("status", "")).strip().lower() != "success":
                    pending_pairs.append((chapter_index, chapter))
                    continue

                chapter_dir = self._build_chapter_dir(manga_dir, manga_title, chapter, chapter_index)
                if not chapter_dir.exists():
                    pending_pairs.append((chapter_index, chapter))
                    continue

                saved_files = [
                    p for p in chapter_dir.iterdir()
                    if p.is_file() and not p.name.startswith(".")
                ]
                if not saved_files:
                    pending_pairs.append((chapter_index, chapter))
                    continue

                saved_images = len(saved_files)
                pre_done_results.append(
                    ChapterResult(
                        title=chapter.title,
                        url=chapter.url,
                        number=chapter.number,
                        total_images=saved_images,
                        saved_images=saved_images,
                        downloaded_bytes=0,
                        status="success",
                        error="dedupe skipped",
                    )
                )
                self.log(f"[SKIP] {chapter.title}: already completed (dedupe)")
        else:
            pending_pairs = selected_pairs[:]

        total_selected = len(selected_pairs)
        done_chapters = len(pre_done_results)
        total_images = sum(item.total_images for item in pre_done_results)
        saved_images = sum(item.saved_images for item in pre_done_results)
        downloaded_bytes = sum(item.downloaded_bytes for item in pre_done_results)

        self.emit_progress(
            event="init",
            done_chapters=done_chapters,
            total_chapters=total_selected,
            saved_images=saved_images,
            total_images=total_images,
        )

        cookies = {c.name: c.value for c in self.scraper.cookies}
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        connector = aiohttp.TCPConnector(limit=max(16, self.image_concurrency * 2), ssl=False)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector, cookies=cookies) as session:
            tasks = [
                asyncio.create_task(
                    self._download_chapter_safe(session, manga_dir, manga_title, chapter, chapter_index)
                )
                for chapter_index, chapter in pending_pairs
            ]

            chapter_results: list[ChapterResult] = list(pre_done_results)

            try:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    chapter_results.append(result)
                    done_chapters += 1
                    total_images += result.total_images
                    saved_images += result.saved_images
                    downloaded_bytes += result.downloaded_bytes

                    if self.enable_chapter_dedupe:
                        chapter_idx = chapter_index_map.get(normalize_url(result.url), 0)
                        chapter_key = self._chapter_key(result.url)
                        chapter_title_hash = hashlib.sha1(
                            f"{normalize_url(result.url)}|{result.title}".encode("utf-8", errors="ignore")
                        ).hexdigest()
                        chapter_dir = self._build_chapter_dir(
                            manga_dir, manga_title, Chapter(result.title, result.url, result.number), chapter_idx
                        )
                        self._download_index[chapter_key] = {
                            "chapter_url": normalize_url(result.url),
                            "chapter_title": result.title,
                            "chapter_title_hash": chapter_title_hash,
                            "chapter_number": result.number,
                            "chapter_index": chapter_idx + 1,
                            "status": result.status,
                            "total_images": result.total_images,
                            "saved_images": result.saved_images,
                            "downloaded_bytes": result.downloaded_bytes,
                            "chapter_dir": str(chapter_dir),
                            "updated_at": datetime.now().isoformat(timespec="seconds"),
                        }
                        self._save_download_index(manga_dir)

                    self.emit_progress(
                        event="chapter_done",
                        done_chapters=done_chapters,
                        total_chapters=total_selected,
                        saved_images=saved_images,
                        total_images=total_images,
                        last_chapter_title=result.title,
                        last_chapter_status=result.status,
                    )
            except asyncio.CancelledError:
                for pending in tasks:
                    pending.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                if self.enable_chapter_dedupe:
                    self._save_download_index(manga_dir)
                self.emit_progress(
                    event="cancelled",
                    done_chapters=done_chapters,
                    total_chapters=total_selected,
                    saved_images=saved_images,
                    total_images=total_images,
                )
                raise

        order = {normalize_url(ch.url): idx for idx, (_, ch) in enumerate(selected_pairs)}
        chapter_results.sort(key=lambda item: order.get(normalize_url(item.url), 10**9))

        failed = [r for r in chapter_results if r.status in {"failed", "partial"}]
        succeeded = [r for r in chapter_results if r.status == "success"]

        retry_file = None
        if failed:
            retry_file = self._write_failed_retry_file(manga_dir, failed)

        archive_file = self._archive_manga_dir(manga_dir)
        metadata_file = self._write_metadata_sidecar(
            manga_dir,
            manga_title=manga_title,
            chapters=chapters,
            selected=selected,
            chapter_results=chapter_results,
            archive_file=archive_file,
        )
        failure_reasons = self._failure_reason_counts(chapter_results)
        if self.enable_chapter_dedupe:
            self._save_download_index(manga_dir)

        self.emit_progress(
            event="finished",
            done_chapters=len(chapter_results),
            total_chapters=total_selected,
            saved_images=sum(item.saved_images for item in chapter_results),
            total_images=sum(item.total_images for item in chapter_results),
            successful_chapters=len(succeeded),
            failed_chapters=len(failed),
            retry_file=str(retry_file) if retry_file else "",
            downloaded_bytes=downloaded_bytes,
        )

        self.log(
            f"Done. success={len(succeeded)}, failed_or_partial={len(failed)}, total={len(chapter_results)}"
        )

        return DownloadReport(
            manga_title=manga_title,
            manga_dir=manga_dir,
            total_chapters_found=len(chapters),
            selected_chapters=len(selected),
            successful_chapters=len(succeeded),
            failed_chapters=len(failed),
            retry_file=retry_file,
            chapter_results=chapter_results,
            started_at=started_at,
            finished_at=datetime.now(),
            downloaded_bytes=sum(item.downloaded_bytes for item in chapter_results),
            archive_file=archive_file,
            metadata_file=metadata_file,
            failure_reasons=failure_reasons,
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Async Toonily manga downloader")
    parser.add_argument("url", help="Toonily series URL")
    parser.add_argument(
        "-o",
        "--output",
        default="downloads",
        help="Output folder (default: downloads)",
    )
    parser.add_argument(
        "-c",
        "--chapters",
        default="all",
        help="Chapter selector, e.g. all | 1,2,5-8",
    )
    parser.add_argument(
        "--retry-file",
        help="Retry using chapter URLs from a previous failed list file",
    )
    parser.add_argument(
        "--failed-list-file",
        help="Where to save failed chapters list (default: <manga_dir>/failed_chapters_retry.txt)",
    )
    parser.add_argument(
        "--no-failed-list",
        action="store_true",
        help="Do not write failed chapters retry list",
    )
    parser.add_argument(
        "--chapter-concurrency",
        type=int,
        default=3,
        help="How many chapters to process in parallel",
    )
    parser.add_argument(
        "--image-concurrency",
        type=int,
        default=10,
        help="How many images to download in parallel",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry count for failed requests",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=45,
        help="Timeout seconds per request",
    )
    parser.add_argument(
        "--redis-host",
        default="",
        help="Redis host for HTML cache, e.g. 127.0.0.1",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)",
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=0,
        help="Redis db index (default: 0)",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=900,
        help="Redis cache ttl seconds (default: 900)",
    )
    parser.add_argument(
        "--redis-username",
        default="",
        help="Redis username (optional)",
    )
    parser.add_argument(
        "--redis-password",
        default="",
        help="Redis password (optional)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable Redis cache even if redis host is configured",
    )
    parser.add_argument(
        "--site-name",
        default="toonily",
        help="Site name for naming templates",
    )
    parser.add_argument(
        "--manga-dir-template",
        default="{site}/{manga}",
        help="Manga dir template",
    )
    parser.add_argument(
        "--chapter-dir-template",
        default="{chapter_number}-{chapter_title}",
        help="Chapter dir template",
    )
    parser.add_argument(
        "--page-name-template",
        default="{page:03}",
        help="Page file name template (without extension)",
    )
    parser.add_argument(
        "--image-output-format",
        default="original",
        choices=["original", "jpg", "webp"],
        help="Output image format",
    )
    parser.add_argument(
        "--image-quality",
        type=int,
        default=85,
        help="Output image quality (1-100) for converted images",
    )
    parser.add_argument(
        "--keep-original-images",
        action="store_true",
        help="Keep original image files when converting format",
    )
    parser.add_argument(
        "--archive-format",
        default="none",
        choices=["none", "cbz", "zip"],
        help="Archive output after download",
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Disable metadata.json sidecar output",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Disable chapter-level dedupe index",
    )
    parser.add_argument(
        "--retry-base-delay",
        type=float,
        default=0.8,
        help="Retry base delay in seconds (exponential backoff)",
    )
    parser.add_argument(
        "--retry-all-errors",
        action="store_true",
        help="Retry all errors (default only retries recoverable errors like timeout/429)",
    )
    parser.add_argument(
        "--bandwidth-day-kbps",
        type=int,
        default=0,
        help="Daytime bandwidth limit KB/s (0 means unlimited)",
    )
    parser.add_argument(
        "--bandwidth-night-kbps",
        type=int,
        default=0,
        help="Night bandwidth limit KB/s (0 means unlimited)",
    )
    parser.add_argument(
        "--night-start-hour",
        type=int,
        default=22,
        help="Night start hour (0-23)",
    )
    parser.add_argument(
        "--night-end-hour",
        type=int,
        default=7,
        help="Night end hour (0-23)",
    )
    return parser


async def _main_async(args: argparse.Namespace) -> None:
    chapter_urls = None
    if args.retry_file:
        chapter_urls = parse_retry_file(Path(args.retry_file).resolve())
        if not chapter_urls:
            raise RuntimeError("Retry file is empty or contains no valid chapter URLs.")
        print(f"Loaded {len(chapter_urls)} chapter URLs from retry file.")

    failed_list_file = Path(args.failed_list_file).resolve() if args.failed_list_file else None

    downloader = ToonilyAsyncDownloader(
        series_url=args.url,
        output_dir=Path(args.output).resolve(),
        chapter_selector=args.chapters,
        chapter_concurrency=args.chapter_concurrency,
        image_concurrency=args.image_concurrency,
        retries=args.retries,
        timeout=args.timeout,
        chapter_urls=chapter_urls,
        write_failed_file=not args.no_failed_list,
        failed_list_file=failed_list_file,
        cache_enabled=not args.no_cache,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_username=args.redis_username,
        redis_password=args.redis_password,
        cache_ttl_seconds=args.cache_ttl,
        site_name=args.site_name,
        manga_dir_template=args.manga_dir_template,
        chapter_dir_template=args.chapter_dir_template,
        page_name_template=args.page_name_template,
        image_output_format=args.image_output_format,
        image_quality=args.image_quality,
        keep_original_images=args.keep_original_images,
        auto_archive_format=args.archive_format,
        write_metadata_sidecar=not args.no_metadata,
        enable_chapter_dedupe=not args.no_dedupe,
        retry_base_delay_seconds=args.retry_base_delay,
        retry_recoverable_only=not args.retry_all_errors,
        bandwidth_day_kbps=args.bandwidth_day_kbps,
        bandwidth_night_kbps=args.bandwidth_night_kbps,
        night_start_hour=args.night_start_hour,
        night_end_hour=args.night_end_hour,
    )
    try:
        report = await downloader.run()
    finally:
        await downloader.close()

    if report.retry_file:
        print(f"Failed chapter retry list: {report.retry_file}")


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(_main_async(args))


if __name__ == "__main__":
    main()

