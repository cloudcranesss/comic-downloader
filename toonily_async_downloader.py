import argparse
import asyncio
import hashlib
import mimetypes
import os
import re
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


def sanitize_name(name: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".")
    return cleaned or "untitled"


def normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


def parse_chapter_number(title: str) -> Optional[float]:
    match = re.search(r"chapter\s*(\d+(?:\.\d+)?)", title, re.IGNORECASE)
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
        redis_url: Optional[str] = None,
        redis_username: Optional[str] = None,
        redis_password: Optional[str] = None,
        cache_ttl_seconds: int = 900,
        cache_prefix: str = "toonily:html:",
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
        self.redis_url = (
            (redis_url or "").strip()
            or os.getenv("TOONILY_REDIS_URL", "").strip()
            or os.getenv("REDIS_URL", "").strip()
        )
        self.redis_username = (
            (redis_username or "").strip()
            or os.getenv("TOONILY_REDIS_USERNAME", "").strip()
            or os.getenv("REDIS_USERNAME", "").strip()
        )
        self.redis_password = (
            (redis_password or "").strip()
            or os.getenv("TOONILY_REDIS_PASSWORD", "").strip()
            or os.getenv("REDIS_PASSWORD", "").strip()
        )
        self.cache_ttl_seconds = max(30, int(cache_ttl_seconds))
        self.cache_prefix = cache_prefix

        self._redis: Optional[Redis] = None  # type: ignore[valid-type]
        self._cache_disabled_reason = ""

        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self.scraper.headers.update({"User-Agent": UA})

        self.image_semaphore = asyncio.Semaphore(self.image_concurrency)
        self.chapter_semaphore = asyncio.Semaphore(self.chapter_concurrency)

    def log(self, message: str) -> None:
        try:
            print(message)
        except UnicodeEncodeError:
            safe_message = message.encode("ascii", "backslashreplace").decode("ascii")
            print(safe_message)
        if self.logger:
            self.logger(message)

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
        if not self.redis_url:
            self._disable_cache("redis_url not configured")
            return None
        if Redis is None:
            self._disable_cache("redis package not installed")
            return None
        if self._redis is not None:
            return self._redis

        try:
            client = Redis.from_url(  # type: ignore[attr-defined]
                self.redis_url,
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
                if attempt < self.retries:
                    await self.wait_if_paused()
                    await self.ensure_not_cancelled()
                    await asyncio.sleep(1.2 * attempt)
        raise RuntimeError(f"Failed to fetch {url}: {last_error}")

    async def get_series_details(self) -> tuple[str, list[Chapter]]:
        html = await self.fetch_html(self.series_url)
        soup = BeautifulSoup(html, "html.parser")

        title_node = soup.select_one("div.post-title h1") or soup.select_one("h1")
        if title_node is None:
            raise RuntimeError("Could not find manga title.")

        for span in title_node.select("span"):
            span.decompose()
        manga_title = sanitize_name(title_node.get_text(" ", strip=True))

        chapters: list[Chapter] = []
        for a in soup.select("li.wp-manga-chapter > a"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            text = " ".join(a.get_text(" ", strip=True).split())
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
    ) -> bool:
        async with self.image_semaphore:
            for attempt in range(1, self.retries + 1):
                try:
                    await self.wait_if_paused()
                    await self.ensure_not_cancelled()
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
                        target_file.write_bytes(data)
                        return True
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if attempt < self.retries:
                        await self.wait_if_paused()
                        await self.ensure_not_cancelled()
                        await asyncio.sleep(0.8 * attempt)
            return False

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
        chapter: Chapter,
    ) -> ChapterResult:
        async with self.chapter_semaphore:
            await self.wait_if_paused()
            await self.ensure_not_cancelled()
            chapter_name = sanitize_name(chapter.title)
            chapter_dir = manga_dir / chapter_name
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
                    status="failed",
                    error="no images found",
                )

            total = len(images)
            width = max(3, len(str(total)))
            tasks: list[asyncio.Task[bool]] = []
            skipped = 0

            for index, image_url in enumerate(images, start=1):
                await self.wait_if_paused()
                await self.ensure_not_cancelled()
                ext = self._guess_extension(image_url)
                filename = f"{index:0{width}d}{ext}"
                target_file = chapter_dir / filename

                if target_file.exists() and target_file.stat().st_size > 0:
                    skipped += 1
                    continue

                tasks.append(
                    asyncio.create_task(
                        self._download_one_image(session, image_url, target_file, chapter.url)
                    )
                )

            downloaded = 0
            if tasks:
                results = await asyncio.gather(*tasks)
                downloaded = sum(1 for ok in results if ok)

            saved = downloaded + skipped
            if saved == total:
                status = "success"
                error = None
            elif saved > 0:
                status = "partial"
                error = f"saved {saved}/{total}"
            else:
                status = "failed"
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
                status=status,
                error=error,
            )

    async def _download_chapter_safe(
        self,
        session: aiohttp.ClientSession,
        manga_dir: Path,
        chapter: Chapter,
    ) -> ChapterResult:
        try:
            return await self.download_chapter(session, manga_dir, chapter)
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
            "# usage: python toonily_async_downloader.py <series_url> --retry-file <this_file>",
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

        manga_dir = self.output_dir / manga_title
        manga_dir.mkdir(parents=True, exist_ok=True)

        self.log(f"Manga: {manga_title}")
        self.log(f"Total chapters found: {len(chapters)}")
        self.log(f"Selected chapters: {len(selected)}")
        self.log(f"Output dir: {manga_dir}")
        self.emit_progress(
            event="init",
            done_chapters=0,
            total_chapters=len(selected),
            saved_images=0,
            total_images=0,
        )

        cookies = {c.name: c.value for c in self.scraper.cookies}
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        connector = aiohttp.TCPConnector(limit=max(16, self.image_concurrency * 2), ssl=False)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector, cookies=cookies) as session:
            tasks = [
                asyncio.create_task(self._download_chapter_safe(session, manga_dir, chapter))
                for chapter in selected
            ]

            chapter_results: list[ChapterResult] = []
            done_chapters = 0
            total_images = 0
            saved_images = 0

            try:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    chapter_results.append(result)
                    done_chapters += 1
                    total_images += result.total_images
                    saved_images += result.saved_images
                    self.emit_progress(
                        event="chapter_done",
                        done_chapters=done_chapters,
                        total_chapters=len(selected),
                        saved_images=saved_images,
                        total_images=total_images,
                        last_chapter_title=result.title,
                        last_chapter_status=result.status,
                    )
            except asyncio.CancelledError:
                for pending in tasks:
                    pending.cancel()
                self.emit_progress(
                    event="cancelled",
                    done_chapters=done_chapters,
                    total_chapters=len(selected),
                    saved_images=saved_images,
                    total_images=total_images,
                )
                raise

        order = {normalize_url(ch.url): idx for idx, ch in enumerate(selected)}
        chapter_results.sort(key=lambda item: order.get(normalize_url(item.url), 10**9))

        failed = [r for r in chapter_results if r.status in {"failed", "partial"}]
        succeeded = [r for r in chapter_results if r.status == "success"]

        retry_file = None
        if failed:
            retry_file = self._write_failed_retry_file(manga_dir, failed)

        self.emit_progress(
            event="finished",
            done_chapters=len(chapter_results),
            total_chapters=len(selected),
            saved_images=sum(item.saved_images for item in chapter_results),
            total_images=sum(item.total_images for item in chapter_results),
            successful_chapters=len(succeeded),
            failed_chapters=len(failed),
            retry_file=str(retry_file) if retry_file else "",
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
        "--redis-url",
        default="",
        help="Redis URL for HTML cache, e.g. redis://127.0.0.1:6379/0",
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
        help="Disable Redis cache even if redis url is configured",
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
        redis_url=args.redis_url,
        redis_username=args.redis_username,
        redis_password=args.redis_password,
        cache_ttl_seconds=args.cache_ttl,
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
