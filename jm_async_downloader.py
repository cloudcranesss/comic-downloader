from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from toonily_async_downloader import (
    Chapter,
    ChapterResult,
    DownloadReport,
    normalize_url,
    parse_selector,
    sanitize_name,
)

try:
    import jmcomic  # type: ignore[import-not-found]
except Exception as exc:  # pragma: no cover - optional dependency
    jmcomic = None  # type: ignore[assignment]
    _JM_IMPORT_ERROR: Optional[Exception] = exc
else:
    _JM_IMPORT_ERROR = None
    try:
        jmcomic.disable_jm_log()
    except Exception:
        pass


def jm_available() -> bool:
    return jmcomic is not None


def jm_unavailable_reason() -> str:
    if jmcomic is not None:
        return ""
    if _JM_IMPORT_ERROR is None:
        return "jmcomic 未安装。"
    return f"jmcomic 不可用：{_JM_IMPORT_ERROR}"


def _require_jm() -> Any:
    if jmcomic is None:
        raise RuntimeError(jm_unavailable_reason())
    return jmcomic


def _squash_spaces(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _parse_jm_id(value: Any) -> str:
    jm = _require_jm()
    return str(jm.JmcomicText.parse_to_jm_id(value))


def _resolve_base_url(client: Any) -> str:
    domains = list(getattr(client, "domain_list", []) or [])
    domain = domains[0] if domains else "18comic.vip"
    text = str(domain).strip()
    if text.startswith("http://") or text.startswith("https://"):
        text = text.split("://", 1)[1]
    text = text.strip().strip("/")
    if not text:
        text = "18comic.vip"
    return f"https://{text}"


def _build_album_url(base_url: str, album_id: str) -> str:
    return normalize_url(f"{base_url}/album/{album_id}")


def _build_photo_url(base_url: str, photo_id: str) -> str:
    return normalize_url(f"{base_url}/photo/{photo_id}")


def _normalize_cover_url(cover: str, base_url: str, album_id: str) -> str:
    text = str(cover or "").strip()
    if not text:
        jm = _require_jm()
        return str(jm.JmcomicText.get_album_cover_url(album_id))
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("/"):
        return f"{base_url}{text}"
    if text.startswith(("http://", "https://")):
        return text
    return text


def _build_jm_option(
    *,
    output_dir: Path,
    chapter_concurrency: int,
    image_concurrency: int,
    retries: int,
    timeout: int,
) -> Any:
    jm = _require_jm()
    option_dict = jm.JmOption.default_dict()
    option_dict["log"] = False

    option_dict["dir_rule"]["base_dir"] = str(output_dir.resolve())
    option_dict["dir_rule"]["rule"] = "Bd_Aname_Pindextitle"

    option_dict["download"]["threading"]["photo"] = max(1, int(chapter_concurrency))
    option_dict["download"]["threading"]["image"] = max(1, int(image_concurrency))
    option_dict["client"]["retry_times"] = max(1, int(retries))

    meta_data = option_dict["client"]["postman"]["meta_data"]
    meta_data["timeout"] = max(10, int(timeout))

    return jm.JmOption.construct(option_dict, cover_default=False)


def _chapter_from_photo(photo: Any, base_url: str) -> Chapter:
    photo_id = str(getattr(photo, "photo_id", "")).strip()
    if not photo_id:
        photo_id = str(getattr(photo, "id", "")).strip()
    index = _to_float(getattr(photo, "album_index", None))
    if index is None:
        index = _to_float(getattr(photo, "sort", None))

    title = _squash_spaces(getattr(photo, "indextitle", "") or getattr(photo, "name", ""))
    if not title:
        if index is None:
            title = f"Chapter {photo_id}"
        elif float(index).is_integer():
            title = f"Chapter {int(index)}"
        else:
            title = f"Chapter {index}"

    return Chapter(
        title=title,
        url=_build_photo_url(base_url, photo_id),
        number=index,
    )


def _format_latest_hint(info: dict[str, Any]) -> str:
    update_raw = info.get("update_at")
    if update_raw in (None, "", 0, "0"):
        return ""

    try:
        ts = int(update_raw)
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


async def _login_jm_client(
    client: Any,
    *,
    username: str,
    password: str,
    required: bool,
) -> bool:
    user = username.strip()
    pwd = str(password or "")

    if not user or not pwd:
        if required:
            raise RuntimeError("请先在设置中填写 JM 账号和密码。")
        return False

    try:
        await asyncio.to_thread(client.login, user, pwd)
        return True
    except Exception as exc:
        if required:
            raise RuntimeError(f"JM 登录失败：{exc}") from exc
        return False


async def search_jm(
    keyword: str,
    *,
    output_dir: Path,
    chapter_concurrency: int,
    image_concurrency: int,
    retries: int,
    timeout: int,
    max_results: int = 40,
    jm_username: str = "",
    jm_password: str = "",
) -> list[dict[str, Any]]:
    query = keyword.strip()
    if not query:
        return []

    option = _build_jm_option(
        output_dir=output_dir,
        chapter_concurrency=chapter_concurrency,
        image_concurrency=image_concurrency,
        retries=retries,
        timeout=timeout,
    )
    client = await asyncio.to_thread(option.new_jm_client)
    base_url = _resolve_base_url(client)
    await _login_jm_client(
        client,
        username=jm_username,
        password=jm_password,
        required=False,
    )

    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    if query.startswith(("http://", "https://", "JM", "jm")) or query.isdigit():
        try:
            album_id = _parse_jm_id(query)
            album = await asyncio.to_thread(client.get_album_detail, album_id)
            direct_url = _build_album_url(base_url, str(album.album_id))
            seen.add(direct_url)
            results.append(
                {
                    "title": _squash_spaces(str(getattr(album, "name", ""))) or f"JM-{album_id}",
                    "url": direct_url,
                    "latest": "",
                    "cover": _normalize_cover_url("", base_url, album_id),
                }
            )
        except Exception:
            pass

    try:
        page = await asyncio.to_thread(client.search_site, query, 1)
    except Exception:
        return results[:max_results]

    for album_id, info in page.content:
        aid = str(album_id).strip()
        if not aid:
            continue

        url = _build_album_url(base_url, aid)
        if url in seen:
            continue

        title = _squash_spaces(str(info.get("name") or ""))
        if not title:
            title = f"JM-{aid}"

        cover = _normalize_cover_url(str(info.get("image") or ""), base_url, aid)

        results.append(
            {
                "title": title,
                "url": url,
                "latest": _format_latest_hint(info),
                "cover": cover,
            }
        )
        seen.add(url)
        if len(results) >= max_results:
            break

    return results[:max_results]


async def fetch_series_snapshot_jm(
    series_url: str,
    *,
    output_dir: Path,
    chapter_concurrency: int,
    image_concurrency: int,
    retries: int,
    timeout: int,
    jm_username: str = "",
    jm_password: str = "",
) -> tuple[str, list[Chapter]]:
    option = _build_jm_option(
        output_dir=output_dir,
        chapter_concurrency=chapter_concurrency,
        image_concurrency=image_concurrency,
        retries=retries,
        timeout=timeout,
    )
    client = await asyncio.to_thread(option.new_jm_client)
    await _login_jm_client(
        client,
        username=jm_username,
        password=jm_password,
        required=False,
    )
    base_url = _resolve_base_url(client)

    album_id = _parse_jm_id(series_url)
    album = await asyncio.to_thread(client.get_album_detail, album_id)

    title = _squash_spaces(str(getattr(album, "name", ""))) or f"JM-{album_id}"
    chapters = [_chapter_from_photo(photo, base_url) for photo in album]
    chapters = sorted(
        chapters,
        key=lambda c: (float("inf") if c.number is None else c.number, c.title.lower()),
    )
    return title, chapters


async def sync_jm_favorites(
    *,
    output_dir: Path,
    chapter_concurrency: int,
    image_concurrency: int,
    retries: int,
    timeout: int,
    jm_username: str,
    jm_password: str,
    max_pages: int = 100,
) -> list[dict[str, str]]:
    option = _build_jm_option(
        output_dir=output_dir,
        chapter_concurrency=chapter_concurrency,
        image_concurrency=image_concurrency,
        retries=retries,
        timeout=timeout,
    )
    client = await asyncio.to_thread(option.new_jm_client)
    await _login_jm_client(
        client,
        username=jm_username,
        password=jm_password,
        required=True,
    )
    base_url = _resolve_base_url(client)

    items: list[dict[str, str]] = []
    seen_album_ids: set[str] = set()

    page_no = 1
    total_pages = 1
    max_pages = max(1, int(max_pages))

    while page_no <= total_pages and page_no <= max_pages:
        page = await asyncio.to_thread(client.favorite_folder, page_no)
        total_pages = max(1, int(getattr(page, "page_count", 1)))

        rows = list(getattr(page, "content", []) or [])
        if not rows and page_no == 1:
            break

        for album_id, info in rows:
            aid = str(album_id).strip()
            if not aid or aid in seen_album_ids:
                continue

            title = _squash_spaces(str((info or {}).get("name") or "")) or f"JM-{aid}"
            cover = _normalize_cover_url(str((info or {}).get("image") or ""), base_url, aid)

            items.append(
                {
                    "album_id": aid,
                    "title": title,
                    "url": _build_album_url(base_url, aid),
                    "cover": cover,
                }
            )
            seen_album_ids.add(aid)

        if not rows:
            break
        page_no += 1

    return items


class JMDownloadCancelled(BaseException):
    pass


@dataclass
class _ChapterState:
    title: str
    url: str
    number: Optional[float]
    total_images: int = 0
    saved_images: int = 0
    downloaded_images: int = 0
    skipped_images: int = 0
    done: bool = False
    error: str = ""


class _JMProgressTracker:
    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        selected: list[tuple[str, Chapter]],
        emit_progress: Callable[[dict[str, Any]], None],
        pause_waiter: Optional[Callable[[], Awaitable[None]]],
        cancel_checker: Optional[Callable[[], bool]],
    ) -> None:
        self._loop = loop
        self._emit_progress = emit_progress
        self._pause_waiter = pause_waiter
        self._cancel_checker = cancel_checker
        self._lock = threading.Lock()

        self._states: dict[str, _ChapterState] = {}
        self._order: list[str] = []

        for photo_id, chapter in selected:
            if photo_id in self._states:
                continue
            self._order.append(photo_id)
            self._states[photo_id] = _ChapterState(
                title=chapter.title,
                url=chapter.url,
                number=chapter.number,
            )

        self.done_chapters = 0
        self.total_images = 0
        self.saved_images = 0

    def _emit_threadsafe(self, payload: dict[str, Any]) -> None:
        try:
            self._loop.call_soon_threadsafe(self._emit_progress, payload)
        except RuntimeError:
            return

    def checkpoint_before_photo(self) -> None:
        if self._pause_waiter is not None:
            fut = asyncio.run_coroutine_threadsafe(self._pause_waiter(), self._loop)
            fut.result()
        self.checkpoint_before_image()

    def checkpoint_before_image(self) -> None:
        if self._cancel_checker and self._cancel_checker():
            raise JMDownloadCancelled("Task cancelled by user.")

    def _ensure_state(self, photo: Any) -> tuple[str, _ChapterState]:
        photo_id = str(getattr(photo, "photo_id", "")).strip() or str(getattr(photo, "id", "")).strip()
        if not photo_id:
            photo_id = "unknown"

        state = self._states.get(photo_id)
        if state is None:
            url = _build_photo_url("https://18comic.vip", photo_id)
            state = _ChapterState(
                title=_squash_spaces(getattr(photo, "name", "") or f"Chapter {photo_id}"),
                url=url,
                number=_to_float(getattr(photo, "album_index", None)) or _to_float(getattr(photo, "sort", None)),
            )
            self._states[photo_id] = state
            self._order.append(photo_id)
        return photo_id, state

    def on_before_photo(self, photo: Any) -> None:
        with self._lock:
            _, state = self._ensure_state(photo)
            if state.total_images <= 0:
                total = int(len(photo))
                state.total_images = max(0, total)
                self.total_images += state.total_images

    def on_before_image(self, image: Any) -> None:
        if not bool(getattr(image, "exists", False)):
            return
        with self._lock:
            _, state = self._ensure_state(image.from_photo)
            state.saved_images += 1
            state.skipped_images += 1
            self.saved_images += 1

    def on_after_image(self, image: Any) -> None:
        with self._lock:
            _, state = self._ensure_state(image.from_photo)
            state.saved_images += 1
            state.downloaded_images += 1
            self.saved_images += 1

    def on_after_photo(self, photo: Any) -> None:
        with self._lock:
            _, state = self._ensure_state(photo)
            if not state.done:
                state.done = True
                self.done_chapters += 1
            done = self.done_chapters
            total = len(self._order)
            saved = self.saved_images
            total_images = self.total_images
            title = state.title

        self._emit_threadsafe(
            {
                "event": "chapter_done",
                "done_chapters": done,
                "total_chapters": total,
                "saved_images": saved,
                "total_images": total_images,
                "last_chapter_title": title,
                "last_chapter_status": "success",
            }
        )

    def build_results(self, downloader: Any) -> list[ChapterResult]:
        failed_photo_error: dict[str, str] = {}
        failed_image_count: dict[str, int] = {}

        for photo, exc in getattr(downloader, "download_failed_photo", []):
            photo_id = str(getattr(photo, "photo_id", "")).strip() or str(getattr(photo, "id", "")).strip()
            failed_photo_error[photo_id] = str(exc)

        for image, exc in getattr(downloader, "download_failed_image", []):
            photo = getattr(image, "from_photo", None)
            photo_id = str(getattr(photo, "photo_id", "")).strip() or str(getattr(photo, "id", "")).strip()
            failed_image_count[photo_id] = failed_image_count.get(photo_id, 0) + 1
            failed_photo_error.setdefault(photo_id, str(exc))

        results: list[ChapterResult] = []
        for photo_id in self._order:
            state = self._states.get(photo_id)
            if state is None:
                continue

            total = max(0, int(state.total_images))
            saved = max(0, int(state.saved_images))
            error_text = failed_photo_error.get(photo_id, state.error).strip()

            if total <= 0:
                status = "failed" if saved <= 0 else "partial"
                if not error_text:
                    error_text = "no images found"
            elif saved >= total and failed_image_count.get(photo_id, 0) == 0 and photo_id not in failed_photo_error:
                status = "success"
                error_text = ""
            elif saved > 0:
                status = "partial"
                if not error_text:
                    error_text = f"saved {saved}/{total}"
            else:
                status = "failed"
                if not error_text:
                    error_text = f"saved {saved}/{total}"

            results.append(
                ChapterResult(
                    title=state.title,
                    url=state.url,
                    number=state.number,
                    total_images=total,
                    saved_images=saved,
                    status=status,
                    error=error_text or None,
                )
            )

        return results


if jmcomic is not None:

    class _JMProgressDownloader(jmcomic.JmDownloader):
        def __init__(
            self,
            option: Any,
            *,
            tracker: _JMProgressTracker,
            selected_photo_ids: set[str],
        ) -> None:
            super().__init__(option)
            self._tracker = tracker
            self._selected_photo_ids = selected_photo_ids

        def do_filter(self, detail: Any) -> Any:
            detail = super().do_filter(detail)
            if not self._selected_photo_ids:
                return detail
            if hasattr(detail, "is_album") and detail.is_album():
                return [photo for photo in detail if str(getattr(photo, "photo_id", "")) in self._selected_photo_ids]
            return detail

        def before_photo(self, photo: Any) -> None:
            self._tracker.checkpoint_before_photo()
            super().before_photo(photo)
            self._tracker.on_before_photo(photo)

        def before_image(self, image: Any, img_save_path: str) -> None:
            self._tracker.checkpoint_before_image()
            super().before_image(image, img_save_path)
            self._tracker.on_before_image(image)

        def after_image(self, image: Any, img_save_path: str) -> None:
            super().after_image(image, img_save_path)
            self._tracker.on_after_image(image)

        def after_photo(self, photo: Any) -> None:
            super().after_photo(photo)
            self._tracker.on_after_photo(photo)

else:

    class _JMProgressDownloader:  # pragma: no cover - jmcomic unavailable
        pass


class JMAsyncDownloader:
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
        jm_username: str = "",
        jm_password: str = "",
    ) -> None:
        self.series_url = series_url.strip()
        self.output_dir = output_dir
        self.chapter_selector = chapter_selector
        self.chapter_concurrency = max(1, chapter_concurrency)
        self.image_concurrency = max(1, image_concurrency)
        self.retries = max(1, retries)
        self.timeout = max(10, timeout)
        self.chapter_urls = chapter_urls or []
        self.write_failed_file = bool(write_failed_file)
        self.failed_list_file = failed_list_file
        self.logger = logger
        self.progress_callback = progress_callback
        self.pause_waiter = pause_waiter
        self.cancel_checker = cancel_checker
        self.jm_username = jm_username.strip()
        self.jm_password = str(jm_password or "")

    def log(self, message: str) -> None:
        try:
            print(message)
        except UnicodeEncodeError:
            safe = message.encode("ascii", "backslashreplace").decode("ascii")
            print(safe)
        if self.logger is not None:
            self.logger(message)

    def emit_progress(self, **payload: Any) -> None:
        if self.progress_callback is None:
            return
        try:
            self.progress_callback(payload)
        except Exception:
            return

    async def wait_if_paused(self) -> None:
        if self.pause_waiter is not None:
            await self.pause_waiter()

    def is_cancelled(self) -> bool:
        return bool(self.cancel_checker and self.cancel_checker())

    async def ensure_not_cancelled(self) -> None:
        if self.is_cancelled():
            raise asyncio.CancelledError("Task cancelled by user.")

    async def close(self) -> None:
        return None

    def _select_chapters(self, chapters: list[Chapter]) -> list[Chapter]:
        if self.chapter_urls:
            wanted: set[str] = set()
            for raw in self.chapter_urls:
                try:
                    wanted.add(_parse_jm_id(raw))
                except Exception:
                    continue
            if not wanted:
                return []
            selected: list[Chapter] = []
            for chapter in chapters:
                try:
                    photo_id = _parse_jm_id(chapter.url)
                except Exception:
                    continue
                if photo_id in wanted:
                    selected.append(chapter)
            return selected
        return parse_selector(self.chapter_selector, chapters)

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
            "# JM failed chapters retry list",
            f"# generated_at: {datetime.now().isoformat(timespec='seconds')}",
            f"# series_url: {self.series_url}",
            "# usage: 将下面链接放入重试文件，通过 WebUI 的下载更新重试",
            "",
        ]

        for item in failed_results:
            lines.append(f"# {item.title} | status={item.status} | reason={item.error or '-'}")
            lines.append(item.url)

        target.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.log(f"Retry list written: {target}")
        return target

    async def run(self) -> DownloadReport:
        _require_jm()
        started_at = datetime.now()
        await self.wait_if_paused()
        await self.ensure_not_cancelled()

        option = _build_jm_option(
            output_dir=self.output_dir,
            chapter_concurrency=self.chapter_concurrency,
            image_concurrency=self.image_concurrency,
            retries=self.retries,
            timeout=self.timeout,
        )
        client = await asyncio.to_thread(option.new_jm_client)
        await _login_jm_client(
            client,
            username=self.jm_username,
            password=self.jm_password,
            required=False,
        )
        base_url = _resolve_base_url(client)

        album_id = _parse_jm_id(self.series_url)
        album = await asyncio.to_thread(client.get_album_detail, album_id)

        manga_title = sanitize_name(_squash_spaces(str(getattr(album, "name", ""))) or f"JM-{album_id}")
        all_chapters = [_chapter_from_photo(photo, base_url) for photo in album]
        all_chapters = sorted(
            all_chapters,
            key=lambda c: (float("inf") if c.number is None else c.number, c.title.lower()),
        )
        selected_chapters = self._select_chapters(all_chapters)
        if not selected_chapters:
            raise RuntimeError("No chapters matched your selection.")

        selected: list[tuple[str, Chapter]] = []
        selected_photo_ids: set[str] = set()
        for chapter in selected_chapters:
            photo_id = _parse_jm_id(chapter.url)
            selected.append((photo_id, chapter))
            selected_photo_ids.add(photo_id)

        manga_dir = Path(option.dir_rule.decide_album_root_dir(album))
        manga_dir.mkdir(parents=True, exist_ok=True)

        self.log(f"Manga: {manga_title}")
        self.log(f"Total chapters found: {len(all_chapters)}")
        self.log(f"Selected chapters: {len(selected_chapters)}")
        self.log(f"Output dir: {manga_dir}")
        self.emit_progress(
            event="init",
            done_chapters=0,
            total_chapters=len(selected_chapters),
            saved_images=0,
            total_images=0,
        )

        loop = asyncio.get_running_loop()
        tracker = _JMProgressTracker(
            loop=loop,
            selected=selected,
            emit_progress=lambda payload: self.emit_progress(**payload),
            pause_waiter=self.pause_waiter,
            cancel_checker=self.cancel_checker,
        )

        def _run_sync_download() -> tuple[Any, Optional[BaseException]]:
            downloader = _JMProgressDownloader(
                option,
                tracker=tracker,
                selected_photo_ids=selected_photo_ids,
            )
            try:
                downloader.download_by_album_detail(album)
                return downloader, None
            except JMDownloadCancelled:
                raise
            except BaseException as exc:
                return downloader, exc

        try:
            downloader, sync_error = await asyncio.to_thread(_run_sync_download)
        except JMDownloadCancelled:
            self.emit_progress(
                event="cancelled",
                done_chapters=tracker.done_chapters,
                total_chapters=len(selected_chapters),
                saved_images=tracker.saved_images,
                total_images=tracker.total_images,
            )
            raise asyncio.CancelledError("Task cancelled by user.")

        chapter_results = tracker.build_results(downloader)
        order = {normalize_url(ch.url): idx for idx, ch in enumerate(selected_chapters)}
        chapter_results.sort(key=lambda item: order.get(normalize_url(item.url), 10**9))

        if sync_error is not None:
            self.log(f"[WARN] JM downloader reported error: {sync_error}")

        failed = [item for item in chapter_results if item.status in {"failed", "partial"}]
        succeeded = [item for item in chapter_results if item.status == "success"]
        retry_file = None
        if failed:
            retry_file = self._write_failed_retry_file(manga_dir, failed)

        total_images = sum(item.total_images for item in chapter_results)
        saved_images = sum(item.saved_images for item in chapter_results)
        self.emit_progress(
            event="finished",
            done_chapters=len(chapter_results),
            total_chapters=len(selected_chapters),
            saved_images=saved_images,
            total_images=total_images,
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
            total_chapters_found=len(all_chapters),
            selected_chapters=len(selected_chapters),
            successful_chapters=len(succeeded),
            failed_chapters=len(failed),
            retry_file=retry_file,
            chapter_results=chapter_results,
            started_at=started_at,
            finished_at=datetime.now(),
        )
