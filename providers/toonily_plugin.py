from __future__ import annotations

from typing import Any, Callable, Optional

from core.provider_base import SiteProvider
from downloaders.toonily import Chapter, ToonilyAsyncDownloader


class ToonilyProvider(SiteProvider):
    provider_id = "toonily"
    display_name = "Toonily"
    enabled = True

    def __init__(
        self,
        *,
        search_func: Callable[[Any, str], Any],
        snapshot_func: Callable[[Any, str, Optional[Callable[[str], None]]], Any],
    ) -> None:
        self._search_func = search_func
        self._snapshot_func = snapshot_func

    async def search(self, state: Any, keyword: str) -> list[dict[str, Any]]:
        return await self._search_func(state, keyword)

    async def fetch_series_snapshot(
        self,
        state: Any,
        series_url: str,
        logger: Optional[Callable[[str], None]] = None,
    ) -> tuple[str, list[Chapter]]:
        return await self._snapshot_func(state, series_url, logger=logger)

    def create_downloader(
        self,
        state: Any,
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
            redis_host=state.redis_host,
            redis_port=state.redis_port,
            redis_db=state.redis_db,
            redis_username=state.redis_username,
            redis_password=state.redis_password,
            cache_ttl_seconds=state.cache_ttl_seconds,
            manga_dir_template=state.manga_dir_template,
            chapter_dir_template=state.chapter_dir_template,
            page_name_template=state.page_name_template,
            image_output_format=state.image_output_format,
            image_quality=state.image_quality,
            keep_original_images=state.keep_original_images,
            auto_archive_format=state.auto_archive_format,
            write_metadata_sidecar=state.write_metadata_sidecar,
            enable_chapter_dedupe=state.enable_chapter_dedupe,
            retry_base_delay_seconds=state.retry_base_delay_seconds,
            retry_recoverable_only=state.retry_recoverable_only,
            bandwidth_day_kbps=state.bandwidth_day_kbps,
            bandwidth_night_kbps=state.bandwidth_night_kbps,
            night_start_hour=state.night_start_hour,
            night_end_hour=state.night_end_hour,
        )


def register(context: dict[str, Any]) -> SiteProvider:
    return ToonilyProvider(
        search_func=context["search_toonily"],
        snapshot_func=context["fetch_series_snapshot_toonily"],
    )
