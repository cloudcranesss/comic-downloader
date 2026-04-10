from __future__ import annotations

from typing import Any, Callable, Optional

from downloaders.jm import (
    JMAsyncDownloader,
    fetch_series_snapshot_jm,
    jm_available,
    jm_unavailable_reason,
    search_jm,
)
from downloaders.toonily import Chapter
from provider_base import SiteProvider


class JMProvider(SiteProvider):
    provider_id = "jmcomic"
    display_name = "JMComic"

    def __init__(self) -> None:
        self.enabled = jm_available()
        self.disabled_reason = jm_unavailable_reason()

    async def search(self, state: Any, keyword: str) -> list[dict[str, Any]]:
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
        state: Any,
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
        state: Any,
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


def register(context: dict[str, Any]) -> SiteProvider:
    return JMProvider()
