from __future__ import annotations

from typing import Any, Callable, Optional

from downloaders.toonily import Chapter


class SiteProvider:
    provider_id = ""
    display_name = ""
    enabled = True
    disabled_reason = ""

    def ui_label(self) -> str:
        if self.enabled:
            return self.display_name
        return f"{self.display_name}（未启用）"

    async def search(self, state: Any, keyword: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_series_snapshot(
        self,
        state: Any,
        series_url: str,
        logger: Optional[Callable[[str], None]] = None,
    ) -> tuple[str, list[Chapter]]:
        raise NotImplementedError

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
        raise NotImplementedError
