from __future__ import annotations

from app.webui_core import *

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
        proxy_env = os.getenv("COMIC_PROXY_URL", "").strip()
        try:
            self.default_proxy_url = normalize_proxy_url(proxy_env) if proxy_env else ""
        except Exception:
            self.default_proxy_url = ""
        self.provider_proxy_settings: dict[str, dict[str, Any]] = {}
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
                raw_proxy = str(raw.get("default_proxy_url", raw.get("proxy_url", self.default_proxy_url)) or "").strip()
                try:
                    self.default_proxy_url = normalize_proxy_url(raw_proxy) if raw_proxy else ""
                except Exception:
                    self.default_proxy_url = ""
                raw_proxy_settings = raw.get("provider_proxy_settings")
                if isinstance(raw_proxy_settings, dict):
                    parsed_proxy_settings: dict[str, dict[str, Any]] = {}
                    for pid_raw, row in raw_proxy_settings.items():
                        pid = str(pid_raw).strip().lower()
                        if not pid:
                            continue
                        enabled = False
                        url = ""
                        if isinstance(row, dict):
                            enabled = parse_bool(row.get("enabled", False), False)
                            url_raw = str(row.get("url", "") or "").strip()
                            try:
                                url = normalize_proxy_url(url_raw) if url_raw else ""
                            except Exception:
                                url = ""
                        parsed_proxy_settings[pid] = {
                            "enabled": enabled,
                            "url": url,
                        }
                    self.provider_proxy_settings = parsed_proxy_settings
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
        if "COMIC_PROXY_URL" in os.environ:
            proxy_text = os.getenv("COMIC_PROXY_URL", "").strip()
            try:
                self.default_proxy_url = normalize_proxy_url(proxy_text) if proxy_text else ""
            except Exception:
                self.default_proxy_url = ""
        if not self.redis_host:
            self.cache_enabled = False

        self.normalize_enabled_providers()
        self.normalize_provider_proxy_settings()

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
            "default_proxy_url": self.default_proxy_url,
            "provider_proxy_settings": {
                pid: {
                    "enabled": parse_bool((row or {}).get("enabled", False), False),
                    "url": str((row or {}).get("url", "") or "").strip(),
                }
                for pid, row in sorted(self.provider_proxy_settings.items(), key=lambda item: item[0])
            },
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

    def normalize_provider_proxy_settings(self) -> None:
        ensure_providers_loaded()
        known = {str(pid).strip().lower() for pid in PROVIDERS.keys() if str(pid).strip()}
        normalized: dict[str, dict[str, Any]] = {}
        for pid in sorted(known):
            row = self.provider_proxy_settings.get(pid, {})
            enabled = False
            url = ""
            if isinstance(row, dict):
                enabled = parse_bool(row.get("enabled", False), False)
                url_raw = str(row.get("url", "") or "").strip()
                try:
                    url = normalize_proxy_url(url_raw) if url_raw else ""
                except Exception:
                    url = ""
            normalized[pid] = {
                "enabled": enabled,
                "url": url,
            }
        self.provider_proxy_settings = normalized

    def set_provider_proxy_setting(self, provider_id: str, *, enabled: bool, url: str) -> None:
        pid = (provider_id or "").strip().lower()
        if not pid:
            return
        normalized_url = normalize_proxy_url(url) if str(url or "").strip() else ""
        self.provider_proxy_settings[pid] = {
            "enabled": bool(enabled),
            "url": normalized_url,
        }

    def provider_proxy_row(self, provider_id: str) -> dict[str, Any]:
        pid = (provider_id or "").strip().lower()
        row = self.provider_proxy_settings.get(pid, {})
        if not isinstance(row, dict):
            row = {}
        return {
            "enabled": parse_bool(row.get("enabled", False), False),
            "url": str(row.get("url", "") or "").strip(),
        }

    def get_provider_proxy_url(self, provider_id: str) -> str:
        row = self.provider_proxy_row(provider_id)
        if not bool(row.get("enabled", False)):
            return ""
        return str(row.get("url", "") or self.default_proxy_url).strip()

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


