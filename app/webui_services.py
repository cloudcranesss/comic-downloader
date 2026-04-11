from __future__ import annotations

from app.webui_core import *

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
        proxy_url=state.get_provider_proxy_url("toonily"),
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
        proxy_url=state.get_provider_proxy_url("toonily"),
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

