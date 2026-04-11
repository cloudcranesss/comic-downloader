from __future__ import annotations

import asyncio
from typing import Any, Optional

from aiohttp import web

from downloaders.toonily import DownloadReport

from app.webui_base import *
from app.webui_rendering import *

async def run_download_job(state: UIState, job: dict[str, Any]) -> None:
    job["status"] = "running"
    job["started_at"] = now_iso()
    state.append_job_log(job, "开始下载任务。")
    provider = get_provider(str(job.get("provider_id") or DEFAULT_PROVIDER_ID))
    state.append_job_log(job, f"使用站点：{provider.display_name}")
    effective_proxy = state.get_provider_proxy_url(provider.provider_id)
    if effective_proxy:
        state.append_job_log(job, f"使用代理：{mask_proxy_url(effective_proxy)}")

    reason = provider_disabled_reason(state, provider)
    if reason:
        job["status"] = "failed"
        job["finished_at"] = now_iso()
        job["error"] = reason
        state.append_job_log(job, f"任务失败：{job['error']}")
        state.record_download_report(provider.provider_id, None, "failed", reason)
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

    report: Optional[DownloadReport] = None
    downloader: Optional[Any] = None
    try:
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
        report = await downloader.run()
        job["status"] = "completed"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "任务完成。")
        if report.retry_file:
            job["retry_file"] = str(report.retry_file)
        state.record_download_report(provider.provider_id, report, "completed")
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "任务已取消。")
        state.record_download_report(provider.provider_id, report, "cancelled", "cancelled")
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = now_iso()
        state.append_job_log(job, f"任务失败：{exc}")
        state.record_download_report(provider.provider_id, report, "failed", str(exc))
    finally:
        if downloader is not None:
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

    if is_job_final(str(job.get("status") or "")):
        state.append_job_history(build_job_history_item(job=job, report=report))
        if state.webhook_enabled and state.webhook_url and state.webhook_event_enabled(str(job.get("status") or "")):
            asyncio.create_task(push_job_webhook(state, job, report))


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
        finally:
            job["task"] = None
            dispatch_jobs(state)

    task.add_done_callback(_finish_callback)


async def enqueue_book_updates_job(
    state: UIState,
    book: dict[str, Any],
    *,
    source_message: str = "",
) -> tuple[bool, str]:
    try:
        _, chapters = await fetch_series_snapshot(
            state,
            str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
            str(book.get("series_url") or ""),
        )
    except Exception as exc:
        return False, str(exc)

    pending = compute_pending_chapters(book, chapters)
    set_site_latest_fields(book, chapters)
    book["pending_update_count"] = len(pending)
    book["last_checked_at"] = now_iso()

    if not pending:
        return False, ""

    chapter_urls = [item.url for item in pending]
    title = f"下载更新：{book.get('title') or '未命名漫画'} ({len(chapter_urls)} 章)"
    job = state.create_job(
        title=title,
        series_url=str(book.get("series_url") or ""),
        chapter_selector="all",
        chapter_urls=chapter_urls,
        mode="download_updates",
        book_id=str(book.get("id") or ""),
        provider_id=str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
    )
    if source_message:
        state.append_job_log(job, source_message)
    return True, str(job.get("id") or "")


def create_retry_job_from_failed(state: UIState, failed_job: dict[str, Any]) -> Optional[dict[str, Any]]:
    status = str(failed_job.get("status") or "")
    if status not in {"failed", "cancelled"}:
        return None
    job = state.create_job(
        title=f"重试：{str(failed_job.get('title') or '下载任务')}",
        series_url=str(failed_job.get("series_url") or ""),
        chapter_selector=str(failed_job.get("chapter_selector") or "all"),
        chapter_urls=[str(item) for item in list(failed_job.get("chapter_urls") or []) if str(item).strip()],
        mode=str(failed_job.get("mode") or "download_all"),
        book_id=str(failed_job.get("book_id") or ""),
        provider_id=str(failed_job.get("provider_id") or DEFAULT_PROVIDER_ID),
    )
    state.append_job_log(job, f"由失败任务 {failed_job.get('id')} 重试创建。")
    return job


def cancel_job(state: UIState, job: dict[str, Any]) -> bool:
    status = str(job.get("status") or "")
    if status not in {"queued", "running", "paused", "cancelling"}:
        return False

    job["cancel_requested"] = True
    job["pause_event"].set()
    task = job.get("task")
    if task is None:
        job["status"] = "cancelled"
        job["finished_at"] = now_iso()
        state.append_job_log(job, "排队任务已取消。")
        state.append_job_history(build_job_history_item(job=job, report=None))
        if state.webhook_enabled and state.webhook_url and state.webhook_event_enabled("cancelled"):
            asyncio.create_task(push_job_webhook(state, job, None))
    else:
        job["status"] = "cancelling"
        state.append_job_log(job, "收到取消请求，正在停止任务。")
        if not task.done():
            task.cancel()
    return True


async def handle_queue_action(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    action = str(form.get("action", "") or "").strip().lower()

    selected_ids = list(dict.fromkeys(form_getall_str(form, "job_ids")))
    move_job_id = str(form.get("job_id", "") or "").strip()

    if action in {"move_up", "move_down"}:
        job = state.jobs.get(move_job_id)
        if job is None or str(job.get("status") or "") != "queued":
            raise build_redirect("/queue", msg="只能调整排队中的任务。")
        queued = queued_jobs_sorted(state)
        idx = next((i for i, row in enumerate(queued) if str(row.get("id") or "") == move_job_id), -1)
        if idx < 0:
            raise build_redirect("/queue", msg="任务不存在。")
        target_idx = idx - 1 if action == "move_up" else idx + 1
        if target_idx < 0 or target_idx >= len(queued):
            raise build_redirect("/queue", msg="已到边界，无法继续移动。")
        current = queued[idx]
        target = queued[target_idx]
        current_order = queue_order_value(current)
        target_order = queue_order_value(target)
        current["queue_order"] = target_order
        target["queue_order"] = current_order
        normalize_queue_orders(state)
        dispatch_jobs(state)
        raise build_redirect("/queue", msg="队列顺序已更新。")

    if action == "cancel_selected":
        if not selected_ids:
            raise build_redirect("/queue", msg="请先勾选任务。")
        cancelled = 0
        for job_id in selected_ids:
            job = state.jobs.get(job_id)
            if job is None:
                continue
            if cancel_job(state, job):
                cancelled += 1
        dispatch_jobs(state)
        raise build_redirect("/queue", msg=f"已处理取消请求 {cancelled} 个任务。")

    if action in {"retry_failed", "retry_all_failed"}:
        target_ids = selected_ids
        if action == "retry_all_failed":
            target_ids = [
                str(job.get("id") or "")
                for job in state.jobs.values()
                if str(job.get("status") or "") in {"failed", "cancelled"}
            ]
        if not target_ids:
            raise build_redirect("/queue", msg="没有可重试的失败任务。")
        retried = 0
        last_job_id = ""
        for job_id in target_ids:
            failed_job = state.jobs.get(job_id)
            if failed_job is None:
                continue
            new_job = create_retry_job_from_failed(state, failed_job)
            if new_job is None:
                continue
            retried += 1
            last_job_id = str(new_job.get("id") or "")
        dispatch_jobs(state)
        if retried and last_job_id:
            raise build_redirect("/queue", msg=f"已重试 {retried} 个任务。")
        raise build_redirect("/queue", msg="没有可重试的失败任务。")

    if action == "remove_finished":
        removed = 0
        target_ids = selected_ids
        if not target_ids:
            target_ids = [
                str(job.get("id") or "")
                for job in state.jobs.values()
                if is_job_final(str(job.get("status") or ""))
            ]
        for job_id in target_ids:
            job = state.jobs.get(job_id)
            if job is None:
                continue
            if is_job_final(str(job.get("status") or "")):
                state.jobs.pop(job_id, None)
                removed += 1
        if state.current_job_id and state.current_job_id not in state.jobs:
            state.current_job_id = ""
        raise build_redirect("/queue", msg=f"已移除 {removed} 个已结束任务。")

    raise build_redirect("/queue", msg="未知队列操作。")


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
    elif action == "cancel":
        cancel_job(state, job)
    dispatch_jobs(state)

    return web.json_response({"ok": True, "state": serialize_job(job)})


async def handle_scheduler_run(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    if state._scheduler_running:
        raise build_redirect("/health", msg="计划任务正在执行中，请稍后刷新。")

    state._scheduler_running = True
    try:
        scanned, enqueued = await run_scheduler_cycle(state)
        state.scheduler_last_run_at = now_iso()
        if state.scheduler_enabled:
            state.schedule_next_run(immediate=False)
        await state.save_settings()
    except Exception as exc:
        state._scheduler_running = False
        raise build_redirect("/health", msg=f"计划任务执行失败：{exc}")
    finally:
        state._scheduler_running = False

    raise build_redirect("/health", msg=f"计划任务执行完成：检查 {scanned} 本，新增任务 {enqueued} 个。")
