from __future__ import annotations

from app.webui_base import *
from app.webui_routes_pages import *
from app.webui_routes_actions import *
from app.webui_routes_jobs import *

async def on_shutdown(app: web.Application) -> None:
    state: UIState = app["state"]
    scheduler_task: Optional[asyncio.Task[Any]] = app.get("scheduler_task")
    if scheduler_task is not None and not scheduler_task.done():
        scheduler_task.cancel()
        await asyncio.gather(scheduler_task, return_exceptions=True)

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


async def on_startup(app: web.Application) -> None:
    state: UIState = app["state"]
    dispatch_jobs(state)
    if state.scheduler_enabled and not state.scheduler_next_run_at:
        state.schedule_next_run(immediate=False)
        await state.save_settings()
    app["scheduler_task"] = asyncio.create_task(scheduler_loop(app))


def create_app() -> web.Application:
    ensure_providers_loaded()
    ensure_data_dir_ready()
    state = UIState()
    state.load()
    state.output_dir.mkdir(parents=True, exist_ok=True)

    app = web.Application(middlewares=[flash_message_middleware])
    app["state"] = state

    app.add_routes(
        [
            web.get("/", handle_root),
            web.get("/dashboard", handle_dashboard),
            web.get("/progress", handle_progress),
            web.get("/queue", handle_queue),
            web.post("/queue/action", handle_queue_action),
            web.post("/search", handle_search),
            web.post("/search/action", handle_search_action),
            web.post("/dashboard/import", handle_batch_import),
            web.get("/bookshelf", handle_bookshelf),
            web.get("/bookshelf/export", handle_bookshelf_export),
            web.post("/bookshelf/import", handle_bookshelf_import),
            web.post("/bookshelf/jm-login", handle_bookshelf_jm_login),
            web.post("/bookshelf/jm-logout", handle_bookshelf_jm_logout),
            web.get("/follow", handle_follow),
            web.get("/follow/summary", handle_follow_summary),
            web.post("/follow/bulk", handle_follow_bulk),
            web.get("/health", handle_health),
            web.post("/bookshelf/sync-jm-favorites", handle_bookshelf_sync_jm_favorites),
            web.post("/bookshelf/bulk", handle_bookshelf_bulk),
            web.post("/bookshelf/{book_id}/{action}", handle_book_action),
            web.get("/api/books/{book_id}", handle_book_card),
            web.get("/settings", handle_settings_get),
            web.post("/settings", handle_settings_post),
            web.get("/job/{job_id}/state", handle_job_state),
            web.post("/job/{job_id}/{action}", handle_job_action),
            web.post("/scheduler/run", handle_scheduler_run),
        ]
    )
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app
