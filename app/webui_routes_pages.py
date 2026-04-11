from __future__ import annotations

from aiohttp import web

from app.webui_base import *
from app.webui_rendering import *

async def handle_root(_: web.Request) -> web.StreamResponse:
    raise build_redirect("/dashboard")


async def handle_dashboard(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    search_page = parse_int(request.query.get("sp", "1"), 1, minimum=1, maximum=999)
    search_page_size = parse_int(request.query.get("sps", "12"), 12, minimum=4, maximum=40)
    html = render_dashboard(
        state,
        msg,
        search_page=search_page,
        search_page_size=search_page_size,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_progress(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    selected_job_id = request.query.get("job", "").strip() or (state.current_job_id or "")
    html = render_progress(state, msg, selected_job_id)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_queue(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    html = render_queue(state, msg)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_bookshelf(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    bookshelf_page = parse_int(request.query.get("bp", "1"), 1, minimum=1, maximum=999)
    bookshelf_page_size = parse_int(request.query.get("bps", "24"), 24, minimum=6, maximum=60)
    bookshelf_group = str(request.query.get("bg", "") or "").strip()
    html = render_bookshelf(
        state,
        msg,
        bookshelf_page=bookshelf_page,
        bookshelf_page_size=bookshelf_page_size,
        bookshelf_group=bookshelf_group,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_follow(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    follow_page = parse_int(request.query.get("fp", "1"), 1, minimum=1, maximum=999)
    follow_page_size = parse_int(request.query.get("fps", "24"), 24, minimum=6, maximum=60)
    html = render_follow(
        state,
        msg,
        follow_page=follow_page,
        follow_page_size=follow_page_size,
    )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_follow_summary(request: web.Request) -> web.Response:
    state = get_app_state(request)
    follow_books = [book for book in state.list_books() if bool(book.get("follow_enabled", True))]
    pending_total = sum(max(0, int(book.get("pending_update_count", 0))) for book in follow_books)
    return web.json_response(
        {
            "ok": True,
            "total_books": len(follow_books),
            "pending_total": pending_total,
        }
    )


async def handle_health(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    html = render_health(state, msg)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def handle_settings_get(request: web.Request) -> web.Response:
    state = get_app_state(request)
    msg = pop_flash_message(request)
    html = render_settings(state, msg)
    return web.Response(text=html, content_type="text/html", charset="utf-8")
