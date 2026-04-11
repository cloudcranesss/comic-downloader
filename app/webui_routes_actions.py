from __future__ import annotations

from app.webui_base import *
from app.webui_rendering import *

def apply_import_book_fields(book: dict[str, Any], raw: dict[str, Any]) -> None:
    if "follow_enabled" in raw:
        book["follow_enabled"] = parse_bool(raw.get("follow_enabled"), bool(book.get("follow_enabled", True)))
    for key in (
        "last_downloaded_chapter_number",
        "last_downloaded_chapter_title",
        "last_downloaded_chapter_url",
        "latest_site_chapter_number",
        "latest_site_chapter_title",
        "latest_site_chapter_url",
        "last_checked_at",
        "last_update_at",
    ):
        if key in raw:
            value = raw.get(key)
            if key.endswith("_number"):
                book[key] = parse_float(value)
            else:
                book[key] = normalize_url(str(value)) if key.endswith("_url") else str(value or "").strip()
    if "pending_update_count" in raw:
        try:
            book["pending_update_count"] = max(0, int(raw.get("pending_update_count") or 0))
        except Exception:
            pass
    if "group" in raw:
        book["group"] = str(raw.get("group") or "").strip()


async def handle_bookshelf_export(request: web.Request) -> web.Response:
    state = get_app_state(request)
    items = state.list_books()
    payload = {
        "exported_at": now_iso(),
        "count": len(items),
        "books": items,
    }
    filename = f"bookshelf-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    return web.Response(
        text=json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
        charset="utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def handle_bookshelf_import(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    payload_text = ""
    upload = form.get("bookshelf_file")
    if hasattr(upload, "file"):
        try:
            payload_text = upload.file.read().decode("utf-8", errors="ignore")
        except Exception:
            payload_text = ""
    if not payload_text:
        payload_text = str(form.get("bookshelf_json", "") or "").strip()
    if not payload_text:
        raise back("请先选择要导入的 JSON 文件。")

    try:
        raw_data = json.loads(payload_text)
    except Exception as exc:
        raise back(f"导入失败，JSON 解析错误：{exc}")

    if isinstance(raw_data, list):
        rows = raw_data
    elif isinstance(raw_data, dict):
        candidate = raw_data.get("books")
        if not isinstance(candidate, list):
            candidate = raw_data.get("items")
        rows = candidate if isinstance(candidate, list) else []
    else:
        rows = []
    if not rows:
        raise back("导入内容为空或格式不正确。")

    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue
        series_url = normalize_url(str(row.get("series_url") or row.get("url") or "").strip())
        if not series_url:
            skipped += 1
            continue
        provider_id = str(row.get("provider_id") or detect_provider_id_by_url(series_url) or DEFAULT_PROVIDER_ID).strip().lower()
        title = str(row.get("title") or guess_title_from_url(series_url)).strip()
        cover = str(row.get("cover") or "").strip()
        group = str(row.get("group") or "").strip()
        book, is_created = state.upsert_book(
            provider_id=provider_id,
            title=title,
            series_url=series_url,
            cover=cover,
            group=group,
        )
        apply_import_book_fields(book, row)
        if is_created:
            created += 1
        else:
            updated += 1

    await state.save_bookshelf()
    raise back(f"导入完成：新增 {created} 本，更新 {updated} 本，跳过 {skipped} 条。")


async def handle_search(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    keyword = str(form.get("query", "")).strip()
    page_size = parse_int(form.get("page_size", "12"), 12, minimum=4, maximum=40)
    provider_id = str(form.get("provider_id", state.last_search_provider or DEFAULT_PROVIDER_ID)).strip().lower()
    provider = get_provider(provider_id)
    if not keyword:
        raise build_redirect("/dashboard", msg="请输入漫画名称。", sp=1, sps=page_size)
    reason = provider_disabled_reason(state, provider)
    if reason:
        raise build_redirect("/dashboard", msg=f"{provider.display_name} 不可用：{reason}", sp=1, sps=page_size)

    state.last_search_query = keyword
    state.last_search_provider = provider.provider_id
    state.last_search_results = await search_by_provider(state, provider.provider_id, keyword)
    if not state.last_search_results:
        raise build_redirect("/dashboard", msg="未搜索到结果，可尝试英文名或更短关键词。", sp=1, sps=page_size)
    raise build_redirect(
        "/dashboard",
        msg=f"搜索完成，共 {len(state.last_search_results)} 条结果。",
        sp=1,
        sps=page_size,
    )


async def handle_search_action(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()

    action = str(form.get("action", "")).strip()
    sp = parse_int(form.get("sp", "1"), 1, minimum=1, maximum=999)
    sps = parse_int(form.get("sps", "12"), 12, minimum=4, maximum=40)
    provider_id = str(form.get("provider_id", state.last_search_provider or DEFAULT_PROVIDER_ID)).strip().lower()
    provider = get_provider(provider_id)
    title = str(form.get("title", "")).strip()
    series_url = str(form.get("url", "")).strip()
    cover = str(form.get("cover", "")).strip()

    if not series_url:
        raise build_redirect("/dashboard", msg="缺少漫画链接。")
    reason = provider_disabled_reason(state, provider)
    if reason:
        raise build_redirect("/dashboard", msg=f"{provider.display_name} 不可用：{reason}")

    if action == "add_bookshelf":
        book, created = state.upsert_book(
            provider_id=provider.provider_id,
            title=title,
            series_url=series_url,
            cover=cover,
        )
        try:
            await refresh_book_snapshot(state, book)
        except Exception:
            pass
        await state.save_bookshelf()
        word = "已加入书架" if created else "书架已存在，已更新信息"
        raise build_redirect("/dashboard", msg=word, sp=sp, sps=sps)

    if action == "download_all":
        job = state.create_job(
            title=f"下载全部：{title or series_url}",
            series_url=series_url,
            chapter_selector="all",
            chapter_urls=[],
            mode="download_all",
            provider_id=provider.provider_id,
        )
        dispatch_jobs(state)
        raise build_redirect("/dashboard", msg="下载任务已创建。", sp=sp, sps=sps)

    if action == "follow_download":
        book, _ = state.upsert_book(
            provider_id=provider.provider_id,
            title=title,
            series_url=series_url,
            cover=cover,
        )
        await state.save_bookshelf()
        job = state.create_job(
            title=f"追更下载：{book['title']}",
            series_url=book["series_url"],
            chapter_selector="all",
            chapter_urls=[],
            mode="follow_download",
            book_id=book["id"],
            provider_id=provider.provider_id,
        )
        dispatch_jobs(state)
        raise build_redirect("/dashboard", msg="已加入书架并创建追更下载任务。", sp=sp, sps=sps)

    raise build_redirect("/dashboard", msg="未知操作。")


async def handle_batch_import(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()

    provider_id = str(form.get("provider_id", AUTO_PROVIDER_ID)).strip().lower()
    auto_detect = provider_id == AUTO_PROVIDER_ID
    fallback_provider_id = first_enabled_provider_id(state)
    if not auto_detect:
        fallback_provider_id = provider_id
        fixed_provider = get_provider(fallback_provider_id)
        fixed_reason = provider_disabled_reason(state, fixed_provider)
        if fixed_reason:
            raise build_redirect("/dashboard", msg=f"{fixed_provider.display_name} 不可用：{fixed_reason}")

    import_mode = str(form.get("import_mode", "queue_download")).strip().lower()
    text_payload = str(form.get("import_text", "") or "")
    filename = ""
    file_field = form.get("import_file")
    if hasattr(file_field, "filename") and hasattr(file_field, "file"):
        filename = str(getattr(file_field, "filename", "") or "")
        try:
            text_payload = file_field.file.read().decode("utf-8", errors="ignore")
        except Exception:
            text_payload = ""

    urls = parse_bulk_import_payload(text_payload, filename=filename)
    if not urls:
        raise build_redirect("/dashboard", msg="未识别到可导入的链接，请检查 txt/csv/json 内容。")

    created_books = 0
    updated_books = 0
    queued_jobs = 0
    skipped_urls = 0
    detected_success = 0
    detected_fallback = 0
    detected_unknown = 0
    provider_count: dict[str, int] = {}

    for url in urls:
        target_provider_id = fallback_provider_id
        if auto_detect:
            detected_id = detect_provider_id_by_url(url)
            if detected_id:
                detected_provider = get_provider(detected_id)
                detected_reason = provider_disabled_reason(state, detected_provider)
                if detected_reason:
                    target_provider_id = fallback_provider_id
                    detected_fallback += 1
                else:
                    target_provider_id = detected_provider.provider_id
                    detected_success += 1
            else:
                detected_unknown += 1

        provider = get_provider(target_provider_id)
        reason = provider_disabled_reason(state, provider)
        if reason:
            skipped_urls += 1
            continue

        title = guess_title_from_url(url)
        book, created = state.upsert_book(
            provider_id=provider.provider_id,
            title=title,
            series_url=url,
            cover="",
        )
        if created:
            created_books += 1
        else:
            updated_books += 1
        provider_count[provider.provider_id] = provider_count.get(provider.provider_id, 0) + 1

        if import_mode == "queue_download":
            job = state.create_job(
                title=f"批量导入下载：{book['title']}",
                series_url=book["series_url"],
                chapter_selector="all",
                chapter_urls=[],
                mode="batch_import",
                book_id=book["id"],
                provider_id=provider.provider_id,
            )
            state.append_job_log(job, "由批量导入创建。")
            queued_jobs += 1

    await state.save_bookshelf()
    dispatch_jobs(state)
    provider_summary = ", ".join(
        f"{provider_name(pid)} {count}"
        for pid, count in sorted(provider_count.items(), key=lambda item: item[0])
    ) or "无"

    if import_mode == "queue_download":
        if auto_detect:
            raise build_redirect(
                "/dashboard",
                msg=(
                    f"批量导入完成：链接 {len(urls)}，识别成功 {detected_success}，回退 {detected_fallback}，"
                    f"未识别 {detected_unknown}，跳过 {skipped_urls}；新增书架 {created_books}，"
                    f"更新 {updated_books}，入队任务 {queued_jobs}。分流：{provider_summary}。"
                ),
            )
        raise build_redirect(
            "/dashboard",
            msg=(
                f"批量导入完成：链接 {len(urls)}，跳过 {skipped_urls}；新增书架 {created_books}，"
                f"更新 {updated_books}，入队任务 {queued_jobs}。站点：{provider_summary}。"
            ),
        )
    if auto_detect:
        raise build_redirect(
            "/dashboard",
            msg=(
                f"批量导入完成：链接 {len(urls)}，识别成功 {detected_success}，回退 {detected_fallback}，"
                f"未识别 {detected_unknown}，跳过 {skipped_urls}；新增书架 {created_books}，"
                f"更新 {updated_books}。分流：{provider_summary}。"
            ),
        )
    raise build_redirect(
        "/dashboard",
        msg=(
            f"批量导入完成：链接 {len(urls)}，跳过 {skipped_urls}；新增书架 {created_books}，"
            f"更新 {updated_books}。站点：{provider_summary}。"
        ),
    )


async def handle_follow_bulk(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    ajax = request_wants_json(request, form)
    page = parse_int(form.get("fp", "1"), 1, minimum=1, maximum=999)
    page_size = parse_int(form.get("fps", "24"), 24, minimum=6, maximum=60)
    bulk_action = str(form.get("bulk_action", "")).strip().lower()
    raw_selected_ids: list[str] = []
    if hasattr(form, "getall"):
        try:
            raw_selected_ids = [str(v).strip() for v in form.getall("book_ids") if str(v).strip()]
        except KeyError:
            raw_selected_ids = []
    else:
        value = str(form.get("book_ids", "") or "").strip() if hasattr(form, "get") else ""
        if value:
            raw_selected_ids = [value]
    selected_ids = list(dict.fromkeys(raw_selected_ids))

    def back_redirect(message: str) -> web.HTTPSeeOther:
        return build_redirect("/follow", msg=message, fp=page, fps=page_size)

    follow_books = [book for book in state.list_books() if bool(book.get("follow_enabled", True))]

    if bulk_action == "follow_check_all":
        if not follow_books:
            if ajax:
                return web.json_response({"ok": False, "message": "当前没有开启追更的漫画。"}, status=400)
            raise back_redirect("当前没有开启追更的漫画。")
        checked = 0
        failed = 0
        for book in follow_books:
            try:
                await refresh_book_snapshot(state, book)
                checked += 1
            except Exception:
                failed += 1
        await state.save_bookshelf()
        pending_total = sum(max(0, int(book.get("pending_update_count", 0))) for book in follow_books)
        if failed:
            message = f"一键检查完成：成功 {checked} 本，失败 {failed} 本，待更新合计 {pending_total} 章。"
            if ajax:
                return web.json_response(
                    {
                        "ok": True,
                        "message": message,
                        "checked": checked,
                        "failed": failed,
                        "total_books": len(follow_books),
                        "pending_total": pending_total,
                    }
                )
            raise back_redirect(message)
        message = f"一键检查完成：已检查 {checked} 本，待更新合计 {pending_total} 章。"
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": message,
                    "checked": checked,
                    "failed": 0,
                    "total_books": len(follow_books),
                    "pending_total": pending_total,
                }
            )
        raise back_redirect(message)

    if bulk_action == "follow_update_all":
        if not follow_books:
            if ajax:
                return web.json_response({"ok": False, "message": "当前没有开启追更的漫画。"}, status=400)
            raise back_redirect("当前没有开启追更的漫画。")
        queued = 0
        no_update = 0
        failed = 0
        job_ids: list[str] = []
        for book in follow_books:
            created, detail = await enqueue_book_updates_job(
                state,
                book,
                source_message="由追更页一键更新创建。",
            )
            if created:
                queued += 1
                if detail:
                    job_ids.append(detail)
            elif detail:
                failed += 1
            else:
                no_update += 1
        dispatch_jobs(state)
        await state.save_bookshelf()
        message = f"一键更新完成：已创建 {queued} 个任务，无更新 {no_update} 本，失败 {failed} 本。"
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": message,
                    "queued": queued,
                    "no_update": no_update,
                    "failed": failed,
                    "job_ids": job_ids,
                }
            )
        raise back_redirect(message)

    if not selected_ids:
        if ajax:
            return web.json_response({"ok": False, "message": "请先选择至少一本漫画。"}, status=400)
        raise back_redirect("请先选择至少一本漫画。")

    selected_books: list[dict[str, Any]] = []
    for book_id in selected_ids:
        book = state.get_book(book_id)
        if book is not None:
            selected_books.append(book)
    if not selected_books:
        if ajax:
            return web.json_response({"ok": False, "message": "所选项目不存在或已被移除。"}, status=404)
        raise back_redirect("所选项目不存在或已被移除。")

    if bulk_action == "bulk_disable_follow":
        changed = 0
        for book in selected_books:
            if bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = False
                changed += 1
        await state.save_bookshelf()
        message = f"已取消 {changed} 本漫画的追更。"
        if ajax:
            return web.json_response({"ok": True, "message": message, "changed": changed})
        raise back_redirect(message)

    if ajax:
        return web.json_response({"ok": False, "message": "未知批量操作。"}, status=400)
    raise back_redirect("未知批量操作。")


async def handle_bookshelf_jm_login(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    provider = get_provider("jmcomic")
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    reason = provider_disabled_reason(state, provider)
    if reason:
        raise back(f"JM 不可用：{reason}")

    if not state.jm_username or not state.jm_password:
        raise build_redirect("/settings", msg="请先填写 JM 用户名和密码，再手动登录。")

    try:
        login_user = await manual_login_jm(
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
            proxy_url=state.get_provider_proxy_url("jmcomic"),
        )
    except Exception as exc:
        state.jm_manual_logged_in = False
        state.jm_manual_login_user = ""
        raise back(f"JM 手动登录失败：{exc}")

    state.jm_manual_logged_in = True
    state.jm_manual_login_user = login_user
    raise back(f"JM 手动登录成功：{login_user}")


async def handle_bookshelf_jm_logout(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    try:
        await manual_logout_jm(
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
            proxy_url=state.get_provider_proxy_url("jmcomic"),
        )
    except Exception:
        # Some jmcomic versions may not expose logout, treat local logout as source of truth.
        pass

    state.jm_manual_logged_in = False
    state.jm_manual_login_user = ""
    raise back("JM 已退出手动登录状态。")


async def handle_bookshelf_sync_jm_favorites(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    provider = get_provider("jmcomic")
    form = await request.post()
    bp = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    bps = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    bg = str(form.get("bg", "") or "").strip()

    def back(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": bp, "bps": bps}
        if bg:
            params["bg"] = bg
        return build_redirect("/bookshelf", msg=message, **params)

    reason = provider_disabled_reason(state, provider)
    if reason:
        raise back(f"JM 不可用：{reason}")

    if not state.jm_username or not state.jm_password:
        raise build_redirect("/settings", msg="请先填写 JM 用户名和密码，再同步收藏。")

    try:
        favorites = await sync_jm_favorites(
            output_dir=state.output_dir,
            chapter_concurrency=state.chapter_concurrency,
            image_concurrency=state.image_concurrency,
            retries=state.retries,
            timeout=state.timeout,
            jm_username=state.jm_username,
            jm_password=state.jm_password,
            proxy_url=state.get_provider_proxy_url("jmcomic"),
        )
    except Exception as exc:
        raise back(f"同步 JM 收藏失败：{exc}")

    created = 0
    updated = 0
    created_follow_off = 0
    for item in favorites:
        book, is_created = state.upsert_book(
            provider_id="jmcomic",
            title=str(item.get("title") or "").strip(),
            series_url=str(item.get("url") or "").strip(),
            cover=str(item.get("cover") or "").strip(),
        )
        if is_created:
            if bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = False
                created_follow_off += 1
            created += 1
        else:
            updated += 1

    await state.save_bookshelf()
    raise back(
        f"JM 收藏同步完成：共 {len(favorites)} 条，新增 {created}（默认关闭追更 {created_follow_off}），更新 {updated}。"
    )


async def handle_bookshelf_bulk(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    page = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
    page_size = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
    group_filter = str(form.get("bg", "") or "").strip()
    bulk_action = str(form.get("bulk_action", "")).strip().lower()

    raw_ids: list[str] = []
    if hasattr(form, "getall"):
        raw_ids = [str(v).strip() for v in form.getall("book_ids") if str(v).strip()]
    selected_ids = list(dict.fromkeys(raw_ids))

    def back_redirect(message: str) -> web.HTTPSeeOther:
        params: dict[str, Any] = {"bp": page, "bps": page_size}
        if group_filter:
            params["bg"] = group_filter
        return build_redirect("/bookshelf", msg=message, **params)

    if not selected_ids:
        raise back_redirect("请先选择至少一本漫画。")

    selected_books: list[dict[str, Any]] = []
    for book_id in selected_ids:
        book = state.get_book(book_id)
        if book is not None:
            selected_books.append(book)
    if not selected_books:
        raise back_redirect("所选项目不存在或已被移除。")

    if bulk_action == "bulk_enable_follow":
        changed = 0
        for book in selected_books:
            if not bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = True
                changed += 1
        await state.save_bookshelf()
        if changed:
            raise back_redirect(f"已为 {changed} 本漫画开启追更。")
        raise back_redirect("所选漫画已全部开启追更。")

    if bulk_action == "bulk_follow_download":
        enabled_changed = 0
        queued = 0
        unchanged = 0
        failed_titles: list[str] = []
        for book in selected_books:
            if not bool(book.get("follow_enabled", True)):
                book["follow_enabled"] = True
                enabled_changed += 1
            created, detail = await enqueue_book_updates_job(
                state,
                book,
                source_message="由书架批量追更下载创建。",
            )
            if created:
                queued += 1
            elif detail:
                failed_titles.append(f"{book.get('title') or '未命名漫画'}：{detail}")
            else:
                unchanged += 1
        await state.save_bookshelf()
        dispatch_jobs(state)

        msg = f"批量追更完成：入队 {queued} 本，无更新 {unchanged} 本"
        if enabled_changed:
            msg += f"，并开启追更 {enabled_changed} 本"
        if failed_titles:
            msg += f"，失败 {len(failed_titles)} 本"
        msg += "。"

        raise back_redirect(msg)

    if bulk_action == "bulk_download_all":
        queued = 0
        for book in selected_books:
            title = f"下载全部：{book.get('title') or '未命名漫画'}"
            job = state.create_job(
                title=title,
                series_url=str(book.get("series_url") or ""),
                chapter_selector="all",
                chapter_urls=[],
                mode="download_all",
                book_id=str(book.get("id") or ""),
                provider_id=str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
            )
            queued += 1
        dispatch_jobs(state)
        if queued:
            raise back_redirect(f"已为 {queued} 本漫画创建下载任务。")
        raise back_redirect("未能创建下载任务，请稍后重试。")

    if bulk_action == "bulk_set_group":
        group_name = str(form.get("bulk_group_name", "") or "").strip()
        for book in selected_books:
            book["group"] = group_name
        await state.save_bookshelf()
        if group_name:
            raise back_redirect(f"已将 {len(selected_books)} 本漫画设置到分组：{group_name}。")
        raise back_redirect(f"已将 {len(selected_books)} 本漫画设为未分组。")

    raise back_redirect("未知批量操作。")


async def handle_book_action(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    ajax = request_wants_json(request, form)
    src = str(form.get("src", "bookshelf")).strip().lower()
    if src == "follow":
        page = parse_int(form.get("fp", "1"), 1, minimum=1, maximum=999)
        page_size = parse_int(form.get("fps", "24"), 24, minimum=6, maximum=60)
        back_path = "/follow"
        back_params = {"fp": page, "fps": page_size}
    else:
        page = parse_int(form.get("bp", "1"), 1, minimum=1, maximum=999)
        page_size = parse_int(form.get("bps", "24"), 24, minimum=6, maximum=60)
        group_filter = str(form.get("bg", "") or "").strip()
        back_path = "/bookshelf"
        back_params = {"bp": page, "bps": page_size}
        if group_filter:
            back_params["bg"] = group_filter

    def back_redirect(message: str) -> web.HTTPSeeOther:
        return build_redirect(back_path, msg=message, **back_params)

    book_id = request.match_info["book_id"]
    action = request.match_info["action"]
    book = state.get_book(book_id)
    if book is None:
        if ajax:
            return web.json_response({"ok": False, "message": "书架项目不存在。"}, status=404)
        raise back_redirect("书架项目不存在。")

    if action == "remove":
        state.remove_book(book_id)
        await state.save_bookshelf()
        if ajax:
            return web.json_response({"ok": True, "message": "已移除。", "removed": True, "book_id": book_id})
        raise back_redirect("已移除。")

    if action == "toggle_follow":
        book["follow_enabled"] = not bool(book.get("follow_enabled", True))
        await state.save_bookshelf()
        message = f"追更已{'开启' if book['follow_enabled'] else '关闭'}。"
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": message,
                    "book": build_book_card_payload(book),
                    "book_id": str(book.get("id") or ""),
                }
            )
        raise back_redirect(message)

    if action == "check":
        try:
            pending = await refresh_book_snapshot(state, book)
            await state.save_bookshelf()
            message = f"检查完成，待更新 {len(pending)} 章。"
            if ajax:
                return web.json_response(
                    {
                        "ok": True,
                        "message": message,
                        "book": build_book_card_payload(book),
                        "book_id": str(book.get("id") or ""),
                        "pending_count": len(pending),
                    }
                )
            raise back_redirect(message)
        except Exception as exc:
            if ajax:
                return web.json_response({"ok": False, "message": f"检查失败：{exc}"}, status=400)
            raise back_redirect(f"检查失败：{exc}")

    if action in {"download_updates", "download_all"}:
        chapter_urls: list[str] = []
        mode = "download_updates" if action == "download_updates" else "download_all"

        try:
            _, chapters = await fetch_series_snapshot(
                state,
                str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
                book["series_url"],
            )
        except Exception as exc:
            if ajax:
                return web.json_response({"ok": False, "message": f"读取章节失败：{exc}"}, status=400)
            raise back_redirect(f"读取章节失败：{exc}")

        if action == "download_updates":
            pending = compute_pending_chapters(book, chapters)
            chapter_urls = [item.url for item in pending]
            set_site_latest_fields(book, chapters)
            book["pending_update_count"] = len(chapter_urls)
            book["last_checked_at"] = now_iso()
            if not chapter_urls:
                await state.save_bookshelf()
                if ajax:
                    return web.json_response(
                        {
                            "ok": True,
                            "message": "没有新的章节需要下载。",
                            "no_job": True,
                            "book": build_book_card_payload(book),
                            "book_id": str(book.get("id") or ""),
                        }
                    )
                raise back_redirect("没有新的章节需要下载。")
            title = f"下载更新：{book['title']} ({len(chapter_urls)} 章)"
        else:
            title = f"下载全部：{book['title']}"

        await state.save_bookshelf()
        job = state.create_job(
            title=title,
            series_url=book["series_url"],
            chapter_selector="all",
            chapter_urls=chapter_urls,
            mode=mode,
            book_id=book["id"],
            provider_id=str(book.get("provider_id") or DEFAULT_PROVIDER_ID),
        )
        dispatch_jobs(state)
        if ajax:
            return web.json_response(
                {
                    "ok": True,
                    "message": "任务已创建。",
                    "job_id": str(job.get("id") or ""),
                    "book_id": str(book.get("id") or ""),
                    "mode": mode,
                }
            )
        raise back_redirect("任务已创建。")

    if ajax:
        return web.json_response({"ok": False, "message": "未知操作。"}, status=400)
    raise back_redirect("未知操作。")


async def handle_book_card(request: web.Request) -> web.Response:
    state = get_app_state(request)
    book_id = str(request.match_info.get("book_id") or "").strip()
    book = state.get_book(book_id)
    if book is None:
        return web.json_response({"ok": False, "message": "book_not_found"}, status=404)
    return web.json_response({"ok": True, "book": build_book_card_payload(book)})


async def handle_settings_post(request: web.Request) -> web.StreamResponse:
    state = get_app_state(request)
    form = await request.post()
    try:
        state.output_dir = Path(str(form.get("output_dir", state.output_dir)).strip() or state.output_dir).resolve()
        state.chapter_concurrency = max(1, int(form.get("chapter_concurrency", state.chapter_concurrency)))
        state.image_concurrency = max(1, int(form.get("image_concurrency", state.image_concurrency)))
        state.retries = max(1, int(form.get("retries", state.retries)))
        state.timeout = max(10, int(form.get("timeout", state.timeout)))
        state.max_parallel_jobs = parse_int(form.get("max_parallel_jobs", state.max_parallel_jobs), state.max_parallel_jobs, minimum=1, maximum=20)
        state.retry_base_delay_seconds = max(0.2, float(form.get("retry_base_delay_seconds", state.retry_base_delay_seconds)))
        state.retry_recoverable_only = str(form.get("retry_recoverable_only", "1")).strip() == "1"
        state.enable_chapter_dedupe = str(form.get("enable_chapter_dedupe", "1")).strip() == "1"
        image_fmt = str(form.get("image_output_format", state.image_output_format)).strip().lower()
        state.image_output_format = image_fmt if image_fmt in {"original", "jpg", "webp"} else "original"
        state.image_quality = parse_int(form.get("image_quality", state.image_quality), state.image_quality, minimum=1, maximum=100)
        state.keep_original_images = str(form.get("keep_original_images", "0")).strip() == "1"
        archive_fmt = str(form.get("auto_archive_format", state.auto_archive_format)).strip().lower()
        state.auto_archive_format = archive_fmt if archive_fmt in {"none", "cbz", "zip"} else "none"
        state.write_metadata_sidecar = str(form.get("write_metadata_sidecar", "1")).strip() == "1"
        state.manga_dir_template = str(form.get("manga_dir_template", state.manga_dir_template)).strip() or "{site}/{manga}"
        state.chapter_dir_template = str(form.get("chapter_dir_template", state.chapter_dir_template)).strip() or "{chapter_number}-{chapter_title}"
        state.page_name_template = str(form.get("page_name_template", state.page_name_template)).strip() or "{page:03}"
        state.bandwidth_day_kbps = max(0, int(form.get("bandwidth_day_kbps", state.bandwidth_day_kbps)))
        state.bandwidth_night_kbps = max(0, int(form.get("bandwidth_night_kbps", state.bandwidth_night_kbps)))
        state.night_start_hour = parse_int(form.get("night_start_hour", state.night_start_hour), state.night_start_hour, minimum=0, maximum=23)
        state.night_end_hour = parse_int(form.get("night_end_hour", state.night_end_hour), state.night_end_hour, minimum=0, maximum=23)
        scheduler_enabled_before = state.scheduler_enabled
        state.scheduler_enabled = str(form.get("scheduler_enabled", "0")).strip() == "1"
        state.scheduler_interval_minutes = parse_int(form.get("scheduler_interval_minutes", state.scheduler_interval_minutes), state.scheduler_interval_minutes, minimum=5, maximum=1440)
        state.scheduler_auto_download = str(form.get("scheduler_auto_download", "1")).strip() == "1"
        if state.scheduler_enabled and (not scheduler_enabled_before or not state.scheduler_next_run_at):
            state.schedule_next_run(immediate=False)
        if not state.scheduler_enabled:
            state.scheduler_next_run_at = ""
        state.redis_host = str(form.get("redis_host", "")).strip()
        state.redis_port = parse_int(form.get("redis_port", state.redis_port), state.redis_port, minimum=1, maximum=65535)
        state.redis_db = parse_int(form.get("redis_db", state.redis_db), state.redis_db, minimum=0, maximum=999999)
        state.redis_username = str(form.get("redis_username", "")).strip()
        state.redis_password = str(form.get("redis_password", "")).strip()
        state.cache_ttl_seconds = max(30, int(form.get("cache_ttl_seconds", state.cache_ttl_seconds)))
        state.cache_enabled = str(form.get("cache_enabled", "1")) == "1"
        state.jm_username = str(form.get("jm_username", state.jm_username)).strip()
        state.jm_password = str(form.get("jm_password", state.jm_password)).strip()
        state.jm_manual_logged_in = False
        state.jm_manual_login_user = ""
        state.webhook_enabled = str(form.get("webhook_enabled", "0")).strip() == "1"
        state.webhook_url = str(form.get("webhook_url", state.webhook_url)).strip()
        state.webhook_token = str(form.get("webhook_token", state.webhook_token)).strip()
        state.webhook_event_completed = str(form.get("webhook_event_completed", "1")).strip() == "1"
        state.webhook_event_failed = str(form.get("webhook_event_failed", "1")).strip() == "1"
        state.webhook_event_cancelled = str(form.get("webhook_event_cancelled", "0")).strip() == "1"
        state.webhook_timeout_seconds = parse_int(
            form.get("webhook_timeout_seconds", state.webhook_timeout_seconds),
            state.webhook_timeout_seconds,
            minimum=3,
            maximum=30,
        )
        state.compact_mode_enabled = str(form.get("compact_mode_enabled", "0")).strip() == "1"
        view_mode = str(form.get("manga_view_mode", state.manga_view_mode)).strip().lower()
        state.manga_view_mode = view_mode if view_mode in {"poster", "list"} else "poster"
        default_proxy_raw = str(form.get("default_proxy_url", state.default_proxy_url) or "").strip()
        state.default_proxy_url = normalize_proxy_url(default_proxy_raw) if default_proxy_raw else ""
        for provider in list_providers():
            pid = provider.provider_id
            proxy_enabled = str(form.get(f"proxy_enabled_{pid}", "0")).strip() == "1"
            proxy_raw = str(form.get(f"proxy_url_{pid}", "") or "").strip()
            proxy_url = normalize_proxy_url(proxy_raw) if proxy_raw else ""
            state.set_provider_proxy_setting(pid, enabled=proxy_enabled, url=proxy_url)
        state.normalize_provider_proxy_settings()
        enabled_values = []
        if hasattr(form, "getall"):
            enabled_values = [str(v).strip().lower() for v in form.getall("enabled_providers") if str(v).strip()]
        state.set_enabled_providers(set(enabled_values))
        await state.save_settings()
        dispatch_jobs(state)
    except Exception as exc:
        raise build_redirect("/settings", msg=f"保存失败：{exc}")
    raise build_redirect("/settings", msg="设置已保存。")
