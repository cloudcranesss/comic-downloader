from __future__ import annotations

import math
from html import escape
from typing import Any, Optional
from urllib.parse import urlencode, urljoin

from app.webui_base import *

def render_layout(
    *,
    title: str,
    active_nav: str,
    body: str,
    script: str = "",
    compact_mode: bool = False,
    manga_view_mode: str = "poster",
) -> str:
    nav_items = [
        ("dashboard", "主页", "/dashboard"),
        ("progress", "进度", "/progress"),
        ("queue", "队列", "/queue"),
        ("bookshelf", "书架", "/bookshelf"),
        ("follow", "追更", "/follow"),
        ("health", "监控", "/health"),
        ("settings", "设置", "/settings"),
    ]
    nav_html_parts = []
    for key, label, href in nav_items:
        cls = "nav-link active" if key == active_nav else "nav-link"
        nav_html_parts.append(f'<a class="{cls}" href="{href}">{escape(label)}</a>')
    nav_html = "\n".join(nav_html_parts)
    theme_bootstrap_script = (
        "<script>"
        "(function(){"
        "try{"
        "var key='comic-ui-theme';"
        "var saved=localStorage.getItem(key);"
        "var prefersDark=window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches;"
        "var theme=(saved==='dark'||saved==='light')?saved:(prefersDark?'dark':'light');"
        "document.documentElement.setAttribute('data-theme',theme);"
        "}catch(_){document.documentElement.setAttribute('data-theme','light');}"
        "})();"
        "</script>"
    )
    theme_toggle_script = (
        "<script>"
        "(function(){"
        "var key='comic-ui-theme';"
        "var btn=document.getElementById('theme-toggle');"
        "if(!btn){return;}"
        "var root=document.documentElement;"
        "function applyTheme(theme){"
        "root.setAttribute('data-theme',theme);"
        "btn.setAttribute('title',theme==='dark'?'当前深色主题，点击切换浅色':'当前浅色主题，点击切换深色');"
        "btn.setAttribute('aria-label',theme==='dark'?'当前深色主题，点击切换浅色':'当前浅色主题，点击切换深色');"
        "}"
        "var saved='';"
        "try{saved=localStorage.getItem(key)||'';}catch(_){saved='';}"
        "var prefersDark=window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches;"
        "var current=(saved==='dark'||saved==='light')?saved:(prefersDark?'dark':'light');"
        "applyTheme(current);"
        "btn.addEventListener('click',function(){"
        "current=root.getAttribute('data-theme')==='dark'?'light':'dark';"
        "applyTheme(current);"
        "try{localStorage.setItem(key,current);}catch(_){ }"
        "});"
        "})();"
        "</script>"
    )

    return (
        "<!doctype html>\n"
        f"<html lang=\"zh-CN\" data-compact=\"{'1' if compact_mode else '0'}\" "
        f"data-view-mode=\"{escape(manga_view_mode if manga_view_mode in {'poster', 'list'} else 'poster')}\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        f"  <title>{escape(title)}</title>\n"
        "  <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\n"
        "  <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>\n"
        "  <link href=\"https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Noto+Sans+SC:wght@400;500;700;900&display=swap\" rel=\"stylesheet\">\n"
        f"  {theme_bootstrap_script}\n"
        "  <style>\n"
        "    :root {\n"
        "      --primary: #0f6fff;\n"
        "      --primary-light: #287df8;\n"
        "      --secondary: #0ea5a0;\n"
        "      --accent: #0ea5a0;\n"
        "      --warning: #f59e0b;\n"
        "      --danger: #dc2626;\n"
        "      --text: #1f2937;\n"
        "      --muted: #667085;\n"
        "      --bg-dark: #f6f7fb;\n"
        "      --bg-darker: #eef2f8;\n"
        "      --panel: #ffffff;\n"
        "      --panel-border: #dfe4ee;\n"
        "      --glass: #f5f8ff;\n"
        "      --glass-hover: #edf2ff;\n"
        "      --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);\n"
        "    }\n"
        "    * { box-sizing: border-box; }\n"
        "    body {\n"
        "      margin: 0;\n"
        "      color: var(--text);\n"
        "      font-family: 'Manrope', 'Noto Sans SC', 'PingFang SC', 'Microsoft YaHei', sans-serif;\n"
        "      background: radial-gradient(1200px 520px at 8% -10%, #e7efff 0%, transparent 60%), radial-gradient(1000px 460px at 95% -12%, #d8fbf6 0%, transparent 62%), linear-gradient(180deg, var(--bg-dark) 0%, #f9fbff 100%);\n"
        "      min-height: 100vh;\n"
        "      overflow-x: hidden;\n"
        "    }\n"
        "    .bg-effects { display: none; }\n"
        "    .grid-pattern { display: none; }\n"
        "    @keyframes float {\n"
        "      0%,100% { transform: translate(0,0) scale(1); }\n"
        "      25% { transform: translate(36px, -42px) scale(1.05); }\n"
        "      50% { transform: translate(-24px, 34px) scale(0.95); }\n"
        "      75% { transform: translate(18px, 44px) scale(1.02); }\n"
        "    }\n"
        "    .shell { width: min(1320px, calc(100% - 24px)); margin: 14px auto 30px; }\n"
        "    .top {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: space-between;\n"
        "      gap: 12px;\n"
        "      margin-bottom: 14px;\n"
        "      position: sticky;\n"
        "      top: 10px;\n"
        "      z-index: 20;\n"
        "      background: rgba(255,255,255,0.92);\n"
        "      border: 1px solid var(--panel-border);\n"
        "      border-radius: 16px;\n"
        "      backdrop-filter: blur(8px);\n"
        "      box-shadow: var(--shadow);\n"
        "      padding: 12px 14px;\n"
        "    }\n"
        "    .logo { font-size: 28px; font-weight: 900; letter-spacing: 0.4px; color: #0b1324; font-family: 'Noto Sans SC', sans-serif; }\n"
        "    .top-actions {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: flex-end;\n"
        "      gap: 8px;\n"
        "      flex-wrap: wrap;\n"
        "      margin: 0;\n"
        "    }\n"
        "    .nav { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin: 0; padding: 0; }\n"
        "    .nav-link {\n"
        "      text-decoration: none;\n"
        "      color: #334155;\n"
        "      font-size: 14px;\n"
        "      padding: 9px 12px;\n"
        "      border-radius: 11px;\n"
        "      border: 1px solid transparent;\n"
        "      transition: all 0.15s ease;\n"
        "      background: transparent;\n"
        "      font-weight: 700;\n"
        "    }\n"
        "    .nav-link:hover { color: #0b1324; border-color: #d8e2f4; background: #f1f5ff; transform: translateY(-1px); }\n"
        "    .nav-link.active {\n"
        "      color: #fff;\n"
        "      font-weight: 800;\n"
        "      background: linear-gradient(135deg, var(--primary), var(--primary-light));\n"
        "      border-color: var(--primary);\n"
        "      box-shadow: 0 8px 18px rgba(15, 111, 255, 0.35);\n"
        "    }\n"
        "    .theme-toggle {\n"
        "      min-width: 36px;\n"
        "      justify-content: center;\n"
        "      font-weight: 700;\n"
        "      min-height: 36px;\n"
        "      padding: 8px;\n"
        "      line-height: 1.2;\n"
        "      align-self: center;\n"
        "      position: relative;\n"
        "      top: -1px;\n"
        "      margin: 0;\n"
        "    }\n"
        "    .theme-toggle .btn-icon { width: 16px; height: 16px; }\n"
        "    .panel {\n"
        "      background: var(--panel);\n"
        "      border: 1px solid var(--panel-border);\n"
        "      border-radius: 16px;\n"
        "      box-shadow: var(--shadow);\n"
        "      padding: 16px;\n"
        "      margin-bottom: 14px;\n"
        "    }\n"
        "    .title { margin: 0 0 10px; font-size: 20px; font-weight: 900; letter-spacing: 0.2px; }\n"
        "    .subtle { color: var(--muted); font-size: 13px; line-height: 1.5; }\n"
        "    .msg {\n"
        "      position: fixed;\n"
        "      top: 12px;\n"
        "      left: 50%;\n"
        "      transform: translate(-50%, -10px);\n"
        "      z-index: 9999;\n"
        "      width: min(760px, calc(100% - 20px));\n"
        "      border-radius: 12px;\n"
        "      background: #f0f6ff;\n"
        "      border: 1px solid #b4d0ff;\n"
        "      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.16);\n"
        "      color: #0b3f9a;\n"
        "      padding: 10px 12px;\n"
        "      font-size: 14px;\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      gap: 8px;\n"
        "      opacity: 0;\n"
        "      pointer-events: none;\n"
        "      transition: opacity 0.2s ease, transform 0.2s ease;\n"
        "    }\n"
        "    .msg.show {\n"
        "      opacity: 1;\n"
        "      transform: translate(-50%, 0);\n"
        "      pointer-events: auto;\n"
        "    }\n"
        "    .msg.hide {\n"
        "      opacity: 0;\n"
        "      transform: translate(-50%, -10px);\n"
        "      pointer-events: none;\n"
        "    }\n"
        "    .msg-text {\n"
        "      flex: 1;\n"
        "      min-width: 0;\n"
        "    }\n"
        "    .msg-close {\n"
        "      border: 0;\n"
        "      background: transparent;\n"
        "      color: #0b3f9a;\n"
        "      font-size: 18px;\n"
        "      line-height: 1;\n"
        "      cursor: pointer;\n"
        "      padding: 0 2px;\n"
        "      opacity: 0.85;\n"
        "    }\n"
        "    .msg-close:hover { opacity: 1; }\n"
        "    .split-grid { display: grid; grid-template-columns: 1.2fr 1fr; gap: 12px; }\n"
        "    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 10px; }\n"
        "    .result-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));\n"
        "      gap: 10px;\n"
        "      align-items: stretch;\n"
        "    }\n"
        "    .search-form { display: grid; grid-template-columns: minmax(220px, 1fr) 220px 140px auto; gap: 10px; align-items: end; }\n"
        "    .input,\n"
        "    .select {\n"
        "      background: #fff;\n"
        "      color: var(--text);\n"
        "      border: 1px solid #d6deec;\n"
        "      border-radius: 11px;\n"
        "      padding: 9px 11px;\n"
        "      min-height: 40px;\n"
        "      width: 100%;\n"
        "      transition: border-color 0.15s ease, box-shadow 0.15s ease;\n"
        "    }\n"
        "    .input:focus,\n"
        "    .select:focus { outline: none; border-color: #8ab3ff; box-shadow: 0 0 0 3px rgba(15, 111, 255, 0.14); }\n"
        "    .input::placeholder { color: #94a3b8; }\n"
        "    .btn {\n"
        "      border: 1px solid var(--primary);\n"
        "      border-radius: 11px;\n"
        "      padding: 9px 12px;\n"
        "      min-height: 38px;\n"
        "      cursor: pointer;\n"
        "      color: #fff;\n"
        "      font-weight: 800;\n"
        "      font-size: 13px;\n"
        "      background: linear-gradient(135deg, var(--primary), var(--primary-light));\n"
        "      transition: all 0.16s ease;\n"
        "      text-decoration: none;\n"
        "      display: inline-flex;\n"
        "      align-items: center;\n"
        "      justify-content: center;\n"
        "      white-space: nowrap;\n"
        "    }\n"
        "    .btn:hover { transform: translateY(-1px); filter: brightness(1.02); box-shadow: 0 8px 16px rgba(15, 111, 255, 0.26); }\n"
        "    .btn.secondary {\n"
        "      border-color: #0e8f8a;\n"
        "      background: linear-gradient(135deg, #0ea5a0, #139c8e);\n"
        "    }\n"
        "    .btn.ghost {\n"
        "      color: #334155;\n"
        "      border: 1px solid #c8d4ea;\n"
        "      background: #fff;\n"
        "      box-shadow: none;\n"
        "    }\n"
        "    .btn.warn { border-color: var(--danger); background: linear-gradient(135deg, var(--danger), #ef4444); color: #fff; }\n"
        "    .btn[disabled] { opacity: 0.52; cursor: not-allowed; transform: none; }\n"
        "    .icon-btn {\n"
        "      min-height: 34px;\n"
        "      padding: 8px 10px;\n"
        "      border-radius: 10px;\n"
        "      justify-content: center;\n"
        "      gap: 6px;\n"
        "    }\n"
        "    .btn-icon {\n"
        "      width: 14px;\n"
        "      height: 14px;\n"
        "      display: inline-block;\n"
        "      flex: 0 0 auto;\n"
        "    }\n"
        "    .btn-text {\n"
        "      display: inline-block;\n"
        "      line-height: 1;\n"
        "    }\n"
        "    .icon-btn .btn-text {\n"
        "      display: inline-block;\n"
        "      max-width: 96px;\n"
        "      overflow: hidden;\n"
        "      text-overflow: ellipsis;\n"
        "      white-space: nowrap;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    .actions .icon-btn,\n"
        "    .book-actions .icon-btn,\n"
        "    .job-actions .icon-btn,\n"
        "    html[data-view-mode='list'] .book-card > .book-actions .icon-btn,\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions .icon-btn {\n"
        "      min-width: 72px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "      gap: 4px;\n"
        "    }\n"
        "    .actions .icon-btn .btn-text,\n"
        "    .book-actions .icon-btn .btn-text,\n"
        "    .job-actions .icon-btn .btn-text,\n"
        "    html[data-view-mode='list'] .book-card > .book-actions .icon-btn .btn-text,\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions .icon-btn .btn-text {\n"
        "      display: inline-block;\n"
        "      max-width: 74px;\n"
        "    }\n"
        "    html[data-compact='1'] .subtle { font-size: 12px; }\n"
        "    .result-card {\n"
        "      border-radius: 14px;\n"
        "      border: 1px solid #dbe3f0;\n"
        "      background: linear-gradient(180deg, #ffffff 0%, #fbfcff 100%);\n"
        "      padding: 8px;\n"
        "      display: flex;\n"
        "      flex-direction: column;\n"
        "      gap: 6px;\n"
        "      min-height: 360px;\n"
        "      height: 100%;\n"
        "    }\n"
        "    .result-cover-wrap {\n"
        "      width: 88%;\n"
        "      margin: 0 auto;\n"
        "      aspect-ratio: 3 / 4;\n"
        "      border-radius: 10px;\n"
        "      max-height: 240px;\n"
        "      overflow: hidden;\n"
        "      background: #edf1f8;\n"
        "      border: 1px solid #dce4f2;\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: center;\n"
        "    }\n"
        "    .result-cover {\n"
        "      width: 100%;\n"
        "      height: 100%;\n"
        "      object-fit: cover;\n"
        "      display: block;\n"
        "    }\n"
        "    .result-cover-empty {\n"
        "      color: #94a3b8;\n"
        "      font-size: 12px;\n"
        "      letter-spacing: 0.2px;\n"
        "    }\n"
        "    .result-title {\n"
        "      font-size: 14px;\n"
        "      font-weight: 800;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 2);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 2;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .link { color: #0f6fff; text-decoration: none; font-size: 12px; }\n"
        "    .result-link {\n"
        "      word-break: break-all;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 2);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 2;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .result-latest {\n"
        "      white-space: nowrap;\n"
        "      overflow: hidden;\n"
        "      text-overflow: ellipsis;\n"
        "      min-height: 1.4em;\n"
        "    }\n"
        "    .link:hover { text-decoration: underline; }\n"
        "    .actions { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; margin-top: auto; }\n"
        "    .actions form { margin: 0; min-width: 0; }\n"
        "    .actions .btn { width: 100%; }\n"
        "    .job-actions {\n"
        "      grid-template-columns: repeat(3, minmax(0, 1fr));\n"
        "      gap: 6px;\n"
        "      margin-top: 10px;\n"
        "    }\n"
        "    .job-meta { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 10px; color: var(--muted); }\n"
        "    .badge {\n"
        "      display: inline-block;\n"
        "      padding: 3px 10px;\n"
        "      border-radius: 999px;\n"
        "      border: 1px solid #cad6ef;\n"
        "      font-size: 12px;\n"
        "      color: #1555c0;\n"
        "      background: #edf4ff;\n"
        "    }\n"
        "    .progress {\n"
        "      width: 100%;\n"
        "      height: 10px;\n"
        "      border-radius: 999px;\n"
        "      background: #e6edf8;\n"
        "      overflow: hidden;\n"
        "      margin: 6px 0 12px;\n"
        "    }\n"
        "    .bar {\n"
        "      height: 100%;\n"
        "      width: 0%;\n"
        "      background: linear-gradient(90deg, var(--primary), var(--accent));\n"
        "      transition: width 0.3s ease;\n"
        "    }\n"
        "    .log-box {\n"
        "      height: 260px;\n"
        "      border-radius: 12px;\n"
        "      border: 1px solid #dce4f2;\n"
        "      background: #f7f9fd;\n"
        "      color: #1f2937;\n"
        "      padding: 10px;\n"
        "      overflow: auto;\n"
        "      font-family: Consolas, \"Courier New\", monospace;\n"
        "      font-size: 12px;\n"
        "      white-space: pre-wrap;\n"
        "      line-height: 1.45;\n"
        "    }\n"
        "    .book-card {\n"
        "      padding: 8px;\n"
        "      border: 1px solid #dbe3f0;\n"
        "      border-radius: 14px;\n"
        "      background: linear-gradient(180deg, #ffffff 0%, #fbfcff 100%);\n"
        "      display: flex;\n"
        "      flex-direction: column;\n"
        "      gap: 6px;\n"
        "      height: 100%;\n"
        "      min-height: 360px;\n"
        "    }\n"
        "    .bookshelf-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));\n"
        "      gap: 10px;\n"
        "      align-items: stretch;\n"
        "    }\n"
        "    .book-card .result-cover-wrap {\n"
        "      width: 88%;\n"
        "      margin: 0 auto 6px;\n"
        "    }\n"
        "    .book-title {\n"
        "      margin: 0;\n"
        "      font-size: 14px;\n"
        "      line-height: 1.35;\n"
        "      min-height: calc(1.35em * 2);\n"
        "      display: -webkit-box;\n"
        "      -webkit-line-clamp: 2;\n"
        "      -webkit-box-orient: vertical;\n"
        "      overflow: hidden;\n"
        "    }\n"
        "    .book-meta-list { display: flex; flex-direction: column; gap: 4px; }\n"
        "    .book-meta { font-size: 12px; color: var(--muted); margin: 0; line-height: 1.4; }\n"
        "    .book-meta.clamp-1 { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n"
        "    .book-actions {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(3, minmax(0, 1fr));\n"
        "      align-items: center;\n"
        "      gap: 4px;\n"
        "      margin-top: auto;\n"
        "    }\n"
        "    .book-actions form {\n"
        "      margin: 0;\n"
        "      min-width: 0;\n"
        "      width: 100%;\n"
        "    }\n"
        "    .book-actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 72px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    .book-actions .btn-icon {\n"
        "      width: 12px;\n"
        "      height: 12px;\n"
        "    }\n"
        "    .follow-page .follow-toolbar {\n"
        "      display: flex;\n"
        "      justify-content: space-between;\n"
        "      align-items: flex-start;\n"
        "      gap: 8px;\n"
        "      flex-wrap: wrap;\n"
        "      margin: 8px 0 12px;\n"
        "    }\n"
        "    .follow-page .follow-toolbar-left,\n"
        "    .follow-page .follow-toolbar-right {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      gap: 6px;\n"
        "      flex-wrap: wrap;\n"
        "    }\n"
        "    .follow-page .book-actions {\n"
        "      grid-template-columns: repeat(2, minmax(0, 1fr));\n"
        "      gap: 6px;\n"
        "    }\n"
        "    .follow-page .book-actions .btn {\n"
        "      min-width: 0;\n"
        "      font-size: 12px;\n"
        "      padding: 6px;\n"
        "    }\n"
        "    .follow-page .book-actions .btn-text {\n"
        "      max-width: 56px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-grid,\n"
        "    html[data-view-mode='list'] .bookshelf-grid {\n"
        "      grid-template-columns: 1fr;\n"
        "      gap: 10px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card {\n"
        "      display: grid;\n"
        "      grid-template-columns: 78px minmax(0, 1fr) auto;\n"
        "      grid-template-areas:\n"
        "        'cover title actions'\n"
        "        'cover provider actions'\n"
        "        'cover link actions'\n"
        "        'cover latest actions';\n"
        "      column-gap: 12px;\n"
        "      row-gap: 3px;\n"
        "      min-height: 0;\n"
        "      align-items: start;\n"
        "      padding: 10px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-cover-wrap {\n"
        "      grid-area: cover;\n"
        "      width: 78px;\n"
        "      max-height: 104px;\n"
        "      margin: 0;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-title {\n"
        "      grid-area: title;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 2;\n"
        "      font-size: 14px;\n"
        "      margin-top: 1px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > div:not(.result-cover-wrap):not(.result-title):not(.result-link):not(.result-latest):not(.actions) {\n"
        "      grid-area: provider;\n"
        "      margin-top: 1px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-link {\n"
        "      grid-area: link;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 1;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .result-latest {\n"
        "      grid-area: latest;\n"
        "      min-height: 0;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .actions {\n"
        "      grid-area: actions;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: 1fr;\n"
        "      min-width: 88px;\n"
        "      justify-content: center;\n"
        "      align-content: center;\n"
        "      gap: 5px;\n"
        "      align-self: center;\n"
        "    }\n"
        "    html[data-view-mode='list'] .result-card > .actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 88px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card {\n"
        "      display: grid;\n"
        "      grid-template-columns: 78px minmax(0, 1fr) auto;\n"
        "      grid-template-areas:\n"
        "        'cover select actions'\n"
        "        'cover title actions'\n"
        "        'cover group actions'\n"
        "        'cover metas actions';\n"
        "      column-gap: 12px;\n"
        "      row-gap: 3px;\n"
        "      min-height: 0;\n"
        "      align-items: start;\n"
        "      padding: 10px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .result-cover-wrap {\n"
        "      grid-area: cover;\n"
        "      width: 78px;\n"
        "      max-height: 104px;\n"
        "      margin: 0;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > label {\n"
        "      grid-area: select;\n"
        "      margin: 0;\n"
        "      gap: 6px;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-title {\n"
        "      grid-area: title;\n"
        "      margin: 0;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 2;\n"
        "      font-size: 14px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-meta {\n"
        "      grid-area: group;\n"
        "      margin: 0;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-meta-list {\n"
        "      grid-area: metas;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(2, minmax(0, 1fr));\n"
        "      column-gap: 10px;\n"
        "      row-gap: 2px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-actions {\n"
        "      grid-area: actions;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: 1fr;\n"
        "      min-width: 88px;\n"
        "      justify-content: center;\n"
        "      align-content: center;\n"
        "      gap: 5px;\n"
        "      align-self: center;\n"
        "    }\n"
        "    html[data-view-mode='list'] .book-card > .book-actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 88px;\n"
        "      min-height: 32px;\n"
        "      padding: 6px 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card {\n"
        "      grid-template-columns: 78px minmax(0, 1fr) minmax(132px, 34%);\n"
        "      grid-template-areas:\n"
        "        'cover select actions'\n"
        "        'cover title actions'\n"
        "        'cover metas actions';\n"
        "      column-gap: 12px;\n"
        "      row-gap: 4px;\n"
        "      padding: 10px;\n"
        "      min-height: 0;\n"
        "      align-items: start;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .result-cover-wrap {\n"
        "      grid-area: cover;\n"
        "      width: 78px;\n"
        "      max-height: 104px;\n"
        "      margin: 0;\n"
        "      border-radius: 8px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > label {\n"
        "      grid-area: select;\n"
        "      margin: 0;\n"
        "      gap: 6px;\n"
        "      font-size: 12px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-title {\n"
        "      grid-area: title;\n"
        "      margin: 0;\n"
        "      min-height: 0;\n"
        "      -webkit-line-clamp: 2;\n"
        "      font-size: 14px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-meta-list {\n"
        "      grid-area: metas;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: 1fr;\n"
        "      row-gap: 2px;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions {\n"
        "      grid-area: actions;\n"
        "      margin: 0;\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(2, minmax(0, 1fr));\n"
        "      min-width: 132px;\n"
        "      justify-content: center;\n"
        "      align-content: center;\n"
        "      gap: 4px;\n"
        "      align-self: center;\n"
        "    }\n"
        "    html[data-view-mode='list'] .follow-page .book-card > .book-actions .btn {\n"
        "      width: 100%;\n"
        "      min-width: 0;\n"
        "      min-height: 32px;\n"
        "      padding: 6px;\n"
        "    }\n"
        "    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 10px; }\n"
        "    .stat-card {\n"
        "      border: 1px solid #dbe3f0;\n"
        "      background: #fff;\n"
        "      border-radius: 12px;\n"
        "      padding: 10px;\n"
        "    }\n"
        "    .stat-label { color: var(--muted); font-size: 12px; margin-bottom: 4px; }\n"
        "    .stat-value { font-size: 18px; font-weight: 800; }\n"
        "    .settings-grid {\n"
        "      display: grid;\n"
        "      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));\n"
        "      gap: 10px;\n"
        "    }\n"
        "    .settings-section {\n"
        "      border: 1px solid #dbe3f0;\n"
        "      border-radius: 12px;\n"
        "      padding: 12px;\n"
        "      margin-bottom: 12px;\n"
        "      background: #f8fafc;\n"
        "    }\n"
        "    .settings-title {\n"
        "      margin: 0 0 8px;\n"
        "      font-size: 15px;\n"
        "      font-weight: 700;\n"
        "    }\n"
        "    .site-badge {\n"
        "      display: inline-flex;\n"
        "      align-items: center;\n"
        "      gap: 6px;\n"
        "      font-size: 12px;\n"
        "      line-height: 1;\n"
        "      padding: 4px 8px;\n"
        "      border-radius: 999px;\n"
        "      border: 1px solid #d5deee;\n"
        "      background: #f7faff;\n"
        "      color: #1e3a8a;\n"
        "    }\n"
        "    .site-badge.toonily { color: #a16207; border-color: #f3ddb0; background: #fff7e7; }\n"
        "    .site-badge.jmcomic { color: #0f766e; border-color: #b5eee6; background: #ecfdf9; }\n"
        "    .site-icon { width: 14px; height: 14px; display: inline-block; }\n"
        "    .pager {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: space-between;\n"
        "      gap: 10px;\n"
        "      flex-wrap: wrap;\n"
        "      margin-bottom: 10px;\n"
        "    }\n"
        "    .pager .subtle { margin: 0; }\n"
        "    .pager-form {\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      gap: 8px;\n"
        "      flex-wrap: wrap;\n"
        "      margin: 0;\n"
        "    }\n"
        "    .pager-links { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }\n"
        "    label { display: block; font-size: 13px; margin-bottom: 5px; color: #c5d7ea; }\n"
        "    html[data-theme='dark'] {\n"
        "      color-scheme: dark;\n"
        "      --text: #e3ebf7;\n"
        "      --muted: #9cb0cc;\n"
        "      --bg-dark: #0b1220;\n"
        "      --bg-darker: #0f1a2d;\n"
        "      --panel: #101a2d;\n"
        "      --panel-border: #24364f;\n"
        "      --shadow: 0 12px 28px rgba(0, 0, 0, 0.35);\n"
        "      --primary: #4e9dff;\n"
        "      --primary-light: #3f87df;\n"
        "      --secondary: #13b9a4;\n"
        "      --accent: #13b9a4;\n"
        "    }\n"
        "    html[data-theme='dark'] body {\n"
        "      background: radial-gradient(1200px 520px at 8% -10%, #1a2a45 0%, transparent 60%), radial-gradient(1000px 460px at 95% -12%, #10343a 0%, transparent 62%), linear-gradient(180deg, #0b1220 0%, #0f1a2d 100%);\n"
        "    }\n"
        "    html[data-theme='dark'] .top {\n"
        "      background: rgba(16, 26, 45, 0.92);\n"
        "      border-color: #2a3d59;\n"
        "    }\n"
        "    html[data-theme='dark'] .logo { color: #f0f5ff; }\n"
        "    html[data-theme='dark'] .nav-link {\n"
        "      color: #c0cee3;\n"
        "    }\n"
        "    html[data-theme='dark'] .nav-link:hover {\n"
        "      color: #f0f5ff;\n"
        "      border-color: #324865;\n"
        "      background: #18263d;\n"
        "    }\n"
        "    html[data-theme='dark'] .input,\n"
        "    html[data-theme='dark'] .select {\n"
        "      background: #0f1a2b;\n"
        "      color: #e3ebf7;\n"
        "      border-color: #304463;\n"
        "    }\n"
        "    html[data-theme='dark'] .input::placeholder { color: #7f92ad; }\n"
        "    html[data-theme='dark'] .btn.ghost {\n"
        "      color: #d1deef;\n"
        "      border-color: #39506e;\n"
        "      background: #142239;\n"
        "    }\n"
        "    html[data-theme='dark'] .result-card,\n"
        "    html[data-theme='dark'] .book-card {\n"
        "      border-color: #2a3f5e;\n"
        "      background: linear-gradient(180deg, #111e32 0%, #0f1a2d 100%);\n"
        "    }\n"
        "    html[data-theme='dark'] .result-cover-wrap {\n"
        "      background: #17263d;\n"
        "      border-color: #314969;\n"
        "    }\n"
        "    html[data-theme='dark'] .result-cover-empty { color: #90a3c0; }\n"
        "    html[data-theme='dark'] .badge {\n"
        "      border-color: #334b6b;\n"
        "      color: #a9cbff;\n"
        "      background: #152841;\n"
        "    }\n"
        "    html[data-theme='dark'] .progress { background: #1b2c45; }\n"
        "    html[data-theme='dark'] .log-box {\n"
        "      border-color: #314969;\n"
        "      background: #0d1727;\n"
        "      color: #d6e1f2;\n"
        "    }\n"
        "    html[data-theme='dark'] .stat-card {\n"
        "      border-color: #2a3f5e;\n"
        "      background: #101c2f;\n"
        "    }\n"
        "    html[data-theme='dark'] .settings-section {\n"
        "      border-color: #304463;\n"
        "      background: #0f1a2d;\n"
        "    }\n"
        "    html[data-theme='dark'] .site-badge {\n"
        "      border-color: #324866;\n"
        "      background: #14233a;\n"
        "      color: #b6cef0;\n"
        "    }\n"
        "    html[data-theme='dark'] .site-badge.toonily {\n"
        "      color: #f8d17e;\n"
        "      border-color: #7a6431;\n"
        "      background: #2f2816;\n"
        "    }\n"
        "    html[data-theme='dark'] .site-badge.jmcomic {\n"
        "      color: #6ee0cc;\n"
        "      border-color: #2f6e67;\n"
        "      background: #112a29;\n"
        "    }\n"
        "    @media (max-width: 980px) {\n"
        "      .split-grid { grid-template-columns: 1fr; }\n"
        "      .search-form { grid-template-columns: 1fr 1fr; }\n"
        "    }\n"
        "    @media (max-width: 780px) {\n"
        "      .shell { width: calc(100% - 14px); margin-top: 10px; }\n"
        "      .top { flex-direction: column; align-items: flex-start; }\n"
        "      .top-actions { width: 100%; justify-content: space-between; }\n"
        "      .logo { font-size: 24px; }\n"
        "      .search-form { grid-template-columns: 1fr; }\n"
        "      .result-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; }\n"
        "      .bookshelf-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; }\n"
        "      .result-card,\n"
        "      .book-card { min-height: 330px; }\n"
        "      .actions { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n"
        "      .job-actions { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n"
        "      .follow-page .follow-toolbar { flex-direction: column; align-items: stretch; }\n"
        "      .follow-page .follow-toolbar-left,\n"
        "      .follow-page .follow-toolbar-right { width: 100%; }\n"
        "      .follow-page .book-actions { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n"
        "      .settings-grid { grid-template-columns: 1fr; }\n"
        "    }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        "  <div class=\"shell\">\n"
        "    <div class=\"top\">\n"
        "      <div class=\"logo\">漫画下载</div>\n"
        "      <div class=\"top-actions\">\n"
        f"        <nav class=\"nav\">{nav_html}</nav>\n"
        "        <button class=\"btn ghost icon-btn theme-toggle\" id=\"theme-toggle\" type=\"button\" title=\"切换主题\" aria-label=\"切换主题\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M12 3a1 1 0 0 1 1 1v1a1 1 0 1 1-2 0V4a1 1 0 0 1 1-1zm0 15a4 4 0 1 1 0-8 4 4 0 0 1 0 8zm8-5a1 1 0 0 1 1 1 1 1 0 0 1-1 1h-1a1 1 0 1 1 0-2zm-14 0a1 1 0 1 1 0 2H5a1 1 0 1 1 0-2zm10.66-6.66a1 1 0 0 1 1.41 0l.7.7a1 1 0 1 1-1.41 1.41l-.7-.7a1 1 0 0 1 0-1.41zM7.34 16.66a1 1 0 0 1 1.41 0l.7.7a1 1 0 1 1-1.41 1.41l-.7-.7a1 1 0 0 1 0-1.41zm11.43 0a1 1 0 0 1 0 1.41l-.7.7a1 1 0 1 1-1.41-1.41l.7-.7a1 1 0 0 1 1.41 0zM8.75 6.34a1 1 0 0 1 0 1.41l-.7.7A1 1 0 0 1 6.64 7.04l.7-.7a1 1 0 0 1 1.41 0z\" fill=\"currentColor\"/>"
        "</svg>"
        "<span class=\"btn-text\">主题</span>"
        "</button>\n"
        "      </div>\n"
        "    </div>\n"
        f"    {body}\n"
        "  </div>\n"
        f"{theme_toggle_script}\n"
        f"{script}\n"
        "</body>\n"
        "</html>\n"
    )


def status_text(status: str) -> str:
    mapping = {
        "queued": "排队中",
        "running": "进行中",
        "paused": "已暂停",
        "cancelling": "取消中",
        "cancelled": "已取消",
        "failed": "失败",
        "completed": "完成",
    }
    return mapping.get(status, status)


def render_message(msg: str) -> str:
    if not msg:
        return ""
    return (
        "<div class=\"msg\" id=\"toast-msg\" role=\"status\" aria-live=\"polite\">"
        f"<span class=\"msg-text\">{escape(msg)}</span>"
        "<button type=\"button\" class=\"msg-close\" aria-label=\"关闭提示\" "
        "onclick=\"(function(btn){const el=btn.closest('.msg');if(!el)return;el.classList.remove('show');el.classList.add('hide');setTimeout(function(){if(el.isConnected){el.remove();}},220);})(this)\">×</button>"
        "</div>"
        "<script>(function(){"
        "const el=document.getElementById('toast-msg');"
        "if(!el)return;"
        "requestAnimationFrame(function(){el.classList.add('show');});"
        "setTimeout(function(){"
        "if(!el.isConnected)return;"
        "el.classList.remove('show');"
        "el.classList.add('hide');"
        "setTimeout(function(){if(el.isConnected){el.remove();}},220);"
        "},2800);"
        "})();</script>"
    )


def render_job_panel(job: dict[str, Any], *, heading: str, full_page: bool = False) -> tuple[str, str]:
    chapter_pct = chapter_percent(job["done_chapters"], job["total_chapters"])
    image_pct = chapter_percent(job["saved_images"], job["total_images"])
    logs = "\n".join(job["logs"])
    retry_file = job.get("retry_file", "")

    progress_jump = (
        ""
        if full_page
        else (
            f"<a class=\"btn ghost icon-btn\" href=\"/progress?job={escape(job['id'])}\" "
            "title=\"打开完整进度界面\" aria-label=\"打开完整进度界面\">"
            "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
            "<path d=\"M4 12s3-6 8-6 8 6 8 6-3 6-8 6-8-6-8-6z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"/>"
            "<circle cx=\"12\" cy=\"12\" r=\"2.5\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\"/>"
            "</svg>"
            "<span class=\"btn-text\">打开完整进度界面</span>"
            "</a>"
        )
    )
    back_home = (
        "<a class=\"btn ghost icon-btn\" href=\"/dashboard\" title=\"返回主页\" aria-label=\"返回主页\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M4 11.5 12 5l8 6.5V20h-5v-5H9v5H4z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linejoin=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">返回主页</span>"
        "</a>"
        if full_page
        else ""
    )
    retry_text = f"失败重试清单：{retry_file}" if retry_file else ""
    provider_text = provider_name(str(job.get("provider_id") or DEFAULT_PROVIDER_ID))

    stats_html = ""
    if full_page:
        stats_html = (
            "<div class=\"stat-grid\">"
            "<div class=\"stat-card\"><div class=\"stat-label\">任务状态</div>"
            f"<div class=\"stat-value\" id=\"stat-status\">{escape(status_text(job['status']))}</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">成功章节</div>"
            f"<div class=\"stat-value\" id=\"stat-success\">{int(job.get('successful_chapters', 0))}</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">失败章节</div>"
            f"<div class=\"stat-value\" id=\"stat-failed\">{int(job.get('failed_chapters', 0))}</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">章节完成率</div>"
            f"<div class=\"stat-value\" id=\"stat-chapter-pct\">{chapter_pct}%</div></div>"
            "<div class=\"stat-card\"><div class=\"stat-label\">图片完成率</div>"
            f"<div class=\"stat-value\" id=\"stat-image-pct\">{image_pct}%</div></div>"
            "</div>"
        )

    job_html = (
        "<div class=\"panel\" id=\"job-panel\" "
        f"data-job-id=\"{escape(job['id'])}\">"
        f"<h2 class=\"title\">{escape(heading)}</h2>"
        f"{stats_html}"
        f"<div class=\"job-meta\"><span class=\"badge\" id=\"job-status\">{escape(status_text(job['status']))}</span>"
        f"<span>创建：<span id=\"job-created\">{escape(fmt_time(job['created_at']))}</span></span>"
        f"<span>开始：<span id=\"job-started\">{escape(fmt_time(job['started_at']))}</span></span>"
        f"<span>结束：<span id=\"job-finished\">{escape(fmt_time(job['finished_at']))}</span></span></div>"
        f"<div style=\"font-weight:700;margin-bottom:6px;\">{escape(job['title'])}</div>"
        f"<div class=\"subtle\">站点：{escape(provider_text)}</div>"
        f"<div class=\"subtle\" id=\"job-error\">{escape(job.get('error', ''))}</div>"
        f"<div class=\"subtle\" id=\"retry-file\">{escape(retry_text)}</div>"
        "<div style=\"margin-top:12px;\">章节进度：<span id=\"chapter-text\">"
        f"{job['done_chapters']}/{job['total_chapters']}</span></div>"
        "<div class=\"progress\"><div class=\"bar\" id=\"chapter-bar\" "
        f"style=\"width:{chapter_pct}%;\"></div></div>"
        "<div>图片进度：<span id=\"image-text\">"
        f"{job['saved_images']}/{job['total_images']}</span></div>"
        "<div class=\"progress\"><div class=\"bar\" id=\"image-bar\" "
        f"style=\"width:{image_pct}%;\"></div></div>"
        "<div class=\"actions job-actions\">"
        "<button id=\"btn-pause\" class=\"btn ghost icon-btn\" type=\"button\" title=\"暂停任务\" aria-label=\"暂停任务\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M8 6v12M16 6v12\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">暂停</span>"
        "</button>"
        "<button id=\"btn-resume\" class=\"btn secondary icon-btn\" type=\"button\" title=\"继续任务\" aria-label=\"继续任务\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M8 6l10 6-10 6z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linejoin=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">继续</span>"
        "</button>"
        "<button id=\"btn-cancel\" class=\"btn warn icon-btn\" type=\"button\" title=\"取消任务\" aria-label=\"取消任务\">"
        "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
        "<path d=\"M6 6l12 12M18 6 6 18\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>"
        "</svg>"
        "<span class=\"btn-text\">取消</span>"
        "</button>"
        f"{progress_jump}"
        f"{back_home}"
        "</div>"
        "<div style=\"margin:10px 0 6px;\" class=\"subtle\">实时日志（自动滚动）</div>"
        f"<pre id=\"log-box\" class=\"log-box\">{escape(logs)}</pre>"
        "</div>"
    )

    script = (
        "<script>\n"
        "(function(){\n"
        "  const panel = document.getElementById('job-panel');\n"
        "  if (!panel) return;\n"
        "  const jobId = panel.dataset.jobId;\n"
        "  const statusEl = document.getElementById('job-status');\n"
        "  const chapterText = document.getElementById('chapter-text');\n"
        "  const imageText = document.getElementById('image-text');\n"
        "  const chapterBar = document.getElementById('chapter-bar');\n"
        "  const imageBar = document.getElementById('image-bar');\n"
        "  const logBox = document.getElementById('log-box');\n"
        "  const errorEl = document.getElementById('job-error');\n"
        "  const retryEl = document.getElementById('retry-file');\n"
        "  const createdEl = document.getElementById('job-created');\n"
        "  const startedEl = document.getElementById('job-started');\n"
        "  const finishedEl = document.getElementById('job-finished');\n"
        "  const statStatusEl = document.getElementById('stat-status');\n"
        "  const statSuccessEl = document.getElementById('stat-success');\n"
        "  const statFailedEl = document.getElementById('stat-failed');\n"
        "  const statChapterPctEl = document.getElementById('stat-chapter-pct');\n"
        "  const statImagePctEl = document.getElementById('stat-image-pct');\n"
        "  const btnPause = document.getElementById('btn-pause');\n"
        "  const btnResume = document.getElementById('btn-resume');\n"
        "  const btnCancel = document.getElementById('btn-cancel');\n"
        "  const finalStates = new Set(['completed', 'failed', 'cancelled']);\n"
        "  let poll = null;\n"
        "  function pct(done, total){ if (!total || total <= 0) return 0; return Math.max(0, Math.min(100, Math.floor(done*100/total))); }\n"
        "  function fmt(v){ if(!v) return '-'; return String(v).replace('T',' '); }\n"
        "  function updateButtons(ctrl){\n"
        "    btnPause.disabled = !ctrl.can_pause;\n"
        "    btnResume.disabled = !ctrl.can_resume;\n"
        "    btnCancel.disabled = !ctrl.can_cancel;\n"
        "  }\n"
        "  async function control(action){\n"
        "    try {\n"
        "      await fetch('/job/' + encodeURIComponent(jobId) + '/' + action, {method:'POST'});\n"
        "      await refresh();\n"
        "    } catch (_) {}\n"
        "  }\n"
        "  btnPause.addEventListener('click', ()=>control('pause'));\n"
        "  btnResume.addEventListener('click', ()=>control('resume'));\n"
        "  btnCancel.addEventListener('click', ()=>control('cancel'));\n"
        "  async function refresh(){\n"
        "    try {\n"
        "      const res = await fetch('/job/' + encodeURIComponent(jobId) + '/state');\n"
        "      if (!res.ok) return;\n"
        "      const data = await res.json();\n"
        "      const cPct = pct(data.done_chapters, data.total_chapters);\n"
        "      const iPct = pct(data.saved_images, data.total_images);\n"
        "      statusEl.textContent = data.status_text;\n"
        "      chapterText.textContent = data.done_chapters + '/' + data.total_chapters;\n"
        "      imageText.textContent = data.saved_images + '/' + data.total_images;\n"
        "      chapterBar.style.width = cPct + '%';\n"
        "      imageBar.style.width = iPct + '%';\n"
        "      errorEl.textContent = data.error || '';\n"
        "      if (createdEl) createdEl.textContent = fmt(data.created_at);\n"
        "      if (startedEl) startedEl.textContent = fmt(data.started_at);\n"
        "      if (finishedEl) finishedEl.textContent = fmt(data.finished_at);\n"
        "      if (retryEl) retryEl.textContent = data.retry_file ? ('失败重试清单：' + data.retry_file) : '';\n"
        "      if (statStatusEl) statStatusEl.textContent = data.status_text;\n"
        "      if (statSuccessEl) statSuccessEl.textContent = String(data.successful_chapters || 0);\n"
        "      if (statFailedEl) statFailedEl.textContent = String(data.failed_chapters || 0);\n"
        "      if (statChapterPctEl) statChapterPctEl.textContent = cPct + '%';\n"
        "      if (statImagePctEl) statImagePctEl.textContent = iPct + '%';\n"
        "      const nextLogs = (data.logs || []).join('\\n');\n"
        "      if (logBox.textContent !== nextLogs){\n"
        "        logBox.textContent = nextLogs;\n"
        "        logBox.scrollTop = logBox.scrollHeight;\n"
        "      }\n"
        "      updateButtons(data.controls || {can_pause:false, can_resume:false, can_cancel:false});\n"
        "      if (finalStates.has(data.status) && poll){\n"
        "        clearInterval(poll);\n"
        "        poll = null;\n"
        "      }\n"
        "    } catch (_) {}\n"
        "  }\n"
        "  refresh();\n"
        "  poll = setInterval(refresh, 2000);\n"
        "})();\n"
        "</script>"
    )
    return job_html, script


def render_dashboard(
    state: UIState,
    msg: str,
    *,
    search_page: int,
    search_page_size: int,
) -> str:
    selected_provider = get_provider(state.last_search_provider)
    if not provider_enabled_for_state(state, selected_provider):
        for candidate in list_providers():
            if provider_enabled_for_state(state, candidate):
                state.last_search_provider = candidate.provider_id
                break

    results: Optional[dict[str, Any]] = None
    if state.last_search_results:
        total_results = len(state.last_search_results)
        page_size = max(4, min(40, int(search_page_size)))
        page_count = max(1, math.ceil(total_results / page_size))
        page = max(1, min(int(search_page), page_count))
        start = (page - 1) * page_size
        end = start + page_size
        page_results = state.last_search_results[start:end]

        def dashboard_page_url(target_page: int) -> str:
            params: dict[str, str] = {"sp": str(target_page), "sps": str(page_size)}
            return f"/dashboard?{urlencode(params)}"

        items: list[dict[str, Any]] = []
        for item in page_results:
            title = str(item.get("title", "") or "")
            url = str(item.get("url", "") or "")
            latest = str(item.get("latest", "") or "") or "-"
            cover = item.get("cover", "")
            provider_id = str(item.get("provider_id") or state.last_search_provider or DEFAULT_PROVIDER_ID)
            provider_badge = render_provider_badge(provider_id)
            cover_url = str(cover).strip()
            if cover_url.startswith("//"):
                cover_url = f"https:{cover_url}"
            elif cover_url.startswith("/"):
                cover_url = urljoin("https://toonily.com", cover_url)
            if cover_url and not cover_url.startswith(("http://", "https://")):
                cover_url = ""

            items.append(
                {
                    "title": title,
                    "url": url,
                    "latest": latest,
                    "cover_url": cover_url,
                    "provider_id": provider_id,
                    "provider_badge_html": provider_badge,
                }
            )

        results = {
            "keyword": state.last_search_query,
            "total": total_results,
            "page": page,
            "page_count": page_count,
            "page_size": page_size,
            "has_prev": page > 1,
            "has_next": page < page_count,
            "prev_url": dashboard_page_url(page - 1),
            "next_url": dashboard_page_url(page + 1),
            "page_size_options": [
                {"value": size, "selected": size == page_size} for size in (8, 12, 16, 24, 40)
            ],
            "items": items,
        }

    provider_options: list[dict[str, Any]] = []
    for provider in list_providers():
        reason = provider_disabled_reason(state, provider)
        if reason:
            if provider.enabled:
                label = f"{provider.display_name}（已停用）"
            else:
                label = f"{provider.display_name}（不可用）"
        else:
            label = provider.display_name
        provider_options.append(
            {
                "value": provider.provider_id,
                "label": label,
                "selected": provider.provider_id == state.last_search_provider,
            }
        )
    import_provider_options = [
        {
            "value": AUTO_PROVIDER_ID,
            "label": "自动识别站点（推荐）",
            "selected": True,
        }
    ] + [
        {
            "value": item["value"],
            "label": f"{item['label']}（固定）",
            "selected": False,
        }
        for item in provider_options
    ]

    search_page_size_options = [
        {"value": size, "selected": size == max(4, min(40, int(search_page_size)))}
        for size in (8, 12, 16, 24, 40)
    ]
    body = render_template(
        "dashboard.html",
        message_html=render_message(msg),
        search_query=state.last_search_query,
        provider_options=provider_options,
        import_provider_options=import_provider_options,
        search_page_size_options=search_page_size_options,
        results=results,
    )
    return render_layout(
        title="漫画下载 - 主页",
        active_nav="dashboard",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_progress(state: UIState, msg: str, selected_job_id: str) -> str:
    progress_content_html = ""
    script = ""

    job = state.jobs.get(selected_job_id) if selected_job_id else None
    if job is None and state.current_job_id:
        job = state.jobs.get(state.current_job_id)

    if job is None:
        progress_content_html = (
            "<div class=\"panel\">"
            "<h2 class=\"title\">任务进度</h2>"
            "<div class=\"subtle\">当前没有活跃任务。请前往主页搜索漫画并创建下载任务。</div>"
            "<div style=\"margin-top:10px;\">"
            "<a class=\"btn icon-btn\" href=\"/dashboard\" title=\"去主页创建任务\" aria-label=\"去主页创建任务\">"
            "<svg class=\"btn-icon\" viewBox=\"0 0 24 24\" aria-hidden=\"true\">"
            "<path d=\"M4 11.5 12 5l8 6.5V20h-5v-5H9v5H4z\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linejoin=\"round\"/>"
            "</svg>"
            "<span class=\"btn-text\">去主页创建任务</span>"
            "</a>"
            "</div>"
            "</div>"
        )
    else:
        panel_html, script = render_job_panel(job, heading="任务进度", full_page=True)
        progress_content_html = panel_html

    body = render_template(
        "progress.html",
        message_html=render_message(msg),
        progress_content_html=progress_content_html,
    )

    return render_layout(
        title="漫画下载 - 进度",
        active_nav="progress",
        body=body,
        script=script,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_queue(state: UIState, msg: str) -> str:
    queued_rows: list[dict[str, Any]] = []
    queued_jobs = queued_jobs_sorted(state)
    for idx, job in enumerate(queued_jobs, start=1):
        pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
        queued_rows.append(
            {
                "id": str(job.get("id") or ""),
                "index": idx,
                "title": str(job.get("title") or ""),
                "provider_badge_html": render_provider_badge(pid),
                "created_at": fmt_time(str(job.get("created_at") or "")),
                "can_move_up": idx > 1,
                "can_move_down": idx < len(queued_jobs),
            }
        )

    running_rows: list[dict[str, Any]] = []
    for job in state.jobs.values():
        status = str(job.get("status") or "")
        if status not in {"running", "paused", "cancelling"}:
            continue
        pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
        running_rows.append(
            {
                "id": str(job.get("id") or ""),
                "title": str(job.get("title") or ""),
                "provider_badge_html": render_provider_badge(pid),
                "status": status_text(status),
                "done_chapters": int(job.get("done_chapters", 0)),
                "total_chapters": int(job.get("total_chapters", 0)),
                "created_at": fmt_time(str(job.get("created_at") or "")),
                "started_at": fmt_time(str(job.get("started_at") or "")),
            }
        )
    running_rows.sort(key=lambda row: row["created_at"], reverse=True)

    failed_rows: list[dict[str, Any]] = []
    for job in state.jobs.values():
        if str(job.get("status") or "") != "failed":
            continue
        pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
        failed_rows.append(
            {
                "id": str(job.get("id") or ""),
                "title": str(job.get("title") or ""),
                "provider_badge_html": render_provider_badge(pid),
                "error": str(job.get("error") or ""),
                "finished_at": fmt_time(str(job.get("finished_at") or "")),
            }
        )
    failed_rows.sort(key=lambda row: row["finished_at"], reverse=True)

    body = render_template(
        "queue.html",
        message_html=render_message(msg),
        summary={
            "queued": len(queued_rows),
            "running": len(running_rows),
            "failed": len(failed_rows),
            "total": len(state.jobs),
        },
        queued_rows=queued_rows,
        running_rows=running_rows,
        failed_rows=failed_rows,
    )
    return render_layout(
        title="漫画下载 - 队列",
        active_nav="queue",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def normalize_cover_url(value: Any) -> str:
    cover_url = str(value or "").strip()
    if cover_url.startswith("//"):
        cover_url = f"https:{cover_url}"
    elif cover_url.startswith("/"):
        cover_url = urljoin("https://toonily.com", cover_url)
    if cover_url and not cover_url.startswith(("http://", "https://")):
        return ""
    return cover_url


def build_book_card_payload(book: dict[str, Any]) -> dict[str, Any]:
    follow_enabled = bool(book.get("follow_enabled", True))
    follow_text = "开启" if follow_enabled else "关闭"
    pending = int(book.get("pending_update_count", 0))
    provider_id = str(book.get("provider_id") or DEFAULT_PROVIDER_ID)
    provider_badge = render_provider_badge(provider_id)
    group_name = str(book.get("group") or "").strip()
    return {
        "id": str(book["id"]),
        "title": str(book.get("title") or "未命名漫画"),
        "cover_url": normalize_cover_url(book.get("cover")),
        "group": group_name,
        "provider_badge_html": provider_badge,
        "downloaded_text": (
            f"已下载：{book.get('last_downloaded_chapter_title') or '-'} "
            f"/ #{format_chapter_number(book.get('last_downloaded_chapter_number'))}"
        ),
        "latest_text": (
            f"最新：{book.get('latest_site_chapter_title') or '-'} "
            f"/ #{format_chapter_number(book.get('latest_site_chapter_number'))}"
        ),
        "summary_text": (
            f"待更新：{pending} | 追更：{follow_text} | "
            f"分组：{group_name or '未分组'} | 检查：{fmt_time(book.get('last_checked_at', ''))}"
        ),
        "follow_enabled": follow_enabled,
    }


def render_bookshelf(
    state: UIState,
    msg: str,
    *,
    bookshelf_page: int,
    bookshelf_page_size: int,
    bookshelf_group: str = "",
) -> str:
    jm_provider = get_provider("jmcomic")
    has_jm_login = bool(state.jm_username and state.jm_password)
    jm_reason = provider_disabled_reason(state, jm_provider)
    jm_enabled_for_use = not jm_reason

    all_books = state.list_books()
    selected_group = str(bookshelf_group or "").strip()
    grouped_counts: dict[str, int] = {}
    for item in all_books:
        key = str(item.get("group") or "").strip()
        grouped_counts[key] = grouped_counts.get(key, 0) + 1

    if selected_group:
        filtered_books = [book for book in all_books if str(book.get("group") or "").strip() == selected_group]
    else:
        filtered_books = all_books

    total_books = len(filtered_books)
    page_size = max(6, min(60, int(bookshelf_page_size)))
    page_count = max(1, math.ceil(total_books / page_size)) if total_books else 1
    page = max(1, min(int(bookshelf_page), page_count))
    start = (page - 1) * page_size
    end = start + page_size
    page_books = filtered_books[start:end]

    def bookshelf_page_url(target_page: int) -> str:
        params: dict[str, str] = {"bp": str(target_page), "bps": str(page_size)}
        if selected_group:
            params["bg"] = selected_group
        return f"/bookshelf?{urlencode(params)}"

    books = [build_book_card_payload(book) for book in page_books]
    for book in books:
        book["follow_button_text"] = "关闭追更" if book["follow_enabled"] else "开启追更"

    body = render_template(
        "bookshelf.html",
        message_html=render_message(msg),
        sync={
            "jm_enabled": jm_enabled_for_use,
            "has_jm_login": has_jm_login,
            "jm_username": state.jm_username,
            "manual_logged_in": state.jm_manual_logged_in,
            "manual_login_user": state.jm_manual_login_user,
            "jm_disabled_reason": jm_reason or "未知原因",
        },
        total_books=total_books,
        all_books_count=len(all_books),
        follow_count=sum(1 for item in all_books if bool(item.get("follow_enabled", True))),
        group_filter={
            "value": selected_group,
            "options": [
                {
                    "value": key,
                    "label": (key or "未分组"),
                    "count": grouped_counts[key],
                    "selected": key == selected_group,
                }
                for key in sorted(grouped_counts.keys(), key=lambda k: (k == "", k.lower()))
            ],
        },
        pager={
            "page": page,
            "page_count": page_count,
            "page_size": page_size,
            "has_prev": page > 1,
            "has_next": page < page_count,
            "prev_url": bookshelf_page_url(page - 1),
            "next_url": bookshelf_page_url(page + 1),
            "page_size_options": [
                {"value": size, "selected": size == page_size} for size in (12, 24, 36, 60)
            ],
        },
        books=books,
    )
    return render_layout(
        title="漫画下载 - 书架",
        active_nav="bookshelf",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_follow(
    state: UIState,
    msg: str,
    *,
    follow_page: int,
    follow_page_size: int,
) -> str:
    follow_books = [book for book in state.list_books() if bool(book.get("follow_enabled", True))]
    total_books = len(follow_books)
    pending_total = sum(max(0, int(book.get("pending_update_count", 0))) for book in follow_books)
    page_size = max(6, min(60, int(follow_page_size)))
    page_count = max(1, math.ceil(total_books / page_size)) if total_books else 1
    page = max(1, min(int(follow_page), page_count))
    start = (page - 1) * page_size
    end = start + page_size
    page_books = follow_books[start:end]

    def follow_page_url(target_page: int) -> str:
        return f"/follow?{urlencode({'fp': str(target_page), 'fps': str(page_size)})}"

    books = [build_book_card_payload(book) for book in page_books]

    body = render_template(
        "follow.html",
        message_html=render_message(msg),
        total_books=total_books,
        pending_total=pending_total,
        pager={
            "page": page,
            "page_count": page_count,
            "page_size": page_size,
            "has_prev": page > 1,
            "has_next": page < page_count,
            "prev_url": follow_page_url(page - 1),
            "next_url": follow_page_url(page + 1),
            "page_size_options": [
                {"value": size, "selected": size == page_size} for size in (12, 24, 36, 60)
            ],
        },
        books=books,
    )
    return render_layout(
        title="漫画下载 - 追更",
        active_nav="follow",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_health(state: UIState, msg: str) -> str:
    rows: list[dict[str, Any]] = []
    for provider in list_providers():
        row = state.ensure_health_entry(provider.provider_id)
        reasons = dict(row.get("failure_reasons") or {})
        top_reasons = sorted(reasons.items(), key=lambda item: item[1], reverse=True)[:6]
        rows.append(
            {
                "provider_id": provider.provider_id,
                "provider_name": provider.display_name,
                "available": bool(row.get("available", provider.enabled)),
                "last_check_at": fmt_time(str(row.get("last_check_at", ""))),
                "last_error": str(row.get("last_error", "")),
                "avg_speed_kbps": float(row.get("avg_speed_kbps", 0.0)),
                "total_jobs": int(row.get("total_jobs", 0)),
                "success_jobs": int(row.get("success_jobs", 0)),
                "failed_jobs": int(row.get("failed_jobs", 0)),
                "failure_reasons": top_reasons,
            }
        )
    recent_summary = summarize_recent_history(state, hours=24)
    recent_items = []
    for item in recent_summary["latest_items"]:
        row = dict(item)
        row["provider_name"] = provider_name(str(item.get("provider_id") or DEFAULT_PROVIDER_ID))
        row["finished_at_text"] = fmt_time(str(item.get("finished_at") or ""))
        recent_items.append(row)

    body = render_template(
        "health.html",
        message_html=render_message(msg),
        scheduler={
            "enabled": state.scheduler_enabled,
            "interval_minutes": state.scheduler_interval_minutes,
            "auto_download": state.scheduler_auto_download,
            "last_run_at": fmt_time(state.scheduler_last_run_at),
            "next_run_at": fmt_time(state.scheduler_next_run_at),
            "running": state._scheduler_running,
        },
        rows=rows,
        recent_summary=recent_summary,
        recent_items=recent_items,
    )
    return render_layout(
        title="漫画下载 - 监控",
        active_nav="health",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def render_settings(state: UIState, msg: str) -> str:
    jm_provider = get_provider("jmcomic")
    provider_switches: list[dict[str, Any]] = []
    for provider in list_providers():
        reason = provider_disabled_reason(state, provider)
        proxy_row = state.provider_proxy_row(provider.provider_id)
        effective_proxy = state.get_provider_proxy_url(provider.provider_id)
        provider_switches.append(
            {
                "id": provider.provider_id,
                "name": provider.display_name,
                "checked": state.is_provider_enabled(provider.provider_id),
                "runtime_available": provider.enabled,
                "reason": reason,
                "proxy_enabled": bool(proxy_row.get("enabled", False)),
                "proxy_url": str(proxy_row.get("url", "") or ""),
                "effective_proxy_label": mask_proxy_url(effective_proxy),
            }
        )

    body = render_template(
        "settings.html",
        message_html=render_message(msg),
        settings={
            "output_dir": str(state.output_dir),
            "chapter_concurrency": state.chapter_concurrency,
            "image_concurrency": state.image_concurrency,
            "retries": state.retries,
            "timeout": state.timeout,
            "max_parallel_jobs": state.max_parallel_jobs,
            "retry_base_delay_seconds": state.retry_base_delay_seconds,
            "retry_recoverable_only": state.retry_recoverable_only,
            "enable_chapter_dedupe": state.enable_chapter_dedupe,
            "image_output_format": state.image_output_format,
            "image_quality": state.image_quality,
            "keep_original_images": state.keep_original_images,
            "auto_archive_format": state.auto_archive_format,
            "write_metadata_sidecar": state.write_metadata_sidecar,
            "manga_dir_template": state.manga_dir_template,
            "chapter_dir_template": state.chapter_dir_template,
            "page_name_template": state.page_name_template,
            "bandwidth_day_kbps": state.bandwidth_day_kbps,
            "bandwidth_night_kbps": state.bandwidth_night_kbps,
            "night_start_hour": state.night_start_hour,
            "night_end_hour": state.night_end_hour,
            "scheduler_enabled": state.scheduler_enabled,
            "scheduler_interval_minutes": state.scheduler_interval_minutes,
            "scheduler_auto_download": state.scheduler_auto_download,
            "scheduler_last_run_at": fmt_time(state.scheduler_last_run_at),
            "scheduler_next_run_at": fmt_time(state.scheduler_next_run_at),
            "redis_host": state.redis_host,
            "redis_port": state.redis_port,
            "redis_db": state.redis_db,
            "redis_username": state.redis_username,
            "redis_password": state.redis_password,
            "cache_ttl_seconds": state.cache_ttl_seconds,
            "cache_enabled": state.cache_enabled,
            "jm_username": state.jm_username,
            "jm_password": state.jm_password,
            "webhook_enabled": state.webhook_enabled,
            "webhook_url": state.webhook_url,
            "webhook_token": state.webhook_token,
            "webhook_event_completed": state.webhook_event_completed,
            "webhook_event_failed": state.webhook_event_failed,
            "webhook_event_cancelled": state.webhook_event_cancelled,
            "webhook_timeout_seconds": state.webhook_timeout_seconds,
            "compact_mode_enabled": state.compact_mode_enabled,
            "manga_view_mode": state.manga_view_mode,
            "jm_enabled": provider_enabled_for_state(state, jm_provider),
            "jm_disabled_reason": provider_disabled_reason(state, jm_provider) or "未知原因",
            "provider_switches": provider_switches,
            "default_proxy_url": state.default_proxy_url,
        },
    )
    return render_layout(
        title="漫画下载 - 设置",
        active_nav="settings",
        body=body,
        compact_mode=state.compact_mode_enabled,
        manga_view_mode=state.manga_view_mode,
    )


def job_controls(job: dict[str, Any]) -> dict[str, bool]:
    status = job.get("status", "")
    return {
        "can_pause": status == "running",
        "can_resume": status == "paused",
        "can_cancel": status in {"queued", "running", "paused", "cancelling"},
    }


def serialize_job(job: dict[str, Any]) -> dict[str, Any]:
    pid = str(job.get("provider_id") or DEFAULT_PROVIDER_ID)
    return {
        "id": job["id"],
        "title": job["title"],
        "provider_id": pid,
        "provider_name": provider_name(pid),
        "series_url": job["series_url"],
        "status": job["status"],
        "status_text": status_text(job["status"]),
        "error": job.get("error", ""),
        "retry_file": job.get("retry_file", ""),
        "created_at": job.get("created_at", ""),
        "queue_order": queue_order_value(job),
        "started_at": job.get("started_at", ""),
        "finished_at": job.get("finished_at", ""),
        "done_chapters": int(job.get("done_chapters", 0)),
        "total_chapters": int(job.get("total_chapters", 0)),
        "saved_images": int(job.get("saved_images", 0)),
        "total_images": int(job.get("total_images", 0)),
        "successful_chapters": int(job.get("successful_chapters", 0)),
        "failed_chapters": int(job.get("failed_chapters", 0)),
        "logs": job.get("logs", []),
        "controls": job_controls(job),
    }

