import argparse
import importlib.util
import logging
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
REQUIREMENTS_FILE = BASE_DIR / "requirements.txt"
IMPORT_NAME_MAP = {
    "beautifulsoup4": "bs4",
}
PIP_SOURCES = [
    ("PyPI", "https://pypi.org/simple", "https://pypi.org/simple/pip/"),
    ("TUNA", "https://pypi.tuna.tsinghua.edu.cn/simple", "https://pypi.tuna.tsinghua.edu.cn/simple/pip/"),
    ("Aliyun", "https://mirrors.aliyun.com/pypi/simple", "https://mirrors.aliyun.com/pypi/simple/pip/"),
    ("USTC", "https://pypi.mirrors.ustc.edu.cn/simple", "https://pypi.mirrors.ustc.edu.cn/simple/pip/"),
]


def safe_print(message: str) -> None:
    text = str(message)
    try:
        print(text)
        return
    except UnicodeEncodeError:
        pass

    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        fallback = text.encode(encoding, errors="backslashreplace").decode(encoding, errors="ignore")
        print(fallback)
    except Exception:
        # Last resort: avoid crashing startup due to terminal encoding.
        print(text.encode("ascii", errors="backslashreplace").decode("ascii"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="漫画下载 WebUI")
    parser.add_argument("--host", default="127.0.0.1", help="WebUI host")
    parser.add_argument("--port", type=int, default=9999, help="WebUI port")
    parser.add_argument(
        "--skip-auto-install",
        action="store_true",
        help="跳过启动前自动安装依赖",
    )
    return parser.parse_args()


def parse_requirement_names(requirements_file: Path) -> list[str]:
    if not requirements_file.exists():
        return []

    names: list[str] = []
    pattern = re.compile(r"^[A-Za-z0-9_.-]+")
    for raw_line in requirements_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip().lstrip("\ufeff")
        if not line or line.startswith("#"):
            continue
        if line.startswith("-"):
            continue
        marker_split = line.split(";", 1)[0].strip()
        match = pattern.match(marker_split)
        if not match:
            continue
        names.append(match.group(0))
    return names


def module_name_for_package(package_name: str) -> str:
    key = package_name.strip().lower()
    if key in IMPORT_NAME_MAP:
        return IMPORT_NAME_MAP[key]
    return key.replace("-", "_")


def find_missing_packages(requirements_file: Path) -> list[str]:
    missing: list[str] = []
    for package_name in parse_requirement_names(requirements_file):
        module_name = module_name_for_package(package_name)
        if importlib.util.find_spec(module_name) is None:
            missing.append(package_name)
    return missing


def select_best_pip_source(timeout_seconds: float = 1.8) -> tuple[str, str]:
    best_name = "PyPI"
    best_index = "https://pypi.org/simple"
    best_latency = float("inf")
    headers = {"User-Agent": "comic-downloader-bootstrap/1.0"}

    for source_name, index_url, health_url in PIP_SOURCES:
        start = time.perf_counter()
        request = urllib.request.Request(health_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                if getattr(response, "status", 200) >= 400:
                    continue
            latency = time.perf_counter() - start
            if latency < best_latency:
                best_latency = latency
                best_name = source_name
                best_index = index_url
        except (urllib.error.URLError, TimeoutError, OSError):
            continue

    return best_name, best_index


def auto_install_dependencies(requirements_file: Path) -> None:
    missing_packages = find_missing_packages(requirements_file)
    if not missing_packages:
        safe_print("[bootstrap] 依赖检查完成，未发现缺失。")
        return

    safe_print(f"[bootstrap] 检测到缺失依赖：{', '.join(missing_packages)}")
    source_name, index_url = select_best_pip_source()
    safe_print(f"[bootstrap] 自动选择 pip 源：{source_name} ({index_url})")

    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--timeout",
        "20",
        "--retries",
        "2",
        "-r",
        str(requirements_file),
        "-i",
        index_url,
    ]
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError("自动安装依赖失败，请检查网络或手动执行 pip install -r requirements.txt")

    safe_print("[bootstrap] 依赖安装完成。")


def main() -> None:
    args = parse_args()
    if not args.skip_auto_install:
        auto_install_dependencies(REQUIREMENTS_FILE)

    from aiohttp import web
    from app.webui import create_app

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    access_logger = logging.getLogger("aiohttp.access")

    app = create_app()
    try:
        web.run_app(
            app,
            host=args.host,
            port=args.port,
            access_log=access_logger,
            access_log_format='%a "%r" %s %b',
            print=safe_print,
        )
    except OSError as exc:
        winerror = getattr(exc, "winerror", None)
        errno = getattr(exc, "errno", None)
        text = str(exc)
        if winerror == 10048 or errno == 10048 or "10048" in text:
            raise SystemExit(
                f"端口 {args.port} 已被占用，程序已退出。请释放该端口后重试，或使用 --port 指定其它固定端口。"
            ) from exc
        raise


if __name__ == "__main__":
    main()
