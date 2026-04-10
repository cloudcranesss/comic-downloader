import argparse

from aiohttp import web

from app.webui import create_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="漫画下载 WebUI")
    parser.add_argument("--host", default="127.0.0.1", help="WebUI host")
    parser.add_argument("--port", type=int, default=8000, help="WebUI port")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app()
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
