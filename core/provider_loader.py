from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Optional

from core.provider_base import SiteProvider


def _load_module(module_name: str, file_path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module spec: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_provider_plugins(
    plugins_dir: Path,
    context: dict[str, Any],
    *,
    logger: Optional[Callable[[str], None]] = None,
) -> dict[str, SiteProvider]:
    providers: dict[str, SiteProvider] = {}
    if not plugins_dir.exists():
        if logger:
            logger(f"provider plugin dir not found: {plugins_dir}")
        return providers

    for idx, plugin_file in enumerate(sorted(plugins_dir.glob("*_plugin.py"))):
        if plugin_file.name.startswith("_"):
            continue

        module_name = f"provider_plugin_{plugin_file.stem}_{idx}"
        try:
            module = _load_module(module_name, plugin_file)
        except Exception as exc:
            if logger:
                logger(f"load plugin failed: {plugin_file.name}: {exc}")
            continue

        register = getattr(module, "register", None)
        if not callable(register):
            if logger:
                logger(f"skip plugin without register(): {plugin_file.name}")
            continue

        try:
            provider = register(context)
        except Exception as exc:
            if logger:
                logger(f"register plugin failed: {plugin_file.name}: {exc}")
            continue

        provider_id = str(getattr(provider, "provider_id", "")).strip().lower()
        if not provider_id:
            if logger:
                logger(f"skip plugin with empty provider_id: {plugin_file.name}")
            continue

        providers[provider_id] = provider

    return providers
