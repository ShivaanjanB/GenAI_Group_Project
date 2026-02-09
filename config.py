"""
Configuration handling for the audit‑trail pipeline.

This module defines a simple ``Settings`` dataclass to represent runtime
configuration loaded from files in the ``config`` directory. The current
implementation expects a JSON or YAML file named ``settings.json`` or
``settings.yaml`` containing at least ``as_of_date`` (ISO date string)
and ``log_level`` (e.g. "INFO"). Optionally a ``sources`` section can
provide metadata for document sources.

If configuration cannot be loaded, a ``ConfigError`` will be raised.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore


class ConfigError(Exception):
    """Raised when there is a problem loading or parsing configuration files."""


@dataclass
class Settings:
    """
    Dataclass representing runtime settings for the pipeline.

    Attributes
    ----------
    as_of_date : date
        Cutoff date beyond which documents should not be considered. Only
        documents published on or before this date are eligible unless
        explicitly marked as matched.
    log_level : str
        Logging level for the pipeline (e.g. "INFO", "DEBUG").
    sources : Any
        Optional configuration for document sources; structure is not
        enforced here and is passed through unchanged.
    """
    as_of_date: date
    log_level: str
    sources: Optional[Any] = None


def _parse_date(value: Union[str, date]) -> date:
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(value).date()
    except Exception as exc:
        raise ConfigError(f"Invalid date format: {value}") from exc


def load_settings(config_dir: Path) -> Settings:
    """
    Load pipeline settings from the given configuration directory.

    The function looks for ``settings.json``, ``settings.yaml`` or
    ``settings.yml`` within ``config_dir``. At minimum, the file must
    define ``as_of_date`` and ``log_level``. Additional fields are
    preserved in the ``sources`` attribute.

    Parameters
    ----------
    config_dir : Path
        Directory containing configuration files.

    Returns
    -------
    Settings
        Parsed settings dataclass.

    Raises
    ------
    ConfigError
        If configuration cannot be loaded or required fields are missing.
    FileNotFoundError
        If no supported configuration file is found.
    """
    # Determine candidate config files
    candidates = [
        config_dir / "settings.json",
        config_dir / "settings.yaml",
        config_dir / "settings.yml",
    ]
    config_path: Optional[Path] = None
    for path in candidates:
        if path.exists():
            config_path = path
            break
    if config_path is None:
        raise FileNotFoundError(f"No settings file found in {config_dir}")
    # Load config
    if config_path.suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ConfigError("PyYAML is required to load YAML configuration files")
        with config_path.open("r", encoding="utf-8") as f:
            try:
                data = yaml.safe_load(f)
            except Exception as exc:
                raise ConfigError(f"Failed to parse YAML: {exc}") from exc
    else:
        with config_path.open("r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception as exc:
                raise ConfigError(f"Failed to parse JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("Configuration root must be a mapping")
    if "as_of_date" not in data or "log_level" not in data:
        raise ConfigError("Configuration must include 'as_of_date' and 'log_level'")
    as_of_date = _parse_date(data["as_of_date"])
    log_level = str(data["log_level"]).upper()
    sources = data.get("sources")
    return Settings(as_of_date=as_of_date, log_level=log_level, sources=sources)