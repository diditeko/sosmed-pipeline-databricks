import os
import yaml
import re
from pathlib import Path
from functools import lru_cache


class ConfigLoader:
    """
    Central config loader untuk seluruh pipeline.
    Menggabungkan:
    - Global configs/*.yaml
    - Environment overrides configs/env/<env>.yaml
    - Resolusi secret: {{secret:KEY}}
    - Fallback .env lokal
    """
    
    BASE_DIR = Path(__file__).resolve().parents[1] / "configs"
    SECRET_PATTERN = re.compile(r"\{\{\s*secret:([a-zA-Z0-9._-]+)\s*\}\}")

    def __init__(self, env=None):
        self.env = env or os.getenv("ENV", "dev").lower()

        self.global_files = [
            "databricks.yaml",
            "mongodb_config.yaml",
            "elasticsearch.yaml",
            "kafka_config.yaml",
            "paths.yaml",
        ]

    # ---------- PUBLIC API ----------
    @lru_cache(maxsize=1)
    def load(self):
        """Load full merged config."""
        config = {}

        # 1. Load global configs
        for file in self.global_files:
            cfg_path = self.BASE_DIR / file
            if cfg_path.exists():
                config = self._deep_merge(config, self._load_yaml(cfg_path))

        # 2. Apply environment overrides
        env_file = self.BASE_DIR / "env" / f"{self.env}.yaml"
        if env_file.exists():
            config = self._deep_merge(config, self._load_yaml(env_file))

        # 3. Resolve secrets
        config = self._resolve_secrets(config)

        return config

    # ---------- INTERNAL METHODS ----------
    def _load_yaml(self, path: Path):
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}

    def _deep_merge(self, base, override):
        """Deep merge dict (recursive). Override values replace base."""
        for key, value in override.items():
            if (
                key in base
                and isinstance(base[key], dict)
                and isinstance(value, dict)
            ):
                base[key] = self._deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    def _resolve_secrets(self, config):

        if isinstance(config, dict):
            return {k: self._resolve_secrets(v) for k, v in config.items()}

        if isinstance(config, list):
            return [self._resolve_secrets(v) for v in config]

        if isinstance(config, str):
            return self._resolve_string_secret(config)

        return config

    def _resolve_string_secret(self, value):
        """Resolve {{secret:KEY}} placeholder."""

        match = self.SECRET_PATTERN.findall(value)
        if not match:
            return value

        # Resolve setiap secret yang ditemukan
        for key in match:
            resolved = self._get_secret(key)
            value = value.replace(f"{{{{secret:{key}}}}}", resolved)

        return value

    def _get_secret(self, key):
        """
        Resolusi secret:
        1) Databricks Secret Scope (jika tersedia)
        2) ENV var (.env untuk dev)
        """
        # --- Databricks secret manager ---
        try:
            import dbutils  # type: ignore  # in Databricks runtime only

            scope, actual_key = key.split(".", 1)
            return dbutils.secrets.get(scope=scope, key=actual_key)

        except Exception:
            pass

        # --- Fallback ENV variable ---
        val = os.getenv(key)
        if val:
            return val

        raise ValueError(
            f"Secret '{key}' tidak ditemukan. "
            "Pastikan sudah di-set di Databricks Secret Scope atau .env."
        )


# ---------- CONVENIENCE FUNCTION ----------
@lru_cache(maxsize=1)
def load_config(env=None):
    return ConfigLoader(env).load()
