from __future__ import annotations


class _DummyConfigStore:
    def get(self, key: str, default: str = "") -> str:
        return default


config_store = _DummyConfigStore()
