from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
import requests
import requests_cache
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class HttpError(RuntimeError):
    pass


@dataclass
class RateLimiter:
    rps: float
    _last_ts: float = 0.0

    def wait(self) -> None:
        if self.rps <= 0:
            return
        min_interval = 1.0 / self.rps
        now = time.time()
        elapsed = now - self._last_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_ts = time.time()

class HttpClient:
    def __init__(self, cache_path: str, expire_seconds: int, contact_email: str = "") -> None:
        self.session = requests_cache.CachedSession(
            cache_name=cache_path,
            backend="sqlite",
            expire_after=expire_seconds,
        )
        ua = "book-success-analysis/1.0 (research)"
        if contact_email:
            ua += f" contact:{contact_email}"
        self.session.headers.update({"User-Agent": ua})
    
    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.8, min=1, max=20),
        retry=retry_if_exception_type((requests.RequestException, HttpError)),
    )
    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
        resp = self.session.get(url, params=params, timeout=timeout)
        if resp.status_code == 429:
            raise HttpError("Rate limited (HTTP 429)")
        if resp.status_code >= 500:
            raise HttpError(f"Server error (HTTP {resp.status_code})")
        if resp.status_code >= 400:
            raise HttpError(f"Client error (HTTP {resp.status_code}): {resp.text[:300]}")
        return resp.json()