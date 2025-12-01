# scraper_api.py
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any

import requests


class TwitterApiError(Exception):
    """Custom exception for TwitterAPI.io related errors."""
    pass


class TwitterApiClient:
    """
    Client wrapper untuk TwitterAPI.io dengan:
    - Global rate-limiting (min_interval detik antar request)
    - Simple retry untuk status 429 (Too Many Requests)
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.twitterapi.io",
        min_interval: float = 10.0,       # > 5 detik untuk free-tier
        max_retries: int = 2,            # retry kalau kena 429
        backoff_seconds: float = 20.0,   # tunggu sebelum retry
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

        self._last_request_ts: float = 0.0

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _rate_limit_wait(self) -> None:
        """Pastikan jeda minimal min_interval detik antar request."""
        now = time.time()
        elapsed = now - self._last_request_ts
        wait = self.min_interval - elapsed
        if wait > 0:
            print(f"[DEBUG] Global sleep {wait:.1f}s (respecting free-tier limit)")
            time.sleep(wait)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """
        Wrapper HTTP request dengan:
        - rate-limit global
        - retry kalau 429
        """
        url = f"{self.base_url}{path}"
        headers = {
            "x-api-key": self.api_key,
            "Accept": "application/json",
        }

        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            self._rate_limit_wait()

            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json,
                    timeout=30,
                )
                self._last_request_ts = time.time()

                # Kalau bukan 429, langsung return
                if resp.status_code != 429:
                    return resp

                # Kalau 429 dan masih boleh retry
                if attempt < self.max_retries:
                    print(
                        f"[WARN] Got 429 on {path}, attempt {attempt + 1}/{self.max_retries}. "
                        f"Backing off {self.backoff_seconds:.0f}s..."
                    )
                    time.sleep(self.backoff_seconds)
                    continue

                # Sudah habis retry
                return resp

            except Exception as e:
                last_exc = e
                print(f"[ERROR] HTTP error on {path}: {e}")
                if attempt < self.max_retries:
                    print(f"[WARN] Retrying in {self.backoff_seconds:.0f}s...")
                    time.sleep(self.backoff_seconds)
                else:
                    break

        if last_exc:
            raise TwitterApiError(f"HTTP request failed after retries: {last_exc}")
        raise TwitterApiError("Unknown HTTP error")

    # ----------------------------
    # TwitterAPI.io endpoints
    # ----------------------------
    def advanced_search(
        self,
        query: str,
        query_type: str = "Latest",
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
        """
        Wrapper untuk /twitter/tweet/advanced_search

        Return:
            tweets: List tweet dict
            next_cursor: cursor untuk next page (atau None)
            has_next: bool apakah masih ada next page
        """
        params: Dict[str, Any] = {
            "query": query,
            "queryType": query_type,
        }
        if cursor:
            params["cursor"] = cursor

        resp = self._request("GET", "/twitter/tweet/advanced_search", params=params)

        if resp.status_code != 200:
            raise TwitterApiError(
                f"advanced_search failed ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        tweets = data.get("tweets", [])
        next_cursor = data.get("next_cursor")
        has_next = bool(data.get("has_next_page", False))

        return tweets, next_cursor, has_next

    def get_replies(
        self,
        tweet_id: str,
        max_pages: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Wrapper untuk /twitter/tweet/replies

        Hanya ambil sampai max_pages (default 1 page = cukup untuk banyak kasus).
        """
        all_replies: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page = 0

        while True:
            params: Dict[str, Any] = {"tweetId": tweet_id}
            if cursor:
                params["cursor"] = cursor

            resp = self._request("GET", "/twitter/tweet/replies", params=params)

            if resp.status_code != 200:
                raise TwitterApiError(
                    f"get_replies failed ({resp.status_code}) for {tweet_id}: {resp.text}"
                )

            data = resp.json()
            replies = data.get("tweets", [])
            cursor = data.get("next_cursor")
            has_next = bool(data.get("has_next_page", False))

            all_replies.extend(replies)
            page += 1

            print(
                f"[DEBUG]   Replies page {page} -> {len(replies)} replies "
                f"(total={len(all_replies)})"
            )

            if not has_next or not cursor:
                break
            if page >= max_pages:
                break

        return all_replies

    # ----------------------------
    # Convenience helper
    # ----------------------------
    def search_keyword_last_n_days(
        self,
        keyword: str,
        days_back: int = 3,
        max_pages: int = 5,
        query_type: str = "Latest",
    ) -> List[Dict[str, Any]]:
        """
        Cari tweet berdasarkan keyword untuk n hari ke belakang
        dengan paginasi (advanced_search).
        """
        today = datetime.utcnow().date()
        since = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
        until = today.strftime("%Y-%m-%d")

        query = f"{keyword} since:{since} until:{until}"
        print(
            f"[DEBUG] Query for '{keyword}': {query}"
        )

        all_tweets: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page = 0

        while True:
            tweets, cursor, has_next = self.advanced_search(
                query=query,
                query_type=query_type,
                cursor=cursor,
            )

            all_tweets.extend(tweets)
            page += 1
            print(
                f"[DEBUG]  Page {page} -> {len(tweets)} tweets "
                f"(total={len(all_tweets)})"
            )

            if not has_next or not cursor:
                break
            if max_pages is not None and page >= max_pages:
                print("[DEBUG]  Reached max_pages limit, stopping pagination.")
                break

        return all_tweets
