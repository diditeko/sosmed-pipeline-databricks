# main.py
import time
from ast import literal_eval

from decouple import config

from scraper_api import TwitterApiClient, TwitterApiError
from producer_new import KafkaTweetProducer  # asumsi sama seperti sebelumnya


def dedupe_by_id(tweets):
    """Hapus duplikat tweet berdasarkan id."""
    by_id = {}
    for t in tweets:
        tid = t.get("id")
        if tid:
            by_id[tid] = t
    return list(by_id.values())


def build_tweet_payload(tweet, keyword: str) -> dict:
    """Mapping struktur tweet dari TwitterAPI.io ke payload Kafka (untuk root tweet)."""
    author = tweet.get("author") or {}
    return {
        "type": "tweet",
        "id": tweet.get("id"),
        "url": tweet.get("url"),
        "keyword": keyword,
        "text": tweet.get("text"),
        "created_at": tweet.get("createdAt"),
        "lang": tweet.get("lang"),
        "username": author.get("userName"),
        "user_id": author.get("id"),
        "user_name": author.get("name"),
        "is_reply": tweet.get("isReply", False),
        "in_reply_to_id": tweet.get("inReplyToId"),
        "in_reply_to_username": tweet.get("inReplyToUsername"),
        "reply_count": tweet.get("replyCount", 0),
        "retweet_count": tweet.get("retweetCount", 0),
        "like_count": tweet.get("likeCount", 0),
        "quote_count": tweet.get("quoteCount", 0),
        "view_count": tweet.get("viewCount", 0),
        "bookmark_count": tweet.get("bookmarkCount", 0),
        # bisa tambah field lain kalau perlu
    }


def build_reply_payload(reply, root_tweet_id: str, keyword: str) -> dict:
    """Mapping struktur tweet reply ke payload Kafka (type='reply')."""
    author = reply.get("author") or {}
    return {
        "type": "reply",
        "id": reply.get("id"),
        "url": reply.get("url"),
        "root_tweet_id": root_tweet_id,
        "keyword": keyword,
        "text": reply.get("text"),
        "created_at": reply.get("createdAt"),
        "lang": reply.get("lang"),
        "username": author.get("userName"),
        "user_id": author.get("id"),
        "user_name": author.get("name"),
        "is_reply": reply.get("isReply", True),
        "in_reply_to_id": reply.get("inReplyToId"),
        "in_reply_to_username": reply.get("inReplyToUsername"),
        "reply_count": reply.get("replyCount", 0),
        "retweet_count": reply.get("retweetCount", 0),
        "like_count": reply.get("likeCount", 0),
        "quote_count": reply.get("quoteCount", 0),
        "view_count": reply.get("viewCount", 0),
        "bookmark_count": reply.get("bookmarkCount", 0),
    }


def main():
    # -----------------------------
    # Load config dari .env
    # -----------------------------
    KAFKA_TOPIC = config("topic_raw")
    KEYWORDS = literal_eval(config("Keywords"))
    API_KEY = config("TWITTERAPI_KEY")

    DAYS_BACK = int(config("DAYS_BACK", default=3))
    REPLY_THRESHOLD = int(config("REPLY_THRESHOLD", default=10))
    MAX_PAGES = int(config("MAX_PAGES", default=5))

    print(KEYWORDS)
    print(f"[+] Kafka topic: {KAFKA_TOPIC}")
    print(f"[+] Keywords: {KEYWORDS}")
    print(f"[+] Days back: {DAYS_BACK}")
    print(f"[+] Reply threshold: {REPLY_THRESHOLD}")

    # -----------------------------
    # Init client & producer
    # -----------------------------
    client = TwitterApiClient(
        api_key=API_KEY,
        min_interval=10.0,       # > 5 detik (free-tier limit)
        max_retries=2,
        backoff_seconds=20.0,
    )
    producer = KafkaTweetProducer(topic=KAFKA_TOPIC)

    # -----------------------------
    # Loop keyword
    # -----------------------------
    for keyword in KEYWORDS:
        print(f"\n[+] Fetching tweets for keyword: '{keyword}' (last {DAYS_BACK} days)")

        try:
            tweets = client.search_keyword_last_n_days(
                keyword=keyword,
                days_back=DAYS_BACK,
                max_pages=MAX_PAGES,
                query_type="Latest",
            )
        except TwitterApiError as e:
            print(f"[!] Error fetching tweets for keyword '{keyword}': {e}")
            continue

        # Dedupe tweet berdasarkan id
        tweets = dedupe_by_id(tweets)
        print(f"[+] Got {len(tweets)} unique tweets for keyword '{keyword}'")

        replies_fetched = set()

        # -----------------------------
        # Kirim root tweet + (opsional) replies
        # -----------------------------
        for tweet in tweets:
            tweet_id = tweet.get("id")
            reply_count = tweet.get("replyCount", 0) or 0

            # Kirim root tweet ke Kafka
            tweet_payload = build_tweet_payload(tweet, keyword=keyword)
            producer.send(tweet_payload)
            print(f"[tweet] Sent {tweet_id} (replies={reply_count})")

            # Kalau reply-nya banyak, fetch juga reply-nya
            if reply_count >= REPLY_THRESHOLD and tweet_id not in replies_fetched:
                print(
                    f"   [+] Tweet {tweet_id} has replyCount={reply_count}, "
                    f"fetching replies..."
                )
                try:
                    replies = client.get_replies(tweet_id, max_pages=1)
                    print(
                        f"   [+] Got {len(replies)} replies for tweet {tweet_id}"
                    )
                    for r in replies:
                        reply_payload = build_reply_payload(
                            r,
                            root_tweet_id=tweet_id,
                            keyword=keyword,
                        )
                        producer.send(reply_payload)
                        print(
                            f"   [reply] Sent reply {reply_payload['id']} "
                            f"(root={tweet_id})"
                        )
                    replies_fetched.add(tweet_id)
                except TwitterApiError as e:
                    print(
                        f"   [!] Error fetching replies for {tweet_id}: {e}"
                    )

        # Jeda sebelum pindah keyword berikutnya (aman untuk limit global)
        print(
            f"[+] Done keyword '{keyword}', sleeping 8s before next keyword..."
        )
        time.sleep(8)


if __name__ == "__main__":
    main()
