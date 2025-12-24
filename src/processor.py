import collections
import csv
import hashlib
import json
import os.path
import pickle
import random
import time
from googleapiclient.errors import HttpError
from progress.counter import Counter
from progress.bar import IncrementalBar

from src import helpers
from src.service import Service

_progressPadding = 29
_CACHE_DIR = "cache"
_CACHE_TTL_SECONDS = 86400
_MAX_RETRIES = 5
_RETRY_BASE_DELAY_SECONDS = 1.0
_RETRY_MAX_DELAY_SECONDS = 60.0
_DEFAULT_MAX_RETRY_ROUNDS = 5


class Processor:
    # Talk to google api, fetch results and decorate them
    def __init__(self, query=None, max_retry_rounds=None):
        self.service = Service().instance()
        self.user_id = "me"
        self.query = query
        self.cache_key = self._build_cache_key(query)
        if max_retry_rounds is None:
            self.max_retry_rounds = _DEFAULT_MAX_RETRY_ROUNDS
        elif max_retry_rounds <= 0:
            self.max_retry_rounds = None
        else:
            self.max_retry_rounds = max_retry_rounds
        self.messagesQueue = collections.deque()
        self.failedMessagesQueue = collections.deque()
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(_CACHE_DIR):
            os.makedirs(_CACHE_DIR)

    def _build_cache_key(self, query):
        if not query:
            return None
        return hashlib.sha1(query.encode("utf-8")).hexdigest()[:10]

    def _cache_path(self, prefix):
        if self.cache_key:
            filename = f"{prefix}_{self.cache_key}.pickle"
        else:
            filename = f"{prefix}.pickle"
        return os.path.join(_CACHE_DIR, filename)

    def _should_retry(self, exception):
        if not isinstance(exception, HttpError):
            return False

        status = getattr(exception.resp, "status", None)
        if status in (429, 500, 503):
            return True

        if status == 403:
            reason = self._extract_error_reason(exception)
            if reason in ("rateLimitExceeded", "userRateLimitExceeded"):
                return True

        return False

    def _extract_error_reason(self, exception):
        try:
            payload = json.loads(exception.content.decode("utf-8"))
        except (ValueError, AttributeError, UnicodeDecodeError):
            return None

        try:
            return payload["error"]["errors"][0]["reason"]
        except (KeyError, IndexError, TypeError):
            return None

    def _sleep_with_backoff(self, attempt):
        base_delay = min(_RETRY_MAX_DELAY_SECONDS, _RETRY_BASE_DELAY_SECONDS * (2 ** attempt))
        jitter = random.uniform(0, base_delay * 0.1)
        time.sleep(base_delay + jitter)

    def _execute_with_backoff(self, request_callable, context):
        attempt = 0
        while True:
            try:
                return request_callable()
            except HttpError as exception:
                if not self._should_retry(exception) or attempt >= _MAX_RETRIES:
                    raise
                wait_time = min(
                    _RETRY_MAX_DELAY_SECONDS,
                    _RETRY_BASE_DELAY_SECONDS * (2 ** attempt),
                )
                jitter = random.uniform(0, wait_time * 0.1)
                print(
                    f"{helpers.loader_icn} Rate limit or transient error while {context}. "
                    f"Retrying in {wait_time + jitter:.1f}s"
                )
                time.sleep(wait_time + jitter)
                attempt += 1

    def get_messages(self, force_refresh=False):
        # Get all messages of user
        # Output format:
        # [{'id': '13c...7', 'threadId': '13c...7'}, ...]

        cache_file = self._cache_path("messages")

        # Check if cache exists and is not older than 24 hours
        if (
            not force_refresh
            and os.path.exists(cache_file)
            and (time.time() - os.path.getmtime(cache_file) < _CACHE_TTL_SECONDS)
        ):
            print(f"{helpers.loader_icn} Loading messages from cache")
            with open(cache_file, "rb") as token:
                messages = pickle.load(token)
                return messages

        # includeSpamTrash
        # labelIds

        list_kwargs = {
            "userId": self.user_id,
            "fields": "messages(id,threadId),nextPageToken,resultSizeEstimate",
        }
        if self.query:
            list_kwargs["q"] = self.query

        response = self._execute_with_backoff(
            lambda: self.service.users().messages().list(**list_kwargs).execute(),
            "listing messages",
        )
        messages = []
        est_max = response["resultSizeEstimate"] * 5

        progress = Counter(
            f"{helpers.loader_icn} Fetching messages page ".ljust(_progressPadding, " ")
        )

        if "messages" in response:
            messages.extend(response["messages"])

        while "nextPageToken" in response:
            page_token = response["nextPageToken"]

            list_kwargs["pageToken"] = page_token
            response = self._execute_with_backoff(
                lambda: self.service.users().messages().list(**list_kwargs).execute(),
                "listing messages",
            )
            messages.extend(response["messages"])

            progress.next()

        progress.finish()
        
        # Cache the messages
        with open(cache_file, "wb") as token:
            pickle.dump(messages, token)

        return messages

    def process_message(self, request_id, response, exception):
        if exception is not None:
            if self._should_retry(exception):
                self.failedMessagesQueue.append(request_id)
            else:
                print(
                    f"{helpers.loader_icn} Skipping message {request_id} due to error: {exception}"
                )
            return

        headers = response.get("payload", {}).get("headers", [])

        _date = next(
            (header["value"] for header in headers if header["name"] == "Date"), None
        )
        _from = next(
            (header["value"] for header in headers if header["name"] == "From"), None
        )
        _subject = next(
            (header["value"] for header in headers if header["name"] == "Subject"), None
        )

        self.messagesQueue.append(
            {
                "id": response["id"],
                "labels": response.get("labelIds", []),
                "fields": {"from": _from, "date": _date, "subject": _subject},
            }
        )

    def get_metadata(self, messages, force_refresh=False):
        # Get metadata for all messages:
        # 1. Create a batch get message request for all messages
        # 2. Process the returned output
        #
        # Output format:
        # {
        #   'id': '16f....427',
        #   'labels': ['UNREAD', 'CATEGORY_UPDATES', 'INBOX'],
        #   'fields': [
        #     {'name': 'Date', 'value': 'Tue, 24 Dec 2019 22:13:09 +0000'},
        #     {'name': 'From', 'value': 'Coursera <no-reply@t.mail.coursera.org>'}
        #   ]
        # }

        cache_file = self._cache_path("metadata")

        cache_exists = os.path.exists(cache_file)
        cache_fresh = cache_exists and (
            time.time() - os.path.getmtime(cache_file) < _CACHE_TTL_SECONDS
        )

        if cache_exists and not force_refresh:
            if cache_fresh:
                print(f"{helpers.loader_icn} Loading message metadata from cache")
            else:
                print(f"{helpers.loader_icn} Loading stale metadata cache to resume")
            with open(cache_file, "rb") as token:
                cached = pickle.load(token)
                self.messagesQueue = (
                    cached if isinstance(cached, collections.deque) else collections.deque(cached)
                )
            cached_ids = {message["id"] for message in self.messagesQueue}
            messages = [message for message in messages if message["id"] not in cached_ids]
            if not messages:
                return

        progress = IncrementalBar(
            f"{helpers.loader_icn} Fetching messages meta data ".ljust(
                _progressPadding, " "
            ),
            max=len(messages),
        )

        for messages_batch in helpers.chunks(messages, 100):
            # for messages_batch in [messages[0:1000]]:
            batch = self.service.new_batch_http_request()

            for message in messages_batch:
                msg_id = message["id"]
                batch.add(
                    self.service.users()
                    .messages()
                    .get(
                        userId=self.user_id,
                        id=msg_id,
                        format="metadata",
                        metadataHeaders=["From", "Date", "Subject"],
                        fields="id,labelIds,payload/headers",
                    ),
                    callback=self.process_message,
                    request_id=msg_id,
                )

            self._execute_with_backoff(batch.execute, "fetching message metadata")
            progress.next(len(messages_batch))

        progress.finish()

        self._retry_failed_messages()
        
        # Cache the metadata
        with open(cache_file, "wb") as token:
            pickle.dump(self.messagesQueue, token)

    def load_cached_metadata(self):
        cache_file = self._cache_path("metadata")
        if not os.path.exists(cache_file):
            return False
        print(f"{helpers.loader_icn} Loading message metadata from cache")
        with open(cache_file, "rb") as token:
            cached = pickle.load(token)
            self.messagesQueue = (
                cached if isinstance(cached, collections.deque) else collections.deque(cached)
            )
        return True

    def export_csv(self, path):
        if not self.messagesQueue:
            print(f"{helpers.loader_icn} No metadata loaded; skipping CSV export.")
            return

        rows = list(self.messagesQueue)
        fieldnames = ["id", "from", "date", "subject", "labels"]
        with open(path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                fields = row.get("fields", {})
                writer.writerow(
                    {
                        "id": row.get("id"),
                        "from": fields.get("from"),
                        "date": fields.get("date"),
                        "subject": fields.get("subject"),
                        "labels": ",".join(row.get("labels", [])),
                    }
                )
        print(f"{helpers.loader_icn} Exported CSV to {path}")

    def get_query_message_ids(self):
        if not self.query:
            return None

        response = self._execute_with_backoff(
            lambda: self.service.users()
            .messages()
            .list(
                userId=self.user_id,
                q=self.query,
                fields="messages(id,threadId),nextPageToken",
            )
            .execute(),
            "listing query messages",
        )
        messages = []

        if "messages" in response:
            messages.extend(response["messages"])

        while "nextPageToken" in response:
            page_token = response["nextPageToken"]
            response = self._execute_with_backoff(
                lambda: self.service.users()
                .messages()
                .list(
                    userId=self.user_id,
                    pageToken=page_token,
                    q=self.query,
                    fields="messages(id,threadId),nextPageToken",
                )
                .execute(),
                "listing query messages",
            )
            messages.extend(response["messages"])

        return {message["id"] for message in messages}

    def filter_messages_queue(self, message_ids):
        if message_ids is None:
            return
        if not message_ids:
            self.messagesQueue = collections.deque()
            return
        self.messagesQueue = collections.deque(
            message for message in self.messagesQueue if message["id"] in message_ids
        )

    def _retry_failed_messages(self):
        retry_round = 0
        while self.failedMessagesQueue and (
            self.max_retry_rounds is None or retry_round < self.max_retry_rounds
        ):
            failed_ids = list(self.failedMessagesQueue)
            self.failedMessagesQueue = collections.deque()

            print(
                f"{helpers.loader_icn} Retrying {len(failed_ids)} failed messages "
                f"(round {retry_round + 1}/"
                f"{self.max_retry_rounds if self.max_retry_rounds is not None else '∞'})"
            )

            self._sleep_with_backoff(retry_round)

            for messages_batch in helpers.chunks(failed_ids, 50):
                batch = self.service.new_batch_http_request()
                for msg_id in messages_batch:
                    batch.add(
                        self.service.users()
                        .messages()
                        .get(
                            userId=self.user_id,
                            id=msg_id,
                            format="metadata",
                        metadataHeaders=["From", "Date", "Subject"],
                        fields="id,labelIds,payload/headers",
                    ),
                    callback=self.process_message,
                    request_id=msg_id,
                    )

                try:
                    self._execute_with_backoff(batch.execute, "retrying message metadata")
                except HttpError as exception:
                    if not self._should_retry(exception):
                        print(f"{helpers.loader_icn} Batch retry failed: {exception}")
                        return

            retry_round += 1
            if self.failedMessagesQueue and len(self.failedMessagesQueue) >= len(failed_ids):
                print(
                    f"{helpers.loader_icn} Retry did not reduce failures; stopping retries."
                )
                return

        if self.failedMessagesQueue:
            print(
                f"{helpers.loader_icn} Unable to fetch {len(self.failedMessagesQueue)} "
                "messages after retries"
            )
