import collections
import os.path
import pickle
import time
from progress.counter import Counter
from progress.bar import IncrementalBar

from src import helpers
from src.service import Service

_progressPadding = 29
_CACHE_DIR = "cache"


class Processor:
    # Talk to google api, fetch results and decorate them
    def __init__(self):
        self.service = Service().instance()
        self.user_id = "me"
        self.messagesQueue = collections.deque()
        self.failedMessagesQueue = collections.deque()
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(_CACHE_DIR):
            os.makedirs(_CACHE_DIR)

    def get_messages(self):
        # Get all messages of user
        # Output format:
        # [{'id': '13c...7', 'threadId': '13c...7'}, ...]
        
        cache_file = os.path.join(_CACHE_DIR, "messages.pickle")
        
        # Check if cache exists and is not older than 24 hours
        if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file) < 86400):
            print(f"{helpers.loader_icn} Loading messages from cache")
            with open(cache_file, "rb") as token:
                messages = pickle.load(token)
                return messages

        # includeSpamTrash
        # labelIds

        response = self.service.users().messages().list(userId=self.user_id).execute()
        messages = []
        est_max = response["resultSizeEstimate"] * 5

        progress = Counter(
            f"{helpers.loader_icn} Fetching messages page ".ljust(_progressPadding, " ")
        )

        if "messages" in response:
            messages.extend(response["messages"])

        while "nextPageToken" in response:
            page_token = response["nextPageToken"]

            response = (
                self.service.users()
                .messages()
                .list(userId=self.user_id, pageToken=page_token)
                .execute()
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
            self.failedMessagesQueue.append(exception.uri)
            return

        headers = response["payload"]["headers"]

        _date = next(
            (header["value"] for header in headers if header["name"] == "Date"), None
        )
        _from = next(
            (header["value"] for header in headers if header["name"] == "From"), None
        )

        self.messagesQueue.append(
            {
                "id": response["id"],
                "labels": response.get("labelIds", []),
                "fields": {"from": _from, "date": _date},
            }
        )

    def get_metadata(self, messages):
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

        cache_file = os.path.join(_CACHE_DIR, "metadata.pickle")
        
        # Check if cache exists and is not older than 24 hours
        if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file) < 86400):
            print(f"{helpers.loader_icn} Loading message metadata from cache")
            with open(cache_file, "rb") as token:
                self.messagesQueue = pickle.load(token)
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
                    self.service.users().messages().get(userId=self.user_id, id=msg_id),
                    callback=self.process_message,
                )

            batch.execute()
            progress.next(len(messages_batch))

        progress.finish()
        
        # Cache the metadata
        with open(cache_file, "wb") as token:
            pickle.dump(self.messagesQueue, token)
