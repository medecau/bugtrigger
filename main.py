import base64
import datetime as dt
import json
import logging
import os
from abc import ABC, abstractmethod
from time import sleep
from typing import Any, Dict, List, Set, Union

import dataset
import dotenv
from atproto import Client
from e2b_code_interpreter import Sandbox
from retry import retry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("codebot")

dotenv.load_dotenv()

ATPROTO_TIMEOUT = 60

bsky_user = os.getenv("BSKY_USER")
bsky_pass = os.getenv("BSKY_PASS")

db = dataset.connect("sqlite:///code_bot.db")
logs = db["logs"]
executions = db["executions"]


def log_to_db(msg: str) -> None:
    logs.insert(dict(msg=msg, ts=now()))


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class BlueskyBot(ABC):
    """
    Abstract Bluesky bot base class that handles the core API interactions.
    Subclasses must implement notification filtering and processing logic.
    """

    def __init__(
        self,
        username: str,
        password: str,
        session_file: str = "session.txt",
        poll_interval: int = 30,
        timeout: int = 60,
    ):
        self.username = username
        self.password = password
        self.session_file = session_file
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.logger = logging.getLogger(f"codebot.{self.__class__.__name__.lower()}")
        self.client = self._create_client()

    def _create_client(self) -> Client:
        """Create a new client with a fresh login"""
        client = Client()
        self.logger.info("Creating new client with fresh credentials")
        client.login(self.username, self.password)
        self.logger.info("Session created successfully")
        return client

    @retry(delay=30, backoff=1.1, jitter=(5, 10), max_delay=300)
    def _fetch_notifications(self) -> List:
        response = self.client.app.bsky.notification.list_notifications(
            timeout=self.timeout
        )
        return response.notifications

    @retry(delay=10, backoff=1.1, jitter=(5, 10), max_delay=600)
    def _update_seen(self, last_seen_at: str) -> None:
        self.client.app.bsky.notification.update_seen(
            {"seen_at": last_seen_at}, timeout=self.timeout
        )

    def _send_response(self, note: Any, response: Dict[str, Union[str, List]]) -> None:
        output = response.get("text", "")
        images = response.get("images", [])

        parent = {"cid": note.cid, "uri": note.uri}
        if note.record.reply:
            reply_to = {"root": note.record.reply.root, "parent": parent}
        else:  # this is the root post
            reply_to = {"root": parent, "parent": parent}

        try:
            if images:
                self.client.send_images(text=output, images=images, reply_to=reply_to)
            else:
                self.client.send_post(text=output, reply_to=reply_to)
        except Exception as e:
            self.logger.error(f"Error sending response: {e}")

    @abstractmethod
    def filter_notifications(self, notifications: List) -> List:
        """Filters notifications according to bot-specific criteria"""
        pass

    @abstractmethod
    def should_handle_notification(self, notification: Any) -> bool:
        """Determines if a notification should be processed by this bot"""
        pass

    @abstractmethod
    def process_notification(self, notification: Any) -> Dict[str, Union[str, List]]:
        """Processes a notification and returns a response dict with text and optional images"""
        pass

    def before_batch_processing(self, notifications: List[Any]) -> None:
        """Hook called before processing a batch of notifications"""
        pass

    def after_batch_processing(self, notifications: List[Any]) -> None:
        """Hook called after processing a batch of notifications"""
        pass

    def _recreate_client_if_needed(self) -> None:
        """Recreate client in case of any issues"""
        try:
            # Make a simple API call to verify client is working
            self.client.get_profile(self.username)
        except Exception as e:
            self.logger.warning(f"Client error detected: {e}")
            self.logger.info("Creating a fresh client")
            self.client = self._create_client()

    def _process_batch(self, notifications: List[Any], last_seen_at: str) -> None:
        """Process a batch of notifications and update seen status"""
        self.logger.info(f"Found {len(notifications)} new notifications to process")
        self.before_batch_processing(notifications)

        for note in notifications:
            try:
                response = self.process_notification(note)
            except Exception as e:
                self.logger.error(f"Error processing notification: {e}")
                response = {
                    "text": f"Error processing your request: {str(e)}",
                    "images": [],
                }

            self._send_response(note, response)

        self.after_batch_processing(notifications)

        if notifications:
            self._update_seen(last_seen_at)

    def _handle_client_operations(self) -> None:
        """Execute client operations with proper error handling"""
        last_seen_at = self.client.get_current_time_iso()
        all_notifications = self._fetch_notifications()
        filtered_notifications = self.filter_notifications(all_notifications)

        self._process_batch(filtered_notifications, last_seen_at)

    def run(self) -> None:
        """Main bot loop with simplified error handling structure"""
        self.logger.info("Starting bot loop")

        while True:
            try:
                # Check client validity first
                try:
                    self._recreate_client_if_needed()
                except Exception as e:
                    self.logger.error(f"Critical client error: {e}")
                    sleep(self.poll_interval * 2)
                    continue

                # Execute main client operations
                try:
                    self._handle_client_operations()
                except Exception as api_error:
                    self.logger.error(f"API error in main loop: {api_error}")
                    try:
                        self.client = self._create_client()
                    except Exception as refresh_error:
                        self.logger.error(f"Failed to recreate client: {refresh_error}")

            except Exception as e:
                self.logger.error(f"Unexpected error in main loop: {e}")

            sleep(self.poll_interval)


class CodeExecutionBot(BlueskyBot):
    """Bot that executes code snippets from Bluesky posts"""

    HELLO_MESSAGE = """
    HELLO WORLD!

    I AM A BOT THAT WILL EXECUTE YOUR CODE.
    PUT THIS ON THE FIRST LINE OF YOUR POST:
    #! @runcode.bsky.social python
    AND THEN WRITE YOUR CODE.

    DM ISSUES TO @medecau.bsky.social

    BE KIND AND ENJOY! 
    """.strip()

    BOOTSTRAP = """
import json, base64
bsky = json.loads(base64.b64decode({payload}).decode())
def say_hello():
  print('''{HELLO_MESSAGE}''')
""".strip()

    def __init__(
        self,
        username: str,
        password: str,
        session_file: str = "session.txt",
        poll_interval: int = 30,
        timeout: int = 60,
    ):
        super().__init__(username, password, session_file, poll_interval, timeout)
        self.sandbox = None

    def _filter_base_notifications(self, notifications: List) -> List:
        """Filters out read notifications and blocked/muted users"""
        filtered = (n for n in notifications if not n.is_read)
        filtered = (n for n in filtered if n.reason in {"mention", "reply"})
        filtered = (n for n in filtered if not n.author.viewer.blocked_by)
        filtered = (n for n in filtered if not n.author.viewer.blocking)
        filtered = (n for n in filtered if not n.author.viewer.blocking_by_list)
        filtered = (n for n in filtered if not n.author.viewer.muted)
        filtered = (n for n in filtered if not n.author.viewer.muted_by_list)

        return list(filtered)

    def filter_notifications(self, notifications: List) -> List:
        """Filters and rate limits notifications based on bot criteria"""
        # First apply basic filtering
        filtered_notifications = self._filter_base_notifications(notifications)

        # Then apply bot-specific filtering criteria
        content_filtered = [
            n for n in filtered_notifications if self.should_handle_notification(n)
        ]

        # Apply rate limiting - only process one notification per author
        seen_authors: Set[str] = set()
        rate_limited_notifications = []

        for note in content_filtered:
            if note.author.handle in seen_authors:
                self.logger.info(
                    f"Rate limiting notification from {note.author.handle}"
                )
                # Send rate limit message
                self._send_response(
                    note,
                    {
                        "text": "Error: Too many mentions. Please wait for a response before mentioning again.",
                        "images": [],
                    },
                )
            else:
                seen_authors.add(note.author.handle)
                rate_limited_notifications.append(note)

        return rate_limited_notifications

    def should_handle_notification(self, notification: Any) -> bool:
        return notification.record.text.strip().startswith("#!")

    def before_batch_processing(self, notifications: List[Any]) -> None:
        if notifications:
            try:
                self.sandbox = Sandbox(timeout=20)
                self.logger.info("Created sandbox for batch processing")
            except Exception as e:
                self.logger.error(f"Failed to create sandbox: {e}")
                self.sandbox = None

    def after_batch_processing(self, notifications: List[Any]) -> None:
        if self.sandbox:
            try:
                self.sandbox.kill()
                self.logger.info("Cleaned up sandbox after batch processing")
            except Exception as e:
                self.logger.error(f"Error cleaning up sandbox: {e}")
            finally:
                self.sandbox = None

    def process_notification(self, note: Any) -> Dict[str, Union[str, List]]:
        try:
            shebang, code = note.record.text.strip().split("\n", maxsplit=1)
        except ValueError:
            return {"text": "Error: Missing code after shebang.", "images": []}

        try:
            _, user, lang = [part for part in shebang.split() if part]
        except ValueError:
            return {"text": "Error: Invalid shebang.", "images": []}

        lang = lang.strip()
        if lang.startswith("#"):
            lang = lang[1:]

        if user != f"@{self.client.me.handle}":
            return {"text": "Error: Wrong interpreter.", "images": []}

        # Add context for Python code
        if lang == "python":
            code = self._prepare_python_code(note, code)

        self.logger.info(f"Executing {lang} code for {note.author.handle}")
        result = self._execute_code(code, lang)

        # Record in database
        executions.insert(
            dict(
                input=note.record.text,
                output=result.get("text", ""),
                author=note.author.handle,
                url=note.uri,
                ts=now(),
            )
        )

        return result

    def _prepare_python_code(self, note: Any, code: str) -> str:
        """Adds bootstrap code with reply context for Python execution"""
        if hasattr(note.record, "reply") and note.record.reply:
            try:
                resp = self.client.get_posts(
                    [note.record.reply.parent.uri, note.record.reply.root.uri]
                )

                posts = [
                    {"author": post.author.handle, "text": post.record.text}
                    for post in resp.posts
                ]
                payload = base64.b64encode(
                    json.dumps({"parent": posts[0], "root": posts[1]}).encode()
                )
            except Exception as e:
                self.logger.warning(f"Error getting reply context: {e}")
                payload = base64.b64encode(json.dumps(None).encode())
        else:
            payload = base64.b64encode(json.dumps(None).encode())

        bootstrap = self.BOOTSTRAP.format(
            payload=payload, HELLO_MESSAGE=self.HELLO_MESSAGE
        )
        return f"{bootstrap}\n{code}"

    def _execute_code(self, code: str, language: str) -> Dict[str, Union[str, List]]:
        sandbox_to_use = self.sandbox
        local_sandbox = False

        if not sandbox_to_use:
            try:
                sandbox_to_use = Sandbox(timeout=20)
                local_sandbox = True
                self.logger.info("Created new sandbox for single execution")
            except Exception as e:
                self.logger.error(f"Failed to create sandbox: {e}")
                return {"text": f"Error creating sandbox: {e}", "images": []}

        try:
            execution = sandbox_to_use.run_code(code, language=language, timeout=15)

            # Format output
            output = "\n".join(execution.logs.stdout + execution.logs.stderr)
            if execution.error:
                output += f"\n{execution.error.name}: {execution.error.value}"

            if len(output) > 300:
                rightmost_newline = output[:297].rfind("\n")
                rightmost_space = output[:297].rfind(" ")
                cutoff = max(rightmost_newline, rightmost_space)
                output = output[:cutoff] + "..."

            result = {"text": output.strip(), "images": []}

            # Extract images from results
            if hasattr(execution, "results") and execution.results:
                for result_item in execution.results[:4]:  # Limit to 4 images max
                    if hasattr(result_item, "png") and result_item.png:
                        result["images"].append(base64.b64decode(result_item.png))

            return result

        except Exception as e:
            self.logger.error(f"Code execution error: {e}")
            return {"text": f"Error: {e}", "images": []}
        finally:
            if local_sandbox and sandbox_to_use:
                try:
                    sandbox_to_use.kill()
                except Exception:
                    pass


def main() -> None:
    logger.info("Starting CodeExecutionBot")
    log_to_db("Starting CodeExecutionBot")

    bot = CodeExecutionBot(
        username=bsky_user,
        password=bsky_pass,
        poll_interval=30,
        timeout=ATPROTO_TIMEOUT,
    )

    try:
        bot.run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        log_to_db("Bot stopped by user")
    except Exception as e:
        error_msg = f"Bot crashed: {e}"
        logger.error(error_msg)
        log_to_db(error_msg)
        raise


if __name__ == "__main__":
    main()
