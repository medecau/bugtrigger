import base64
import datetime as dt
import json
import logging
import os
from time import sleep
from typing import Any, Dict, List, Optional, Set, Union

import dataset
import dotenv
from atproto import Client, Session, SessionEvent
from e2b_code_interpreter import Sandbox
from retry import retry

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("codebot")

dotenv.load_dotenv()

ATPROTO_TIMEOUT = 60

bsky_user = os.getenv("BSKY_USER")
bsky_pass = os.getenv("BSKY_PASS")

# Database setup - kept external to the bot class
db = dataset.connect("sqlite:///code_bot.db")
logs = db["logs"]
executions = db["executions"]


def log_to_db(msg: str) -> None:
    """Log message to database"""
    logs.insert(dict(msg=msg, ts=now()))


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


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


class BlueskyBot:
    """
    Generic Bluesky bot class that handles different types of notifications.
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

        # Client initialization
        self.client = self._init_client()

        # Handler configuration - maps notification types to their handler functions
        self.notification_handlers = {}

        # Standard logger for the bot
        self.logger = logging.getLogger("codebot.bot")

    def _get_session(self) -> Optional[str]:
        """Get saved session data if available"""
        try:
            with open(self.session_file, encoding="UTF-8") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def _save_session(self, session_string: str) -> None:
        """Save session data to file"""
        with open(self.session_file, "w", encoding="UTF-8") as f:
            f.write(session_string)

    def _on_session_change(self, event: SessionEvent, session: Session) -> None:
        """Handle session change events"""
        self.logger.info(f"Session changed: {event}")
        if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
            self.logger.info("Saving changed session")
            self._save_session(session.export())

    def _init_client(self) -> Client:
        """Initialize the Bluesky client with authentication"""
        client = Client()
        client.on_session_change(self._on_session_change)

        session_string = self._get_session()
        if session_string:
            self.logger.info("Reusing session")
            client.login(session_string=session_string)
        else:
            self.logger.info("Creating new session")
            client.login(self.username, self.password)

        return client

    @retry(delay=30, backoff=1.1, jitter=(5, 10), max_delay=300)
    def _fetch_notifications(self) -> List:
        """Fetch notifications from Bluesky API with retry"""
        response = self.client.app.bsky.notification.list_notifications(
            timeout=self.timeout
        )
        return response.notifications

    def _filter_notifications(self, notifications: List) -> Dict[str, List]:
        """
        Filter notifications and categorize them by handler type.

        Returns a dict with handler names as keys and lists of notifications as values.
        """
        # Base filtering for all notifications
        filtered = (n for n in notifications if not n.is_read)
        filtered = (n for n in filtered if n.reason in {"mention", "reply"})
        filtered = (n for n in filtered if not n.author.viewer.blocked_by)
        filtered = (n for n in filtered if not n.author.viewer.blocking)
        filtered = (n for n in filtered if not n.author.viewer.blocking_by_list)
        filtered = (n for n in filtered if not n.author.viewer.muted)
        filtered = (n for n in filtered if not n.author.viewer.muted_by_list)

        # Apply specific handlers' filters
        categorized = {}
        base_filtered = list(filtered)

        for handler_name, config in self.notification_handlers.items():
            filter_func = config.get("filter")
            if filter_func:
                matching_notes = [n for n in base_filtered if filter_func(n)]
                if matching_notes:
                    categorized[handler_name] = matching_notes

        return categorized

    @retry(delay=10, backoff=1.1, jitter=(5, 10), max_delay=600)
    def _update_seen(self, last_seen_at: str) -> None:
        """Mark notifications as seen with retry"""
        self.client.app.bsky.notification.update_seen(
            {"seen_at": last_seen_at}, timeout=self.timeout
        )

    def _send_response(self, note: Any, response: Dict[str, Union[str, List]]) -> None:
        """Send a response to a notification"""
        output = response.get("text", "")
        images = response.get("images", [])

        parent = {"cid": note.cid, "uri": note.uri}
        if note.record.reply:
            reply_to = {"root": note.record.reply.root, "parent": parent}
        else:  # this is the root post
            reply_to = {"root": parent, "parent": parent}

        try:
            if images:
                # Include images in the response if available
                self.client.send_images(text=output, images=images, reply_to=reply_to)
            else:
                # Send text-only response
                self.client.send_post(text=output, reply_to=reply_to)
        except Exception as e:
            self.logger.error(f"Error sending response: {e}")

    def add_notification_handler(self, name: str, filter_func, handler_func) -> None:
        """
        Add a new notification handler to the bot.

        Args:
            name: Unique identifier for this handler
            filter_func: Function that takes a notification and returns True if it should be handled
            handler_func: Function that processes the notification and returns a response dict
        """
        self.notification_handlers[name] = {
            "filter": filter_func,
            "handler": handler_func,
        }
        self.logger.info(f"Added notification handler: {name}")

    def run(self) -> None:
        """Main bot loop that processes notifications and dispatches to handlers"""
        self.logger.info("Starting bot loop")

        if not self.notification_handlers:
            self.logger.warning("No notification handlers registered")

        while True:
            last_seen_at = self.client.get_current_time_iso()

            # Fetch and categorize notifications
            notifications = self._fetch_notifications()
            categorized = self._filter_notifications(notifications)

            total_notifications = sum(len(notes) for notes in categorized.values())
            self.logger.info(
                f"Found {total_notifications} new notifications to process"
            )

            # Track seen authors to prevent spam
            seen_authors: Set[str] = set()

            # Process each category of notifications
            for handler_name, notes in categorized.items():
                handler = self.notification_handlers[handler_name]["handler"]
                self.logger.info(
                    f"Processing {len(notes)} notifications with handler: {handler_name}"
                )

                for note in notes:
                    # Rate limiting per user
                    if note.author.handle in seen_authors:
                        self.logger.info(
                            f"Skipping duplicate from {note.author.handle}"
                        )
                        response = {
                            "text": "Error: Too many mentions. Please wait for a response before mentioning again.",
                            "images": [],
                        }
                    else:
                        seen_authors.add(note.author.handle)
                        try:
                            response = handler(self, note)
                        except Exception as e:
                            self.logger.error(f"Handler error for {handler_name}: {e}")
                            response = {
                                "text": f"Error processing your request: {str(e)}",
                                "images": [],
                            }

                    # Send the response
                    self._send_response(note, response)

            # Mark notifications as seen
            if total_notifications > 0:
                self._update_seen(last_seen_at)

            # Wait before next poll
            sleep(self.poll_interval)


class CodeExecutionBot(BlueskyBot):
    """
    Specialized Bluesky bot for executing code from posts.
    """

    def __init__(
        self,
        username: str,
        password: str,
        session_file: str = "session.txt",
        poll_interval: int = 30,
        timeout: int = 60,
    ):
        # Initialize logger before super().__init__ which calls _init_client
        self.logger = logging.getLogger("codebot.execution")

        super().__init__(username, password, session_file, poll_interval, timeout)

        # Register the code execution handler
        self.add_notification_handler(
            name="code_execution",
            filter_func=lambda n: n.record.text.strip().startswith("#!"),
            handler_func=self._handle_code_execution,
        )

    def _execute_code(self, code: str, language: str) -> Dict[str, Union[str, List]]:
        """Execute code in a sandbox and return the results"""
        try:
            sbx = Sandbox(timeout=20)
            execution = sbx.run_code(code, language=language, timeout=15)
            sbx.kill()

            # Process standard output
            output = "\n".join(execution.logs.stdout + execution.logs.stderr)
            if execution.error:
                output += f"\n{execution.error.name}: {execution.error.value}"

            if len(output) > 300:
                rightmost_newline = output[:297].rfind("\n")
                rightmost_space = output[:297].rfind(" ")
                cutoff = max(rightmost_newline, rightmost_space)
                output = output[:cutoff] + "..."

            # Return result and images
            result = {"text": output.strip(), "images": []}

            # Extract images from results (if any)
            if hasattr(execution, "results") and execution.results:
                for result_item in execution.results[:4]:  # Limit to 4 images max
                    if hasattr(result_item, "png") and result_item.png:
                        # Add the image data to results
                        result["images"].append(base64.b64decode(result_item.png))

            return result

        except Exception as e:
            self.logger.error(f"Code execution error: {e}")
            return {"text": f"Error: {e}", "images": []}

    def _handle_code_execution(self, bot, note: Any) -> Dict[str, Union[str, List]]:
        """Handle a code execution notification"""
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

        # Handle Python bootstrap for context
        if lang == "python":
            if note.reason == "reply":
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

            bootstrap = BOOTSTRAP.format(payload=payload, HELLO_MESSAGE=HELLO_MESSAGE)
            code = f"{bootstrap}\n{code}"

        self.logger.info(f"Executing {lang} code for {note.author.handle}")

        # Execute the code
        result = self._execute_code(code, lang)

        # Record execution in database (done outside the class)
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


def main() -> None:
    """Initialize and run the bot"""
    logger.info("Starting CodeExecutionBot")

    # Log startup to database
    log_to_db("Starting CodeExecutionBot")

    # Create and run the specialized code execution bot
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
