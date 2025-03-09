import base64
import datetime as dt
import json
import os
from functools import lru_cache
from time import sleep

import dataset
import dotenv
from atproto import Client, Session, SessionEvent
from e2b_code_interpreter import Sandbox
from retry import retry

dotenv.load_dotenv()

ATPROTO_TIMEOUT = 60

bsky_user = os.getenv("BSKY_USER")
bsky_pass = os.getenv("BSKY_PASS")


db = dataset.connect("sqlite:///code_bot.db")
logs = db["logs"]
executions = db["executions"]


def log(msg: str):
    logs.insert(dict(msg=msg, ts=now()))


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


@lru_cache
def run_code(code: str, lang: str) -> str:
    sbx = Sandbox(timeout=10)
    execution = sbx.run_code(code, language=lang, timeout=2)
    sbx.kill()

    output = "\n".join(execution.logs.stdout + execution.logs.stderr)
    if execution.error:
        output += f"\n{execution.error.name}: {execution.error.value}"

    if len(output) > 300:
        rightmost_newline = output[:297].rfind("\n")
        rightmost_space = output[:297].rfind(" ")
        cutoff = max(rightmost_newline, rightmost_space)
        output = output[:cutoff] + "..."

    return output


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


def handle_note(client, note):
    shebang, code = note.record.text.strip().split("\n", maxsplit=1)

    try:
        _, user, lang = [part for part in shebang.split() if part]
    except ValueError:
        return "Error: Invalid shebang."

    lang = lang.strip()
    if lang.startswith("#"):
        lang = lang[1:]

    if lang == "python":
        if note.reason == "reply":
            resp = client.get_posts(
                [note.record.reply.parent.uri, note.record.reply.root.uri]
            )

            posts = [
                {"author": post.author.handle, "text": post.record.text}
                for post in resp.posts
            ]
            payload = base64.b64encode(
                json.dumps({"parent": posts[0], "root": posts[1]}).encode()
            )
        else:
            payload = base64.b64encode(json.dumps(None).encode())
        boostrap = BOOTSTRAP.format(payload=payload, HELLO_MESSAGE=HELLO_MESSAGE)
        code = f"{boostrap}\n{code}"

    if user != f"@{client.me.handle}":
        return "Error: Wrong interpreter."

    try:
        output = run_code(code, lang)
    except Exception as e:
        return f"Error: {e}"

    executions.insert(
        dict(
            input=note.record.text,
            output=output,
            author=note.author.handle,
            url=note.uri,
            ts=now(),
        )
    )

    return output.strip()


def get_session() -> str | None:
    try:
        with open("session.txt", encoding="UTF-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


def save_session(session_string: str) -> None:
    with open("session.txt", "w", encoding="UTF-8") as f:
        f.write(session_string)


def on_session_change(event: SessionEvent, session: Session) -> None:
    print("Session changed:", event, repr(session))
    if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
        print("Saving changed session")
        save_session(session.export())


def init_client() -> Client:
    client = Client()
    client.on_session_change(on_session_change)

    session_string = get_session()
    if session_string:
        print("Reusing session")
        client.login(session_string=session_string)
    else:
        print("Creating new session")
        client.login(bsky_user, bsky_pass)

    return client


@retry(delay=30, backoff=1.1, jitter=(5, 10), max_delay=300)
def fecth_notifications(client: Client) -> list:
    response = client.app.bsky.notification.list_notifications(timeout=ATPROTO_TIMEOUT)
    return response.notifications


def filter_notifications(notifications: list) -> list:
    _ = (n for n in notifications if not n.is_read)
    _ = (n for n in _ if n.reason in {"mention", "reply"})
    _ = (n for n in _ if not n.author.viewer.blocked_by)
    _ = (n for n in _ if not n.author.viewer.blocking)
    _ = (n for n in _ if not n.author.viewer.blocking_by_list)
    _ = (n for n in _ if not n.author.viewer.muted)
    _ = (n for n in _ if not n.author.viewer.muted_by_list)
    _ = (n for n in _ if n.record.text.strip().startswith("#!"))
    return list(_)


@retry(delay=10, backoff=1.1, jitter=(5, 10), max_delay=600)
def update_seen(client: Client, last_seen_at: str) -> None:
    client.app.bsky.notification.update_seen(
        {"seen_at": last_seen_at}, timeout=ATPROTO_TIMEOUT
    )


def main() -> None:
    client = init_client()
    while True:
        last_seen_at = client.get_current_time_iso()

        notifications = fecth_notifications(client)
        notifications = filter_notifications(notifications)

        log(msg=f"Found {len(notifications)} new mentions")

        seen = set()
        for note in notifications:
            if note.author.handle in seen:
                log(msg=f"Skipping duplicate mention from {note.author.handle}")
                output = "Error: Too many mentions. Please wait for a response before mentioning again."

            else:
                seen.add(note.author.handle)
                output = handle_note(client, note)

            parent = {"cid": note.cid, "uri": note.uri}
            if note.record.reply:
                reply_to = {"root": note.record.reply.root, "parent": parent}
            else:  # this is the root post
                reply_to = {"root": parent, "parent": parent}

            try:
                client.send_post(text=output, reply_to=reply_to)
            except Exception as e:
                log(msg=f"Error: {e}")

        update_seen(client, last_seen_at)

        sleep(30)


if __name__ == "__main__":
    main()
