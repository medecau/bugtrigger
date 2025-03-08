import datetime as dt
import os
from functools import lru_cache
from time import sleep

import dataset
import dotenv
from atproto import Client, Session, SessionEvent
from e2b_code_interpreter import Sandbox

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
    sbx = Sandbox()
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


def main() -> None:
    client = init_client()
    while True:
        last_seen_at = client.get_current_time_iso()

        try:
            response = client.app.bsky.notification.list_notifications(
                timeout=ATPROTO_TIMEOUT
            )
        except Exception as e:
            log(msg=f"Error: {e}")
            sleep(60)
            continue

        allowed_reasons = {"mention", "reply"}
        unread = [note for note in response.notifications if not note.is_read]
        notifications = [note for note in unread if note.reason in allowed_reasons]

        log(msg=f"Found {len(notifications)} new mentions")

        seen = set()
        for note in notifications:
            if (
                note.author.viewer.blocked_by
                or note.author.viewer.blocking
                or note.author.viewer.blocking_by_list
                or note.author.viewer.muted
                or note.author.viewer.muted_by_list
            ):
                log(msg=f"Skipping blocked/muted user: {note.author.handle}")
                continue

            elif note.author.handle in seen:
                log(msg=f"Skipping duplicate mention from {note.author.handle}")
                output = "Error: Too many mentions. Please wait for a response before mentioning again."

            else:
                seen.add(note.author.handle)

                code = note.record.text.strip()

                if not code.startswith("#!"):
                    log(msg="Error: Missing shebang")
                    continue

                shebang = code.split("\n")[0]
                try:
                    _, user, lang = [part for part in shebang.split() if part]
                    lang = lang.strip()
                    if lang.startswith("#"):
                        lang = lang[1:]

                    if user != f"@{client.me.handle}":
                        log(msg=f"Skipping wrong user: {user}")
                        output = "Error: Wrong interpreter."
                    else:
                        try:
                            output = run_code(code, lang)
                        except Exception as e:
                            log(msg=f"Error: {e}")
                            output = f"Error: {e}"
                except ValueError:
                    log(msg=f"Error: Invalid shebang: {shebang}")
                    output = "Error: Invalid shebang."

            executions.insert(
                dict(
                    input=note.record.text,
                    output=output,
                    author=note.author.handle,
                    url=note.uri,
                    ts=now(),
                )
            )

            parent = {"cid": note.cid, "uri": note.uri}
            if note.record.reply:
                reply_to = {"root": note.record.reply.root, "parent": parent}
            else:  # this is the root post
                reply_to = {"root": parent, "parent": parent}

            try:
                client.send_post(text=output, reply_to=reply_to)
            except Exception as e:
                log(msg=f"Error: {e}")

        for _ in range(5):
            try:
                client.app.bsky.notification.update_seen(
                    {"seen_at": last_seen_at}, timeout=ATPROTO_TIMEOUT
                )
                break
            except Exception as e:
                log(msg=f"Error: {e}")
                sleep(60)

        if seen:
            sleep(60)
        else:
            sleep(20)


if __name__ == "__main__":
    main()
