import datetime as dt
import os
from time import sleep

import dataset
import dotenv
from atproto import Client
from e2b_code_interpreter import Sandbox
from functools import lru_cache

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
def run_code(code: str, lang:str) -> str:
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


def main() -> None:
    client = Client()
    client.login(bsky_user, bsky_pass)

    while True:
        last_seen_at = client.get_current_time_iso()

        try:
            response = client.app.bsky.notification.list_notifications(timeout=ATPROTO_TIMEOUT)
        except Exception as e:
            log(msg=f"Error: {e}")
            sleep(60)
            continue

        unread = [
            notification
            for notification in response.notifications
            if not notification.is_read
        ]
        mentions = [
            notification for notification in unread if notification.reason == "mention"
        ]

        log(msg=f"Found {len(mentions)} new mentions")

        seen = set()
        for notification in mentions:
            if (
                notification.author.viewer.blocked_by
                or notification.author.viewer.blocking
                or notification.author.viewer.blocking_by_list
                or notification.author.viewer.muted
                or notification.author.viewer.muted_by_list
            ):
                log(msg=f"Skipping blocked/muted user: {notification.author.handle}")
                continue

            elif notification.author.handle in seen:
                log(msg=f"Skipping duplicate mention from {notification.author.handle}")
                output = "Error: Too many mentions. Please wait for a response before mentioning again."

            else:
                seen.add(notification.author.handle)

                code = notification.record.text
                shebang = code.split("\n")[0]
                _, user, lang = shebang.split()
                lang = lang.strip()
                if user != f'@{client.me.handle}':
                    log(msg=f"Skipping wrong user: {user}")
                    output = "Error: Wrong interpreter."
                else:
                    try:
                        output = run_code(code, lang)
                    except Exception as e:
                        log(msg=f"Error: {e}")
                        output = f"Error: {e}"

            executions.insert(
                dict(
                    input=notification.record.text,
                    output=output,
                    author=notification.author.handle,
                    url=notification.uri,
                    ts=now(),
                )
            )

            parent = {"cid": notification.cid, "uri": notification.uri}
            if notification.record.reply:
                reply_to = {"root": notification.record.reply.root, "parent": parent}
            else:  # this is the root post
                reply_to = {"root": parent, "parent": parent}

            client.send_post(text=output, reply_to=reply_to, timeout=ATPROTO_TIMEOUT)


        client.app.bsky.notification.update_seen({"seen_at": last_seen_at}, timeout=ATPROTO_TIMEOUT)
        print("Sleeping...")

        if seen:
            sleep(60)
        else:
            sleep(20)


if __name__ == "__main__":
    main()
