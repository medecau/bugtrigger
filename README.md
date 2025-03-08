# bugtrigger.bsky.social

it's a bluesky bot that executes code from posts that mention the bot

example post:

```
#! @bugtrigger.bsky.social python
import base64
print(base64.b64decode(b'aGVsbG8gd29ybGQ=').decode('utf-8'))
```
