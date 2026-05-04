# efb-qq-image-fallback-middleware

EFB middleware that recovers QQ images which the go-cqhttp slave failed
to download, by fetching them from a hash-addressed fallback server that
you populate from PC's NTQQ image cache.

## What it does

1. The QQ slave fails to download some image, and emits a failure-text
   message containing the original URL.
2. This middleware detects the failure marker, extracts the 32-hex MD5
   hash out of the URL, and tries a synchronous GET against to fallback
   server (`{server_base}/{hash[:2]}/{hash[2:4]}/{hash}`).
3. **Hit**: the failure message is rewritten in place into a real
   `MsgType.Image` before it reaches the master. The user sees the real
   image arrive on time.
4. **Miss**: the failure message is rewritten into a placeholder image
   (a small grey PNG) so the master records an image-type message. The
   (hash, msg-uid, chat) is persisted in SQLite.
5. A background worker re-tries misses on a configurable schedule
   (default: 5 min, 1 h, 6 h, 1 d, 3 d, 7 d). On hit, it delivers a
   `Message` with `edit=True, edit_media=True` via
   `coordinator.send_message`, which tells the master to replace the
   placeholder with the real image (e.g. Telegram's `editMessageMedia`).
   If the optional local refresh API is enabled, your uploader can also
   trigger an immediate full refresh after uploading new files.
6. After the last scheduled attempt, the entry is dropped.

## Dependencies

- A working EFB installation with the QQ slave (`milkice.qq` + the
  go-cqhttp sub-plugin).
- Python 3.9+, `httpx`, `PyYAML`, `python-magic`.

## Install

```bash
pip install .
```

Then add to `~/.ehforwarderbot/profiles/<profile>/config.yaml`:

```yaml
middlewares:
  - qqimg.fallback
```

Create `~/.ehforwarderbot/profiles/<profile>/qqimg.fallback/config.yaml`
(see `config.example.yaml`).

Apply the slave patch:

```bash
cd /path/to/efb-qq-plugin-go-cqhttp
patch -p1 < slave_patch/msgdecorator_include_url.patch
pip install -e .
```

Restart EFB.

## Configuration

See `config.example.yaml`. The minimum required setting is
`server_base_url`. Everything else has sensible defaults.

The retry schedule is a plain list of offsets in seconds from the time
the failure was first enqueued. To disable background retries entirely
(only attempt the synchronous fetch on arrival), set it to `[]`.

### Local refresh API

Set `refresh_api_enabled: true` to listen on
`http://127.0.0.1:8765/refresh` by default. After your uploader finishes
uploading images to the fallback server, call either:

```bash
curl "http://127.0.0.1:8765/refresh"
curl -X POST "http://127.0.0.1:8765/refresh"
curl -X POST "http://127.0.0.1:8765/refresh" \
  -H "Content-Type: application/json" \
  -d '{"ids":["0123456789abcdef0123456789abcdef"]}'
```

The endpoint returns immediately with `accepted` or `already_running`.
The refresh itself runs in the background. `GET /refresh` and
`POST /refresh` with an empty body try every row currently in the
SQLite pending queue. `POST /refresh` with a JSON `ids` array only tries
pending rows whose hash matches one of the uploaded IDs. In both cases,
rows are grouped by hash so each hash is fetched at most once in that
refresh pass. Scheduled retries still remain as the fallback path.

If `refresh_api_token` is set, pass it as `?token=...`,
`X-Refresh-Token`, or `Authorization: Bearer ...`.

## Runtime data

- Pending queue: `~/.ehforwarderbot/profiles/<profile>/qqimg.fallback/queue.sqlite`

Safe to delete if you want to blow away in-flight retries. The
middleware re-creates it on startup.

## Inspecting the queue

```bash
sqlite3 ~/.ehforwarderbot/profiles/default/qqimg.fallback/queue.sqlite \
  "SELECT hash, msg_uid, attempts, datetime(next_try_at,'unixepoch') FROM pending ORDER BY next_try_at;"
```

## Limitations

- **Master must support media-edit**. Telegram (ETM) does. The framework
  sets `edit_media=True` and `edit=True`; the master is responsible for
  mapping that to `editMessageMedia` or its equivalent. If your master
  can't edit a media message, the placeholder will persist and the real
  image will never replace it. In that case set the schedule to `[]` so
  the placeholder isn't retried pointlessly.
- **Hash must be extractable from the URL**. gchatpic_new, offpic_new,
  and bare-hex URLs all work. NT-new URLs (`multimedia.nt.qq.com.cn`)
  don't embed the hash, but per the project design, those shouldn't be
  failing in the first place.
- **Retries have no jitter**. If your server goes down and many messages
  enqueue with the same `first_seen + delay`, they'll all tick at once.
  Fine at this scale (tens of retries per hour peak), but worth noting.

## Troubleshooting

**"loaded but disabled (server_base_url is empty)"**: you haven't
written the config yet. Check the log for the exact path it's looking
at, and create the file there.

**Placeholder appears but never gets replaced**: check the worker log.
Common causes: (1) server URL wrong or server down, (2) hash not on the
server yet (your PC sync hasn't seen this image), (3) master doesn't
support media-edit — enable trace logging on the master to confirm.

**Edits never arrive at master**: the worker log will show
`delivered edit ...` on success. If it's showing that but the master
isn't reflecting it, the master's edit plumbing is the issue, not this
middleware's.
