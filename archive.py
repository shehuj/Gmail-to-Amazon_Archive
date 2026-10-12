#!/usr/bin/env python3
"""Archive Gmail messages to Amazon S3, verify them, and (optionally) trash them.

Modes (run in this order):
  archive  Read-only Gmail access. Downloads each message in raw RFC 822 form
           (headers, body and attachments) and uploads it to S3 as a .eml file.
  verify   Downloads every archived object from S3 and checks its SHA-256
           against the local manifest.
  cleanup  Moves *verified* messages to Gmail Trash. Requires --confirm and a
           typed confirmation (or --yes). Never empties Trash.
  import   Imports a Google Takeout export (.mbox, or the .zip/.tgz archives) into
           S3 without using the Gmail API quota. Use it for the bulk of a large
           mailbox; `archive` then picks up new mail.
  login    Authorizes both Gmail scopes in a browser and saves the token files,
           e.g. to copy into GitHub Actions secrets.

Progress is tracked in a local SQLite database so every mode is resumable.
"""

import argparse
import base64
import email.errors
import email.header
import email.parser
import email.utils
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import sys
import tarfile
import threading
import zipfile
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone

import boto3
import httplib2
from botocore.config import Config
from botocore.exceptions import ClientError
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPE_READONLY = "https://www.googleapis.com/auth/gmail.readonly"
SCOPE_MODIFY = "https://www.googleapis.com/auth/gmail.modify"

# Separate token files so the read-only token can never be used to trash mail.
TOKEN_READONLY = "token_readonly.json"
TOKEN_MODIFY = "token_modify.json"

# Recorded when Gmail returns 404 (message deleted since it was listed); not retried.
NOT_FOUND = "not found in Gmail"

log = logging.getLogger("archive")


# --------------------------------------------------------------------------- #
# State database
# --------------------------------------------------------------------------- #

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id            TEXT PRIMARY KEY,
    thread_id     TEXT,
    internal_date INTEGER,
    s3_key        TEXT,
    sha256        TEXT,
    size          INTEGER,
    archived_at   TEXT,
    verified_at   TEXT,
    verify_error  TEXT,
    trashed_at    TEXT,
    error         TEXT,
    message_id_header TEXT,
    duplicate_of  TEXT,
    storage_class TEXT,
    source        TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def open_db(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    # Add columns introduced after the first release to existing databases.
    columns = {r["name"] for r in db.execute("PRAGMA table_info(messages)")}
    for column in ("message_id_header", "duplicate_of", "storage_class", "source"):
        if column not in columns:
            db.execute(f"ALTER TABLE messages ADD COLUMN {column} TEXT")
    db.executescript(
        "CREATE INDEX IF NOT EXISTS idx_messages_sha256 ON messages(sha256);"
        "CREATE INDEX IF NOT EXISTS idx_messages_duplicate_of ON messages(duplicate_of);"
    )
    return db


def meta_get(db, key):
    row = db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def meta_set(db, key, value):
    if value is None:
        db.execute("DELETE FROM meta WHERE key = ?", (key,))
    else:
        db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
    db.commit()


# --------------------------------------------------------------------------- #
# Clients
# --------------------------------------------------------------------------- #

def gmail_credentials(credentials_file, scope, token_file):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, [scope])
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError as e:
                sys.exit(
                    f"Gmail token in '{token_file}' could not be refreshed ({e}). Run "
                    "'python archive.py --mode login' to authorize again."
                )
        elif os.environ.get("CI"):
            sys.exit(
                f"No valid Gmail token in '{token_file}' and no browser is available in CI. "
                "Run 'python archive.py --mode login' locally and store the token file "
                "contents as a GitHub secret."
            )
        else:
            if not os.path.exists(credentials_file):
                sys.exit(
                    f"OAuth client file '{credentials_file}' not found. "
                    "Create a Desktop OAuth client in Google Cloud Console and save it there."
                )
            with open(credentials_file) as f:
                client_type = json.load(f)
            if "installed" not in client_type:
                kind = client_type.get("type") or ", ".join(client_type) or "empty"
                sys.exit(
                    f"'{credentials_file}' is not a Desktop OAuth client (found: {kind}). "
                    "In Google Cloud Console use Credentials > Create credentials > "
                    "OAuth client ID > Desktop app, and save that JSON here. "
                    "Service account keys cannot access a personal Gmail mailbox."
                )
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, [scope])
            creds = flow.run_local_server(port=0)
        fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(creds.to_json())
    return creds


class ThreadLocalGmail:
    """One Gmail service per thread: the underlying httplib2 client is not thread-safe."""

    def __init__(self, creds):
        self.creds = creds
        self.local = threading.local()

    def get(self):
        if not hasattr(self.local, "service"):
            self.local.service = build("gmail", "v1", credentials=self.creds, cache_discovery=False)
        return self.local.service


def run_parallel(fn, items, workers, deadline=None):  # items: list or iterator
    """Yield (item, result, error) for fn(item) as each finishes, up to `workers` at once.

    Keeps at most 2 x workers tasks in flight, so workers never wait on a slow batch and
    a huge item list doesn't create every future up front. Stops submitting new work
    once `deadline` (a time.monotonic() value) has passed.
    """
    pool = ThreadPoolExecutor(max_workers=workers)
    in_flight = {}
    pending = iter(items)
    submitted = 0
    exhausted = False
    try:
        while True:
            while not exhausted and len(in_flight) < workers * 2:
                if deadline and time.monotonic() >= deadline:
                    total = f" of {len(items)}" if hasattr(items, "__len__") else ""
                    log.warning("--max-runtime reached; stopping after %d%s. "
                                "Run again to continue.", submitted, total)
                    exhausted = True
                    break
                item = next(pending, None)
                if item is None:
                    exhausted = True
                    break
                in_flight[pool.submit(fn, item)] = item
                submitted += 1
            if not in_flight:
                return
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                item = in_flight.pop(fut)
                exc = fut.exception()
                yield item, (None if exc else fut.result()), exc
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


class Progress:
    """Logs throughput and ETA at most every `interval` seconds."""

    def __init__(self, total, label, interval=30):
        self.total, self.label, self.interval = total, label, interval
        self.start = self.last = time.monotonic()

    def log(self, i, failed):
        t = time.monotonic()
        if t - self.last < self.interval and i != self.total:
            return
        self.last = t
        elapsed = t - self.start
        rate = i / elapsed if elapsed else 0
        eta = (self.total - i) / rate if rate else 0
        log.info("%s: %d/%d (failed %d) %.1f msg/s, ETA %dh%02dm",
                 self.label, i, self.total, failed, rate, eta // 3600, eta % 3600 // 60)


def s3_client(args):
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    config = Config(
        max_pool_connections=max(10, args.workers + 2),
        retries={"max_attempts": 8, "mode": "adaptive"},
    )
    return session.client("s3", config=config)


def check_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        sys.exit(f"Cannot access bucket '{bucket}': {e}")


class GmailRateLimiter:
    """Spaces Gmail requests evenly across all worker threads and adapts to the quota.

    Gmail's per-user quota is measured in units per minute (6,000 by default for new
    projects), and downloading a full raw message costs far more units than metadata
    calls, so the safe rate can't be known up front. The limiter learns it:

    - On a quota error, every worker pauses together and the rate drops by 30%.
      Errors from requests already in flight during that pause are ignored, so one
      burst counts as one event.
    - After each minute without quota errors, the rate rises by 5%, up to `max_rate`.
    - The rate reached is saved in the state database, and the next run starts there.
    """

    MIN_RATE = 0.2

    def __init__(self, rate, max_rate=None):
        self.rate = float(rate)
        self.max_rate = float(max_rate or rate)
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.paused_until = 0.0
        self.ignore_errors_until = 0.0
        self.last_change = time.monotonic()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            if self.rate < self.max_rate and now - self.last_change >= 60:
                self.rate = min(self.max_rate, self.rate * 1.05)
                self.last_change = now
            slot = max(now, self.next_slot, self.paused_until)
            self.next_slot = slot + 1 / self.rate
        if slot > now:
            time.sleep(slot - now)

    def quota_exceeded(self, pause):
        """Pause all workers for `pause` seconds and lower the rate (once per burst)."""
        with self.lock:
            now = time.monotonic()
            if now < self.ignore_errors_until:
                return  # a request sent before the last pause; already handled
            self.paused_until = now + pause
            # Requests in flight finish within a few seconds of the pause starting.
            self.ignore_errors_until = now + pause + 5
            self.rate = max(self.MIN_RATE, self.rate * 0.7)
            self.last_change = now + pause
            rate = self.rate
        log.warning("Gmail quota reached; pausing all workers for %.0fs, then %.2f requests/s",
                    pause, rate)


# Replaced by configure_gmail_rate() with the --rate setting and the learned rate.
gmail_limiter = GmailRateLimiter(40)


def configure_gmail_rate(db, args):
    """Start at the rate the last run ended at (if any), capped by --rate."""
    global gmail_limiter
    learned = meta_get(db, "gmail_rate")
    start = min(args.rate, float(learned) * 1.1) if learned else args.rate
    gmail_limiter = GmailRateLimiter(start, max_rate=args.rate)
    log.info("Gmail rate: starting at %.2f requests/s%s (max %.0f)", start,
             " (learned from previous runs)" if learned else "", args.rate)


def save_gmail_rate(db):
    meta_set(db, "gmail_rate", f"{gmail_limiter.rate:.3f}")


def is_quota_error(e):
    content = (e.content or b"").lower()
    return e.resp.status == 429 or (
        e.resp.status == 403
        and (b"ratelimitexceeded" in content or b"quota exceeded" in content)
    )


def gmail_call(request, retries=6, quota_retries=12):
    """Execute a Gmail API request through the shared rate limiter.

    Quota errors pause all workers (up to a minute, since Gmail's quota is per minute)
    and are retried up to `quota_retries` times. 5xx and network errors use
    exponential backoff with jitter, up to `retries` times.
    """
    errors = quota_errors = 0
    while True:
        gmail_limiter.acquire()
        try:
            return request.execute()
        except HttpError as e:
            if is_quota_error(e):
                quota_errors += 1
                if quota_errors > quota_retries:
                    raise
                gmail_limiter.quota_exceeded(min(10 * 2 ** (quota_errors - 1), 60) + random.random())
                continue
            if e.resp.status not in (500, 502, 503, 504) or errors >= retries:
                raise
            reason = f"HTTP {e.resp.status}"
        except (OSError, httplib2.HttpLib2Error) as e:  # timeouts, resets, DNS, TLS
            if errors >= retries:
                raise
            reason = type(e).__name__
        errors += 1
        delay = min(2 ** errors, 32) + random.random()
        log.warning("Gmail API %s, retrying in %.0fs", reason, delay)
        time.sleep(delay)


# --------------------------------------------------------------------------- #
# archive
# --------------------------------------------------------------------------- #

def list_messages(gmail, db, query, relist=False):
    """Add message IDs from Gmail to the messages table.

    The first listing walks every page and can be resumed via the saved page token.
    After it completes, later runs list newest-first and stop at the first page with no
    new IDs, so new mail is picked up quickly. `relist` forces a full listing again
    (e.g. after importing old mail, which Gmail lists by its original date).
    """
    if meta_get(db, "listing_query") != query:
        meta_set(db, "listing_query", query)
        meta_set(db, "listing_page_token", None)
        meta_set(db, "listing_complete", None)
    incremental = meta_get(db, "listing_complete") == query and not relist
    page_token = None if incremental else meta_get(db, "listing_page_token")
    listed = new = 0
    last_log = time.monotonic()
    while True:
        resp = gmail_call(
            gmail.users().messages().list(
                userId="me", q=query or None, pageToken=page_token, maxResults=500
            )
        )
        msgs = resp.get("messages", [])
        before = db.total_changes
        db.executemany(
            "INSERT OR IGNORE INTO messages (id, thread_id) VALUES (?, ?)",
            [(m["id"], m.get("threadId")) for m in msgs],
        )
        page_new = db.total_changes - before
        listed += len(msgs)
        new += page_new
        page_token = resp.get("nextPageToken")
        if not incremental:
            meta_set(db, "listing_page_token", page_token)  # also commits
        db.commit()
        if time.monotonic() - last_log >= 10:
            log.info("Listed %d message IDs (%d new)", listed, new)
            last_log = time.monotonic()
        if not page_token or (incremental and page_new == 0):
            break
    meta_set(db, "listing_complete", query)
    log.info("Listing done: %d message IDs checked, %d new", listed, new)


def s3_key_for(prefix, msg_id, internal_date_ms):
    dt = datetime.fromtimestamp(internal_date_ms / 1000, tz=timezone.utc)
    return f"{prefix.rstrip('/')}/{dt:%Y/%m}/{msg_id}.eml"


def deadline_for(args):
    return time.monotonic() + args.max_runtime * 60 if args.max_runtime else None


class DedupeIndex:
    """Thread-safe index of archived content, used to skip duplicate messages.

    A message is a duplicate if another Gmail message with the same SHA-256 (identical
    raw content) or, unless disabled, the same Message-ID header was already archived.
    """

    def __init__(self, db, by_message_id):
        self.by_message_id = by_message_id
        self.lock = threading.Lock()
        self.sha = {}
        self.mid = {}
        for r in db.execute(
            "SELECT id, s3_key, sha256, message_id_header FROM messages "
            "WHERE archived_at IS NOT NULL AND duplicate_of IS NULL"
        ):
            self.sha[r["sha256"]] = (r["id"], r["s3_key"])
            if r["message_id_header"]:
                self.mid[r["message_id_header"]] = (r["id"], r["s3_key"])

    def claim(self, msg_id, digest, message_id, key):
        """Return (original_id, original_key) if a duplicate, else register this message."""
        with self.lock:
            hit = self.sha.get(digest)
            if not hit and self.by_message_id and message_id:
                hit = self.mid.get(message_id)
            if hit and hit[0] != msg_id:
                return hit
            self.sha[digest] = (msg_id, key)
            if message_id:
                self.mid[message_id] = (msg_id, key)
            return None

    def release(self, msg_id, digest, message_id):
        """Forget a claim whose upload failed, so a later copy can be uploaded instead."""
        with self.lock:
            if self.sha.get(digest, (None,))[0] == msg_id:
                del self.sha[digest]
            if message_id and self.mid.get(message_id, (None,))[0] == msg_id:
                del self.mid[message_id]


def message_id_header(raw):
    headers = email.parser.BytesHeaderParser().parsebytes(raw)
    value = headers.get("Message-ID") or headers.get("Message-Id") or ""
    return str(value).strip() or None


def s3_has_object(s3, bucket, key, digest):
    """True if `key` already exists in S3 with the same SHA-256 (e.g. from a previous run)."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return head.get("Metadata", {}).get("sha256") == digest


def store_message(args, s3, dedupe, msg_id, raw, thread_id, internal_date, labels,
                  force_upload=False):
    """Upload one raw message to S3 unless it is a duplicate or already there.

    Returns a dict whose "outcome" is "uploaded", "in_s3" (identical object already
    there) or "duplicate" (same content as another archived message; not uploaded).
    `force_upload` skips the "already in S3" check, for objects that failed verification.
    """
    digest = hashlib.sha256(raw).hexdigest()
    key = s3_key_for(args.prefix, msg_id, internal_date)
    message_id = message_id_header(raw)
    result = dict(thread_id=thread_id, internal_date=internal_date, key=key,
                  sha256=digest, size=len(raw), message_id=message_id, duplicate_of=None,
                  storage_class=None)

    original = dedupe.claim(msg_id, digest, message_id, key)
    if original:
        result.update(outcome="duplicate", duplicate_of=original[0], key=original[1])
        return result

    try:
        if not force_upload and s3_has_object(s3, args.bucket, key, digest):
            result["outcome"] = "in_s3"
            return result

        put = dict(
            Bucket=args.bucket,
            Key=key,
            Body=raw,
            ContentType="message/rfc822",
            ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode(),
            Metadata={
                "gmail-id": msg_id,
                "gmail-thread-id": thread_id or "",
                "gmail-internal-date": str(internal_date),
                "gmail-labels": metadata_ascii(",".join(labels))[:1500],
                "sha256": digest,
            },
        )
        if args.kms_key_id:
            put.update(ServerSideEncryption="aws:kms", SSEKMSKeyId=args.kms_key_id)
        else:
            put.update(ServerSideEncryption="AES256")
        if args.storage_class:
            put["StorageClass"] = args.storage_class
        s3.put_object(**put)
        result["storage_class"] = args.storage_class or "STANDARD"
    except Exception:
        dedupe.release(msg_id, digest, message_id)
        raise
    result["outcome"] = "uploaded"
    return result


def metadata_ascii(value):
    """S3 user metadata must be ASCII; Takeout label names can contain any characters."""
    return value.encode("ascii", "backslashreplace").decode("ascii")


def archive_one(args, s3, gmail, dedupe, msg_id, force_upload=False):
    """Download one message from Gmail and store it in S3. Runs in a worker thread."""
    msg = gmail_call(gmail.get().users().messages().get(
        userId="me", id=msg_id, format="raw", fields="id,threadId,internalDate,labelIds,raw"))
    raw = base64.urlsafe_b64decode(msg["raw"])
    return store_message(args, s3, dedupe, msg_id, raw, msg.get("threadId"),
                         int(msg.get("internalDate", 0)), msg.get("labelIds", []), force_upload)


def record_stored(db, msg_id, r, source):
    """Record a store_message() result in the state database (main thread only)."""
    db.execute(
        "UPDATE messages SET thread_id=COALESCE(?, thread_id), internal_date=?, s3_key=?, "
        "sha256=?, size=?, message_id_header=?, duplicate_of=?, archived_at=?, error=NULL, "
        "verified_at=NULL, verify_error=NULL, storage_class=COALESCE(?, storage_class), "
        "source=? WHERE id=?",
        (r["thread_id"], r["internal_date"], r["key"], r["sha256"], r["size"],
         r["message_id"], r["duplicate_of"], now(), r["storage_class"], source, msg_id),
    )


def requeue_orphaned_duplicates(db):
    """A duplicate is only safe while its original is archived. If the original's upload
    failed (or the run stopped first), put the duplicate back in the queue."""
    orphans = db.execute(
        "UPDATE messages SET archived_at=NULL, duplicate_of=NULL, s3_key=NULL, verified_at=NULL "
        "WHERE trashed_at IS NULL AND duplicate_of IN "
        "(SELECT id FROM messages WHERE archived_at IS NULL)"
    ).rowcount
    db.commit()
    if orphans:
        log.warning("%d duplicates re-queued because their original is not archived", orphans)


def cmd_archive(args):
    db = open_db(args.db)
    s3 = s3_client(args)
    check_bucket(s3, args.bucket)
    creds = gmail_credentials(args.credentials, SCOPE_READONLY, TOKEN_READONLY)
    configure_gmail_rate(db, args)

    list_messages(build("gmail", "v1", credentials=creds, cache_discovery=False), db,
                  args.query, args.relist)

    sql = (
        "SELECT id, verify_error FROM messages "
        "WHERE (archived_at IS NULL OR verify_error IS NOT NULL) AND trashed_at IS NULL "
        f"AND (error IS NULL OR error != '{NOT_FOUND}') ORDER BY id"
    )
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    rows = db.execute(sql).fetchall()
    pending = [r["id"] for r in rows]
    # The S3 copy of these failed verification, so it must be replaced even though it exists.
    reupload = {r["id"] for r in rows if r["verify_error"]}
    log.info("%d messages to archive with %d workers", len(pending), args.workers)

    gmail = ThreadLocalGmail(creds)
    dedupe = DedupeIndex(db, by_message_id=not args.no_dedupe_message_id)
    progress = Progress(len(pending), "Archive")
    counts = {"uploaded": 0, "in_s3": 0, "duplicate": 0}
    failed = 0
    results = run_parallel(lambda m: archive_one(args, s3, gmail, dedupe, m, m in reupload),
                           pending, args.workers, deadline_for(args))
    try:
        for i, (msg_id, r, err) in enumerate(results, 1):
            if err is None:
                record_stored(db, msg_id, r, "gmail-api")
                counts[r["outcome"]] += 1
                if r["outcome"] == "duplicate":
                    log.debug("Duplicate %s of %s; not uploaded", msg_id, r["duplicate_of"])
            else:
                if isinstance(err, HttpError) and err.resp.status == 404:
                    msg = NOT_FOUND  # deleted from Gmail since listing; nothing to archive
                else:
                    msg = f"{type(err).__name__}: {err}"[:500]
                db.execute("UPDATE messages SET error=? WHERE id=?", (msg, msg_id))
                failed += 1
                log.error("Failed %s: %s", msg_id, msg)
            if i % 200 == 0:
                db.commit()
            progress.log(i, failed)
    finally:
        # Runs on Ctrl+C / SIGTERM too, so recorded progress is never lost.
        results.close()  # stop the worker pool
        db.commit()
        requeue_orphaned_duplicates(db)
        save_gmail_rate(db)

    log.info(
        "Archive finished: %d uploaded, %d already in S3, %d duplicates skipped, %d failed",
        counts["uploaded"], counts["in_s3"], counts["duplicate"], failed,
    )
    print_summary(db)


# --------------------------------------------------------------------------- #
# import (Google Takeout)
# --------------------------------------------------------------------------- #

# Each message in a Takeout mbox starts with "From <X-GM-MSGID>@xxx <date>". The
# number is the Gmail message ID in decimal; the Gmail API uses the same ID in hex.
TAKEOUT_FROM = re.compile(rb"^From (\d+)@xxx (.*?)\r?\n?$")
# Labels whose messages the API listing excludes, so the import skips them too.
SKIP_LABELS = {"spam", "trash", "chat", "chats"}
MBOXRD_ESCAPE = re.compile(rb"^>(>*From )")


def takeout_files(paths):
    """Expand the --takeout paths into a sorted list of .mbox/.zip/.tgz files."""
    files = []
    for path in paths:
        if os.path.isdir(path):
            for root, _, names in os.walk(path):
                files += [os.path.join(root, n) for n in sorted(names)
                          if n.lower().endswith((".mbox", ".zip", ".tgz", ".tar.gz"))]
        elif os.path.exists(path):
            files.append(path)
        else:
            sys.exit(f"Takeout path not found: {path}")
    return files


def takeout_sources(files):
    """Yield (label, binary stream, file on disk, index) for every .mbox in `files`.

    Reads .mbox files directly and Takeout .zip/.tgz archives without extracting them.
    The file on disk's tell() gives progress, also through compressed archives.
    """
    for index, path in enumerate(files):
        lower = path.lower()
        with open(path, "rb") as raw:
            if lower.endswith(".mbox"):
                yield path, raw, raw, index
            elif lower.endswith(".zip"):
                with zipfile.ZipFile(raw) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".mbox"):
                            with zf.open(name) as member:
                                yield f"{path}:{name}", member, raw, index
            elif lower.endswith((".tgz", ".tar.gz")):
                with tarfile.open(fileobj=raw, mode="r|gz") as tf:
                    for member in tf:
                        if member.isfile() and member.name.lower().endswith(".mbox"):
                            yield f"{path}:{member.name}", tf.extractfile(member), raw, index
            else:
                log.warning("Skipping %s: not an .mbox, .zip or .tgz file", path)


def iter_mbox(stream):
    """Yield (from_line, raw message bytes) from an mbox stream, one message at a time.

    Handles the mboxrd ">From " escaping Takeout uses and drops the blank line that
    separates messages. Memory use is one message at a time.
    """
    from_line, lines, prev_blank = None, [], True
    for line in stream:
        if line.startswith(b"From ") and (prev_blank or TAKEOUT_FROM.match(line)):
            if from_line is not None:
                yield from_line, finish_mbox_message(lines)
            from_line, lines = line, []
        elif from_line is not None:
            lines.append(MBOXRD_ESCAPE.sub(rb"\1", line))
        prev_blank = line in (b"\n", b"\r\n")
    if from_line is not None:
        yield from_line, finish_mbox_message(lines)


def finish_mbox_message(lines):
    raw = b"".join(lines)
    if raw.endswith(b"\r\n\r\n"):
        return raw[:-2]
    if raw.endswith(b"\n\n"):
        return raw[:-1]
    return raw


def raw_header(raw, name):
    """Return a header's value from raw message bytes, decoded to text.

    Reads the bytes directly so UTF-8 label names survive (the email parser replaces
    non-ASCII bytes), unfolds continuation lines and decodes =?UTF-8?...?= words.
    """
    head = raw.split(b"\r\n\r\n", 1)[0].split(b"\n\n", 1)[0]
    m = re.search(rb"(?im)^" + re.escape(name.encode()) + rb":[ \t]*(.*(?:\r?\n[ \t].*)*)", head)
    if not m:
        return ""
    text = re.sub(rb"\r?\n[ \t]+", b" ", m.group(1)).strip().decode("utf-8", "replace")
    try:
        return str(email.header.make_header(email.header.decode_header(text)))
    except (email.errors.HeaderParseError, LookupError, UnicodeDecodeError):
        return text


def parse_takeout_message(from_line, raw):
    """Return (gmail_id or None, thread_id, labels, internal_date_ms) for a Takeout message."""
    m = TAKEOUT_FROM.match(from_line)
    gmail_id = format(int(m.group(1)), "x") if m else None
    headers = email.parser.BytesHeaderParser().parsebytes(raw)
    thrid = raw_header(raw, "X-GM-THRID")
    thread_id = format(int(thrid), "x") if thrid.isdigit() else None
    labels = [lab.strip() for lab in raw_header(raw, "X-Gmail-Labels").split(",") if lab.strip()]
    internal_date = 0
    try:
        if m:
            dt = datetime.strptime(m.group(2).decode().strip(), "%a %b %d %H:%M:%S %z %Y")
            internal_date = int(dt.timestamp() * 1000)
    except ValueError:
        pass
    if not internal_date and headers.get("Date"):
        try:
            internal_date = int(email.utils.parsedate_to_datetime(headers["Date"]).timestamp() * 1000)
        except (TypeError, ValueError):
            pass
    return gmail_id, thread_id, labels, max(internal_date, 0)


def check_takeout_ids(files, known, sample_size=2000):
    """Return (matching, checked) for the first `sample_size` messages that have an ID."""
    matched = checked = 0
    for _, stream, _, _ in takeout_sources(files):
        for from_line, raw in iter_mbox(stream):
            gmail_id, _, labels, _ = parse_takeout_message(from_line, raw)
            if not gmail_id or SKIP_LABELS & {lab.lower() for lab in labels}:
                continue
            checked += 1
            matched += gmail_id in known
            if checked >= sample_size:
                return matched, checked
    return matched, checked


def cmd_import(args):
    db = open_db(args.db)
    if not args.dry_run:
        s3 = s3_client(args)
        check_bucket(s3, args.bucket)

    # Snapshot of what the state database already knows (one row per Gmail ID).
    known = {r["id"]: (r["archived_at"] is not None, r["verify_error"] is not None,
                       r["trashed_at"] is not None)
             for r in db.execute("SELECT id, archived_at, verify_error, trashed_at FROM messages")}
    listed = len(known)
    stats = dict(messages=0, skipped_label=0, no_id=0, already=0, matched=0, new=0)
    files = takeout_files(args.takeout)
    sizes = [os.path.getsize(f) for f in files]
    total_bytes = sum(sizes)
    position = {"raw": None, "index": 0}

    def bytes_done():
        try:
            current = position["raw"].tell() if position["raw"] else 0
        except (OSError, ValueError):  # file already closed
            current = 0
        return sum(sizes[:position["index"]]) + current

    def candidates():
        """Parse the export and yield messages that still need archiving."""
        for label, stream, raw_file, index in takeout_sources(files):
            log.info("Reading %s", label)
            position.update(raw=raw_file, index=index)
            for from_line, raw in iter_mbox(stream):
                stats["messages"] += 1
                gmail_id, thread_id, labels, internal_date = parse_takeout_message(from_line, raw)
                if SKIP_LABELS & {lab.lower() for lab in labels}:
                    stats["skipped_label"] += 1
                    continue
                if not gmail_id:
                    stats["no_id"] += 1
                    continue
                state = known.get(gmail_id)
                if state is None:
                    stats["new"] += 1
                elif state[0] and not state[1] or state[2]:
                    stats["already"] += 1  # archived (and not failed verify) or trashed
                    continue
                else:
                    stats["matched"] += 1
                if args.limit and stats["matched"] + stats["new"] > args.limit:
                    return
                yield gmail_id, raw, thread_id, labels, internal_date, bool(state and state[1])

    if args.dry_run:
        last_log = time.monotonic()
        for _ in candidates():
            if time.monotonic() - last_log >= 30:
                last_log = time.monotonic()
                log.info("Scanned %d messages (%.0f%% of the export)",
                         stats["messages"], 100 * bytes_done() / max(total_bytes, 1))
        seen = stats["already"] + stats["matched"] + stats["new"]
        match = f"{(stats['already'] + stats['matched']) / seen:.1%}" if seen else "n/a"
        print(
            "\nTakeout dry run (nothing uploaded)\n"
            f"  export size:             {human_bytes(total_bytes)}\n"
            f"  messages in export:      {stats['messages']}\n"
            f"  skipped spam/trash/chat: {stats['skipped_label']}\n"
            f"  without a Gmail ID:      {stats['no_id']}\n"
            f"  already archived:        {stats['already']}\n"
            f"  to import, listed:       {stats['matched']}\n"
            f"  to import, not listed:   {stats['new']} (newer than the listing, or unmatched)\n"
            f"  match rate:              {match} of messages match the state database"
        )
        return

    # Safety check before anything is uploaded: Takeout IDs must match the API listing,
    # or cleanup (which trashes by ID) and the API workflow wouldn't line up with it.
    if listed and not args.allow_unmatched:
        matched, sample = check_takeout_ids(files, known)
        if sample and matched / sample < 0.9:
            sys.exit(
                f"Only {matched / sample:.0%} of the first {sample} Takeout messages match the "
                "Gmail IDs in the state database, so nothing was uploaded. Check that --db is "
                "the current state database (use --sync-state), or pass --allow-unmatched."
            )
        log.info("ID check: %d of the first %d Takeout messages match the state database",
                 matched, sample)

    dedupe = DedupeIndex(db, by_message_id=not args.no_dedupe_message_id)
    counts = {"uploaded": 0, "in_s3": 0, "duplicate": 0}
    failed = 0
    start = last_log = time.monotonic()
    def store(item):
        gmail_id, raw, thread_id, labels, internal_date, force = item
        return store_message(args, s3, dedupe, gmail_id, raw, thread_id, internal_date,
                             labels, force)

    results = run_parallel(store, candidates(), args.workers, deadline_for(args))
    try:
        for i, (item, r, err) in enumerate(results, 1):
            gmail_id = item[0]
            if err is None:
                db.execute("INSERT OR IGNORE INTO messages (id) VALUES (?)", (gmail_id,))
                record_stored(db, gmail_id, r, "takeout")
                counts[r["outcome"]] += 1
            else:
                failed += 1
                log.error("Failed %s: %s: %s", gmail_id, type(err).__name__, err)
            if i % 200 == 0:
                db.commit()
            t = time.monotonic()
            if t - last_log >= 30:
                last_log = t
                done, read = sum(counts.values()), bytes_done()
                eta = (t - start) * (total_bytes - read) / read if read else 0
                log.info("Import: %.0f%% of export read, %d stored (%d uploaded, %d duplicates), "
                         "%d already archived, %d failed, %.1f msg/s, ETA %dh%02dm",
                         100 * read / max(total_bytes, 1), done, counts["uploaded"],
                         counts["duplicate"], stats["already"], failed, done / (t - start),
                         eta // 3600, eta % 3600 // 60)
    finally:
        results.close()  # stop the worker pool
        db.commit()

    log.info(
        "Import finished: %d uploaded, %d already in S3, %d duplicates skipped, %d failed; "
        "%d already archived, %d spam/trash/chat skipped, %d without a Gmail ID",
        counts["uploaded"], counts["in_s3"], counts["duplicate"], failed,
        stats["already"], stats["skipped_label"], stats["no_id"],
    )
    print_summary(db)


# --------------------------------------------------------------------------- #
# verify
# --------------------------------------------------------------------------- #

def cmd_verify(args):
    db = open_db(args.db)
    s3 = s3_client(args)
    check_bucket(s3, args.bucket)

    sql = (
        "SELECT id, s3_key, sha256, size FROM messages "
        "WHERE archived_at IS NOT NULL AND duplicate_of IS NULL"
    )
    if not args.reverify:
        sql += " AND verified_at IS NULL"
    rows = db.execute(sql).fetchall()
    log.info("%d archived messages to verify", len(rows))

    def check(r):
        obj = s3.get_object(Bucket=args.bucket, Key=r["s3_key"])
        h = hashlib.sha256()
        size = 0
        for chunk in obj["Body"].iter_chunks(1024 * 1024):
            h.update(chunk)
            size += len(chunk)
        if h.hexdigest() != r["sha256"]:
            return f"sha256 mismatch (expected {r['sha256']}, got {h.hexdigest()})"
        if size != r["size"]:
            return f"size mismatch (expected {r['size']}, got {size})"
        return None

    progress = Progress(len(rows), "Verify")
    ok = bad = 0
    results = run_parallel(check, rows, args.workers, deadline_for(args))
    try:
        for i, (r, mismatch, exc) in enumerate(results, 1):
            err = mismatch or (exc and f"{type(exc).__name__}: {exc}"[:500])
            if err:
                bad += 1
                log.error("Verify FAILED %s: %s", r["id"], err)
                db.execute(
                    "UPDATE messages SET verified_at=NULL, verify_error=? WHERE id=?",
                    (err, r["id"]),
                )
            else:
                ok += 1
                db.execute(
                    "UPDATE messages SET verified_at=?, verify_error=NULL WHERE id=?",
                    (now(), r["id"]),
                )
            if i % 200 == 0:
                db.commit()
            progress.log(i, bad)
    finally:
        results.close()  # stop the worker pool
        db.commit()

    # Duplicates were not uploaded; they are verified when their original is.
    db.execute(
        "UPDATE messages SET verified_at = "
        "(SELECT o.verified_at FROM messages o WHERE o.id = messages.duplicate_of) "
        "WHERE duplicate_of IS NOT NULL AND archived_at IS NOT NULL"
    )
    db.commit()

    log.info("Verify finished: %d OK, %d failed", ok, bad)
    print_summary(db)
    if bad:
        sys.exit(1)


# --------------------------------------------------------------------------- #
# cleanup
# --------------------------------------------------------------------------- #

def cmd_cleanup(args):
    db = open_db(args.db)

    unverified = db.execute(
        "SELECT COUNT(*) FROM messages WHERE archived_at IS NOT NULL AND verified_at IS NULL"
    ).fetchone()[0]
    if unverified and not args.allow_partial:
        sys.exit(
            f"{unverified} archived messages are not verified. Run --mode verify and resolve "
            "failures first (or pass --allow-partial to trash only the verified ones)."
        )

    sql = (
        "SELECT id FROM messages WHERE verified_at IS NOT NULL AND trashed_at IS NULL "
        "ORDER BY internal_date"
    )
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    ids = [r["id"] for r in db.execute(sql)]
    if not ids:
        log.info("Nothing to clean up.")
        return

    print(f"{len(ids)} verified messages will be moved to Gmail Trash.")
    print("Trash is NOT emptied; Gmail deletes Trash items permanently after 30 days.")
    if args.dry_run:
        print("Dry run: no changes made.")
        return
    if not args.confirm:
        sys.exit("Refusing to modify Gmail without --confirm.")
    if not args.yes:
        typed = input(f"Type TRASH {len(ids)} to proceed: ").strip()
        if typed != f"TRASH {len(ids)}":
            sys.exit("Confirmation did not match; aborting.")

    gmail = ThreadLocalGmail(gmail_credentials(args.credentials, SCOPE_MODIFY, TOKEN_MODIFY))
    configure_gmail_rate(db, args)

    def trash(msg_id):
        gmail_call(gmail.get().users().messages().trash(userId="me", id=msg_id))

    progress = Progress(len(ids), "Cleanup")
    done = gone = failed = 0
    results = run_parallel(trash, ids, args.workers, deadline_for(args))
    try:
        for i, (msg_id, _, err) in enumerate(results, 1):
            if err is None:
                db.execute("UPDATE messages SET trashed_at=? WHERE id=?", (now(), msg_id))
                done += 1
            elif isinstance(err, HttpError) and err.resp.status == 404:
                # Already deleted from Gmail; nothing left to trash.
                db.execute("UPDATE messages SET trashed_at=?, error=? WHERE id=?",
                           (now(), NOT_FOUND, msg_id))
                gone += 1
            else:
                failed += 1
                log.error("Trash failed %s: %s", msg_id, err)
            if i % 200 == 0:
                db.commit()
            progress.log(i, failed)
    finally:
        results.close()  # stop the worker pool
        db.commit()
        save_gmail_rate(db)

    log.info("Cleanup finished: %d trashed, %d already gone from Gmail, %d failed",
             done, gone, failed)
    print_summary(db)


# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #

# S3 list prices in USD for us-east-1 (check https://aws.amazon.com/s3/pricing/).
# storage: per GB-month; put/get: per 1,000 requests; retrieval: per GB read;
# min_size: smallest billable object size in bytes.
S3_PRICES = {
    "STANDARD":            dict(storage=0.023,   put=0.005, get=0.0004, retrieval=0,    min_size=0),
    "INTELLIGENT_TIERING": dict(storage=0.023,   put=0.005, get=0.0004, retrieval=0,    min_size=0),
    "STANDARD_IA":         dict(storage=0.0125,  put=0.01,  get=0.001,  retrieval=0.01, min_size=128 * 1024),
    "ONEZONE_IA":          dict(storage=0.01,    put=0.01,  get=0.001,  retrieval=0.01, min_size=128 * 1024),
    "GLACIER_IR":          dict(storage=0.004,   put=0.02,  get=0.01,   retrieval=0.03, min_size=128 * 1024),
    "DEEP_ARCHIVE":        dict(storage=0.00099, put=0.05,  get=0.0004, retrieval=0,    min_size=0),
}
# Intelligent-Tiering monitoring fee per 1,000 objects of 128 KB or more, per month.
INTELLIGENT_TIERING_MONITORING = 0.0025
GB = 1024 ** 3


def human_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def estimate_cost(db):
    """Estimated S3 cost: (storage per month, one-time request cost so far, objects, reads).

    Counts each uploaded object once (duplicates aren't uploaded). Reads are the
    "already in S3" check on upload plus one verify download per verified object.
    Excludes data transfer, which is free for the first 100 GB a month.
    """
    monthly = requests = 0.0
    objects = reads = 0
    for r in db.execute(
        "SELECT COALESCE(storage_class, 'STANDARD') AS cls, COUNT(*) AS n, "
        "COALESCE(SUM(size), 0) AS bytes, "
        "COALESCE(SUM(CASE WHEN size >= 131072 THEN 1 ELSE 0 END), 0) AS large, "
        "COALESCE(SUM(CASE WHEN verified_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS verified, "
        "COALESCE(SUM(CASE WHEN verified_at IS NOT NULL THEN size ELSE 0 END), 0) AS verified_bytes "
        "FROM messages WHERE archived_at IS NOT NULL AND duplicate_of IS NULL "
        "GROUP BY cls"
    ):
        price = S3_PRICES.get(r["cls"], S3_PRICES["STANDARD"])
        billable = r["bytes"]
        if price["min_size"]:
            billable = db.execute(
                "SELECT COALESCE(SUM(MAX(size, ?)), 0) FROM messages WHERE archived_at IS NOT NULL "
                "AND duplicate_of IS NULL AND COALESCE(storage_class, 'STANDARD') = ?",
                (price["min_size"], r["cls"]),
            ).fetchone()[0]
        monthly += billable / GB * price["storage"]
        if r["cls"] == "INTELLIGENT_TIERING":
            monthly += r["large"] / 1000 * INTELLIGENT_TIERING_MONITORING
        n_reads = r["n"] + r["verified"]
        requests += (r["n"] * price["put"] + n_reads * price["get"]) / 1000
        requests += r["verified_bytes"] / GB * price["retrieval"]
        objects += r["n"]
        reads += n_reads
    return monthly, requests, objects, reads


def usd(x):
    return f"${x:,.2f}" if x >= 0.01 or x == 0 else f"${x:.4f}"


def print_summary(db):
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731
    in_s3 = q("SELECT COALESCE(SUM(size), 0) FROM messages "
              "WHERE archived_at IS NOT NULL AND duplicate_of IS NULL")
    monthly, requests, objects, reads = estimate_cost(db)
    print(
        "\nState summary\n"
        f"  listed:         {q('SELECT COUNT(*) FROM messages')}\n"
        f"  archived:       {q('SELECT COUNT(*) FROM messages WHERE archived_at IS NOT NULL')}\n"
        f"    duplicates:   {q('SELECT COUNT(*) FROM messages WHERE duplicate_of IS NOT NULL')}\n"
        f"    from Takeout: {q("SELECT COUNT(*) FROM messages WHERE source = 'takeout'")}\n"
        f"  archive errors: {q('SELECT COUNT(*) FROM messages WHERE archived_at IS NULL AND error IS NOT NULL')}\n"
        f"  verified:       {q('SELECT COUNT(*) FROM messages WHERE verified_at IS NOT NULL')}\n"
        f"  verify errors:  {q('SELECT COUNT(*) FROM messages WHERE verify_error IS NOT NULL')}\n"
        f"  trashed:        {q('SELECT COUNT(*) FROM messages WHERE trashed_at IS NOT NULL')}\n"
        f"  bytes in S3:    {in_s3} ({human_bytes(in_s3)})\n"
        "\nEstimated AWS cost (S3 list prices, us-east-1)\n"
        f"  storage:        {usd(monthly)} per month\n"
        f"  requests:       {usd(requests)} so far ({objects} uploads, {reads} reads)\n"
        f"  total so far:   {usd(requests + monthly)} (requests + one month of storage)"
    )


def cmd_login(args):
    for scope, token_file in ((SCOPE_READONLY, TOKEN_READONLY), (SCOPE_MODIFY, TOKEN_MODIFY)):
        print(f"Authorizing {scope.rsplit('/', 1)[-1]} -> {token_file}")
        gmail_credentials(args.credentials, scope, token_file)
    print("Done. For GitHub Actions, store each file's contents as a repository secret.")


def cmd_status(args):
    print_summary(open_db(args.db))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", required=True, choices=["archive", "verify", "cleanup", "status", "login", "import"])
    p.add_argument("--bucket", help="S3 bucket name (required for archive, verify and import)")
    p.add_argument("--region", default=None, help="AWS region, e.g. us-east-1")
    p.add_argument("--profile", default=None, help="AWS CLI profile name")
    p.add_argument("--prefix", default="gmail", help="S3 key prefix (default: gmail)")
    p.add_argument("--db", default="archive_state.sqlite3", help="Local state database")
    p.add_argument("--credentials", default="credentials.json", help="Google OAuth client file")
    p.add_argument("--query", default="",
                   help="Gmail search query to restrict messages, e.g. 'before:2020/01/01'. "
                        "Spam and Trash are always excluded.")
    p.add_argument("--limit", type=int, default=0, help="Process at most N messages this run")
    p.add_argument("--workers", type=int, default=8,
                   help="Parallel Gmail/S3 requests for archive/verify/cleanup (default 8). Gmail allows "
                        "roughly 50 message downloads per second per user.")
    p.add_argument("--rate", type=float, default=20,
                   help="Max Gmail requests per second across all workers (default 20). "
                        "The script lowers it automatically to fit your Gmail quota and "
                        "remembers the rate that worked for the next run.")
    p.add_argument("--storage-class", default=None,
                   help="S3 storage class, e.g. STANDARD_IA, GLACIER_IR, DEEP_ARCHIVE")
    p.add_argument("--kms-key-id", default=None, help="Use SSE-KMS with this key (default SSE-S3)")
    p.add_argument("--no-dedupe-message-id", action="store_true",
                   help="archive: only treat byte-identical messages as duplicates, not "
                        "messages that share a Message-ID header")
    p.add_argument("--takeout", nargs="+", metavar="PATH",
                   help="import: Takeout .mbox files, .zip/.tgz archives or folders")
    p.add_argument("--allow-unmatched", action="store_true",
                   help="import: continue even if Takeout IDs don't match the state database")
    p.add_argument("--sync-state", action="store_true",
                   help="download the state database from s3://BUCKET/state/ before the run "
                        "and upload it afterwards (keeps local runs in step with the workflows)")
    p.add_argument("--relist", action="store_true",
                   help="archive: list every message again instead of only new mail")
    p.add_argument("--reverify", action="store_true", help="verify: re-check already verified objects")
    p.add_argument("--confirm", action="store_true", help="cleanup: required to modify Gmail")
    p.add_argument("--yes", action="store_true",
                   help="cleanup: skip the typed confirmation (for automation; needs --confirm)")
    p.add_argument("--max-runtime", type=int, default=0,
                   help="archive/verify/cleanup: stop cleanly after N minutes (resume later)")
    p.add_argument("--dry-run", action="store_true",
                   help="cleanup: show what would be trashed; import: report what would be imported")
    p.add_argument("--allow-partial", action="store_true",
                   help="cleanup: proceed even if some archived messages are unverified")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    if args.mode in ("archive", "verify") and not args.bucket:
        p.error("--bucket is required for archive and verify modes")
    if args.mode == "import" and not args.takeout:
        p.error("--takeout is required for import mode")
    if args.mode == "import" and not args.dry_run and not args.bucket:
        p.error("--bucket is required for import (except with --dry-run)")
    if args.sync_state and not args.bucket:
        p.error("--sync-state requires --bucket")
    if args.yes and not args.confirm:
        p.error("--yes requires --confirm")
    if not 0 < args.rate <= 50:
        p.error("--rate must be more than 0 and at most 50")
    if not 1 <= args.workers <= 64:
        p.error("--workers must be between 1 and 64")
    if args.limit < 0 or args.max_runtime < 0:
        p.error("--limit and --max-runtime must be 0 or more")
    return args


STATE_KEY = "state/archive_state.sqlite3"


def sync_state_down(args):
    """Download the shared state database from S3, keeping a backup of the local copy."""
    s3 = s3_client(args)
    try:
        s3.head_object(Bucket=args.bucket, Key=STATE_KEY)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            log.warning("No state database at s3://%s/%s yet; using the local one.",
                        args.bucket, STATE_KEY)
            return
        sys.exit(f"Cannot read s3://{args.bucket}/{STATE_KEY}: {e}")
    if os.path.exists(args.db):
        os.replace(args.db, args.db + ".bak")
        log.info("Backed up local state database to %s.bak", args.db)
    s3.download_file(args.bucket, STATE_KEY, args.db)
    log.info("Downloaded state database from s3://%s/%s", args.bucket, STATE_KEY)


def sync_state_up(args):
    if not os.path.exists(args.db):
        return
    check = sqlite3.connect(args.db).execute("PRAGMA integrity_check").fetchone()[0]
    if check != "ok":
        log.error("State database failed its integrity check (%s); not uploading.", check)
        return
    s3_client(args).upload_file(args.db, args.bucket, STATE_KEY)
    log.info("Uploaded state database to s3://%s/%s", args.bucket, STATE_KEY)


def _raise_interrupt(signum, frame):
    raise KeyboardInterrupt


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # The Gmail client logs every rate-limit response itself; gmail_call reports them.
    logging.getLogger("googleapiclient.http").setLevel(logging.ERROR)

    # GitHub Actions cancels and timeouts send SIGTERM; handle it like Ctrl+C so
    # progress is committed before exiting.
    signal.signal(signal.SIGTERM, _raise_interrupt)
    modifies_state = args.mode in ("archive", "verify", "cleanup", "import") and not args.dry_run
    if args.sync_state:
        sync_state_down(args)
    try:
        {"archive": cmd_archive, "verify": cmd_verify, "cleanup": cmd_cleanup,
         "status": cmd_status, "login": cmd_login, "import": cmd_import}[args.mode](args)
    except KeyboardInterrupt:
        log.warning("Interrupted; progress so far is saved. Run the same command to continue.")
        sys.exit(130)
    finally:
        # Runs after errors and interrupts too, so the shared copy keeps all progress.
        if args.sync_state and modifies_state:
            sync_state_up(args)


if __name__ == "__main__":
    main()
