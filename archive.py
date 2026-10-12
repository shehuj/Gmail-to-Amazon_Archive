#!/usr/bin/env python3
"""Archive Gmail messages to Amazon S3, verify them, and (optionally) trash them.

Modes (run in this order):
  archive  Read-only Gmail access. Downloads each message in raw RFC 822 form
           (headers, body and attachments) and uploads it to S3 as a .eml file.
  verify   Downloads every archived object from S3 and checks its SHA-256
           against the local manifest.
  cleanup  Moves *verified* messages to Gmail Trash. Requires --confirm and a
           typed confirmation (or --yes). Never empties Trash.
  login    Authorizes both Gmail scopes in a browser and saves the token files,
           e.g. to copy into GitHub Actions secrets.

Progress is tracked in a local SQLite database so every mode is resumable.
"""

import argparse
import base64
import email.parser
import hashlib
import json
import logging
import os
import random
import signal
import sqlite3
import sys
import threading
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
    duplicate_of  TEXT
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
    for column in ("message_id_header", "duplicate_of"):
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


def run_parallel(fn, items, workers, deadline=None):
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
                    log.warning("--max-runtime reached; stopping after %d of %d. "
                                "Run again to continue.", submitted, len(items))
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


def gmail_call(request, retries=6):
    """Execute a Gmail API request, retrying rate limits, 5xx and network errors.

    Uses exponential backoff with jitter (about 1 minute in total before giving up).
    """
    for attempt in range(retries + 1):
        try:
            return request.execute()
        except HttpError as e:
            status = e.resp.status
            rate_limited = status == 403 and b"ratelimitexceeded" in (e.content or b"").lower()
            if attempt == retries or not (status in (429, 500, 502, 503, 504) or rate_limited):
                raise
            reason = f"HTTP {status}"
        except (OSError, httplib2.HttpLib2Error) as e:  # timeouts, resets, DNS, TLS
            if attempt == retries:
                raise
            reason = type(e).__name__
        delay = min(2 ** attempt, 32) + random.random()
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


def archive_one(args, s3, gmail, dedupe, msg_id, force_upload=False):
    """Download one message from Gmail and upload it to S3. Runs in a worker thread.

    Returns a dict whose "outcome" is "uploaded", "in_s3" (identical object already
    there) or "duplicate" (same content as another archived message; not uploaded).
    `force_upload` skips the "already in S3" check, for objects that failed verification.
    """
    msg = gmail_call(gmail.get().users().messages().get(
        userId="me", id=msg_id, format="raw", fields="id,threadId,internalDate,labelIds,raw"))
    raw = base64.urlsafe_b64decode(msg["raw"])
    digest = hashlib.sha256(raw).hexdigest()
    internal_date = int(msg.get("internalDate", 0))
    key = s3_key_for(args.prefix, msg_id, internal_date)
    message_id = message_id_header(raw)
    result = dict(thread_id=msg.get("threadId"), internal_date=internal_date, key=key,
                  sha256=digest, size=len(raw), message_id=message_id, duplicate_of=None)

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
                "gmail-thread-id": msg.get("threadId", ""),
                "gmail-internal-date": str(internal_date),
                "gmail-labels": ",".join(msg.get("labelIds", []))[:1500],
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
    except Exception:
        dedupe.release(msg_id, digest, message_id)
        raise
    result["outcome"] = "uploaded"
    return result


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
                db.execute(
                    "UPDATE messages SET thread_id=?, internal_date=?, s3_key=?, sha256=?, "
                    "size=?, message_id_header=?, duplicate_of=?, archived_at=?, error=NULL, "
                    "verified_at=NULL, verify_error=NULL WHERE id=?",
                    (r["thread_id"], r["internal_date"], r["key"], r["sha256"], r["size"],
                     r["message_id"], r["duplicate_of"], now(), msg_id),
                )
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

    log.info(
        "Archive finished: %d uploaded, %d already in S3, %d duplicates skipped, %d failed",
        counts["uploaded"], counts["in_s3"], counts["duplicate"], failed,
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

    log.info("Cleanup finished: %d trashed, %d already gone from Gmail, %d failed",
             done, gone, failed)
    print_summary(db)


# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #

def print_summary(db):
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731
    print(
        "\nState summary\n"
        f"  listed:         {q('SELECT COUNT(*) FROM messages')}\n"
        f"  archived:       {q('SELECT COUNT(*) FROM messages WHERE archived_at IS NOT NULL')}\n"
        f"    duplicates:   {q('SELECT COUNT(*) FROM messages WHERE duplicate_of IS NOT NULL')}\n"
        f"  archive errors: {q('SELECT COUNT(*) FROM messages WHERE archived_at IS NULL AND error IS NOT NULL')}\n"
        f"  verified:       {q('SELECT COUNT(*) FROM messages WHERE verified_at IS NOT NULL')}\n"
        f"  verify errors:  {q('SELECT COUNT(*) FROM messages WHERE verify_error IS NOT NULL')}\n"
        f"  trashed:        {q('SELECT COUNT(*) FROM messages WHERE trashed_at IS NOT NULL')}\n"
        f"  bytes in S3:    {q('SELECT COALESCE(SUM(size), 0) FROM messages WHERE archived_at IS NOT NULL AND duplicate_of IS NULL')}"
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
    p.add_argument("--mode", required=True, choices=["archive", "verify", "cleanup", "status", "login"])
    p.add_argument("--bucket", help="S3 bucket name (required for archive/verify)")
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
    p.add_argument("--storage-class", default=None,
                   help="S3 storage class, e.g. STANDARD_IA, GLACIER_IR, DEEP_ARCHIVE")
    p.add_argument("--kms-key-id", default=None, help="Use SSE-KMS with this key (default SSE-S3)")
    p.add_argument("--no-dedupe-message-id", action="store_true",
                   help="archive: only treat byte-identical messages as duplicates, not "
                        "messages that share a Message-ID header")
    p.add_argument("--relist", action="store_true",
                   help="archive: list every message again instead of only new mail")
    p.add_argument("--reverify", action="store_true", help="verify: re-check already verified objects")
    p.add_argument("--confirm", action="store_true", help="cleanup: required to modify Gmail")
    p.add_argument("--yes", action="store_true",
                   help="cleanup: skip the typed confirmation (for automation; needs --confirm)")
    p.add_argument("--max-runtime", type=int, default=0,
                   help="archive/verify/cleanup: stop cleanly after N minutes (resume later)")
    p.add_argument("--dry-run", action="store_true", help="cleanup: show what would be trashed")
    p.add_argument("--allow-partial", action="store_true",
                   help="cleanup: proceed even if some archived messages are unverified")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    if args.mode in ("archive", "verify") and not args.bucket:
        p.error("--bucket is required for archive and verify modes")
    if args.yes and not args.confirm:
        p.error("--yes requires --confirm")
    if not 1 <= args.workers <= 64:
        p.error("--workers must be between 1 and 64")
    if args.limit < 0 or args.max_runtime < 0:
        p.error("--limit and --max-runtime must be 0 or more")
    return args


def _raise_interrupt(signum, frame):
    raise KeyboardInterrupt


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # GitHub Actions cancels and timeouts send SIGTERM; handle it like Ctrl+C so
    # progress is committed before exiting.
    signal.signal(signal.SIGTERM, _raise_interrupt)
    try:
        {"archive": cmd_archive, "verify": cmd_verify,
         "cleanup": cmd_cleanup, "status": cmd_status, "login": cmd_login}[args.mode](args)
    except KeyboardInterrupt:
        log.warning("Interrupted; progress so far is saved. Run the same command to continue.")
        sys.exit(130)


if __name__ == "__main__":
    main()
