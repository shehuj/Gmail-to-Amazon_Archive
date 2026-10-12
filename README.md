# Gmail-to-Amazon Archive

Archive Gmail messages to a private Amazon S3 bucket, verify the archive, and then
(optionally) move the archived messages to Gmail Trash to free up storage.

Python · Gmail API · Boto3 · SQLite · AWS S3

> **Status:**
>
> - **Tested with real Gmail and S3:**
>   - `archive` from a Mac.
>   - The **Ingest Gmail to S3** workflow on GitHub Actions, using access keys:
>     new mail was picked up, the archive step uploaded 20 messages, `verify`
>     passed 40 of 40, and the state database round trip through S3 worked.
> - **Tested offline only** (fake Gmail and S3): duplicate handling, `cleanup`,
>   `--max-runtime`, interrupt handling, and the OIDC role sign-in.
>
> Start each new stage with a small `--limit` run. In particular, run the cleanup
> workflow as a dry run before the first real cleanup.

## How it works

The migration runs in separate, resumable stages:

| Mode | Gmail access | What it does |
| --- | --- | --- |
| `archive` | read-only | Downloads each message in raw RFC 822 form (headers, body and attachments) and uploads it to S3 as a `.eml` file. Records its SHA-256 checksum. |
| `verify` | none | Downloads every archived object from S3 and compares its SHA-256 and size with the local manifest. |
| `cleanup` | modify | Moves **verified** messages to Gmail Trash. Never empties Trash. |
| `status` | none | Prints counts of listed, archived, verified and trashed messages. |
| `login` | both | Authorizes both Gmail scopes in a browser and saves the token files, e.g. for GitHub Actions secrets. |

Objects are stored as:

```text
s3://YOUR_BUCKET/gmail/YYYY/MM/<gmail-message-id>.eml
```

Each object is encrypted at rest (SSE-S3 by default, or SSE-KMS). It carries metadata
for the Gmail message ID, thread ID, internal date, labels and SHA-256.

Progress is tracked in a local SQLite database (`archive_state.sqlite3`), so you can
interrupt any stage and re-run it to resume where it stopped.

### Duplicates

`archive` skips duplicates, so each email is stored in S3 only once:

| Duplicate | How it's detected | What happens |
| --- | --- | --- |
| Already archived | Same Gmail message ID in the state database | Skipped without downloading |
| Identical copy | Another Gmail message with the same SHA-256 (byte-for-byte identical) | Not uploaded |
| Same email, different copy | Another Gmail message with the same `Message-ID` header. Copies from imports, forwards or mailing lists can differ slightly in headers such as `Received`. | Not uploaded. Use `--no-dedupe-message-id` to turn this check off. |
| Already in S3 | An object at the same key with the same SHA-256, e.g. after the state database was lost | Not uploaded again. This also avoids extra versions in a versioned bucket. |

How duplicates are handled after archiving:

- **Recorded, not uploaded.** A duplicate is recorded in the state database with
  `duplicate_of` set to the original message and points to the original's S3 object.
- **Verified with the original.** `verify` checks only the originals. Each duplicate
  is marked verified when its original is.
- **Trashed by cleanup.** `cleanup` moves duplicates to Trash too, because their
  content is already archived.
- **Re-queued if the original fails.** If the original's upload fails, its
  duplicates go back in the queue and are archived on the next run.
- **Corrupted objects are always replaced.** A message whose S3 object failed
  verification is always re-uploaded, even though an object exists at its key.

## Files

| File | Purpose |
| --- | --- |
| `archive.py` | Archive, verify, gated cleanup, status and login modes |
| `requirements.txt` | Python dependencies |
| `.github/workflows/ingest.yml` | Manually triggered GitHub Actions workflow: archive + verify |
| `.github/workflows/cleanup.yml` | Manually triggered GitHub Actions workflow: move verified mail to Trash |
| `aws/` | IAM trust and permissions policies for the GitHub Actions role, and the S3 lifecycle rule |
| `.gitignore` | Keeps credentials, OAuth tokens and local state out of git |

## Setup

### 1. Python environment

Requires Python 3.9+.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Google OAuth client

1. In [Google Cloud Console](https://console.cloud.google.com/), create or pick a project.
2. Enable the **Gmail API** (APIs & Services → Library).
3. Configure the OAuth consent screen (**Google Auth Platform**):
   - Choose **External**.
   - Under **Audience → Test users**, add the Gmail address you are archiving.
4. Create credentials: **OAuth client ID → Desktop app**. Don't use a service
   account key: service accounts can't access a personal Gmail mailbox.
5. Download the JSON file and save it in this folder as `credentials.json`. It should
   have a single top-level key, `installed`.

On the first run, a browser window opens so you can authorize access. Google shows
"Google hasn't verified this app". That's expected for your own app, so click
**Continue**.

- `archive` requests the read-only scope `gmail.readonly` and saves the token to `token_readonly.json`.
- `cleanup` requests `gmail.modify` and saves the token to a separate `token_modify.json`.
  The read-only token can never be used to trash mail.
- `python archive.py --mode login` authorizes both scopes up front.

While the app is in **Testing** mode, tokens expire after 7 days. Then you must
delete `token_*.json` and sign in again. To avoid this, publish the app to
**In production** (see [GitHub Actions](#github-actions)).

### 3. AWS credentials and bucket

Configure AWS CLI credentials, using an existing profile or IAM Identity Center:

```bash
aws configure            # or: aws configure sso
aws sts get-caller-identity
```

Create a private bucket with public access blocked, default encryption and versioning enabled:

```bash
BUCKET=your-gmail-archive-bucket
REGION=us-east-1

aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
# For regions other than us-east-1, add:
#   --create-bucket-configuration LocationConstraint="$REGION"

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

Add a lifecycle rule so overwritten versions don't keep costing money:

```bash
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration file://aws/s3-lifecycle.json
```

The rule in `aws/s3-lifecycle.json` applies to the whole bucket:

- **Old versions** are deleted 30 days after they are overwritten or deleted. This
  includes old copies of the state database, which is re-uploaded on every
  workflow run.
- **Leftover delete markers** are removed automatically.
- **Unfinished multipart uploads** are cleaned up after 7 days. Large uploads, such
  as `aws s3 cp` of the state database, use multipart.

Current objects are never deleted. The trade-off is that an object overwritten or
deleted by mistake can only be recovered within those 30 days.

Note that `put-bucket-lifecycle-configuration` **replaces** any lifecycle rules the
bucket already has. Check first with
`aws s3api get-bucket-lifecycle-configuration --bucket "$BUCKET"`.

The IAM identity running the script needs these permissions on the bucket:

- `s3:ListBucket`
- `s3:PutObject` and `s3:GetObject` on `arn:aws:s3:::YOUR_BUCKET/*`
- KMS permissions for the key, if you use `--kms-key-id`

## Usage

### Step 1: Archive (read-only)

Start with a small trial run:

```bash
python archive.py --mode archive --bucket YOUR_BUCKET --region us-east-1 --limit 20
```

Then try a bigger batch and check the speed and estimated time left in the
progress lines:

```bash
python archive.py --mode archive --bucket YOUR_BUCKET --region us-east-1 --limit 2000
```

Then archive everything. `caffeinate -i` keeps your Mac awake. Raise `--workers`
for speed, and lower it if you see `Gmail API 429` warnings:

```bash
caffeinate -i python archive.py --mode archive --bucket YOUR_BUCKET --region us-east-1 --workers 12
```

The first run lists every message ID, which takes a few minutes for a large
mailbox. Later runs only look for new mail, newest first, and stop at the first page
with nothing new. If you import old mail into Gmail, run once with `--relist`:
Gmail lists imported mail by its original date, so the quick check can miss it.

You can stop a run at any time with Ctrl+C. GitHub cancels and timeouts are also
handled. Progress is saved, and the next run continues where this one stopped.

Choose your storage class before the full run. `--storage-class` only applies to
new uploads.

Archive only part of your mailbox with a Gmail search query:

```bash
python archive.py --mode archive --bucket YOUR_BUCKET --region us-east-1 \
  --query "before:2020/01/01"
```

### Step 2: Verify

```bash
python archive.py --mode verify --bucket YOUR_BUCKET --region us-east-1
```

This downloads each archived object and compares its SHA-256 checksum and size with
the local manifest. The command exits non-zero if any object fails.

- Review every failure before going further.
- Re-run `archive` to retry messages that failed to upload or failed verification.
- Use `--reverify` to re-check objects that already passed.

### Step 3: Spot-check the archive

Before you remove anything from Gmail, download a few objects and open them in a
mail client:

```bash
aws s3 ls s3://YOUR_BUCKET/gmail/ --recursive | head
aws s3 cp s3://YOUR_BUCKET/gmail/2019/06/<id>.eml .
open <id>.eml    # opens in Apple Mail
```

Pick some messages that have attachments. Check that the attachments open.

### Step 4: Cleanup (moves mail to Gmail Trash)

Only run this after verification passes. First preview what would be trashed:

```bash
python archive.py --mode cleanup --dry-run
```

Then run it:

```bash
python archive.py --mode cleanup --confirm
```

Cleanup has these safeguards:

- It refuses to run while any archived message is unverified. Use `--allow-partial` to override.
- It requires `--confirm`, plus typing `TRASH <count>` at the prompt. For automation, `--yes` replaces the prompt.
- It only trashes messages that are recorded as verified in the local database.
- It never empties Trash. Gmail permanently deletes Trash items after 30 days.

Before emptying Trash yourself, check two things:

- The archived messages and attachments can be retrieved from S3.
- Your Gmail storage has decreased as expected.

### Check progress at any time

```bash
python archive.py --mode status
```

## Options

| Option | Default | Description |
| --- | --- | --- |
| `--mode` | (required) | `archive`, `verify`, `cleanup`, `status` or `login` |
| `--bucket` | (none) | S3 bucket. Required for `archive` and `verify`. |
| `--region` | AWS config | AWS region, e.g. `us-east-1` |
| `--profile` | AWS default | AWS CLI profile name |
| `--prefix` | `gmail` | S3 key prefix |
| `--db` | `archive_state.sqlite3` | Local state database |
| `--credentials` | `credentials.json` | Google OAuth client file |
| `--query` | (all mail) | Gmail search query. Spam and Trash are always excluded. |
| `--limit` | 0 (no limit) | Process at most N messages in this run |
| `--workers` | 8 | Parallel requests for `archive`, `verify` and `cleanup`. Gmail allows roughly 50 message requests per second per user. |
| `--storage-class` | `STANDARD` | e.g. `STANDARD_IA`, `GLACIER_IR`, `INTELLIGENT_TIERING`, `DEEP_ARCHIVE` |
| `--kms-key-id` | (SSE-S3) | Encrypt with SSE-KMS using this key |
| `--relist` | off | `archive`: list every message again instead of only new mail |
| `--no-dedupe-message-id` | off | `archive`: only treat byte-identical messages as duplicates, not messages sharing a `Message-ID` |
| `--reverify` | off | `verify`: re-check objects that already passed |
| `--dry-run` | off | `cleanup`: show the count only, change nothing |
| `--confirm` | off | `cleanup`: required to modify Gmail |
| `--yes` | off | `cleanup`: skip the typed confirmation, for automation. Requires `--confirm`. |
| `--max-runtime` | 0 (no limit) | Stop `archive`, `verify` or `cleanup` cleanly after N minutes. Run again to continue. |
| `--allow-partial` | off | `cleanup`: proceed even if some messages are unverified |
| `-v`, `--verbose` | off | Debug logging |

Storage class notes:

- `GLACIER_IR` and `STANDARD_IA` are good fits for mail you rarely open.
- `DEEP_ARCHIVE` is cheapest, but retrieval takes hours. `verify` cannot read those objects until they are restored.

## Important notes

- **Spam and Trash are excluded.** The migration covers every other message Gmail
  returns, including archived mail (not in the Inbox).
- **Back up `archive_state.sqlite3`.** It is the manifest of what was archived, the
  checksums, and what was verified and trashed. Store a copy securely alongside the
  archive. For example:

  ```bash
  aws s3 cp archive_state.sqlite3 s3://YOUR_BUCKET/state/archive_state.sqlite3
  ```

  This is the same location the GitHub Actions workflows use.

- **Never commit secrets.** `credentials.json`, `token_*.json` and the database are
  excluded by `.gitignore`.
- **Rate limits and network errors.** Gmail rate limits, 5xx errors, timeouts and
  dropped connections are retried with exponential backoff, for about a minute
  before a message is marked failed. S3 requests use the AWS SDK's adaptive
  retries. If you see many rate-limit warnings, lower `--workers`.
- **Large mailboxes take hours.** Runs can be stopped and resumed at any time.
  Progress is saved every 100 messages.

## Recovery

| Situation | What to do |
| --- | --- |
| Script interrupted | Re-run the same command. Completed messages are skipped. |
| Some messages failed to archive | Check `status`, then re-run `archive`. Messages deleted from Gmail after listing are recorded as `not found in Gmail` and are not retried. |
| Verification failures | Fix the cause, then re-run `archive`. Messages that failed verification are re-uploaded. Then run `verify` again. |
| Revoke OAuth access | Delete `token_*.json` and remove the app at <https://myaccount.google.com/permissions>. |
| Restore a message to Gmail | Within 30 days, move it out of Trash in Gmail. After that, import the `.eml` from S3 using a mail client. |
| Workflow stopped at `max_runtime` | Run the workflow again. It continues from the state database in S3. |
| Lost `archive_state.sqlite3` | Objects in S3 still carry the Gmail ID and SHA-256 in their metadata. Do not run `cleanup` without a valid manifest. |

## Troubleshooting

| Error | Cause and fix |
| --- | --- |
| `ModuleNotFoundError: No module named 'google'` | The virtual environment isn't active. Run `source .venv/bin/activate`, or run `.venv/bin/python archive.py ...`. |
| `... is not a Desktop OAuth client (found: service_account)` | `credentials.json` is a service account key. Create an **OAuth client ID → Desktop app** instead. Delete the service account key in Cloud Console. |
| `Error 403: access_denied` ("can only be accessed by developer-approved testers") | Your Gmail address isn't a test user. Add it under **Google Auth Platform → Audience → Test users**, in the project that owns `credentials.json`. |
| `Gmail token ... could not be refreshed` / `invalid_grant` | The token expired (7 days in Testing mode) or access was revoked. Run `python archive.py --mode login`. Update the GitHub secrets if you use the workflows. |
| `Cannot access bucket` | Check the bucket name, the `--region`, and your AWS credentials (`aws sts get-caller-identity`). |
| Many `Gmail API 429` warnings | You are hitting Gmail's rate limit. Lower `--workers`. |
| `No valid Gmail token ... no browser is available in CI` | The `GMAIL_TOKEN_READONLY` or `GMAIL_TOKEN_MODIFY` secret is missing or invalid. |

## GitHub Actions

Two workflows run the tool from GitHub. Both are triggered manually from the
**Actions** tab (**Run workflow**):

| Workflow | Gmail access | What it does |
| --- | --- | --- |
| **Ingest Gmail to S3** (`ingest.yml`) | read-only | Runs `archive`, then `verify`. Each run picks up mail that arrived since the last run. Inputs: `query`, `limit`, `workers`, `storage_class`, `max_runtime`, `run_verify`. |
| **Clean up Gmail mailbox** (`cleanup.yml`) | modify | Runs `cleanup`. Defaults to a dry run. A real run needs `dry_run` unchecked and `confirm` set to exactly `TRASH`. |

How the workflows behave:

- **Configuration is checked first.** The **Check configuration** step lists every
  missing setting before anything else runs.
- **State is shared through S3.** Each run downloads
  `s3://YOUR_BUCKET/state/archive_state.sqlite3`, works on it, and uploads it
  again, even if the run fails part-way. If the download fails, the upload is
  skipped, so a blank database never overwrites real progress. Bucket versioning
  keeps older copies.
- **Runs never overlap.** Both workflows share one concurrency group, so a second
  run waits for the first to finish.
- **Long jobs stop cleanly.** GitHub-hosted jobs are limited to 6 hours. Each run
  stops after `max_runtime` minutes and saves its progress. A cancelled run also
  saves its progress. Run the workflow again to continue. For the first full
  migration of a large mailbox, running from your Mac is simpler.

### Required settings

Add these under **Settings → Secrets and variables → Actions**:

| Setting | Type | Needed by | Value |
| --- | --- | --- | --- |
| `AWS_REGION` | variable or secret | both | Bucket region, e.g. `us-east-1` |
| `S3_BUCKET` | variable or secret | both | Bucket name, e.g. `gmailstore` |
| `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` | secrets | both | IAM user access keys (option A below) |
| `AWS_ROLE_ARN` | secret | both | IAM role for OIDC (option B below). If set, it is used instead of access keys. |
| `GMAIL_TOKEN_READONLY` | secret | ingest | Contents of `token_readonly.json` |
| `GMAIL_TOKEN_MODIFY` | secret | cleanup (real runs) | Contents of `token_modify.json`. Can be a repository secret or a `mailbox-cleanup` environment secret. |

### 1. Create the Gmail tokens

While the OAuth app is in **Testing** mode, Google expires its tokens after 7 days.
That would break the workflows. Publish the app first:

1. Open the consent screen settings. In newer consoles, go to **Google Auth Platform → Audience**.
2. Click **Publish app** and set the status to **In production**.

For personal use you don't need Google's verification. You'll still see the
"Google hasn't verified this app" warning when you sign in.

Then create both token files on your Mac and store them as secrets:

```bash
python archive.py --mode login     # browser opens twice: read-only, then modify
gh secret set GMAIL_TOKEN_READONLY < token_readonly.json
gh secret set GMAIL_TOKEN_MODIFY < token_modify.json
```

If you published the app *after* creating the tokens, delete `token_*.json` and
run `--mode login` again. Tokens issued in Testing mode still expire after 7 days.

### 2. Give GitHub access to AWS

Choose one option.

#### Option A: access keys (simplest)

Create an IAM user with the permissions in `aws/github-s3-policy.json` (edit the
bucket name if yours differs). Then store its keys and the other settings:

```bash
gh secret set AWS_ACCESS_KEY_ID          # paste when prompted
gh secret set AWS_SECRET_ACCESS_KEY      # paste when prompted
gh secret set AWS_REGION --body us-east-1
gh secret set S3_BUCKET --body gmailstore
```

Access keys don't expire, so treat them like a password. Rotate them
periodically, or move to option B.

#### Option B: OIDC role (recommended)

GitHub signs in to an IAM role with short-lived credentials. No AWS keys are
stored in GitHub.

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# One time per AWS account (skip if the GitHub OIDC provider already exists)
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com

sed "s/ACCOUNT_ID/$ACCOUNT_ID/" aws/github-oidc-trust.json > /tmp/gmail-archive-trust.json
aws iam create-role --role-name gmail-archive-github \
  --assume-role-policy-document file:///tmp/gmail-archive-trust.json \
  --max-session-duration 21600
aws iam put-role-policy --role-name gmail-archive-github \
  --policy-name gmail-archive-s3 \
  --policy-document file://aws/github-s3-policy.json

gh secret set AWS_ROLE_ARN --body "arn:aws:iam::$ACCOUNT_ID:role/gmail-archive-github"
gh secret set AWS_REGION --body us-east-1
gh secret set S3_BUCKET --body gmailstore
```

About these policies:

- **Trust policy:** only allows runs from the `main` branch and from the
  `mailbox-cleanup` environment of `shehuj/Gmail-to-Amazon_Archive`.
- **Permissions policy:** only allows listing the bucket and reading and writing
  its objects.
- **Session length:** `--max-session-duration 21600` lets a run keep its AWS
  credentials for up to 6 hours.

Once `AWS_ROLE_ARN` is set, the workflows use it automatically. You can then
delete the access key secrets and deactivate the IAM user's keys.

### 3. Protect the cleanup workflow (optional)

The cleanup job runs in the `mailbox-cleanup` environment. GitHub creates it on
the first run, or you can create it yourself:

```bash
gh api -X PUT repos/shehuj/Gmail-to-Amazon_Archive/environments/mailbox-cleanup
```

Then go to **Settings → Environments → mailbox-cleanup** and set it up:

- **Required reviewer:** add yourself. Every cleanup run then waits for your
  approval before it can touch the mailbox. This is available on public
  repositories and on paid plans for private ones.
- **Modify token:** move `GMAIL_TOKEN_MODIFY` from the repository secrets into this
  environment, so only the cleanup job can read it:

  ```bash
  gh secret set GMAIL_TOKEN_MODIFY --env mailbox-cleanup < token_modify.json
  gh secret delete GMAIL_TOKEN_MODIFY
  ```

### 4. Seed the state database

If you have already archived from your Mac, upload your local state so the
workflows continue from it instead of listing and archiving from scratch:

```bash
aws s3 cp archive_state.sqlite3 s3://gmailstore/state/archive_state.sqlite3
```

From then on, the S3 copy is the source of truth. If you also run the script
locally:

1. Download the state database from S3 first:
   `aws s3 cp s3://gmailstore/state/archive_state.sqlite3 .`
2. Run the script.
3. Upload the state database back to S3.
4. Never run locally while a workflow is running.

### 5. Run the workflows

In the **Actions** tab, choose a workflow and click **Run workflow**. Or use the
command line:

```bash
gh workflow run ingest.yml -f limit=5000 -f workers=12
gh workflow run cleanup.yml                                  # dry run
gh workflow run cleanup.yml -f dry_run=false -f confirm=TRASH -f limit=1000
gh run watch
```

Each run writes the archive state summary to the run's summary page.

### Workflow troubleshooting

| Error | Fix |
| --- | --- |
| `Check configuration`: `Missing ...` | Add each listed setting (see [Required settings](#required-settings)). |
| `Input required and not supplied: aws-region` | The run used an older workflow version. Push the latest workflows to `main` and run again. |
| `Could not assume role` / `Not authorized to perform sts:AssumeRoleWithWebIdentity` | The OIDC trust policy doesn't match the repository, branch or environment. Run from `main`, and check `aws/github-oidc-trust.json`. |
| `Cannot access bucket` / `AccessDenied` | Check `S3_BUCKET` and `AWS_REGION`, and that the IAM user or role has the permissions in `aws/github-s3-policy.json`. |
| `Gmail token ... could not be refreshed` | The token expired or was revoked. Publish the app to **In production**, run `--mode login`, and update the token secrets. |
| `No state database at ...` (cleanup) | Run the ingest workflow first, or seed the state database (step 4). |
| Run stopped with `--max-runtime reached` | Expected for long runs. Run the workflow again to continue. |
