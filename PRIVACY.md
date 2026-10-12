# Privacy Policy

Last updated: October 11, 2026

Gmail-to-Amazon Archive ("the app") is a personal, self-hosted tool. It is not a
service offered to the public. It is run only by its owner, on their own computer
or in their own GitHub Actions workflows, to archive their own Gmail account.

## What data the app accesses

With your permission, granted through Google's OAuth consent screen, the app uses
the Gmail API to:

- **Read your messages** (`gmail.readonly` scope). It downloads each email in raw
  form, including headers, body and attachments, and its labels.
- **Move messages to Trash** (`gmail.modify` scope). This is used only when you run
  the cleanup step, and only for messages that were archived and verified first.
  The app never permanently deletes mail and never sends, drafts or changes
  messages.

## How the data is used and stored

- Messages are uploaded only to an **Amazon S3 bucket that you own and control**.
  They are encrypted at rest.
- A local state file (`archive_state.sqlite3`) records the message IDs, checksums
  and archive status. It is stored on your computer and in your own S3 bucket.
- OAuth tokens are stored on your computer and, if you use the workflows, as
  encrypted secrets in your own GitHub repository.

## What the app does not do

- It does not send your data to the app's developer or to any third party, other
  than Google (Gmail API) and Amazon Web Services (your S3 bucket).
- It does not use your data for advertising, analytics, profiling or training AI
  or machine learning models.
- It does not sell, rent or share your data.

The app's use of information received from Google APIs adheres to the
[Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy),
including the Limited Use requirements.

## Revoking access and deleting data

- **Revoke the app's access to Gmail** at any time at
  <https://myaccount.google.com/permissions>, and delete the local `token_*.json`
  files.
- **Delete archived data** by deleting the objects in your S3 bucket and the
  `archive_state.sqlite3` file.

## Contact

For questions about this policy, open an issue at
<https://github.com/shehuj/Gmail-to-Amazon_Archive/issues>.
