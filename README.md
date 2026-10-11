# Gmail-to-Amazon_Archive

Your implementation package is ready

Gmail → S3 Archive Starter

Python · Gmail API · Boto3 · SQLite · AWS S3

4 files, including a resumable migration script and setup guide

Download the Gmail-to-S3 starter project⁠￼

The package includes:

File

Purpose

archive.py

Archive, verify and gated cleanup modes

README.md

AWS setup, Google OAuth, commands and recovery steps

requirements.txt

Python dependencies

.gitignore

Helps prevent committing credentials and local state


The script has been syntax-checked. It has not been tested against your actual Gmail account or AWS environment.

Follow these steps on your Mac

1. Extract the project

Download and extract the ZIP. Open Terminal in the extracted gmail_s3_archive directory.

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

2. Prepare Google and AWS

* In  ⁠Google Cloud Console, enable the Gmail API and create a Desktop OAuth client. Save its JSON file as credentials.json.
* Configure AWS CLI credentials using your existing AWS profile or IAM Identity Center.
* Create a private S3 bucket with public access blocked, encryption and versioning enabled. AWS recommends private access, least-privilege permissions and appropriate encryption for S3 data.   

3. Run the migration in safe stages

First, archive your messages using read-only Gmail access:

python archive.py --mode archive \
  --bucket YOUR_BUCKET_NAME \
  --region us-east-1

  Next, run the integrity verification:

python archive.py --mode verify \
  --bucket YOUR_BUCKET_NAME \
  --region us-east-1

The verification downloads the archived objects and compares their SHA-256 checksums against the local manifest. Review all failures before proceeding.

Only after successful verification should you authorize the separate cleanup phase. That phase moves verified messages to Gmail Trash using the Gmail API’s message-trash operation.  

Do not empty Trash automatically. First confirm the archived messages and attachments can be retrieved from S3 and that Gmail storage has decreased as expected.

Two things to know before starting

* The default migration excludes messages already in Spam or Trash. It covers the other messages returned by Gmail, including archived mail.
* Your local archive_state.sqlite3 database is important. Back it up securely and keep it alongside the archive records; it tracks which messages have been archived and verified.

My recommendation is to run the first migration from your Mac, then decide whether to automate future archiving. That keeps the initial setup simple and avoids unnecessary Lambda, scheduling and monitoring costs.

