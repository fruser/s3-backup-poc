# s3-backup-poc
Simple S3 Backup Proof Of Concept

## Requirements

* Python 3.4+ is required to run this POC. You can use [Miniconda](https://conda.io/en/latest/miniconda.html) distribution
* 2 separate test AWS accounts
* IAM roles for these AWS accounts with Administrative
* [AWS CLI](https://aws.amazon.com/cli/) with 2 profiles configured against these AWS accounts

## Usage


Install required packages using pip, preferably, within Python
environment:

`pip install -r requirements.txt`

Script's description:

```
usage: setup.py [-h] --source SOURCE --dest DEST --src_profile SRC_PROFILE
                --dest_profile DEST_PROFILE [--src_region SRC_REGION]
                [--dest_region DEST_REGION] --src_accountid SRC_ACCOUNTID
                --dest_accountid DEST_ACCOUNTID [--cleanup]

optional arguments:
  -h, --help            show this help message and exit
  --source SOURCE       Name of the source bucket
  --dest DEST           Name of the destination bucket
  --src_profile SRC_PROFILE
                        AWS CLI profile to use for the source data
  --dest_profile DEST_PROFILE
                        AWS CLI profile to use for the destination data
  --src_region SRC_REGION
                        Region for the source bucket. Default is `ca-
                        central-1`
  --dest_region DEST_REGION
                        Region for the source bucket. Default is `ca-
                        central-1`
  --src_accountid SRC_ACCOUNTID
                        Account id for the source AWS account
  --dest_accountid DEST_ACCOUNTID
                        Account id for the destination AWS account
  --cleanup             Remove existing resources
```

Example:

```
python setup.py \
    --source source_bucket_name \
    --dest destination_bucket_name \
    --src_profile source_profile \
    --dest_profile destination_profile \
    --src_accountid source_account_id \
    --dest_accountid destination_account_id \
    --cleanup
```
