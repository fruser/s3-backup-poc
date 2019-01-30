import argparse
import boto3
import json
import logging
from pathlib import Path
import uuid

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def aws_client(profile, service='s3'):
    logger.debug('Configuring S3 client for {0} profile...'.format(profile))
    session = boto3.Session(profile_name = profile)
    return session.client(service)


def cleanup(saved_state):
    logger.info('Clean up...')
    with open(saved_state) as infile:
        state = json.loads(infile.read())
        client = aws_client()


def enable_replicaton(source, dest):
    pass


class Bucket:

    source_account = ''

    def __init__(self, name, region, profile, lifecycle_rules, iam, bucket_policy):
        self.bucket = name + '-' + uuid.uuid4().hex
        self.region = region
        self.profile = profile
        self.lifecycle_rules = lifecycle_rules
        self.iam = {
            'role_name': 'S3CrossAccountReplicationIamRole' + '-' + uuid.uuid4().hex,
            'role': iam
        }
        self.bucket_policy = bucket_policy
        self.save_state()
        self.client = aws_client(profile)

    def create_bucket(self):
        logger.info('Creating {0} bucket...'.format(self.bucket))
        self.client.create_bucket(
            ACL='private',
            Bucket=self.bucket,
            CreateBucketConfiguration={
                'LocationConstraint': self.region
            }
        )

    def save_state(self):
        with open('setup_state.json', 'a+') as outfile:
            feeds = json.loads(outfile)
            feeds.append(self.__dict__)
            json.dump(feeds, outfile, ensure_ascii = False)

    def enable_versioning(self):
        logger.info('Enabling versioning for {0} bucket'.format(self.bucket))
        self.client.put_bucket_versioning(
                Bucket = self.bucket,
                VersioningConfiguration = {
                    'Status': 'Enabled'
                }
        )

    def enable_lifecycle_policy(self):
        logger.info('Enabling lifecycle policy for {0} bucket'.format(self.bucket))
        self.client.put_bucket_lifecycle_configuration(
                Bucket = self.bucket,
                LifecycleConfiguration = {
                    'Rules': self.lifecycle_rules
                }
        )

    def create_iam_role(self):
        logger.info('Adding IAM role for {0} profile'.format(self.profile))
        self.client = aws_client(self.profile, 'iam')
        self.client.create_role(
            Path='/service-role/',
            RoleName=self.iam['role_name'],
            AssumeRolePolicyDocument=json.dumps(self.iam['role']),
            Description='S3 Cross-Account Replication IAM Role',
        )

    def apply_security(self):
        logger.info('Adding policy to {0} bucket'.format(self.bucket))
        self.client.put_bucket_policy(
                Bucket = self.bucket,
                ConfirmRemoveSelfBucketAccess = False,
                Policy = json.dumps(self.bucket_policy)
        )


def main():
    # TODO Implement Cleanup logic
    # TODO Implement Continue logic

    a = argparse.ArgumentParser()
    a.add_argument('--source', required=True,
                   help='Name of the source bucket')
    a.add_argument('--dest', required=True,
                   help='Name of the destination bucket')
    a.add_argument('--src_profile', required=True,
                   help='AWS CLI profile to use for the source data')
    a.add_argument('--dest_profile', required=True,
                   help='AWS CLI profile to use for the destination data')
    a.add_argument('--src_region', default='ca-central-1',
                   help='Region for the source bucket. Default is `ca-central-1`')
    a.add_argument('--dest_region', default='ca-central-1',
                   help='Region for the source bucket. Default is `ca-central-1`')
    a.add_argument('--src_accountid', required=True,
                   help='Account id for the source AWS account')
    a.add_argument('--cleanup', required=False, action='store_true',
                   help = 'Remove existing resources')
    args = a.parse_args()
    logger.debug(args)

    state = 'setup_state.json'
    if Path(state).is_file():
        if args.cleanup:
            cleanup(state)
        else:
            logger.info('Cannot continue. Please add `--cleanup` flag and re-run the script...')
            quit()
    else:
        with open(state, mode = 'w', encoding = 'utf-8') as f:
            f.write(json.dumps([]))

    Bucket.source_account = args.src_accountid

    source_lifecycle_policy = [
        {
            'Expiration': {
                'ExpiredObjectDeleteMarker': True
            },
            'ID': 'version-expiration-policy',
            'Filter': {'Prefix': ''},
            'Status': 'Enabled',
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': 1
            },
            'AbortIncompleteMultipartUpload': {
                'DaysAfterInitiation': 7
            }
        }
    ]

    dest_lifecycle_policy = [
        {
            'ID': 'archival-rule',
            'Filter': {
                'Prefix': ''
            },
            'Status': 'Enabled',
            'Transitions': [
                {
                    'Days': 31,
                    'StorageClass': 'GLACIER'
                }
            ],
            'NoncurrentVersionTransitions': [
                {
                    'NoncurrentDays': 31,
                    'StorageClass': 'GLACIER'
                }
            ],
            'AbortIncompleteMultipartUpload': {
                'DaysAfterInitiation': 7
            }
        }
    ]

    source_iam_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Action': [
                    's3:Get*',
                    's3:ListBucket'
                ],
                'Effect': 'Allow',
                'Resource': [
                    'arn:aws:s3:::{0}'.format(args.source),
                    'arn:aws:s3:::{0}/*'.format(args.source)
                ]
            },
            {
                'Action': [
                    's3:ReplicateObject',
                    's3:ReplicateDelete',
                    's3:ReplicateTags',
                    's3:GetObjectVersionTagging',
                    's3:ObjectOwnerOverrideToBucketOwner'
                ],
                'Effect': 'Allow',
                'Resource': 'arn:aws:s3:::{0}/*'.format(args.dest)
            }
        ]
    }

    dest_bucket_policy = {
        'Version': '2008-10-17',
        'Id': 'S3-Console-Replication-Policy',
        'Statement': [
            {
                'Sid': 'S3ReplicationPolicy',
                'Effect': 'Allow',
                'Principal': {
                    'AWS': 'arn:aws:iam::{0}:root'.format(Bucket.source_account)
                },
                'Action': [
                    's3:GetBucketVersioning',
                    's3:PutBucketVersioning',
                    's3:ReplicateObject',
                    's3:ReplicateDelete',
                    's3:ObjectOwnerOverrideToBucketOwner'
                ],
                'Resource': [
                    'arn:aws:s3:::{0}'.format(args.dest),
                    'arn:aws:s3:::{0}/*'.format(args.dest)
                ]
            }
        ]
    }

    source_bucket_policy = {}
    dest_iam_policy = {}

    source = Bucket(args.source, args.src_region, args.src_profile, source_lifecycle_policy, source_iam_policy, source_bucket_policy)
    dest = Bucket(args.dest, args.dest_region, args.dest_profile, dest_lifecycle_policy, dest_iam_policy, dest_bucket_policy)

    for bucket in [source, dest]:
        bucket.create_bucket()
        bucket.enable_versioning()
        bucket.enable_lifecycle_policy()

    dest.apply_security()
    source.create_iam_role()


if __name__ == '__main__':
    main()
