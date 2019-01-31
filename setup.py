import argparse
import boto3
from botocore.exceptions import ClientError
import json
import logging
from pathlib import Path
import uuid
import os

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

STATE_FILE = 'setup_state.json'


def aws_client(profile, service='s3'):
    logger.debug('Configuring S3 client for {0} profile...'.format(profile))
    session = boto3.Session(profile_name = profile)
    return session.client(service)


def cleanup(saved_state):
    logger.info('Clean up...')
    with open(saved_state) as infile:
        for line in infile:
            state = json.loads(line)
            client = aws_client(state['profile'])
            bucket = state['bucket']['name']

            logger.info('Deleting objects from {0} bucket...'.format(bucket))
            paginator = client.get_paginator('list_object_versions')
            response_iterator = paginator.paginate(Bucket = bucket)
            try:
                for response in response_iterator:
                    versions = response.get('Versions', [])
                    versions.extend(response.get('DeleteMarkers', []))
                    for version in versions:
                        logger.debug('Deleting {} version {}...'.format(version['Key'], version['VersionId']))
                        client.delete_object(Bucket = bucket, Key = version['Key'], VersionId = version['VersionId'])

                response = client.delete_bucket(
                                Bucket=state['bucket']['name']
                            )
                logger.debug(response)
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    logger.info("Bucket no longer exists...")


def append_record(record):
    with open(STATE_FILE, 'a') as f:
        json.dump(record, f)
        f.write(os.linesep)


class Bucket:

    source_account = ''
    dest_account = ''

    def __init__(self, name, region, profile, lifecycle_rules, iam, bucket_policy):
        self.bucket = {
            'name': name,
            'arn': ''
        }
        self.region = region
        self.profile = profile
        self.lifecycle_rules = lifecycle_rules
        self.iam = {
            'role_name': 'S3CrossAccountReplicationIamRole' + '-' + uuid.uuid4().hex[:8],
            'role_arn': '',
            'policy_arn': '',
            'policy': iam,
            'policy_name': 'S3CrossAccountReplicationIamPolicy' + '-' + uuid.uuid4().hex[:8]
        }
        self.bucket_policy = bucket_policy
        self.save_state()
        self.client = aws_client(profile)

    def create_bucket(self):
        logger.info('Creating {0} bucket...'.format(self.bucket))
        response = self.client.create_bucket(
            ACL='private',
            Bucket=self.bucket['name'],
            CreateBucketConfiguration={
                'LocationConstraint': self.region
            }
        )
        self.bucket['arn'] = 'arn:aws:s3:::' + self.bucket['name']
        logger.debug('Response: ', response)

    def save_state(self):
        append_record(self.__dict__)

    def enable_versioning(self):
        logger.info('Enabling versioning for {0} bucket'.format(self.bucket))
        self.client.put_bucket_versioning(
                Bucket = self.bucket['name'],
                VersioningConfiguration = {
                    'Status': 'Enabled'
                }
        )

    def enable_lifecycle_policy(self):
        logger.info('Enabling lifecycle policy for {0} bucket'.format(self.bucket))
        self.client.put_bucket_lifecycle_configuration(
                Bucket = self.bucket['name'],
                LifecycleConfiguration = {
                    'Rules': self.lifecycle_rules
                }
        )

    def enable_replication(self, destination_arn):
        logger.info('Adding Replication for {0} bucket'.format(self.bucket))
        self.client = aws_client(self.profile, 's3')
        response = self.client.put_bucket_replication(
                Bucket = self.bucket['name'],
                ReplicationConfiguration = {
                    'Role': self.iam['role_arn'],
                    'Rules': [
                        {
                            'ID': 'ReplicationConfiguration' + '-' + uuid.uuid4().hex[:8],
                            'Priority': 123,
                            'Filter': {
                                'Prefix': ''
                            },
                            'Status': 'Enabled',
                            'Destination': {
                                'Bucket': destination_arn,
                                'Account': Bucket.dest_account,
                                'StorageClass': 'STANDARD',
                                'AccessControlTranslation': {
                                    'Owner': 'Destination'
                                }
                            },
                            'DeleteMarkerReplication': {
                                'Status': 'Disabled'
                            }
                        }
                    ]
                }
        )
        logger.debug('Response: ', response)

    def create_policy(self):
        logger.info('Adding IAM policy for {0} profile'.format(self.profile))
        response = self.client.create_policy(
                PolicyName = self.iam['policy_name'],
                Path = '/service-role/',
                PolicyDocument = json.dumps(self.iam['policy']),
                Description = 'S3 Cross-Account Replication IAM Policy'
        )
        self.iam['policy_arn'] = response['Policy']['Arn']
        logger.debug('Response: ', response)

    def create_iam_role(self):
        logger.info('Adding IAM role for {0} profile'.format(self.profile))
        self.client = aws_client(self.profile, 'iam')
        self.create_policy()

        trust_relationship = {
          'Version': '2012-10-17',
          'Statement': [
            {
              'Effect': 'Allow',
              'Principal': {
                'Service': 's3.amazonaws.com'
              },
              'Action': 'sts:AssumeRole'
            }
          ]
        }

        response = self.client.create_role(
            Path='/service-role/',
            RoleName=self.iam['role_name'],
            AssumeRolePolicyDocument=json.dumps(trust_relationship),
            Description='S3 Cross-Account Replication IAM Role',
        )
        self.iam['role_arn'] = response['Role']['Arn']
        logger.debug('Response: ', response)

        response = self.client.attach_role_policy(
                RoleName = self.iam['role_name'],
                PolicyArn = self.iam['policy_arn']
        )
        logger.debug('Response: ', response)

    def apply_security(self):
        logger.info('Adding policy to {0} bucket'.format(self.bucket))
        self.client.put_bucket_policy(
                Bucket = self.bucket['name'],
                ConfirmRemoveSelfBucketAccess = False,
                Policy = json.dumps(self.bucket_policy)
        )


def main():
    # TODO Implement Continue logic
    # TODO Complete Clean Up logic

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
    a.add_argument('--dest_region', default='us-east-2',
                   help='Region for the source bucket. Default is `ca-central-1`')
    a.add_argument('--src_accountid', required=True,
                   help='Account id for the source AWS account')
    a.add_argument('--dest_accountid', required = True,
                   help = 'Account id for the destination AWS account')
    a.add_argument('--cleanup', required=False, action='store_true',
                   help = 'Remove existing resources')
    args = a.parse_args()
    logger.debug(args)

    if Path(STATE_FILE).is_file():
        if args.cleanup:
            cleanup(STATE_FILE)
            os.remove(STATE_FILE)
            logger.info('Cleanup complete...')
        else:
            logger.info('Cannot continue. Please add `--cleanup` flag and re-run the script...')
        quit()

    Bucket.source_account = args.src_accountid
    Bucket.dest_account = args.dest_accountid

    source_bucket = args.source + '-' + uuid.uuid4().hex
    dest_bucket = args.dest + '-' + uuid.uuid4().hex

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
                    'arn:aws:s3:::{0}'.format(source_bucket),
                    'arn:aws:s3:::{0}/*'.format(source_bucket)
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
                'Resource': 'arn:aws:s3:::{0}/*'.format(dest_bucket)
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
                    'arn:aws:s3:::{0}'.format(dest_bucket),
                    'arn:aws:s3:::{0}/*'.format(dest_bucket)
                ]
            }
        ]
    }

    source_bucket_policy = {}
    dest_iam_policy = {}

    source = Bucket(source_bucket, args.src_region, args.src_profile, source_lifecycle_policy, source_iam_policy, source_bucket_policy)
    dest = Bucket(dest_bucket, args.dest_region, args.dest_profile, dest_lifecycle_policy, dest_iam_policy, dest_bucket_policy)

    for bucket in [source, dest]:
        bucket.create_bucket()
        bucket.enable_versioning()
        bucket.enable_lifecycle_policy()

    dest.apply_security()
    source.create_iam_role()
    source.enable_replication(dest.bucket['arn'])

    logger.info('Setup is complete...')


if __name__ == '__main__':
    main()
