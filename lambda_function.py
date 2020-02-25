import os
import boto3
from datetime import datetime, timedelta

# Load Environment Variables
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_BEFORE_KEY = os.environ['S3_BEFORE_KEY']
S3_AFTER_KEY = os.environ['S3_AFTER_KEY']
S3_BEFORE_FORMAT = os.environ['S3_BEFORE_FORMAT']
FROM_DATE = os.environ['FROM_DATE']
TO_DATE = os.environ['TO_DATE']
DELETE_FRAG = os.environ['DELETE_FRAG']

def date_range(from_date: datetime, to_date: datetime):
    """
    Create Generator Range of Date

    Args:
        from_date (datetime) : datetime param of start date
        to_date (datetime) : datetime param of end date
    Returns:
        Generator
    """
    diff = (to_date - from_date).days + 1
    return (from_date + timedelta(i) for i in range(diff))

def pre_format_key():
    """
    Reformat S3 Key Parameter given 

    Args:
        None
    Returns:
        None
    """
    global S3_BEFORE_KEY
    global S3_AFTER_KEY
    if S3_BEFORE_KEY[-1] == '/':
        S3_BEFORE_KEY = S3_BEFORE_KEY[:-1]
    if S3_AFTER_KEY[-1] == '/':
        S3_AFTER_KEY = S3_AFTER_KEY[:-1]


def change_s3_key(date: datetime):
    """
    Change S3 key from datetime format to Hive format at specific date

    Args:
        date (datetime) : target date to change key
    Returns:
        None
    """
    before_date_str = datetime.strftime(date, S3_BEFORE_FORMAT)
    print('Change following date key format : {}'.format(before_date_str))
    before_path = f'{S3_BEFORE_KEY}/{before_date_str}/'
    after_path = "{}/year={}/month={}/date={}".format(
        S3_AFTER_KEY, date.strftime('%Y'), date.strftime('%m'), date.strftime('%d')
    )
    
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Delimiter="/",
        Prefix=before_path
    )
    try:
        for content in response["Contents"]:
            key = content['Key']
            file_name = key.split('/')[-1]
            after_key = f'{after_path}/{file_name}'
            s3.copy_object(
                Bucket=S3_BUCKET_NAME,
                CopySource={'Bucket': S3_BUCKET_NAME, 'Key': key},
                Key=after_key
            )
            if DELETE_FRAG == 'True':
                s3.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
    except Exception as e:
        print(e)
        return


def lambda_handler(event, context):
    pre_format_key()
    from_date = datetime.strptime(FROM_DATE, "%Y%m%d")
    to_date = datetime.strptime(TO_DATE, "%Y%m%d")
    for date in date_range(from_date, to_date):
        change_s3_key(date)
