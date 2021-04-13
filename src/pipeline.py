import csv
import requests
import boto3
import os
import random
from botocore.exceptions import ClientError

FREIGHT_CSV_URL = "https://data.transportation.gov/api/views/uxue-t623/rows.csv?accessType=DOWNLOAD"
CODES_PDF_URL = "https://www.bts.gov/sites/bts.dot.gov/files/docs/browse-statistical-products-and-data/transborder-freight-data/220171/codes-north-american-transborder-freight-raw-data.pdf"

print("Getting codes pdf...")
try:
    pdf_response = requests.get(CODES_PDF_URL)
    with open('codes.pdf', 'wb') as f:
        f.write(pdf_response.content)
    print("Got it!")
except:
    print("Could not download US Freight Codes PDF")

# Get env variables
KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
SNS_ARN = os.environ.get('SNS_ARN')
ROLE_ARN = os.environ.get('ROLE_ARN')
REGION = os.environ.get('REGION')

# upload to s3 for textract
try:
    s3_resource = boto3.resource(
        's3',
        region_name=REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET_KEY
    )

    s3_resource.Bucket(BUCKET_NAME).put_object(
        Key='raw/codes.pdf',
        Body=open('codes.pdf', 'rb')
    )
    print("US Freight Codes PDF uploaded to S3!")
except:
    print("Could not upload Freight Codes PDF to S3")

# Calling textract
textract = boto3.client('textract', region_name=REGION, aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET_KEY)
job_number = random.randint(0, 1000)
job_number = str(job_number)
textract_response = textract.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': BUCKET_NAME,
            'Name': 'raw/codes.pdf',
        }
    },
    FeatureTypes=[
        'TABLES',
        'FORMS'
    ],
    ClientRequestToken=job_number,
    JobTag='FreightCodes',
    NotificationChannel={
        'SNSTopicArn': SNS_ARN,
        'RoleArn': ROLE_ARN
    },
    OutputConfig={
        'S3Bucket': BUCKET_NAME,
        'S3Prefix': 'processed'
    }
)

job_id = textract_response['JobId']
print(job_id)


# print("Done with codes! Now on to the data...")

# with requests.Session() as s:
#     download = s.get(FREIGHT_CSV_URL)

#     decoded_content = download.content.decode('utf-8')

#     cr = csv.reader(decoded_content.splitlines(), delimiter=',')
#     my_list = list(cr)
#     for row in my_list:
#         print(row)
