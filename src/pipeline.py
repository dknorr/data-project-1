import csv
from csv import reader
import requests
import boto3
import os
import random
from botocore.exceptions import ClientError
import mysql.connector


FREIGHT_CSV_URL = "https://data.transportation.gov/api/views/uxue-t623/rows.csv?accessType=DOWNLOAD"
CODES_PDF_URL = "https://www.bts.gov/sites/bts.dot.gov/files/docs/browse-statistical-products-and-data/transborder-freight-data/220171/codes-north-american-transborder-freight-raw-data.pdf"

print("Getting codes PDF...")
try:
    pdf_response = requests.get(CODES_PDF_URL)
    with open('codes.pdf', 'wb') as f:
        f.write(pdf_response.content)
    print("Got it!")
except:
    print("Could not download US Freight Codes PDF")

print("Getting Freight data CSV...")
try:
    csv_response = requests.get(FREIGHT_CSV_URL)
    with open('freight-data.csv', 'wb') as f:
        f.write(csv_response.content)
    print("Got it!")
except:
    print("Could not download US Freight Data CSV")

# Get env variables
KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
SNS_ARN = os.environ.get('SNS_ARN')
ROLE_ARN = os.environ.get('ROLE_ARN')
REGION = os.environ.get('REGION')
RDS_URL = os.environ.get('RDS_URL')
RDS_USER = os.environ.get('RDS_USER')
RDS_PASSWORD = os.environ.get('RDS_PASSWORD')

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
# textract = boto3.client('textract', region_name=REGION, aws_access_key_id=KEY,
#                         aws_secret_access_key=SECRET_KEY)
# job_number = random.randint(0, 1000)
# job_number = str(job_number)
# textract_response = textract.start_document_analysis(
#     DocumentLocation={
#         'S3Object': {
#             'Bucket': BUCKET_NAME,
#             'Name': 'raw/codes.pdf',
#         }
#     },
#     FeatureTypes=[
#         'TABLES',
#         'FORMS'
#     ],
#     ClientRequestToken=job_number,
#     JobTag='FreightCodes',
#     NotificationChannel={
#         'SNSTopicArn': SNS_ARN,
#         'RoleArn': ROLE_ARN
#     },
#     OutputConfig={
#         'S3Bucket': BUCKET_NAME,
#         'S3Prefix': 'processed'
#     }
# )

#job_id = textract_response['JobId']
#print(job_id)

print("Done with codes! Now on to the data...")

mydb = mysql.connector.connect(
  host=RDS_URL,
  user=RDS_USER,
  password=RDS_PASSWORD
)

mycursor = mydb.cursor()
mycursor.execute("SHOW DATABASES")
db_exists = False

for x in mycursor:
    if 'freight' in x:
        db_exists = True

if(db_exists):
    mycursor.execute("USE freight")
else:
    mycursor.execute("CREATE DATABASE freight")
    mycursor.execute("USE freight")

disagmot_exists = False
commodity2_exists = False
freightlog_exists = False

mycursor.execute("SHOW TABLES")
for x in mycursor:
  if 'disagmot' in x:
      disagmot_exists = True
  if 'commodity2' in x:
      commodity2_exists = True
  if 'freightlog' in x:
      freightlog_exists = True

if(not disagmot_exists):
    mycursor.execute("CREATE TABLE `disagmot` ( `key` INT NOT NULL AUTO_INCREMENT , `code` INT NULL DEFAULT NULL , `description` TINYTEXT NULL DEFAULT NULL , PRIMARY KEY (`key`)) ENGINE = InnoDB")
if(not commodity2_exists):
    mycursor.execute("CREATE TABLE `commodity2` ( `key` INT NOT NULL AUTO_INCREMENT , `code` INT NULL DEFAULT NULL , `description` TEXT NULL DEFAULT NULL , PRIMARY KEY (`key`)) ENGINE = InnoDB")
if(not freightlog_exists):
    mycursor.execute("CREATE TABLE `freightlog` ( `key` INT NOT NULL AUTO_INCREMENT , `trdtype` INT NULL DEFAULT NULL , `depe` VARCHAR(4) NULL DEFAULT NULL , `commodity2` INT NULL DEFAULT NULL , `disagmot` INT NULL DEFAULT NULL , `country` INT NULL DEFAULT NULL , `value` INT NULL DEFAULT NULL , `shipwt` INT NULL DEFAULT NULL , `freight_charges` INT NULL DEFAULT NULL , `df` SET('1','2') NULL DEFAULT NULL , `contcode` SET('X','0','1') NULL DEFAULT NULL , `month` INT NULL DEFAULT NULL , `year` YEAR NULL DEFAULT NULL , PRIMARY KEY (`key`)) ENGINE = InnoDB")

# Filling in DISAGMOT table. Data comes from Mock-Textract table-10.csv
mycursor.execute("TRUNCATE TABLE `disagmot`")
with open('mock-textract/table-10.csv', 'r') as read_obj:
    sql = "INSERT INTO `disagmot` (`key`, `code`, `description`) VALUES (%s, %s, %s)"
    csv_reader = reader(read_obj)
    next(csv_reader) #to skip header
    for row in csv_reader:
        val = (None, int(row[0].strip()), row[1].strip())
        # print(val)
        mycursor.execute(sql, val)

# Filling in COMMODITY2 table. Data comes from Mock-Textract table-2 to table-6.csv
mycursor.execute("TRUNCATE TABLE `commodity2`")
files = ['mock-textract/table-2.csv', 'mock-textract/table-3.csv', 'mock-textract/table-4.csv', 'mock-textract/table-5.csv', 'mock-textract/table-6.csv']
for this_file in files:
    with open(this_file, 'r') as read_obj:
        sql = "INSERT INTO `commodity2` (`key`, `code`, `description`) VALUES (%s, %s, %s)"
        csv_reader = reader(read_obj)
        if this_file == 'mock-textract/table-2.csv':
            next(csv_reader) #to skip header
        for row in csv_reader:
            if(row[0] == ""):
                next
            else:
                val = (None, int(row[0].strip()), row[1].strip())
                # print(val)
                mycursor.execute(sql, val)

# Filling in FREIGHTLOG table. Data comes from freight-data.csv
mycursor.execute("TRUNCATE TABLE `freightlog`")
# Only loading some data to test with
cnt = 0
with open('freight-data.csv', 'r') as read_obj:
    sql = "INSERT INTO `freightlog` (`key`, `trdtype`, `depe`, `commodity2`, `disagmot`, `country`, `value`, `shipwt`, `freight_charges`, `df`, `contcode`, `month`, `year`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    csv_reader = reader(read_obj)
    next(csv_reader) #to skip header
    for row in csv_reader:
        val = (None, int(row[0].strip()), row[1].strip(), int(row[2].strip()), int(row[3].strip()), int(row[4].strip()), int(row[5].strip()), int(row[6].strip()), int(row[7].strip()), row[8].strip(), row[9].strip(), int(row[10].strip()), int(row[11].strip()))
        # print(val)
        mycursor.execute(sql, val)
        cnt += 1
        if(cnt >= 50):
          break  

mydb.commit()

mycursor.close()



# with requests.Session() as s:
#      download = s.get(FREIGHT_CSV_URL)

#      decoded_content = download.content.decode('utf-8')

#      cr = csv.reader(decoded_content.splitlines(), delimiter=',')
#      my_list = list(cr)
#      for row in my_list:
#          print(row)

