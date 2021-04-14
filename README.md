# data-project-1
Basic ETL Pipeline Section for DS3002 Data Project 1

## Overall Goals
The high level goal for this project was to take 2021 US Freight shipping data and ingest it alongside the Freight Codes used by the USDOT.
Yearly shipping data is available for download [here](https://data.transportation.gov/api/views/uxue-t623/rows.csv?accessType=DOWNLOAD). It is available as a CSV file. Unfortunately, the mappings for the shorthands used in this data, available [here](https://www.bts.gov/sites/bts.dot.gov/files/docs/browse-statistical-products-and-data/transborder-freight-data/220171/codes-north-american-transborder-freight-raw-data.pdf), is only made available as a pdf.

The plan was to upload the codes pdf to Amazon S3, trigger an Amazon Textract Job (an AWS serice which parses PDFs and can identify table data), which then sends an SNS message to a Lambda function when the PDF processing is complete. The Lambda function then checks the status of the Textract job, to be sure it succeeded, and then converts the Textract output block objects (delivered via JSON) into CSV and uploads them back to S3.

The pipeline then takes the parsed codes, uploads them as tables to an RDS intance, and then takes the yearly shipping data and uploads that to RDS with the shorthands replaced with the appropriate mappings.

## What Happened
I very quickly exceeded the limits of free tier Textract usage while I figured out how to integrate it. As such, I disabled the Lambda trigger and commented out the section of the pipeline code that called Textract. With a hefty bill from AWS in hand, I ended up mocking out the Textract service by making the results statically available in a directory. This is how I was able to continue my project and check that the rest of my system worked without incurring additional charges. The code that was supposed to be used in my Lambda function is available in lambda.py. 

Without using a proper Pandas Dataframe, which I did not consider to be in the scope of this project, the upload of all 16,000 shipping records takes about 15 minutes. For testing purposes, I set a timer in my code for 3 minutes since that was enough for about 4,000 records. 

Using my "static Textract service," I was able to add an additional column to the data. For each shipping record, I included the description of the "commodity2" code that is included in the transborder freight data codes pdf. At the end, my pipeline section displays the names of all the freight record columns inserted into RDS and the number of records.

## How It Does It
The pipeline checks for RDS databases and tables and creates them if needed. Thus, as long as the RDS instance is well formed, there is no additional work to be done. When the Docker image is run, it requires the following environmental variables to be passed in. The SNS_ARN and ROLE_ARN variables were only needed for Textract and thus are not needed in the final version.

-ACCESS_KEY=(IAM Programmatic access to S3, Textract(not needed in the end), RDS)
-BUCKET_NAME=(Bucket where files for Textract are uploaded)
-SECRET_KEY=(Secret Key for Access Key)
-SNS_ARN=(Used for Textract)
-ROLE_ARN=(Role for Textract that has SNS permissions)
-REGION=(Region used for all AWS services in this pipeline section)
-RDS_URL=(URL for Publically available RDS instance with appropriate security group to allow external programmatic access)
-RDS_USER=(User in abovce RDS instance)
-RDS_PASSWORD=(Password for above RDS instance)

Build the container:
    docker build -t dp1 .

Run the dp1 image and pass in the environment variables (in a file called .env in this example)
    docker run --env-file .env -it dp1