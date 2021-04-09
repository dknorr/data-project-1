import csv
import requests

FREIGHT_CSV_URL = "https://data.transportation.gov/api/views/uxue-t623/rows.csv?accessType=DOWNLOAD"

with requests.Session() as s:
    download = s.get(FREIGHT_CSV_URL)

    decoded_content = download.content.decode('utf-8')

    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    for row in my_list:
        print(row)