import webbrowser, os
import json
import boto3
import io
from io import BytesIO
import sys
from pprint import pprint

# Code for taking Textract blocks and creating a CSV file, taken from AWS Documentation


def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}

                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '
    return text


def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)

    # get cells.
    csv = 'Table: {0}\n\n'.format(table_id)

    for row_index, cols in rows.items():

        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'

    csv += '\n\n\n'
    return csv


def get_table_csv_results(response):

    # Get the text blocks
    blocks = response['Blocks']
    pprint(blocks)

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        return "<b> NO Table FOUND </b>"

    csv = ''
    for index, table in enumerate(table_blocks):
        csv += generate_table_csv(table, blocks_map, index + 1)
        csv += '\n\n'

    return csv


def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])

    if(message['Status'] == 'SUCCEEDED'):
        client = boto3.client('textract')
        response = client.get_document_analysis(JobId=message['JobId'])

        table_csv = get_table_csv_results(response)
        output_file = '/tmp/output.csv'

        # replace content
        with open(output_file, "wt") as fout:
            fout.write(table_csv)

        encoded_string = table_csv.encode("utf-8")
        BUCKET_NAME = os.environ['BUCKET_NAME']
        file_name = message['JobId']
        s3_path = "/csv/" + file_name

        s3 = boto3.resource("s3")
        s3.Bucket(BUCKET_NAME).put_object(Key=s3_path, Body=open('/tmp/output.csv', 'rb'))


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
