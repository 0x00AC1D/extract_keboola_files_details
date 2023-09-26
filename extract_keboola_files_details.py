import requests
import json
import time
import csv
import logging


logging.basicConfig(level=logging.INFO)

URL_ENDPOINT = 'https://connection.keboola.com/v2/storage/files'
RETRY_INTERVAL = 3
MAX_RETRIES = 2
OUTPUT_FIELDS = ('created', 'creatorToken', 'id', 'isEncrypted', 'isPublic', 'isSliced', 
    'maxAgeDays', 'name', 'provider', 'region', 'runId', 'runIds', 'sizeBytes', 'tags', 'url')

with open('config.json') as config_file:
    TOKEN = json.load(config_file)['token']

headers = {'X-StorageApi-Token': TOKEN}
offset = 0
limit = 100
params = {
    'limit': limit,
    'showExpired': 'true'
}
retries = 0

with open('output.csv', 'w', newline='') as output_file:
    writer = csv.DictWriter(output_file, fieldnames=OUTPUT_FIELDS, delimiter=';')
    writer.writeheader()

while True:
    response = requests.get(URL_ENDPOINT, headers=headers, params={**params, 'offset': offset})

    if response.status_code // 100 != 2:
        logging.info(f'Request with offset: {offset} and limit: {limit} failed with status code: {response.status_code}')
        logging.info(f'Failed request body: {response.text}')

        if retries >= MAX_RETRIES:
            logging.error(f'Request attempts with offset: {offset} and limit: {limit} reached max retries: {MAX_RETRIES}')
            raise Exception('Max request retries reached')

        logging.info(f'Retrying in {RETRY_INTERVAL} seconds')
        time.sleep(RETRY_INTERVAL)
        retries += 1
        continue
    
    if not response.json():
        logging.info(f'Request with offset: {offset} and limit: {limit} returned no data')
        break

    logging.info(f'Successfully retrieved detail for files with offset: {offset} and limit: {limit}')

    with open('output.csv', 'a', newline='') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=OUTPUT_FIELDS, delimiter=';')

        for file_detail in response.json():
            writer.writerow(file_detail)

    offset += limit
    retries = 0
