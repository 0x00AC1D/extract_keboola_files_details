import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import time
import csv
import logging


logging.basicConfig(level=logging.INFO)

URL_ENDPOINT = 'https://connection.keboola.com/v2/storage/files'
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
session = requests.Session()
retry_policy = Retry(total=MAX_RETRIES, status_forcelist=tuple(range(400, 600)))
session.mount('http://', HTTPAdapter(max_retries=retry_policy))
session.mount('https://', HTTPAdapter(max_retries=retry_policy))


with open('output.csv', 'w', newline='') as output_file:
    writer = csv.DictWriter(output_file, fieldnames=OUTPUT_FIELDS, delimiter=';')
    writer.writeheader()

    while True:
        response = session.get(URL_ENDPOINT, headers=headers, params={**params, 'offset': offset})        
        if not response.json():
            logging.info(f'Request with offset: {offset} and limit: {limit} returned no data')
            break

        logging.info(f'Successfully retrieved detail for files with offset: {offset} and limit: {limit}')

        for file_detail in response.json():
            writer.writerow(file_detail)

        offset += limit
        retries = 0
