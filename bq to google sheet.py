import arrow
import base64
import boto3
import json
import logging
import numpy as np
import pandas as pd
import sqlite3
import time
import requests
import hashlib
from elasticsearch import Elasticsearch
 
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from googleapiclient import discovery
from google.cloud import bigquery

from pyspark.sql.functions import arrays_zip

# mysql and ElasticSearch connection string
sql_conn = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=UTF8MB4&use_unicode=1'

-------New Cell
# reading source and sink config from Secrets Manager
def get_config(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.exception(e)
        raise e
    
    # Decrypts secret using the associated KMS CMK.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])    
    return secret

raw_config = get_config(secret_name="config/data-lake/source-and-sink", region_name="us-west-2")
config = json.loads(raw_config)

gcp = {
      'project_id': 'vid-ai',
      'src_bucket': 'advertiser-first-party-data',
      'prefix': 'advertiser-first-party-transformed',
      'dest_bucket': 'advertiser-first-party-data',
      'dataset': 'first_party_data',
      'key': json.loads(config['GCP']['service_account_key'])
  }

credentials = service_account.Credentials.from_service_account_info(gcp['key'])
print(credentials)

---New Cell

spreadsheetId = '1XL3jYHAtiqRreKCnTed5VDjTtJVzt7LmAoH_HGmm1HY'
sheet = 'adset_bid_history'
service = discovery.build('sheets', 'v4', credentials=credentials)

---New Cell

# read from BQ
query = '''SELECT DISTINCT * FROM (SELECT T.action_datetime, T.mp_adset_id, T.bid, T.daily_budget, T.name, FROM `snapchat.adset_history` T  WHERE CAST(T.action_datetime AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND T.mp_adset_id IN (SELECT DISTINCT A.mp_adset_id FROM `snapchat.adset_history` A JOIN snapchat.campaign B ON B.tyroo_campaign_id = A.tyroo_campaign_id JOIN snapchat.adset c ON c.mp_adset_id = A.mp_adset_id WHERE B.tyroo_account_id = 2071 AND c.status = TRUE)) tb'''

df = pd.read_gbq(query, project_id="vid-ai", credentials=credentials, dialect="standard")

# df.head()
# df = df.drop('index', axis=1)

--New Cell

df['action_datetime'] = df['action_datetime'].apply(lambda x: x.date())

df = df.drop_duplicates(keep="last", inplace=False)
df['action_datetime'] = df['action_datetime'].astype(str)

##### Updating Sheet
values = df.values.tolist()
body = {
    'values': values
}

# result = service.spreadsheets().values().clear(spreadsheetId=spreadsheetId, range=sheet).execute()
result = service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=sheet, valueInputOption="USER_ENTERED", body=body).execute()
print('{0} row updated.'.format(result['updates']['updatedRange']))
