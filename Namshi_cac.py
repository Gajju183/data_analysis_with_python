from googleapiclient import discovery
import boto3
from google.oauth2 import service_account
from google.cloud import bigquery
from botocore.exceptions import ClientError

import numpy as np
import pandas as pd
import json


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
      'sync_access_key': 'AKIAWWIF6DFXDW27DDQC',
      'sync_secret_key': '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',
      'prefix': 'advertiser-first-party-transformed',
      'dest_bucket': 'advertiser-first-party-data',
      'dataset': 'first_party_data',
      'key': json.loads(config['GCP']['service_account_key'])
  }

credentials = service_account.Credentials.from_service_account_info(gcp['key'])


adset_query = '''SELECT B.campaignId, B.adsetId,B.creativeId, B.randomuserid,B.eventitemjson, A.installDate, FORMAT_DATETIME('%F %T', DATETIME(B.dateTime, 'America/Los_Angeles')) AS PurchaseDate FROM `vid-ai.first_party_data.events` B JOIN (SELECT randomuserid, FORMAT_DATETIME('%F %T', DATETIME(dateTime, 'America/Los_Angeles')) AS installDate FROM `vid-ai.first_party_data.events` WHERE tyrooAttributionPartnerId = '15' AND DATE(dateTime, 'America/Los_Angeles') BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND adsetId is not null AND pkgName in ('com.namshi.android','id840127349','840127349') AND actionkey = 'install') A ON A.randomuserid = B.randomuserid WHERE tyrooAttributionPartnerId = '15' AND DATE(dateTime, 'America/Los_Angeles') BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND adsetId is not null AND pkgName in ( 'com.namshi.android','id840127349','840127349') AND actionkey = 'sale';'''

data = pd.read_gbq(adset_query, project_id="vid-ai", credentials=credentials, dialect="standard")



data['revenue'] = data['eventitemjson'].apply(lambda x: json.loads(x).get('revenue'))
df = data.drop('eventitemjson', axis =1)
# df['Time_DIFF'] = (df['PurchaseDate']-df['installDate']).astype('timedelta64[h]')
df['Time_DIFF'] = (pd.to_datetime(df['PurchaseDate'])-pd.to_datetime(df['installDate'])).astype('timedelta64[h]')
from datetime import date



df['converts'] = df['Time_DIFF'].apply(lambda x: 1 if x <= 24 else 0)

df['mp_campaign_id'] = df['campaignId'].apply(lambda x: x[len(x)-37:-1])
df['mp_adset_id'] = df['adsetId'].apply(lambda x: x[len(x)-37:-1])
df['mp_ad_id'] = df['creativeId'].apply(lambda x: x[len(x)-37:-1])
df1 = df.drop({'campaignId','adsetId','creativeId','randomuserid'}, axis =1)
df1['installDate'] = df1['installDate'].astype(str)
df1['PurchaseDate'] = df1['PurchaseDate'].astype(str)

df1['installDate'] = df1['installDate'].astype('datetime64[ns]')
df1['PurchaseDate'] = df1['PurchaseDate'].astype('datetime64[ns]')
df1['revenue'] = df1['revenue'].astype(float)
df1['Time_DIFF'] = df1['Time_DIFF'].astype(int)

df1['converts'] = df1['converts'].astype(int)
df1['mp_campaign_id'] = df1['mp_campaign_id'].astype(str)
df1['mp_adset_id'] = df1['mp_adset_id'].astype(str)
df1['mp_ad_id'] = df1['mp_ad_id'].astype(str)


schema = [{'name': 'installDate', 'type': 'DATETIME'},{'name': 'PurchaseDate', 'type': 'DATETIME'},{'name': 'revenue', 'type': 'FLOAT'},{'name': 'Time_DIFF', 'type': 'INTEGER'},{'name': 'converts', 'type': 'INTEGER'},{'name': 'mp_campaign_id', 'type': 'STRING'},{'name': 'mp_adset_id', 'type': 'STRING'},{'name': 'mp_ad_id', 'type': 'STRING'}]

df1.to_gbq(destination_table='first_party_data.namshi_cac', project_id='vid-ai', if_exists='append', progress_bar=True, table_schema=schema, credentials=credentials)