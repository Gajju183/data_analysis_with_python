import numpy as np # calculation
import pandas as pd # Excel for Python
import datetime as dt # allow for date and time series manipulation
from datetime import timedelta
import boto3
from io import BytesIO

AWS_ACCESS_KEY = 'AKIATD4GPDJ44SNZZOO6'
AWS_SECRET_KEY = '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'

client = boto3.client('s3',aws_access_key_id = AWS_ACCESS_KEY, aws_secret_access_key =AWS_SECRET_KEY )

s3 = boto3.resource('s3',aws_access_key_id = AWS_ACCESS_KEY, aws_secret_access_key =AWS_SECRET_KEY )

my_bucket = s3.Bucket('flam-data-room')

files = list(my_bucket.objects.filter(Prefix='Master/'))

li=[]
for key in files[1:]:
    obj = client.get_object(Bucket='flam-data-room', Key=key.key)
    df = pd.read_csv(BytesIO(obj['Body'].read()),header=0).astype('int64')
    li.append(df)

master = pd.concat(li, axis=0, ignore_index=True).sort_values(by=['year','month','day']).drop_duplicates().reset_index(drop=True)

master['date'] = master.apply(lambda x: dt.date(x['year'], x['month'], x['day']), axis=1)
master['date'] = master['date'].astype('datetime64[ns]')
master['activity_month'] = master.apply(lambda x: dt.date(x['year'], x['month'],1), axis=1)
master['activity_month'] = master['activity_month'].astype('datetime64[ns]')

master['User_D0'] = master.groupby('phone_number')['date'].transform('min')
master['User_D0'] = master['User_D0'].astype('datetime64[ns]')

master['Month_D0'] = master.groupby(['phone_number','activity_month'])['day'].transform('min')
master['user_ret_D'] = (master['date'] - master['User_D0']).dt.days
master['User_M0'] = master['User_D0'].to_numpy().astype('datetime64[M]')