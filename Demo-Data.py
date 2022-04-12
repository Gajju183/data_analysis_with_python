
config['ELASTICSEARCH']#['SNAPCHAT_HOURLY']

# Making elasticsearch connection

#Snap Hourly data
hourly_stats = 'http://vcdn.tyroo.com/uploaded-files/hourlystatsjson20200320064311.json'
hourly_stats_position = 'http://vcdn.tyroo.com/uploaded-files/hourlystatsPositionjson20200320073350.json'

#Snap lifetime data
lifetime_stats = 'http://vcdn.tyroo.com/uploaded-files/snaplifetimedatajson20200320081118.json'
lifetime_stats_position = 'http://vcdn.tyroo.com/uploaded-files/Snaplifetimepositiondatajson20200320085654.json'

#MMP DATA
product_stats = 'http://vcdn.tyroo.com/uploaded-files/MMPproductdatajson20200320170027.json'
mmp_hourly_stats = 'http://vcdn.tyroo.com/uploaded-files/snapmmphourlystatsjson20200326095838.json'


# INDEX= 'snap_hourly_stats'
# INDEX= 'snap_hourly_stats_position'
# INDEX= 'snap_lifetime_stats'
# INDEX= 'snap_lifetime_stats_position'
# INDEX= 'mmp_products_stats'
INDEX= 'snap_mmp_hourly_stats'

ip = 'http://internal-TyrooES-1662776173.us-west-2.elb.amazonaws.com'
es_port = 9200

es = Elasticsearch(
    [ip],
    http_auth=('snap', '_4F$Pzp45xxU^LW8'),#Products
#     http_auth=('snap-reporting', 'PesU6d@gWkbTb&JQ'),#snap reporting
#     http_auth=('read', 'awL$fk@QPzU8W-yz'),
  
    scheme="http",
    port=es_port,
    timeout=60,
    max_retries=2,
    retry_on_timeout=True)

print(INDEX, ip, es_port)

#  Create index key
# def create_key(ad_id, datetime):
#     keys = ad_id + '_' + datetime
#     es_object_id = keys
#     es_object_id = es_object_id.encode('utf-8')
#     es_object_id = hashlib.md5(es_object_id).hexdigest()
#     return es_object_id
  
def rec_to_actions(df, date):
    for record in df.to_dict(orient="records"):
#         print(record)
#         ad_id = record['mp_ad_id']
#         es_id = create_key(ad_id, date)
        yield ('{ "index" : { "_index" : "%s"}}'% (INDEX))
        yield (json.dumps(record, default=str))

        
def dump_to_es(date):
    print('Getting json data...')
    data = pd.read_json(mmp_hourly_stats)
    print(data.shape)
    
#     data = pd.read_json(product_stats, orient='records')
#     req = requests.get('http://vcdn.tyroo.com/uploaded-files/snaphourlypositionstatsjson20200320051434.json').json()
#     data = pd.DataFrame(req)
#     print(data.shape)

#     print(data.columns.tolist())
#     print(data['tyroo_sync_date_time'])
    
    try: 
      r = es.bulk(rec_to_actions(data, date))
      print("Data exported to ES ")
    except Exception as e:
      print(e)
      
      

for day in days:
    date = day.floor('day').format('YYYY-MM-DD HH:mm:ss' )
    print(date)
    dump_to_es(date)