
config['ELASTICSEARCH']['MMP_USER_RETENTION']

es = Elasticsearch(config['ELASTICSEARCH']['MMP_USER_RETENTION']['nodes'],http_auth=(config['ELASTICSEARCH']['MMP_USER_RETENTION']['auth']['username'], config['ELASTICSEARCH']['MMP_USER_RETENTION']['auth']['password']),timeout=600)

documents = es.search(index='fb_daily_product_stats',body={},size=10000)


df = documents["hits"]["hits"]


for key, val in documents["hits"].items():
    if key == "hits":
        for num, doc in enumerate(val):
            print (num, '-->', doc, "\n")

# df1 = pd.DataFrame(df)
# df1.head()

fields = {}
for num, doc in enumerate(df):
        pass
    # do stuff here!


source_data = doc["_source"]


import numpy as np
for key, val in source_data.items():
        try:
            fields[key] = np.append(fields[key], val)
        except KeyError:
            fields[key] = np.array([val])

for key, val in fields.items():
    print (key, "--->", val)
    print ("NumPy array len:", len(val), "\n")



elastic_df = pd.DataFrame(fields)

print ('elastic_df:', type(elastic_df), "\n")
print (elastic_df)

# adset_result_p = adset_result1.toPandas()
import csv
path = '/dbfs/mnt/feeddump/Gajraj/6thStreet/feed_KSA.csv'
data_KSA.to_csv(
    path,
    index=False,
    encoding="utf-8",
    quoting=csv.QUOTE_NONNUMERIC,
    float_format="%.2f",
)