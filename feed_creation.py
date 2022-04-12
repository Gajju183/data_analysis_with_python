

config['GCP']['service_account_key']


# credentials = json.loads(config['GCP']['service_account_key'])
credentials = service_account.Credentials.from_service_account_info(json.loads(config['GCP']['service_account_key']))
service = discovery.build('sheets', 'v4', credentials=credentials)

adset_query = '''SELECT DISTINCT itemDetails.productID, itemDetails.availability, itemDetails.condition, itemDetails.description, itemDetails.imageURL1, itemDetails.imageURL2, itemDetails.imageURL3, itemDetails.productURL, itemDetails.title, itemDetails.actualPrice, itemDetails.salePrice, itemDetails.brand, itemDetails.color, itemDetails.gender, itemDetails.googleCategory, itemDetails.productType, itemDetails.category, itemDetails.LANGUAGE, itemDetails.currencyCode
FROM `vid-ai.first_party_data.ounass`, UNNEST(itemDetails) itemDetails WHERE CAST( eventDateTime AS DATE) BETWEEN '2020-01-01' AND '2020-03-21' AND stdeventid IN (27,62) AND itemDetails.productID IS NOT NULL'''

df = pd.read_gbq(adset_query, project_id="vid-ai", credentials=credentials, dialect="standard")

data = df.drop_duplicates(subset ="productID")


data.head(1)

data = data.rename(columns={"productID": "id", "imageURL1": "image_link", "productURL":"link", "actualPrice":"price", "salePrice":"sale_price",
                    "googleCategory":"google_product_category","productType":"product_type","currencyCode":"currency_code","LANGUAGE":"language"})


data = data.drop(['imageURL2','imageURL3'], axis = 1)

data['currency_code'].unique()

data['material'] = ''
# data = data[data['language'] == 'en' ]

data.head()

# ['AED', 'SAR', 'OMR', 'KWD', 'BHD', 'QAR']
data_bahrain = data[data['currency_code'] == 'BHD' ]
data_OMAN = data[data['currency_code'] == 'OMR' ]
data_QATAR = data[data['currency_code'] == 'QAR' ]
data_KUWAIT = data[data['currency_code'] == 'KWD' ]
data_UAE = data[data['currency_code'] == 'AED' ]
data_KSA = data[data['currency_code'] == 'SAR' ]




data_UAE.shape


# adset_result_p = adset_result1.toPandas()
import csv
path = '/dbfs/mnt/feeddump/Gajraj/ounass/feed_bahrain.csv'
data_bahrain.to_csv(
    path,
    index=False,
    encoding="utf-8",
    quoting=csv.QUOTE_NONNUMERIC,
    float_format="%.2f",
)

