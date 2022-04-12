import random
import json
from randomtimestamp import randomtimestamp
from dateutil import parser
import pandas as pd
from datetime import date


# start_date = date.today().replace(day=1, month=1).toordinal()
# end_date = date.today().toordinal()
# creative_data = pd.read_csv('C:\\Users\\Gajraj\\Python\\Demo\\2277_product.csv')
id_data = pd.read_csv('adaccounts.csv')
# products_data = pd.read_csv('products1.csv')
# dates = dates = ["2020-03-01 21:48:49","2020-03-02 21:48:49","2020-03-03 21:48:49","2020-03-04 21:48:49","2020-03-05 21:48:49","2020-03-06 21:48:49","2020-03-07 21:48:49","2020-03-08 21:48:49","2020-03-09 21:48:49","2020-03-10 21:48:49","2020-03-11 21:48:49","2020-03-12 21:48:49","2020-03-13 21:48:49","2020-03-16 21:48:49","2020-03-17 21:48:49","2020-03-18 21:48:49","2020-03-19 21:48:49"]

final_data = []

# account_currency = ['INR']
# timezone = ['Asia/Kolkata']
view_attribution_window = ['1_DAY', "7_DAY", '28_DAY']
click_attribution_window = ['1_DAY', "7_DAY", '28_DAY']
account_currency = ['AED', "SAR", 'BHD','KWD','OMR']
adset_country_code = ['UAE','KSA','KWT','OM','BH']

id_len = len(id_data)
# products_data_len = (products_data)

for x in range(5000):
    print(" Records created {}!".format(x))
    rand_creative = random.randrange(0, id_len)
    rand_spend = random.randrange(1, 24)

    data =  {
        "account_currency":account_currency[random.randrange(0, 4)],
        "adset_country_code":adset_country_code[random.randrange(0, 4)],
        "click_attribution_window":click_attribution_window[random.randrange(0, 2)],
        "clicks": random.randrange(53, 900),
        "conversion_add_cart": random.randrange(3, 50),
        "conversion_purchases": random.randrange(0, 12),
        "conversion_view_content": random.randrange(90, 754),
        "date_time": randomtimestamp(2020,True),
        "impressions": random.randrange(2000, 20041),
        "mp_account_id": str(id_data['fb_adaccount_id'][rand_creative]),
        "mp_account_name": str(id_data['adaccount_name'][rand_creative]),
        "mp_ad_id": str(id_data['fb_ad_id'][rand_creative]),
        "mp_ad_name": str(id_data['ad_name'][rand_creative]),
        "mp_adset_id": str(id_data['fb_adset_id'][rand_creative]),
        "mp_adset_name": str(id_data['adset_name'][rand_creative]),
        "mp_campaign_id": str(id_data['fb_campaign_id'][rand_creative]),
        "mp_campaign_name": str(id_data['campaign_name'][rand_creative]),
        "spend": rand_spend,
        "spend_account_currency": rand_spend * 76.92,
        "timezone":'Asia/Dubai',
        "tyroo_account_id": str(id_data['adaccount_id'][rand_creative]),
        "tyroo_ad_id": str(id_data['ad_id'][rand_creative]),
        "tyroo_adset_id": str(id_data['adset_id'][rand_creative]),
        "tyroo_brand_id": str(id_data['brand_id'][rand_creative]),  
        "tyroo_brand_name": str(id_data['brand_name'][rand_creative]),
        "tyroo_campaign_id": str(id_data['campaign_id'][rand_creative]),
        "view_attribution_window":view_attribution_window[random.randrange(0, 2)],
  }
#     print("data is {}".format(data))
    final_data.append(data)
#     print("final data is {}".format(final_data))


try:
  f = open("Demo_fb_hourly.json", "w")
  # print(final_data)   
  f.write(json.dumps(final_data))
  f.close()
except Exception as error:
  print(error)
  pass




df = pd.DataFrame(final_data)
df.to_csv('D:\\Python\\Demo\\Facebook\\Demo_fb_hourly.csv',index= False)