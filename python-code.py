{\rtf1\ansi\ansicpg1252\deff0\nouicompat\deflang1033{\fonttbl{\f0\fnil\fcharset0 Calibri;}}
{\*\generator Riched20 10.0.17134}\viewkind4\uc1 
\pard\sa200\sl276\slmult1\f0\fs22\lang9 import pandas as pd \par
\par
# read csv\par
df = pd.read_csv('/home/pradhvan/Downloads/6th Street purchase.csv')\par
\par
# Lists to store data \par
order_id = []\par
final_amount = []\par
shipping_fee = []\par
country = []\par
discount_amount = []\par
product_count = []\par
transaction_id = []\par
language = []\par
currency = []\par
product_skus = []\par
total_amount = []\par
product_names = []\par
values = []\par
shipping = []\par
tax = []\par
\par
# Loop over json to get value to get value or NULL \par
for val in df['custom_data']:\par
    # load string into json\par
    data = json.loads(val)\par
    order_id.append(data.get('order_id','NULL'))\par
    final_amount.append(data.get('final_amount','NULL'))\par
    shipping_fee.append(data.get('shipping_fee','NULL'))\par
    country.append(data.get('country','NULL'))\par
    discount_amount.append(data.get('discounted_amount','NULL'))\par
    product_count.append(data.get('product_count','NULL'))\par
    transaction_id.append(data.get('transaction_id','NULL'))\par
    language.append(data.get('language','NULL'))\par
    currency.append(data.get('currency','NULL'))\par
    total_amount.append(data.get('total_amount','NULL'))\par
    product_names.append(data.get('product_names','NULL'))\par
    values.append(data.get('values','NULL'))\par
    shipping.append(data.get('shipping','NULL'))\par
    tax.append(data.get('tax','NULL'))\par
    product_skus.append(data.get('product_skus','NULL'))\par
\par
# Dataframe    \par
nff = pd.DataFrame()\par
nff['order_id'] = order_id\par
nff['final_amount'] = final_amount\par
nff['shipping_fee'] = shipping_fee\par
nff['country'] = country\par
nff['discount_amount'] = discount_amount\par
nff['product_count'] = product_count\par
nff['transaction_id'] = transaction_id\par
nff['language'] = language\par
nff['currency'] = currency\par
nff['product_skus'] = product_skus\par
nff['total_amount'] = total_amount\par
nff['product_names'] = product_names\par
nff['values'] = values\par
nff['shipping'] = shipping\par
nff['tax'] = tax\par
\par
# export to csv \par
nff.to_csv('6data.csv') \par
}
 