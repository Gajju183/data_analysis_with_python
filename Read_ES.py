
config['ELASTICSEARCH']#['MMP_PRODUCT_STATS']

spark.sql("set spark.sql.caseSensitive=true")
product_catalog = (
    spark.read.format("org.elasticsearch.spark.sql")
    .option("es.port", config['ELASTICSEARCH']['MMP_PRODUCT_STATS']['nodes'][0].split(':')[1])
    .option("es.nodes", config['ELASTICSEARCH']['MMP_PRODUCT_STATS']['nodes'][0].split(':')[0])
    .option("es.nodes.discovery", "true")
    .option("es.mapping.date.rich", "false")
    .option("es.net.ssl", "false")
    .option("inferSchema", "false")
    .option("es.net.http.auth.user", config['ELASTICSEARCH']['MMP_PRODUCT_STATS']['auth']['username'])
    .option("es.net.http.auth.pass", config['ELASTICSEARCH']['MMP_PRODUCT_STATS']['auth']['password'])
#     .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .load(config['ELASTICSEARCH']['MMP_PRODUCT_STATS']['resource'])
)


product_catalog=product_catalog.select('tyroo_account_id',"date_time",'product_affinity_category','conversion_purchases_value','conversion_purchases')

display(product_catalog)
product_catalog.createOrReplaceTempView('data')


# print(type(product_catalog['tyroo_account_id']))
new_df = spark.sql("Select tyroo_account_id, EXTRACT(Month FROM date_time) AS Month, product_affinity_category, SUM(conversion_purchases_value), SUM(conversion_purchases) FROM data WHERE  CAST(date_time AS DATE) BETWEEN '2019-11-01 00:00:00' AND '2020-02-01 00:00:00' GROUP BY 1,2,3")

new_df1 = new_df.filter("tyroo_account_id == '2063' or tyroo_account_id == '2211' or tyroo_account_id == '2213' or tyroo_account_id == '2061' or tyroo_account_id == '2195' or tyroo_account_id == '2247' or tyroo_account_id == '2275' or tyroo_account_id == '2277' or tyroo_account_id == '2287' or tyroo_account_id == '2289' or tyroo_account_id == '2291' or tyroo_account_id == '2301' or tyroo_account_id == '2309' or tyroo_account_id == '2313' or tyroo_account_id == '2371' or tyroo_account_id == '2418' or tyroo_account_id == '2461' or tyroo_account_id == '2467' or tyroo_account_id == '2470' or tyroo_account_id == '2471'")

df2 = new_df1.repartition(1)

df2.write.csv('dbfs:/mnt/feeddump/Gajraj/industry_data.csv')

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