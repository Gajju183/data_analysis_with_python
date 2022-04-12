import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd


data = spark.read.format('csv').option("header","true").load('dbfs:/mnt/feeddump/Gajraj/data.csv')


data.show(truncate=False)



spark = SparkSession \
    .builder \
    .appName("RFM Analysis with PySpark") \
    .getOrCreate()



data = data.withColumn("Quantity", data["Quantity"].cast(IntegerType()))
data = data.withColumn("UnitPrice", data["UnitPrice"].cast(DoubleType()))
data = data.withColumn("Date", to_date(unix_timestamp("InvoiceDate", "MM/dd/yyyy").cast("timestamp")))

# define Total column
data = data.withColumn("Total", round(data["UnitPrice"] * data["Quantity"], 2))

# calculate difference in days between 2011-12-31 and the Invoice Date
data = data.withColumn("RecencyDays", expr("datediff('2011-12-31', Date)"))


rfm_table = data.groupBy("CustomerId")\
                        .agg(min("RecencyDays").alias("Recency"), \
                             count("InvoiceNo").alias("Frequency"), \
                             sum("Total").alias("Monetary"))


rfm_table.show()

R = rfm_table.approxQuantile("Recency", [0.25, 0.5, 0.75], 0)
F = rfm_table.approxQuantile("Frequency", [0.25, 0.5, 0.75], 0)
M = rfm_table.approxQuantile("Monetary", [0.25, 0.5, 0.75], 0)

rfm_table = rfm_table.withColumn("R", \
                                 when(col("Recency") >= R[2] , 1).\
                                 when(col("Recency") >= R[1] , 2).\
                                 when(col("Recency") >= R[0] , 3).\
                                 otherwise(4))

rfm_table = rfm_table.withColumn("F", \
                                 when(col("Frequency") > F[2] , 4).\
                                 when(col("Frequency") > F[1] , 3).\
                                 when(col("Frequency") > F[0] , 2).\
                                 otherwise(1))

rfm_table = rfm_table.withColumn("M", \
                                 when(col("Monetary") >= M[2] , 4).\
                                 when(col("Monetary") >= M[1] , 3).\
                                 when(col("Monetary") >= M[0] , 2).\
                                 otherwise(1))

rfm_table = rfm_table.withColumn("RFM_Score", concat(col("R"), col("F"), col("M")))

rfm_table.show()

# grouped_by_rfmscore = rfm_table.groupBy("RFM_Score").count().orderBy("count", ascending=False).alias("customers")
import pyspark.sql.functions as F
grouped_by_rfmscore = rfm_table.groupBy("RFM_Score").count().select('RFM_Score', F.col('count').alias('customers'))


grouped_by_rfmscore.orderBy("customers", ascending=False).show(grouped_by_rfmscore.count(),False)
# grouped_by_rfmscore.show(grouped_by_rfmscore.count(),False)

type(grouped_by_rfmscore)



grouped_by_rfmscore = grouped_by_rfmscore.withColumn('customers', F.col('customers').cast(IntegerType()))
grouped_by_rfmscore = grouped_by_rfmscore.withColumn('RFM_Score', F.col('RFM_Score').cast(IntegerType()))
grouped_by_rfmscore = grouped_by_rfmscore.orderBy("RFM_Score", ascending=False)
grouped_by_rfmscore_pandas = grouped_by_rfmscore.toPandas()



grouped_by_rfmscore_pandas.head()


# import seaborn as sns
# import matplotlib.pyplot as plt

# a4_dims = (20, 9)
# fig, ax = plt.subplots(figsize=a4_dims)
# ax = sns.barplot(x='RFM_Score', y='count', data=grouped_by_rfmscore_pandas,saturation=7)
# ax.set(xlabel='RFM_Score', ylabel='Number of Customer')
# plt.show()

display(grouped_by_rfmscore_pandas)

