import os
os.environ["AWS_ACCESS_KEY_ID"] = "ACCESS_KEY"
os.environ["AWS_SECRET_ACCESS_KEY"] = "SECRET_ACCESS_KEY"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_DEFAULT_REGION"] = "ru-central1"

#import findspark
#findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TransactionValidation") \
    .config("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SECRET_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.yandexcloud.net") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()
df=spark.read.parquet("s3a://CLEAR_DATA_BUCKET/")
df = df.filter(df.tx_datetime.isNotNull())
df=df.orderBy(['tx_datetime', 'customer_id'])
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def add_rolling_features(df):
    windows = {
        '1': Window.partitionBy("customer_id").orderBy("tx_datetime").rowsBetween(-1, 0),
        '7': Window.partitionBy("customer_id").orderBy("tx_datetime").rowsBetween(-7, 0),
        '30': Window.partitionBy("customer_id").orderBy("tx_datetime").rowsBetween(-30, 0)
    }

    for period, window in windows.items():
        df = df.withColumn(f'avg_transaction_count_{period}', F.count("transaction_id").over(window).cast("float"))
        df = df.withColumn(f'avg_transaction_mean_{period}', F.avg("tx_amount").over(window))

    terminal_windows = {
        '1': Window.partitionBy("terminal_id").orderBy("tx_datetime").rowsBetween(-1, 0),
        '7': Window.partitionBy("terminal_id").orderBy("tx_datetime").rowsBetween(-7, 0),
        '30': Window.partitionBy("terminal_id").orderBy("tx_datetime").rowsBetween(-30, 0)
    }


    for period, window in terminal_windows.items():
        df = df.withColumn(f'avg_transaction_terminal_id_count_{period}', F.count("transaction_id").over(window).cast("float"))

    return df
df = add_rolling_features(df)

df = df.drop("transaction_id", "customer_id", "terminal_id", "tx_fraud_scenario")

from pyspark.sql.functions import col, unix_timestamp
df = df.withColumn("tx_datetime", col("tx_datetime").cast("timestamp"))
df = df.withColumn("tx_datetime", unix_timestamp(col("tx_datetime")))

df.write.mode("overwrite").option("header", "true").parquet("s3a://REEDIT_DATA_BUCKET/data")
