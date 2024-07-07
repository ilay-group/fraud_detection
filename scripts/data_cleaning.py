import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp


accesskey = "accesskey"
secretkey = "secretkey"
data_path = "s3a://data_path/"
write_path = "s3a://write_path"


def remove_outliers_iqr(df, columns):
    for col_name in columns:
        quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
        q1 = quantiles[0]
        q3 = quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df = df.filter((col(col_name) >= lower_bound) & (col(col_name) <= upper_bound))
    return df


spark = SparkSession.builder \
    .appName("TransactionValidation") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", accesskey) \
    .config("spark.hadoop.fs.s3a.secret.key", secretkey) \
    .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

df = spark.read.option("comment", "#").option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .schema("transaction_id LONG, tx_datetime STRING, customer_id INT, terminal_id INT, tx_amount DOUBLE, tx_time_seconds LONG, tx_time_days LONG, tx_fraud INT, tx_fraud_scenario INT") \
    .csv(data_path)

df = df.dropDuplicates(['transaction_id'])

df = df.filter(
    (col("transaction_id") >= 0) &
    (col("customer_id") >= 0) &
    (col("terminal_id") >= 0) &
    (col("tx_amount") >= 0) &
    (col("tx_time_seconds") >= 0) &
    (col("tx_time_days") >= 0) &
    (col("tx_fraud") >= 0) &
    (col("tx_fraud_scenario") >= 0)
)

df = df.withColumn("tx_datetime", unix_timestamp("tx_datetime", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

df = remove_outliers_iqr(df, ["customer_id", "terminal_id", "tx_time_seconds", "transaction_id", "tx_time_days"])

df = df.orderBy("transaction_id")

df = df.repartition(40)

df.write.mode("overwrite").option("header", "true").parquet(write_path) 

spark.stop()
