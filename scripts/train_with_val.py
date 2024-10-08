random_seed = 47

import os
os.environ["AWS_ACCESS_KEY_ID"] = "ACCESS_KEY"
os.environ["AWS_SECRET_ACCESS_KEY"] = "SECRET_ACCESS_KEY"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_DEFAULT_REGION"] = "ru-central1"

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

import re
from datetime import datetime

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import FloatType, DoubleType, IntegerType, StringType, TimestampType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder



def set_or_create_experiment(experiment_name):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(experiment_name)
        print(f"Эксперимент '{experiment_name}' был создан.")
    mlflow.set_experiment(experiment_name)
    print(f"Эксперимент '{experiment_name}' установлен как активный.")
    return mlflow.set_experiment(experiment_name)

import mlflow
from mlflow.tracking import MlflowClient
mlflow.set_tracking_uri("http://YOUR_MLFLOW_SERVER:8000")

experiment_name = "lrmodel"
experiment = mlflow.get_experiment_by_name(experiment_name)
    
if experiment is not None:
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], order_by=["start_time DESC"])
    
    if not runs.empty:
       last_run_id = runs.iloc[0].run_id
       print(f"ID последнего запуска в эксперименте '{experiment_name}': {last_run_id}")
    else:
       print(f"Нет доступных запусков в эксперименте '{experiment_name}'.")
else:
    print(f"Эксперимент с именем '{experiment_name}' не найден.")

experiment = set_or_create_experiment("lrmodel")
experiment_id = experiment.experiment_id
run_name = 'My run name' + 'TEST_LogReg' + str(datetime.now())

def get_pipeline():
    assembler = VectorAssembler(inputCols=['tx_datetime', 'tx_amount', 'tx_time_seconds', 'tx_time_days', 'avg_transaction_count_1', 'avg_transaction_mean_1', 'avg_transaction_count_7', 'avg_transaction_mean_7', 'avg_transaction_count_30', 'avg_transaction_mean_30', 'avg_transaction_terminal_id_count_1', 'avg_transaction_terminal_id_count_7', 'avg_transaction_terminal_id_count_30'], outputCol='features')
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    pipeline = Pipeline(stages=[assembler,
                          scaler,
                          ]).fit(df).transform(df)
    return pipeline
    
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("TransactionValidation") \
    .config("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SECRET_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.yandexcloud.net") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.772") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "50g") \
    .config("spark.local.dir", "/tmp/") \
    .getOrCreate()

df = spark.read.parquet("s3a://REEDIT_DATA_BUCKET/data").sample(fraction=0.1, seed=random_seed)

df_1 = df.filter(df["tx_fraud"] == 1)
df_0 = df.filter(df["tx_fraud"] == 0)
import pyspark.sql.functions as f
# балансирование классов
df1_oversampled = df_1 \
    .withColumn("dummy",
        f.explode(
            f.array(*[f.lit(x) for x in range(int(df_0.count() / df_1.count()))])
        )
    ) \
    .drop("dummy")
df = df_0.unionAll(df1_oversampled)


train_fraction = 0.8
test_fraction = 0.2

assert train_fraction + test_fraction == 1, "Сумма долей должна быть равна 1"

training, test = get_pipeline().randomSplit([train_fraction, test_fraction], seed=random_seed)
mlflow.start_run(run_name=run_name, experiment_id=experiment_id)

lr = LogisticRegression(
        maxIter=10,
        featuresCol="features",
        labelCol="tx_fraud"
)

import numpy as np
num_reg_params = 1
num_elastic_net_params = 1
reg_params = np.linspace(0, 1, num_reg_params).tolist()
elastic_net_params = np.linspace(0, 1, num_elastic_net_params).tolist()
paramGrid = (ParamGridBuilder()
                 .addGrid(lr.regParam, reg_params)
                 .addGrid(lr.elasticNetParam, elastic_net_params)
                 .build())
cv = CrossValidator(estimator=lr,
                        estimatorParamMaps=paramGrid,
                        evaluator=MulticlassClassificationEvaluator(labelCol="tx_fraud", predictionCol="prediction", metricName="f1"),
                        numFolds=3)
cvModel = cv.fit(training)
bestModel = cvModel.bestModel
mlflow.log_param('optimal_regParam', bestModel._java_obj.getRegParam())
mlflow.log_param('optimal_elasticNetParam', bestModel._java_obj.getElasticNetParam())


import random
import numpy as np
from pyspark.sql import functions as f
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import PipelineModel

# Инициализация списков для хранения F1 и ROC AUC
f1_scores = []
roc_auc_scores = []
precisions = [[] for _ in range(10)]
recalls = [[] for _ in range(10)]
f1_scores_old = []
roc_auc_scores_old = []
precisions_old = [[] for _ in range(10)]
recalls_old = [[] for _ in range(10)]

for i in range(1):
    random.seed(i)
    
    df = spark.read.parquet("s3a://REEDIT_DATA_BUCKET/data").sample(fraction=0.1, seed=random_seed)

    df_1 = df.filter(df["tx_fraud"] == 1)
    df_0 = df.filter(df["tx_fraud"] == 0)

    df1_oversampled = df_1.withColumn("dummy", 
        f.explode(f.array(*[f.lit(x) for x in range(int(df_0.count() / df_1.count()))]))
    ).drop("dummy")  
    df = df_0.unionAll(df1_oversampled)


    old_model_l = "runs:/"+last_run_id+"/lrModel"
    old_model = mlflow.spark.load_model(old_model_l)
    predicted = bestModel.transform(get_pipeline())
    predicted_old = old_model.transform(get_pipeline())

    tp = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 1)).count()    
    fp = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 1)).count()    
    fn = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 0)).count()
    tp_old = predicted_old.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 1)).count()    
    fp_old = predicted_old.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 1)).count()    
    fn_old = predicted_old.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 0)).count()


    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    F1 = (2 * recall * precision) / (recall + precision) if (recall + precision) > 0 else 0
    precision_old = tp_old / (tp_old + fp_old) if (tp_old + fp_old) > 0 else 0
    recall_old = tp_old / (tp_old + fn_old) if (tp_old + fn_old) > 0 else 0
    F1_old = (2 * recall_old * precision_old) / (recall_old + precision_old) if (recall_old + precision_old) > 0 else 0

    print(f"Iteration {i + 1}: F1 Score: {F1:.4f}")
    f1_scores.append(F1)
    print(f"Iteration {i + 1}: F1 Score old model: {F1_old:.4f}")
    f1_scores_old.append(F1_old)


    evaluator = BinaryClassificationEvaluator(labelCol="tx_fraud", rawPredictionCol="rawPrediction")
    roc_auc = evaluator.evaluate(predicted)
    roc_auc_old = evaluator.evaluate(predicted_old)
    print(f"Iteration {i + 1}: ROC AUC: {roc_auc:.4f}")
    roc_auc_scores.append(roc_auc)
    print(f"Iteration {i + 1}: ROC AUC: {roc_auc:.4f}")
    roc_auc_scores_old.append(roc_auc_old)


    predicted_probs = predicted.select("tx_fraud", "rawPrediction").rdd.map(lambda row: (float(row[0]), float(row[1][1]))).collect()
    y_true = [x[0] for x in predicted_probs]
    y_scores = [x[1] for x in predicted_probs]

    thresholds = np.linspace(0.0, 1.0, 10)

    for j, threshold in enumerate(thresholds):
        predictions = [1 if score >= threshold else 0 for score in y_scores]

        tp = sum(1 for i in range(len(y_true)) if y_true[i] == 1 and predictions[i] == 1)
        fp = sum(1 for i in range(len(y_true)) if y_true[i] == 0 and predictions[i] == 1)
        fn = sum(1 for i in range(len(y_true)) if y_true[i] == 1 and predictions[i] == 0)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0

        precisions[j].append(precision)
        recalls[j].append(recall)

        print(f"Threshold: {threshold:.2f}, Precision: {precision:.4f}, Recall: {recall:.4f}")


avg_f1 = sum(f1_scores) / len(f1_scores)
avg_roc_auc = sum(roc_auc_scores) / len(roc_auc_scores)
avg_f1_old = sum(f1_scores_old) / len(f1_scores_old)
avg_roc_auc_old = sum(roc_auc_scores_old) / len(roc_auc_scores_old)

mlflow.log_metric("avg_F1_Score", avg_f1)
mlflow.log_metric("avg_ROC_AUC", avg_roc_auc)
mlflow.log_metric("avg_F1_Score_old", avg_f1_old)
mlflow.log_metric("avg_ROC_AUC_old", avg_roc_auc_old)

for i, (precision, recall) in enumerate(zip([sum(precisions[j]) / len(precisions[j]) for j in range(10)], [sum(recalls[j]) / len(recalls[j]) for j in range(10)])):
    mlflow.log_metric(f"avg_Precision_Threshold_{thresholds[i]:.2f}", precision)
    mlflow.log_metric(f"avg_Recall_Threshold_{thresholds[i]:.2f}", recall)


mlflow.spark.log_model(bestModel, "lrModel")
mlflow.end_run()
