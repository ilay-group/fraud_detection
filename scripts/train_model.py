import os
import sys
import logging


os.environ["AWS_ACCESS_KEY_ID"] = "AWS_ACCESS_KEY_ID"
os.environ["AWS_SECRET_ACCESS_KEY"] = "AWS_SECRET_ACCESS_KEY"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_DEFAULT_REGION"] = "ru-central1"


import findspark
findspark.init()

import os
import logging
import argparse
from datetime import datetime


from sklearn.datasets import load_diabetes
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit


from pyspark.ml import Pipeline


import mlflow
from mlflow.tracking import MlflowClient

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


spark = SparkSession.builder.appName('train').getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", "AWS_ACCESS_KEY_ID")
hadoop_conf.set("fs.s3a.secret.key", "AWS_SECRET_ACCESS_KEY")
hadoop_conf.set("fs.s3a.endpoint", "storage.yandexcloud.net")


def get_dataframe():
    df = spark.read.parquet("s3a://ydryhrd/")
    return df


def get_pipeline():
    indexer = StringIndexer(inputCols=["tx_fraud_scenario"], outputCols=["tx_fraud_scenario_index"])
    onehot = OneHotEncoder(inputCols=["tx_fraud_scenario_index"], outputCols=["tx_fraud_scenario_encoded"])
    assembler = VectorAssembler(inputCols=['tx_amount', 'tx_time_seconds', 'tx_time_days', 'tx_fraud_scenario_encoded'], outputCol='features')
    regression = LinearRegression(featuresCol='features', labelCol='tx_fraud')
    
    pipeline = Pipeline(stages=[indexer, onehot, assembler, regression])
    return pipeline

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
TRACKING_SERVER_HOST = "TRACKING_SERVER_HOST"
mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:8000")
logger.info("tracking URI: %s", {mlflow.get_tracking_uri()})
logger.info("Loading Data ...")
data = get_dataframe()

# Настройка клиента MLflow и эксперимента
client = MlflowClient()
experiment = mlflow.set_experiment("baseline")
experiment_id = experiment.experiment_id

# Установка имени запуска
run_name = 'My run name ' + str(datetime.now())

# Запуск MLflow
with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    
    # Получение пайплайна
    inf_pipeline = get_pipeline()
    
    # Получение последнего этапа (регрессор или классификатор)
    classifier = inf_pipeline.getStages()[-1]

    # Создание сетки параметров
    paramGrid = (ParamGridBuilder()
         .addGrid(classifier.fitIntercept, [True, False])
         .addGrid(classifier.regParam, [0.001, 0.01, 0.1, 1, 10])
         .addGrid(classifier.elasticNetParam, [0.0, 0.25, 0.5, 0.75, 1.0])
         .build())

    # Оценка
    evaluator_accuracy = MulticlassClassificationEvaluator(labelCol='tx_fraud', predictionCol='prediction', metricName='accuracy')
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol='tx_fraud', predictionCol='prediction', metricName='f1')

    # Процент данных для обучения 
    trainRatio = 1 - 0.2

    # TrainValidationSplit
    tvs = TrainValidationSplit(estimator=inf_pipeline,
         estimatorParamMaps=paramGrid,
         evaluator=evaluator_accuracy,
         trainRatio=trainRatio,
         parallelism=30)

    logger.info("Fitting new inference pipeline ...")
    model = tvs.fit(data)
  
    run_id = mlflow.active_run().info.run_id
    logger.info(f"Logging optimal parameters to MLflow run {run_id} ...")
    
    # Логирование оптимальных параметров
    best_regParam = model.bestModel.stages[-1].getRegParam()
    best_fitIntercept = model.bestModel.stages[-1].getFitIntercept()
    best_elasticNetParam = model.bestModel.stages[-1].getElasticNetParam()

    logger.info(model.bestModel.stages[-1].explainParam('regParam'))
    logger.info(model.bestModel.stages[-1].explainParam('fitIntercept'))
    logger.info(model.bestModel.stages[-1].explainParam('elasticNetParam'))

    mlflow.log_param('optimal_regParam', best_regParam)
    mlflow.log_param('optimal_fitIntercept', best_fitIntercept)
    mlflow.log_param('optimal_elasticNetParam', best_elasticNetParam)

    logger.info("Scoring the model ...")
    predictions = model.transform(data)
    
    # Оценка модели
    rmse = evaluator_accuracy.evaluate(predictions)
    logger.info(f"Logging metrics to MLflow run {run_id} ...")
    mlflow.log_metric("rmse", rmse)
    logger.info(f"Model RMSE: {rmse}")

    logger.info("Exporting/logging pipeline ...")
    mlflow.spark.log_model(model, "duck7")  # Укажите путь для логирования модели
    logger.info("Done")

# Остановка Spark сессии
spark.stop()


