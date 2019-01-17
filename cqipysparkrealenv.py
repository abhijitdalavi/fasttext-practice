import sys
import json
import fastavro  # 0.17.9
import os
import logging
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()

sc.addFile("data/model/fake/lv1/",recursive=True)
sc.addFile("data/model/fake/lv2/",recursive=True)
sc.addFile("data/model/fake/lv3/",recursive=True)
sc.addFile("data/dictionary/dict_token.txt")
sc.addFile("data/dictionary/dict_garbage.txt")

spark = SparkSession(sc)

import const, predict

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

model_lv1 = None
model_lv2 = None
model_lv3 = None

ds = None
lst = list()

with open(const.get_cqi_input_file_path() + 'part-00000-ef5244bc-86ef-49a1-91e0-cc95a8f5cd95-c000.avro', 'rb') as fo:
    reader = fastavro.reader(fo)
    for record in reader:
        lst.append(
            [record['itemId'],
             record['productId'],
             record['categoryCode'],
             record['originalAttr'],
             record['normalizedAttr'],
             record['excludeType'],
             record['categoryCodeLv1'],
             record['categoryNameLv1'],
             0])
    cSchema = StructType([StructField("itemId", IntegerType()) \
                             ,StructField("productId", IntegerType()),
                          StructField("categoryCode", StringType()),
                          StructField("originalAttr", StringType()),
                          StructField("normalizedAttr", StringType()),
                          StructField("excludeType", StringType()),
                          StructField("categoryCodeLv1", StringType()),
                          StructField("categoryNameLv1", StringType()),
                          StructField("isCleansed", IntegerType())])
    ds = spark.createDataFrame(lst, cSchema)

ds.show()

from predict import predict_category

schema = StructType([
    StructField('pred_lv1_catecode', StringType()),
    StructField('pred_lv2_catecode', StringType()),
    StructField('pred_lv3_catecode', StringType()),
    StructField('pred_lv4_catecode', StringType()),
    StructField('pred_lv5_catecode', StringType()),
    StructField('pred_lv6_catecode', StringType()),
    StructField('pred_lv1_score', StringType()),
    StructField('pred_lv2_score', StringType()),
    StructField('pred_lv3_score', StringType()),
    StructField('pred_final_score', StringType()),
])
udf_predict = udf(predict_category, schema)
udf_predict_result = ds.withColumn("finalresult", udf_predict('normalizedAttr','productId','itemId'))
udf_predict_result.show()



