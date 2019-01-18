import fastavro  # 0.17.9
import logging
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.session import SparkSession
from pyspark import SparkFiles

sc = SparkContext.getOrCreate()

sc.addFile("hdfs://localhost:8020/cqicategory/model/lv1/",recursive=True)
sc.addFile("hdfs://localhost:8020/cqicategory/model/lv2/",recursive=True)
sc.addFile("hdfs://localhost:8020/cqicategory/model/lv3/",recursive=True)
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_token.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_garbage.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_book_cate.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_book_definite_keyword.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_book_keyword.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_fashion_definite_keyword.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_fashion_keyword.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_sports_brand.txt")
sc.addFile("hdfs://localhost:8020/cqicategory/dictionary/dict_sports_definite_keyword.txt")

sc.addFile("hdfs://localhost:8020/cqicategory/part-00000-ef5244bc-86ef-49a1-91e0-cc95a8f5cd95-c000.avro")

spark = SparkSession(sc)

import const

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

model_lv1 = None
model_lv2 = None
model_lv3 = None

ds = None
lst = list()

with open(SparkFiles.get('part-00000-ef5244bc-86ef-49a1-91e0-cc95a8f5cd95-c000.avro'), 'rb') as fo:
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
    cSchema = StructType([StructField("itemId", IntegerType()),
                          StructField("productId", IntegerType()),
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
udf_predict_result.select('itemId','productId','categoryCode','originalAttr','normalizedAttr','excludeType',
                          'categoryCodeLv1','categoryNameLv1','isCleansed','finalresult.pred_lv1_catecode',
                          'finalresult.pred_lv2_catecode','finalresult.pred_lv3_catecode','finalresult.pred_lv4_catecode',
                          'finalresult.pred_lv5_catecode','finalresult.pred_lv6_catecode','finalresult.pred_lv1_score',
                          'finalresult.pred_lv2_score','finalresult.pred_lv3_score','finalresult.pred_final_score').show(20)



