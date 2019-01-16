
# coding: utf-8

# In[1]:


import json
import regex
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.session import SparkSession
from pyspark import SparkFiles


THREDSHOLD = 0.6

sc = SparkContext.getOrCreate()

sc.addFile('s3://s3-cdp-prod-airflow-dag/1.10/artifacts/brandnorm/cqi_brand/python/ner/crf.model.5Feat_33018Pos_11350Neg')
sc.addFile('s3://s3-cdp-prod-airflow-dag/1.10/artifacts/brandnorm/cqi_brand/python/ner/brand_dict.txt')
#sc.addPyFile('crfTaggerManager.py')

spark = SparkSession(sc)

with open(SparkFiles.get('brand_dict.txt'), 'r') as infile:
        brandDictMap = json.load(infile)

from crf_title_tagger import extract_features

# load logger
from log_factory import *
logger = get_prod_logger()
        
def get_raw_df():
    df = spark.read.orc('s3://s3-cdp-prod-hive/temp/cqi_item_brand_field_no_extraction/')\
        .select('item_id','original_category_codes','level_one_category_codes','level_two_category_codes','original_product_name','original_brand')\
        .toDF('itemId','originalCategory','levelOneCategory','levelTwoCategory','originalProductName','originalbrand')
    return df

def title_brand_normalize(string, brandDict=brandDictMap):
    stringList = string.lower().split()
    res = trigram(stringList, brandDict)
    return (res)


def trigram(stringList, brandDict):
    if len(stringList) < 3:
        return bigram(stringList, brandDict)
    for i in range(len(stringList) - 2):
        if ' '.join([stringList[i], stringList[i + 1], stringList[i + 2]]) in brandDict:
            # print (' '.join([stringList[i], stringList[i+1], stringList[i+2]]))
            if i == len(stringList) - 3:  # if i is third to last word
                return (bigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1], stringList[i + 2]])]).strip()
            else:
                return (bigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1], stringList[i + 2]])] + ' ' + trigram(stringList[i + 3:],
                                                                                                     brandDict)).strip()
    return bigram(stringList, brandDict)


def bigram(stringList, brandDict):
    if len(stringList) < 2:
        return unigram(stringList, brandDict)
    for i in range(len(stringList) - 1):
        if ' '.join([stringList[i], stringList[i + 1]]) in brandDict:
            # print (' '.join([stringList[i], stringList[i+1]]))
            if i == len(stringList) - 2:  # if i is the second to last word
                return (unigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1]])]).strip()
            else:
                return (unigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1]])] + ' ' + bigram(stringList[i + 2:], brandDict)).strip()
    return unigram(stringList, brandDict)

def unigram(stringList, brandDict):
    res = ''
    for s in stringList:
        if s in brandDict:
            res = res + brandDict[s] + ' '
        else:
            res = res + s + ' '
    return (res.strip())


def cleanSpecialChar(s):
    return pattern.sub(' ', s)


def get_ner_brand(titleToken, brand_signal, probability):
    terms = titleToken.split()
    ner_brand = ''
    if probability >= THREDSHOLD and any([t == 'b' for t in brand_signal]):
        first_index = brand_signal.index('b')
        ner_brand = terms[first_index]
        while first_index + 1 < len(brand_signal) and brand_signal[first_index + 1] == 'i':
            ner_brand = ner_brand + ' ' + terms[first_index + 1]
            first_index += 1
    return ner_brand

def is_brand_in_dict(brand, brandDict=brandDictMap):
    if not brand.strip():
        return False
    else:
        return brand in brandDict
        
dataDFRaw = get_raw_df()

# remove special characters; normalize synonyms by using current brand dictionary
pattern = regex.compile("[^\p{L}\p{N}'.]")

clean_func = udf(cleanSpecialChar, StringType())   
dataDFRaw = dataDFRaw.withColumn('productNameToken', clean_func('originalProductName')) 

norm_func = udf(title_brand_normalize, StringType())   
dataDFRaw = dataDFRaw.withColumn('productNameTokenNorm', norm_func('productNameToken')) 

schema = StructType([
    StructField('brand_signal', ArrayType(StringType())),
    StructField('probability', FloatType())
])
udf_tagger_feature = udf(extract_features, schema)
tokenWithFeatureData = dataDFRaw.withColumn("feature", udf_tagger_feature('productNameTokenNorm'))  


udf_ner_brand = udf(get_ner_brand, StringType())
ner_brand_df = tokenWithFeatureData.withColumn('nerBrand', udf_ner_brand('productNameTokenNorm',
     'feature.brand_signal','feature.probability')) 

udf_in_dict = udf(is_brand_in_dict, BooleanType())
ner_brand_df = ner_brand_df.withColumn('nerBrandInDict', udf_in_dict('nerBrand'))


final_df = ner_brand_df.select('itemId','originalCategory','levelOneCategory','levelTwoCategory','originalProductName',
                               'originalbrand', 'productNameToken', 'productNameTokenNorm', 'feature.brand_signal',
                               'feature.probability','nerBrand','nerBrandInDict')

final_df.write.orc("s3://catalog-quality-item-prod/ner/title/", mode="overwrite")