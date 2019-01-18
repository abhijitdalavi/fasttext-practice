# -*- coding: utf-8 -*-
"""
This module provides centralizes place to store and manage constant variable values
for log formats, dictionary key name ,file path, file name, etc.

"""

__author__ = "Steve"
__status__ = "production"
__version__ = "0.1"
__date__ = "Jan 2019"

import definitions
import os
import sys
import datetime
sys.path.append('../')

LOG_MSG_FORMAT = '[%(levelname)s] %(asctime)s %(message)s'
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

JSON_KEY_ROOT = "categoryPrediction"
JSON_KEY_PRODUCTS = "products"
JSON_KEY_PRODUCT_ID = "productId"
JSON_KEY_PRODUCT_NAME = "productName"
JSON_KEY_PRODUCT_BRAND = "brand"
JSON_KEY_PRODUCT_ATTRIBUTES = "attributes"
JSON_KEY_PRODUCT_DESCRIPTION = "productDescription"
JSON_KEY_TEXT_RAW = "productText"
JSON_KEY_TEXT_NORM = "normalizedProductText"
JSON_KEY_TOP = "top"
JSON_KEY_LEVEL = "level"
JSON_KEY_PREDICT_TYPE = "predictType"
JSON_KEY_RANK = "rank"
JSON_KEY_SCORE_FINAL = "scoreFinal"
JSON_KEY_SCORE_LV1 = "scoreLv1"
JSON_KEY_SCORE_LV2 = "scoreLv2"
JSON_KEY_SCORE_LV3 = "scoreLv3"
JSON_KEY_SCORE_LV4 = "scoreLv4"
JSON_KEY_PREDICT = "predict"
JSON_KEY_SUCCESS = "success"
JSON_KEY_ERROR = "error"
JSON_KEY_CATE1 = "cateIdLv1"
JSON_KEY_CATE2 = "cateIdLv2"
JSON_KEY_CATE3 = "cateIdLv3"
JSON_KEY_CATE4 = "cateIdLv4"
JSON_KEY_CATE5 = "cateIdLv5"
JSON_KEY_CATE6 = "cateIdLv6"

FILE_PATH_PROJECT = definitions.ROOT_DIR
FILE_PATH_MODEL_CURRENT = "current"
FILE_PATH_MODEL_PRODUCTION = "production"

MODEL_LABEL_KEY = "__label__"
MODEL_PREFIX = "cp_int_cate"
MODEL_LV1_PREFIX = "cp_int_cate1_"
MODEL_LV4_PREFIX = "cp_int_cate4_"
MODEL_POSTFIX = ".bin"

PROCESSED_TRAIN_PREFIX = 'processed_train_'
PROCESSED_TEST_PREFIX = 'processed_test_'

DELIMITER_KEYWORD = ' |,|_|:|\+|\.|/|-|[(|)]|\[|\]|\n'
DELIMITER_LOCALE = r'([a-zA-Z]+)'
# DELIMITER_LOCALE = r'([a-zA-Z0-9]+)'

TRAINING_MAX_PATIENCE = 0
TRAINING_THREAD = 32
MAX_TOP = 5

TEXT_NORM_DELIMITER = ' '

CATE_INT_NAMES = {'80285': 'Kitchen',
                  '56112': 'Beauty',
                  '63897': 'Household',
                  '102984': 'Toy',
                  '59258': 'Food',
                  '77834': 'Homedeco',
                  '69182': 'Fashion',
                  '76844': 'Baby',
                  '78647': 'Car',
                  '79648': 'Office',
                  '65799': 'Pet',
                  '62588': 'Electronics',
                  '66679': 'Books',
                  '79138': 'Album/DVD',
                  '103371': 'Sports'}

CATE_INT_BOOK = '66679'
CATE_INT_SPORTS = '103371'
CATE_INT_FASHION = '69182'
CATE_INT_BABY = '76844'

PREDICT_TYPE_ML = 'ML'
PREDICT_TYPE_RULE = 'RULE'


def get_file_directory(file_path, levels=0):
    common = file_path
    for i in range(levels + 1):
        common = os.path.dirname(common)
    return os.path.abspath(common)


def check_dir(file_name):

    dir_name = get_file_directory(file_name)

    os.makedirs(dir_name, exist_ok=True)


def get_data_file_path():

    file_path = definitions.ROOT_DIR + "data/"

    check_dir(file_path)

    return file_path


def get_raw_file_path():

    file_path = get_data_file_path() + "raw/"
    check_dir(file_path)
    return file_path


def get_cds_file_path():

    file_path = get_data_file_path() + "cds/"
    check_dir(file_path)
    return file_path


def get_garbage_file_path():

    file_path = get_data_file_path() + "garbage/"
    check_dir(file_path)
    return file_path


def get_common_file_path():

    file_path = definitions.ROOT_DIR + "common/"
    check_dir(file_path)
    return file_path


def get_processed_file_path():

    file_path = get_data_file_path() + "processed/"
    check_dir(file_path)
    return file_path


def get_processed_test_file_path():

    file_path = get_processed_file_path() + "test/"
    check_dir(file_path)
    return file_path


def get_processed_train_file_path():

    file_path = get_processed_file_path() + "train/"
    check_dir(file_path)
    return file_path


def get_cqi_file_path():

    file_path = get_data_file_path() + "cqi/"
    check_dir(file_path)
    return file_path


def get_cqi_input_file_path():

    file_path = get_cqi_file_path() + "input/"
    check_dir(file_path)
    return file_path


def get_cqi_output_file_path():

    file_path = get_cqi_file_path() + "output/"
    check_dir(file_path)
    return file_path


def get_raw_temp_file_path():

    file_path = get_raw_file_path() + 'temp/'
    check_dir(file_path)
    return file_path


def get_raw_train_vitamin_file_name():

    file_name = get_raw_temp_file_path() + 'raw_train_vitamin.txt'
    return file_name


def get_raw_train_cds_file_name():

    file_name = get_raw_temp_file_path() + 'raw_train_cds.txt'
    return file_name


def get_raw_train_cds_temp_file_name():

    file_name = get_raw_temp_file_path() + 'raw_train_cds_temp.txt'
    return file_name


def get_raw_train_product_id_file_name():
    file_name = get_raw_temp_file_path() + 'product_id_list_for_cds_retrieval_train.csv'
    return file_name


def get_raw_train_product_id_with_catecode_file_name():
    file_name = get_raw_temp_file_path() + 'product_id_catecode_list_for_cds_retrieval_train.csv'
    return file_name


def get_raw_category_file_name():

    file_name = get_raw_file_path() + 'category.txt'
    return file_name


def get_raw_event_category_file_name():

    file_name = get_raw_file_path() + 'event_category.txt'
    return file_name


def get_raw_train_file_name():

    file_name = get_raw_file_path() + 'raw_train.txt'
    return file_name


def get_raw_test_vitamin_file_name():
    file_name = get_raw_temp_file_path() + 'raw_validate_vitamin.txt'
    return file_name


def get_raw_augment_temp():
    file_name = get_raw_temp_file_path() + 'raw_augment_temp.txt'
    return file_name


def get_raw_validate_cds_file_name():
    file_name = get_raw_temp_file_path() + 'raw_validate_cds.txt'
    return file_name


def get_raw_validate_cds_temp_file_name():
    file_name = get_raw_temp_file_path() + 'raw_validate_cds_temp.txt'
    return file_name


def get_raw_validate_product_id_file_name():
    file_name = get_raw_temp_file_path() + 'product_id_list_for_cds_retrieval_validate.csv'
    return file_name


def get_raw_validate_product_id_with_catecode_file_name():
    file_name = get_raw_temp_file_path() + 'product_id_catecode_list_for_cds_retrieval_validate.csv'
    return file_name


def get_raw_validate_file_name():
    # file_name = get_raw_file_path() + 'raw_audit.txt'
    file_name = get_raw_file_path() + 'raw_validate.txt'
    return file_name


def get_garbage_product_names_file_name(year_month):
    file_name = get_garbage_file_path() + 'garbage_product_names_' + year_month + '.txt'
    return file_name


def get_processed_train_lv1_file_name():

    file_name = get_processed_file_path() + PROCESSED_TRAIN_PREFIX + 'lv1.txt'
    return file_name


def get_processed_train_sub_file_name(parent_cate_code, level):

    file_name = (get_processed_train_file_path() + "lv" + str(level) + "/" +
                 PROCESSED_TRAIN_PREFIX +
                 'lv' + str(level) + '_' +
                 str(parent_cate_code) + '.txt')

    return file_name


def get_processed_test_lv1_file_name():

    file_name = get_processed_file_path() + PROCESSED_TEST_PREFIX + 'lv1.txt'
    return file_name


def get_processed_test_sub_file_name(parent_cate_code, level):

    file_name = (get_processed_test_file_path() + "lv" + str(level) + "/" +
                 PROCESSED_TEST_PREFIX +
                 'lv' + str(level) + '_' +
                 str(parent_cate_code) + '.txt')

    return file_name


def get_model_file_path(sub_file_directory=None):

    if sub_file_directory is None:
        file_path = get_data_file_path() + "model/"
    else:
        file_path = get_data_file_path() + "model/" + sub_file_directory + "/"

    check_dir(file_path)

    return file_path


def get_model_file_name(sub_file_directory, level, epoch, lr, cate=None):

    sub_cate = '_' + str(cate)
    file_path = get_model_file_path(sub_file_directory) + "lv" + str(level) + "/"
    check_dir(file_path)
    file_name = (file_path + 'cp_int_cate' + str(level) + sub_cate + '_' + str(epoch) + '_' + str(lr))
    return file_name


def get_model_file_exist(sub_file_directory, level, cate=None):

    sub_cate = '_' + str(cate)
    file_path = get_model_file_path(sub_file_directory) + "lv" + str(level) + "/"
    check_dir(file_path)

    for file in os.listdir(file_path):
        if file.startswith('cp_int_cate' + str(level) + sub_cate + '_'):
            return True

    return False


def get_dictionary_file_path():
    return ""


def get_feedback_file_path():

    file_path = get_data_file_path() + "feedback/"
    return file_path


def get_audit_file_path():

    file_path = get_data_file_path() + "audit/"
    return file_path


def get_audit_temp_file_path():

    file_path = get_audit_file_path() + 'temp/'
    check_dir(file_path)
    return file_path


def get_audit_temp_file_name():
    file_name = get_audit_temp_file_path() + 'audit_temp.txt'
    return file_name


def get_token_dictionary_file_name():

    file_name = get_dictionary_file_path() + "dict_token.txt"
    return file_name


def get_garbage_dictionary_file_name():

    file_name = get_dictionary_file_path() + "dict_garbage.txt"
    return file_name


def get_book_cate_dictionary_file_name():

    file_name = get_dictionary_file_path() + "dict_book_cate.txt"
    return file_name


def get_jikgu_prod_dictionary_file_name():

    file_name = get_dictionary_file_path() + "jikgu_prod_dictionary.txt"
    return file_name


def get_cleansed_prod_dictionary_file_name():

    file_name = get_dictionary_file_path() + "cleansed_prod_dictionary.txt"
    return file_name


def get_feedback_catalog_file_name():

    file_name = get_feedback_file_path() + "feedback_catalog.txt"
    return file_name


def get_feedback_seller_file_name():

    file_name = get_feedback_file_path() + "feedback_seller.txt"
    return file_name


def get_feedback_manual_file_name():

    file_name = get_feedback_file_path() + "feedback_manual.txt"
    return file_name


def get_feedback_cate_name_file_name():

    file_name = get_feedback_file_path() + "feedback_cate_name.txt"
    return file_name


def get_sports_brand_file_name():

    file_name = get_dictionary_file_path() + "dict_sports_brand.txt"
    return file_name


def get_sports_definite_keyword_file_name():

    file_name = get_dictionary_file_path() + "dict_sports_definite_keyword.txt"
    return file_name


def get_book_keyword_file_name():

    file_name = get_dictionary_file_path() + "dict_book_keyword.txt"
    return file_name


def get_book_definite_keyword_file_name():

    file_name = get_dictionary_file_path() + "dict_book_definite_keyword.txt"
    return file_name


def get_fashion_keyword_file_name():

    file_name = get_dictionary_file_path() + "dict_fashion_keyword.txt"
    return file_name


def get_fashion_definite_keyword_file_name():

    file_name = get_dictionary_file_path() + "dict_fashion_definite_keyword.txt"
    return file_name


def get_time():
    time = datetime.datetime.now().replace(microsecond=0)
    return time


def file_len(file_name):
    i = 0
    with open(file_name) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def delete_file(dir_or_file_name):
    if os.path.exists(dir_or_file_name):
        os.system("rm -rf " + dir_or_file_name)
