from common import const, predict, loader
import pandas as pd
import sys
import json
import fastavro  # 0.17.9
import os
from multiprocessing import Pool
import logging

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

model_lv1 = None
model_lv2 = None
model_lv3 = None


def load_model():
    global model_lv1
    model_lv1 = loader.load_model_lv1(const.get_model_file_path(const.FILE_PATH_MODEL_PRODUCTION) + "lv1/")

    global model_lv2s
    model_lv2s = loader.load_model_sub(const.get_model_file_path(const.FILE_PATH_MODEL_PRODUCTION) + "lv2/", 2)

    global model_lv3s
    model_lv3s = loader.load_model_sub(const.get_model_file_path(const.FILE_PATH_MODEL_PRODUCTION) + "lv3/", 3)


# get prediction result for one avro file
def create_cqi_output(filename):

    lst = list()
    # read in one avro file
    with open(const.get_cqi_input_file_path() + filename, 'rb') as fo:
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
                 record['categoryNameLv1']])

    # noinspection PyUnresolvedReferences
    df = pd.DataFrame(
        lst,
        columns=['itemId',
                 'productId',
                 'categoryCode',
                 'originalAttr',
                 'normalizedAttr',
                 'excludeType',
                 'categoryCodeLv1',
                 'categoryNameLv1'])
    lst = None

    df['originCateCode'] = df['categoryCode']
    df['originString'] = df['originalAttr']
    df['cleanseString'] = ''
    df['predCateCode'] = ''
    df['predCateCode1'] = ''
    df['predCateCode2'] = ''
    df['predCateCode3'] = ''
    df['predCateCode4'] = ''
    df['predCateCode5'] = ''
    df['predCateCode6'] = ''
    df['scoreCateCode1'] = 0.0
    df['scoreCateCode2'] = 0.0
    df['scoreCateCode3_6'] = 0.0
    df['scoreFinal'] = 0.0
    df['success'] = 0

    # noinspection PyUnresolvedReferences
    cleansed_prod_df = pd.read_csv(
        const.get_cleansed_prod_dictionary_file_name(),
        names=['productId', 'isCleansed'],
        sep='\t',
        dtype=[('productId', 'long'), ('isCleansed', 'str')])

    # df = pd.merge(df, book_cate_df, on='originCateCode', how='left')
    # df = pd.merge(df, jikgu_prod_df, on='productId', how='left')
    # noinspection PyUnresolvedReferences
    df = pd.merge(df, cleansed_prod_df, on='productId', how='left')

    for i, row in df.iterrows():
        if not df.at[i, 'originString'] or len(df.at[i, 'originString']) == 0:
            continue

        pred = predict.predict(
            model_lv1,
            model_lv2s,
            model_lv3s,
            df.at[i, 'normalizedAttr'],  # input already garbage filtered string
            product_id=df.at[i, 'productId'],
            item_id=df.at[i, 'itemId'],
            garbage_filter=False)[0]

        df.at[i, 'cleanseString'] = pred.get_normalized_input_string()

        if "OLD" not in str(df.at[i, 'categoryNameLv1']).upper():

            if "JIKGU" in df.at[i, 'excludeType']:
                continue

            if "BOOK" in df.at[i, 'excludeType']:
                continue

            if "DVD" in df.at[i, 'excludeType']:
                continue

            if df.at[i, 'isCleansed'] == '1':
                if len(str(df.at[i, 'excludeType'])) == 0:
                    df.at[i, 'excludeType'] = 'OPERATOR_MODEL'
                else:
                    df.at[i, 'excludeType'] = str(df.at[i, 'excludeType']) + ',OPERATOR_MODEL'
                continue

            if pred.get_predict_error() is True:
                continue

            if pred.get_final_score() < 0.25:
                df.at[i, 'scoreCateCode1'] = pred.get_lv1_score()
                df.at[i, 'scoreCateCode2'] = pred.get_lv2_score()
                df.at[i, 'scoreCateCode3_6'] = pred.get_lv3_score()
                df.at[i, 'scoreFinal'] = pred.get_final_score()
                continue

        df.at[i, 'predCateCode'] = pred.get_catecode()
        df.at[i, 'predCateCode1'] = pred.get_lv1_catecode()
        df.at[i, 'predCateCode2'] = pred.get_lv2_catecode()
        df.at[i, 'predCateCode3'] = pred.get_lv3_catecode()
        df.at[i, 'predCateCode4'] = pred.get_lv4_catecode()
        df.at[i, 'predCateCode5'] = pred.get_lv5_catecode()
        df.at[i, 'predCateCode6'] = pred.get_lv6_catecode()
        df.at[i, 'scoreCateCode1'] = pred.get_lv1_score()
        df.at[i, 'scoreCateCode2'] = pred.get_lv2_score()
        df.at[i, 'scoreCateCode3_6'] = pred.get_lv3_score()
        df.at[i, 'scoreFinal'] = pred.get_final_score()
        if pred.get_predict_error() is True:
            df.at[i, 'success'] = 0
        else:
            df.at[i, 'success'] = 1

    # write result out to avro file
    schema = {
        'name': 'topLevelRecord',
        'type': 'record',
        'fields': [
            {'name': 'itemId', 'type': ['long', 'null']},
            {'name': 'productId', 'type': ['long', 'null']},
            {'name': 'originCateCode', 'type': ['string', 'null']},
            {'name': 'originString', 'type': 'string'},
            {'name': 'cleanseString', 'type': 'string'},
            {'name': 'predCateCode', 'type': ['string', 'null']},
            {'name': 'predCateCode1', 'type': ['string', 'null']},
            {'name': 'predCateCode2', 'type': ['string', 'null']},
            {'name': 'predCateCode3', 'type': ['string', 'null']},
            {'name': 'predCateCode4', 'type': ['string', 'null']},
            {'name': 'predCateCode5', 'type': ['string', 'null']},
            {'name': 'predCateCode6', 'type': ['string', 'null']},
            {'name': 'scoreCateCode1', 'type': ['float', 'null']},
            {'name': 'scoreCateCode2', 'type': ['float', 'null']},
            {'name': 'scoreCateCode3_6', 'type': ['float', 'null']},
            {'name': 'scoreFinal', 'type': ['float', 'null']},
            {'name': 'excludeType', 'type': 'string'}
        ]}

    output = df[
        ['itemId',
         'productId',
         'originCateCode',
         'originString',
         'cleanseString',
         'predCateCode',
         'predCateCode1',
         'predCateCode2',
         'predCateCode3',
         'predCateCode4',
         'predCateCode5',
         'predCateCode6',
         'scoreCateCode1',
         'scoreCateCode2',
         'scoreCateCode3_6',
         'scoreFinal',
         'excludeType']]

    records = output.to_json(orient='records')
    records = json.loads(records)
    with open(const.get_cqi_output_file_path() + filename, 'wb') as out:
        fastavro.writer(out, schema, records)

    logger.info("Successfully write " + filename)


def run_cqi_predict(arg_value):

    logger.info("cqi predict started...")
    # create a directory to store the result

    if arg_value != "resume":
        const.delete_file(const.get_cqi_output_file_path())
        os.makedirs(const.get_cqi_output_file_path(), exist_ok=True)

    # select n avro files
    input_directory = os.fsencode(const.get_cqi_input_file_path())
    input_file_names = [os.fsdecode(s) for s in os.listdir(input_directory)]
    input_file_names = list(filter(lambda x: x.endswith(".avro"), input_file_names))

    output_directory = os.fsencode(const.get_cqi_output_file_path())
    output_file_names = [os.fsdecode(s) for s in os.listdir(output_directory)]
    output_file_names = list(filter(lambda x: x.endswith(".avro"), output_file_names))

    set_input_file_name = set(input_file_names)
    set_output_file_name = set(output_file_names)

    if arg_value == "resume":
        final_file_name = set_input_file_name - set_output_file_name
    else:
        final_file_name = set_input_file_name

    # multi-processing
    thread_num = 10
    pool = Pool(processes=thread_num, maxtasksperchild=1)
    pool.starmap(create_cqi_output, zip(final_file_name))

    pool.close()
    pool.join()

    os.system('touch ' + const.get_cqi_output_file_path() + '_SUCCESS')

    logger.info("cqi predict complete!")


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        arg = "resume"
    else:
        arg = sys.argv[1]
        if arg != "resume" and arg != "overwrite":
            sys.exit("Please enter python " + __file__ + " [resume/overwrite]. Default is resume")
    load_model()
    run_cqi_predict(arg)
