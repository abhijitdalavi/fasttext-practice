from common import feature, const, util
import fasttext
# from pyfasttext import FastText
import os
import sys
import findspark
import pyspark
from pyspark.sql import SparkSession
import logging

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def format_data_line(line):

    result_string = ''

    try:
        (product_id,
         catecode,
         int_catecode1,
         int_cate1,
         int_catecode2,
         int_cate2,
         int_catecode3,
         int_cate3,
         int_catecode4,
         int_cate4,
         int_catecode5,
         int_cate5,
         int_catecode6,
         int_cate6,
         input_string) = line.split("\t")

        if int_catecode1 == const.CATE_INT_BOOK:
            text_norm = feature.normalize_text(input_string, token=False, date_filter=True)
        else:
            text_norm = feature.normalize_text(input_string)

        lb_lv1 = int_catecode1.replace('.0', '')
        lb_lv2 = int_catecode2.replace('.0', '')
        lb_lv3 = int_catecode3.replace('.0', '')
        lb_lv4 = int_catecode4.replace('.0', '')
        lb_lv5 = int_catecode5.replace('.0', '')
        lb_lv6 = int_catecode6.replace('.0', '')

        # lb_lv3_6 = lb_lv3 + "_" + lb_lv4 + "__"

        if len(lb_lv4) == 0 or lb_lv4 == '0' or lb_lv3 == lb_lv4:
            lb_lv3_6 = lb_lv3 + "___"
        else:
            if len(lb_lv5) == 0 or lb_lv5 == '0' or lb_lv4 == lb_lv5:
                lb_lv3_6 = lb_lv3 + "_" + lb_lv4 + "__"
            else:
                if len(lb_lv6) == 0 or lb_lv6 == '0' or lb_lv5 == lb_lv6:
                    lb_lv3_6 = lb_lv3 + "_" + lb_lv4 + "_" + lb_lv5 + "_"
                else:
                    lb_lv3_6 = lb_lv3 + "_" + lb_lv4 + "_" + lb_lv5 + "_" + lb_lv6

        result_string = (lb_lv1 + '\t' + lb_lv2 + '\t' + lb_lv3_6 + '\t' + text_norm)

    except Exception as e:
        logger.error(e)

    return result_string


def get_category_dict(input_file_name, token_index, category_dict):

    with open(input_file_name, 'r') as input_f:

        for line in input_f:

            line_split = line.split('\t')

            label = line_split[token_index]
            label = label.replace('.0', '')

            if len(label) > 0:
                if label not in category_dict:
                    category_dict[label] = 1

    return category_dict


# TODO: Need to wait for Fast text to provide model load function release for faster iteration process
# ./fast text supervised -input train.txt -output model -inputModel model.bin -incr
def train(model_path, level=1, loss_function='softmax', resume="resume"):
    # model = FastText()

    logger.info('Training LEVEL' + str(level) + ' model started')

    category_dict_train = dict()
    category_dict_test = dict()
    summary_logs = list()

    if resume != "resume":
        logger.info('Non-resume training...delete model directory')
        const.delete_file(const.get_model_file_path(model_path) + "lv" + str(level) + "/")
    else:
        logger.info('Resume training enabled')

    if level == 1:
        category_dict_train['ROOT'] = 1
        category_dict_test['ROOT'] = 1
    else:
        label_index = 2 * level - 2

        category_dict_train = get_category_dict(const.get_raw_train_file_name(), label_index, category_dict_train)
        category_dict_test = get_category_dict(const.get_raw_validate_file_name(), label_index, category_dict_test)

    for label in category_dict_train:

        if resume == "resume":
            file_exist = const.get_model_file_exist(model_path, level=level, cate=label)
            if file_exist:
                logger.info("There is already model for LV" + str(level) + " " + str(label) + " skip training...")
                continue

        word_n_gram = 3
        learning_rate = 0.1
        current_best_name = ''
        current_best_score = 0.0
        patience = 0
        lf = loss_function
        max_patience = const.TRAINING_MAX_PATIENCE
        epoch_start = 50

        if label == const.CATE_INT_BOOK or label == '34405' or label == '93185':  # domestic/foreign books
            bucket = 3000000
            epoch_start = 50
            max_patience = 0
            lf = 'hs'
        else:

            if level == 1:
                bucket = 2000000
                epoch_start = 50
                learning_rate = 0.5
            elif level == 2:
                bucket = 1000000
                epoch_start = 200
                learning_rate = 0.1
            else:
                epoch_start = 300
                learning_rate = 0.1
                train_file_size = os.path.getsize(const.get_processed_train_sub_file_name(label, level))
                if train_file_size > 100000000:  # 100MB
                    bucket = 3000000
                elif train_file_size > 40000000:  # 40MB
                    bucket = 2000000
                elif train_file_size > 1000000:  # 1MB
                    bucket = 100000
                else:
                    bucket = 10000

        for epoch in range(epoch_start, 301):

            start_date = const.get_time()

            model_file_name = const.get_model_file_name(
                model_path,
                level=level,
                epoch=epoch,
                lr=learning_rate,
                cate=label)

            classifier = fasttext.supervised(
                input_file=const.get_processed_train_sub_file_name(label, level),
                output=model_file_name,
                lr=learning_rate,
                epoch=epoch,
                loss=lf,
                word_ngrams=word_n_gram,
                thread=const.TRAINING_THREAD,
                silent=0,
                encoding='utf-8',
                ws=5,
                dim=50,
                bucket=bucket)

            if label not in category_dict_test:
                logger.warning("There is no testing data with level" + str(level) + " catecode:" + label)
                break

            result = classifier.test(const.get_processed_test_sub_file_name(label, level))
            end_date = const.get_time()

            result_log = ("LV" + str(level) + " " + str(label) + ': precision:' + str(round(result.precision, 4)) +
                          '(size:' + str(result.nexamples) + ', labels:' + str(len(classifier.labels)) +
                          ') ep:' + str(epoch) + ', lr:' + str(learning_rate) +
                          ', n-gram:' + str(word_n_gram) +
                          ', duration:' + str(end_date - start_date))

            if current_best_score < result.precision:
                current_best_score = result.precision
                logger.info(result_log + ' Model improved!!!!')
                if current_best_name != '':
                    os.remove(current_best_name)
                current_best_name = model_file_name + '.bin'
                patience = 0
            else:
                logger.info(result_log)
                os.remove(model_file_name + '.bin')
                patience = patience + 1

            if patience >= max_patience:
                summery_log = "LV" + str(level) + " " + str(label) + ' Patience exceed ' + \
                              str(const.TRAINING_MAX_PATIENCE) + ', best score is:' + \
                              str(round(current_best_score, 4)) + '(size:' + str(result.nexamples) + \
                              ', labels:' + str(len(classifier.labels)) + ')'

                logger.info(summery_log)
                summary_logs.append(summery_log)

                break

            sys.stdout.flush()

    logger.info('Training LEVEL' + str(level) + '  model completed!')
    logger.info('=============================')
    for log in summary_logs:
        logger.info(log)
    logger.info('=============================')


def pre_process(input_file, output_file_path, output_file_prefix):

    logger.info('Pre-processing started for file:' + input_file)

    findspark.init()
    sc = None
    try:
        sc = pyspark.SparkContext(appName=__name__)
        spark = SparkSession(sc)

        const.delete_file(output_file_path)

        raw_data = sc.textFile(input_file)
        processed_data = raw_data.map(format_data_line)
        standard_data = processed_data.map(lambda row: row.split("\t"))
        standard_data.persist(pyspark.StorageLevel.DISK_ONLY)

        lv1_data = standard_data.map(
            lambda row: ('ROOT',
                         (const.MODEL_LABEL_KEY +
                          row[0] + ' ' +
                          row[3])))

        lv1_data.persist(pyspark.StorageLevel.DISK_ONLY)
        lv1_df = lv1_data.toDF()
        lv1_output_file_prefix = output_file_path + "lv1/" + output_file_prefix + "lv1_"
        lv1_df.write.partitionBy("_1").csv(lv1_output_file_prefix)
        util.coalesce_folder(lv1_output_file_prefix, append=False)

        """
        if dropout:
            lv1_dropout_data = lv1_data.map(lambda row: (row[0], feature.dropout(row[1])))
            lv1_dropout_df = lv1_dropout_data.toDF()
            lv1_dropout_df.write.partitionBy("_1").csv(lv1_output_file_prefix)
            coalesce_file(lv1_output_file_prefix, append=True)
        """
        lv1_data.unpersist()

        lv2_data = standard_data.map(
            lambda row: (row[0],
                         (const.MODEL_LABEL_KEY +
                          row[0] + '_' +
                          row[1] + ' ' +
                          row[3])))

        lv2_data.persist(pyspark.StorageLevel.DISK_ONLY)
        lv2_df = lv2_data.toDF()
        lv2_output_file_prefix = output_file_path + "lv2/" + output_file_prefix + "lv2_"
        lv2_df.write.partitionBy("_1").csv(lv2_output_file_prefix)
        util.coalesce_folder(output_file_path + "lv2/" + output_file_prefix + "lv2_", append=False)

        """
        if dropout:
            lv2_dropout_data = lv2_data.map(lambda row: (row[0], feature.dropout(row[1])))
            lv2_dropout_df = lv2_dropout_data.toDF()
            lv2_dropout_df.write.partitionBy("_1").csv(lv2_output_file_prefix)
            coalesce_file(lv2_output_file_prefix, append=True)
        """
        lv2_data.unpersist()

        lv3_data = standard_data.map(
            lambda row: (row[1],
                         (const.MODEL_LABEL_KEY +
                          row[0] + '_' +
                          row[1] + '_' +
                          row[2] + ' ' +
                          row[3])))

        lv3_data.persist(pyspark.StorageLevel.DISK_ONLY)
        lv3_df = lv3_data.toDF()
        lv3_output_file_prefix = output_file_path + "lv3/" + output_file_prefix + "lv3_"
        lv3_df.write.partitionBy("_1").csv(lv3_output_file_prefix)
        util.coalesce_folder(lv3_output_file_prefix, append=False)
        """
        if dropout:
            lv3_dropout_data = lv3_data.map(lambda row: (row[0], feature.dropout(row[1])))
            lv3_dropout_df = lv3_dropout_data.toDF()
            lv3_dropout_df.write.partitionBy("_1").csv(lv3_output_file_prefix)
            coalesce_file(lv3_output_file_prefix, append=True)
        """
        lv3_data.unpersist()

        standard_data.unpersist()
        spark.stop()

    finally:
        sc.stop()

    logger.info('Pre-processing completed for file:' + input_file)


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        arg = "all"
    else:
        arg = sys.argv[1]

    if "all" == arg or "only_process" == arg:

        response = input("Are you sure you want to run pre process? It will delete previous files [y/n] ")

        if response == "n":
            sys.exit("Process stopped.")

        pre_process(const.get_raw_validate_file_name(),
                    const.get_processed_test_file_path(),
                    const.PROCESSED_TEST_PREFIX)

        pre_process(const.get_raw_train_file_name(),
                    const.get_processed_train_file_path(),
                    const.PROCESSED_TRAIN_PREFIX)

    if "all" == arg or "only_train" == arg:

        if len(sys.argv) <= 2:
            train_arg = "resume"
        else:
            train_arg = sys.argv[2]

        if train_arg != "resume":
            response = input("Are you sure you want to re-train? It will delete previous files [y/n] ")

            if response == "n":
                sys.exit("Process stopped.")

        train(const.FILE_PATH_MODEL_CURRENT, 1, resume=train_arg)
        train(const.FILE_PATH_MODEL_CURRENT, 2, resume=train_arg)
        train(const.FILE_PATH_MODEL_CURRENT, 3, resume=train_arg)
