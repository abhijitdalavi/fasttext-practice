from common import loader, const, predict
import sys
from collections import OrderedDict
import logging

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

model_lv1 = None
model_lv2s = None
model_lv3s = None


def load_models(model_version=None):

    _model_version = const.FILE_PATH_MODEL_CURRENT
    if model_version is not None:
        _model_version = model_version

    logger.info("Loading model version:" + _model_version)

    global model_lv1
    model_lv1 = loader.load_model_lv1(const.get_model_file_path(_model_version)+"lv1/")

    global model_lv2s
    model_lv2s = loader.load_model_sub(const.get_model_file_path(_model_version)+"lv2/", 2)

    global model_lv3s
    model_lv3s = loader.load_model_sub(const.get_model_file_path(_model_version)+"lv3/", 3)


"""
def load_models(model_version=None):

    _model_version = const.FILE_PATH_MODEL_CURRENT
    if model_version is not None:
        _model_version = model_version

    logger.info("Loading model version:" + _model_version)

    global model_lv1
    model_lv1 = loader.load_model_lv1(const.get_model_file_path(_model_version))

    global model_lv4s
    model_lv4s = loader.load_model_lv4(const.get_model_file_path(_model_version))
"""


def audit(input_file_name, output_file_name):
    logger.info("audit started")

    with open(input_file_name, 'r') as inf:
        with open(output_file_name, 'w') as ouf:

            for line in inf:

                (product_id,
                 original_cate1,
                 original_cate2,
                 original_cate3,
                 original_cate4,
                 original_cate5,
                 original_cate6,
                 input_string
                 ) = line.split("\t")

                if len(original_cate4) == 0:
                    formatted_cate4 = original_cate3
                else:
                    formatted_cate4 = original_cate4

                if len(original_cate5) == 0:
                    formatted_cate5 = formatted_cate4
                else:
                    formatted_cate5 = original_cate5

                if len(original_cate6) == 0:
                    formatted_cate6 = formatted_cate5
                else:
                    formatted_cate6 = original_cate6

                result = predict.predict(model_lv1, model_lv2s, model_lv3s, input_string, top=3, product_id=product_id)[0]

                ouf.write(product_id)
                ouf.write('\t')
                ouf.write(result.get_input_string())
                ouf.write('\t')
                ouf.write(result.get_normalized_input_string())
                ouf.write('\t')
                ouf.write('Original')
                ouf.write('\t')
                ouf.write(original_cate1)
                ouf.write('\t')
                ouf.write(original_cate2)
                ouf.write('\t')
                ouf.write(original_cate3)
                ouf.write('\t')
                ouf.write(formatted_cate4.strip('\n'))
                ouf.write('\t')
                ouf.write(formatted_cate5.strip('\n'))
                ouf.write('\t')
                ouf.write(formatted_cate6.strip('\n'))
                ouf.write('\n')
                ouf.write(product_id)
                ouf.write('\t')
                ouf.write(result.get_input_string())
                ouf.write('\t')
                ouf.write(result.get_normalized_input_string())
                ouf.write('\t')
                ouf.write('Predict')
                ouf.write('\t')
                ouf.write(str(result.get_lv1_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv2_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv3_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv4_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv5_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv6_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv1_score()))
                ouf.write('\t')
                ouf.write(str(result.get_lv2_score()))
                ouf.write('\t')
                ouf.write(str(result.get_lv3_score()))
                ouf.write('\t')
                ouf.write(result.get_predict_type())
                ouf.write('\n')

    logger.info("audit completed!")


def audit_seller(input_file_name, output_file_name):
    logger.info("audit seller started")

    with open(input_file_name, 'r') as inf:
        with open(output_file_name, 'w') as ouf:

            for line in inf:

                (deal_srl,
                 option_srl,
                 cat_info,
                 p_info,
                 dt
                 ) = line.split(",")

                result = predict.predict(model_lv1, model_lv2s, model_lv3s, cat_info + ' ' + p_info, top=3,
                                         product_id=deal_srl)[0]

                ouf.write(deal_srl)
                ouf.write('\t')
                ouf.write(option_srl)
                ouf.write('\t')
                ouf.write(cat_info)
                ouf.write('\t')
                ouf.write(p_info)
                ouf.write('\t')
                ouf.write(dt.strip('\n'))
                ouf.write('\t')
                ouf.write(str(result.get_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv1_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv2_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv3_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv4_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv5_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv6_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv1_score()))
                ouf.write('\t')
                ouf.write(str(result.get_lv2_score()))
                ouf.write('\t')
                ouf.write(str(result.get_lv3_score()))
                ouf.write('\n')

    logger.info("audit seller completed!")


def evaluate(input_file_name, output_file_name):
    logger.info("evaluate started")

    total_count = 0.0
    lv1_correct_count = 0.0
    lv2_correct_count = 0.0
    lv3_correct_count = 0.0
    lv4_correct_count = 0.0
    lv5_correct_count = 0.0
    lv6_correct_count = 0.0

    total_by_correct_lv1 = dict()
    lv2_correct_by_correct_lv1 = dict()
    lv3_correct_by_correct_lv1 = dict()
    lv4_correct_by_correct_lv1 = dict()
    lv5_correct_by_correct_lv1 = dict()
    lv6_correct_by_correct_lv1 = dict()

    with open(input_file_name, 'r') as inf:
        with open(output_file_name, 'w') as ouf:

            for line in inf:

                (t_type,
                 product_id,
                 input_string,
                 correct_catecode,
                 correct_catecode1,
                 correct_catecode2,
                 correct_catecode3,
                 correct_catecode4,
                 correct_catecode5,
                 correct_catecode6,
                 correct_cate1,
                 correct_cate2,
                 correct_cate3,
                 correct_cate4,
                 correct_cate5,
                 correct_cate6) = line.split("\t")

                result = predict.predict(model_lv1, model_lv2s, model_lv3s, input_string, top=3, product_id=product_id)[0]

                ouf.write(t_type)
                ouf.write('\t')
                ouf.write(product_id)
                ouf.write('\t')
                ouf.write(result.get_input_string())
                ouf.write('\t')
                ouf.write(result.get_normalized_input_string())
                ouf.write('\t')
                ouf.write(correct_catecode1.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_catecode2.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_catecode3.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_catecode4.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_catecode5.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_catecode6.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_cate1.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_cate2.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_cate3.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_cate4.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_cate5.strip('\n'))
                ouf.write('\t')
                ouf.write(correct_cate6.strip('\n'))
                ouf.write('\t')
                ouf.write(str(result.get_lv1_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv2_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv3_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv4_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv5_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv6_catecode()))
                ouf.write('\t')
                ouf.write(str(result.get_lv1_score()))
                ouf.write('\t')
                ouf.write(str(result.get_lv2_score()))
                ouf.write('\t')
                ouf.write(str(result.get_lv3_score()))
                ouf.write('\t')
                ouf.write(str(result.get_final_score()))
                ouf.write('\t')
                ouf.write(result.get_predict_type())
                ouf.write('\t')
                if correct_catecode1 == str(result.get_lv1_catecode()):
                    ouf.write("LV1_CORRECT")
                else:
                    ouf.write("LV1_WRONG")
                ouf.write('\t')
                if correct_catecode2 == str(result.get_lv2_catecode()):
                    ouf.write("LV2_CORRECT")
                else:
                    ouf.write("LV2_WRONG")
                ouf.write('\t')
                if correct_catecode3 == str(result.get_lv3_catecode()):
                    ouf.write("LV3_CORRECT")
                else:
                    ouf.write("LV3_WRONG")
                ouf.write('\t')
                if correct_catecode4 == str(result.get_lv4_catecode()):
                    ouf.write("LV4_CORRECT")
                else:
                    ouf.write("LV4_WRONG")
                ouf.write('\t')
                if correct_catecode5 == str(result.get_lv5_catecode()):
                    ouf.write("LV5_CORRECT")
                else:
                    ouf.write("LV5_WRONG")
                ouf.write('\t')
                if correct_catecode6 == str(result.get_lv6_catecode()):
                    ouf.write("LV6_CORRECT")
                else:
                    ouf.write("LV6_WRONG")
                ouf.write('\n')

                total_count = total_count + 1.0
                if correct_catecode1 not in total_by_correct_lv1:
                    total_by_correct_lv1[correct_catecode1] = 1.0
                else:
                    total_by_correct_lv1[correct_catecode1] = \
                        total_by_correct_lv1[correct_catecode1] + 1.0

                if str(result.get_lv1_catecode()) == correct_catecode1:
                    lv1_correct_count = lv1_correct_count + 1.0

                if str(result.get_lv2_catecode()) == correct_catecode2:
                    lv2_correct_count = lv2_correct_count + 1.0
                    if correct_catecode1 not in lv2_correct_by_correct_lv1:
                        lv2_correct_by_correct_lv1[correct_catecode1] = 1.0
                    else:
                        lv2_correct_by_correct_lv1[correct_catecode1] = \
                            lv2_correct_by_correct_lv1[correct_catecode1] + 1.0

                if str(result.get_lv3_catecode()) == correct_catecode3:
                    lv3_correct_count = lv3_correct_count + 1.0
                    if correct_catecode1 not in lv3_correct_by_correct_lv1:
                        lv3_correct_by_correct_lv1[correct_catecode1] = 1.0
                    else:
                        lv3_correct_by_correct_lv1[correct_catecode1] = \
                            lv3_correct_by_correct_lv1[correct_catecode1] + 1.0

                if str(result.get_lv4_catecode()) == correct_catecode4:
                    lv4_correct_count = lv4_correct_count + 1.0
                    if correct_catecode1 not in lv4_correct_by_correct_lv1:
                        lv4_correct_by_correct_lv1[correct_catecode1] = 1.0
                    else:
                        lv4_correct_by_correct_lv1[correct_catecode1] = \
                            lv4_correct_by_correct_lv1[correct_catecode1] + 1.0

                if str(result.get_lv5_catecode()) == correct_catecode5:
                    lv5_correct_count = lv5_correct_count + 1.0
                    if correct_catecode1 not in lv5_correct_by_correct_lv1:
                        lv5_correct_by_correct_lv1[correct_catecode1] = 1.0
                    else:
                        lv5_correct_by_correct_lv1[correct_catecode1] = \
                            lv5_correct_by_correct_lv1[correct_catecode1] + 1.0

                if str(result.get_lv6_catecode()) == correct_catecode6:
                    lv6_correct_count = lv6_correct_count + 1.0
                    if correct_catecode1 not in lv6_correct_by_correct_lv1:
                        lv6_correct_by_correct_lv1[correct_catecode1] = 1.0
                    else:
                        lv6_correct_by_correct_lv1[correct_catecode1] = \
                            lv6_correct_by_correct_lv1[correct_catecode1] + 1.0

    ordered_total_by_correct_lv1 = OrderedDict(reversed(sorted(total_by_correct_lv1.items(), key=lambda x: x[1])))
    logger.info("\tCATEGORY\tSIZE\tLV1\tLV2\tLV3\tLV4\tLV5\tLV6")
    logger.info("\tROOT\t%d\t%f\t%f\t%f\t%f\t%f\t%f" %
                (int(total_count),
                 round(float(lv1_correct_count / total_count), 4),
                 round(float(lv2_correct_count / total_count), 4),
                 round(float(lv3_correct_count / total_count), 4),
                 round(float(lv4_correct_count / total_count), 4),
                 round(float(lv5_correct_count / total_count), 4),
                 round(float(lv6_correct_count / total_count), 4)))

    for cate1 in ordered_total_by_correct_lv1:
        logger.info("\t%s\t%d\t\t%f\t%f\t%f\t%f\t%f" %
                    (const.CATE_INT_NAMES[cate1],
                     int(total_by_correct_lv1[cate1]),
                     round(float(lv2_correct_by_correct_lv1.get(cate1, 0) / total_by_correct_lv1[cate1]), 4),
                     round(float(lv3_correct_by_correct_lv1.get(cate1, 0) / total_by_correct_lv1[cate1]), 4),
                     round(float(lv4_correct_by_correct_lv1.get(cate1, 0) / total_by_correct_lv1[cate1]), 4),
                     round(float(lv5_correct_by_correct_lv1.get(cate1, 0) / total_by_correct_lv1[cate1]), 4),
                     round(float(lv6_correct_by_correct_lv1.get(cate1, 0) / total_by_correct_lv1[cate1]), 4)))
    logger.info("evaluate completed!")


if __name__ == '__main__':
    if len(sys.argv) < 4:
        raise Exception("Should run this script as: Python " + __file__
                        + " [audit/evaluate] [INPUT_AUDIT_FILE] [OUTPUT_AUDIT_FILE] [MODEL_VERSION]")

    if len(sys.argv) < 5:
        load_models()
    else:
        load_models(sys.argv[4])

    if "audit" == sys.argv[1]:
        audit(const.get_audit_file_path() + sys.argv[2], const.get_audit_file_path() + sys.argv[3])
    elif "audit_seller" == sys.argv[1]:
        audit_seller(const.get_audit_file_path() + sys.argv[2], const.get_audit_file_path() + sys.argv[3])
    elif "evaluate" == sys.argv[1]:
        evaluate(const.get_audit_file_path() + sys.argv[2], const.get_audit_file_path() + sys.argv[3])
    else:
        logger.warning("No proper argument input. Use [audit/evaluate]")
