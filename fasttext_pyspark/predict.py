from fasttext_pyspark import feature, const, dto, rules_new
from pyspark import SparkFiles
import fasttext
# from pyfasttext import FastText
import os
import logging
import copy

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def load_model_lv1():
    model_lv1 = None
    model_lv1_loaded = False
    root = os.path.join(SparkFiles.getRootDirectory(), "lv1")
    for filename in os.listdir(root):
        logger.info("filename: " + filename)
        if filename.endswith(const.MODEL_POSTFIX) and filename.startswith(const.MODEL_LV1_PREFIX):
            # model_lv1 = FastText()
            # model_lv1.load_model(os.path.join(model_file_path, filename))
            logger.info("modelfile: " + os.path.join(root, filename))
            model_lv1 = fasttext.load_model(os.path.join(root, filename))
            logger.info("Loaded LEVEL1 model: " + filename)
            model_lv1_loaded = True
            break

    if model_lv1_loaded is False:
        raise Exception("Category prediction LEVEL1 model NOT loaded!")

    return model_lv1


def load_model_sub(sub, level):
    models = dict()
    model_loaded = 0
    root = os.path.join(SparkFiles.getRootDirectory(), sub)
    for filename in os.listdir(root):
        if filename.endswith(const.MODEL_POSTFIX) and filename.startswith(const.MODEL_PREFIX + str(level) + '_'):

            cate_code = filename.split('_')[3]

            if cate_code in models:
                raise ValueError("Duplicate model for:" + str(cate_code))
            logger.info("Loading " + filename)
            # model_sub = FastText()
            # model_sub.load_model(os.path.join(model_file_path, filename))
            # models[cate_code] = model_sub
            models[cate_code] = fasttext.load_model(os.path.join(root, filename))
            model_loaded = model_loaded + 1
            logger.info("Loaded LEVEL" + str(level) + " model: " + filename)

    return models

def predict(normalizedAttr, product_id=None, item_id=None, garbage_filter=True, top=1):

    original_string = normalizedAttr.replace('\n', '').replace('\t', '')
    pred = dto.Prediction(original_string, product_id, item_id)

    # Go through garbage filter regex replace logic only if garbage_filter=true because its expensive
    if garbage_filter:
        pred.set_normalized_input_string(feature.normalize_text(pred.get_input_string(), number_filter=False))
    else:
        pred.set_normalized_input_string(feature.normalize_text_without_garbage_filter(pred.get_input_string()))

    # If for somehow garbage filtered string is left with blank, just use the original input string
    if not pred.get_normalized_input_string() or len(pred.get_normalized_input_string()) == 0:
        pred.set_normalized_input_string(original_string)

    # Prediction for lv1
    pred_lv1s = list()
    manual_result = rules_new.manual_rule_lv1(pred.get_normalized_input_string())
    if manual_result is not None:
        pred_lv1 = copy.deepcopy(pred)
        pred_lv1.set_ml_lv1_catecode(manual_result)
        pred_lv1.set_ml_lv1_score(1)
        pred_lv1s.append(pred_lv1)

    else:
        prediction_lv1 = top_level_model.predict_proba([pred.get_normalized_input_string()], k=top)

        for i in range(0, len(prediction_lv1[0])):
            pred_lv1 = copy.deepcopy(pred)

            try:
                pred_lv1.set_ml_lv1_catecode(prediction_lv1[0][i][0].replace(const.MODEL_LABEL_KEY, ''))
                pred_lv1.set_ml_lv1_score(prediction_lv1[0][i][1])

            except Exception as e:
                logger.info("predict error:"+str(e))
                logger.info(traceback.format_exc())
                pred_lv1.set_predict_error(True)

            pred_lv1s.append(pred_lv1)

    # Prediction for lv2
    pred_lv2s = list()
    for i in range(0, len(pred_lv1s)):
        if pred_lv1s[i].get_ml_lv1_catecode() == const.CATE_INT_BOOK:

            if garbage_filter:
                pred_lv1s[i].set_normalized_input_string(
                    feature.normalize_text(pred_lv1s[i].get_input_string(),
                                           token=False,
                                           number_filter=True,
                                           date_filter=True))
            else:
                pred_lv1s[i].set_normalized_input_string(
                    feature.normalize_text_without_garbage_filter(pred_lv1s[i].get_input_string()))

            if not pred_lv1s[i].get_normalized_input_string() or len(pred_lv1s[i].get_normalized_input_string()) == 0:
                pred_lv1s[i].set_normalized_input_string(original_string)

        classifier_lv2 = second_level_model[pred_lv1s[i].get_lv1_catecode()]
        prediction_lv2 = classifier_lv2.predict_proba([pred_lv1s[i].get_normalized_input_string()], k=top)

        for j in range(0, len(prediction_lv2[0])):
            pred_lv2 = copy.deepcopy(pred_lv1s[i])

            try:
                pred_lv2.set_ml_lv2_catecode(prediction_lv2[0][j][0].replace(const.MODEL_LABEL_KEY, '').split('_')[1])
                pred_lv2.set_ml_lv2_score(prediction_lv2[0][j][1])
            except KeyError:
                pred_lv2.set_predict_error(True)

            pred_lv2s.append(pred_lv2)

    # Prediction for lv3 ~ 6
    pred_lv3s = list()
    for i in range(0, len(pred_lv2s)):

        classifier_lv3 = third_level_mode[pred_lv2s[i].get_lv2_catecode()]
        prediction_lv3 = classifier_lv3.predict_proba([pred_lv2s[i].get_normalized_input_string()], k=top)

        for j in range(0, len(prediction_lv3[0])):
            pred_lv3 = copy.deepcopy(pred_lv2s[i])

            try:
                pred_lv3.set_ml_lv3_catecode_chunk(prediction_lv3[0][j][0].replace(const.MODEL_LABEL_KEY, ''))
                pred_lv3.set_ml_lv3_score(prediction_lv3[0][j][1])
            except KeyError:
                pred_lv3.set_predict_error(True)

            pred_lv3s.append(pred_lv3)

    # Sort predictions for one product in final score descending basis
    pred_lv3s.sort(key=lambda x: x.get_final_score(), reverse=True)
    pred_final = pred_lv3s[:top][0]
    return pred_final.ml_lv1_catecode,str(pred_final.ml_lv1_score)
    #     ,pred_final.rule_lv1_catecode,
    # str(pred_final.rule_lv1_score),pred_final.ml_lv2_catecode,str(pred_final.ml_lv2_score),pred_final.rule_lv2_catecode,\
    # str(pred_final.rule_lv2_score),pred_final.ml_lv3_catecode_chunk,str(pred_final.ml_lv3_score),pred_final.rule_lv3_catecode_chunk,\
    # str(pred_final.rule_lv3_score)


top_level_model = load_model_lv1()
second_level_model = load_model_sub("lv2",2)
third_level_mode = load_model_sub("lv3",3)
