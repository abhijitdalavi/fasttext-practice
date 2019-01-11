"""
This module is for predicting the input text to its most relevant category id.
"""

from common import feature, const, dto, rules_new
import copy
import logging

__author__ = "Gil"
__status__ = "production"
__version__ = "0.1"
__date__ = "April 2018"

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def predict(model_lv1, model_lv2s, model_lv3s, input_str, top=1, product_id=None, item_id=None, garbage_filter=True):

    original_string = input_str.replace('\n', '').replace('\t', '')
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
        prediction_lv1 = model_lv1.predict_proba([pred.get_normalized_input_string()], k=top)

        for i in range(0, len(prediction_lv1[0])):
            pred_lv1 = copy.deepcopy(pred)

            try:
                pred_lv1.set_ml_lv1_catecode(prediction_lv1[0][i][0].replace(const.MODEL_LABEL_KEY, ''))
                pred_lv1.set_ml_lv1_score(prediction_lv1[0][i][1])

            except KeyError:
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

        classifier_lv2 = model_lv2s[pred_lv1s[i].get_lv1_catecode()]
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

        classifier_lv3 = model_lv3s[pred_lv2s[i].get_lv2_catecode()]
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

    return pred_lv3s[:top]


if __name__ == '__main__':
    pass
