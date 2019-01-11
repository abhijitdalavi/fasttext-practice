"""
This module uses flask to provide python web API that returns category id for given text input that
represent characteristics of a product such as product name, brand, attribute values.
"""

__author__ = "Gil"
__status__ = "production"
__version__ = "0.1"
__date__ = "April 2018"

from common import const, loader, predict
from flask import Flask, request, jsonify, abort
from collections import OrderedDict
import traceback
import json
import logging
import sys

logger = logging.getLogger('werkzeug')
logger.setLevel(logging.INFO)

app = Flask(__name__)

model_lv1 = None
model_lv2s = None
model_lv3s = None


def load_models(model_version=None):

    _model_version = const.FILE_PATH_MODEL_PRODUCTION
    if model_version is not None:
        _model_version = model_version

    logger.info("Loading model version:" + _model_version)

    global model_lv1
    model_lv1 = loader.load_model_lv1(const.get_model_file_path(_model_version)+"lv1/")

    global model_lv2s
    model_lv2s = loader.load_model_sub(const.get_model_file_path(_model_version)+"lv2/", 2)

    global model_lv3s
    model_lv3s = loader.load_model_sub(const.get_model_file_path(_model_version)+"lv3/", 3)


@app.route("/")
def default():
    abort(400)


@app.route("/predict", methods=["POST"])
def prediction():

    root = None
    sub_root = None

    try:

        root = request.get_json(force=True)

        if type(root) is str:
            root = json.loads(root)

        sub_root = get_json(root, const.JSON_KEY_ROOT)
        products = get_json(sub_root, const.JSON_KEY_PRODUCTS)

        # if top k prediction return config is not set from the request, uses value 1 as default
        top = 1
        if const.JSON_KEY_TOP in sub_root:
            top = get_json(sub_root, const.JSON_KEY_TOP)

        # top k return cannot exceed max top default value due to risk of under performance
        if top >= const.MAX_TOP:
            top = const.MAX_TOP

        for p in products:
            # get product id
            product_id = get_json(p, const.JSON_KEY_PRODUCT_ID)

            # get product name and brand and concatenate
            product_name = get_json(p, const.JSON_KEY_PRODUCT_NAME)
            product_brand = ''
            if const.JSON_KEY_PRODUCT_BRAND in p:
                product_brand = get_json(p, const.JSON_KEY_PRODUCT_BRAND)
            text_raw = product_name + ' ' + product_brand

            # get attribute and sort by their attribute key name's alphabetically order
            # and concatenate their values after product name and brand
            product_attributes = ''
            if const.JSON_KEY_PRODUCT_ATTRIBUTES in p:
                product_attributes = get_json(p, const.JSON_KEY_PRODUCT_ATTRIBUTES)
            product_attributes_dict = {k: v for d in product_attributes for k, v in d.items()}
            for att in OrderedDict(sorted(product_attributes_dict.items())).values():
                text_raw = text_raw + ' ' + att

            # concatenate product description at the end if there is any value present
            product_description = ''
            if const.JSON_KEY_PRODUCT_DESCRIPTION in p:
                product_description = get_json(p, const.JSON_KEY_PRODUCT_DESCRIPTION)
            text_raw = text_raw + ' ' + product_description

            # set concatenated value for json output
            p[const.JSON_KEY_TEXT_RAW] = text_raw

            # call predict function given the concatenated text value
            pred = predict.predict(
                model_lv1,
                model_lv2s,
                model_lv3s,
                text_raw,
                top=top,
                product_id=product_id)

            # returns list of k top prediction category for given product
            pred_list = list()
            for i in range(0, len(pred)):
                if pred[i].get_predict_error():
                    raise Exception("There is error in prediction result. Please check your input text.")

                pred_result = dict()

                pred_result[const.JSON_KEY_RANK] = i + 1
                pred_result[const.JSON_KEY_CATE1] = pred[i].get_lv1_catecode()
                pred_result[const.JSON_KEY_SCORE_LV1] = str(pred[i].get_lv1_score())
                pred_result[const.JSON_KEY_CATE2] = pred[i].get_lv2_catecode()
                pred_result[const.JSON_KEY_SCORE_LV2] = str(pred[i].get_lv2_score())
                pred_result[const.JSON_KEY_CATE3] = pred[i].get_lv3_catecode()
                pred_result[const.JSON_KEY_SCORE_LV3] = str(pred[i].get_lv3_score())
                pred_result[const.JSON_KEY_CATE4] = pred[i].get_lv4_catecode()
                pred_result[const.JSON_KEY_CATE5] = pred[i].get_lv5_catecode()
                pred_result[const.JSON_KEY_CATE6] = pred[i].get_lv6_catecode()
                pred_result[const.JSON_KEY_SCORE_FINAL] = str(pred[i].get_final_score())

                pred_list.append(pred_result)

            p[const.JSON_KEY_PREDICT] = pred_list
            p[const.JSON_KEY_TEXT_NORM] = pred[0].get_normalized_input_string()

        sub_root[const.JSON_KEY_PRODUCTS] = products
        sub_root[const.JSON_KEY_TOP] = top
        sub_root[const.JSON_KEY_SUCCESS] = True

    except Exception as e:

        if sub_root is None:
            root = json.loads('{"' + const.JSON_KEY_ROOT + '": {}}')
            sub_root = get_json(root, const.JSON_KEY_ROOT)

        sub_root[const.JSON_KEY_SUCCESS] = False
        sub_root[const.JSON_KEY_ERROR] = str(e)

        traceback.print_tb(e.__traceback__)

    return jsonify(root)


def get_json(root, key):
    if key not in root:
        raise KeyError("Key: '" + key + "' cannot be found in the json!")

    return root[key]


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


@app.errorhandler(Exception)
def uncaught_exception(e):
    logger.error(e)
    traceback.print_tb(e.__traceback__)

    return jsonify('Error:' + str(e))


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def main(model_version=None):
    logger.info("Initializing Flask! Loading category prediction models...")

    load_models(model_version)

    logger.info("Successfully loaded models! Flask listening to request...")
    logger.setLevel(logging.ERROR)
    app.run(debug=False, use_reloader=False, host='0.0.0.0', port=5000)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        main()
    else:
        main(sys.argv[1])
