from stevetest_common import const
import fasttext
# from pyfasttext import FastText
import os
import logging

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_model_lv1(model_file_path):
    model_lv1 = None
    model_lv1_loaded = False
    for filename in os.listdir(model_file_path):
        if filename.endswith(const.MODEL_POSTFIX) and filename.startswith(const.MODEL_LV1_PREFIX):
            # model_lv1 = FastText()
            # model_lv1.load_model(os.path.join(model_file_path, filename))
            model_lv1 = fasttext.load_model(os.path.join(model_file_path, filename))
            logger.info("Loaded LEVEL1 model: " + filename)
            model_lv1_loaded = True
            break

    if model_lv1_loaded is False:
        raise Exception("Category prediction LEVEL1 model NOT loaded!")

    return model_lv1


def load_model_sub(model_file_path, level):
    models = dict()
    model_loaded = 0
    for filename in os.listdir(model_file_path):
        if filename.endswith(const.MODEL_POSTFIX) and filename.startswith(const.MODEL_PREFIX + str(level) + '_'):

            cate_code = filename.split('_')[3]

            if cate_code in models:
                raise ValueError("Duplicate model for:" + str(cate_code))
            logger.info("Loading " + filename)
            # model_sub = FastText()
            # model_sub.load_model(os.path.join(model_file_path, filename))
            # models[cate_code] = model_sub
            models[cate_code] = fasttext.load_model(os.path.join(model_file_path, filename))
            model_loaded = model_loaded + 1
            logger.info("Loaded LEVEL" + str(level) + " model: " + filename)

    return models


if __name__ == '__main__':
    pass
