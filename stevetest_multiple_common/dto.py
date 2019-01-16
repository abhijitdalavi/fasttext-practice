"""
This module defines common data value object with getters and setters
that could be used throughout the layers.

"""

__author__ = "Steve"
__status__ = "production"
__version__ = "0.1"
__date__ = "Jan 2019"

from stevetest_multiple_common import const
import decimal


class Prediction(object):
    product_id = None
    item_id = None
    input_string = None
    normalized_input_string = None
    predict_type = None

    ml_lv1_catecode = None
    ml_lv2_catecode = None
    ml_lv3_catecode_chunk = None

    ml_lv1_score = 0.0
    ml_lv2_score = 0.0
    ml_lv3_score = 0.0

    rule_lv1_catecode = None
    rule_lv2_catecode = None
    rule_lv3_catecode_chunk = None

    rule_lv1_score = 0.0
    rule_lv2_score = 0.0
    rule_lv3_score = 0.0

    predict_error = False

    def __init__(self, input_string, product_id=None, item_id=None):

        self.input_string = input_string
        self.product_id = product_id
        self.item_id = item_id
        self.predict_type = const.PREDICT_TYPE_ML

    def set_predict_error(self, error):
        self.predict_error = error

    def get_predict_error(self):
        return self.predict_error

    def get_input_string(self):
        return self.input_string

    def set_normalized_input_string(self, string):
        self.normalized_input_string = string

    def get_normalized_input_string(self):
        return self.normalized_input_string

    def set_predict_type(self, predict_type):
        self.predict_type = predict_type

    def get_predict_type(self):
        return self.predict_type

    def get_lv1_catecode(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv1_catecode()
        else:
            return self.get_rule_lv1_catecode()

    def get_lv2_catecode(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv2_catecode()
        else:
            return self.get_rule_lv2_catecode()

    def get_lv3_catecode(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv3_catecode()
        else:
            return self.get_rule_lv3_catecode()

    def get_lv4_catecode(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv4_catecode()
        else:
            return self.get_rule_lv4_catecode()

    def get_lv5_catecode(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv5_catecode()
        else:
            return self.get_rule_lv5_catecode()

    def get_lv6_catecode(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv6_catecode()
        else:
            return self.get_rule_lv6_catecode()

    def get_catecode(self):
        catecode6 = self.get_lv6_catecode()
        catecode5 = self.get_lv5_catecode()
        catecode4 = self.get_lv4_catecode()
        catecode3 = self.get_lv3_catecode()

        if catecode6 is not None and catecode6 != '' and catecode6 != '0':
            return catecode6
        elif catecode5 is not None and catecode5 != '' and catecode5 != '0':
            return catecode5
        elif catecode4 is not None and catecode4 != '' and catecode4 != '0':
            return catecode4
        else:
            return catecode3

    def get_lv1_score(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv1_score()
        else:
            return self.get_rule_lv1_score()

    def get_lv2_score(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv2_score()
        else:
            return self.get_rule_lv2_score()

    def get_lv3_score(self):
        if self.predict_type == const.PREDICT_TYPE_ML:
            return self.get_ml_lv3_score()
        else:
            return self.get_rule_lv3_score()

    def get_final_score(self):

        try:
            if self.predict_type == const.PREDICT_TYPE_ML:
                return decimal.Decimal(
                    self.get_ml_lv1_score() *
                    self.get_ml_lv2_score() *
                    self.get_ml_lv3_score()).quantize(decimal.Decimal('.0001'), rounding=decimal.ROUND_HALF_UP)

            else:
                return self.get_rule_lv3_score()
        except TypeError:
            return 0.0

    def set_ml_lv1_catecode(self, catecode):
        self.ml_lv1_catecode = catecode

    def get_ml_lv1_catecode(self):
        return self.ml_lv1_catecode

    def set_ml_lv2_catecode(self, catecode):
        self.ml_lv2_catecode = catecode

    def get_ml_lv2_catecode(self):
        return self.ml_lv2_catecode

    def set_ml_lv3_catecode_chunk(self, chunk):
        self.ml_lv3_catecode_chunk = chunk

    def get_ml_lv3_catecode(self):
        if not self.ml_lv3_catecode_chunk:
            return None

        catecodes = self.ml_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        return catecodes[2]

    def get_ml_lv4_catecode(self):
        if not self.ml_lv3_catecode_chunk:
            return ''

        catecodes = self.ml_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        if len(catecodes[3]) == 0:
            return catecodes[2]
        else:
            return catecodes[3]

    def get_ml_lv5_catecode(self):
        if not self.ml_lv3_catecode_chunk:
            return ''

        catecodes = self.ml_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        if len(catecodes[4]) == 0:
            if len(catecodes[3]) == 0:
                return catecodes[2]
            else:
                return catecodes[3]
        else:
            return catecodes[4]

    def get_ml_lv6_catecode(self):
        if not self.ml_lv3_catecode_chunk:
            return ''

        catecodes = self.ml_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        if len(catecodes[5]) == 0:
            if len(catecodes[4]) == 0:
                if len(catecodes[3]) == 0:
                    return catecodes[2]
                else:
                    return catecodes[3]
            else:
                return catecodes[4]
        else:
            return catecodes[5]

    def set_ml_lv1_score(self, score):
        self.ml_lv1_score = decimal.Decimal(score).quantize(decimal.Decimal('.0001'), rounding=decimal.ROUND_HALF_UP)

    def get_ml_lv1_score(self):
        return self.ml_lv1_score

    def set_ml_lv2_score(self, score):
        self.ml_lv2_score = decimal.Decimal(score).quantize(decimal.Decimal('.0001'), rounding=decimal.ROUND_HALF_UP)

    def get_ml_lv2_score(self):
        return self.ml_lv2_score

    def set_ml_lv3_score(self, score):
        self.ml_lv3_score = decimal.Decimal(score).quantize(decimal.Decimal('.0001'), rounding=decimal.ROUND_HALF_UP)

    def get_ml_lv3_score(self):
        return self.ml_lv3_score

    def set_rule_lv1_catecode(self, catecode):
        self.rule_lv1_catecode = catecode

    def get_rule_lv1_catecode(self):
        return self.rule_lv1_catecode

    def set_rule_lv2_catecode(self, catecode):
        self.rule_lv2_catecode = catecode

    def get_rule_lv2_catecode(self):
        return self.rule_lv2_catecode

    def set_rule_lv3_catecode_chunk(self, chunk):
        self.rule_lv3_catecode_chunk = chunk

    def get_rule_lv3_catecode(self):
        if not self.rule_lv3_catecode_chunk:
            return None

        catecodes = self.rule_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        return catecodes[2]

    def get_rule_lv4_catecode(self):
        if not self.rule_lv3_catecode_chunk:
            return ''

        catecodes = self.rule_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        if len(catecodes[3]) == 0:
            return catecodes[2]
        else:
            return catecodes[3]

    def get_rule_lv5_catecode(self):
        if not self.rule_lv3_catecode_chunk:
            return ''

        catecodes = self.rule_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        if len(catecodes[4]) == 0:
            if len(catecodes[3]) == 0:
                return catecodes[2]
            else:
                return catecodes[3]
        else:
            return catecodes[4]

    def get_rule_lv6_catecode(self):
        if not self.rule_lv3_catecode_chunk:
            return ''

        catecodes = self.rule_lv3_catecode_chunk.split('_')
        if len(catecodes) != 6:
            return None

        if len(catecodes[5]) == 0:
            if len(catecodes[4]) == 0:
                if len(catecodes[3]) == 0:
                    return catecodes[2]
                else:
                    return catecodes[3]
            else:
                return catecodes[4]
        else:
            return catecodes[5]

    def set_rule_lv1_score(self, score):
        self.rule_lv1_score = score

    def get_rule_lv1_score(self):
        return self.rule_lv1_score

    def set_rule_lv2_score(self, score):
        self.rule_lv2_score = score

    def get_rule_lv2_score(self):
        return self.rule_lv2_score

    def set_rule_lv3_score(self, score):
        self.rule_lv3_score = score

    def get_rule_lv3_score(self):
        return self.rule_lv3_score

    def __str__(self,print_all=True):
        if print_all:
            return " ".join(str(item) for item in (
                self.product_id,self.item_id,self.predict_type,self.ml_lv1_catecode,str(self.ml_lv1_score),self.rule_lv1_catecode,
                str(self.rule_lv1_score),self.ml_lv2_catecode,str(self.ml_lv2_score),self.rule_lv2_catecode,str(self.rule_lv2_score),
                self.ml_lv3_catecode_chunk,str(self.ml_lv3_score),self.rule_lv3_catecode_chunk,str(self.rule_lv3_score)))
        else:
            return self.item_id
