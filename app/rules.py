from common import const
import re
import logging

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
sports_brands = open(const.get_sports_brand_file_name()).read().split()


def manual_rule_lv1(predicted_cate1, text_norm):

    return_cate1 = predicted_cate1

    if len(text_norm) == 0:
        return return_cate1

    tokens = re.split(const.TEXT_NORM_DELIMITER, text_norm.lower().strip())

    # Make sure these important keywords also exist in the dictionary for tokenizing.

    if predicted_cate1 == const.CATE_INT_SPORTS:

        return_cate1 = fashion_keyword_rule(return_cate1, tokens)

    elif predicted_cate1 == const.CATE_INT_BABY:

        return_cate1 = baby_to_sports_keyword_rule(return_cate1, tokens)

    return return_cate1


def sports_brand_rule(predicted_cate1, text_list):

    return_cate1 = predicted_cate1

    intersection = list(set(sports_brands) & set(text_list))

    if len(intersection) > 0:
        logger.debug("----------")
        logger.debug(text_list)
        logger.debug(intersection)
        logger.debug("----------")
        return_cate1 = const.CATE_INT_SPORTS

    return return_cate1


def fashion_keyword_rule(predicted_cate1, text_list):

    return_cate1 = predicted_cate1

    only_fashion_keywords = ['스니커즈', '하이탑', '슬립온', '단화', '슈퍼스타', '가젤', '스탠스미스' '컨버스', '코트로얄']
    intersection = list(set(only_fashion_keywords) & set(text_list))

    if len(intersection) > 0:
        logger.debug("----------")
        logger.debug(text_list)
        logger.debug(intersection)
        logger.debug("----------")
        return_cate1 = const.CATE_INT_FASHION
    else:
        if '힐리스' in text_list and '아동' in text_list:
            return_cate1 = const.CATE_INT_FASHION

    return return_cate1


def baby_to_sports_keyword_rule(predicted_cate1, text_list):

    return_cate1 = predicted_cate1

    if (('프로팀' in text_list and '유니폼' in text_list) or
            ('팀' in text_list and '유니폼' in text_list) or
            ('팀' in text_list and '조끼' in text_list) or
            ('팀조끼' in text_list) or
            ('야구복' in text_list) or
            ('축구복' in text_list) or
            ('팀유니폼' in text_list) or
            ('수경' in text_list) or
            ('물안경' in text_list) or
            ('수경끈' in text_list) or
            ('수영모' in text_list) or
            ('스포츠' in text_list)):

        return_cate1 = const.CATE_INT_SPORTS

    return return_cate1


if __name__ == '__main__':
    pass
