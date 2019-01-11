"""
This module contains functions that returns normalized feature output
by garbage keyword elimination and tokenizing keywords for given product name text input.
"""

__author__ = "Gil"
__status__ = "production"
__version__ = "0.1"
__date__ = "April 2018"

from common import const
import re
from aca import Automaton
from collections import OrderedDict
import random
import logging

FORMAT = const.LOG_MSG_FORMAT
logging.basicConfig(format=FORMAT, datefmt=const.LOG_DATE_FORMAT)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Dictionary(object):
    automaton = Automaton()
    with open(const.get_token_dictionary_file_name()) as f:
        token_dict = f.read().split()

    token_dictionary = [x.strip() for x in token_dict]
    automaton.add_all(token_dictionary)

    with open(const.get_garbage_dictionary_file_name()) as f:
        garbage_dict = f.readlines()

    garbage_dictionary = [x.strip() for x in garbage_dict]
    garbage_dictionary.sort(key=lambda item: (-len(item), item))


def normalize_text(input_str, token=True, number_filter=False, model_no_filter=False, date_filter=False):

    input_garbage_filtered = eliminate_garbage_text(str(input_str), number_filter, model_no_filter, date_filter)

    input_tokenize = tokenize_text(input_garbage_filtered, token)

    return input_tokenize


def normalize_text_without_garbage_filter(input_str, token=True):

    input_tokenize = tokenize_text(input_str, token)

    return input_tokenize


def eliminate_garbage_text(input_str, number_filter=False, model_no_filter=False, date_filter=False):
    input_formatted = input_str.lower().replace('\"', '').replace("\t", " ")

    garbage_escaped = map(re.escape, Dictionary.garbage_dictionary)
    input_filtered = re.sub(r"\b%s\b" % '\\b|\\b'.join(garbage_escaped), ' ', input_formatted)

    if number_filter:
        input_filtered = re.sub(r"\b(kor|KOR)?[0-9]{5,99}\b", '', input_filtered)

    if model_no_filter:
        input_filtered = re.sub(r"\b((?=[A-Za-z/-_]{0,19}\d)[A-Za-z0-9/-_]{3,20})\b", '', input_filtered)

    if date_filter:
        input_filtered = re.sub(r"\d{4}\s?년\s?\d{1,2}\s?월\s?\d{1,2}\s?일", '', input_filtered)

    input_filtered = re.sub(r"(류)\b(?<!의류|석류)", '', input_filtered)

    return input_filtered


def tokenize_text(input_str, token):
    tokens = re.split(const.DELIMITER_KEYWORD, input_str.strip())
    tokens = list(filter(lambda x: x != '', tokens))

    tokens = list(OrderedDict((x, True) for x in tokens).keys())

    token_str = ''
    if token:
        for t in tokens:
            dict_tokens = tokenize(t)
            for dt in dict_tokens:
                token_str = token_str + dt + const.TEXT_NORM_DELIMITER
    else:
        token_str = const.TEXT_NORM_DELIMITER.join(tokens)

    tokens = re.split(const.TEXT_NORM_DELIMITER, token_str.lower().strip())
    tokens = list(filter(lambda x: x != '', tokens))

    return const.TEXT_NORM_DELIMITER.join(tokens)


def tokenize(input_text):
    final_result = []
    split_text = re.split(' ', input_text.strip().replace(const.TEXT_NORM_DELIMITER, ''))
    # split_text = re.split(const.DELIMITER_LOCALE, input_text.strip().replace(const.TEXT_NORM_DELIMITER, ''))
    split_text = list(filter(lambda x: x != '', split_text))

    for text in split_text:
        text_len = len(text)

        indices = []

        start = -1
        end = -1
        for match in Dictionary.automaton.get_matches(text):

            if start == -1 and match.start != 0:
                indices.append((0, match.start))
                indices.append((match.start, match.end))
            else:
                if end != -1 and end != match.start:
                    indices.append((end, match.start))
                indices.append((match.start, match.end))

            start = match.start
            end = match.end

        if end == -1:
            indices.append((0, text_len))
        elif end < text_len:
            indices.append((end, text_len))

        result = []
        for (start, end) in indices:
            result.append(text[start: start + (end - start)])

        final_result.extend(result)

    return final_result


def dropout(input_text_with_label_in_first):

    tokens = re.split(const.TEXT_NORM_DELIMITER, input_text_with_label_in_first.lower().strip())
    token_label = tokens[0]
    token_list = tokens[1:]

    drops = int(len(token_list) * 0.2)
    keeps = len(token_list) - drops
    dropped_token_list = set(random.sample(token_list, keeps))
    dropped_token_list = [i for i in token_list if i in dropped_token_list]

    return token_label + const.TEXT_NORM_DELIMITER + const.TEXT_NORM_DELIMITER.join(dropped_token_list)


if __name__ == '__main__':
    pass
