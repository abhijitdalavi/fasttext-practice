from stevetest_common import const
import re


def manual_rule_lv1(input_str):
    if match_book_isbn(input_str) >= 1:
        return const.CATE_INT_BOOK

    if match_book_keyword(input_str) >= 1 and match_book_quantity(input_str) >= 1:
        return const.CATE_INT_BOOK

    if match_book_keyword(input_str) >= 2:
        return const.CATE_INT_BOOK

    if match_book_definite_keyword(input_str) >= 1:
        return const.CATE_INT_BOOK

    return None


class Rules(object):
    with open(const.get_sports_brand_file_name()) as f:
        sports_brand_dictionary = f.readlines()

    sports_brand_dictionary = [x.strip() for x in sports_brand_dictionary]
    sports_brand_dictionary.sort(key=lambda item: (-len(item), item))

    with open(const.get_sports_definite_keyword_file_name()) as f:
        sports_definite_keyword_dictionary = f.readlines()

    sports_definite_keyword_dictionary = [x.strip() for x in sports_definite_keyword_dictionary]
    sports_definite_keyword_dictionary.sort(key=lambda item: (-len(item), item))

    with open(const.get_book_keyword_file_name()) as f:
        book_keyword_dictionary = f.readlines()

    book_keyword_dictionary = [x.strip() for x in book_keyword_dictionary]
    book_keyword_dictionary.sort(key=lambda item: (-len(item), item))

    with open(const.get_book_definite_keyword_file_name()) as f:
        book_definite_keyword_dictionary = f.readlines()

    book_definite_keyword_dictionary = [x.strip() for x in book_definite_keyword_dictionary]
    book_definite_keyword_dictionary.sort(key=lambda item: (-len(item), item))

    with open(const.get_fashion_definite_keyword_file_name()) as f:
        fashion_definite_keyword_dictionary = f.readlines()

    fashion_definite_keyword_dictionary = [x.strip() for x in fashion_definite_keyword_dictionary]
    fashion_definite_keyword_dictionary.sort(key=lambda item: (-len(item), item))

    with open(const.get_fashion_keyword_file_name()) as f:
        fashion_keyword_dictionary = f.readlines()

    fashion_keyword_dictionary = [x.strip() for x in fashion_keyword_dictionary]
    fashion_keyword_dictionary.sort(key=lambda item: (-len(item), item))


def match_book_isbn(input_str):
    pattern = re.compile(r'(978|979)(?:-?\d){10}\b')
    results = pattern.findall(input_str)
    results = list(set(results))

    return len(results)


def match_book_keyword(input_str):
    results = re.findall(r"\b%s\b" % '\\b|\\b'.join(Rules.book_keyword_dictionary), input_str)
    results = list(set(results))

    return len(results)


def match_book_definite_keyword(input_str):
    results = re.findall(r"\b%s\b" % '\\b|\\b'.join(Rules.book_definite_keyword_dictionary), input_str)
    results = list(set(results))

    return len(results)


def match_book_quantity(input_str):
    pattern = re.compile(r'\d{1,3}[ ]*권')
    results = pattern.findall(input_str)
    results = list(set(results))

    return len(results)


def match_sports_brand(input_str):
    results = re.findall(r"\b%s\b" % '\\b|\\b'.join(Rules.sports_brand_dictionary), input_str)
    results = list(set(results))

    return len(results)


def match_sports_definite_keyword(input_str):
    results = re.findall(r"\b%s\b" % '\\b|\\b'.join(Rules.sports_definite_keyword_dictionary), input_str)
    results = list(set(results))

    return len(results)


def match_fashion_definite_keyword(input_str):
    results = re.findall(r"\b%s\b" % '\\b|\\b'.join(Rules.fashion_definite_keyword_dictionary), input_str)
    results = list(set(results))

    return len(results)


def match_fashion_keyword(input_str):
    results = re.findall(r"\b%s\b" % '\\b|\\b'.join(Rules.fashion_keyword_dictionary), input_str)
    results = list(set(results))

    return len(results)


if __name__ == '__main__':
    pass
