
# coding: utf-8

# In[ ]:


import pycrfsuite
import re
from pyspark import SparkFiles



    #brandtagger = None

    #@staticmethod
    #def is_loaded():
    #    return CrfTagger.brandtagger is not None

    #@staticmethod
    #def load_models():
    #    path = SparkFiles.get('crf.model.5Feat_33018Pos_11350Neg')
    #    brandtagger = pycrfsuite.Tagger()
    #    brandtagger.open(path)

def initialBrandTagger():
    brandtagger = pycrfsuite.Tagger()
    print('initialing brandtagger')
    brandtagger.open(
        SparkFiles.get('crf.model.5Feat_33018Pos_11350Neg'))
    return brandtagger

def extract_features(termStr):
    terms = termStr.split()
    list = []
    for i in range(len(terms)):
        word = terms[i]
        string = re.sub(r'\.', '\.', word)  # for regular expression features
        # Common features for all words
        features = {
            'bias': 1.0,
            'word.lower': word.lower(),
            'word.position': i
        }
        # Features for words that are not at the beginning of a document
        if i > 0:
            word1 = terms[i - 1]
            string1 = re.sub(r'\.', '\.', word1)  # for regular expression features
            features.update({
                '-1:word.lower': word1.lower()
            })
        else:
            # Indicate that it is the 'beginning of a document'
            features.update({'BOS': 1.0})

        # Features for words that are not at the end of a document
        if i < len(terms) - 1:
            word1 = terms[i + 1]
            string1 = re.sub(r'\.', '\.', word1)  # for regular expression features
            features.update({
                '+1:word.lower': word1.lower()
            })
        else:
            # Indicate that it is the 'beginning of a document'
            features.update({'EOS': 1.0})  
        list.append(features)
    tag_attr = brandtagger.tag(list)
    brandtagger.set(list)
    prob = brandtagger.probability(tag_attr)
    return tag_attr,prob

brandtagger = initialBrandTagger()  
