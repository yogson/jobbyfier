import sys
import re

import pandas as pd
from nltk import word_tokenize, FreqDist
from nltk.corpus import stopwords
import itertools
import string
from pymongo import MongoClient

from config import STOP_LIST

RUSSIAN_ALPHABET = 'АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя'

client = MongoClient(['172.19.33.120'])
db = client.jobs


def clean_html(raw_html):
    cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    cleantext = re.sub(cleanr, ' ', raw_html)
    return cleantext


def get_collection_full_text(collection_name, key='description'):
    collection = db.get_collection(collection_name)
    qs = collection.find({}, {key: 1})
    full_text = ''
    for vacancy in qs:
        full_text += '\n'+clean_html(vacancy.get(key))
    return full_text


def get_top_words(text, eng_only=True):

    def trans(chars):
        return str.maketrans(dict(zip(chars, list(' ' * len(chars)))))

    if eng_only:
        trans_tab = trans(list(string.punctuation) + list('\r\n«»\–') + list(RUSSIAN_ALPHABET) + list(string.digits))
    else:
        trans_tab = trans(list(string.punctuation) + list('\r\n«»\–'))

    df = pd.DataFrame({
        'comm': re.split(r'[\n\r\.\?!]', text)
    })
    df['comm'] = df['comm'].str.translate(trans_tab).str.lower()

    words = [word for word in word_tokenize(df['comm'].str.cat(sep=' '))]
    words = [word for word in words if word not in itertools.chain(
        stopwords.words("english"),
        stopwords.words("russian"),
        STOP_LIST
    )]

    return FreqDist(words)


def get_expirience(text):
    pass



if __name__ == '__main__':
    if len(sys.argv) > 1:
        collection = 'vacancies_'+sys.argv[1]
        if len(sys.argv) == 3 and sys.argv[2] == 'eng':
            top = get_top_words(get_collection_full_text(collection))
        else:
            top = get_top_words(get_collection_full_text(collection, key='description'), eng_only=False)

        for word, count in top.most_common(100):
            print(f'{word}: \t{count}')
