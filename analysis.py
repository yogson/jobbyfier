import sys
import re

import pandas as pd
from nltk import word_tokenize, FreqDist
import string
from pymongo import MongoClient

RUSSIAN_ALPHABET = 'АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя'

client = MongoClient()
db = client.jobs


def clean_html(raw_html):
    cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    cleantext = re.sub(cleanr, ' ', raw_html)
    return cleantext


def get_collection_full_text(collection_name):
    collection = db.get_collection(collection_name)
    qs = collection.find()
    full_text = ''
    for vacancy in qs:
        full_text += '\n'+clean_html(vacancy.get('description'))
    return full_text


def get_top_words(text):

    def trans(chars):
        return str.maketrans(dict(zip(chars, list(' ' * len(chars)))))

    trans_tab = trans(list(string.punctuation) + list('\r\n«»\–') + list(RUSSIAN_ALPHABET) + list(string.digits))

    df = pd.DataFrame({
        'comm': re.split(r'[\n\r\.\?!]', text)
    })
    df['comm'] = df['comm'].str.translate(trans_tab).str.lower()

    words = [word for word in word_tokenize(df['comm'].str.cat(sep=' '))]

    return FreqDist(words)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        collection = 'vacancies_'+sys.argv[1]
        top = get_top_words(get_collection_full_text('collection'))
        for word, count in top.most_common(50):
            print(f'{word}: \t{count}')
