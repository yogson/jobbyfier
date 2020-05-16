import sys
import re
import time
import multiprocessing

import pandas as pd
from nltk import word_tokenize, FreqDist
from nltk.corpus import stopwords
import itertools
import string
from pymongo import MongoClient

from config import STOP_LIST, CORES_TO_USE

RUSSIAN_ALPHABET = 'АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя'

client = MongoClient(['172.19.33.120'])
db = client.jobs


def clean_html(raw_html):
    cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    cleantext = re.sub(cleanr, ' ', raw_html)
    return cleantext


def get_collection_full_text(collection_name=None, key='description', parts=None):

    collection = db.get_collection(collection_name)
    qs = collection.find({}, {key: 1})
    full_text = ''

    if parts:
        queryset_len = collection.count_documents({})
        part_size = queryset_len//parts
        parts_list = []

        for i in range(0, parts-1):

            if i == parts-1:
                full_text = ''
                for vacancy in qs[i*part_size: ]:
                    full_text += '\n'+clean_html(vacancy.get(key)) if vacancy.get(key) else ''
                parts_list.append(full_text)
            else:
                full_text = ''
                for vacancy in qs[i*part_size: (i+1)*part_size]:
                    full_text += '\n'+clean_html(vacancy.get(key)) if vacancy.get(key) else ''
                parts_list.append(full_text)
                qs.rewind()

        return parts_list

    else:

        for vacancy in qs:
            full_text += '\n'+clean_html(vacancy.get(key)) if vacancy.get(key) else ''

        return [full_text]


def tokenize(frame, return_list: list):
    return_list.append(word_tokenize(frame))


def multi_tokenizer(df):

    df_part = len(df)//8

    manager = multiprocessing.Manager()
    return_list = manager.list()
    jobs = []
    for i in range(0, 7):
        if i == 7:
            part = df[i*df_part:]
        else:
            part = df[i*df_part: (i+1)*df_part]
        proc = multiprocessing.Process(target=tokenize, args=(part, return_list))
        jobs.append(proc)
        proc.start()

    for proc in jobs:
        proc.join()

    return itertools.chain.from_iterable(return_list)


def get_top_words(text=None, result_list=None, eng_only=None):

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

    tokenized = word_tokenize(df['comm'].str.cat(sep=' '))

    words = [word for word in tokenized]

    stop_words = list(itertools.chain(
        stopwords.words("english"),
        stopwords.words("russian"),
        STOP_LIST
    ))

    result_list.append([word for word in words if word not in stop_words])


def get_expirience(text):
    pass


if __name__ == '__main__':
    if len(sys.argv) > 1:
        start_time = time.time()

        collection = 'vacancies_'+sys.argv[1]
        text_list = get_collection_full_text(collection, key='description', parts=CORES_TO_USE)

        if len(sys.argv) == 3 and sys.argv[2] == 'eng':
            eng_only = True
        else:
            eng_only = False

        manager = multiprocessing.Manager()
        result_list = manager.list()
        jobs = []
        for text in text_list:
            proc = multiprocessing.Process(
                target=get_top_words,
                kwargs={'text': text, 'result_list': result_list, 'eng_only': eng_only}
            )
            jobs.append(proc)
            proc.start()

        for proc in jobs:
            proc.join()

        top = FreqDist(itertools.chain.from_iterable(result_list))

        for word, count in top.most_common(100):
            print(f'{word}: \t{count}')
        print('Total time:', time.time() - start_time)
