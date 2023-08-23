import os

from elasticsearch import Elasticsearch
import chardet
import textract
import re
import my_text

es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/home/uka/PycharmProjects/elasticsearch-8.9.0/config/certs/http_ca.crt",
    basic_auth=("elastic", "8EI8epgnm-PDLcja0nJ4")
)

Hash = ''


def gettext(link):
    link = f'/var/data/DOC_ELASTIC/{link}'
    print(link)

    text = textract.process(link)
    coding = chardet.detect(text)
    coding = coding['encoding']
    print(coding)
    text = text.decode(coding)
    text_2 = re.sub('^\s+|\n|\r|\t|\"|\b|\f|\s+$', ' ', text)
    text_2 = text_2.replace("\\", " ")
    text_2 = text_2.replace("/", " ")
    text3 = re.sub(" +", " ", text_2)

    return text3


def line_text(path):
    try:
        text = gettext(path)

        text_len = int(len(text) / 2)
        text_keyword = text[text_len:(text_len + 20)].replace(" ", "")
        resp = es.search(index="doc_index", query={"term": {"check": {"value": text_keyword}}})
        Hash = resp["hits"]["total"]['value']  # или 0 или 1)
    except:
        os.remove(f'/var/data/DOC_ELASTIC/{path}')
        return
    doc = {
        'id_user': 67838716,
        'doc_text': text,
        'check': text_keyword,
        'path_to_file': path
    }
    if text and Hash == 0:
        es.index(index="doc_index", document=doc)
    else:
        print(f"{path} уже есть! дубль")
        os.remove(f'/var/data/DOC_ELASTIC/{path}')
        return


array_text = my_text.text
for i in array_text:
    line_text(i)
