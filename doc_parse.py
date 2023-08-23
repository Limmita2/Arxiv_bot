import chardet
import textract
import re

# path = "/var/data/DOC_ELASTIC"


def gettext(link):
    link = f'/var/data/DOC_ELASTIC/{link}'
    try:
        text = textract.process(link)
        coding = chardet.detect(text)
        coding = coding['encoding']
        text = text.decode(coding)

        text_2 = re.sub('^\s+|\n|\r|\t|\"|\b|\f|\s+$', ' ', text)
        text_2 = text_2.replace("\\", " ")
        text_2 = text_2.replace("/", " ")
        text3 = re.sub(" +", " ", text_2)

        return text3
    except:
        return False
