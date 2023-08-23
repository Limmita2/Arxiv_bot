import os
import urllib
import random
import doc_parse
import sys
import conf

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from aiogram.types import InlineKeyboardButton
from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/home/uka/PycharmProjects/elasticsearch-8.9.0/config/certs/http_ca.crt",
    basic_auth=("elastic", "8EI8epgnm-PDLcja0nJ4")
)
TOKEN = conf.TOKEN
bot = Bot(TOKEN)
dp = Dispatcher(bot)
path_f = '/var/data/DOC_ELASTIC/'
data_staty = []


# Начало работы с ботом
@dp.message_handler(commands=['start'])
async def process_start_command(message: types.Message):
    await message.reply("Привiт!\nТвiй запит направлено адмiну")
    await bot.send_message("67838716", message.chat)


# Текстовый запрос боту
@dp.message_handler()
async def echo_message(msg: types.Message):
    kol = 0
    print(msg.chat.id)       #Для получения информации о ID
    for group in conf.GROUPS:
        if msg.chat.id == group:
            kol = 1
    if kol == 0:
        await msg.reply("Всі запити тільки через групу")
        return
    for dat in data_staty:
        if dat['id_user'] == msg.from_user.id:
            data_staty.remove(dat)
    user_staty = {
        "id_user": msg.from_user.id,
        "text_user": msg.text,
        "staty": 0
    }
    resp = es.search(index="doc_index", query={"match": {"doc_text": msg.text}})
    doc_val = resp['hits']['total']['value']
    # score = resp['max_score']
    # print(score)
    for hit in resp['hits']['hits']:
        builder = types.InlineKeyboardMarkup(row_width=1)
        text_name = "%(path_to_file)s" % hit["_source"]
        id_text = hit['_id']
        score = hit['_score']
        score_roubd = round(score, 1)
        button_file = InlineKeyboardButton('Завантажити', callback_data=id_text)
        builder.add(button_file)
        try:
            await bot.send_message(msg.from_user.id, f'<b>{text_name}</b> ({score_roubd})', reply_markup=builder, parse_mode='HTML')
        except:
            await bot.send_message(msg.from_user.id, f'Привітайся з ботом @archivesukabot. Натистни у нього старт.')
            return
    await bot.send_message(msg.from_user.id, '❌❌❌❌❌❌❌❌❌')
    if doc_val != 0:
        builder_source = types.InlineKeyboardMarkup(row_width=1)
        button_source = InlineKeyboardButton('Завантажити 10 наступних', callback_data="styte")
        builder_source.add(button_source)
        if doc_val - 10 < 1:
            return
        await bot.send_message(msg.from_user.id,
                               text=f'Mаємо ще <b>{doc_val - 10}</b> документiв по запиту : <i>{msg.text}</i>',
                               reply_markup=builder_source, parse_mode='HTML')
        data_staty.append(user_staty)
    await bot.send_message(msg.from_user.id, '❌❌❌❌❌❌❌❌❌')


# Отработка инлайн кнопок (ответ)
@dp.callback_query_handler(lambda c: True)
async def process_callback(callback_query: types.CallbackQuery):
    global text_user, count
    if callback_query.data != "styte":
        data = callback_query.data
        resp = es.get(index="doc_index", id=data)
        path_file = resp['_source']['path_to_file']
        media = types.MediaGroup()
        media.attach_document(document=types.InputFile(f"{path_f}{path_file}"))
        await callback_query.message.answer_media_group(media=media)
        await callback_query.answer()
    else:
        for dat in data_staty:
            if dat['id_user'] == callback_query.from_user.id:
                text_user = dat['text_user']
                count = int(dat['staty']) + 10
                dat['staty'] = count
        resp = es.search(index="doc_index", body={"query": {"match": {"doc_text": text_user}}, "from": count})
        doc_val = resp['hits']['total']['value']
        for hit in resp['hits']['hits']:
            builder = types.InlineKeyboardMarkup(row_width=1)
            text_name = "%(path_to_file)s" % hit["_source"]
            id_text = hit['_id']
            score = hit['_score']
            score_roubd = round(score, 1)
            button_file = InlineKeyboardButton('Завантажити', callback_data=id_text)
            builder.add(button_file)
            await bot.send_message(callback_query.from_user.id, f'<b>{text_name}</b> ({score_roubd})', reply_markup=builder,
                                   parse_mode='HTML')
        await bot.send_message(callback_query.from_user.id, '❌❌❌❌❌❌❌❌❌')

        builder_source = types.InlineKeyboardMarkup(row_width=1)
        button_source = InlineKeyboardButton('‼️Завантажити ще 10 наступних‼️', callback_data="styte")
        builder_source.add(button_source)
        if doc_val - count < 10:
            return
        await bot.send_message(callback_query.from_user.id,
                               text=f'Mаємо <b>{doc_val - count - 10}</b> документiв по запиту : <i>"{text_user}"</i>',
                               reply_markup=builder_source, parse_mode='HTML')
    await bot.send_message(callback_query.from_user.id, '❌❌❌❌❌❌❌❌❌')


# Прием документов и загрузка их на сервер
@dp.message_handler(content_types=['document'])
async def scan_message(msg: types.Message):
    document_id = msg.document.file_id
    file_info = await bot.get_file(document_id)
    fi = file_info.file_path
    name = msg.document.file_name
    name_path = f'{random.randint(1, 99)}{"_"}{name}'
    urllib.request.urlretrieve(f'https://api.telegram.org/file/bot{TOKEN}/{fi}', f'{path_f}{name_path}')
    await bot.send_message(msg.from_user.id, 'Файл відпрацьовується... Чекайте!')
    text = doc_parse.gettext(name_path)
    if not text:
        await bot.send_message(msg.from_user.id, '‼️Файл не принят‼️')
        os.remove(f'{path_f}{name_path}')
        return
    text_len = int(len(text) / 2)
    text_keyword = text[text_len:(text_len + 20)].replace(" ", "")

    # Проверка на наличие (дублировани)
    try:
        resp = es.search(index="doc_index", query={"term": {"check": {"value": text_keyword}}})
        Hash = resp["hits"]["total"]['value']  # или 0 или 1)
    except:
        await bot.send_message(msg.from_user.id, '‼️Збiй роботи серверу!‼️')
        os.remove(f'{path_f}{name_path}')
        return

    doc = {
        'id_user': msg.from_user.id,
        'doc_text': text,
        'check': text_keyword,
        'path_to_file': name_path
    }

    if text and Hash == 0:
        try:
            es.index(index="doc_index", document=doc)
            await bot.send_message(msg.from_user.id, 'Дякую! Файл завантажен!')
        except:
            await bot.send_message(msg.from_user.id, '‼️Збiй роботи серверу!‼️')
            os.remove(f'{path_f}{name_path}')

    elif Hash != 0:
        await bot.send_message(msg.from_user.id, 'Дякую! Такий документ вже iснуе!')
        os.remove(f'{path_f}{name_path}')
        return
    else:
        await bot.send_message(msg.from_user.id, 'Невдача!  Файл відправлено адміну @limmita, він сам все зробить.')
        await bot.send_message("67838716", f'Невдача!  Файл {name}')
        return


if __name__ == '__main__':
    executor.start_polling(dp)