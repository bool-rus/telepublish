import requests, json, os, time

token = os.environ['TEST_BOT_TOKEN']
chat = os.environ['CHANNEL']
api = f'https://api.telegram.org/bot{token}'

def tg(method, data, files=None):
    url = f'{api}/{method}'
    r = requests.post(url, data=data, files=files)
    j = r.json()
    assert j.get('ok'), f'{method}: {j}'
    return j

# 1. Короткий текст
tg('sendMessage', {'chat_id': chat, 'text': 'Привет! Первое объявление.'})
print('1 text short')
time.sleep(2)

# 2. Длинный текст
tg('sendMessage', {'chat_id': chat, 'text': '''Второе объявление с длинным текстом.
Тут может быть важная информация для всех жильцов.
Пожалуйста, ознакомьтесь и примите к сведению.
С уважением, администрация.'''.strip()})
print('2 text long')
time.sleep(2)

# 3. Фото с капшном
with open('test_data/photo.jpg', 'rb') as f:
    tg('sendPhoto', {'chat_id': chat, 'caption': 'Котёнок ищет дом (фото)'},
       {'photo': f})
print('3 photo')
time.sleep(2)

# 4. Документ с капшном
with open('test_data/test.txt', 'rb') as f:
    tg('sendDocument', {'chat_id': chat, 'caption': 'Важный документ'},
       {'document': ('test.txt', f, 'text/plain')})
print('4 doc')
time.sleep(2)

# 5. Альбом из 3 фото с капшном
files = {}
media = []
for i in range(3):
    key = f'p{i}'
    files[key] = ('photo.jpg', open('test_data/photo.jpg', 'rb'), 'image/jpeg')
    item = {'type': 'photo', 'media': f'attach://{key}'}
    if i == 0:
        item['caption'] = 'Альбом с тремя фото'
    media.append(item)
tg('sendMediaGroup', {'chat_id': chat, 'media': json.dumps(media)}, files)
for f in files.values():
    f[1].close()
print('5 album 3 photos')
time.sleep(3)

# 6. Текст + файл реплаем
r = tg('sendMessage', {'chat_id': chat, 'text': 'Пост с файлом в ответ'})
mid = r['result']['message_id']
time.sleep(2)
with open('test_data/test.txt', 'rb') as f:
    tg('sendDocument', {'chat_id': chat, 'reply_to_message_id': str(mid)},
       {'document': ('test.txt', f, 'text/plain')})
print('6 text + file reply')
time.sleep(2)

# 7. Фото + файл реплаем
with open('test_data/photo.jpg', 'rb') as f:
    r = tg('sendPhoto', {'chat_id': chat, 'caption': 'Фото + файл в ответ'},
           {'photo': f})
mid = r['result']['message_id']
time.sleep(2)
with open('test_data/test.txt', 'rb') as f:
    tg('sendDocument', {'chat_id': chat, 'reply_to_message_id': str(mid)},
       {'document': ('test.txt', f, 'text/plain')})
print('7 photo + file reply')
time.sleep(2)

# 8. Ещё одно короткое
tg('sendMessage', {'chat_id': chat, 'text': 'Последнее объявление. Всем спасибо!'})
print('8 text short')
print('Done!')
