import datetime
import string
import threading
import uuid
import os
import random

import ffmpegio
import yadisk
import requests
import queue

from requests.auth import HTTPDigestAuth
from yadisk.exceptions import ParentNotFoundError, PathNotFoundError

# Globals
SERVER_NAME = "Отрадная"
APP_TOKEN = "y0_AgAAAABl3sHIAAvpVwAAAAEGxjnpAABpGseraHRK3bgTIqzeVm3n_DDVVg"
NUMBER_OF_THREADS = 5
URLS = [
    {"resource": "http://admin:123456zxC@192.168.112.50:80/ISAPI/Streaming/channels/1/picture", "name": "Главный_вход"},
    {"resource": "http://admin:123456zxC@192.168.110.51:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_1"},
    {"resource": "http://admin:123456zxC@192.168.110.52:80/ISAPI/Streaming/channels/1/picture", "name": "Разгрузка"},
    {"resource": "http://admin:123456zxC@192.168.110.53:80/ISAPI/Streaming/channels/1/picture", "name": "Кассы_1_2_3"},
    {"resource": "http://admin:123456zxC@192.168.110.54:80/ISAPI/Streaming/channels/1/picture", "name": "Парковка"},
    {"resource": "http://admin:123456zxC@192.168.110.55:80/ISAPI/Streaming/channels/1/picture", "name": "Генератор"},
    {"resource": "http://admin:123456zxC@192.168.110.56:80/ISAPI/Streaming/channels/1/picture",
     "name": "Кабинет_товароведа"},
    {"resource": "http://admin:123456zxC@192.168.110.57:80/ISAPI/Streaming/channels/1/picture",
     "name": "Холодильник_2"},
    {"resource": "http://admin:123456zxC@192.168.110.58:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_2"},
    {"resource": "http://admin:123456zxC@192.168.110.59:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_3"},
    {"resource": "http://admin:123456zxC@192.168.110.60:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_4"},
    {"resource": "http://admin:123456zxC@192.168.110.61:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_5"},
    {"resource": "http://admin:123456zxC@192.168.110.62:80/ISAPI/Streaming/channels/1/picture", "name": "Касса_3"},
    {"resource": "http://admin:123456zxC@192.168.110.63:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_6"},
    {"resource": "http://admin:123456zxC@192.168.110.64:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_7"},
    {"resource": "http://admin:123456zxC@192.168.110.65:80/ISAPI/Streaming/channels/1/picture", "name": "Приемка"},
    {"resource": "http://admin:123456zxC@192.168.110.66:80/ISAPI/Streaming/channels/1/picture", "name": "Сейф"},
    {"resource": "http://admin:123456zxC@192.168.110.67:80/ISAPI/Streaming/channels/1/picture",
     "name": "Холодильник_1"},
    {"resource": "http://admin:123456zxC@192.168.110.68:80/ISAPI/Streaming/channels/1/picture", "name": "Полюшко"},
    {"resource": "http://admin:123456zxC@192.168.110.69:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_9"},
    {"resource": "http://admin:123456zxC@192.168.110.70:80/ISAPI/Streaming/channels/1/picture", "name": "Фасовка"},
    {"resource": "http://admin:123456zxC@192.168.110.71:80/ISAPI/Streaming/channels/1/picture", "name": "Зал_8"},
    {"resource": "http://admin:123456zxC@192.168.110.72:80/ISAPI/Streaming/channels/1/picture", "name": "Касса_2"},
    {"resource": "http://admin:123456zxC@192.168.110.75:80/ISAPI/Streaming/channels/1/picture", "name": "Касса_1"},
]
Q = queue.Queue()
CLIENT = yadisk.Client(token=APP_TOKEN)
CLEAR_OFFSET = 72


def init_folders():
    if not CLIENT.exists(f"/{SERVER_NAME}"):
        CLIENT.mkdir(f"/{SERVER_NAME}")
    else:
        return

    for resource in URLS:
        if not CLIENT.exists(f"/{SERVER_NAME}/{resource["name"]}"):
            CLIENT.mkdir(f"/{SERVER_NAME}/{resource["name"]}")


def clear_folder(path: str, offset: int, perm=True):
    dir_struct = CLIENT.listdir(path)
    for file in dir_struct:
        if (datetime.datetime.now(datetime.timezone.utc) - file.created) > datetime.timedelta(days=offset):
            CLIENT.remove(file.path, permanently=perm)


def salt(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def send_to_cloud(source, destination):
    with CLIENT:
        if CLIENT.check_token():
            CLIENT.upload(source, destination)
            os.remove(source)


def compress_image(input_image_path, output_image_path, quality_scale=2):
    ffmpegio.transcode(input_image_path, output_image_path, **{"q:v": quality_scale})


def load_shot(source) -> str:
    response = requests.get(source,
                            auth=HTTPDigestAuth('admin', '123456zxC'),
                            verify=False,
                            stream=True,
                            timeout=5)

    if response.status_code != 200:
        raise requests.HTTPError

    bdata = response.content

    path = os.path.expandvars("${TEMP}\\" + f"{str(uuid.uuid4())}.jpg")

    with open(path, 'wb') as file:
        file.write(bdata)

    return path


def worker():
    while True:
        if Q.empty():
            break

        cam_info = Q.get(block=False)
        url = cam_info["resource"]
        channel_folder = cam_info["name"]

        try:
            _input = load_shot(url)
        except (requests.HTTPError, requests.ConnectTimeout):
            Q.task_done()
            continue

        datetime_now = datetime.datetime.now()
        filename = datetime_now.strftime("%d%m%y-%H-%M-%S-") + salt()
        _output = os.path.expandvars("${TEMP}\\" + f"{filename}.jpg")
        compress_image(_input, _output, 20)
        destination = "/" + SERVER_NAME + "/" + channel_folder + "/" + f"{filename}.jpg"

        try:
            send_to_cloud(_output, destination)
        except (ParentNotFoundError, PathNotFoundError):
            os.remove(_input)
            os.remove(_output)
            Q.task_done()
            continue

        os.remove(_input)
        clear_folder("/" + SERVER_NAME + "/" + channel_folder, CLEAR_OFFSET)
        Q.task_done()


if __name__ == '__main__':
    init_folders()

    for url in URLS:
        Q.put(url)

    threads = []
    for _ in range(NUMBER_OF_THREADS):
        thread = threading.Thread(target=worker)
        thread.start()
        threads.append(thread)

    Q.join()

    for thread in threads:
        thread.join()
