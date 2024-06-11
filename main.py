import datetime
import string
import threading
import uuid
import os
import random
import tomllib
import ffmpegio
import yadisk
import requests
import queue
import logging

from requests.auth import HTTPDigestAuth
from yadisk.exceptions import ParentNotFoundError, PathNotFoundError

# Globals
with open("glob.toml", "rb") as f:
    GLOBALS = tomllib.load(f)

Q = queue.Queue()
CLIENT = yadisk.Client(token=GLOBALS['APP_TOKEN'])
logging.basicConfig(level=logging.DEBUG, filename="log.txt")
TRASH = []


def init_folders(config):
    logging.debug(f"{datetime.datetime.now()} Начало проверки папок")
    if not CLIENT.exists(f"/{GLOBALS['SERVER_NAME']}"):
        logging.debug(f"{datetime.datetime.now()} Корневой каталог не обнаружен - создание")
        CLIENT.mkdir(f"/{GLOBALS['SERVER_NAME']}")
    else:
        logging.debug(f"{datetime.datetime.now()} Корневой каталог найден - возврат")
        return

    for resource in config["URLS"]:
        if not CLIENT.exists(f"/{GLOBALS['SERVER_NAME']}/{resource["name"]}"):
            logging.debug(f"{datetime.datetime.now()} Каталог камеры не найден - создание")
            CLIENT.mkdir(f"/{GLOBALS['SERVER_NAME']}/{resource["name"]}")


def load_config(path):
    with open(path, "rb") as f:
        config = tomllib.load(f)
        logging.debug(f"{datetime.datetime.now()} Конфигурационный файл прочитан - возврат")
        return config


def clear_folder(path: str, offset: int, perm=True):
    dir_struct = CLIENT.listdir(path)
    for file in dir_struct:
        if (datetime.datetime.now(datetime.timezone.utc) - file.created) > datetime.timedelta(days=offset):
            CLIENT.remove(file.path, permanently=perm)
            logging.debug(f"{datetime.datetime.now()} Устаревший файл {file.path} удален")


def salt(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def send_to_cloud(source, destination):
    CLIENT.upload(source, destination, timeout=10, n_retries=2, retry_interval=4)
    logging.debug(f"{datetime.datetime.now()} Скриншот {source} отправлен в {destination}")


def compress_image(input_image_path, output_image_path, quality_scale=2):
    ffmpegio.transcode(input_image_path, output_image_path, **{"q:v": quality_scale})


def load_shot(source, login, pwd) -> str:
    response = requests.get(source,
                            auth=HTTPDigestAuth(login, pwd),
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
        login = cam_info["login"]
        pwd = cam_info["pwd"]

        try:
            _input = load_shot(url, login, pwd)
            logging.debug(f"{datetime.datetime.now()} Получен скриншот {_input}")
        except (requests.HTTPError, requests.ConnectTimeout):
            Q.task_done()
            continue

        datetime_now = datetime.datetime.now()
        filename = datetime_now.strftime("%d%m%y-%H-%M-%S-") + salt()
        _output = os.path.expandvars("${TEMP}\\" + f"{filename}.jpg")
        compress_image(_input, _output, GLOBALS['QUALITY_SCALE'])
        logging.debug(f"{datetime.datetime.now()} Выполнено сжатие скриншота {_output}")
        destination = "/" + GLOBALS['SERVER_NAME'] + "/" + channel_folder + "/" + f"{filename}.jpg"

        try:
            send_to_cloud(_output, destination)
        except (ParentNotFoundError, PathNotFoundError):
            Q.task_done()
            continue

        clear_folder("/" + GLOBALS['SERVER_NAME'] + "/" + channel_folder, GLOBALS['CLEAR_OFFSET'])
        TRASH.append(_input)
        logging.debug(f"{datetime.datetime.now()} Скриншот {_input} помещен в корзину")
        TRASH.append(_output)
        logging.debug(f"{datetime.datetime.now()} Скриншот {_output} помещен в корзину")
        Q.task_done()


if __name__ == '__main__':
    logging.debug(f"{datetime.datetime.now()} Запуск программы")

    config = load_config("conf.toml")
    init_folders(config)

    for resource in config["URLS"]:
        Q.put(resource)
        logging.debug(f"{datetime.datetime.now()} Задание {resource["name"]} поставлено в очередь задач")

    threads = []
    for _ in range(GLOBALS['NUMBER_OF_THREADS']):
        thread = threading.Thread(target=worker)
        thread.start()
        logging.debug(f"{datetime.datetime.now()} Запущен поток {thread.name}")
        threads.append(thread)

    Q.join()

    for thread in threads:
        logging.debug(f"{datetime.datetime.now()} Ожидание завершения потока {thread.name}")
        thread.join()

    CLIENT.close()

    for item in TRASH:
        os.remove(item)
        logging.debug(f"{datetime.datetime.now()} Файл {item} удален.")
