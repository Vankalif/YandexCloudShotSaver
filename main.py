import datetime
import uuid
import os
import tomllib
import ffmpegio
import yadisk
import requests
import logging
import concurrent.futures

from requests.auth import HTTPDigestAuth
from yadisk.exceptions import ParentNotFoundError, PathNotFoundError

# Globals
with open("glob.toml", "rb") as f:
    GLOBALS = tomllib.load(f)

CLIENT = yadisk.Client(token=GLOBALS['APP_TOKEN'], session="httpx")
logging.basicConfig(level=logging.DEBUG, filename="log.txt")
TRASH = []


def init_folders(config):
    logging.debug(f"{datetime.datetime.now()} Начало проверки папок")
    if not CLIENT.exists(f"/{GLOBALS['SERVER_NAME']}"):
        logging.debug(f"{datetime.datetime.now()} Создан корневой каталог /{GLOBALS['SERVER_NAME']}")
        CLIENT.mkdir(f"/{GLOBALS['SERVER_NAME']}")
    else:
        logging.debug(f"{datetime.datetime.now()} Корневой каталог найден - возврат")
        return

    for resource in config["URLS"]:
        if not CLIENT.exists(f"/{GLOBALS['SERVER_NAME']}/{resource["name"]}"):
            CLIENT.mkdir(f"/{GLOBALS['SERVER_NAME']}/{resource["name"]}")
            logging.debug(
                f"{datetime.datetime.now()} Создан облачный каталог /{GLOBALS['SERVER_NAME']}/{resource["name"]}")


def delete_files_in_directory(directory_path):
    files = os.listdir(directory_path)
    for file in files:
        file_path = os.path.join(directory_path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
            logging.debug(f"{datetime.datetime.now()} Файл удален {file_path}")


def load_config(path):
    with open(path, "rb") as f:
        config = tomllib.load(f)
        return config


def get_offset_shot_name(ch_id, offset) -> str:
    name = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=offset)).strftime("%d%m%y-%H-%M-")
    name = name + ch_id
    return name


def send_to_cloud(source, destination):
    logging.debug(f"{datetime.datetime.now()} Скриншот {source} отправляется.")
    CLIENT.upload(source, destination)
    logging.debug(f"{datetime.datetime.now()} Скриншот {source} отправлен в {destination}")


def compress_image(input_image_path, output_image_path, quality_scale=2):
    ffmpegio.transcode(input_image_path, output_image_path, **{"q:v": quality_scale})


def load_shot(source, login, pwd) -> str:
    logging.debug(f"{datetime.datetime.now()} Выполняется запрос к {source} с параметрами {login}/{pwd}")
    response = requests.get(source,
                            auth=HTTPDigestAuth(login, pwd),
                            verify=False,
                            stream=True,
                            timeout=5)

    response.raise_for_status()
    bdata = response.content
    path = os.path.expandvars("${TEMP}\\" + f"{str(uuid.uuid4())}.jpg")

    with open(path, 'wb') as file:
        file.write(bdata)

    return path


def worker(cam_info):
    resource = cam_info["resource"]
    channel_folder = cam_info["name"]
    login = cam_info["login"]
    pwd = cam_info["pwd"]

    try:
        _input = load_shot(resource, login, pwd)
        logging.debug(f"{datetime.datetime.now()} Получен скриншот {_input}")
    except (requests.HTTPError, requests.ConnectTimeout):
        logging.debug(f"{datetime.datetime.now()} При получении скриншота {_input} произошел сбой.")
        return

    datetime_now = datetime.datetime.now()
    filename = datetime_now.strftime("%d%m%y-%H-%M-") + cam_info["ch_id"]
    _output = os.path.expandvars("${TEMP}\\" + f"{filename}.jpg")
    compress_image(_input, _output, GLOBALS['QUALITY_SCALE'])
    logging.debug(f"{datetime.datetime.now()} Выполнено сжатие скриншота {_output}")
    destination = "/" + GLOBALS['SERVER_NAME'] + "/" + channel_folder + "/" + f"{filename}.jpg"

    try:
        send_to_cloud(_output, destination)
    except (ParentNotFoundError, PathNotFoundError):
        logging.debug(f"{datetime.datetime.now()} Сбой при загрузке скриншота {_output}")
        return


if __name__ == '__main__':
    logging.debug(f"{datetime.datetime.now()} Запуск программы")
    exit(0)

    config = load_config("conf.toml")
    init_folders(config)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=GLOBALS['NUMBER_OF_THREADS']) as executor:
            executor.map(worker, config["URLS"])
            executor.shutdown()
            CLIENT.close()

    except Exception as e:
        logging.debug(f"{datetime.datetime.now()} Общий сбой {e}")
    finally:
        delete_files_in_directory(os.path.expandvars("${TEMP}\\"))
