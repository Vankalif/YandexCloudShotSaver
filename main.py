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

from requests.auth import HTTPDigestAuth
from yadisk.exceptions import ParentNotFoundError, PathNotFoundError

# Globals
with open("glob.toml", "rb") as f:
    GLOBALS = tomllib.load(f)
Q = queue.Queue()
CLIENT = yadisk.Client(token=GLOBALS['APP_TOKEN'])


def init_folders(config):
    if not CLIENT.exists(f"/{GLOBALS['SERVER_NAME']}"):
        CLIENT.mkdir(f"/{GLOBALS['SERVER_NAME']}")
    else:
        return

    for resource in config["URLS"]:
        if not CLIENT.exists(f"/{GLOBALS['SERVER_NAME']}/{resource["name"]}"):
            CLIENT.mkdir(f"/{GLOBALS['SERVER_NAME']}/{resource["name"]}")


def load_config(path):
    with open(path, "rb") as f:
        config = tomllib.load(f)
        return config


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
        except (requests.HTTPError, requests.ConnectTimeout):
            Q.task_done()
            continue

        datetime_now = datetime.datetime.now()
        filename = datetime_now.strftime("%d%m%y-%H-%M-%S-") + salt()
        _output = os.path.expandvars("${TEMP}\\" + f"{filename}.jpg")
        compress_image(_input, _output, 20)
        destination = "/" + GLOBALS['SERVER_NAME'] + "/" + channel_folder + "/" + f"{filename}.jpg"

        try:
            send_to_cloud(_output, destination)
        except (ParentNotFoundError, PathNotFoundError):
            os.remove(_input)
            os.remove(_output)
            Q.task_done()
            continue

        os.remove(_input)
        clear_folder("/" + GLOBALS['SERVER_NAME'] + "/" + channel_folder, GLOBALS['CLEAR_OFFSET'])
        Q.task_done()


if __name__ == '__main__':
    config = load_config("conf.toml")
    init_folders(config)

    for resource in config["URLS"]:
        Q.put(resource)

    threads = []
    for _ in range(GLOBALS['NUMBER_OF_THREADS']):
        thread = threading.Thread(target=worker)
        thread.start()
        threads.append(thread)

    Q.join()

    for thread in threads:
        thread.join()
