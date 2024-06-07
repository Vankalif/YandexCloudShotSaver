import datetime
import string
import threading
import uuid
import os
import random
import yadisk
import requests
import ffmpeg
import queue

from requests.auth import HTTPDigestAuth

# Globals
SERVER_NAME = "Отрадная"
APP_TOKEN = "y0_AgAAAABl3sHIAAvpVwAAAAEGxjnpAABpGseraHRK3bgTIqzeVm3n_DDVVg"
NUMBER_OF_THREADS = 5
URLS = [
    {"resource": "http://admin:123456zxC@192.168.110.2:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_1"},
    {"resource": "http://admin:123456zxC@192.168.110.3:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_2"},
    {"resource": "http://admin:123456zxC@192.168.110.4:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_3"},
    {"resource": "http://admin:123456zxC@192.168.110.5:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_4"},
    {"resource": "http://admin:123456zxC@192.168.110.6:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_5"}
]
Q = queue.Queue()
CLIENT = yadisk.Client(token=APP_TOKEN)
CLEAR_OFFSET = 72


def init_folders():
    if not CLIENT.exists(f"/{SERVER_NAME}"):
        CLIENT.mkdir(f"/{SERVER_NAME}")

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
    ffmpeg.input(input_image_path).output(output_image_path, qscale=quality_scale).run(quiet=True)


def load_shot(source) -> str:
    bdata = requests.get(source,
                         auth=HTTPDigestAuth('admin', '123456zxC'),
                         verify=False,
                         stream=True).content
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
        _input = load_shot(url)
        datetime_now = datetime.datetime.now()
        filename = datetime_now.strftime("%d%m%y-%H-%M-%S-") + salt()
        _output = os.path.expandvars("${TEMP}\\" + f"{filename}.jpg")
        compress_image(_input, _output, 20)
        destination = "/" + SERVER_NAME + "/" + channel_folder + "/" + f"{filename}.jpg"
        send_to_cloud(_output, destination)
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

