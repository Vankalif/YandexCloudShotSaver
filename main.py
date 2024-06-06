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

SERVER_NAME = "TEST-SERVER"
APP_TOKEN = "y0_AgAAAABl3sHIAAvpVwAAAAEGxjnpAABpGseraHRK3bgTIqzeVm3n_DDVVg"
urls = [
    {"resource": "http://admin:123456zxC@192.168.110.2:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_1"},
    {"resource": "http://admin:123456zxC@192.168.110.3:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_2"},
    {"resource": "http://admin:123456zxC@192.168.110.4:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_3"},
    {"resource": "http://admin:123456zxC@192.168.110.5:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_4"},
    {"resource": "http://admin:123456zxC@192.168.110.6:80/ISAPI/Streaming/channels/1/picture", "name": "Камера_5"}
]
q = queue.Queue()


def salt(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def send_to_cloud(source, destination, token):
    client = yadisk.Client(token=token)

    with client:
        if client.check_token():
            client.upload(source, destination)
            os.remove(source)


def compress_image(input_image_path, output_image_path, quality_scale=2):
    ffmpeg.input(input_image_path).output(output_image_path, qscale=quality_scale).run()


def load_shot(url) -> str:
    bdata = requests.get(url).content
    path = os.path.expandvars("${TEMP}\\" + f"{str(uuid.uuid4())}.jpg")

    with open(path, 'wb') as file:
        file.write(bdata)

    return path


def process_image(info):
    _input = load_shot(info.resource)
    datetime_now = datetime.datetime.now()
    filename = datetime_now.strftime("%d%m%y-%H-%M-%S-") + salt()
    _output = os.path.expandvars("${TEMP}\\" + f"{filename}.jpg")
    compress_image(_input, _output, 20)
    destination = SERVER_NAME + "/" + info.name + "/" + filename
    send_to_cloud(_output, destination, APP_TOKEN)


if __name__ == '__main__':
    for url in urls:
        q.put(url)

    while not q.empty():
        thread1 = threading.Thread(target=process_image, args=q.get())
        thread1.start()
        thread1.join()
