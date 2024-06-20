import tomllib
import yadisk
import time
import datetime
from concurrent.futures import ThreadPoolExecutor


# Globals
with open("..\\glob.toml", "rb") as glob_file:
    GLOBALS = tomllib.load(glob_file)

# Config
with open("..\\conf.toml", "rb") as conf_file:
    CONFIG = tomllib.load(conf_file)

client = yadisk.Client(token=GLOBALS['APP_TOKEN'])


def cleaner(item):
    if (datetime.datetime.now(datetime.timezone.utc) - item.created) > datetime.timedelta(days=GLOBALS['CLEAR_OFFSET']):
        try:
            client.remove(item.path)
        except yadisk.exceptions.PathNotFoundError:
            print(f"path - {path} not found. Aborting.")


def finder(path: str) -> list:
    files = []
    fields = ["name", "type", "path", "created"]

    try:
        dir_generator = client.listdir(path, timeout=45, fields=fields)
        print(f"checking {path}")
        for i in dir_generator:
            files.append(i)
    except yadisk.exceptions.PathNotFoundError:
        print(f"path - {path} not found.")
        return files

    print(f"check {path} - complete. Files found {len(files)}")
    return files


if __name__ == '__main__':
    files = []
    start_time = time.time()
    for item in CONFIG["URLS"]:
        path = "/" + GLOBALS["SERVER_NAME"] + "/" + item["name"]
        res = finder(path)
        for i in res:
            files.append(i)

    with ThreadPoolExecutor(max_workers=None) as executor:
        results = list(executor.map(cleaner, files))

    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Elapsed time: ', elapsed_time)
