import traceback
import pika
import json
import sys
import os

from common import get_page_content, DEBUG_WORKER_INPUT_FILE

def main():
    data = None
    if "--test" in sys.argv:
        with open(DEBUG_WORKER_INPUT_FILE, "r") as fd:
            data = json.load(fd)

    for top_site in data:
        try:
            download_to_disk(top_site)
        except Exception as e:
            print(f"Failed to download website {top_site['website']}: {e}")


def download_to_disk(info):
    dirname = os.path.dirname(info['filepath'])
    os.makedirs(dirname, exist_ok=True)

    web_page = get_page_content(info['website'])

    with open(info['filepath'], "w", encoding="utf8") as fd:
        fd.write(web_page)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
