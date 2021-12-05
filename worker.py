import traceback
import pika
import json
import sys
import os

from common import get_page_content, open_rabbitmq_channel
from common import parse_arguments, DEFAULT_RABBITMQ_QUEUE_NAME
from common import DEBUG_WORKER_INPUT_FILE

# flag -> (argument count, default)
OPTIONS = {
    "--test": (0, False),
    "--queue": (1, DEFAULT_RABBITMQ_QUEUE_NAME)
}

settings = {}


def main():
    global settings
    settings = parse_arguments(sys.argv[1:], OPTIONS)

    if settings["--test"]:
        with open(DEBUG_WORKER_INPUT_FILE, "r") as fd:
            data = json.load(fd)

        for top_site in data:
            try:
                download_to_disk(top_site)
            except Exception as e:
                print(f"Failed to download website {top_site['website']}: {e}")

        exit(0)

    conn, ch = open_rabbitmq_channel(settings["--queue"])
    ch.basic_qos(prefetch_count=1)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    ch.basic_consume(
        queue=settings["--queue"], auto_ack=False, on_message_callback=callback_rabbitmq)
    ch.start_consuming()


def download_to_disk(info):
    dirname = os.path.dirname(info['LocatieDisk'])
    os.makedirs(dirname, exist_ok=True)

    web_page = get_page_content(info['Link'])

    with open(info['LocatieDisk'], "w", encoding="utf8") as fd:
        fd.write(web_page)


def callback_rabbitmq(ch, method, properties, body):
    print(" [x] Received %r" % body)

    message = json.loads(body)

    if "action" in message and message["action"] == "STOP":
        print(" [x] STOP message received. Stopped.")
        exit(0)

    try:
        download_to_disk(message)
    except Exception as e:
        print(f"Failed to download website {message['Link']}: {e}")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        exit(0)
    except Exception:
        print(traceback.format_exc())
        exit(-1)
