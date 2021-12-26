import json
import sys
import os

from common import WorkerPoolCommon


class Worker(WorkerPoolCommon):
    # flag -> (argument count, default)
    OPTIONS = {
        "--test": (0, False),
        "--queue": (1, WorkerPoolCommon.DEFAULT_RABBITMQ_QUEUE_NAME)
    }

    def __init__(self, args):
        super(Worker, self).__init__()
        self._settings = self.parse_arguments(args, self.OPTIONS)

    def __enter__(self):
        self._conn, self._ch = self.open_rabbitmq_channel(
            self._settings["--queue"])
        self._ch.basic_qos(prefetch_count=1)

        return self

        # if self._settings["--test"]:
        #     with open(self.DEBUG_WORKER_INPUT_FILE, "r") as fd:
        #         data = json.load(fd)

        #     for top_site in data:
        #         try:
        #             self.download_to_disk(top_site)
        #         except Exception as e:
        #             print(f"Failed to download website {top_site['website']}: {e}")

        #     exit(0)

    def __exit__(self, exception_type, exception_value, traceback):
        self._ch.close()
        self._conn.close()

    def run(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self._ch.basic_consume(
            queue=self._settings["--queue"], auto_ack=False, on_message_callback=self.callback_rabbitmq)
        self._ch.start_consuming()

    @classmethod
    def download_to_disk(cls, info):
        dirname = os.path.dirname(info['LocatieDisk'])
        os.makedirs(dirname, exist_ok=True)

        web_page = cls.get_page_content(info['Link'])

        with open(info['LocatieDisk'], "w", encoding="utf8") as fd:
            fd.write(web_page)

    def callback_rabbitmq(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        message = json.loads(body)

        if "action" in message and message["action"] == "STOP":
            print(" [x] STOP message received. Stopped consuming.")
            self._ch.stop_consuming()
            return

        try:
            self.download_to_disk(message)
        except Exception as e:
            print(f"Failed to download website {message['Link']}: {e}")
        finally:
            self._ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    with Worker(sys.argv[1:]) as worker:
        try:
            worker.run()
        except KeyboardInterrupt:
            print('Interrupted.')
