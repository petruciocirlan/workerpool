import json
import sys
import os

from common import WorkerPoolCommon


class Worker(WorkerPoolCommon):
    # flag -> (argument count, default)
    OPTIONS = {
        "--queue": (1, WorkerPoolCommon.DEFAULT_RABBITMQ_QUEUE_NAME)
    }

    def __init__(self, args):
        super(Worker, self).__init__()
        self._settings = self.parse_arguments(args, self.OPTIONS)
        self.open_logger("Worker")
        self._logger.info("Creating an instance of Worker.")

    def __enter__(self):
        self._logger.info("Setting up RabbitMQ channel parameters.")
        self._conn, self._ch = self.open_rabbitmq_channel(
            self._settings["--queue"])
        self._ch.basic_qos(prefetch_count=1)
        self._logger.info("Done setting up RabbitMQ channel parameters.")

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._ch.close()
        self._conn.close()

    def run(self):
        self._logger.info("Starting consuming.")
        self._ch.basic_consume(
            queue=self._settings["--queue"], auto_ack=False, on_message_callback=self.callback_rabbitmq)
        self._ch.start_consuming()
        self._logger.info("Stopped consuming.")

    @classmethod
    def download_to_disk(cls, info):
        dirname = os.path.dirname(info['LocatieDisk'])
        os.makedirs(dirname, exist_ok=True)

        web_page = cls.get_page_content(info['Link'])
        with open(info['LocatieDisk'], "w", encoding="utf8") as fd:
            fd.write(web_page)

    def callback_rabbitmq(self, ch, method, properties, body):
        self._logger.debug(f"Received '{body.decode('utf-8')}' from queue.")
        message = json.loads(body)

        if "action" in message and message["action"] == "STOP":
            self._logger.info("Received 'stop' task. Stopping consuming.")
            self._ch.stop_consuming()
            return

        try:
            self._logger.debug(
                f"Downloading main page of {message['Link']} to disk in folder {message['LocatieDisk']}.")
            self.download_to_disk(message)
            self._logger.debug(
                f"Done downloading main page of {message['Link']} to disk in folder {message['LocatieDisk']}.")
        except Exception as e:
            self._logger.error(
                f"Failed to download main page of website {message['Link']}: {e}!")
        finally:
            self._ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    with Worker(sys.argv[1:]) as worker:
        try:
            worker.run()
        except KeyboardInterrupt:
            print('Interrupted.')
