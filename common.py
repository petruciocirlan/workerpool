import requests
import pika


class WorkerPoolCommon:
    DEFAULT_RABBITMQ_QUEUE_NAME = "WorkerPool"
    DEBUG_WORKER_INPUT_FILE = "test-rabbitmq-json-example.json"

    @staticmethod
    def get_page_content(url):
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(
                f"Failed to retrieve page contents: status code {response.status_code}")

        return response.text

    @staticmethod
    def open_rabbitmq_channel(queue_name=DEFAULT_RABBITMQ_QUEUE_NAME, clear_queue=False):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        if clear_queue:
            channel.queue_delete(queue=queue_name)

        channel.queue_declare(queue=queue_name)

        return connection, channel

    @staticmethod
    def parse_arguments(args, default_options):
        settings = {}

        for option in default_options:
            settings[option] = default_options[option][1]

        index = 0
        while index < len(args):
            flag = args[index]
            index += 1

            if flag not in default_options:
                raise Exception(f"Unrecognized option {flag}")

            argument_count = default_options[flag][0]

            if argument_count == 0:
                settings[flag] = True
                continue

            if index + argument_count > len(args):
                raise Exception(f"Not enough arguments for option {flag}")

            arguments = list()
            for _ in range(argument_count):
                arguments.append(args[index])
                index += 1

            if argument_count == 1:
                settings[flag] = arguments[0]
            else:
                settings[flag] = tuple(arguments)

        return settings
