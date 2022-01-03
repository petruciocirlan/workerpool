import logging
import sys
import os

import requests
import pika


class WorkerPoolCommon:
    """WorkerPoolCommon class implements methods used by both Master and Worker classes."""

    DEFAULT_RABBITMQ_QUEUE_NAME = "WorkerPool"

    @staticmethod
    def get_page_content(url):
        """Return page content of website at given url."""
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            raise Exception(
                f"Failed to retrieve page contents: status code {response.status_code}")

        return response.text

    @staticmethod
    def open_rabbitmq_channel(queue_name=DEFAULT_RABBITMQ_QUEUE_NAME, clear_queue=False):
        """Open a connection to RabbitMQ on localhost and a channel with new queue (re)declared."""
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        if clear_queue:
            channel.queue_delete(queue=queue_name)

        channel.queue_declare(queue=queue_name)

        return connection, channel

    @staticmethod
    def parse_arguments(args, default_options):
        """Parse commandline arguments, with respect to default options."""
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

    def open_logger(self, logger_name):
        """Open logger session with formatted output to both disk and console."""
        self._logger = logging.getLogger(logger_name)

        script_directory = os.path.dirname(os.path.realpath(__file__))
        logs_directory = os.path.join(script_directory, "logs/")
        os.makedirs(logs_directory, exist_ok=True)
        filepath = os.path.join(logs_directory, logger_name + '.log')

        output_file_handler = logging.FileHandler(filepath)
        stdout_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03d pid:%(process)5d [%(levelname)-8s] %(module)s - %(funcName)s: %(message)s", "%Y-%m-%d %H:%M:%S")

        output_file_handler.setFormatter(formatter)
        stdout_handler.setFormatter(formatter)

        self._logger.addHandler(output_file_handler)
        self._logger.addHandler(stdout_handler)

        self._logger.setLevel(logging.DEBUG)
