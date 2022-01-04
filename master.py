import subprocess
import json
import sys
import os

from common import WorkerPoolCommon
from html_parser import CountriesHTMLParser, TopSitesHTMLParser


class Master(WorkerPoolCommon):
    """Open workers as subprocesses, crawl alexa.com/topsites
    for links to most visited sites per country
    and send them as tasks to localhost RabbitMQ queue.
    """

    # Option flags for commandline.
    OPTIONS = {
        "--queue": (1, WorkerPoolCommon.DEFAULT_RABBITMQ_QUEUE_NAME),
        "--worker-count": (1, 16)
    }

    TOPSITE_URL = "https://www.alexa.com/topsites/"

    def __init__(self, args):
        """Initialize Master instance.

        Parse commandline arguments. Open logger session.
        """
        super().__init__()
        self._settings = self.parse_arguments(args, self.OPTIONS)
        self.open_logger("Master")
        self._logger.info("Creating an instance of Master.")

    def __enter__(self):
        """Enter context.

        Open connection to RabbitMQ queue and open worker subprocesses.
        """
        self._logger.info("Connecting to RabbitMQ queue.")
        self._conn, self._ch = self.open_rabbitmq_channel(
            self._settings["--queue"], clear_queue=True)
        self._logger.info("Done connecting to RabbitMQ queue.")

        self._open_subprocesses = list()
        worker_count = self._settings["--worker-count"]
        self._logger.info(f"Creating {worker_count} workers.")
        for _ in range(worker_count):
            script_directory = os.path.dirname(os.path.realpath(__file__))
            worker_script_path = os.path.join(
                script_directory, "worker.py")
            proc = subprocess.Popen(["python", worker_script_path])
            self._open_subprocesses.append(proc)
        self._logger.info(f"Done creating {worker_count} workers.")

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """Exit context.

        Close hanging worker processes and RabbitMQ connection.
        """
        for proc in self._open_subprocesses:
            if proc.poll() is None:
                proc.terminate()
                self._logger.debug(
                    f"Terminated unfinished worker with pid {proc.pid}.")
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    self._logger.debug(
                        f"Killed unresponding worker with pid {proc.pid}.")

        if hasattr(self, "_ch") and self._ch.is_open:
            self._ch.close()

        if hasattr(self, "_conn") and self._conn.is_open:
            self._conn.close()

        if exception_type is not None:
            self._logger.warning(f"Interrupted: {exception_type}")

    def run(self):
        """Scrape links to most visited websites per country and send tasks to RabbitMQ queue."""
        self._logger.info(
            "Scraping links to each country's page with top 500 visited websites.")
        try:
            country_pages_links = self.get_country_pages_links()
        except Exception as e:
            self._logger.error(
                f"Failed to get links to top 500s for each country: {e}!")
            raise Exception(f"Failed to get country links: {e}")
        self._logger.info(
            "Done scraping links to each country's page with top 500 visited websites.")

        self._logger.info(
            f"Scraping top 50 links for each country and sending tasks.")
        for link in country_pages_links:
            self._logger.info(
                f"Scraping links to top sites of country {link['country']}.")
            try:
                top_sites = self.get_top_country_sites(link['url'])
            except Exception as e:
                self._logger.error(
                    f"Failed to get top 50 most visited websites for country {link['country']}: {e}!")
                continue
            self._logger.info(
                f"Done craping links to top sites of country {link['country']}.")

            self._logger.info(
                f"Sending tasks to workers for country {link['country']}.")
            self.send_tasks(link['country'], top_sites)
            self._logger.info(
                f"Done sending tasks to workers for country {link['country']}.")
        self._logger.info(
            f"Done scraping top 50 links for each country and sending tasks.")

        self._logger.info("Sending 'STOP' tasks to all workers.")
        self.send_stop_messages()
        self._logger.info("Done sending 'STOP' tasks to all workers.")

        self._logger.info("Closing RabbitMQ connections.")
        self._ch.close()
        self._conn.close()
        self._logger.info("Done closing RabbitMQ connections.")

        self._logger.info("Waiting for workers to finish.")
        for proc in self._open_subprocesses:
            proc.wait()
        self._logger.info("Done waiting for workers to finish.")

    @classmethod
    def get_country_pages_links(cls):
        """Return links to all countries' top 500 rankings pages."""
        main_page_contents = cls.get_page_content(
            cls.TOPSITE_URL + 'countries')

        country_links = CountriesHTMLParser().extract_links(main_page_contents)

        # Links are relative; prepend topsite url
        for index in range(len(country_links)):
            country_links[index]['url'] = cls.TOPSITE_URL + \
                country_links[index]['url']

        return country_links

    @classmethod
    def get_top_country_sites(cls, url):
        """Return links to most visited websites for a country, given by url."""
        country_page_contents = cls.get_page_content(url)

        top_sites = TopSitesHTMLParser().extract_links(country_page_contents)

        return top_sites

    def send_tasks(self, folder_name, top_sites):
        """Serialize and send tasks to RabbitMQ queue."""
        jsons = list()
        for rank, website in enumerate(top_sites, 1):
            filename = f"%(index)02d %(website)s.html" % {
                "index": rank,
                "website": website
            }

            # "topsites/{country}/{index} {website}.html"
            filepath = os.path.join(
                os.getcwd(), "topsites", folder_name, filename)
            website = f"http://{website}"

            obj = {
                "Link": website,
                "LocatieDisk": filepath
            }

            jsons += [obj]

            if self._ch is not None:
                obj_json = json.dumps(obj)

                self._ch.basic_publish(
                    exchange='', routing_key=self._settings["--queue"], body=obj_json)

                self._logger.debug(f"Sent {obj_json} to queue.")

    def send_stop_messages(self):
        """Send 'STOP' messages to worker subprocesses to signal end of tasks."""
        for _ in range(self._settings["--worker-count"]):
            obj_json = json.dumps({"action": "STOP"})

            self._ch.basic_publish(
                exchange='', routing_key=self._settings["--queue"], body=obj_json)


if __name__ == "__main__":
    try:
        with Master(sys.argv[1:]) as master:
            master.run()
    except:
        pass
