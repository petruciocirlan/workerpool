import subprocess
import json
import sys
import os

from bs4 import BeautifulSoup

from common import WorkerPoolCommon

# TODO(@pciocirlan): improve exceptions
# TODO(@pciocirlan): add logger (to .log file)
# TODO(@pciocirlan): PEP documentation


class Master(WorkerPoolCommon):
    # flag -> (argument count, default)
    OPTIONS = {
        "--test": (0, False),
        "--queue": (1, WorkerPoolCommon.DEFAULT_RABBITMQ_QUEUE_NAME),
        "--worker-count": (1, 16)
    }

    TOPSITE_URL = "https://www.alexa.com/topsites/"

    # UNRESPONSIVE_COUNTRY_PAGES = [
    # # just added (refreshed topsites and newly appeared, but link is down at the moment)
    # # "Aland Islands",
    # ]

    def __init__(self, args):
        super(Master, self).__init__()
        self._settings = self.parse_arguments(args, self.OPTIONS)

    def __enter__(self):
        self._open_subprocesses = list()

        if not self._settings["--test"]:
            self._conn, self._ch = self.open_rabbitmq_channel(
                self._settings["--queue"], clear_queue=True)

            for _ in range(self._settings["--worker-count"]):
                script_directory = os.path.dirname(os.path.realpath(__file__))
                worker_script_path = os.path.join(
                    script_directory, "worker.py")
                proc = subprocess.Popen(["python", worker_script_path])
                self._open_subprocesses.append(proc)

        return self

    def __exit__(self, exception_type, exception_value, traceback):
        for proc in self._open_subprocesses:
            # TODO(@pciocirlan): log info
            if proc.poll() is None:
                proc.kill()

        self._ch.close()
        self._conn.close()

    def run(self):
        try:
            country_pages_links = self.get_country_pages_links()
        except Exception as e:
            raise Exception(f"Get country links: {e}")

        for link in country_pages_links:
            # if link['country'] in self.UNRESPONSIVE_COUNTRY_PAGES:
            #     continue

            print(
                f"Looking at {link['country']} top sites ... ", end="", flush=True)
            try:
                top_sites = self.get_top_country_sites(link['url'])
            except Exception as e:
                print(f"failed: {e}")
                continue

            print("done!")

            self.send_tasks(link['country'], top_sites)

        if not self._settings["--test"]:
            self.send_stop_messages(self._ch)

        if self._conn is not None:
            self._conn.close()

    @classmethod
    def get_country_pages_links(cls):
        main_page_contents = cls.get_page_content(
            cls.TOPSITE_URL + 'countries')
        parsed_html = BeautifulSoup(main_page_contents, 'html.parser')
        country_links = list()

        country_lists = parsed_html.find_all("ul", class_="countries")
        for country_list in country_lists:
            links = country_list.find_all("a")

            country_links += [{"country": link.text,
                               "url": cls.TOPSITE_URL + link['href']} for link in links]

        return country_links

    @classmethod
    def get_top_country_sites(cls, url):
        # print(f"Looking at: {url}")

        top_sites = list()
        country_page_contents = cls.get_page_content(url)
        parsed_html = BeautifulSoup(country_page_contents, 'html.parser')
        site_blocks = parsed_html.find_all("div", class_="site-listing")

        for site_block in site_blocks:
            site_anchor = site_block.find("a")

            top_sites += [site_anchor.text]

        return top_sites

    def send_tasks(self, folder_name, top_sites):
        jsons = list()
        for rank, website in enumerate(top_sites, 1):
            filename = f"%(index)02d %(website)s.html" % {
                "index": rank,
                "website": website
            }

            # "topsites/{country}/{index} {website}.html"
            filepath = os.path.join("topsites", folder_name, filename)
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

                print(f"Sent {obj_json} to queue!")

        if self._settings["--test"]:
            with open(self.DEBUG_WORKER_INPUT_FILE, "w") as fd:
                json.dump(jsons, fd, indent=4)
                exit(0)

    def send_stop_messages(self): 
        for _ in range(self._settings["--worker-count"]):
            obj_json = json.dumps({"action": "STOP"})

            self._ch.basic_publish(
                exchange='', routing_key=self._settings["--queue"], body=obj_json)


if __name__ == "__main__":
    with Master(sys.argv[1:]) as master:
        try:
            master.run()
        except KeyboardInterrupt:
            print('Interrupted.')
