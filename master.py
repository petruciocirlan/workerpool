from bs4 import BeautifulSoup
import subprocess
import traceback
import json
import sys
import os

from common import get_page_content, open_rabbitmq_channel
from common import parse_arguments, DEFAULT_RABBITMQ_QUEUE_NAME
from common import DEBUG_WORKER_INPUT_FILE

# TODO(@pciocirlan): improve exceptions
# TODO(@pciocirlan): add logger (to .log file)
# TODO(@pciocirlan): PEP documentation

# flag -> (argument count, default)
OPTIONS = {
    "--test": (0, False),
    "--queue": (1, DEFAULT_RABBITMQ_QUEUE_NAME),
    "--worker-count": (1, 16)
}

TOPSITE_URL = "https://www.alexa.com/topsites/"

UNRESPONSIVE_COUNTRY_PAGES = [
    # just added (refreshed topsites and newly appeared, but link is down at the moment)
    "Aland Islands",
]

open_subprocesses = list()
settings = {}

def main():
    global settings
    settings = parse_arguments(sys.argv[1:], OPTIONS)

    try:
        country_pages_links = get_country_pages_links()
    except Exception as e:
        raise Exception(f"Get country links: {e}")

    conn, ch = None, None
    if not settings["--test"]:
        conn, ch = open_rabbitmq_channel(settings["--queue"], clear_queue=True)

        global open_subprocesses
        for _ in range(settings["--worker-count"]):
            proc = subprocess.Popen(["python", "worker.py"])
            open_subprocesses.append(proc)

    for link in country_pages_links:
        if link['country'] in UNRESPONSIVE_COUNTRY_PAGES:
            continue

        print(
            f"Looking at {link['country']} top sites ... ", end="", flush=True)
        try:
            top_sites = get_top_country_sites(link['url'])
        except Exception as e:
            print(f"failed: {e}")
            continue
        print("done!")

        send_tasks(ch, link['country'], top_sites)

    if not settings["--test"]:
        send_stop_messages(ch)

    if conn is not None:
        conn.close()


def get_country_pages_links():
    main_page_contents = get_page_content(TOPSITE_URL + 'countries')

    parsed_html = BeautifulSoup(main_page_contents, 'html.parser')

    country_links = list()

    country_lists = parsed_html.find_all("ul", class_="countries")
    for country_list in country_lists:
        links = country_list.find_all("a")

        country_links += [{"country": link.text,
                           "url": TOPSITE_URL + link['href']} for link in links]

    return country_links


def get_top_country_sites(url):
    # print(f"Looking at: {url}")

    country_page_contents = get_page_content(url)

    parsed_html = BeautifulSoup(country_page_contents, 'html.parser')

    site_blocks = parsed_html.find_all("div", class_="site-listing")

    top_sites = list()

    for site_block in site_blocks:
        site_anchor = site_block.find("a")

        top_sites += [site_anchor.text]

    return top_sites


def send_tasks(ch, folder_name, top_sites):
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

        if ch is not None:
            obj_json = json.dumps(obj)

            ch.basic_publish(
                exchange='', routing_key=settings["--queue"], body=obj_json)

            print(f"Sent {obj_json} to queue!")

    if settings["--test"]:
        with open(DEBUG_WORKER_INPUT_FILE, "w") as fd:
            json.dump(jsons, fd, indent=4)
            exit(0)


def send_stop_messages(ch):
    for _ in range(settings["--worker-count"]):
        obj_json = json.dumps({"action": "STOP"})

        ch.basic_publish(
            exchange='', routing_key=settings["--queue"], body=obj_json)


def kill_subprocesses():
    global open_subprocesses
    for proc in open_subprocesses:
        proc.kill()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        kill_subprocesses()
        exit(0)
    except Exception as e:
        print(traceback.format_exc())
        kill_subprocesses()
        exit(-1)
