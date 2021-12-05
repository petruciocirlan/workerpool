from bs4 import BeautifulSoup
import traceback
import json
import sys
import os

from common import get_page_content, DEBUG_WORKER_INPUT_FILE

TOPSITE_URL = "https://www.alexa.com/topsites/"

UNRESPONSIVE_COUNTRY_PAGES = [
    "Aland Islands", # just added (refreshed topsites and newly appeared, but link is down at the moment)
]

# TODO(@pciocirlan): improve exceptions
# TODO(@pciocirlan): add logger (to .log file)

def main():
    try:
        country_pages_links = get_country_pages_links()
    except Exception as e:
        raise Exception(f"Get country links: {e}")

    for link in country_pages_links:
        if link['country'] in UNRESPONSIVE_COUNTRY_PAGES:
            continue

        print(f"Looking at {link['country']} top sites ... ", end="", flush=True)
        top_50_sites = get_top_country_sites(link['url'])
        print("done!")

        jsons = list()
        for rank, website in enumerate(top_50_sites, 1):
            filename = f"%(index)02d %(website)s.html" % {
                "index": rank,
                "website": website
                }

            # "{country}/{index} {website}.html"
            filepath = os.path.join("topsites", link['country'], filename)
            website = f"http://{website}"

            jsons += [{
                "filepath": filepath,
                "website": website
                }]

        if "--test" in sys.argv:
            with open(DEBUG_WORKER_INPUT_FILE, "w") as fd:
                json.dump(jsons, fd, indent=4)
                exit(0)


def get_country_pages_links():
    main_page_contents = get_page_content(TOPSITE_URL + 'countries')

    parsed_html = BeautifulSoup(main_page_contents, 'html.parser')

    country_links = list()

    country_lists = parsed_html.find_all("ul", class_="countries")
    for country_list in country_lists:
        links = country_list.find_all("a")

        country_links += [{"country": link.text, "url": TOPSITE_URL + link['href']} for link in links]        

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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(traceback.format_exc())
