from bs4 import BeautifulSoup
import requests

TOPSITE_URL = "https://www.alexa.com/topsites/"

UNRESPONSIVE_COUNTRY_PAGES = [
    "Aland Islands", # just added (refreshed topsites and newly appeared, but link is down at the moment)
]

# TODO(@pciocirlan): improve exceptions

def main():
    try:
        country_pages_links = get_country_pages_links()
    except Exception as e:
        raise Exception(f"Get country links: {e}")

    for link in country_pages_links:
        if link[0] in UNRESPONSIVE_COUNTRY_PAGES:
            continue

        print(f"Looking at {link[0]}...")
        top_50_sites = get_top_country_sites(link[1])

        print(top_50_sites)
        print()


def get_country_pages_links():
    main_page_contents = get_page_content(TOPSITE_URL + 'countries')

    parsed_html = BeautifulSoup(main_page_contents, 'html.parser')

    country_links = list()

    country_lists = parsed_html.find_all("ul", class_="countries")
    for country_list in country_lists:
        links = country_list.find_all("a")

        country_links += [(link.text, TOPSITE_URL + link['href']) for link in links]        

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

def get_page_content(url):
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to retrieve page contents: status code {response.status_code}")

    return response.text


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
