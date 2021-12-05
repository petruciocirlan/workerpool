from bs4 import BeautifulSoup
import requests

MAIN_PAGE_URL = "https://www.alexa.com/topsites/"

# TODO(@pciocirlan): improve exceptions

def main():
    try:
        country_pages_links = get_country_pages_links()
    except Exception as e:
        raise Exception(f"Get country links: {e}")

    for link in country_pages_links:
        print(link)


def get_country_pages_links():
    try:
        main_page_contents = get_page_content(MAIN_PAGE_URL + 'countries')
    except Exception as e:
        raise Exception(f"Failed to retrieve page contents: {e}")

    parsed_html = BeautifulSoup(main_page_contents, 'html.parser')

    country_links = list()

    country_lists = parsed_html.find_all("ul", class_="countries")
    for country_list in country_lists:
        links = country_list.find_all("a")

        country_links += [MAIN_PAGE_URL + link['href'] for link in links]        

    return country_links


def get_page_content(url):
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Failed GET request")

    return response.text


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
