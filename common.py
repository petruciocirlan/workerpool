import requests

DEBUG_WORKER_INPUT_FILE = "test-rabbitmq-json-example.json"

def get_page_content(url):
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to retrieve page contents: status code {response.status_code}")

    return response.text