from html.parser import HTMLParser


class CountriesHTMLParser(HTMLParser):
    """Parse Alexa's 'Top Sites For Countries' page
    and retrieves links to Top 500 Visited page for each country.
    """

    def __init__(self) -> None:
        super().__init__()
        self.country_links = list()
        self.inCountriesList = False
        self.inAnchor = False

    def handle_starttag(self, tag, attrs) -> None:
        if tag == "ul":
            for attr in attrs:
                if attr[0] == 'class' and 'countries' in attr[1]:
                    self.inCountriesList = True
                    break
        elif tag == "a" and self.inCountriesList:
            for attr in attrs:
                if attr[0] == 'href':
                    self.inAnchor = True
                    self.country_links.append({"url": attr[1]})
                    break

    def handle_endtag(self, tag: str) -> None:
        if tag == "ul":
            self.inCountriesList = False
        elif tag == "a":
            self.inAnchor = False

    def handle_data(self, data: str) -> None:
        if self.inAnchor:
            self.country_links[-1]['country'] = data

    def extract_links(self, html):
        self.feed(html)
        return self.country_links.copy()


class TopSitesHTMLParser(HTMLParser):
    """Parse Alexa's 'Top Sites' page for a specific country
    and retrieve links to each top site.
    """

    def __init__(self) -> None:
        super().__init__()
        self.top_sites = list()
        self.inDescriptionCell = False
        self.inAnchor = False

    def handle_starttag(self, tag, attrs) -> None:
        if tag == "div":
            for attr in attrs:
                if attr[0] == 'class' and 'DescriptionCell' in attr[1]:
                    self.inDescriptionCell = True
                    break
        elif tag == "a" and self.inDescriptionCell:
            for attr in attrs:
                if attr[0] == 'href':
                    self.inAnchor = True
                    break

    def handle_endtag(self, tag: str) -> None:
        if tag == "div":
            self.inDescriptionCell = False
        elif tag == "a":
            self.inAnchor = False

    def handle_data(self, data: str) -> None:
        if self.inAnchor:
            self.top_sites.append(data)

    def extract_links(self, html):
        self.feed(html)
        return self.top_sites.copy()
