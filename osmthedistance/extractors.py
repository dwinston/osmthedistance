from pathlib import Path

import requests
from bs4 import BeautifulSoup

EXTRACTS_DIR = Path("~/.osmthedistance/osmextracts/").expanduser()
EXTRACTS_DIR.mkdir(parents=True, exist_ok=True)


class BBBikeExtractor:
    def __init__(self):
        url = "https://download.bbbike.org/osm/bbbike/"
        rv = requests.get("https://download.bbbike.org/osm/bbbike/")
        if rv.status_code != 200:
            raise Exception(f"Failed to fetch BBBike extract listing: {url}")
        soup = BeautifulSoup(rv.text, "html.parser")
        self.regions = {
            link.text.replace(" ", "").lower(): link.text
            for link in soup.tbody.find_all("a")
        }

    def region_link(self, text):
        text_key = text.replace(" ", "").lower()
        if text_key in self.regions:
            region = self.regions.get(text_key)
            return f"https://download.bbbike.org/osm/bbbike/{region}/{region}.osm.gz"
        else:
            raise Exception(f"No region key {text_key} found in available regions")

    def about(self):
        return ("BBBike.org, a cycle route planner, serves regularly-updated OSM extracts of more than "
                "200 cities and regions world wide (https://download.bbbike.org/osm/bbbike/). "
                "Donations accepted via https://extract.bbbike.org/community.html .")
