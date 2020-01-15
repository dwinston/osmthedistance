import gzip
import urllib.request

from lxml import etree

from osmthedistance.extractors import BBBikeExtractor, EXTRACTS_DIR
from osmthedistance.parsetargets import NullTarget, MongoTarget


def download_extract(text, extractor=BBBikeExtractor):
    e = extractor()
    print(e.about())
    region_link = e.region_link(text)
    filename = EXTRACTS_DIR.joinpath(region_link.split("/")[-1])
    if not filename.exists():
        print(f"Downloading {region_link} to {EXTRACTS_DIR}")
        urllib.request.urlretrieve(region_link, filename=filename)
    else:
        print(f"Extract {filename} already exists. Remove file first to re-download.")
    return filename


def parse_to_mongo(filename, estimate_ntags=True, **mongo_target_kwargs):
    filename = str(filename)  # `lxml.etree` cannot parse from `Path` object.
    ntags = None
    if estimate_ntags:
        print("Estimating upper-bound of number of tags as number of lines in file...")
        with gzip.open(filename, 'rb') as f:
            ntags_estimate = sum(1 for _ in f)
        null_parser = etree.XMLParser(target=NullTarget(ntags_estimate=ntags_estimate))
        print("First-pass parsing to obtain number of tags...")
        ntags = etree.parse(filename, null_parser)
    mongo_parser = etree.XMLParser(target=MongoTarget(ntags=ntags, **mongo_target_kwargs))
    return etree.parse(filename, mongo_parser)
