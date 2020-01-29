from itertools import tee

from haversine import haversine, Unit
from tqdm import tqdm
import pydash as py_


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def lat_lon(node):
    coords = node["loc"]["coordinates"]
    return coords[1], coords[0]


def distance_along(from_node, to_node, nodes):
    adding = False
    distance = 0
    for n1, n2 in pairwise(nodes):
        if n1["_id"] == from_node:
            adding = True
        if adding:
            distance += haversine(lat_lon(n1), lat_lon(n2), unit=Unit.METERS)
        if adding and n2["_id"] == to_node:
            break
    return distance


def to_geojson(docs):
    """Returns node collection documents in GeoJSON format."""
    return [{
        "loc": {"type": "Point", "coordinates": [float(doc["lon"]), float(doc["lat"])]},
        "_id": doc["id"],
    } for doc in docs]


def process_in_chunks(keys, process, chunk_size=10000):
    docs = []
    chunks = py_.chunk(keys, chunk_size)
    pbar = tqdm(total=len(keys))
    for chunk in chunks:
        docs.extend(process(chunk))
        pbar.update(len(chunk))
    pbar.close()
    return docs


def remdups(seq, key=id):
    """Return new sequence with duplicates (identified by key) removed."""
    rv = []
    seen = set()
    for item in seq:
        k = key(item)
        if k in seen:
            continue
        else:
            rv.append(item)
            seen.add(k)
    return rv
