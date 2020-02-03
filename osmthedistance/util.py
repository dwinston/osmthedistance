from itertools import tee
from math import radians, sqrt, sin, asin, degrees, atan, cos, atan2

from haversine import haversine, Unit
from haversine.haversine import get_avg_earth_radius
from tqdm import tqdm
import pydash as py_


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def triplewise(iterable):
    """s -> (s0,s1,s2), (s1,s2,s3), (s2,s3,s4), ..."""
    a, b, c = tee(iterable, n=3)
    next(b, None)
    next(c, None)
    next(c, None)
    return zip(a, b, c)


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


def surface_turn_angle(point1, point2, point3):
    """Angle of turn going from point1 to point3 through point2."""
    # unpack latitude/longitude
    lat1, lng1 = point1
    lat2, lng2 = point2
    lat3, lng3 = point3

    # convert all latitudes/longitudes from decimal degrees to radians
    lat1, lng1 = radians(lat1), radians(lng1)
    lat2, lng2 = radians(lat2), radians(lng2)
    lat3, lng3 = radians(lat3), radians(lng3)

    # arc lengths (angles in radians subtended from center of sphere) between points
    a = sqrt((lat1 - lat2) ** 2 + (lng1 - lng2) ** 2)
    b = sqrt((lat2 - lat3) ** 2 + (lng2 - lng3) ** 2)
    c = sqrt((lat1 - lat3) ** 2 + (lng1 - lng3) ** 2)  # not actually traversed

    # law of haversines to get angle C of spherical triangle.
    angle = degrees(2 * asin(sqrt(
        (sin(c * 0.5) ** 2 - sin((a - b) * 0.5) ** 2) /
        (sin(a) * sin(b))
    )))

    # get direction of "out-of-plane" component of cross product => left or right turn
    ai, aj = lng2 - lng1, lat2 - lat1
    bi, bj = lng3 - lng2, lat3 - lat2
    left_turn = ai * bj - aj * bi > 0
    return (1 if left_turn else -1) * (180 - angle)
