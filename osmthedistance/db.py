from collections import defaultdict
from operator import itemgetter

from pymongo import GEOSPHERE, MongoClient
from tqdm import tqdm

from osmthedistance.util import (
    distance_along, to_geojson, process_in_chunks, remdups
)


class Mongo:
    def __init__(self, connection_uri="mongodb://localhost/admin", dbname="osm"):
        self._client = MongoClient(connection_uri)
        self.db = self._client[dbname]

    def filter_ways(self, predicate, save_to_db=True):
        total = self.db.way.estimated_document_count()
        ways = []
        for way in tqdm(self.db.way.find({}, ["id", "tag"]), total=total):
            if predicate(way):
                ways.append(way["id"])
        print(f"{len(ways)} ({len(ways) / total:.0%}) ways are okay for running!")
        if save_to_db:
            collname = f"way_{predicate.__name__}"
            self.db.drop_collection(collname)
            self.db[collname].insert_many([{"_id": wid} for wid in ways])
            print(f"Saved to db collection '{collname}'")

    def intersection_nodes(self, predicate, save_to_db=True):
        collname = f"way_{predicate.__name__}"
        total = self.db[collname].estimated_document_count()
        if total == 0:
            raise Exception("Cannot find collection of ways filtered by predicate. Call `filter_ways` first.")
        print("Ensuring index on database way 'id' field...")
        self.db.way.create_index("id")
        print("Aggregating ids of nodes appearing in more than one way...")
        intersection_nodes = {o["_id"]: o["count"] for o in list(self.db.way.aggregate([
            {"$match": {"id": {"$in": self.db[collname].distinct("_id")}}},
            {"$project": {"nd": 1}},
            {"$unwind": "$nd"},
            {"$group": {"_id": "$nd.ref", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
        ], allowDiskUse=True))}
        if save_to_db:
            collname = f"node_{predicate.__name__}"
            self.db.drop_collection(collname)
            self.db[collname].insert_many([{"_id": nid} for nid in intersection_nodes])
            print(f"Saved to db collection '{collname}'")
        return intersection_nodes

    def build_graph(self, predicate):
        """Build graph (vertices+edges) for ways filtered by predicate."""
        # Use intersection nodes to obtain the vertices.
        collname = f"node_{predicate.__name__}"
        total = self.db[collname].estimated_document_count()
        if total == 0:
            raise Exception("Cannot find collection of intersection nodes for ways filtered by predicate. "
                            "Call (`filter_ways` followed by) `intersection_nodes`, then try again.")
        vertex_ids = set(self.db[collname].distinct("_id"))
        print("Ensuring index on database node 'id' field...")
        self.db.node.create_index("id")

        def _process(chunk):
            return to_geojson(self.db.node.find({"id": {"$in": chunk}}, ["id", "lat", "lon"]))
        print(f"Fetching lat/long info for vertices...")
        docs = process_in_chunks(list(vertex_ids), _process)
        vertex_coll = self.db[f"vertex_{predicate.__name__}"]
        vertex_coll.drop()
        vertex_coll.insert_many(docs)
        print(f"Saved vertices to db collection {vertex_coll.name}")
        print(f"Creating index to efficiently query vertices by geolocation...")
        vertex_coll.create_index([("loc", GEOSPHERE)])

        # Iterate over all predicate-ways to obtain inter-vertex distances (edge weights) along each way
        edges = []
        way_ids = self.db[f"way_{predicate.__name__}"].distinct("_id")
        print("Finding edges and computing their weights by Haversine formula...")
        for way in tqdm(self.db.way.find({"id": {"$in": way_ids}}, ["nd.ref"]), total=len(way_ids)):
            node_ids = [o["ref"] for o in way["nd"]]
            nodes_unsorted = to_geojson(self.db.node.find({"id": {"$in": node_ids}}, ["id", "lat", "lon"]))
            _nodes_by_id = {n["_id"]: n for n in nodes_unsorted}
            nodes = [_nodes_by_id[nid] for nid in node_ids]
            last_node_id = None
            for nid in node_ids:
                if nid in vertex_ids:
                    if last_node_id is not None:
                        edges.append((last_node_id, nid, distance_along(last_node_id, nid, nodes)))
                        last_node_id = nid
                    else:
                        last_node_id = nid
        edge_coll = self.db[f"edge_{predicate.__name__}"]
        edge_coll.drop()
        edge_coll.insert_many([{"v": [start_nid, end_nid], "d": distance} for start_nid, end_nid, distance in edges])
        print(f"Saved edges to db collection {edge_coll.name}")
        edge_coll.create_index("v")
        print(f"Creating index to efficiently query edges by vertices...")

    def subgraph_docs(self, predicate, points, max_distance):
        """
        Get all vertex and edge docs satisfying predicate along possible routes.

        A route is considered possible if one can get from start_point to end_point in at most max_distance. For
        example, if start_point == end_point, all vertices within a radius of (max_distance / 2), along with all edges
        connecting these vertices, would be returned.

        If start_point != end_point, it is sufficient to check if a candidate vertex is in the union of
        half-maxdistance-radius disks surrounding each of start_point and end_point. Suppose a vertex was not. Then it
        represents travel more than half-maxdistance from start_point and more than half-maxdistance remains to get to
        end_point.

        points should be a sequence of 2-tuples of the form (longitude, latitude) where
        -180 <= longitude <= 180 and -90 <= latitude <= 90.
        max_distance should be given in miles.

        """
        gj_points = [{"loc": {"type": "Point", "coordinates": p}, "_id": id(p)} for p in points]
        if distance_along(gj_points[0], gj_points[-1], gj_points) > max_distance:
            raise Exception("start_point and end_point are greater than max_distance apart!")

        vertex_coll = self.db[f"vertex_{predicate.__name__}"]
        edge_coll = self.db[f"edge_{predicate.__name__}"]
        vertex_docs = []
        waypoints = []
        for p, gjp in zip(points, gj_points):
            docs = list(vertex_coll.find({"loc": {"$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": p},
                "$maxDistance": (max_distance / 2) * 1609.34  # miles to meters
            }}}))
            try:
                waypoints.append({
                    "id": docs[0]["_id"],
                    "d": distance_along(gjp["_id"], docs[0]["_id"], [gjp, docs[0]]),
                })
            except IndexError:
                raise Exception(f"Zero vertices found within {max_distance} miles of {p}!")
            vertex_docs.extend(docs)
        vids = {v["_id"] for v in vertex_docs}
        vertex_docs = remdups(vertex_docs, key=itemgetter("_id"))
        edge_docs = []
        for doc in edge_coll.find({"v": {"$in": list(vids)}}):
            edge_vids = set(doc["v"])
            if edge_vids & vids == edge_vids:  # both edge vertices in vids
                del doc["_id"]
                edge_docs.append(doc)
        return vertex_docs, edge_docs, waypoints
