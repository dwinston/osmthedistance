from pymongo import MongoClient
from tqdm import tqdm


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

    def intersection_nodes(self, predicate):
        collname = f"way_{predicate.__name__}"
        total = self.db[collname].estimated_document_count()
        if total == 0:
            raise Exception("Cannot find collection of ways filtered by predicate. Call `filter_ways` first.")
        print("Ensuring index on database way 'id' field...")
        self.db.way.create_index("id")
        print("Aggregating ids of nodes appearing in more than one way...")
        intersection_nodes = list(self.db.way.aggregate([
            {"$match": {"id": {"$in": self.db[collname].distinct("_id")}}},
            {"$project": {"nd": 1}},
            {"$unwind": "$nd"},
            {"$group": {"_id": "$nd.ref", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
        ], allowDiskUse=True))
        return {o["_id"]: o["count"] for o in intersection_nodes}

    def build_graph(self, predicate):
        # TODO build graph (vertices+edges) for ways filtered by predicate.
        #  1. Use `intersection_nodes` to obtain the vertices.
        #  2. Iterate over all predicate-ways to obtain inter-vertex distances (edge weights) along each way.
        #  3. Save built graph to db.
        pass

