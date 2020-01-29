from collections import defaultdict


class RouteGraph:
    def __init__(self, vertex_docs, edge_docs, waypoints):
        self.waypoints = waypoints

        self.edges = defaultdict(dict)
        for doc in edge_docs:
            v1, v2 = doc["v"]
            self.edges[v1][v2] = doc["d"]
            self.edges[v2][v1] = doc["d"]
        self.vertices = {}
        for doc in vertex_docs:
            v = doc["_id"]
            self.vertices[v] = {"coords": doc["loc"]["coordinates"]}
