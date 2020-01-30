from collections import deque

from haversine import haversine, Unit
import networkx as nx


class RouteGraph:
    def __init__(self, vertex_docs, edge_docs):

        self.G = nx.Graph()
        self.G.add_nodes_from([
            (d["_id"], {"coords": d["loc"]["coordinates"]})
            for d in vertex_docs
        ])
        self.G.add_edges_from([
            (d["v"][0], d["v"][1], {"weight": d["d"], "d": d["d"]})
            for d in edge_docs
        ])

    def neighbors(self, me):
        return [{"id": n, "coords": self.G.nodes[n]['coords']} for n in self.G.neighbors(me)]

    def routes(self, goal_distance, waypoints, tolerance=0.2, max_turns=10, turn_angle=60, turn_radius=15.24):
        """
        Get routes that follow waypoints and that meet the goal distance within tolerance and max_turns.

        Args:
            goal_distance: goal distance, in miles.
            waypoints: list of dicts with node id as "id" value and optional distance as "d" value.
                The "d" values for the first and last points represent distances from real-life origin
                and destination points to available nodes in the graph.
            tolerance: fractional tolerance for goal distance. Default is 0.2, i.e. +/- 20% of distance.
            max_turns: the maximum number of turns a satisfying route may include.
            turn_angle: in degrees. Default is +/- 60 degrees (i.e. left or right) relative to previous heading.
            turn_radius: in meters. Default is 15.24m, i.e. 50ft.

        Returns:
            list: list of Route objects that satisfy the constraints
        """
        if not all(p['id'] in self.G for p in waypoints):
            raise Exception("Waypoint ids are not node ids.")
        if len(waypoints) < 2:
            raise Exception("Need at least two waypoints.")

        # Goals:
        # - Hit waypoints in order. Prune if hit later waypoint first.
        # - Hit distance within tolerance. Prune if distance > (goal_distance + tolerance).
        # - At most max_turns turns. Prune if n_turns > max_turns.

        added_distance = waypoints[0].get("d", 0) + waypoints[-1].get("d", 0)
        min_distance = (goal_distance - added_distance) * (1 - tolerance)
        max_distance = (goal_distance - added_distance) * (1 + tolerance)

        waypoint_ids = [w["id"] for w in waypoints]
        start_node = {"id": waypoint_ids[0], "coords": self.G.nodes[waypoint_ids[0]]["coords"]}
        completed = []
        considering = deque([Route(nodes=[start_node], waypoints_hit=[start_node])])
        while len(considering):
            route = considering.popleft()
            routes = self.extend_by_one_node(route)
            # Determine which routes will be enqueued, discarded, or added to completed
            for new_r in routes:
                if new_r.nodes[-1]["id"] == waypoints[len(new_r.waypoints_hit)]["id"]:
                    # TODO stuff
                    pass

    def extend_by_one(self, route):
        routes = []
        last_lat_lon = route.nodes[-1]["coords"][::-1]
        for node, coords in self.neighbors(route.nodes[-1]):
            # update distance
            distance = route.distance + haversine(last_lat_lon, coords[::-1], unit=Unit.METERS)
            # update waypoints_hit

            # update n_turns


class Route:
    def __init__(self, nodes, distance=0, n_turns=0, waypoints_hit=None):
        self.nodes = nodes
        self.distance = distance
        self.n_turns = n_turns
        self.waypoints_hit = waypoints_hit or []
