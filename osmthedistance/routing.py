from collections import deque

from haversine import haversine, Unit
import networkx as nx

from osmthedistance.util import triplewise, surface_turn_angle, pairwise


class RouteGraph:
    def __init__(self, vertex_docs, edge_docs, goal_distance, waypoints,
                 goal_tolerance=0.2, max_turns=10, turn_angle=60, turn_radius=15.24):
        """
        Construct routes that follow waypoints and that meet the goal distance within tolerance and max_turns.

        Args:
            goal_distance: goal distance, in miles.
            waypoints: list of dicts with node id as "id" value and optional distance as "d" value.
                The "d" values for the first and last points represent distances from real-life origin
                and destination points to available nodes in the graph.
            goal_tolerance: fractional tolerance for goal distance. Default is 0.2, i.e. +/- 20% of distance.
            max_turns: the maximum number of turns a satisfying route may include.
            turn_angle: in degrees. Default is +/- 60 degrees (i.e. left or right) relative to previous heading.
            turn_radius: in meters. Default is 15.24m, i.e. 50ft.
        """
        if not all(p['id'] in self.G for p in waypoints):
            raise Exception("Waypoint ids are not node ids.")
        if len(waypoints) < 2:
            raise Exception("Need at least two waypoints.")

        self.G = nx.Graph()
        self.G.add_nodes_from([
            (d["_id"], {"coords": d["loc"]["coordinates"][::-1]})  # switch from (lon, lat) to (lat, lon)
            for d in vertex_docs
        ])
        self.G.add_edges_from([
            (d["v"][0], d["v"][1], {"weight": d["d"], "d": d["d"]})
            for d in edge_docs
        ])

        added_distance = waypoints[0].get("d", 0) + waypoints[-1].get("d", 0)
        self.min_distance = (goal_distance - added_distance) * (1 - goal_tolerance)
        self.max_distance = (goal_distance - added_distance) * (1 + goal_tolerance)
        self.waypoint_ids = [w["id"] for w in waypoints]

        self.goal_distance = goal_distance
        self.waypoints = waypoints
        self.goal_tolerance = goal_tolerance
        self.max_turns = max_turns
        self.turn_angle = turn_angle
        self.turn_radius = turn_radius

    def neighbors(self, me):
        return [n for n in self.G.neighbors(me)]

    def lat_lon(self, me):
        return self.G.nodes[me]['coords']

    def routes(self):
        """
        Get routes that follow waypoints and that meet the goal distance within tolerance and max_turns.

        Returns:
            list: list of Route objects that satisfy the constraints
        """

        # Goals:
        # - Hit waypoints in order. Prune if hit later waypoint first.
        # - Hit distance within tolerance. Prune if distance > (goal_distance + tolerance).
        # - At most max_turns turns. Prune if n_turns > max_turns.

        completed = []
        considering = deque([Route(nodes=[self.waypoint_ids[0]], next_waypoint_idx=1)])
        while len(considering):
            route = considering.popleft()
            routes = self.extend_by_one_node(route)
            # Determine which routes will be enqueued, discarded, or added to completed
            for new_r in routes:
                if new_r.next_waypoint_idx is None:
                    # TODO stuff
                    pass

    def extend_by_one(self, route):
        routes = []
        last_lat_lon = self.lat_lon(route.nodes[-1])
        for n in self.neighbors(route.nodes[-1]):
            nodes = route.nodes + [n]
            distance = route.distance + haversine(last_lat_lon, self.lat_lon(n), unit=Unit.METERS)
            if n == self.waypoint_ids[route.next_waypoint_idx]:
                next_waypoint_idx = (None if (route.next_waypoint_idx + 1 == len(self.waypoint_ids))
                                     else (route.next_waypoint_idx + 1))
            else:
                next_waypoint_idx = route.next_waypoint_idx
            entered_turn = route.entered_turn
            n_turns = route.n_turns
            if entered_turn and self.exiting_turn(nodes):
                entered_turn = False
                n_turns += 1
            elif not entered_turn and self.entering_turn(nodes):
                entered_turn = True
            routes.append(nodes, distance, n_turns, entered_turn, next_waypoint_idx)

    def entering_turn(self, nodes) -> bool:
        if len(nodes) < 3:
            return False
        points = [self.lat_lon(n) for n in reversed(nodes)]
        angles = [surface_turn_angle(p1, p2, p3) for p1, p2, p3 in triplewise(points)]
        distances = [haversine(p1, p2, unit=Unit.METERS) for p1, p2 in pairwise(points)]
        distance_accum = distances[0]
        angle_accum = 0
        for d, a in zip(distances[1:], angles):
            distance_accum += d
            angle_accum += a
            if distance_accum > self.turn_radius:
                return False
            elif abs(angle_accum) > self.turn_angle:
                return True
        return False

    def exiting_turn(self, nodes) -> bool:
        if len(nodes) < 3:
            return False
        points = [self.lat_lon(n) for n in reversed(nodes)]
        angles = [surface_turn_angle(p1, p2, p3) for p1, p2, p3 in triplewise(points)]
        distances = [haversine(p1, p2, unit=Unit.METERS) for p1, p2 in pairwise(points)]
        distance_accum = distances[0]
        angle_accum = 0
        for d, a in zip(distances[1:], angles):
            distance_accum += d
            angle_accum += a
            if abs(angle_accum) > self.turn_angle:
                return False
            elif distance_accum > self.turn_radius:
                return True
        return False


class Route:
    def __init__(self, nodes, distance=0, n_turns=0, entered_turn=False, next_waypoint_idx=None):
        self.nodes = nodes
        self.distance = distance
        self.entered_turn = entered_turn
        self.n_turns = n_turns
        self.next_waypoint_idx = next_waypoint_idx
