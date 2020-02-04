from collections import deque
from typing import List

from haversine import haversine, Unit
import networkx as nx

from osmthedistance.util import triplewise, surface_turn_angle, pairwise


class Route:
    def __init__(self, nodes, distance=0, overlap=0, n_turns=0, entered_turn=False, next_waypoint_idx=None):
        self.nodes = nodes
        self.distance = distance
        self.overlap = overlap
        self.entered_turn = entered_turn
        self.n_turns = n_turns
        self.next_waypoint_idx = next_waypoint_idx

    def __repr__(self):
        return str(self.__dict__)


class RouteGraph:
    def __init__(self, vertex_docs, edge_docs, goal_distance, waypoints,
                 goal_tolerance=0.1, max_overlap_fraction=0.1, max_turns=10, turn_angle=60, turn_radius=30.48):
        """
        Construct routes that follow waypoints and that meet the goal distance within tolerance and max_turns.

        Args:
            goal_distance: goal distance, in miles.
            waypoints: list of dicts with node id as "id" value and optional distance as "d" value.
                The "d" values for the first and last points represent distances from real-life origin
                and destination points to available nodes in the graph.
            goal_tolerance: tolerance for goal distance, in miles. Default is 0.1, i.e. +/- 0.1 miles.
            max_overlap_fraction: the maximum fractional overlap in route distance. Default is 0.1, i.e. 10% of the
                total route path may be repeated. For example, a 5-mile route with a 0.5-mile stretch along a path that
                has already been run (1 mile total back-and-forth on that path), has an overlap of 0.1 == (0.5 / 5).
            max_turns: the maximum number of turns a satisfying route may include.
            turn_angle: in degrees. Default is +/- 60 degrees (i.e. left or right) relative to previous heading.
            turn_radius: in meters. Default is 30.48m, i.e. 100ft.
        """
        self.G = nx.Graph()
        self.G.add_nodes_from([
            (d["_id"], {"coords": d["loc"]["coordinates"][::-1]})  # switch from (lon, lat) to (lat, lon)
            for d in vertex_docs
        ])
        self.G.add_edges_from([
            (d["v"][0], d["v"][1], {"weight": d["d"], "d": d["d"]})
            for d in edge_docs
        ])

        if not all(p['id'] in self.G for p in waypoints):
            raise Exception("Waypoint ids are not node ids.")
        if len(waypoints) < 2:
            raise Exception("Need at least two waypoints.")

        added_distance = waypoints[0].get("d", 0) + waypoints[-1].get("d", 0)
        self.min_distance = (1609.34 * goal_distance - added_distance) - 1609.34 * goal_tolerance
        self.max_distance = (1609.34 * goal_distance - added_distance) + 1609.34 * goal_tolerance
        self.waypoint_ids = [w["id"] for w in waypoints]
        # TODO instead of n_turns, turns_per_mile.

        self.goal_distance = goal_distance
        self.waypoints = waypoints
        self.goal_tolerance = goal_tolerance
        self.max_overlap_fraction = max_overlap_fraction
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
        # - Hit distance within tolerance. Prune when possible.
        # - Hit distance within max overlap fraction. Prune when possible.
        # - At most max_turns turns. Prune when possible.
        # - No "crossings", i.e. entering the same node twice. However, the first and last waypoints can be the same.
        #       Prune when possible.
        # - Hit waypoints in order.OK if e.g.spec is [a, b, c] and actual order hit is [a, c, b, c].

        completed = []
        last_waypoint_lat_lon = self.lat_lon(self.waypoint_ids[-1])
        considering = deque([Route(nodes=[self.waypoint_ids[0]], next_waypoint_idx=1)])
        nconsidering_threshold = 10000
        while len(considering):
            if len(considering) >= nconsidering_threshold:
                print("considering", len(considering), "routes")
                nconsidering_threshold += 10000
            route = considering.popleft()
            routes = self.extend_by_one(route)
            # Determine which routes will be rejected, added to completed, or enqueued for further extension.
            for r in routes:
                min_distance_to_go = haversine(self.lat_lon(r.nodes[-1]), last_waypoint_lat_lon, unit=Unit.METERS)
                if ((r.distance + min_distance_to_go > self.max_distance) or
                        (r.overlap > (self.max_overlap_fraction * self.max_distance)) or
                        (r.next_waypoint_idx is not None and len(r.nodes) != len(set(r.nodes))) or
                        (r.n_turns > self.max_turns)):
                    continue
                elif r.next_waypoint_idx is None:
                    if r.distance > self.min_distance:
                        completed.append(r)
                        print("completed", len(completed), "routes")
                else:
                    considering.append(r)

            if len(completed) >= 100:
                break
        return completed

    def extend_by_one(self, route) -> List[Route]:
        routes = []
        last_node = route.nodes[-1]
        last_lat_lon = self.lat_lon(last_node)
        for n in self.neighbors(last_node):
            # The below condition could happen for a loop way with no intersections, such as a short loop in a park.
            # However, it would be difficult to show the resulting route on a map. Thus, I don't consider such a route
            # as valid. Were this decision to be revisited, take care to filter out adjacent duplicate nodes for
            # entering/exiting turns, as `surface_turn_angle` expects no adjacent duplicate nodes.
            if n == last_node:
                continue

            nodes = route.nodes + [n]
            # update distance
            distance_added = haversine(last_lat_lon, self.lat_lon(n), unit=Unit.METERS)
            distance = route.distance + distance_added
            # update overlap
            overlap = route.overlap
            if tuple(sorted([last_node, n])) in {tuple(sorted([n1, n2])) for n1, n2 in pairwise(route.nodes)}:
                overlap += distance_added
            # update next_waypoint_id
            if n == self.waypoint_ids[route.next_waypoint_idx]:
                next_waypoint_idx = (None if (route.next_waypoint_idx + 1 == len(self.waypoint_ids))
                                     else (route.next_waypoint_idx + 1))
            else:
                next_waypoint_idx = route.next_waypoint_idx
            # update entered_turn
            entered_turn = route.entered_turn
            n_turns = route.n_turns
            if not entered_turn and self.entering_turn(nodes):
                entered_turn = True
            if entered_turn and self.exiting_turn(nodes):
                entered_turn = False
                n_turns += 1
            # add without filtering
            routes.append(Route(nodes, distance, overlap, n_turns, entered_turn, next_waypoint_idx))
        return routes

    def entering_turn(self, nodes) -> bool:
        """threshold angle exceeded while under threshold distance"""
        if len(nodes) < 3:
            return False
        points = [self.lat_lon(n) for n in reversed(nodes)]
        angles = [surface_turn_angle(p1, p2, p3) for p1, p2, p3 in triplewise(points)]
        distances = [haversine(p1, p2, unit=Unit.METERS) for p1, p2 in pairwise(points)]
        distance_accum = 0
        angle_accum = 0
        for d, a in zip(distances[1:], angles):
            angle_accum += a
            if abs(angle_accum) > self.turn_angle:
                return True
            distance_accum += d
            if distance_accum > self.turn_radius:
                return False
        return False

    def exiting_turn(self, nodes) -> bool:
        """threshold distance exceeded while under threshold angle (after start point of entered turn)"""
        if len(nodes) < 3:
            return False
        points = [self.lat_lon(n) for n in reversed(nodes)]
        angles = [surface_turn_angle(p1, p2, p3) for p1, p2, p3 in triplewise(points)]
        distances = [haversine(p1, p2, unit=Unit.METERS) for p1, p2 in pairwise(points)]
        distance_accum = distances[0]
        angle_accum = 0
        for d, a in zip(distances[1:], angles):
            if distance_accum > self.turn_radius:
                return True
            angle_accum += a
            if abs(angle_accum) > self.turn_angle:
                return False
            distance_accum += d
        return False
