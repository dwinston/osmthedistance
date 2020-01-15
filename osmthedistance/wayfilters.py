import sys


def running_okay(way):
    if "tag" not in way:
        return False

    foot_allowed = False
    for item in way["tag"]:
        key, val = item["k"], item["v"]
        if key == "highway":
            if val in (
                    "cycleway", "path", "footway", "steps", "pedestrian",
                    "primary", "primary_link", "secondary", "tertiary", "unclassified",
                    "residential", "living_street", "road", "service", "track",
            ):
                foot_allowed = True
            elif val in ("motorway", "motorway_link", "trunk", "trunk_link"):
                foot_allowed = False
        elif key in ("pedestrian", "foot"):
            if val in ("yes", "designated", "permissive", "use_sidepath", "destination"):
                foot_allowed = True
            elif val in ("no", "private", "future"):
                foot_allowed = False
            else:
                print(f"running_okay: don't know how to handle "
                      f"val {val} for tag key {key} in way {way['id']}"
                      , file=sys.stderr)
        elif key == "access" and val in ("no", "private"):
            return False
    return foot_allowed
