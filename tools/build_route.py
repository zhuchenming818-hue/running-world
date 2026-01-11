# tools/build_route.py
import json, os, math, requests
from typing import List, Tuple

def haversine_km(a: Tuple[float,float], b: Tuple[float,float]) -> float:
    # a,b = (lat, lon)
    R = 6371.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 2*R*math.asin(math.sqrt(h))

def load_meta(routes_dir: str, rid: str) -> dict:
    path = os.path.join(routes_dir, rid, "meta.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def osrm_route(profile: str, waypoints: List[dict]) -> dict:
    # OSRM expects lon,lat order in path
    coords = ";".join([f"{p['lon']},{p['lat']}" for p in waypoints])
    url = f"https://router.project-osrm.org/route/v1/{profile}/{coords}"
    params = {"overview":"full", "geometries":"geojson", "steps":"false"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def build_with_dist(geo_coords_lonlat: List[List[float]]) -> dict:
    # geojson: [lon, lat]
    coords = geo_coords_lonlat
    out = []
    cum = 0.0
    prev = None
    for lon, lat in coords:
        if prev is not None:
            cum += haversine_km((prev[1], prev[0]), (lat, lon))
        out.append({"lon": lon, "lat": lat, "cum_km": round(cum, 4)})
        prev = (lon, lat)
    return {"points": out, "total_km": round(cum, 3)}

def sample_nodes(points: List[dict], spacing_km: float) -> dict:
    # points: [{"lon","lat","cum_km"}...]
    if not points:
        return {"spacing_km": spacing_km, "nodes": []}
    nodes = [points[0]]
    next_km = spacing_km
    i = 1
    while next_km <= points[-1]["cum_km"] and i < len(points):
        # move i until cum_km >= next_km
        while i < len(points) and points[i]["cum_km"] < next_km:
            i += 1
        if i >= len(points):
            break
        nodes.append(points[i])
        next_km += spacing_km
    if nodes[-1]["cum_km"] != points[-1]["cum_km"]:
        nodes.append(points[-1])
    return {"spacing_km": spacing_km, "nodes": nodes, "total_km": points[-1]["cum_km"]}

def main():
    import sys
    rid = sys.argv[1] if len(sys.argv) > 1 else None
    if not rid:
        print("Usage: python tools/build_route.py <route_id>")
        return

    routes_dir = "routes"
    meta = load_meta(routes_dir, rid)

    profile = meta.get("profile", "foot")
    spacing = float(meta.get("node_spacing_km", 0.5))
    waypoints = meta["waypoints"]

    osrm = osrm_route(profile, waypoints)
    geom = osrm["routes"][0]["geometry"]  # geojson linestring
    geo = {
        "route_id": rid,
        "name": meta.get("name", rid),
        "profile": profile,
        "geometry": geom
    }

    route_dir = os.path.join(routes_dir, rid)
    os.makedirs(route_dir, exist_ok=True)

    # 1) route_geo.json
    with open(os.path.join(route_dir, "route_geo.json"), "w", encoding="utf-8") as f:
        json.dump(geo, f, ensure_ascii=False, indent=2)

    # 2) route_with_dist.json
    with_dist = build_with_dist(geom["coordinates"])
    with open(os.path.join(route_dir, "route_with_dist.json"), "w", encoding="utf-8") as f:
        json.dump(with_dist, f, ensure_ascii=False, indent=2)

    # 3) nodes_0p5km.json (spacing configurable)
    nodes = sample_nodes(with_dist["points"], spacing)
    with open(os.path.join(route_dir, f"nodes_{str(spacing).replace('.','p')}km.json"), "w", encoding="utf-8") as f:
        json.dump(nodes, f, ensure_ascii=False, indent=2)

    print(f"✅ Built route {rid}: total {with_dist['total_km']} km, nodes {len(nodes['nodes'])}")

if __name__ == "__main__":
    main()
