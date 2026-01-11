import json
import bisect

ROUTE_PATH = "nanjing_beijing_route_with_dist.json"

def load_route(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    points = data["points"]  # 每个点：lon, lat, dist_km
    dists = [p["dist_km"] for p in points]
    total_km = dists[-1] if dists else 0.0
    return data, points, dists, total_km

def locate_by_distance(points, dists, current_km: float):
    """
    返回：index, point（lon/lat/dist_km）, progress(0~1)
    规则：取 dist_km <= current_km 的最后一个点（不超过当前里程）
    """
    if not points:
        raise ValueError("路线 points 为空")

    total_km = dists[-1]
    # clamp 到 [0, total_km]
    current_km = max(0.0, min(float(current_km), total_km))

    # 二分：找到插入位置
    idx = bisect.bisect_right(dists, current_km) - 1
    idx = max(0, min(idx, len(points) - 1))

    p = points[idx]
    progress = current_km / total_km if total_km > 0 else 0.0
    return idx, p, progress, current_km, total_km

if __name__ == "__main__":
    data, points, dists, total_km = load_route(ROUTE_PATH)

    print("路线:", data.get("name"))
    print("总里程(km):", round(total_km, 2))

    # 你可以在这里改成任意数测试
    test_kms = [300]

    for km in test_kms:
        idx, p, prog, km_clamped, total = locate_by_distance(points, dists, km)
        print("-" * 40)
        print(f"输入累计里程: {km} km  ->  clamp后: {km_clamped:.2f} km")
        print(f"位置索引: {idx} / {len(points)-1}")
        print(f"当前位置: lon={p['lon']:.6f}, lat={p['lat']:.6f}, dist_km={p['dist_km']:.3f}")
        print(f"进度: {prog*100:.2f}%")
