import json
import math

IN_PATH = "nanjing_beijing_route_with_dist.json"
STEP_KM = 0.5  # 你可以改成 0.2/0.3/0.5/1.0，建议先 0.5 试手感
OUT_PATH = f"nanjing_beijing_nodes_{str(STEP_KM).replace('.','p')}km.json"

def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t

def build_nodes(points, step_km: float):
    """
    points: 来自 route_with_dist.json 的 points（每个点含 lon/lat/dist_km）
    返回：等距 nodes（每 step_km 生成一个点），包含 lon/lat/dist_km/index
    """
    if not points or len(points) < 2:
        raise ValueError("points 至少要有 2 个点")

    total_km = float(points[-1]["dist_km"])
    if total_km <= 0:
        raise ValueError("total_km 必须 > 0")

    nodes = []

    # 我们从 0 开始，每 step_km 取一个目标距离
    target = 0.0
    seg_i = 0  # 当前所在原始折线段 index（points[seg_i] -> points[seg_i+1]）

    while target <= total_km:
        # 推进到包含 target 的那一段
        while seg_i < len(points) - 2 and float(points[seg_i + 1]["dist_km"]) < target:
            seg_i += 1

        p1 = points[seg_i]
        p2 = points[seg_i + 1]
        d1 = float(p1["dist_km"])
        d2 = float(p2["dist_km"])

        # 处理极少数情况下 d2==d1（重复点）
        if d2 <= d1:
            t = 0.0
        else:
            t = (target - d1) / (d2 - d1)
            t = max(0.0, min(1.0, t))

        lon = lerp(float(p1["lon"]), float(p2["lon"]), t)
        lat = lerp(float(p1["lat"]), float(p2["lat"]), t)

        nodes.append({
            "index": len(nodes),
            "lon": lon,
            "lat": lat,
            "dist_km": round(target, 4)
        })

        target = round(target + step_km, 10)  # 避免浮点累计误差

    # 确保最后一个节点就是终点（有时 target 刚好越过导致差一点点）
    if nodes[-1]["dist_km"] != round(total_km, 4):
        end = points[-1]
        nodes.append({
            "index": len(nodes),
            "lon": float(end["lon"]),
            "lat": float(end["lat"]),
            "dist_km": round(total_km, 4)
        })

    return nodes, total_km

if __name__ == "__main__":
    with open(IN_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    points = data["points"]
    nodes, total_km = build_nodes(points, STEP_KM)

    out = {
        "name": data.get("name", "Nanjing to Beijing"),
        "step_km": STEP_KM,
        "total_km": round(total_km, 4),
        "node_count": len(nodes),
        "nodes": nodes
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("完成 ✅")
    print("输出文件:", OUT_PATH)
    print("总里程:", round(total_km, 2), "km")
    print("节点数:", len(nodes))
    print("首节点:", nodes[0])
    print("末节点:", nodes[-1])
