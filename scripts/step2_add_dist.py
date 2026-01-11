import json
import math

def haversine_km(lon1, lat1, lon2, lat2) -> float:
    """
    计算地球表面两点间距离（公里）
    输入是经纬度（度），OSRM 坐标顺序为 [lon, lat]
    """
    R = 6371.0088  # 地球平均半径（km）
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# 1) 读取原路线
in_path = "nanjing_beijing_route.json"
with open(in_path, "r", encoding="utf-8") as f:
    route = json.load(f)

coords = route["coordinates"]
if not isinstance(coords, list) or len(coords) < 2:
    raise ValueError("coordinates 必须是至少包含 2 个点的数组")

# 2) 计算累计距离
points = []
total_km = 0.0

# 第一个点 dist=0
lon0, lat0 = coords[0]
points.append({"lon": lon0, "lat": lat0, "dist_km": 0.0})

for i in range(1, len(coords)):
    lon1, lat1 = coords[i - 1]
    lon2, lat2 = coords[i]
    seg_km = haversine_km(lon1, lat1, lon2, lat2)
    total_km += seg_km
    points.append({"lon": lon2, "lat": lat2, "dist_km": round(total_km, 4)})

# 3) 保存新路线文件
out_path = "nanjing_beijing_route_with_dist.json"
out = {
    "name": route.get("name", "Nanjing to Beijing"),
    "distance_km_est": round(total_km, 2),  # 根据坐标累计估算的总里程
    "source_distance_km": route.get("distance_km", None),  # 你原文件里写的 distance_km（如果有）
    "points": points
}

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

# 4) 打印检查
print("完成 ✅")
print("点数:", len(points))
print("累计总里程(估算):", out["distance_km_est"], "km")
print("起点:", points[0])
print("终点:", points[-1])
