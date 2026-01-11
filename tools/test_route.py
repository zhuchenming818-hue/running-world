import json

# 打开 JSON 文件
with open("nanjing_beijing_route.json", "r", encoding="utf-8") as f:
    route_data = json.load(f)

# 取出坐标
coords = route_data["coordinates"]

print("坐标点数量:", len(coords))
print("起点:", coords[0])
print("终点:", coords[-1])
