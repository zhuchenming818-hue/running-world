import os
import pydeck as pdk
import pandas as pd
import json
import bisect
import streamlit as st
import requests
import time
import math
import uuid
import textwrap
import base64
import hashlib
import hmac
import streamlit.components.v1 as components
from streamlit_js_eval import streamlit_js_eval
from openai import OpenAI
from storage import load_data, save_data, add_run_km, check_pro_completion, add_run_km_pro
from storage import recompute_profile, delete_runs_by_date, load_invites, save_invites, ensure_access_state, FileLock
from storage import generate_reward_narrative
from datetime import date, timedelta

if "USER_ID" not in st.session_state:
    st.session_state["USER_ID"] = None

# ---- Storage path (Streamlit Cloud safe) ----
# Streamlit Community Cloud 上 repo 目录可能不可写；/tmp 是可写目录
RW_STORAGE_DIR = os.getenv("RW_STORAGE_DIR", "/tmp/runningworld")

RW_SECRET = os.getenv("RW_SECRET", "") or st.secrets.get("RW_SECRET", "")
if not RW_SECRET:
    RW_SECRET = "DEV_ONLY_CHANGE_ME"

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def sign_user_id(user_id: str) -> str:
    sig = hmac.new(RW_SECRET.encode("utf-8"), user_id.encode("utf-8"), hashlib.sha256).digest()
    return f"{_b64url(user_id.encode('utf-8'))}.{_b64url(sig)}"

def verify_token(token: str) -> str | None:
    try:
        p1, p2 = token.split(".", 1)
        user_id = _b64url_decode(p1).decode("utf-8")
        sig = _b64url_decode(p2)
        exp = hmac.new(RW_SECRET.encode("utf-8"), user_id.encode("utf-8"), hashlib.sha256).digest()
        if hmac.compare_digest(sig, exp):
            return user_id
    except Exception:
        pass
    return None

def _extract_token(x):
    """Normalize streamlit_js_eval return value into a string token (or None)."""
    if x is None:
        return None

    # common cases: direct string
    if isinstance(x, str):
        s = x.strip()
        return s if s else None

    # sometimes list-like
    if isinstance(x, list) and x:
        return _extract_token(x[0])

    # sometimes dict-like
    if isinstance(x, dict):
        for k in ("value", "result", "data"):
            if k in x:
                return _extract_token(x.get(k))
        # fallback: if single key dict, try its first value
        if len(x) == 1:
            return _extract_token(next(iter(x.values())))

    return None

def sync_token_with_localstorage(timeout_s: float = 2.5):
    # 1) read t from URL
    t = st.query_params.get("t")
    if isinstance(t, list):
        t = t[0]

    # 2) URL has t -> save to localStorage and continue
    if isinstance(t, str) and t.strip():
        token = t.strip()
        streamlit_js_eval(
            js_expressions=f"localStorage.setItem('rw_t', '{token}')",
            key="rw_set_token",
        )
        # reset restore timers
        st.session_state.pop("_rw_ls_start", None)
        st.session_state["_rw_restored"] = True
        return

    # 3) URL has no t -> wait for localStorage to respond (do NOT fast-rerun)
    if st.session_state.get("_rw_restored"):
        return

    # start a restore window
    import time as _time
    start = st.session_state.get("_rw_ls_start")
    if not isinstance(start, (int, float)):
        start = _time.time()
        st.session_state["_rw_ls_start"] = start

    raw = streamlit_js_eval(
        js_expressions="localStorage.getItem('rw_t')",
        key="rw_get_token",   # IMPORTANT: keep key stable
    )
    ls_token = _extract_token(raw)

    if ls_token:
        st.session_state["_rw_restored"] = True
        st.query_params.clear()
        st.query_params["t"] = ls_token
        st.rerun()

    # Not received yet: give the frontend time, stop this run
    if _time.time() - start < timeout_s:
        st.info("Restoring your session… (reading browser storage)")
        st.stop()

    # timeout: allow mint
    st.session_state["_rw_restored"] = True
    st.session_state.pop("_rw_ls_start", None)
    return

def get_or_create_user_id() -> str:
    # 1) 强制只读 t（忽略 uk 等任何旧字段）
    t = st.query_params.get("t")
    if isinstance(t, list):
        t = t[0]

    if isinstance(t, str) and t.strip():
        user_id = verify_token(t.strip())
        if user_id:
            # ✅ 已有稳定身份：顺手清理掉其它 query（比如 uk）
            if len(st.query_params) != 1 or "t" not in st.query_params:
                st.query_params.clear()
                st.query_params["t"] = t.strip()
                st.rerun()
            return user_id

    # 2) 没有合法 t：mint 新身份
    user_id = "u_" + uuid.uuid4().hex
    token = sign_user_id(user_id)

    st.query_params.clear()
    st.query_params["t"] = token
    st.rerun()

    return user_id

os.makedirs(RW_STORAGE_DIR, exist_ok=True)

sync_token_with_localstorage()   # <- add this line

if st.session_state["USER_ID"] is None:
    st.session_state["USER_ID"] = get_or_create_user_id()

USER_ID = st.session_state["USER_ID"]

DATA_PATH = os.path.join(RW_STORAGE_DIR, f"run_data_{USER_ID}.json")

INVITES_PATH = os.path.join(RW_STORAGE_DIR, "invites.json")
INVITES_LOCK_PATH = INVITES_PATH + ".lock"

# --- Seed invites on first deploy (if /tmp invites empty) ---
SEED_PATH = os.path.join("data", "invites_seed.json")

def _seed_invites_if_needed():
    """
    Seed invites for both local and R2 backends.
    - If current invites is empty/missing -> write seed into the active backend (R2 or local).
    - Uses a file lock to avoid concurrent seeding.
    """
    try:
        with FileLock(INVITES_LOCK_PATH):
            cur = load_invites(INVITES_PATH)  # <-- IMPORTANT: uses backend-aware loader
            if isinstance(cur, dict) and len(cur) > 0:
                return

            with open(SEED_PATH, "r", encoding="utf-8") as f:
                seed = json.load(f)

            if isinstance(seed, dict) and len(seed) > 0:
                save_invites(INVITES_PATH, seed)  # <-- IMPORTANT: writes to R2 when backend=r2
    except Exception:
        # fail-safe: don't block app startup
        pass

_seed_invites_if_needed()

ROUTES_DIR = "routes"

# --- Phase 3.3: minimal commercialization gating ---
FREE_ROUTE_IDS = {"js_free_nj_zj", "js_free_nj_cz"}
PRO_ROUTE_IDS  = {"js_pro_nj_sz", "js_pro_nj_nt", "js_pro_nj_xz", "js_pro_nj_lyg"}
PASS_DURATION_DAYS = 365
ADMIN_TOKEN_ENV = "RW_ADMIN_TOKEN"

def load_all_routes(routes_dir: str = ROUTES_DIR) -> dict:
    routes = {}
    for rid in os.listdir(routes_dir):
        meta_path = os.path.join(routes_dir, rid, "meta.json")
        if os.path.isfile(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            routes[rid] = meta
    return routes

# ---------- 工具函数 ----------
def get_route_nodes_path(route_id: str, meta: dict) -> str:
    # 先用 meta 里的 spacing，如果没有就默认 0.5
    spacing = meta.get("node_spacing_km", 0.5)
    # 生成器输出的命名风格是 nodes_0p5km.json
    spacing_str = str(spacing).replace(".", "p")
    return os.path.join("routes", route_id, f"nodes_{spacing_str}km.json")


def build_route_summary(rid: str, meta: dict, rw_data: dict) -> dict:
    # 1) 总里程
    nodes_path = get_route_nodes_path(rid, meta)
    try:
        _, _, _, total_km = load_nodes(nodes_path)
    except Exception:
        total_km = 0.0

    # 2) 当前累计：直接从 history 汇总（最稳，不依赖 route_progress 是否已回算）
    km_done = sum(
        float(h.get("km", 0.0))
        for h in rw_data.get("history", [])
        if h.get("route_id") == rid
    )

    # 3) 百分比 + 状态
    pct = (km_done / total_km) if total_km > 0 else 0.0
    if km_done <= 1e-9:
        status = "未开始"
    elif total_km > 0 and km_done >= total_km - 1e-6:
        status = "已完成"
    else:
        status = "进行中"

    # 4) 最近一次跑步日期（按 route_id 过滤）
    last_date = None
    for h in reversed(rw_data.get("history", [])):
        if h.get("route_id") == rid:
            last_date = h.get("date")
            break

    # 5) 标题/副标题
    title = meta.get("name", rid)
    end_name = meta.get("waypoints", [{"name": "终点"}])[-1].get("name", "终点")
    subtitle = f"→ {end_name}"

    return {
        "rid": rid,
        "title": title,
        "subtitle": subtitle,
        "km_done": km_done,
        "km_total": float(total_km),
        "pct": float(pct),
        "status": status,
        "last_date": last_date,
    }

def load_route(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    points = data["points"]
    dists = [p["dist_km"] for p in points]
    total_km = dists[-1]
    return data, points, dists, total_km

def load_nodes(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    nodes = data["nodes"]

    # 兼容 dist_km / cum_km 两种命名
    key = "dist_km" if ("dist_km" in nodes[0]) else "cum_km"
    dists = [float(n[key]) for n in nodes]

    total_km = float(data.get("total_km", dists[-1]))
    step_km = float(data.get("step_km", data.get("spacing_km", 0.5)))
    data["step_km"] = step_km  # 统一给后面用

    # 统一把距离字段也塞回 dist_km（后续代码不用改太多）
    if key != "dist_km":
        for n, d in zip(nodes, dists):
            n["dist_km"] = d

    return data, nodes, dists, total_km

def _norm_city_item(x):
    """key_cities 允许是 ['滁州', ...] 或 [{'name':'滁州','km':123}, ...]"""
    if isinstance(x, str):
        return {"name": x}
    if isinstance(x, dict):
        if "name" in x:
            return x
        # 兼容 {'city': '滁州'}
        if "city" in x:
            x = dict(x)
            x["name"] = x["city"]
            return x
    return {"name": str(x)}

def _infer_city_km_from_nodes(city_name: str, nodes_data: list):
    """
    尝试在 nodes 里找到对应城市的里程位置。
    兼容字段：city / name / label / place_name 等；里程字段：dist_km / cumulative_km / km / dist
    """
    if not city_name or not nodes_data:
        return None

    name_l = str(city_name).strip().lower()

    def pick_km(n):
        for k in ("dist_km", "cumulative_km", "km", "dist"):
            v = n.get(k)
            if isinstance(v, (int, float)):
                return float(v)
        return None

    # 1) 精确匹配优先
    for n in nodes_data:
        for k in ("city", "name", "label", "place_name"):
            v = n.get(k)
            if isinstance(v, str) and v.strip().lower() == name_l:
                km = pick_km(n)
                if km is not None:
                    return km

    # 2) 再尝试包含匹配（更宽松）
    for n in nodes_data:
        for k in ("city", "name", "label", "place_name"):
            v = n.get(k)
            if isinstance(v, str) and name_l in v.strip().lower():
                km = pick_km(n)
                if km is not None:
                    return km

    return None

def build_city_stops(meta: dict, nodes_data: list, total_km: float):
    """
    输出 stops: [{'name':..., 'km':...}, ...] 且 km 单调递增。
    """
    raw = meta.get("key_cities", []) or []
    items = [_norm_city_item(x) for x in raw]
    if not items:
        return []

    stops = []
    for idx, it in enumerate(items):
        name = str(it.get("name", "")).strip()
        km = None

        # 优先用 meta 里直接给的 km（未来你想做得最稳就用这个）
        if isinstance(it.get("km"), (int, float)):
            km = float(it["km"])
        else:
            km = _infer_city_km_from_nodes(name, nodes_data)

        stops.append({"name": name, "km": km, "idx": idx})

    # 如果有缺失 km：用均匀分布补齐（V1 兜底）
    known = [s for s in stops if isinstance(s["km"], (int, float))]
    if len(known) < len(stops):
        n = len(stops)
        for i, s in enumerate(stops):
            if s["km"] is None:
                s["km"] = (float(total_km) * i / (n - 1)) if n > 1 and total_km else float(i)

    # 保证单调递增（避免匹配错导致回退）
    stops.sort(key=lambda x: x["idx"])
    last = -1e9
    for s in stops:
        if s["km"] < last:
            s["km"] = last + 0.01  # 微调保持递增
        last = s["km"]

    return [{"name": s["name"], "km": float(s["km"])} for s in stops]

def render_city_metro_line(stops: list, km_done: float, total_km: float):
    if not stops:
        return

    # 找到已解锁最后一站 & 下一站
    unlocked_idx = -1
    for i, s in enumerate(stops):
        if km_done >= s["km"] - 1e-6:
            unlocked_idx = i
    next_idx = unlocked_idx + 1 if unlocked_idx + 1 < len(stops) else None

    st.markdown("### 🚇 路线进度（城市）")

    # 下一站提示
    if next_idx is not None:
        next_city = stops[next_idx]["name"]
        delta = max(0.0, stops[next_idx]["km"] - km_done)
        st.caption(f"下一站：{next_city} · 约 {delta:.1f} km")
    else:
        st.caption("已到达终点城市，恭喜完程。")

    # 当前进度红点：按全程百分比定位
    pct = 0.0
    if total_km and total_km > 1e-9:
        pct = max(0.0, min(float(km_done) / float(total_km), 1.0))

    items_html = []
    for i, s in enumerate(stops):
        if i <= unlocked_idx:
            dot = "🟢"
        elif i == next_idx:
            dot = "🔵"
        else:
            dot = "⚪"
        items_html.append(
            f"""
            <div class="rw-stop">
              <div class="rw-dot">{dot}</div>
              <div class="rw-name">{s['name']}</div>
            </div>
            """
        )

    html = f"""
    <div class="rw-metro">
      <div class="rw-line"></div>

      <!-- 当前进度红点（连续位置） -->
      <div class="rw-progress-dot" style="left: calc(14px + {pct:.6f} * (100% - 28px));"></div>

      {''.join(items_html)}
    </div>

    <style>
      .rw-metro {{
        position: relative;
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 10px;
        padding: 10px 6px 2px 6px;
        margin: 0;
      }}

      .rw-line {{
        position: absolute;
        left: 14px;
        right: 14px;
        top: 18px;
        height: 2px;
        background: rgba(0,0,0,0.15);
        z-index: 0;
      }}

      .rw-progress-dot {{
        position: absolute;
        top: 12px;               /* 对齐到线附近 */
        width: 12px;
        height: 12px;
        border-radius: 999px;
        background: #ff3b30;     /* 红点 */
        box-shadow: 0 0 0 3px rgba(255, 59, 48, 0.25);
        transform: translateX(-50%);
        z-index: 2;
        animation: rwPulse 1.6s ease-in-out infinite;
      }}

      @keyframes rwPulse {{
        0%   {{ transform: translateX(-50%) scale(1.0); box-shadow: 0 0 0 3px rgba(255,59,48,0.22); }}
        50%  {{ transform: translateX(-50%) scale(1.15); box-shadow: 0 0 0 6px rgba(255,59,48,0.12); }}
        100% {{ transform: translateX(-50%) scale(1.0); box-shadow: 0 0 0 3px rgba(255,59,48,0.22); }}
      }}

      .rw-stop {{
        position: relative;
        z-index: 1;
        min-width: 60px;
        text-align: center;
      }}

      .rw-dot {{
        font-size: 18px;
        line-height: 18px;
        margin-bottom: 6px;
      }}

      .rw-name {{
        font-size: 12px;
        line-height: 14px;
        opacity: 0.85;
        word-break: keep-all;
        white-space: nowrap;
      }}
    </style>
    """

    components.html(textwrap.dedent(html).strip(), height=130)

def ai_city_blurb(city_name: str, route_meta: dict) -> str:
    """
    生成城市百科式简短介绍：客观、克制、旅行向。
    """
    system = (
        "你是一个严谨但有温度的旅行百科编辑。你的任务是为一个城市写一段“简短百科式介绍”。"
        "必须：客观、克制、信息密度高，但读起来轻松，避免模板化句式。"
        "禁止：编造具体数字/年份/人口/面积等难以核验的细节；禁止虚构景点。"
        "可以：提到该城市大致地理方位、历史文化气质、代表性关键词（用较通用表达），"
        "以及一句适合跑步旅行者的观感建议。"
    )

    route_name = route_meta.get("title") or route_meta.get("route_name") or ""
    user = (
        f"项目：Running World 跑步旅行\n"
        f"路线：{route_name}\n"
        f"城市：{city_name}\n\n"
        "请输出 1 段中文简介，80~140 字，尽量自然，不要像百科条目那样僵硬。"
        "最后用一个括号里的 3~6 字关键词收束（例如：‘（古城、运河、烟火气）’）。"
    )

    # TODO: 把 call_openai 换成你项目中的真实调用
    text = call_openai(system=system, user=user, max_tokens=220, temperature=0.7)
    return (text or "").strip()

def ai_city_teaser(city_name: str, route_meta: dict) -> str:
    """
    生成下一站“预告”：更短、更像预告片，不等同百科。
    """
    system = (
        "你是一个跑步旅行应用的‘下一站预告’文案编辑。"
        "输出要克制、有画面感，但不要编造具体数字/年份/人口/面积；不要虚构景点。"
        "语气像在说：再跑一段就会遇见什么。"
    )

    route_name = route_meta.get("title") or route_meta.get("route_name") or ""
    user = (
        f"项目：Running World 跑步旅行\n路线：{route_name}\n下一站城市：{city_name}\n\n"
        "请写一段中文预告，40~80 字。"
        "结尾用一个括号关键词收束（3~6字）。"
    )

    return call_openai(system=system, user=user, max_tokens=160, temperature=0.8).strip()

def get_or_generate_city_teaser(route_id: str, city_name: str, meta: dict) -> str:
    cache_key = f"city_teaser::{route_id}::{city_name}"
    cached = st.session_state.get(cache_key)
    if isinstance(cached, str) and cached.strip():
        return cached.strip()

    with st.spinner("正在生成下一站预告…"):
        teaser = ai_city_teaser(city_name=city_name, route_meta=meta)

    st.session_state[cache_key] = teaser
    return teaser

def get_or_generate_city_blurb(route_id: str, city_name: str, meta: dict) -> str:
    """
    session_state 缓存：同一路线同一城市只生成一次，后续秒开。
    """
    cache_key = f"city_blurb::{route_id}::{city_name}"
    cached = st.session_state.get(cache_key)
    if isinstance(cached, str) and cached.strip():
        return cached.strip()

    with st.spinner("正在生成城市简介…"):
        blurb = ai_city_blurb(city_name=city_name, route_meta=meta)

    st.session_state[cache_key] = blurb
    return blurb

def render_clickable_cities(stops: list, km_done: float, meta: dict, route_id: str):
    if not stops:
        return

    # 计算 unlocked_idx / next_idx
    unlocked_idx = -1
    for i, s in enumerate(stops):
        if km_done >= s["km"] - 1e-6:
            unlocked_idx = i
    next_idx = unlocked_idx + 1 if unlocked_idx + 1 < len(stops) else None

    st.markdown("#### 🏙️ 点击城市，查看小百科")

    # 注意：不再默认选中任何城市（避免“进页面就生成”）
    sel_key = f"city_selected__{route_id}"
    if sel_key not in st.session_state:
        st.session_state[sel_key] = None

    # 显示所有城市：已解锁/下一站可点，其余禁用
    cols = st.columns(min(len(stops), 6))  # 每行最多6个，太多会挤；超过会自动换行（Streamlit会往下排）
    for i, s in enumerate(stops):
        c = s["name"]

        status = "locked"
        if i <= unlocked_idx:
            status = "unlocked"
        elif next_idx is not None and i == next_idx:
            status = "next"

        disabled = (status == "locked")
        label = c
        if status == "next":
            label = f"➡️ {c}"
        elif status == "locked":
            label = f"🔒 {c}"

        # 分配到列里（循环使用列）
        with cols[i % len(cols)]:
            if st.button(label, key=f"city_btn__{route_id}__{i}__{c}", disabled=disabled):
                st.session_state[sel_key] = {"name": c, "status": status}

    chosen = st.session_state.get(sel_key)
    if not chosen:
        st.caption("提示：已解锁城市可查看百科；下一站可查看预告；未解锁城市暂不可点击。")
        return

    city_name = chosen["name"]
    status = chosen["status"]

    # 生成按钮：点击后才调用 AI（这一步才会花 token）
    gen_key = f"city_gen__{route_id}__{city_name}"
    regen_key = f"city_regen__{route_id}__{city_name}"

    colA, colB = st.columns([6, 2])

    with colA:
        if status == "unlocked":
            title = f"📌 {city_name} · 简短介绍"
            hint = "点击下方按钮生成百科简介（仅首次生成会稍慢）。"
        else:  # next
            title = f"🛰️ 下一站预告 · {city_name}"
            hint = "这是下一站的简短预告；跑到该城市后可解锁完整百科。"

        st.caption(hint)

    with colB:
        # 重新生成（仅当已经生成过才有意义，但保留也没关系）
        if st.button("🔄 重新生成", key=regen_key):
            if status == "unlocked":
                st.session_state.pop(f"city_blurb::{route_id}::{city_name}", None)
            else:
                st.session_state.pop(f"city_teaser::{route_id}::{city_name}", None)

    # 只有点“生成”才调用
    if st.button("✨ 生成内容", key=gen_key):
        if status == "unlocked":
            text = get_or_generate_city_blurb(route_id, city_name, meta)
        else:
            text = get_or_generate_city_teaser(route_id, city_name, meta)

        with st.expander(title, expanded=True):
            st.write(text)

            # 下一站额外加一句激励（不耗token）
            if status == "next":
                km_to_unlock = None
                # 尝试算距离解锁还差多少（stops里本来就有km）
                for s in stops:
                    if s["name"] == city_name:
                        km_to_unlock = max(0.0, float(s["km"]) - float(km_done))
                        break
                if km_to_unlock is not None:
                    st.caption(f"🏁 再跑约 {km_to_unlock:.1f} km 解锁「{city_name}」完整百科。")
    else:
        # 没点生成时，只显示一个收起框占位（让用户知道内容在哪出现）
        with st.expander(title, expanded=False):
            st.write("点击上方「✨ 生成内容」后，这里会出现文本。")

def locate_by_distance(points, dists, current_km: float):
    total_km = dists[-1]
    current_km = max(0.0, min(float(current_km), total_km))
    idx = bisect.bisect_right(dists, current_km) - 1
    idx = max(0, min(idx, len(points) - 1))
    p = points[idx]
    progress = current_km / total_km if total_km > 0 else 0.0
    return p, progress, current_km, total_km

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

@st.cache_data(show_spinner=False)
def reverse_geocode(lat: float, lon: float):
    """
    用 Nominatim(OSM) 逆地理编码：lat/lon -> city/state/country
    cache_data 会自动缓存结果，避免频繁请求
    """
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "jsonv2",
        "lat": lat,
        "lon": lon,
        "zoom": 10,           # 10 大致到 city 级别
        "addressdetails": 1,
        "accept-language": "zh-CN"
    }
    headers = {
        # Nominatim 要求提供 User-Agent（别用默认的 python-requests）
        "User-Agent": "RunningWorldMVP/0.1 (contact: local-demo)"
    }

    # 轻微延迟，进一步降低触发频控风险
    time.sleep(0.2)

    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()
    addr = data.get("address", {})

    # Nominatim 的“城市”字段可能落在不同键里
    city = (addr.get("city") or addr.get("town") or addr.get("county")
            or addr.get("village") or addr.get("municipality") or addr.get("state_district"))
    state = addr.get("state")
    country = addr.get("country")

    return {
        "display_name": data.get("display_name"),
        "city": city,
        "state": state,
        "country": country
    }

@st.cache_resource(show_spinner=False)
def get_openai_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        # 这里直接抛错，外层 try/except 里会显示 warning
        raise RuntimeError("未检测到环境变量 OPENAI_API_KEY")
    return OpenAI(api_key=api_key)

def call_openai(system: str, user: str, model: str = "gpt-4o-mini",
                temperature: float = 0.7, max_tokens: int = 220) -> str:
    client = get_openai_client()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content.strip()

@st.cache_data(show_spinner=False)
def generate_narration(
    route_name: str,
    km_done: float,
    km_total: float,
    city: str,
    state: str,
    country: str,
    remaining_km: float,
    milestone_hit: str | None,
    province_change: str | None,
    near_city_tip: str | None,
):
    """
    生成 3~6 句中文叙事：克制、不鸡汤、但有画面感。
    用 cache_data 缓存：同样输入不会重复调用 API。
    """
    facts = {
        "route": route_name,
        "km_done": round(km_done, 1),
        "km_total": round(km_total, 1),
        "remaining_km": round(remaining_km, 1),
        "location": f"{city} ({state} {country})",
        "milestone": milestone_hit,
        "province_change": province_change,
        "near_city": near_city_tip,
    }

    system = """
你是一位冷静、克制、但极具画面感的中文陪跑叙事者。
你理解长期跑步者的心理：他们不需要空洞的口号，而需要被放进一段真实而持续的旅程中。

你的任务是输出一段「阶段化·地域叙事式」的陪跑语音文本，整体 3–6 句，自然流动，不要分点。

请大致按照以下叙事顺序完成：

第一步【阶段定位】
用一句话判断并点出用户正处在这条路线的什么阶段，
例如刚离开起点、正在穿越中段、逐渐接近某个重要城市、进入稳定推进期等。
这句话要让人感觉“我被理解了”，而不是被统计。

第二步【地域叙事】
结合用户当前或临近城市的具体景点、地标建筑或特色风物等，进行画面化描绘。
可适当加以想象，使用类似“你也许会看到”“路旁或许会出现”的语气描绘沿途的具体场景，像是在慢跑中偶然瞥见的一幕。
优先使用 near_city（如果存在），否则使用 location 中的城市信息。。

第三步【精神转译与祝福】
从这个地方的气质或意象中，提炼出一种精神状态，
自然转化为对用户继续跑下去的鼓励或祝福。
语气真诚、平静、有陪伴感，避免口号式激励。

整体要求：
- 不要出现具体公里数、百分比或数字
- 不要重复路线名称或城市列表
- 不要空泛，不要套话
- 像是在路上与用户并肩跑着说的话
"""

    user = (
        "请根据下列事实生成陪跑播报（3~6句），输出纯文本，不要列表，不要表情符号：\n"
        f"{facts}"
    )


    return call_openai(
        system=system,
        user=user,
        model="gpt-4o-mini",
        temperature=0.5,
        max_tokens=260,
    )

# ---------- Streamlit 页面 ----------
st.set_page_config(page_title="Running World", layout="wide")

routes = load_all_routes()
if not routes:
    st.error("未找到 routes/*/meta.json")
    st.stop()

# view state
if "view" not in st.session_state:
    st.session_state.view = "picker"   # picker / main
if "active_route_id" not in st.session_state:
    st.session_state.active_route_id = None
if st.session_state.view == "picker":
    st.title("🌍 Running World")
    st.write("选择一条路线进入：")

    # 读取一次数据（用于进度/历史）
    rw_data = load_data(DATA_PATH)
    ensure_access_state(rw_data)
    save_data(DATA_PATH, rw_data)

    rw_data.setdefault("profile", {})
    rw_data["profile"].setdefault("route_progress", {})
    # --- ensure Phase 3.3 fields exist (after storage schema upgrade) ---
    prof = rw_data["profile"]
    prof.setdefault("user_id", USER_ID)
    prof.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
    prof.setdefault("pass", {"tier": "free", "status": "none", "starts_at": None, "ends_at": None, "source": "local", "notes": ""})
    prof.setdefault("entitlements", {"all_routes": False, "ai_basic": True, "ai_plus": False, "street_view": False})

    def _today():
        return date.today()

    # --- Activate pass UI ---
        # --- Activate pass UI (invites.json based) ---
    with st.expander("🔑 激活探索季票（邀请码）", expanded=False):
        code = st.text_input("输入邀请码", value="", placeholder="例如：RW-ALPHA-001")

        # 小提示：展示库存情况（可选，但很实用）
        invites = load_invites(INVITES_PATH)
        remaining = sum(1 for v in invites.values() if isinstance(v, dict) and v.get("status") == "new")
        st.caption(f"当前可用邀请码余量：{remaining}（仅你本地统计）")

        if st.button("激活", key="activate_pass"):
            code = (code or "").strip()
            if not code:
                st.error("邀请码不能为空。")
            else:
                try:
                    with FileLock(INVITES_LOCK_PATH, timeout_s=8.0):
                        # ⚠️ 进入锁后再 load 一次：确保读到的是“最新状态”
                        invites = load_invites(INVITES_PATH)
                        rec = invites.get(code)

                        if not isinstance(rec, dict):
                            st.error("邀请码不存在。")
                        elif rec.get("status") == "revoked":
                            st.error("邀请码已作废。")
                        elif rec.get("status") == "used":
                            st.error("邀请码已被使用。")
                        else:
                            # ✅ 邀请码有效：先写 invites.used（在锁里）
                            rec["status"] = "used"
                            rec["activated_at"] = date.today().isoformat()
                            invites[code] = rec
                            save_invites(INVITES_PATH, invites)

                            # ✅ 再写用户数据（DATA_PATH 是按 USER_ID 分文件的，不需要全局锁）
                            prof["auth"]["mode"] = "invite"
                            prof["auth"]["invite_code"] = code

                            starts = date.today()
                            ends = starts + timedelta(days=PASS_DURATION_DAYS)
                            prof["pass"] = {
                                "tier": "explorer",
                                "status": "active",
                                "starts_at": starts.isoformat(),
                                "ends_at": ends.isoformat(),
                                "source": "manual",
                                "notes": "alpha"
                            }
                            ensure_access_state(rw_data)
                            save_data(DATA_PATH, rw_data)

                            st.success("✅ 已激活：探索季票已生效（全路线解锁）")
                            st.rerun()

                except TimeoutError:
                    st.warning("系统繁忙（多人同时激活中），请稍后再试一次。")



    ent = prof.get("entitlements", {})
    has_all_routes = bool(ent.get("all_routes", False))
    # --- Phase 4.5.2: Pro 用户默认进入 Dashboard ---
    if has_all_routes:
        st.session_state.view = "pro_dashboard"
        st.rerun()

    for rid in routes.keys():
        s = sum(float(h.get("km", 0.0)) for h in rw_data.get("history", []) if h.get("route_id") == rid)
        rw_data["profile"]["route_progress"][rid] = round(s, 3)

    save_data(DATA_PATH, rw_data)

    if not rw_data["profile"]["route_progress"]:
        recompute_profile(rw_data)
        save_data(DATA_PATH, rw_data)

    # 生成每条路线的摘要
    summaries = []
    for rid, meta in routes.items():
        summaries.append(build_route_summary(rid, meta, rw_data))

    # 进行中优先，其次未开始，最后已完成；同状态内按最近日期（有日期的更靠前）
    status_rank = {"进行中": 0, "未开始": 1, "已完成": 2}
    summaries.sort(
        key=lambda s: (
            status_rank.get(s["status"], 9),
            0 if s["last_date"] else 1,
            "" if s["last_date"] is None else s["last_date"],
        )
    )

    # 三列卡片
    cols = st.columns(3)
    for i, s in enumerate(summaries):
        with cols[i % 3]:
            with st.container(border=True):
                st.subheader(s["title"])
                st.caption(f"{s['subtitle']}  ·  route_id: {s['rid']}")

                # 进度条
                st.progress(min(max(s["pct"], 0.0), 1.0))

                # 进度文字
                st.write(f"**{s['km_done']:.1f} / {s['km_total']:.1f} km**  ·  {s['pct']*100:.2f}%")

                # 状态 + 最近一次
                if s["status"] == "进行中":
                    st.write("🟢 进行中")
                elif s["status"] == "已完成":
                    st.write("🏁 已完成")
                else:
                    st.write("⚪ 未开始")

                if s["last_date"]:
                    st.caption(f"最近跑步：{s['last_date']}")
                else:
                    st.caption("最近跑步：暂无")

                is_free = (s["rid"] in FREE_ROUTE_IDS)
                is_unlocked = has_all_routes or is_free

                if not is_unlocked:
                    st.caption("🔒 需要探索季票解锁该路线")
                    st.button("🔒 锁定", key=f"locked_{s['rid']}", disabled=True)
                else:
                    if st.button("进入", key=f"enter_{s['rid']}"):
                        st.session_state.active_route_id = s["rid"]
                        st.session_state.view = "main"
                        st.rerun()
        # --- Admin panel (hidden) ---
    with st.expander("🔧 Admin（邀请码管理）", expanded=False):
        admin_token = os.getenv(ADMIN_TOKEN_ENV, "")
        if not admin_token:
            st.warning(f"未设置管理员口令。请在环境变量中设置 {ADMIN_TOKEN_ENV} 后重启。")
        else:
            entered = st.text_input("Admin Token", type="password", placeholder=f"输入 {ADMIN_TOKEN_ENV}")
            if entered != admin_token:
                st.caption("输入正确口令后，将显示邀请码管理面板。")
            else:
                st.success("已进入 Admin 模式")

                invites = load_invites(INVITES_PATH)

                def _cnt(status: str) -> int:
                    return sum(
                        1 for v in invites.values()
                        if isinstance(v, dict) and v.get("status") == status
                    )

                c_new, c_used, c_rev = _cnt("new"), _cnt("used"), _cnt("revoked")
                st.write(f"📊 统计：new={c_new} ｜ used={c_used} ｜ revoked={c_rev}")

                # Export list
                new_codes = sorted([
                    code for code, rec in invites.items()
                    if isinstance(rec, dict) and rec.get("status") == "new"
                ])
                st.text_area("可用邀请码（复制发放）", value="\n".join(new_codes), height=150)

                # Revoke tool
                col1, col2 = st.columns([3, 1])
                with col1:
                    revoke_code = st.text_input("要作废的邀请码", placeholder="例如：RW-ALPHA-010")
                with col2:
                    if st.button("作废", key="admin_revoke"):
                        rc = (revoke_code or "").strip()
                        if not rc:
                            st.error("请输入要作废的邀请码。")
                        else:
                            try:
                                with FileLock(INVITES_LOCK_PATH, timeout_s=8.0):
                                    invites = load_invites(INVITES_PATH)
                                    rec = invites.get(rc)
                                    if not isinstance(rec, dict):
                                        st.error("该邀请码不存在。")
                                    else:
                                        rec["status"] = "revoked"
                                        invites[rc] = rec
                                        save_invites(INVITES_PATH, invites)
                                        st.success(f"✅ 已作废：{rc}")
                                        st.rerun()
                            except TimeoutError:
                                    st.warning("系统繁忙（多人同时操作邀请码），请稍后再试。")


                # Table view (lightweight, no pandas needed)
                rows = []
                for code, rec in sorted(invites.items()):
                    if not isinstance(rec, dict):
                        continue
                    rows.append({
                        "code": code,
                        "status": rec.get("status", ""),
                        "issued_to": rec.get("issued_to", ""),
                        "issued_at": rec.get("issued_at", ""),
                        "activated_at": rec.get("activated_at", "")
                    })

                st.write("📋 邀请码列表")
                st.dataframe(rows, use_container_width=True, hide_index=True)


    st.stop()

if st.session_state.view == "pro_dashboard":
    st.title("🏁 Running World · Pro 控制台（四线同步）")

    # 载入数据
    rw_data = load_data(DATA_PATH)
    ensure_access_state(rw_data)
    save_data(DATA_PATH, rw_data)

    profile = rw_data.get("profile", {})
    v3 = profile.get("v3", {})
    pro = v3.get("pro", {})
    lock_pro = (str(pro.get("reward_state", "locked")) == "accepted")
    pro_routes = pro.get("routes", {})

    # 如果 pro.routes 为空：用 PRO_ROUTE_IDS 初始化
    if not isinstance(pro_routes, dict):
        pro_routes = {}

    if len(pro_routes) == 0:
        for rid in PRO_ROUTE_IDS:
            pro_routes[rid] = {"km": 0.0, "status": "running", "finished_at": None}
        pro["routes"] = pro_routes
        v3["pro"] = pro
        profile["v3"] = v3
        rw_data["profile"] = profile
        save_data(DATA_PATH, rw_data)

    # 顶部导航
    colX, colY = st.columns([3, 1])
    with colX:
        st.caption("一次输入今日跑量，四条 Pro 路线同步推进。")
    with colY:
        if st.button("🔙 返回路线选择"):
            st.session_state.view = "picker"
            st.rerun()

    st.divider()

    # 今日统一输入
    add_km = st.number_input("今日新增（km）", min_value=0.0, value=0.0, step=1.0)

    c1, c2 = st.columns([1, 3])
    with c1:
        go = st.button("✅ 同步提交", use_container_width=True, disabled=lock_pro)
    with c2:
        st.caption("提示：提交后会写入每条路线的 history（同日同路线自动合并）。")

    if lock_pro:
        st.info("🏁 Pro 挑战已结束：同步推进已锁定。")

    if go and add_km > 0:
        add_run_km_pro(rw_data, km=float(add_km), mode="merge")

        # 保存
        save_data(DATA_PATH, rw_data)

        st.success("已同步推进四条 Pro 路线。")
        st.rerun()

    st.divider()
        # --- Phase 4.5.3: 完成检测（为奖励 pending 做准备）---
    from datetime import date as _date

    profile = rw_data.get("profile", {})
    v3 = profile.get("v3", {})
    pro = v3.get("pro", {})
    pro_routes = pro.get("routes", {})

    # 兜底：保证结构存在
    if not isinstance(pro_routes, dict):
        pro_routes = {}
        pro["routes"] = pro_routes

    pro.setdefault("reward_state", "locked")          # locked/pending/accepted/declined
    pro.setdefault("finished_route_id", None)         # 哪条触发领奖
    pro.setdefault("reward_choice_at", None)

    today_iso = _date.today().isoformat()

    # 如果已经 pending，就不重复切换 finished_route_id
    reward_state = str(pro.get("reward_state", "locked"))
    pending_locked = (reward_state == "pending")

    # 1) 扫描所有 Pro 路线：把“running -> finished”的转变找出来
    newly_finished = []  # [(rid, total_km), ...]
    for rid in PRO_ROUTE_IDS:
        # 只处理 routes 里存在且 meta 也存在的路线
        if rid not in routes:
            continue

        # 当前累计（以 history 为准）
        route_sum = 0.0
        for h in rw_data.get("history", []):
            if h.get("route_id") == rid:
                try:
                    route_sum += float(h.get("km", 0.0))
                except Exception:
                    pass

        # 总里程：用 nodes 自动推断（与你主页面一致）
        try:
            nodes_path = get_route_nodes_path(rid, routes[rid])
            _, _, _, total_km = load_nodes(nodes_path)
            total_km = float(total_km)
        except Exception:
            total_km = 0.0

        # 读取/修复 pro_routes[rid]
        rec = pro_routes.get(rid)
        if not isinstance(rec, dict):
            rec = {"km": 0.0, "status": "running", "finished_at": None}
        rec.setdefault("status", "running")
        rec.setdefault("finished_at", None)

        # 写回 km（统一来源：history 汇总）
        rec["km"] = round(route_sum, 3)

        # 判断是否完赛
        is_done = (total_km > 0 and route_sum >= total_km - 1e-6)

        # 仅当从非 finished -> finished 时算“新完成”
        if is_done and rec.get("status") != "finished":
            rec["status"] = "finished"
            rec["finished_at"] = today_iso
            newly_finished.append((rid, total_km))

        pro_routes[rid] = rec

    # 2) 奖励状态机：只有在 locked/declined 且出现新完成时，才切 pending
    #    accepted 时永远不再触发
    if reward_state not in ("accepted", "pending"):
        if newly_finished:
            # 若一次提交导致多条完成：优先选择 total_km 最短的那条作为“第一触发”
            newly_finished.sort(key=lambda x: x[1])
            trigger_rid = newly_finished[0][0]

            pro["reward_state"] = "pending"
            pro["finished_route_id"] = trigger_rid
            pro["reward_choice_at"] = today_iso

    # 写回并持久化
    pro["routes"] = pro_routes
    v3["pro"] = pro
    profile["v3"] = v3
    rw_data["profile"] = profile
    save_data(DATA_PATH, rw_data)
    # --- Phase 4.5.3: pending 时显示领奖入口（放在完成检测之后，确保是最新状态）---
    rw_data = load_data(DATA_PATH)  # 关键：读回最新 reward_state
    pro = rw_data.get("profile", {}).get("v3", {}).get("pro", {})
    if str(pro.get("reward_state")) == "pending":
        frid = pro.get("finished_route_id")
        st.info("🎁 有一条 Pro 路线已完成，奖励选择已解锁。")
        if frid and st.button("前往领奖 / 做出选择", use_container_width=True):
            st.session_state.active_route_id = frid
            st.session_state.view = "main"
            st.rerun()

    # 渲染四条路线卡片（用你已有的 build_route_summary）
    st.subheader("📊 四线进度总览")

    rw_data = load_data(DATA_PATH)  # 读回一次，确保是最新
    summaries = []
    for rid in PRO_ROUTE_IDS:
        if rid in routes:
            summaries.append(build_route_summary(rid, routes[rid], rw_data))

    # 排序：进行中在前，已完成在后；同状态按完成度高->低
    status_rank = {"进行中": 0, "未开始": 1, "已完成": 2}
    summaries.sort(key=lambda s: (status_rank.get(s["status"], 9), -s["pct"]))

    cols = st.columns(2)
    for i, s in enumerate(summaries):
        with cols[i % 2]:
            with st.container(border=True):
                st.subheader(s["title"])
                st.caption(f"{s['subtitle']}  ·  route_id: {s['rid']}")
                st.progress(min(max(s["pct"], 0.0), 1.0))
                st.write(f"**{s['km_done']:.1f} / {s['km_total']:.1f} km**  ·  {s['pct']*100:.2f}%")

                if s["status"] == "已完成":
                    st.write("🏁 已完成")
                elif s["status"] == "进行中":
                    st.write("🟢 进行中")
                else:
                    st.write("⚪ 未开始")

                if s["last_date"]:
                    st.caption(f"最近跑步：{s['last_date']}")
                else:
                    st.caption("最近跑步：暂无")

                if st.button("进入路线详情", key=f"pro_enter_{s['rid']}"):
                    st.session_state.active_route_id = s["rid"]
                    st.session_state.view = "main"
                    st.rerun()

    st.stop()

route_id = st.session_state.active_route_id
meta = routes[route_id]

# ✅ 用 meta.node_spacing_km 自动选择正确的 nodes 文件（避免总里程首次为 0 / 读错文件）
nodes_path = get_route_nodes_path(route_id, meta)
data, points, dists, total_km = load_nodes(nodes_path)

st.title(f"🏃‍♂️ Running World · {meta.get('name', route_id)}")
# =========================
# Phase 4.6: Pro 奖励选择闸门（接受/拒绝）
# =========================
rw_data_gate = load_data(DATA_PATH)
profile_gate = rw_data_gate.get("profile", {})
ent_gate = profile_gate.get("entitlements", {})
v3_gate = profile_gate.get("v3", {})
pro_gate = v3_gate.get("pro", {})

is_pro_user = bool(ent_gate.get("all_routes", False))
reward_state = str(pro_gate.get("reward_state", "locked"))
finished_rid = pro_gate.get("finished_route_id")

# 仅当：Pro 用户 + pending + 当前页面正好是触发完成的那条路线，才显示领奖 UI
if is_pro_user and reward_state == "pending" and finished_rid == route_id:
    st.warning("🎁 你已完成一条 Pro 路线！现在可以选择领取奖励，或继续挑战更高档。")

    colR1, colR2 = st.columns(2)
    with colR1:
        accept_reward = st.button("🏅 接受奖励（结束本次 Pro 挑战）", use_container_width=True)
    with colR2:
        decline_reward = st.button("🔥 拒绝奖励（继续挑战更高档）", use_container_width=True)

    if accept_reward:
        pro_gate["reward_state"] = "accepted"
        pro_gate["reward_choice_at"] = date.today().isoformat()
        # accepted：你原需求是“全结束”，这里顺手把 pro.active 关掉（可选，但推荐）
        pro_gate["active"] = False
        v3_gate["pro"] = pro_gate
        profile_gate["v3"] = v3_gate
        rw_data_gate["profile"] = profile_gate
        save_data(DATA_PATH, rw_data_gate)
        st.success("已领取奖励：本次 Pro 挑战已结束。")
        st.rerun()

    if decline_reward:
        pro_gate["reward_state"] = "declined"
        pro_gate["reward_choice_at"] = date.today().isoformat()
        # declined：清空触发者，让下一条完成时再进入 pending（Phase 4.5.3 会重新写入）
        pro_gate["finished_route_id"] = None
        v3_gate["pro"] = pro_gate
        profile_gate["v3"] = v3_gate
        rw_data_gate["profile"] = profile_gate
        save_data(DATA_PATH, rw_data_gate)
        st.info("你选择继续挑战：奖励已暂时搁置，完成下一条路线后将再次触发。")
        st.rerun()

# accepted 后：可选择在单路线页面也提示“已封盘”
if is_pro_user and reward_state == "accepted":
    st.info("🏁 Pro 挑战已结束（已接受奖励）。如需继续推进，请在后续版本开启新赛季或重置。")

# Pro 用户：提供返回 Dashboard
rw_data_tmp = load_data(DATA_PATH)
ent_tmp = rw_data_tmp.get("profile", {}).get("entitlements", {})
if bool(ent_tmp.get("all_routes", False)):
    if st.button("⬅️ 返回 Pro 控制台"):
        st.session_state.view = "pro_dashboard"
        st.rerun()

KEY_CITIES = meta.get("key_cities", [])

NEAR_CITY_KM = 25.0  # “接近城市”阈值（可调 10~30）

# ====== JSON 持久化：加载数据 ======
if "rw_data" not in st.session_state:
    st.session_state.rw_data = load_data(DATA_PATH)

rw_data = st.session_state.rw_data
profile = rw_data["profile"]
# ===== Phase 4.3: Pro completion reward UI =====
v3 = profile.get("v3", {})
pro = v3.get("pro", {})

if v3.get("mode") == "pro" and pro.get("reward_state") == "pending":
    # Phase 4.4: generate narrative reward once
    if not pro.get("reward_narrative"):
        rid = pro.get("finished_route_id")
        route_meta = routes.get(rid, {})
        narrative = generate_reward_narrative(route_meta)
        pro["reward_narrative"] = narrative
        save_data(DATA_PATH, rw_data)

    narr = pro.get("reward_narrative", {}) or {}

    title = narr.get("title") or "你完成了一条 Pro 路线"
    body = narr.get("body") or ""

    st.markdown(f"## 🎁 {title}")

    if body.strip():
        st.markdown(
            f"<div style='white-space: pre-line; font-size: 1.05em; line-height: 1.6;'>"
            f"{body}"
            f"</div>",
            unsafe_allow_html=True
        )

    st.markdown("---")
    st.write("现在你可以选择：")


    col1, col2 = st.columns(2)

    with col1:
        if st.button("🎉 接受奖励（结束本次挑战）", key="reward_accept"):
            pro["reward_state"] = "accepted"
            pro["reward_choice_at"] = date.today().isoformat()
            save_data(DATA_PATH, rw_data)
            st.success("奖励已接受，本次 Pro 挑战圆满完成。")
            st.rerun()

    with col2:
        if st.button("🚀 拒绝奖励（继续推进更高难度）", key="reward_decline"):
            pro["reward_state"] = "declined"
            pro["reward_choice_at"] = date.today().isoformat()
            save_data(DATA_PATH, rw_data)
            st.info("你选择继续挑战，旅程仍在延伸。")
            st.rerun()

# ===== per-route session keys =====
rk_key   = f"route_km__{route_id}"
prev_key = f"prev_route_km__{route_id}"
last_key = f"last_add_km__{route_id}"

# ensure route_progress exists in json
profile = rw_data["profile"]
profile.setdefault("route_progress", {})
profile["route_progress"].setdefault(route_id, 0.0)

# init per-route states
if rk_key not in st.session_state:
    st.session_state[rk_key] = float(profile["route_progress"][route_id])
if prev_key not in st.session_state:
    st.session_state[prev_key] = float(st.session_state[rk_key])
if last_key not in st.session_state:
    st.session_state[last_key] = 0.0

# 侧边栏：输入累计跑量
st.sidebar.header("📏 跑量输入")
rw_data_lock = load_data(DATA_PATH)
pro_lock = rw_data_lock.get("profile", {}).get("v3", {}).get("pro", {})
reward_state_lock = str(pro_lock.get("reward_state", "locked"))
lock_inputs = (reward_state_lock == "accepted")

# --- 累计跑量：用 session_state 记住 ---

add_km = st.sidebar.number_input(
    "今日新增（km）",
    min_value=0.0,
    value=0.0,
    step=1.0,
)

colA, colB = st.sidebar.columns(2)
with colA:
    submit = st.sidebar.button("✅ 提交今日跑量", disabled=lock_inputs)
with colB:
    undo = st.sidebar.button("↩ 撤销一次", disabled=lock_inputs)
if lock_inputs:
    st.sidebar.info("Pro 挑战已结束：输入已锁定。")

# 先处理按钮逻辑（写入 JSON 持久化）
if submit and add_km > 0:
    # 判断 v3 模式（默认 free）
    v3 = profile.get("v3", {})
    mode_v3 = "free"
    if isinstance(v3, dict):
        mode_v3 = str(v3.get("mode", "free"))

    if mode_v3 == "pro":
        target_ids = sorted(list(PRO_ROUTE_IDS))
        # 记录每条路线提交前累计（用于各自地图的“今日高亮”）
        for rid in target_ids:
            pk = f"prev_route_km__{rid}"
            rk = f"route_km__{rid}"
            # 初始化缺失的 session key（避免第一次进某条路线看图时报错）
            if rk not in st.session_state:
                st.session_state[rk] = float(profile.get("route_progress", {}).get(rid, 0.0))
            st.session_state[pk] = float(st.session_state[rk])

        # ✅ Phase 4.5+: Pro 同步推进（新逻辑）
        add_run_km_pro(rw_data, km=float(add_km), mode="merge")
        save_data(DATA_PATH, rw_data)

        # （可选）如果你仍希望在“单路线页输入”时也能立刻触发 pending，
        # 那就在这里做一次轻量完成检测：直接复用你在 pro_dashboard 里写的完成检测器。
        # 但为了避免重复逻辑，建议先不在这里做检测，统一由 pro_dashboard 负责触发。

        # 同步各路线 session_state
        for rid in target_ids:
            rk = f"route_km__{rid}"
            st.session_state[rk] = float(rw_data["profile"].get("route_progress", {}).get(rid, 0.0))

        # 用一个全局 last_add_km（pro 模式撤销要一起撤）
        st.session_state["last_add_km__pro"] = float(add_km)
    else:
        # free：只推进当前路线
        st.session_state[prev_key] = float(st.session_state[rk_key])
        rw_data["profile"]["current_route_id"] = route_id
        add_run_km(rw_data, km=float(add_km), mode="merge")
        save_data(DATA_PATH, rw_data)

        # recompute this route's progress from history
        route_sum = sum(float(h.get("km", 0.0)) for h in rw_data.get("history", []) if h.get("route_id") == route_id)
        profile.setdefault("route_progress", {})
        profile["route_progress"][route_id] = round(route_sum, 3)

        st.session_state[rk_key] = float(profile["route_progress"][route_id])
        st.session_state[last_key] = float(add_km)


    # 读回（确保 UI 用到的是最新 profile）
    st.session_state.rw_data = load_data(DATA_PATH)
    rw_data = st.session_state.rw_data
    profile = rw_data["profile"]

    st.rerun()

if undo and float(st.session_state[last_key]) > 0:
    today = date.today().isoformat()
    last = float(st.session_state[last_key])

    # 找到今天该路线记录并回退
    history = rw_data.get("history", [])
    for i in range(len(history) - 1, -1, -1):
        item = history[i]
        if item.get("date") == today and item.get("route_id") == route_id:
            new_km = float(item.get("km", 0.0)) - last
            if new_km > 1e-9:
                item["km"] = round(new_km, 3)
            else:
                history.pop(i)
            break

    rw_data["history"] = history

    # 重算全局 profile（streak/total等）
    recompute_profile(rw_data)

    # 重算本路线 progress
    route_sum = sum(float(h.get("km", 0.0)) for h in rw_data.get("history", []) if h.get("route_id") == route_id)
    rw_data["profile"].setdefault("route_progress", {})
    rw_data["profile"]["route_progress"][route_id] = round(route_sum, 3)

    save_data(DATA_PATH, rw_data)
    st.session_state.rw_data = load_data(DATA_PATH)
    rw_data = st.session_state.rw_data
    profile = rw_data["profile"]

    # 同步 per-route session
    st.session_state[rk_key] = float(profile.get("route_progress", {}).get(route_id, 0.0))
    st.session_state[prev_key] = float(st.session_state[rk_key])
    st.session_state[last_key] = 0.0
    st.rerun()

# 再显示（这里就会是“更新后的累计”）
st.sidebar.write(f"当前累计：**{st.session_state[rk_key]:.2f} km**")

# 可选：给一个“手动校准累计”的入口（只校准当前路线）
with st.sidebar.expander("高级：手动校准当前路线累计"):
    manual = st.number_input(
        "设置本路线累计（km）",
        min_value=0.0,
        value=float(st.session_state[rk_key]),
        step=10.0
    )

    if st.button("应用校准（仅当前路线）"):
        # 1) 校准前记录：用于“今日高亮”
        st.session_state[prev_key] = float(st.session_state[rk_key])

        # 2) 清空当前路线历史（保留其他路线）
        rw_data["history"] = [h for h in rw_data.get("history", []) if h.get("route_id") != route_id]

        # 3) 写入当前路线的累计
        rw_data["profile"].setdefault("route_progress", {})
        rw_data["profile"]["route_progress"][route_id] = float(manual)

        # 4) 你的全局 streak/total 等，建议重算一次（不想动也行）
        recompute_profile(rw_data)

        save_data(DATA_PATH, rw_data)

        # 5) 读回刷新 UI
        st.session_state.rw_data = load_data(DATA_PATH)
        rw_data = st.session_state.rw_data
        profile = rw_data["profile"]

        st.session_state[rk_key] = float(profile.get("route_progress", {}).get(route_id, 0.0))
        st.session_state[last_key] = 0.0
        st.session_state[prev_key] = float(st.session_state[rk_key])
        st.rerun()

use_ai = st.sidebar.checkbox("启用 AI 陪跑播报", value=True)

# 定位当前位置
current_km = float(st.session_state[rk_key])
p, progress, km_clamped, total_km = locate_by_distance(points, dists, current_km)

geo = reverse_geocode(p["lat"], p["lon"])
city = geo.get("city") or "未知地点"
state = geo.get("state") or "未知地区"
country = geo.get("country") or ""

# ---------- Step 5.1 成就与提示（只触发一次） ----------
if "last_state" not in st.session_state:
    st.session_state.last_state = None
if "last_milestone_100" not in st.session_state:
    st.session_state.last_milestone_100 = 0
if "last_near_city" not in st.session_state:
    st.session_state.last_near_city = None

tips = []

milestone_hit = None
province_change = None
near_city_tip = None

# 1) 里程碑：每 100km 触发一次（你也可以改成 50）
milestone_step = 100
milestone_now = int(km_clamped // milestone_step)

if milestone_now > st.session_state.last_milestone_100:
    st.session_state.last_milestone_100 = milestone_now
    achieved = milestone_now * milestone_step
    marathon_eq = achieved / 42.195
    tips.append(f"🎉 里程碑达成：累计 **{achieved} km**（约等于 **{marathon_eq:.1f}** 场马拉松）")
    milestone_hit = f"{achieved} km（≈{marathon_eq:.1f} 场马拉松）"

# 2) 跨省提示：state 变化触发一次
if st.session_state.last_state is None:
    st.session_state.last_state = state
else:
    if state != st.session_state.last_state:
        tips.append(f"🗺️ 你已从 **{st.session_state.last_state}** 进入 **{state}**（真正意义上的跨省推进！）")
        province_change = f"{st.session_state.last_state} -> {state}"
        st.session_state.last_state = state

# 3) 接近城市提示：距离某关键城市 < 阈值，且不同于上次触发城市
nearest = None
for c in KEY_CITIES:
    d = haversine_km(p["lat"], p["lon"], c["lat"], c["lon"])
    if (nearest is None) or (d < nearest["dist_km"]):
        nearest = {"name": c["name"], "dist_km": d}

if nearest and nearest["dist_km"] <= NEAR_CITY_KM:
    if st.session_state.last_near_city != nearest["name"]:
        tips.append(f"📍 你正在接近 **{nearest['name']}**（约 **{nearest['dist_km']:.1f} km**）")
        near_city_tip = f"{nearest['name']}（约 {nearest['dist_km']:.1f} km）"
        st.session_state.last_near_city = nearest["name"]

# 显示文字信息
st.markdown(f"""
### 📍 当前进度
- **累计跑量**：{km_clamped:.1f} km  
- **总里程**：{total_km:.1f} km  
- **进度**：{progress*100:.2f}%  
""")
stops = build_city_stops(meta, points, total_km)
render_city_metro_line(stops, km_clamped, total_km)
route_id = st.session_state.active_route_id   # 你项目里当前路线 id
render_clickable_cities(stops, km_clamped, meta, route_id)
end_name = meta.get("waypoints", [{"name": "终点"}])[-1].get("name", "终点")
st.markdown(f"""
### 🗺️ 你跑到了哪里？
- **位置**：{city}（{state} {country}）
- **提示**：你正在向终点推进，距离 **{end_name}** 还剩 **{(total_km - km_clamped):.1f} km**
""")
st.subheader("✨ 今日成就与提示")
if tips:
    for t in tips:
        st.write("-", t)
else:
    st.write("— 继续推进！下一个里程碑在前方。")

st.subheader("🎧 陪跑播报（AI）")

remaining_km = total_km - km_clamped

if use_ai:
    try:
        narration = generate_narration(
            route_name=data.get("name", "Nanjing to Beijing"),
            km_done=km_clamped,
            km_total=total_km,
            city=city,
            state=state,
            country=country,
            remaining_km=remaining_km,
            milestone_hit=milestone_hit,
            province_change=province_change,
            near_city_tip=near_city_tip,
        )
        st.write(narration)
    except Exception as e:
        st.warning(f"AI 播报暂时不可用：{e}")
else:
    st.write("（AI 陪跑播报已关闭）")

# ---------- Step 6: pydeck 地图（完成段/未完成段 + 节点点阵 + 今日高亮 + 当前点） ----------

STEP_KM = float(data.get("step_km", 0.5))

cur_idx = min(int(km_clamped / STEP_KM), len(points) - 1)
prev_km = float(st.session_state[prev_key])
prev_idx = min(int(float(prev_km) / STEP_KM), len(points) - 1)

done_path = [[pt["lon"], pt["lat"]] for pt in points[:cur_idx + 1]]
todo_path = [[pt["lon"], pt["lat"]] for pt in points[cur_idx:]]

today_path = (
    [[pt["lon"], pt["lat"]] for pt in points[max(prev_idx, 0):cur_idx + 1]]
    if cur_idx > prev_idx else []
)

done_pts = points[:cur_idx + 1]
today_pts = points[max(prev_idx + 1, 0):cur_idx + 1] if cur_idx > prev_idx else []

cur_lon, cur_lat = p["lon"], p["lat"]

todo_layer = pdk.Layer(
    "PathLayer",
    data=[{"path": todo_path}],
    get_path="path",
    width_scale=20,
    width_min_pixels=2,
    rounded=True,
    get_color=[170, 170, 170],
)

done_layer = pdk.Layer(
    "PathLayer",
    data=[{"path": done_path}],
    get_path="path",
    width_scale=20,
    width_min_pixels=4,
    rounded=True,
    get_color=[0, 170, 0],
)

today_path_layer = pdk.Layer(
    "PathLayer",
    data=[{"path": today_path}] if today_path else [],
    get_path="path",
    width_scale=30,
    width_min_pixels=6,
    rounded=True,
    get_color=[255, 140, 0],
)

done_points_layer = pdk.Layer(
    "ScatterplotLayer",
    data=done_pts,
    get_position='[lon, lat]',
    get_radius=2500,
    radius_min_pixels=4,
    radius_max_pixels=10,
    get_fill_color=[0, 120, 255, 160],
    get_line_color=[255, 255, 255, 180],
    line_width_min_pixels=1,
    pickable=False,
)

today_points_layer = pdk.Layer(
    "ScatterplotLayer",
    data=today_pts,
    get_position='[lon, lat]',
    get_radius=3500,
    radius_min_pixels=7,
    radius_max_pixels=14,
    get_fill_color=[255, 140, 0, 220],
    get_line_color=[0, 0, 0, 160],
    line_width_min_pixels=1,
    pickable=False,
)

current_point_layer = pdk.Layer(
    "ScatterplotLayer",
    data=pd.DataFrame([{"lon": cur_lon, "lat": cur_lat}]),
    get_position='[lon, lat]',
    radius_units="pixels",
    get_radius=10,
    radius_min_pixels=10,
    radius_max_pixels=10,
    pickable=True,
    get_fill_color=[0, 0, 0],
    get_line_color=[255, 140, 0, 200],
    line_width_min_pixels=2,
)

view_state = pdk.ViewState(
    longitude=cur_lon,
    latitude=cur_lat,
    zoom=6,
    pitch=0,
)

deck = pdk.Deck(
    layers=[todo_layer, done_layer, today_path_layer, done_points_layer, today_points_layer, current_point_layer],
    initial_view_state=view_state,
    tooltip={"text": "当前位置\n{lon}, {lat}"},
    map_style="light",
)

st.pydeck_chart(deck, use_container_width=True)

st.subheader("📚 跑步记录")

history_all = rw_data.get("history", [])
history = [h for h in history_all if h.get("route_id") == route_id]

if not history:
    st.info("暂无历史记录。提交一次“今日新增跑量”后，这里会显示你的跑步日志。")
else:
    # 按日期倒序
    history_sorted = sorted(history, key=lambda x: x.get("date", ""), reverse=True)

    # 最近 14 条（你也可以改成最近 7 天：先按天聚合）
    recent = history_sorted[:14]

    df = pd.DataFrame(recent)
    df = df.rename(columns={"date": "日期", "km": "里程(km)", "route_id": "路线", "note": "备注"})
    if "备注" not in df.columns:
        df["备注"] = ""

    df["里程(km)"] = df["里程(km)"].astype(float).round(2)

    st.dataframe(df[["日期", "里程(km)", "路线", "备注"]], use_container_width=True, hide_index=True)

    total_days = len(set([x.get("date") for x in history if x.get("date")]))
    st.caption(
        f"累计记录天数：{total_days} 天 | 当前连续：{profile.get('streak_days', 0)} 天 | "
        f"本路线累计：{float(profile.get('route_progress', {}).get(route_id, 0.0)):.2f} km"
    )

with st.expander("🧹 数据管理（谨慎操作）", expanded=False):
    today = date.today().isoformat()
    st.write(f"今天日期：**{today}**")

    # 安全确认：必须勾选才能删
    confirm = st.checkbox("我确认要删除“今天”的跑步记录（不可撤销）", value=False)

    if st.button("删除今天记录", disabled=not confirm):
        # 只删除当前路线的“今天记录”
        route_id = st.session_state.active_route_id

        delete_runs_by_date(rw_data, target_date=today, route_id=route_id)
        route_sum = sum(float(h.get("km", 0.0)) for h in rw_data.get("history", []) if h.get("route_id") == route_id)
        rw_data["profile"].setdefault("route_progress", {})
        rw_data["profile"]["route_progress"][route_id] = round(route_sum, 3)

        recompute_profile(rw_data)
        save_data(DATA_PATH, rw_data)

        # 读回刷新 UI + 同步 session_state.total_km
        st.session_state.rw_data = load_data(DATA_PATH)
        rw_data = st.session_state.rw_data
        profile = rw_data["profile"]
        st.session_state[rk_key] = float(profile.get("route_progress", {}).get(route_id, 0.0))
        st.session_state[last_key] = 0.0
        st.session_state[prev_key] = float(st.session_state[rk_key])

        st.success("已删除今天记录，并完成数据重算。")
        st.rerun()

