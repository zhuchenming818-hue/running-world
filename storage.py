from __future__ import annotations
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional


# --- R2 backend (S3-compatible) ---
_BACKEND = os.getenv("RW_STORAGE_BACKEND", "local").strip().lower()

def _is_r2() -> bool:
    return _BACKEND == "r2"

def _r2_client():
    import boto3
    endpoint = os.getenv("R2_ENDPOINT", "").strip()
    access = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    secret = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    if not endpoint or not access or not secret:
        raise RuntimeError("R2 env missing: R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY")

    # region_name can be anything for R2; "auto" is commonly used
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="auto",
    )

def _r2_bucket() -> str:
    b = os.getenv("R2_BUCKET", "").strip()
    if not b:
        raise RuntimeError("R2_BUCKET missing")
    return b

def _r2_key_for_path(path: str) -> str:
    """
    Keep app.py unchanged: infer object key from local filename.
    run_data_<USER_ID>.json -> users/<USER_ID>/run_data.json
    invites.json -> invites/invites.json
    """
    base = os.path.basename(path)
    if base.startswith("run_data_") and base.endswith(".json"):
        user_id = base[len("run_data_") : -len(".json")]
        return f"users/{user_id}/run_data.json"
    if base == "invites.json":
        return "invites/invites.json"
    # fallback: put under misc/
    return f"misc/{base}"

def _r2_get_json(key: str):
    import json
    s3 = _r2_client()
    try:
        obj = s3.get_object(Bucket=_r2_bucket(), Key=key)
        raw = obj["Body"].read().decode("utf-8")
        return json.loads(raw)
    except Exception as e:
        # NoSuchKey / 404 and other errors -> treat as missing
        # boto3 exceptions vary; simplest is to just return None on failure here
        return None

def _r2_put_json(key: str, data):
    import json
    s3 = _r2_client()
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=_r2_bucket(), Key=key, Body=body, ContentType="application/json; charset=utf-8")

DEFAULT_DATA: Dict[str, Any] = {
    "meta": {
        "schema_version": 3,
        "created_at": "",
        "updated_at": ""
    },
    "profile": {
        "user_id": "local",
        "current_route_id": "nj_bj",
        "total_km": 0.0,
        "streak_days": 0,
        "last_run_date": None,

        # --- Phase 3.3: minimal auth + pass + entitlements ---
        "auth": {
            "mode": "local",          # local / invite (future: oauth)
            "invite_code": None,
            "user_key": None          # stable anonymous id (optional for now)
        },
        "pass": {
            "tier": "free",           # free / explorer
            "status": "none",         # none / active / expired / revoked
            "starts_at": None,        # YYYY-MM-DD
            "ends_at": None,          # YYYY-MM-DD
            "source": "local",        # local / manual / (future: stripe)
            "notes": ""
        },
        "entitlements": {
            "all_routes": False,
            "ai_basic": True,
            "ai_plus": False,
            "street_view": False
        },

        # per-route progress cache (you already compute it in app.py)
        "route_progress": {},

        # --- Phase 4.x: multi-route state machine (v3 profile) ---
        "v3": {
            "mode": "free",  # free / pro
            "free": {
                "selected_route_id": None,
                "progress_km": {}
            },
            "pro": {
                "active": False,
                "routes": {},  # rid -> {km, status, finished_at}
                "reward_state": "locked",        # locked/pending/accepted/declined
                "finished_route_id": None,
                "reward_choice_at": None
            }
        }
    },
    "routes": {
        "nj_bj": {
            "name": "南京 → 北京",
            "total_km": 1020.0,
            "milestones": [
                {"km": 0.0, "title": "南京", "desc": "起点"},
                {"km": 1020.0, "title": "北京", "desc": "终点"}
            ]
        }
    },
    "history": []
}

INVITES_DEFAULT_PATH = "data/invites.json"

def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _ensure_dir(path: str) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)


def atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    """
    Atomic write to avoid file corruption:
    write to temp file -> fsync -> replace.
    """
    _ensure_dir(path)
    directory = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(prefix="rw_", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        # If something failed before replace, cleanup temp.
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass

import time

class FileLock:
    def __init__(self, lock_path: str, timeout_s: float = 8.0, poll_s: float = 0.08):
        self.lock_path = lock_path
        self.timeout_s = timeout_s
        self.poll_s = poll_s
        self._fd = None

    def __enter__(self):
        start = time.time()
        while True:
            try:
                # O_EXCL: atomic create -> only one winner
                self._fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(self._fd, str(os.getpid()).encode("utf-8"))
                return self
            except FileExistsError:
                if time.time() - start > self.timeout_s:
                    raise TimeoutError(f"Could not acquire lock: {self.lock_path}")
                time.sleep(self.poll_s)

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fd is not None:
                os.close(self._fd)
            if os.path.exists(self.lock_path):
                os.remove(self.lock_path)
        except Exception:
            # Avoid masking original errors
            pass
        return False

def _deepcopy_default() -> Dict[str, Any]:
    """
    Safe deep copy of DEFAULT_DATA to avoid shared references.
    """
    return json.loads(json.dumps(DEFAULT_DATA))

def load_data(path: str) -> Dict[str, Any]:
    """
    Load per-user data from local filesystem OR R2 (depending on RW_STORAGE_BACKEND).
    Keeps your existing schema heal/migration behaviour (v2/v3) to avoid breaking old users.
    """
    if _is_r2():
        key = _r2_key_for_path(path)
        data = _r2_get_json(key)

        if not isinstance(data, dict):
            data = _deepcopy_default()
            data.setdefault("meta", {})
            data["meta"].setdefault("schema_version", 3)
            data["meta"].setdefault("created_at", _now_iso())
            data["meta"]["updated_at"] = data["meta"]["created_at"]
            _r2_put_json(key, data)
            return data

        merged = _deepcopy_default()

        # Shallow merge top-level keys
        for k in merged.keys():
            if k in data:
                merged[k] = data[k]

        # Ensure meta/profile exist
        merged.setdefault("meta", {})
        merged.setdefault("profile", {})
        merged.setdefault("routes", {})
        merged.setdefault("history", [])

        # --- schema migrations/heal (keep consistent with your project) ---
        ver = int((merged.get("meta") or {}).get("schema_version") or 1)

        if ver < 2:
            prof = merged.setdefault("profile", {})
            prof.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
            prof.setdefault("pass", {"tier": "free", "status": "none", "starts_at": None, "ends_at": None, "source": "local", "notes": ""})
            prof.setdefault("entitlements", {"all_routes": False, "ai_basic": True, "ai_plus": False, "street_view": False})
            prof.setdefault("route_progress", {})
            merged.setdefault("meta", {})["schema_version"] = 2
            ver = 2

        if ver < 3:
            ensure_profile_v3(merged)
            merged.setdefault("meta", {})["schema_version"] = 3

        # Ensure user_key exists (legacy field; keep for compatibility if your app expects it)
        prof = merged.setdefault("profile", {})
        auth = prof.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
        uk = auth.get("user_key")
        if not isinstance(uk, str) or not uk.strip():
            auth["user_key"] = "u_" + uuid.uuid4().hex

        ensure_access_state(merged)
        ensure_profile_v3(merged)

        merged["meta"].setdefault("created_at", _now_iso())
        merged["meta"]["updated_at"] = _now_iso()

        # Write back healed version (important for keeping future reads stable)
        _r2_put_json(key, merged)
        return merged

    # -------- local filesystem mode --------
    if not os.path.exists(path):
        data = _deepcopy_default()
        data.setdefault("meta", {})
        data["meta"].setdefault("schema_version", 3)
        data["meta"].setdefault("created_at", _now_iso())
        data["meta"]["updated_at"] = data["meta"]["created_at"]
        atomic_write_json(path, data)
        return data

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = None

    if not isinstance(data, dict):
        data = _deepcopy_default()
        data.setdefault("meta", {})
        data["meta"].setdefault("schema_version", 3)
        data["meta"].setdefault("created_at", _now_iso())
        data["meta"]["updated_at"] = data["meta"]["created_at"]
        atomic_write_json(path, data)
        return data

    merged = _deepcopy_default()
    for k in merged.keys():
        if k in data:
            merged[k] = data[k]

    merged.setdefault("meta", {})
    merged.setdefault("profile", {})
    merged.setdefault("routes", {})
    merged.setdefault("history", [])

    ver = int((merged.get("meta") or {}).get("schema_version") or 1)

    if ver < 2:
        prof = merged.setdefault("profile", {})
        prof.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
        prof.setdefault("pass", {"tier": "free", "status": "none", "starts_at": None, "ends_at": None, "source": "local", "notes": ""})
        prof.setdefault("entitlements", {"all_routes": False, "ai_basic": True, "ai_plus": False, "street_view": False})
        prof.setdefault("route_progress", {})
        merged.setdefault("meta", {})["schema_version"] = 2
        ver = 2

    if ver < 3:
        ensure_profile_v3(merged)
        merged.setdefault("meta", {})["schema_version"] = 3

    prof = merged.setdefault("profile", {})
    auth = prof.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
    uk = auth.get("user_key")
    if not isinstance(uk, str) or not uk.strip():
        auth["user_key"] = "u_" + uuid.uuid4().hex

    ensure_access_state(merged)
    ensure_profile_v3(merged)

    merged["meta"].setdefault("created_at", _now_iso())
    merged["meta"]["updated_at"] = _now_iso()

    atomic_write_json(path, merged)
    return merged

def ensure_profile_v3(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase 4.1: introduce profile.v3 without breaking existing app.py.

    Principles:
      - Do NOT delete legacy keys (current_route_id, route_progress, etc.)
      - Create/repair profile.v3 structure
      - Best-effort migrate progress into v3 using existing route_progress/history
      - Decide initial mode:
          * if entitlements.all_routes True -> mode 'pro'
          * else -> mode 'free'
    """
    profile = data.setdefault("profile", {})

    # ensure Phase 3.3 keys exist (in case caller didn't go through v2 branch)
    profile.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
    profile.setdefault("pass", {"tier": "free", "status": "none", "starts_at": None, "ends_at": None, "source": "local", "notes": ""})
    profile.setdefault("entitlements", {"all_routes": False, "ai_basic": True, "ai_plus": False, "street_view": False})
    profile.setdefault("route_progress", {})

    v3 = profile.get("v3")
    if not isinstance(v3, dict):
        v3 = {}
        profile["v3"] = v3

    v3.setdefault("mode", "free")
    v3.setdefault("free", {})
    v3.setdefault("pro", {})

    free = v3["free"]
    pro = v3["pro"]

    if not isinstance(free, dict):
        free = {}
        v3["free"] = free
    if not isinstance(pro, dict):
        pro = {}
        v3["pro"] = pro

    free.setdefault("selected_route_id", None)
    free.setdefault("progress_km", {})
    if not isinstance(free["progress_km"], dict):
        free["progress_km"] = {}

    pro.setdefault("active", False)
    pro.setdefault("routes", {})
    if not isinstance(pro["routes"], dict):
        pro["routes"] = {}
    pro.setdefault("reward_state", "locked")
    pro.setdefault("finished_route_id", None)
    pro.setdefault("reward_choice_at", None)

    # ---- decide initial mode by entitlements ----
    ent = profile.get("entitlements", {})
    has_all = bool(ent.get("all_routes", False)) if isinstance(ent, dict) else False
    v3["mode"] = "pro" if has_all else "free"
    pro["active"] = bool(has_all)

    # ---- migrate selected free route ----
    # prefer existing v3 selection; else use legacy current_route_id if present
    if not free.get("selected_route_id"):
        legacy_cur = profile.get("current_route_id")
        if isinstance(legacy_cur, str) and legacy_cur.strip():
            free["selected_route_id"] = legacy_cur

    # ---- migrate progress into v3 ----
    # source of truth (best effort): profile.route_progress dict (already computed in app.py)
    rp = profile.get("route_progress", {})
    if isinstance(rp, dict):
        for rid, km in rp.items():
            try:
                kmf = float(km)
            except Exception:
                continue
            # store into free.progress_km as a general cache (harmless even if rid is pro)
            free["progress_km"].setdefault(rid, round(kmf, 3))

            # if pro active, also mirror into pro.routes (status default running)
            if pro.get("active"):
                rec = pro["routes"].get(rid)
                if not isinstance(rec, dict):
                    rec = {"km": 0.0, "status": "running", "finished_at": None}
                rec.setdefault("km", 0.0)
                rec.setdefault("status", "running")
                rec.setdefault("finished_at", None)
                rec["km"] = round(kmf, 3)
                pro["routes"][rid] = rec

    # If route_progress missing but history exists, we can lazily fill later in app.py;
    # keep v3 structure valid.

    return data


def _parse_date_yyyy_mm_dd(s: str) -> Optional[date]:
    try:
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except Exception:
        return None

def ensure_access_state(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Centralize pass validity + entitlements calculation.

    Rules (Phase 3.3.x):
      - If pass.status != 'active' -> all_routes False
      - If pass.status == 'active' and ends_at < today -> mark expired, all_routes False
      - If pass.status == 'active' and not expired -> all_routes True
      - Keep ai_basic default True, others False (future upgrades)
    """
    profile = data.setdefault("profile", {})
    profile.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
    p = profile.setdefault("pass", {
        "tier": "free",
        "status": "none",
        "starts_at": None,
        "ends_at": None,
        "source": "local",
        "notes": ""
    })
    ent = profile.setdefault("entitlements", {
        "all_routes": False,
        "ai_basic": True,
        "ai_plus": False,
        "street_view": False
    })

    today = date.today()

    # normalize missing keys
    if not isinstance(p, dict):
        p = {"tier": "free", "status": "none", "starts_at": None, "ends_at": None, "source": "local", "notes": ""}
        profile["pass"] = p
    if not isinstance(ent, dict):
        ent = {"all_routes": False, "ai_basic": True, "ai_plus": False, "street_view": False}
        profile["entitlements"] = ent

    status = p.get("status", "none")
    ends_at = p.get("ends_at")

    # expire check
    if status == "active":
        ends = _parse_date_yyyy_mm_dd(ends_at) if ends_at else None
        if ends is None:
            # ends_at missing/invalid => treat as not active for safety
            p["status"] = "expired"
            ent["all_routes"] = False
        elif ends < today:
            p["status"] = "expired"
            ent["all_routes"] = False
        else:
            ent["all_routes"] = True
    else:
        ent["all_routes"] = False

    # keep defaults for other entitlements (future)
    ent.setdefault("ai_basic", True)
    ent.setdefault("ai_plus", False)
    ent.setdefault("street_view", False)

    return data

def _parse_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _today_str() -> str:
    return date.today().isoformat()


def _recompute_profile_from_history(data: Dict[str, Any]) -> None:
    """
    Recompute total_km, last_run_date, streak_days from history for safety.
    """
    history: List[Dict[str, Any]] = data.get("history", [])
    if not history:
        data["profile"]["total_km"] = 0.0
        data["profile"]["last_run_date"] = None
        data["profile"]["streak_days"] = 0
        return

    # sum
    total = 0.0
    dates = set()
    for item in history:
        total += float(item.get("km", 0.0))
        d = item.get("date")
        if d:
            dates.add(d)

    data["profile"]["total_km"] = round(total, 3)

    # last date
    last_date = max(dates)
    data["profile"]["last_run_date"] = last_date

    # streak: count consecutive days ending at last_date
    streak = 0
    cur = _parse_yyyy_mm_dd(last_date)
    while cur.isoformat() in dates:
        streak += 1
        cur = cur - timedelta(days=1)

    data["profile"]["streak_days"] = streak


def add_run_km(
    data: Dict[str, Any],
    km: float,
    run_date: Optional[str] = None,
    mode: str = "merge",
    note: str = ""
) -> Dict[str, Any]:
    """
    Add a run record.

    mode:
      - "merge": same date => accumulate into that day's single record
      - "append": always append a new record (allow multiple runs per day)

    Returns updated data (mutates in-place too).
    """
    if km <= 0:
        raise ValueError("km must be > 0")

    if run_date is None:
        run_date = _today_str()

    route_id = data["profile"].get("current_route_id", "nj_bj")
    history: List[Dict[str, Any]] = data.get("history", [])

    if mode == "merge":
        # find existing same-date record for same route
        for item in history:
            if item.get("date") == run_date and item.get("route_id") == route_id:
                item["km"] = round(float(item.get("km", 0.0)) + km, 3)
                item["note"] = item.get("note", "") if not note else note
                break
        else:
            history.append({"date": run_date, "km": round(km, 3), "route_id": route_id, "note": note})
    elif mode == "append":
        history.append({"date": run_date, "km": round(km, 3), "route_id": route_id, "note": note})
    else:
        raise ValueError("mode must be 'merge' or 'append'")

    data["history"] = history
    _recompute_profile_from_history(data)

    data["meta"]["updated_at"] = _now_iso()
    return data
    
def add_run_km_pro(
    data: Dict[str, Any],
    km: float,
    run_date: Optional[str] = None,
    mode: str = "merge",
    note: str = ""
) -> Dict[str, Any]:
    """
    Phase 4.5.1
    Pro 模式：一次输入 km，同步推进所有 Pro 路线（profile.v3.pro.routes 里的所有 rid）

    - 会为每条 Pro 路线写入一条 history 记录（按 mode merge/append）
    - 会同步更新：
        * profile.route_progress[rid]
        * profile.v3.pro.routes[rid].km
    - 会复用 add_run_km 的全局 profile 重算逻辑（total_km / streak 等）
    """
    if km <= 0:
        raise ValueError("km must be > 0")

    # 确保 v3 结构存在（你的 load_data 已经会 heal，但这里再稳一层）
    ensure_profile_v3(data)

    profile = data.setdefault("profile", {})
    v3 = profile.setdefault("v3", {})
    pro = v3.setdefault("pro", {})
    routes = pro.setdefault("routes", {})

    if not isinstance(routes, dict) or len(routes) == 0:
        # 没有 Pro 路线就直接返回（Dashboard 会提示）
        return data

    # 统一日期
    if run_date is None:
        run_date = _today_str()

    # 逐条 Pro 路线写入跑量
    for rid in list(routes.keys()):
        # 1) 切换当前路线（复用 add_run_km 的写入逻辑）
        profile["current_route_id"] = rid

        # 2) 写入 history（同日同路线 merge）
        add_run_km(data, km=float(km), run_date=run_date, mode=mode, note=note)

        # 3) 立刻重算该路线的累计（以 history 为准）
        route_sum = 0.0
        for h in data.get("history", []):
            if h.get("route_id") == rid:
                try:
                    route_sum += float(h.get("km", 0.0))
                except Exception:
                    pass

        # 4) 同步到两个缓存位（给 app.py / Dashboard 用）
        profile.setdefault("route_progress", {})
        profile["route_progress"][rid] = round(route_sum, 3)

        rec = routes.get(rid)
        if not isinstance(rec, dict):
            rec = {"km": 0.0, "status": "running", "finished_at": None}
        rec.setdefault("status", "running")
        rec.setdefault("finished_at", None)
        rec["km"] = round(route_sum, 3)
        routes[rid] = rec

    # 写回
    pro["routes"] = routes
    v3["pro"] = pro
    profile["v3"] = v3
    data["profile"] = profile
    data["meta"]["updated_at"] = _now_iso()
    return data

def add_daily_km(
    data: Dict[str, Any],
    km: float,
    route_ids: List[str],
    run_date: Optional[str] = None,
    mode: str = "merge",
    note: str = ""
) -> Dict[str, Any]:
    """
    Phase 4.2: One input -> apply to multiple routes (broadcast).

    Implementation strategy:
      - Reuse add_run_km() by temporarily setting profile.current_route_id
      - After writing history, recompute per-route progress_km (route_progress + v3 mirrors)
    """
    if not route_ids:
        raise ValueError("route_ids must be non-empty")
    if km <= 0:
        raise ValueError("km must be > 0")

    profile = data.setdefault("profile", {})
    legacy_cur = profile.get("current_route_id")

    # write into history for each route
    for rid in route_ids:
        profile["current_route_id"] = rid
        add_run_km(data, km=float(km), run_date=run_date, mode=mode, note=note)

    # restore legacy current route id (best effort)
    if isinstance(legacy_cur, str) and legacy_cur.strip():
        profile["current_route_id"] = legacy_cur

    # recompute per-route progress from history
    rp = profile.setdefault("route_progress", {})
    hist: List[Dict[str, Any]] = data.get("history", [])
    for rid in route_ids:
        s = 0.0
        for h in hist:
            if h.get("route_id") == rid:
                try:
                    s += float(h.get("km", 0.0))
                except Exception:
                    pass
        rp[rid] = round(s, 3)

    # mirror into v3 caches if present
    v3 = profile.get("v3")
    if isinstance(v3, dict):
        free = v3.get("free")
        pro = v3.get("pro")
        if isinstance(free, dict):
            pk = free.setdefault("progress_km", {})
            if isinstance(pk, dict):
                for rid in route_ids:
                    pk[rid] = float(rp.get(rid, 0.0))
        if isinstance(pro, dict):
            proutes = pro.setdefault("routes", {})
            if isinstance(proutes, dict):
                for rid in route_ids:
                    rec = proutes.get(rid)
                    if not isinstance(rec, dict):
                        rec = {"km": 0.0, "status": "running", "finished_at": None}
                    rec.setdefault("status", "running")
                    rec.setdefault("finished_at", None)
                    rec["km"] = float(rp.get(rid, 0.0))
                    proutes[rid] = rec

    data["meta"]["updated_at"] = _now_iso()
    return data

from datetime import datetime

def check_pro_completion(
    data: Dict[str, Any],
    route_totals: Dict[str, float]
) -> Dict[str, Any]:
    """
    Phase 4.3
    Check whether any pro route has completed and trigger reward state.

    Rules:
    - Only works when profile.v3.mode == "pro"
    - Only triggers when reward_state == "locked"
    - First completed route wins (no multi-trigger)
    """
    profile = data.get("profile", {})
    v3 = profile.get("v3", {})

    if not isinstance(v3, dict):
        return data

    if v3.get("mode") != "pro":
        return data

    pro = v3.get("pro", {})
    if not isinstance(pro, dict):
        return data

    # 已进入奖励流程，直接退出（防止重复触发）
    if pro.get("reward_state") != "locked":
        return data

    routes = pro.get("routes", {})
    if not isinstance(routes, dict):
        return data

    for rid, rec in routes.items():
        if not isinstance(rec, dict):
            continue

        status = rec.get("status", "running")
        km = float(rec.get("km", 0.0))
        total = float(route_totals.get(rid, float("inf")))

        if status == "running" and km >= total:
            # 🎯 命中：第一条完成的 pro 路线
            rec["status"] = "finished"
            rec["finished_at"] = datetime.now().isoformat()

            pro["reward_state"] = "pending"
            pro["finished_route_id"] = rid

            return data

    return data

from datetime import datetime

def generate_reward_narrative(route_meta: dict) -> dict:
    """
    Phase 4.4
    Generate a narrative reward message for a completed Pro route.
    """
    route_name = route_meta.get("name", "这条路线")
    region = route_meta.get("region", "")
    tags = route_meta.get("narrative_tags", [])

    # 标题
    title = f"你完成了 {route_name}"

    # 叙事正文（轻文学，不夸张）
    body_lines = [
        f"这是一条横跨 {region} 的挑战路线。",
        "在持续的奔跑中，你把零散的日子，连成了一条清晰的轨迹。"
    ]

    if tags:
        body_lines.append(
            "这条路线的关键词是：" + "、".join(tags) + "。"
        )

    body_lines.append(
        "完成它，并不意味着终点，而是证明你已经具备继续向前的能力。"
    )

    return {
        "title": title,
        "body": "\n\n".join(body_lines),
        "created_at": datetime.now().isoformat()
    }

def save_data(path: str, data: Dict[str, Any]) -> None:
    data.setdefault("meta", {})
    data["meta"]["updated_at"] = _now_iso()

    if _is_r2():
        key = _r2_key_for_path(path)
        _r2_put_json(key, data)
        return

    atomic_write_json(path, data)

def recompute_profile(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Public wrapper: recompute profile fields from history.
    """
    _recompute_profile_from_history(data)
    data["meta"]["updated_at"] = _now_iso()
    return data


def delete_runs_by_date(
    data: Dict[str, Any],
    target_date: str,
    route_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Delete records on a given date.
    - If route_id is None: delete all routes on that date
    - Else: delete only that route on that date
    """
    history: List[Dict[str, Any]] = data.get("history", [])
    new_history: List[Dict[str, Any]] = []

    for item in history:
        if item.get("date") != target_date:
            new_history.append(item)
            continue
        # same date
        if route_id is None:
            # delete it
            continue
        if item.get("route_id") == route_id:
            continue
        new_history.append(item)

    data["history"] = new_history
    _recompute_profile_from_history(data)
    data["meta"]["updated_at"] = _now_iso()
    return data

def load_invites(path: str) -> Dict[str, Any]:
    """
    Invites are a shared object -> for R2 we load from invites/invites.json
    Return {} if missing/corrupt.
    """
    if _is_r2():
        key = _r2_key_for_path(path)
        data = _r2_get_json(key)
        return data if isinstance(data, dict) else {}

    if not os.path.exists(path):
        atomic_write_json(path, {})
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def save_invites(path: str, invites: Dict[str, Any]) -> None:
    if _is_r2():
        key = _r2_key_for_path(path)
        _r2_put_json(key, invites)
        return

    atomic_write_json(path, invites)

def mark_invite_used(invites: Dict[str, Any], code: str, activated_at_iso: str) -> None:
    """
    Mark an invite code as used (in-memory). Caller should save_invites afterwards.
    """
    rec = invites.get(code)
    if not isinstance(rec, dict):
        rec = {}
        invites[code] = rec
    rec["status"] = "used"
    if not rec.get("activated_at"):
        rec["activated_at"] = activated_at_iso
