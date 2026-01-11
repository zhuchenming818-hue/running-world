from __future__ import annotations

import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional


DEFAULT_DATA: Dict[str, Any] = {
    "meta": {
        "schema_version": 2,
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
        "route_progress": {}
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


def load_data(path: str) -> Dict[str, Any]:
    """
    Load JSON, auto-heal missing keys with DEFAULT_DATA.
    If file doesn't exist or corrupted, return a fresh copy.
    """
    if not os.path.exists(path):
        data = json.loads(json.dumps(DEFAULT_DATA))
        data["meta"]["created_at"] = _now_iso()
        data["meta"]["updated_at"] = data["meta"]["created_at"]
        atomic_write_json(path, data)
        return data

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        # corrupted file: backup and reset
        try:
            os.replace(path, path + ".corrupted")
        except OSError:
            pass
        data = json.loads(json.dumps(DEFAULT_DATA))
        data["meta"]["created_at"] = _now_iso()
        data["meta"]["updated_at"] = data["meta"]["created_at"]
        atomic_write_json(path, data)
        return data

    # merge defaults (shallow+some nested)
    merged = json.loads(json.dumps(DEFAULT_DATA))
    merged.update({k: data.get(k, merged[k]) for k in merged.keys()})

    # nested merges
    merged["meta"].update(data.get("meta", {}))
    merged["profile"].update(data.get("profile", {}))

    # routes: keep user's routes if present, else defaults
    merged["routes"] = data.get("routes", merged["routes"])
    merged["history"] = data.get("history", merged["history"])
    # --- Phase 3.3 schema migration: v1 -> v2 ---
    old_ver = int(merged.get("meta", {}).get("schema_version", 1) or 1)
    if old_ver < 2:
        prof = merged.setdefault("profile", {})

        prof.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})
        prof.setdefault("pass", {
            "tier": "free",
            "status": "none",
            "starts_at": None,
            "ends_at": None,
            "source": "local",
            "notes": ""
        })
        prof.setdefault("entitlements", {
            "all_routes": False,
            "ai_basic": True,
            "ai_plus": False,
            "street_view": False
        })
        prof.setdefault("route_progress", {})

        merged.setdefault("meta", {})["schema_version"] = 2
    # --- Phase 3.3.3: ensure stable anonymous user_key ---
    prof = merged.setdefault("profile", {})
    auth = prof.setdefault("auth", {"mode": "local", "invite_code": None, "user_key": None})

    uk = auth.get("user_key")
    if not isinstance(uk, str) or not uk.strip():
        # stable anonymous id, e.g. u_2f7c1b3a9d4e4f0aa1c2d3e4f5a6b7c8
        auth["user_key"] = "u_" + uuid.uuid4().hex
    # --- Phase 3.3.4: centralize pass/entitlements ---
    ensure_access_state(merged)

    # ensure meta timestamps
    if not merged["meta"].get("created_at"):
        merged["meta"]["created_at"] = _now_iso()
    merged["meta"]["updated_at"] = _now_iso()

    # write back healed version
    atomic_write_json(path, merged)
    return merged


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


def save_data(path: str, data: Dict[str, Any]) -> None:
    data["meta"]["updated_at"] = _now_iso()
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

def load_invites(path: str = INVITES_DEFAULT_PATH) -> Dict[str, Any]:
    """
    Load invites.json safely. If missing, create an empty one.
    Structure:
      { "CODE": {"status":"new|used|revoked", "issued_to":"", "issued_at":"", "activated_at":""}, ... }
    """
    if not os.path.exists(path):
        atomic_write_json(path, {})
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        # corrupted file: backup and reset
        try:
            os.replace(path, path + ".corrupted")
        except OSError:
            pass
        atomic_write_json(path, {})
        return {}


def save_invites(path: str, invites: Dict[str, Any]) -> None:
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
