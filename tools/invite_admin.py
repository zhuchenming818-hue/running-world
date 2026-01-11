import json
import os
import sys
import secrets
from datetime import date

INVITES_PATH = os.path.join("data", "invites.json")
PREFIX = "RW-ALPHA"
CODE_LEN = 3  # 001, 002...


def _load() -> dict:
    if not os.path.exists(INVITES_PATH):
        os.makedirs(os.path.dirname(INVITES_PATH), exist_ok=True)
        with open(INVITES_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False, indent=2)
        return {}
    with open(INVITES_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _save(data: dict) -> None:
    with open(INVITES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _next_index(existing_codes: list[str]) -> int:
    mx = 0
    for c in existing_codes:
        # RW-ALPHA-001
        parts = c.split("-")
        if len(parts) >= 3 and parts[-1].isdigit():
            mx = max(mx, int(parts[-1]))
    return mx + 1


def gen(n: int) -> None:
    data = _load()
    existing = set(data.keys())
    idx = _next_index(list(existing))

    created = []
    for _ in range(n):
        code = f"{PREFIX}-{idx:0{CODE_LEN}d}"
        # 防撞（理论上不撞，但保底）
        while code in existing:
            idx += 1
            code = f"{PREFIX}-{idx:0{CODE_LEN}d}"

        data[code] = {
            "status": "new",
            "issued_to": "",
            "issued_at": "",
            "activated_at": ""
        }
        existing.add(code)
        created.append(code)
        idx += 1

    _save(data)
    print(f"✅ generated {len(created)} codes")
    for c in created[:20]:
        print(" ", c)
    if len(created) > 20:
        print(f" ... and {len(created)-20} more")


def revoke(code: str) -> None:
    data = _load()
    rec = data.get(code)
    if not isinstance(rec, dict):
        print("❌ code not found:", code)
        return
    rec["status"] = "revoked"
    data[code] = rec
    _save(data)
    print("✅ revoked:", code)


def issue(code: str, issued_to: str) -> None:
    data = _load()
    rec = data.get(code)
    if not isinstance(rec, dict):
        print("❌ code not found:", code)
        return
    if rec.get("status") != "new":
        print("❌ code not new (cannot issue):", code, "status=", rec.get("status"))
        return
    rec["issued_to"] = issued_to
    rec["issued_at"] = date.today().isoformat()
    data[code] = rec
    _save(data)
    print("✅ issued:", code, "to", issued_to)


def export_new() -> None:
    data = _load()
    codes = [c for c, rec in data.items() if isinstance(rec, dict) and rec.get("status") == "new"]
    codes.sort()
    print("\n".join(codes))
    print(f"\n✅ total new: {len(codes)}")


def usage():
    print(
        "Usage:\n"
        "  python tools/invite_admin.py gen <N>\n"
        "  python tools/invite_admin.py revoke <CODE>\n"
        "  python tools/invite_admin.py issue <CODE> <ISSUED_TO>\n"
        "  python tools/invite_admin.py export\n"
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "gen" and len(sys.argv) == 3:
        gen(int(sys.argv[2]))
    elif cmd == "revoke" and len(sys.argv) == 3:
        revoke(sys.argv[2])
    elif cmd == "issue" and len(sys.argv) >= 4:
        code = sys.argv[2]
        issued_to = " ".join(sys.argv[3:])
        issue(code, issued_to)
    elif cmd == "export":
        export_new()
    else:
        usage()
        sys.exit(1)
