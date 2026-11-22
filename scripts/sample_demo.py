#!/usr/bin/env python3
"""月报抽查名单演示：生成 → 归档更正（清单换版）→ 增量更新 → 失败留痕。

只通过 HTTP API 操作服务，适合在 Docker Compose 里对运行中的容器执行：

    docker compose exec pm25-planner python scripts/sample_demo.py generate
    docker compose exec pm25-planner python scripts/sample_demo.py correct
    docker compose exec pm25-planner python scripts/sample_demo.py break-manifest

``correct`` 会向档案新增一个 2026-01 的 Parquet 文件，然后用
“临时文件 + os.replace”原子替换清单，再调用 /api/samples/update：
- 输出本次实读了哪些文件；
- 输出相对上次成功名单的新增/撤回样本；
- 调用 /api/samples/audit 做全量重抽核对。

``break-manifest`` 故意把清单里某个文件的 size 改错，演示“文件与清单不符”
时更新失败、上次成功名单仍保留；随后 ``restore`` 可恢复正确清单。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PM25 = "pm25"


def call(base_url: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base_url.rstrip("/") + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def read_manifest(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_write_manifest(path: Path, payload: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def spec_payload(args) -> dict:
    return {
        "station": args.station,
        "month": args.month,
        "exceed_quota": args.exceed_quota,
        "missing_quota": args.missing_quota,
        "exceed_seed": args.exceed_seed,
        "missing_seed": args.missing_seed,
    }


def show_sample(body: dict) -> None:
    sample = body["sample"]
    print(f"  快照 {sample['snapshot_id']} · 生成于 {sample['generated_at']}")
    print(f"  合格记录：超标 {sample['eligible_counts']['exceed']} / "
          f"缺测 {sample['eligible_counts']['missing']}")
    for kind, label in (("exceed", "超标"), ("missing", "缺测")):
        print(f"  {label}类名单（{len(sample['candidates'][kind])} 条）：")
        for i, c in enumerate(sample["candidates"][kind], 1):
            print(f"    {i}. 读数 {c['value']!s:<6} 行号 {c['row_number']:>2}  "
                  f"{c['path']}  size={c['size']} mtime_ns={c['mtime_ns']}  "
                  f"key={c['draw_key'][:12]}…")


def show_diff(diff: dict) -> None:
    print(f"  本次实读：{diff['read_files'] or '（无）'}")
    print(f"  复用未变文件：{len(diff['reused_files'])} 个")
    for kind, label in (("exceed", "超标"), ("missing", "缺测")):
        for c in diff["added"][kind]:
            print(f"  + 新增[{label}] 读数 {c['value']} 行号 {c['row_number']} {c['path']}")
        for c in diff["withdrawn"][kind]:
            print(f"  - 撤回[{label}] 读数 {c['value']} 行号 {c['row_number']} {c['path']}")


def cmd_generate(args) -> int:
    status, body = call(args.base_url, "POST", "/api/samples", spec_payload(args))
    if status != 200:
        print("生成失败：", json.dumps(body, ensure_ascii=False, indent=2))
        return 1
    print("已生成并保存名单：")
    show_sample(body)
    return 0


def cmd_update(args) -> int:
    status, body = call(args.base_url, "POST", "/api/samples/update", spec_payload(args))
    if status != 200:
        print("更新失败（上次成功名单已保留）：",
              json.dumps(body, ensure_ascii=False, indent=2))
        return 1
    print(f"更新成功：{body['message']}")
    show_sample(body)
    if body.get("diff"):
        show_diff(body["diff"])
    status, audit = call(args.base_url, "POST", "/api/samples/audit", spec_payload(args))
    if status == 200:
        print(f"  全量重抽核对：{'逐条一致 ✅' if audit['equivalent'] else '不一致 ⛔'}")
        for d in audit["differences"]:
            print("    " + d)
    return 0


def _new_file(args, name: str, values: list[float | None]) -> Path:
    path = args.archive / f"station={args.station}/date={args.month}-15/{name}"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({PM25: pa.array(values, type=pa.float64())}), path,
        compression="snappy",
    )
    return path


def cmd_correct(args) -> int:
    """模拟归档更正：新增一个文件并原子换版清单，然后增量更新名单。"""
    new_file = _new_file(args, "part-corrected.parquet",
                         [61.0, None, 120.0, 15.0, 88.0, None])
    rel = str(new_file.relative_to(args.archive)).replace("\\", "/")
    st = new_file.stat()
    payload = read_manifest(args.manifest)
    payload["version"] = int(payload.get("version", 1)) + 1
    payload["files"] = [f for f in payload["files"] if f["path"] != rel]
    payload["files"].append({"path": rel, "size": st.st_size, "mtime_ns": st.st_mtime_ns})
    atomic_write_manifest(args.manifest, payload)
    print(f"已新增 {rel}（{st.st_size} 字节）并原子替换清单到 version={payload['version']}")
    return cmd_update(args)


def cmd_break_manifest(args) -> int:
    """故意制造“文件与清单不符”，演示更新失败且保留上次成功名单。"""
    payload = read_manifest(args.manifest)
    if not payload.get("files"):
        print("清单为空，无法演示")
        return 1
    victim = payload["files"][0]
    original = victim["size"]
    victim["size"] = original + 1
    payload["version"] = int(payload.get("version", 1)) + 1
    atomic_write_manifest(args.manifest, payload)
    print(f"已把 {victim['path']} 的清单 size 从 {original} 改为 {original + 1}（文件未变）")
    status, body = call(args.base_url, "POST", "/api/samples/update", spec_payload(args))
    detail = body.get("detail", body)
    print(f"更新返回 HTTP {status} · {detail.get('reason_codes')}")
    print("  上次成功名单：", detail.get("last_success_snapshot_id"),
          detail.get("last_success_generated_at"))
    status, last = call(
        args.base_url, "GET",
        f"/api/samples/last?station={args.station}&month={args.month}",
    )
    if status == 200:
        print("  仍可查看上次成功名单：快照", last["sample"]["snapshot_id"],
              "· 最近尝试", last.get("last_attempt", {}).get("outcome"))
    return 0


def cmd_restore(args) -> int:
    payload = read_manifest(args.manifest)
    fixed = 0
    for entry in payload["files"]:
        full = args.archive / entry["path"]
        if not full.exists():
            continue
        st = full.stat()
        if entry.get("size") != st.st_size or entry.get("mtime_ns") != st.st_mtime_ns:
            entry["size"] = st.st_size
            entry["mtime_ns"] = st.st_mtime_ns
            fixed += 1
    payload["version"] = int(payload.get("version", 1)) + 1
    atomic_write_manifest(args.manifest, payload)
    print(f"已按磁盘实际文件修正 {fixed} 条并原子替换清单到 version={payload['version']}")
    return 0


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description="月报抽查名单演示")
    ap.add_argument("command", choices=["generate", "update", "correct", "break-manifest", "restore"])
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--archive", type=Path, default=Path(os.environ.get("PM25_ARCHIVE_DIR", root / "data" / "archive")))
    ap.add_argument("--manifest", type=Path, default=Path(os.environ.get(
        "PM25_MANIFEST_PATH", root / "data" / "manifest" / "manifest.json")))
    ap.add_argument("--station", default="station-A")
    ap.add_argument("--month", default="2026-01")
    ap.add_argument("--exceed-quota", type=int, default=3)
    ap.add_argument("--missing-quota", type=int, default=2)
    ap.add_argument("--exceed-seed", default="2026-01-A-exceed")
    ap.add_argument("--missing-seed", default="2026-01-A-missing")
    args = ap.parse_args()
    args.archive = args.archive.resolve()
    args.manifest = args.manifest.resolve()

    handler = {
        "generate": cmd_generate,
        "update": cmd_update,
        "correct": cmd_correct,
        "break-manifest": cmd_break_manifest,
        "restore": cmd_restore,
    }[args.command]
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
