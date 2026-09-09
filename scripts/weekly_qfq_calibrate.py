# -*- coding: utf-8 -*-
"""前复权缓存周度校准脚本(技术方案 V1.1 T4)

流程(对每只代码):
  1. GET /api/kline-qfq      —— 新接口,吃缓存
  2. GET /api/kline          —— 旧接口,每次全量回源,权威值
  3. 比对共同日期全字段;缓存尾部多出的 bar(原始域追加)属预期,不算不一致
  4. 不一致 -> GET /api/kline-qfq?refresh=1 强制回源重建缓存(自动修正)
  5. 输出 Markdown 报告

用法:
  python3 weekly_qfq_calibrate.py                          # 全市场
  python3 weekly_qfq_calibrate.py --codes 000001,600519    # 指定代码
  python3 weekly_qfq_calibrate.py --limit 100              # 前 100 只(冒烟)
"""
import argparse
import datetime
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = os.environ.get("TDX_API_BASE", "http://127.0.0.1:8081")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(SCRIPT_DIR, "reports")
FIELDS = ["Open", "High", "Low", "Close", "Volume", "Amount"]

# 绕过本机代理
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get(path, timeout=120):
    with _opener.open(BASE + path, timeout=timeout) as r:
        return json.loads(r.read())


def get_codes():
    resp = get("/api/codes")
    data = resp["data"]
    codes = []
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                codes.extend(str(x) for x in v)
            else:
                codes.append(str(v))
    else:
        codes = [str(x) for x in data]
    # 只保留 6 位数字的股票代码,跳过指数/板块等
    codes = sorted({c[-6:] for c in codes if len(c) >= 6 and c[-6:].isdigit()
                    and (c[-6:].startswith(("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688")))})
    return codes


def calibrate_one(code):
    """返回 (code, status, detail)。status: ok / fixed / error"""
    try:
        cached = get(f"/api/kline-qfq?code={code}&type=day")["data"]["List"]
        fresh = get(f"/api/kline?code={code}&type=day")["data"]["List"]
    except Exception as e:
        return code, "error", str(e)[:200]

    c_map = {x["Time"][:10]: x for x in cached}
    f_map = {x["Time"][:10]: x for x in fresh}
    common = set(c_map) & set(f_map)
    diffs = []
    for d in sorted(common):
        a, b = c_map[d], f_map[d]
        for k in FIELDS:
            if a[k] != b[k]:
                diffs.append((d, k, a[k], b[k]))
                break
    if not diffs:
        return code, "ok", f"共同{len(common)}条一致"

    # 不一致 -> refresh=1 强制回源重建
    detail = f"不一致{len(diffs)}条(首:{diffs[0][0]} {diffs[0][1]} {diffs[0][2]}!={diffs[0][3]})"
    try:
        get(f"/api/kline-qfq?code={code}&type=day&refresh=1")
        fixed = get(f"/api/kline-qfq?code={code}&type=day")["data"]["List"]
        fx_map = {x["Time"][:10]: x for x in fixed}
        remain = [d for d in common if any(fx_map[d][k] != f_map[d][k] for k in FIELDS)]
        if not remain:
            return code, "fixed", detail + " -> refresh 后已修正"
        return code, "error", detail + f" -> refresh 后仍有{len(remain)}条不一致"
    except Exception as e:
        return code, "error", detail + f" -> 修正失败: {str(e)[:120]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", help="逗号分隔的代码列表")
    ap.add_argument("--limit", type=int, default=0, help="只校准前 N 只")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    else:
        codes = get_codes()
    if args.limit > 0:
        codes = codes[: args.limit]

    os.makedirs(REPORT_DIR, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report = os.path.join(REPORT_DIR, f"weekly_qfq_calibrate_report_{ts}.md")

    total = len(codes)
    start = time.time()
    ok, fixed, error = [], [], []
    print(f"校准开始: {total} 只, {args.workers} 并发", flush=True)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(calibrate_one, c): c for c in codes}
        done = 0
        for fut in as_completed(futs):
            code, status, detail = fut.result()
            done += 1
            if status == "ok":
                ok.append(code)
            elif status == "fixed":
                fixed.append((code, detail))
            else:
                error.append((code, detail))
            if done % 200 == 0 or done == total:
                print(f"[{done}/{total}] ok={len(ok)} fixed={len(fixed)} error={len(error)} 耗时={time.time()-start:.0f}s", flush=True)

    cost = time.time() - start
    lines = [
        f"# 前复权缓存周度校准报告",
        f"",
        f"> 时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 服务: {BASE} | 总耗时: {cost:.0f}s",
        f"",
        f"| 结果 | 数量 |",
        f"|---|---|",
        f"| 一致 | {len(ok)} |",
        f"| 已自动修正 | {len(fixed)} |",
        f"| 异常 | {len(error)} |",
        f"",
    ]
    if fixed:
        lines += ["## 已自动修正（不一致 -> refresh=1 重建后一致）", ""]
        lines += [f"- `{c}` {d}" for c, d in fixed] + [""]
    if error:
        lines += ["## 异常（需人工复核）", ""]
        lines += [f"- `{c}` {d}" for c, d in error] + [""]
    with open(report, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"校准完成: ok={len(ok)} fixed={len(fixed)} error={len(error)} 耗时={cost:.0f}s")
    print(f"报告: {report}")
    return 0 if not error else 2


if __name__ == "__main__":
    sys.exit(main())
