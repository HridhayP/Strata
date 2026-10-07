"""Summarizes bench-results/*.json: median run (by ops/s) per configuration.

    python scripts/summarize.py [results-dir]
"""
import collections
import glob
import json
import os
import re
import sys

root = sys.argv[1] if len(sys.argv) > 1 else "bench-results"
groups = collections.defaultdict(list)
for path in sorted(glob.glob(os.path.join(root, "*.json"))):
    name = re.sub(r"-r\d+\.json$", "", os.path.basename(path))
    groups[name].append(json.load(open(path)))

print("| config | runs | median ops/s | min-max ops/s | p50 ms | p99 ms | read p99 ms | write p99 ms | errors per run |")
print("|---|---|---|---|---|---|---|---|---|")
for name, runs in groups.items():
    runs.sort(key=lambda d: d["ops_per_sec"])
    m = runs[len(runs) // 2]
    ops = [d["ops_per_sec"] for d in runs]
    errs = ", ".join(str(d["errors"]) for d in runs)
    print(f"| {name} | {len(runs)} | {m['ops_per_sec']:,.0f} | {min(ops):,.0f}-{max(ops):,.0f} | "
          f"{m['p50_ms']:.2f} | {m['p99_ms']:.2f} | {m['read_p99_ms']:.2f} | {m['write_p99_ms']:.2f} | {errs} |")
