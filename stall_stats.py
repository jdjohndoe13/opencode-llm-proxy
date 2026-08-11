#!/usr/bin/env python3
"""Summarize the LLM proxy stall log (default: logs/stall.log).

Each line in the log is a JSON record per request written by the log phase
(see completions.lua log_request()). This script reports, across all requests:

  total request time - sum of total_s (time from request arrival to the last
                       byte returned to the client)
  total stall time   - sum of stall_s (time the proxy waited for the GPUs to
                       cool down before forwarding to the upstream)
"""

import argparse
import json


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("log", nargs="?", default="logs/stall.log")
    ap.add_argument("-j", "--json", action="store_true", help="emit a compact JSON summary")
    args = ap.parse_args()

    requests = 0
    total_s = 0.0
    stall_s = 0.0
    stalled = 0
    aborted = 0

    with open(args.log, encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                e = json.loads(raw)
            except ValueError:
                continue
            requests += 1
            total_s += float(e.get("total_s", 0.0))
            s = float(e.get("stall_s", 0.0))
            stall_s += s
            if s > 0:
                stalled += 1
            if e.get("aborted"):
                aborted += 1

    if args.json:
        print(json.dumps({
            "requests": requests,
            "total_s": round(total_s, 3),
            "stall_s": round(stall_s, 3),
            "stalled_requests": stalled,
            "aborted_requests": aborted,
        }))
        return

    print(f"requests:           {requests}")
    print(f"total request time: {total_s:.3f}s ({total_s / 60:.2f} min)")
    print(f"total stall time:   {stall_s:.3f}s ({stall_s / 60:.2f} min)")
    print(f"stalled requests:   {stalled}")
    print(f"aborted requests:   {aborted}")


if __name__ == "__main__":
    main()
