#!/usr/bin/env python3
"""试机号同步(GA): 拉私有仓 suwei8/pinble-data 的 data/k3_full_history.csv
(Contents API, raw 媒体类型) -> 解析 Issue/ShiJiHao/KaiJiang -> POST /api/ga/sjh。

环境变量: API_BASE API_TOKEN PINBLE_TOKEN(可读 pinble-data 的 PAT)
"""
import csv
import io
import json
import os
import urllib.request

API = os.environ["API_BASE"].rstrip("/")
TOK = os.environ["API_TOKEN"]
GH = os.environ["PINBLE_TOKEN"]
REPO = "suwei8/pinble-data"
CSV_PATH = "data/k3_full_history.csv"


def fetch_csv():
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/contents/{CSV_PATH}",
        headers={"Authorization": f"Bearer {GH}", "User-Agent": "ga-sjh",
                 "Accept": "application/vnd.github.raw+json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def main():
    text = fetch_csv()
    rows = []
    for r in csv.DictReader(io.StringIO(text.lstrip("\ufeff"))):
        iss = (r.get("Issue") or "").strip()
        sjh = (r.get("ShiJiHao") or "").strip()
        kj = (r.get("KaiJiang") or "").strip()
        if iss.isdigit() and len(iss) == 7:
            rows.append({"issue": iss, "shijihao": sjh, "kaijiang": kj})
    body = json.dumps({"items": rows}).encode()
    req = urllib.request.Request(
        f"{API}/api/ga/sjh", data=body,
        headers={"X-GA-Token": TOK, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        print(f"parsed {len(rows)} rows -> {r.read().decode()}", flush=True)


if __name__ == "__main__":
    main()
