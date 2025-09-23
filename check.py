#!/usr/bin/env python3
import os
import re
import gzip
import csv
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import requests
import matplotlib.pyplot as plt

BASE_URL = "https://io.saao.ac.za/IO/logarchive/lesedi"
OUTPUT_DIR = "logs"
DERO_CSV = "derotations.csv"
XY_CSV = "xyresets.csv"

# ---- USER SETTINGS ----
START_DATE = "2025-09-01"    # <-- change if needed (YYYY-MM-DD)
CLUSTER_SECONDS = 5
# ------------------------

def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)

def fetch_and_save(url, dest_path):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(r.content)
        print(f"Downloaded {url}")
        return True
    except requests.HTTPError:
        print(f"Not found: {url}")
        return False

def cluster_events(times):
    """Return a list of representative times where events < CLUSTER_SECONDS apart are merged."""
    if not times:
        return []
    times = sorted(times)
    clustered = [times[0]]
    for t in times[1:]:
        if (t - clustered[-1]).total_seconds() > CLUSTER_SECONDS:
            clustered.append(t)
    return clustered

def parse_schedule_poller(path):
    derotations = []
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if "Derotation occurred" in line:
            ts = datetime.fromisoformat(line.split()[0])
            mode = "UNKNOWN"
            # Search next few lines for instrument mode
            for j in range(i, min(i+5, len(lines))):
                m = re.search(r"Instrument mode\s*=\s*(\w+)", lines[j])
                if m:
                    mode = m.group(1)
                    break
            derotations.append((ts, mode))
    return derotations


def parse_xyslides(path):
    resets = []
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if re.search(r"reset", line, re.IGNORECASE):
                parts = line.split()
                if not parts:
                    continue
                ts_str = parts[0]
                try:
                    ts = datetime.fromisoformat(ts_str)
                except ValueError:
                    # Line does not start with a valid timestamp, skip
                    continue
                resets.append(ts)
    return resets

def main():
    start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end = datetime.now(tz=timezone.utc).date()

    # For nightly counts
    night_dero = defaultdict(lambda: defaultdict(int))  # date -> mode -> count
    night_reset = defaultdict(int)

    with open(DERO_CSV, "w", newline="") as fdero, \
         open(XY_CSV, "w", newline="") as fxy:
        dero_writer = csv.writer(fdero)
        xy_writer = csv.writer(fxy)
        dero_writer.writerow(["Date", "Time", "InstrumentMode"])
        xy_writer.writerow(["Date", "Time"])

        for d in daterange(start, end):
            day_str = d.strftime("%Y-%m-%d")
            day_dir = os.path.join(OUTPUT_DIR, day_str)
            os.makedirs(day_dir, exist_ok=True)

            sched_file = f"schedule_poller.log.{day_str}.gz"
            xy_file = f"xyslides2ports.log.{day_str}.gz"

            # Download logs
            if fetch_and_save(f"{BASE_URL}/{day_str}/{sched_file}",
                              os.path.join(day_dir, sched_file)):
                deros = parse_schedule_poller(os.path.join(day_dir, sched_file))
                # Cluster by 5 sec and write
                if deros:
                    times_by_mode = defaultdict(list)
                    for ts, mode in deros:
                        times_by_mode[mode].append(ts)
                    for mode, times in times_by_mode.items():
                        clustered = cluster_events(times)
                        night_dero[day_str][mode] += len(clustered)
                        for ts in clustered:
                            dero_writer.writerow([day_str, ts.isoformat(), mode])

            if fetch_and_save(f"{BASE_URL}/{day_str}/{xy_file}",
                              os.path.join(day_dir, xy_file)):
                resets = parse_xyslides(os.path.join(day_dir, xy_file))
                clustered = cluster_events(resets)
                night_reset[day_str] += len(clustered)
                for ts in clustered:
                    xy_writer.writerow([day_str, ts.isoformat()])

    # ---- Plot histograms ----
    dates = [d.strftime("%Y-%m-%d") for d in daterange(start, end)]

    # XY resets per night (include zeroes)
    xy_counts = [night_reset.get(day, 0) for day in dates]
    plt.figure(figsize=(10,4))
    plt.bar(dates, xy_counts, color='skyblue')
    plt.xticks(rotation=45, ha="right")
    plt.title("Number of XY Resets per Night")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig("xyresets_per_night.png")
    plt.close()

    # Derotations per night per mode (stacked with transparency)
    modes = set(m for v in night_dero.values() for m in v)
    colors = plt.cm.tab10.colors  # Up to 10 colors
    plt.figure(figsize=(10,4))
    bottom = [0]*len(dates)
    for i, mode in enumerate(sorted(modes)):
        counts = [night_dero.get(day, {}).get(mode, 0) for day in dates]
        plt.bar(dates, counts, bottom=bottom, color=colors[i % len(colors)], alpha=0.7, label=mode)
        bottom = [b + c for b, c in zip(bottom, counts)]

    plt.xticks(rotation=45, ha="right")
    plt.title("Derotations per Night by Instrument Mode")
    plt.ylabel("Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig("derotations_per_night_by_mode.png")
    plt.close()

    print(f"\nSummary files written:\n  {DERO_CSV}\n  {XY_CSV}")
    print("Histograms saved as:")
    print("  xyresets_per_night.png")
    print("  derotations_per_night_by_mode.png")

if __name__ == "__main__":
    main()
