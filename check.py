#!/usr/bin/env python3
import os
import re
import gzip
import csv
import logging
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt

# ---------------- USER SETTINGS ----------------
BASE_URL    = "https://io.saao.ac.za/IO/logarchive/lesedi"
OBSLOG_BASE = "https://io.saao.ac.za/Lesedi_ObsLogs"
OUTPUT_DIR  = "logs"
DERO_CSV    = "derotations.csv"
XY_CSV      = "xyresets.csv"

START_DATE      = "2025-07-15"  # YYYY-MM-DD
CLUSTER_SECONDS = 5
# ------------------------------------------------


def daterange(start_date, end_date):
    """Yield dates from start_date to end_date inclusive."""
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def fetch_and_save(url, dest_path):
    """Download URL to a local path if it exists. Return True if successful."""
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
    except requests.RequestException as e:
        print(f"Request error for {url}: {e}")
        return False


def cluster_events(times):
    """Cluster times so that events within CLUSTER_SECONDS are merged."""
    if not times:
        return []
    times = sorted(times)
    clustered = [times[0]]
    for t in times[1:]:
        if (t - clustered[-1]).total_seconds() > CLUSTER_SECONDS:
            clustered.append(t)
    return clustered


def parse_schedule_poller(path):
    """Extract derotation times and instrument mode from schedule_poller logs."""
    derotations = []
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if "Derotation occurred" in line:
            ts = datetime.fromisoformat(line.split()[0])
            mode = "UNKNOWN"
            for j in range(i, min(i + 5, len(lines))):
                m = re.search(r"Instrument mode\s*=\s*(\w+)", lines[j])
                if m:
                    mode = m.group(1)
                    break
            derotations.append((ts, mode))
    return derotations


def parse_xyslides(path):
    """Extract XY reset times from xyslides2ports logs."""
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
                    continue
                resets.append(ts)
    return resets


def science_count(date_obj):
    """
    Return number of rows with IMAGETYP='SCIENCE' in the nightly HTML log.
    Returns:
        None  -> no log page at all
        int   -> number of SCIENCE frames
    """
    ymd = date_obj.strftime("%Y%m%d")
    url = f"{OBSLOG_BASE}/{ymd}.html"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        if table is None:
            return 0
        rows = table.find_all('tr')[2:]  # skip header rows
        count = 0
        for row in rows:
            cols = [td.text.strip() for td in row.find_all('td')]
            if any(col == "SCIENCE" for col in cols):
                count += 1
        return count
    except Exception as e:
        logging.warning(f"ObsLog parse failed for {ymd}: {e}")
        return None


def main():
    start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end   = datetime.now(tz=timezone.utc).date()

    night_dero  = defaultdict(lambda: defaultdict(int))
    night_reset = defaultdict(int)

    no_log_days  = []   # True if no observing log exists
    low_sci_days = []   # True if <5 SCIENCE frames

    with open(DERO_CSV, "w", newline="") as fdero, open(XY_CSV, "w", newline="") as fxy:
        dero_writer = csv.writer(fdero)
        xy_writer   = csv.writer(fxy)
        dero_writer.writerow(["Date", "Time", "InstrumentMode"])
        xy_writer.writerow(["Date", "Time"])

        for d in daterange(start, end):
            day_str = d.strftime("%Y-%m-%d")
            day_dir = os.path.join(OUTPUT_DIR, day_str)
            os.makedirs(day_dir, exist_ok=True)

            # ---- Check observing log and count SCIENCE frames ----
            sci_count = science_count(d)
            if sci_count is None:
                no_log_days.append(True)
                low_sci_days.append(False)
            else:
                no_log_days.append(False)
                low_sci_days.append(sci_count < 5)

            sched_file = f"schedule_poller.log.{day_str}.gz"
            xy_file    = f"xyslides2ports.log.{day_str}.gz"

            # ---- Download and parse log files ----
            if fetch_and_save(f"{BASE_URL}/{day_str}/{sched_file}",
                              os.path.join(day_dir, sched_file)):
                deros = parse_schedule_poller(os.path.join(day_dir, sched_file))
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

    # ---- Plotting ----
    dates     = [d.strftime("%Y-%m-%d") for d in daterange(start, end)]
    xy_counts = [night_reset.get(day, 0) for day in dates]

    # XY resets with overlays
    plt.figure(figsize=(10, 4))
    plt.bar(dates, xy_counts, color="skyblue")
    for x, (no_log, low_sci) in enumerate(zip(no_log_days, low_sci_days)):
        if no_log:
            plt.bar(dates[x], max(xy_counts[x], 1),
                    color="red", alpha=0.7, width=0.3)
        elif low_sci:
            plt.bar(dates[x], max(xy_counts[x], 1),
                    color="green", alpha=0.7, width=0.3)
    plt.xticks(rotation=45, ha="right")
    plt.title("XY Resets per Night\nRed = No Log, Green = <5 SCIENCE frames")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig("xyresets_per_night.png")
    plt.close()

    # Derotations stacked with overlays
    modes = set(m for v in night_dero.values() for m in v)
    colors = plt.cm.tab10.colors
    plt.figure(figsize=(10, 4))
    bottom = [0] * len(dates)
    for i, mode in enumerate(sorted(modes)):
        counts = [night_dero.get(day, {}).get(mode, 0) for day in dates]
        plt.bar(dates, counts, bottom=bottom,
                color=colors[i % len(colors)], alpha=0.7, label=mode)
        bottom = [b + c for b, c in zip(bottom, counts)]
    # Overlay red/orange bars
    for x, (no_log, low_sci) in enumerate(zip(no_log_days, low_sci_days)):
        if no_log:
            plt.bar(dates[x], max(bottom[x], 1),
                    color="red", alpha=0.7, width=0.3)
        elif low_sci:
            plt.bar(dates[x], max(bottom[x], 1),
                    color="green", alpha=0.7, width=0.3)
    plt.xticks(rotation=45, ha="right")
    plt.title("Derotations per Night by Instrument Mode\n"
              "Red = No Obs Log, Green = <5 SCIENCE frames")
    plt.ylabel("Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig("derotations_per_night_by_mode.png")
    plt.close()

    print("\nSummary files written:\n  ", DERO_CSV, "\n  ", XY_CSV)
    print("Plots saved with red/orange overlays for nights with no or <5 SCIENCE frames.")


if __name__ == "__main__":
    main()
