#!/usr/bin/env python3
"""
Analyze simulation log files to identify performance bottlenecks.
Looks at timestamps to find where slowdowns occur.
"""

import re
import sys
from datetime import datetime
from collections import defaultdict


def parse_timestamp(line):
    """Extract timestamp from log line."""
    match = re.match(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})", line)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
    return None


def parse_log_level(line):
    """Extract log level from line."""
    match = re.search(r"\d\t(INFO|WARN|ERROR|DEBUG)\t", line)
    if match:
        return match.group(1)
    return None


def parse_event_type(line):
    """Extract event type from line."""
    match = re.search(r"\t(INFO|WARN|ERROR|DEBUG)\t(\w+)\t", line)
    if match:
        return match.group(2)
    return None


def analyze_log(filepath):
    """Analyze a log file and report timing information."""

    timestamps = []
    events = defaultdict(int)
    config_changes = []
    time_gaps = []

    prev_time = None
    prev_line = None

    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            ts = parse_timestamp(line)
            if ts is None:
                continue

            timestamps.append((ts, line_num, line.strip()[:200]))

            # Track event types
            event_type = parse_event_type(line)
            if event_type:
                events[event_type] += 1

            # Track config changes
            if "ConfigId=" in line or "config" in line.lower():
                config_match = re.search(r"[Cc]onfig[^0-9]*(\d+)", line)
                if config_match:
                    config_changes.append((ts, int(config_match.group(1)), line_num))

            # Track time gaps
            if prev_time:
                gap = (ts - prev_time).total_seconds()
                if gap > 60:  # Gaps > 1 minute
                    time_gaps.append(
                        (prev_time, ts, gap, prev_line, line.strip()[:100])
                    )

            prev_time = ts
            prev_line = line.strip()[:100]

    return timestamps, events, config_changes, time_gaps


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_log.py <logfile>")
        sys.exit(1)

    filepath = sys.argv[1]
    print(f"Analyzing: {filepath}\n")

    timestamps, events, config_changes, time_gaps = analyze_log(filepath)

    if not timestamps:
        print("No timestamps found in log file!")
        return

    # Basic stats
    start_time = timestamps[0][0]
    end_time = timestamps[-1][0]
    total_duration = (end_time - start_time).total_seconds()

    print("=" * 60)
    print("TIMING SUMMARY")
    print("=" * 60)
    print(f"Start time (simulated):  {start_time}")
    print(f"End time (simulated):    {end_time}")
    print(
        f"Total simulated time:    {total_duration:.1f} seconds ({total_duration / 3600:.2f} hours)"
    )
    print(f"Total log lines:         {len(timestamps)}")
    print()

    # Config change analysis
    print("=" * 60)
    print("CONFIGURATION CHANGES")
    print("=" * 60)
    if config_changes:
        configs = [c[1] for c in config_changes]
        min_config = min(configs)
        max_config = max(configs)
        print(f"Config version range:    {min_config} -> {max_config}")
        print(f"Total config changes:    {max_config - min_config}")
        print(
            f"Config changes per hour: {(max_config - min_config) / (total_duration / 3600):.1f}"
        )
    print()

    # Time gaps analysis
    print("=" * 60)
    print("LARGE TIME GAPS (>1 minute)")
    print("=" * 60)
    if time_gaps:
        for prev_ts, curr_ts, gap, prev_line, curr_line in time_gaps[:10]:
            print(f"\nGap: {gap:.1f} seconds ({gap / 60:.1f} minutes)")
            print(f"  From: {prev_ts}")
            print(f"  To:   {curr_ts}")
            print(f"  Before: {prev_line[:80]}...")
            print(f"  After:  {curr_line[:80]}...")
    else:
        print("No large time gaps found.")
    print()

    # Event frequency analysis
    print("=" * 60)
    print("TOP 20 EVENT TYPES BY FREQUENCY")
    print("=" * 60)
    sorted_events = sorted(events.items(), key=lambda x: -x[1])
    for event, count in sorted_events[:20]:
        rate = count / (total_duration / 3600) if total_duration > 0 else 0
        print(f"  {event:40s} {count:8d} ({rate:.1f}/hour)")
    print()

    # Identify busy periods
    print("=" * 60)
    print("ACTIVITY BY TIME PERIOD")
    print("=" * 60)

    # Group by 10-minute periods
    period_counts = defaultdict(int)
    for ts, _, _ in timestamps:
        period = ts.replace(minute=(ts.minute // 10) * 10, second=0, microsecond=0)
        period_counts[period] += 1

    sorted_periods = sorted(period_counts.items(), key=lambda x: -x[1])
    print("\nBusiest 10-minute periods:")
    for period, count in sorted_periods[:10]:
        print(f"  {period}: {count} events")

    print("\nQuietest 10-minute periods:")
    for period, count in sorted_periods[-5:]:
        print(f"  {period}: {count} events")


if __name__ == "__main__":
    main()
