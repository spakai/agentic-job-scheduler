#!/usr/bin/python3
import re
import sys
from datetime import datetime
from pathlib import Path


def parse_log(path):
    rows = []
    partitions = set()
    partition_counts = [0] * 12
    seen_sequences = set()
    peak = 0
    for line in path.read_text().splitlines():
        if "loadMode=success" not in line:
            continue
        sequence = re.search(r"loadSequence=(\d+)", line)
        active = re.search(r"activeAtStart=(\d+)", line)
        peak_match = re.search(r"peakActive=(\d+)", line)
        partition = re.search(r"partition=(\d+)", line)
        timestamp = re.search(r"timestamp=(\S+)", line)
        if not all((sequence, active, peak_match, partition, timestamp)):
            continue
        sequence_value = int(sequence.group(1))
        active_value = int(active.group(1))
        peak_value = int(peak_match.group(1))
        partition_value = int(partition.group(1))
        timestamp_value = timestamp.group(1).replace("Z", "+00:00")
        try:
            time_value = datetime.fromisoformat(timestamp_value)
        except ValueError:
            continue
        peak = max(peak, peak_value)
        if sequence_value not in seen_sequences:
            seen_sequences.add(sequence_value)
            partitions.add(partition_value)
            partition_counts[partition_value] += 1
            rows.append((time_value, sequence_value, active_value))
    rows.sort(key=lambda row: row[0])
    return rows, partitions, partition_counts, peak


if len(sys.argv) != 4:
    raise SystemExit(
        "Usage: generate-two-main-pod-concurrency-graph.py POD1_LOG POD2_LOG OUTPUT.svg"
    )

pod1_rows, pod1_partitions, pod1_counts, pod1_peak = parse_log(Path(sys.argv[1]))
pod2_rows, pod2_partitions, pod2_counts, pod2_peak = parse_log(Path(sys.argv[2]))
output = Path(sys.argv[3])

width, height = 1600, 1000
left, right = 105, 1510
chart_width = right - left
panel_top = [150, 500]
panel_bottom = [390, 740]
concurrency_limits = [16, 8]

def x(sequence):
    return left + (sequence - 1) * chart_width / 1199

def y(active, top, bottom, scale_max):
    return bottom - active * (bottom - top) / scale_max

all_rows = pod1_rows + pod2_rows
if all_rows:
    common_start = min(row[0] for row in all_rows)
    common_end = max(row[0].timestamp() + 0.100 for row in all_rows)
else:
    common_start = datetime.now()
    common_end = common_start.timestamp() + 0.100

def time_x(time_value):
    elapsed_ms = (time_value.timestamp() - common_start.timestamp()) * 1000
    return left + elapsed_ms * chart_width / max(1, (common_end - common_start.timestamp()) * 1000)

svg = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1000" viewBox="0 0 1600 1000">',
    '<rect width="1600" height="1000" fill="#f8fafc"/>',
    '<style>',
    '.title{font:700 34px sans-serif;fill:#0f172a}',
    '.subtitle{font:18px sans-serif;fill:#475569}',
    '.label{font:16px sans-serif;fill:#334155}',
    '.tick{font:14px sans-serif;fill:#64748b}',
    '.value{font:700 28px sans-serif;fill:#0f172a}',
    '.small{font:700 15px sans-serif;fill:#334155}',
    '</style>',
    '<text x="80" y="65" class="title">Main-only demo: Pod A and Pod B concurrency</text>',
    '<text x="80" y="96" class="subtitle">1,200 randomized success records; Pod A=16 permits; Pod B=8 permits; each panel scaled to its limit</text>',
]

for pod_index, (rows, partitions, partition_counts, peak) in enumerate(
    ((pod1_rows, pod1_partitions, pod1_counts, pod1_peak),
     (pod2_rows, pod2_partitions, pod2_counts, pod2_peak)),
    start=1,
):
    top = panel_top[pod_index - 1]
    bottom = panel_bottom[pod_index - 1]
    concurrency_limit = concurrency_limits[pod_index - 1]
    scale_max = concurrency_limit * 1.25
    pod_label = "A (16 permits)" if pod_index == 1 else "B (8 permits)"
    svg.append('<text x="80" y="{}" class="label">Main worker {} - {} successful handlers, peakActive={} / limit={}</text>'.format(
        top - 20, pod_label, len(rows), peak, concurrency_limit
    ))
    for tick in range(0, concurrency_limit + 1, max(1, concurrency_limit // 4)):
        tick_y = y(tick, top, bottom, scale_max)
        svg.append('<line x1="105" y1="{:.1f}" x2="1510" y2="{:.1f}" stroke="#e2e8f0"/>'.format(tick_y, tick_y))
        svg.append('<text x="87" y="{:.1f}" text-anchor="end" class="tick">{}</text>'.format(tick_y + 5, tick))
    duration_ms = max(1, (common_end - common_start.timestamp()) * 1000)
    tick_step = max(100, round(duration_ms / 8 / 100) * 100)
    tick_ms = 0
    while tick_ms <= duration_ms:
        tick_x = left + tick_ms * chart_width / duration_ms
        svg.append('<text x="{:.1f}" y="{:.1f}" text-anchor="middle" class="tick">{}</text>'.format(
            tick_x, bottom + 28, "{} ms".format(tick_ms)
        ))
        tick_ms += tick_step
    limit_y = y(concurrency_limit, top, bottom, scale_max)
    svg.append('<line x1="105" y1="{:.1f}" x2="1510" y2="{:.1f}" stroke="#ef4444" stroke-width="2" stroke-dasharray="9 7"/>'.format(
        limit_y, limit_y
    ))
    events = []
    for time_value, sequence_value, active_value in rows:
        events.append((time_value.timestamp(), 1))
        events.append((time_value.timestamp() + 0.100, -1))
    events.sort()
    timeline_points = []
    active_count = 0
    for event_time, delta in events:
        active_count = max(0, active_count + delta)
        elapsed_ms = (event_time - common_start.timestamp()) * 1000
        event_x = left + elapsed_ms * chart_width / duration_ms
        timeline_points.append("{:.1f},{:.1f}".format(event_x, y(active_count, top, bottom, scale_max)))
    points = " ".join(timeline_points)
    svg.append('<polyline points="{}" fill="none" stroke="#2563eb" stroke-width="2" stroke-linejoin="round"/>'.format(points))

    partition_label = " ".join(
        "P{}={}".format(partition, partition_counts[partition])
        for partition in sorted(partitions)
    )
    svg.append('<text x="1510" y="{}" text-anchor="end" class="small">Owned: {}</text>'.format(
        top - 20, partition_label or "none"
    ))

svg.append('<text x="807" y="790" text-anchor="middle" class="label">Elapsed time (milliseconds)</text>')
svg.append('<text x="38" y="445" text-anchor="middle" class="label" transform="rotate(-90 38 445)">Active at handler entry (panel-specific scale)</text>')
svg.append('<text x="80" y="840" class="label">Partition ownership</text>')

for partition in range(12):
    bar_x = 180 + partition * 118
    owner1 = partition in pod1_partitions
    owner2 = partition in pod2_partitions
    color = "#2563eb" if owner1 else "#14b8a6"
    svg.append('<rect x="{}" y="870" width="88" height="55" rx="6" fill="{}" stroke="#cbd5e1"/>'.format(bar_x, color))
    owner = "Pod 1" if owner1 else ("Pod 2" if owner2 else "None")
    svg.append('<text x="{}" y="892" text-anchor="middle" class="small" fill="#ffffff">P{}</text>'.format(bar_x + 44, partition))
    svg.append('<text x="{}" y="910" text-anchor="middle" class="tick">{}</text>'.format(bar_x + 44, owner))

svg.append('<text x="80" y="970" class="tick">Source: one Docker log file per main-worker replica; activeAtStart is pod-local. Red dashed line: configured concurrency limit.</text>')
svg.append('</svg>')
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text("\n".join(svg))
print(output)
