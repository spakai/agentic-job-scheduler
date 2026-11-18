#!/usr/bin/python3
import re
import sys
from pathlib import Path


def parse_log(path):
    rows = []
    partitions = set()
    peak = 0
    for line in path.read_text().splitlines():
        if "loadMode=success" not in line:
            continue
        sequence = re.search(r"loadSequence=(\d+)", line)
        active = re.search(r"activeAtStart=(\d+)", line)
        peak_match = re.search(r"peakActive=(\d+)", line)
        partition = re.search(r"partition=(\d+)", line)
        if not all((sequence, active, peak_match, partition)):
            continue
        sequence_value = int(sequence.group(1))
        active_value = int(active.group(1))
        peak_value = int(peak_match.group(1))
        partition_value = int(partition.group(1))
        peak = max(peak, peak_value)
        partitions.add(partition_value)
        rows.append((sequence_value, active_value))
    rows.sort()
    return rows, partitions, peak


if len(sys.argv) != 4:
    raise SystemExit(
        "Usage: generate-two-main-pod-concurrency-graph.py POD1_LOG POD2_LOG OUTPUT.svg"
    )

pod1_rows, pod1_partitions, pod1_peak = parse_log(Path(sys.argv[1]))
pod2_rows, pod2_partitions, pod2_peak = parse_log(Path(sys.argv[2]))
output = Path(sys.argv[3])

width, height = 1600, 1000
left, right = 105, 1510
chart_width = right - left
panel_top = [150, 500]
panel_bottom = [390, 740]
max_active = max(16, pod1_peak, pod2_peak)

def x(sequence):
    return left + (sequence - 1) * chart_width / 1199

def y(active, top, bottom):
    return bottom - active * (bottom - top) / max_active

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
    '<text x="80" y="65" class="title">Two main-worker pods: concurrency and partition ownership</text>',
    '<text x="80" y="96" class="subtitle">1,200 success records, 600 handlers per pod, P0-P11 split across both consumers</text>',
]

for pod_index, (rows, partitions, peak) in enumerate(
    ((pod1_rows, pod1_partitions, pod1_peak),
     (pod2_rows, pod2_partitions, pod2_peak)),
    start=1,
):
    top = panel_top[pod_index - 1]
    bottom = panel_bottom[pod_index - 1]
    svg.append('<text x="80" y="{}" class="label">Main worker {} - {} successful handlers, peakActive={}</text>'.format(
        top - 20, pod_index, len(rows), peak
    ))
    for tick in range(0, max_active + 1, max(1, max_active // 4)):
        tick_y = y(tick, top, bottom)
        svg.append('<line x1="105" y1="{:.1f}" x2="1510" y2="{:.1f}" stroke="#e2e8f0"/>'.format(tick_y, tick_y))
        svg.append('<text x="87" y="{:.1f}" text-anchor="end" class="tick">{}</text>'.format(tick_y + 5, tick))
    for sequence in (1, 200, 400, 600, 800, 1000, 1200):
        tick_x = x(sequence)
        svg.append('<text x="{:.1f}" y="{:.1f}" text-anchor="middle" class="tick">{}</text>'.format(
            tick_x, bottom + 28, sequence
        ))
    peak_y = y(peak, top, bottom)
    svg.append('<line x1="105" y1="{:.1f}" x2="1510" y2="{:.1f}" stroke="#ef4444" stroke-width="2" stroke-dasharray="9 7"/>'.format(
        peak_y, peak_y
    ))
    points = " ".join("{:.1f},{:.1f}".format(x(sequence), y(active, top, bottom))
                      for sequence, active in rows)
    svg.append('<polyline points="{}" fill="none" stroke="#2563eb" stroke-width="2" stroke-linejoin="round"/>'.format(points))

    partition_label = " ".join("P{}".format(partition) for partition in sorted(partitions))
    svg.append('<text x="1510" y="{}" text-anchor="end" class="small">Owned: {}</text>'.format(
        top - 20, partition_label or "none"
    ))

svg.append('<text x="807" y="790" text-anchor="middle" class="label">Handler start sequence</text>')
svg.append('<text x="38" y="445" text-anchor="middle" class="label" transform="rotate(-90 38 445)">Active at handler entry</text>')
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

svg.append('<text x="80" y="970" class="tick">Source: one Docker log file per main-worker replica; activeAtStart is a pod-local handler-entry snapshot.</text>')
svg.append('</svg>')
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text("\n".join(svg))
print(output)
