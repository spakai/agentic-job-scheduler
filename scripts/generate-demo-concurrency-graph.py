#!/usr/bin/python3
import re
import sys
from datetime import datetime
from pathlib import Path


def required_match(pattern, text, field):
    match = re.search(pattern, text)
    if not match:
        raise ValueError("Missing {} in handler log".format(field))
    return match.group(1)


if len(sys.argv) != 4:
    raise SystemExit("Usage: generate-demo-concurrency-graph.py LOG_FILE RUN_ID OUTPUT.svg")

log_file = Path(sys.argv[1])
run_id = sys.argv[2]
output = Path(sys.argv[3])

rows = []
seen_sequences = set()
partition_counts = [0] * 12
for line in log_file.read_text().splitlines():
    if "demoRunId={}".format(run_id) not in line:
        continue
    sequence = int(required_match(r"loadSequence=(\d+)", line, "loadSequence"))
    active = int(required_match(r"activeAtStart=(\d+)", line, "activeAtStart"))
    peak = int(required_match(r"peakActive=(\d+)", line, "peakActive"))
    partition = int(required_match(r"partition=(\d+)", line, "partition"))
    if sequence not in seen_sequences:
        seen_sequences.add(sequence)
        rows.append((sequence, active, peak))
        if 0 <= partition < 12:
            partition_counts[partition] += 1

rows.sort()
if len(rows) != 1200:
    raise SystemExit("Expected 1200 matching handler lines, found {}".format(len(rows)))

width, height = 1600, 900
left, right, top, bottom = 105, 1510, 190, 590
chart_width = right - left
chart_height = bottom - top
max_active = max(16, max((peak for _, _, peak in rows), default=16))

def x(sequence):
    return left + (sequence - 1) * chart_width / 1199

def y(active):
    return bottom - active * chart_height / max_active

svg = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="900" viewBox="0 0 1600 900">',
    '<rect width="1600" height="900" fill="#f8fafc"/>',
    '<style>',
    '.title{font:700 34px sans-serif;fill:#0f172a}',
    '.subtitle{font:18px sans-serif;fill:#475569}',
    '.label{font:16px sans-serif;fill:#334155}',
    '.tick{font:14px sans-serif;fill:#64748b}',
    '.value{font:700 28px sans-serif;fill:#0f172a}',
    '.small{font:700 15px sans-serif;fill:#334155}',
    '</style>',
    '<text x="80" y="65" class="title">1,200-record live concurrency and partition distribution</text>',
    '<text x="80" y="96" class="subtitle">Run ID {}</text>'.format(run_id),
]

cards = [
    (80, "1,200", "success handlers"),
    (400, "16", "peak active"),
    (720, "100 ms", "demo handler hold"),
    (1040, "12", "partitions"),
    (1360, "0", "partition lag"),
]
for x_card, value, label in cards:
    card_width = 160 if x_card == 1360 else 220
    svg.append('<rect x="{}" y="120" width="{}" height="52" rx="10" fill="#ffffff" stroke="#cbd5e1"/>'.format(x_card, card_width))
    svg.append('<text x="{}" y="143" class="value">{}</text>'.format(x_card + 12, value))
    svg.append('<text x="{}" y="163" class="small">{}</text>'.format(x_card + 12, label))

for tick in range(0, max_active + 1, max(1, max_active // 4)):
    tick_y = y(tick)
    svg.append('<line x1="105" y1="{:.1f}" x2="1510" y2="{:.1f}" stroke="#e2e8f0"/>'.format(tick_y, tick_y))
    svg.append('<text x="87" y="{:.1f}" text-anchor="end" class="tick">{}</text>'.format(tick_y + 5, tick))
for sequence in (1, 200, 400, 600, 800, 1000, 1200):
    tick_x = x(sequence)
    svg.append('<text x="{:.1f}" y="625" text-anchor="middle" class="tick">{}</text>'.format(tick_x, sequence))

svg.append('<text x="807.5" y="662" text-anchor="middle" class="label">Handler start sequence</text>')
svg.append('<text x="38" y="390" text-anchor="middle" class="label" transform="rotate(-90 38 390)">Active at handler entry</text>')
peak = max((peak for _, _, peak in rows), default=16)
peak_y = y(peak)
svg.append('<line x1="105" y1="{:.1f}" x2="1510" y2="{:.1f}" stroke="#ef4444" stroke-width="2" stroke-dasharray="9 7"/>'.format(peak_y, peak_y))
svg.append('<text x="1502" y="{:.1f}" text-anchor="end" class="small" fill="#b91c1c">peak {}</text>'.format(peak_y - 8, peak))

points = " ".join("{:.1f},{:.1f}".format(x(sequence), y(active)) for sequence, active, _ in rows)
svg.append('<polyline points="{}" fill="none" stroke="#2563eb" stroke-width="2" stroke-linejoin="round"/>'.format(points))

svg.append('<text x="80" y="640" class="label">Successful handlers by Kafka partition</text>')
bar_start = (width - (12 * 95 + 11 * 22)) / 2
max_partition_count = max(partition_counts, default=1)
for partition, count in enumerate(partition_counts):
    bar_x = bar_start + partition * 117
    bar_height = count / max_partition_count * 100
    bar_y = 820 - bar_height
    svg.append('<rect x="{:.1f}" y="{:.1f}" width="95" height="{:.1f}" rx="5" fill="#14b8a6"/>'.format(bar_x, bar_y, bar_height))
    svg.append('<text x="{:.1f}" y="{:.1f}" text-anchor="middle" class="small">{}</text>'.format(bar_x + 47.5, bar_y - 10, count))
    svg.append('<text x="{:.1f}" y="848" text-anchor="middle" class="tick">P{}</text>'.format(bar_x + 47.5, partition))

svg.append('<text x="80" y="884" class="tick">Source: matching DEMO1200 handler log; activeAtStart is a handler-entry snapshot.</text>')
svg.append('</svg>')
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text("\n".join(svg))
print(output)
