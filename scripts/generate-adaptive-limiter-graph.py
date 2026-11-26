#!/usr/bin/env python3
import re
import sys
from datetime import datetime
from pathlib import Path


WINDOW = re.compile(
    r"Adaptive limiter window role=main.*?"
    r"windowEpochMillis=(\d+).*?"
    r"completedAttempts=(\d+).*?"
    r"p95HandlerMillis=(\d+).*?"
    r"transientFailureRate=([0-9.]+).*?"
    r"backlogPresent=(true|false).*?"
    r"externalPressure=(true|false).*?"
    r"handlerTimeout=(true|false).*?"
    r"(?:slowLatencyEnabled=(true|false).*?"
    r"externalPressureEnabled=(true|false).*?)?"
    r"reason=([A-Z_]+).*?"
    r"qualifyingHealthyWindows=(\d+).*?"
    r"minimumClamp=(true|false).*?"
    r"limitBefore=(\d+).*?"
    r"limitAfter=(\d+)"
)
HANDLER = re.compile(
    r"timestamp=([0-9TZ:.-]+).*?loadMode=success.*?"
    r"demoRunId=(\S+).*?externalResponse=(\S+).*?"
    r"externalPressureAccepted=(true|false).*?activeAtStart=(\d+)"
)


def fail(message):
    raise SystemExit(message)


if len(sys.argv) != 6:
    fail("Usage: generate-adaptive-limiter-graph.py WINDOWS PHASES HANDLERS OUTPUT.svg SUMMARY.txt")

window_file, phase_file, handler_file, output_file, summary_file = map(Path, sys.argv[1:])
windows = []
for line in window_file.read_text().splitlines():
    match = WINDOW.search(line)
    if match:
        (epoch, completed, p95, failure_rate, backlog, external_pressure,
         handler_timeout, slow_latency_enabled, external_pressure_enabled,
         reason, healthy_windows, minimum_clamp, before,
         after) = match.groups()
        windows.append({
            "epoch": int(epoch),
            "completed": int(completed),
            "p95": int(p95),
            "failure_rate": float(failure_rate),
            "backlog": backlog == "true",
            "external_pressure": external_pressure == "true",
            "handler_timeout": handler_timeout == "true",
            "slow_latency_enabled": slow_latency_enabled != "false",
            "external_pressure_enabled": external_pressure_enabled != "false",
            "reason": reason,
            "healthy_windows": int(healthy_windows),
            "minimum_clamp": minimum_clamp == "true",
            "before": int(before),
            "after": int(after),
        })

phases = []
for line in phase_file.read_text().splitlines()[1:]:
    name, run_id, start, end, records, unique_handlers, lag_rows, delay = line.split("\t")
    phases.append({
        "name": name,
        "run_id": run_id,
        "start": int(start),
        "end": int(end),
        "records": int(records),
        "unique_handlers": int(unique_handlers),
        "lag_rows": int(lag_rows),
        "delay": int(delay),
    })

phase_names = {phase["name"] for phase in phases}
expected_phase_names = {"warmup", "recovery"}
if "external-responses" in phase_names:
    expected_phase_names.add("external-responses")
if "pressure" in phase_names:
    expected_phase_names.add("pressure")
if (not windows or phase_names != expected_phase_names
        or not ({"external-responses", "pressure"} & phase_names)):
    fail("Expected warmup/recovery plus external-responses, pressure, or both")
if any(phase["records"] != phase["unique_handlers"] or phase["lag_rows"] != 0
       for phase in phases):
    fail("Every phase must have all unique handlers complete and zero final lag")

for window in windows:
    window["phase"] = next(
        (phase["name"] for phase in phases if phase["start"] <= window["epoch"] <= phase["end"]),
        None,
    )

def qualifying(phase, predicate):
    return [
        window for window in windows
        if window["phase"] == phase
        and window["completed"] >= 20
        and window["backlog"]
        and predicate(window)
    ]

warmup_rises = qualifying("warmup", lambda window:
                          window["p95"] <= 1000 and window["after"] > window["before"])
external_falls = [
    window for window in windows
    if window["phase"] == "external-responses"
    and window["external_pressure"]
    and window["external_pressure_enabled"]
    and window["reason"] == "EXTERNAL_PRESSURE"
    and window["completed"] < 20
    and window["after"] < window["before"]
]
pressure_falls = qualifying("pressure", lambda window:
                            window["slow_latency_enabled"]
                            and window["p95"] > 1000
                            and window["reason"] == "HIGH_LATENCY"
                            and window["after"] < window["before"])
recovery_rises = qualifying("recovery", lambda window:
                            window["p95"] <= 1000 and window["after"] > window["before"])
if not warmup_rises:
    fail("Warmup did not show a healthy, backlog-driven limit increase")
if "external-responses" in phase_names and not external_falls:
    fail("External responses did not bypass the sample floor with a typed pressure decrease")
if "pressure" in phase_names and not pressure_falls:
    fail("Pressure did not show a >=20-sample slow-window limit decrease")
if not recovery_rises:
    fail("Recovery did not show a healthy, backlog-driven limit increase")

handler_points = []
external_outcomes = set()
run_to_phase = {phase["run_id"]: phase["name"] for phase in phases}
for line in handler_file.read_text().splitlines():
    match = HANDLER.search(line)
    if match:
        timestamp, run_id, external_response, accepted, active = match.groups()
        if run_id in run_to_phase:
            epoch = int(datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp() * 1000)
            handler_points.append((epoch, int(active)))
            if run_to_phase[run_id] == "external-responses" and accepted == "true":
                external_outcomes.add(external_response)
if not handler_points:
    fail("No matching handler-start concurrency measurements found")
expected_external_outcomes = {"rest-503", "rest-timeout", "database-timeout"}
if "external-responses" in phase_names and external_outcomes != expected_external_outcomes:
    fail("External-response phase did not emit all reviewed REST/DB outcomes")

start = min(phase["start"] for phase in phases)
end = max(phase["end"] for phase in phases)
if end <= start:
    fail("Invalid phase timestamps")

width, height = 1500, 1000
left, right = 120, 1440
chart_width = right - left
panels = [(170, 390), (470, 690), (770, 900)]

def x(epoch):
    return left + (epoch - start) * chart_width / (end - start)

limit_max = max(16, max(max(window["before"], window["after"]) for window in windows))
p95_max = max(1500, max(window["p95"] for window in windows))
active_max = max(8, max(active for _, active in handler_points))

def y(value, panel, maximum):
    top, bottom = panel
    return bottom - value * (bottom - top) / maximum

signal_description = (
    "aggregate and external pressure" if phase_names == {
        "warmup", "external-responses", "pressure", "recovery"
    } else "typed external pressure" if "external-responses" in phase_names
    else "aggregate p95 pressure"
)
svg = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="1500" height="1000" viewBox="0 0 1500 1000">',
    '<rect width="1500" height="1000" fill="#f8fafc"/>',
    '<style>.title{font:700 30px sans-serif;fill:#0f172a}.sub{font:17px sans-serif;fill:#475569}',
    '.label{font:700 18px sans-serif;fill:#334155}.tick{font:13px sans-serif;fill:#64748b}',
    '.phase{font:700 14px sans-serif;fill:#334155}</style>',
    '<text x="70" y="55" class="title">Main-worker adaptive concurrency: {}</text>'.format(
        signal_description
    ),
    '<text x="70" y="85" class="sub">Five-second windows; typed external signals bypass the 20-sample floor.</text>',
]
phase_colors = {
    "warmup": "#dcfce7",
    "external-responses": "#fef3c7",
    "pressure": "#fee2e2",
    "recovery": "#dbeafe",
}
for phase in phases:
    px = x(phase["start"])
    pw = max(1, x(phase["end"]) - px)
    for top, bottom in panels:
        svg.append(
            '<rect x="{:.1f}" y="{}" width="{:.1f}" height="{}" fill="{}" opacity="0.55"/>'.format(
                px, top, pw, bottom - top, phase_colors[phase["name"]]
            )
        )
    svg.append('<text x="{:.1f}" y="145" class="phase">{}</text>'.format(
        px + 8, "{} ({} ms)".format(phase["name"], phase["delay"])
    ))

panel_info = [
    ("Configured per-process limit", limit_max, panels[0]),
    ("p95 handler latency (ms)", p95_max, panels[1]),
    ("Active at handler entry", active_max, panels[2]),
]
for label, maximum, panel in panel_info:
    top, bottom = panel
    svg.append('<text x="70" y="{}" class="label">{}</text>'.format(top - 18, label))
    for tick in range(0, maximum + 1, max(1, maximum // 4)):
        ty = y(tick, panel, maximum)
        svg.append('<line x1="{}" y1="{:.1f}" x2="{}" y2="{:.1f}" stroke="#cbd5e1"/>'.format(
            left, ty, right, ty
        ))
        svg.append('<text x="{}" y="{:.1f}" text-anchor="end" class="tick">{}</text>'.format(
            left - 12, ty + 4, tick
        ))

limit_points = []
for index, window in enumerate(windows):
    if window["epoch"] < start or window["epoch"] > end:
        continue
    point_x = x(window["epoch"])
    limit_points.extend([
        "{:.1f},{:.1f}".format(point_x, y(window["before"], panels[0], limit_max)),
        "{:.1f},{:.1f}".format(point_x, y(window["after"], panels[0], limit_max)),
    ])
if len(limit_points) < 2:
    fail("No limiter windows fall within the recorded phase interval")
svg.append('<polyline points="{}" fill="none" stroke="#2563eb" stroke-width="4"/>'.format(
    " ".join(limit_points)
))

p95_points = [
    "{:.1f},{:.1f}".format(x(window["epoch"]), y(window["p95"], panels[1], p95_max))
    for window in windows if start <= window["epoch"] <= end
]
svg.append('<polyline points="{}" fill="none" stroke="#ea580c" stroke-width="3"/>'.format(
    " ".join(p95_points)
))
threshold_y = y(1000, panels[1], p95_max)
svg.append('<line x1="{}" y1="{:.1f}" x2="{}" y2="{:.1f}" stroke="#b91c1c" stroke-dasharray="7 6"/>'.format(
    left, threshold_y, right, threshold_y
))
svg.append('<text x="{}" y="{:.1f}" class="tick" fill="#b91c1c">1,000 ms target</text>'.format(
    right - 110, threshold_y - 6
))

active_points = [
    "{:.1f},{:.1f}".format(x(epoch), y(active, panels[2], active_max))
    for epoch, active in handler_points if start <= epoch <= end
]
svg.append('<polyline points="{}" fill="none" stroke="#0f766e" stroke-width="2" opacity="0.65"/>'.format(
    " ".join(active_points)
))

tick_count = 6
for index in range(tick_count + 1):
    epoch = start + (end - start) * index / tick_count
    tx = x(epoch)
    svg.append('<line x1="{:.1f}" y1="{}" x2="{:.1f}" y2="{}" stroke="#94a3b8"/>'.format(
        tx, panels[2][1], tx, panels[2][1] + 6
    ))
    svg.append('<text x="{:.1f}" y="928" text-anchor="middle" class="tick">+{}s</text>'.format(
        tx, round((epoch - start) / 1000)
    ))
svg.append('<text x="780" y="954" text-anchor="middle" class="tick">Elapsed time from warmup publication</text>')
svg.append('<text x="70" y="980" class="tick">Handler points are start-time activeAtStart snapshots; each pod has its own adaptive limit.</text>')
svg.append('</svg>')
output_file.parent.mkdir(parents=True, exist_ok=True)
output_file.write_text("\n".join(svg))

summary_lines = [
    "Adaptive limiter showcase evidence",
    "Initial limit: {} (one main-worker pod)".format(
        next(window["before"] for window in windows if window["phase"] == "warmup")
    ),
    "Warmup increase: {} -> {} at p95={} ms, completed={}.".format(
        warmup_rises[0]["before"], warmup_rises[0]["after"],
        warmup_rises[0]["p95"], warmup_rises[0]["completed"]
    ),
    *([
        "Pressure decrease: {} -> {} at p95={} ms, completed={}.".format(
            pressure_falls[0]["before"], pressure_falls[0]["after"],
            pressure_falls[0]["p95"], pressure_falls[0]["completed"]
        )
    ] if pressure_falls else []),
    *([
        "External-response decrease: {} -> {} at completed={} (<20), reason={}.".format(
            external_falls[0]["before"], external_falls[0]["after"],
            external_falls[0]["completed"], external_falls[0]["reason"]
        ),
        "External outcomes observed: {}.".format(", ".join(sorted(external_outcomes))),
    ] if external_falls else []),
    "Recovery increase: {} -> {} at p95={} ms, completed={}.".format(
        recovery_rises[0]["before"], recovery_rises[0]["after"],
        recovery_rises[0]["p95"], recovery_rises[0]["completed"]
    ),
    "Phases: " + ", ".join(
        "{}={} unique handlers, final lag rows={}, delay={} ms".format(
            phase["name"], phase["unique_handlers"], phase["lag_rows"], phase["delay"]
        )
        for phase in phases
    ),
    "Windows recorded: {}; handler start observations: {}.".format(
        len(windows), len(handler_points)
    ),
    "This is a per-process limit demonstration, not a cluster-wide budget or global ordering test.",
]
summary_file.parent.mkdir(parents=True, exist_ok=True)
summary_file.write_text("\n".join(summary_lines) + "\n")
print("\n".join(summary_lines))
