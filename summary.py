#!/usr/bin/env python

import sys
import typing
from datetime import datetime as dt
from datetime import timedelta
import time

from DowntimeAppTool import delivery_data, downtime_data

def markdown_header(title: str, author: str, sink: typing.TextIO|None = None):
    if sink is None:
        sink = sys.stdout
    print("---", file = sink)
    print(f"title: {title}", file = sink)
    print(f"author: {author}", file = sink)
    print("theme: Malmoe", file=sink)
    print("---", file = sink)

def main(starttime: dt, endtime: dt, file: typing.TextIO):
    starttime = starttime.replace(hour=0, minute=0, second=0, microsecond=0)
    endtime = endtime.replace(hour=23, minute=59, second=59, microsecond=1_000_000 - 1)

    downtimes: list[dict] = [d for d in downtime_data if
        (d['date']>starttime and d['date']<endtime)
        and not d['archived']
    ]
    delivery: list[dict] = [d for d in delivery_data
        if (d['date']>starttime and d['date']<endtime)
    ] 

    R3_faults: list[dict] = [f for f in downtimes if f['machine']=="R3"]
    R1_faults: list[dict] = [f for f in downtimes if f['machine']=="R1"]
    I_faults: list[dict] = [f for f in downtimes if f['machine']=="I"]

    R3_N: int = len(R3_faults)
    R1_N: int = len(R1_faults)
    I_N: int = len(I_faults)

    R3_dt: float = sum(d['duration'] for d in R3_faults) / 60
    R1_dt: float = sum(d['duration'] for d in R1_faults) / 60
    I_dt: float = sum(d['duration'] for d in I_faults) / 60

    R3_plan: list[int] = [d['R3plan'] for d in delivery]
    R1_plan: list[int] = [d['R1plan'] for d in delivery]
    I_plan: list[int] = [d['SPFplan'] for d in delivery]

    R3_del: int = sum(R3_plan)
    R1_del: int = sum(R1_plan)
    I_del: int = sum(I_plan)

    R3_ut: float = (R3_del - R3_dt) / R3_del
    R1_ut: float = (R1_del - R1_dt) / R1_del
    I_ut: float = (I_del - I_dt) / I_del

    R3_mtbf: float = R3_del / R3_N
    R1_mtbf: float = R1_del / R1_N
    I_mtbf: float = I_del / I_N

    R3_mttr: float = R3_dt / R3_N
    R1_mttr: float = R1_dt / R1_N
    I_mttr: float = I_dt / I_N

    title: str = f"Ops report for {starttime.date()} to {endtime.date()}"
    markdown_header(title=title, author="Stephen Molloy", sink=sink)

    print(f"# Summary {starttime.date()} to {endtime.date()}", file=sink)
    print("", file=sink)
    print(f"| Machine | Delivery | Downtime | Uptime | MTTR | MTBF (days) |", file=sink)
    print(f"|---------|:--------:|:--------:|:------:|:----:|:-----------:|", file=sink)
    print(f"| R3 | {R3_del} | {R3_dt:0.2f} | {R3_ut*100:0.2f} | {R3_mttr:0.2f} | {R3_mtbf/24:0.2f} |", file=sink)
    print(f"| R1 | {R1_del} | {R1_dt:0.2f} | {R1_ut*100:0.2f} | {R1_mttr:0.2f} | {R1_mtbf/24:0.2f} |", file=sink)
    print(f"| SPF| {I_del}  | {I_dt:0.2f}  | {I_ut*100:0.2f}  | {I_mttr:0.2f}  | {I_mtbf/24:0.2f}  |", file=sink)
    print("", file=sink)

    for n, dump in enumerate(R3_faults):
        print(f"# {dump['machine']} downtime #{n+1}", file=sink)
        print("", file=sink)
        print(f"- Code: {dump['code']}", file=sink)
        print(f"- {dump['date'].strftime("%Y-%m-%d %H:%M:%S")}", file=sink)
        print(f"- Duration: {dump['duration']} minutes", file=sink)
        print(f"- {dump['description']}", file=sink)
        print("", file=sink)

    for n, dump in enumerate(R1_faults):
        print(f"# {dump['machine']} downtime #{n+1}", file=sink)
        print("", file=sink)
        print(f"- Code: {dump['code']}", file=sink)
        print(f"- {dump['date'].strftime("%Y-%m-%d %H:%M:%S")}", file=sink)
        print(f"- Duration: {dump['duration']} minutes", file=sink)
        print(f"- {dump['description']}", file=sink)
        print("", file=sink)

    for n, dump in enumerate(I_faults):
        desc: str = dump['description'].replace("\\", "/")
        print(f"# SPF downtime #{n+1}", file=sink)
        print("", file=sink)
        print(f"- Code: {dump['code']}", file=sink)
        print(f"- {dump['date'].strftime("%Y-%m-%d %H:%M:%S")}", file=sink)
        print(f"- Duration: {dump['duration']} minutes", file=sink)
        print(f"- {desc}", file=sink)
        print("", file=sink)

if __name__=="__main__":
    WEEKS_SINCE_LAST_MEETING = 5
    sink: typing.TextIO = sys.stdout

    main(dt.now() - timedelta(days=WEEKS_SINCE_LAST_MEETING*7), dt.now(), file=sink)

