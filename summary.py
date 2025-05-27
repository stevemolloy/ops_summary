import sys
import typing
from datetime import datetime as dt
from datetime import timedelta

from matplotlib import pyplot as plt

from DowntimeAppTool import delivery_data, downtime_data
from archiver_tool import get_data

def markdown_header(title: str, author: str, sink: typing.TextIO|None = None):
    if sink is None:
        sink = sys.stdout
    print("---", file = sink)
    print(f"title: {title}", file = sink)
    print(f"author: {author}", file = sink)
    print("theme: Malmoe", file=sink)
    print("---", file = sink)

def main(starttime: dt, endtime: dt):
    starttime = starttime.replace(hour=0, minute=0, second=0, microsecond=0)
    endtime = endtime.replace(hour=23, minute=59, second=59, microsecond=1_000_000 - 1)

    downtimes = [d for d in downtime_data if
        (d['date']>starttime and d['date']<endtime)
        and not d['archived']
    ]
    delivery = [d for d in delivery_data
        if (d['date']>starttime and d['date']<endtime)
    ] 

    R3_faults: list[dict] = [f for f in downtimes if f['machine']=="R3"]
    R1_faults: list[dict] = [f for f in downtimes if f['machine']=="R1"]
    I_faults: list[dict] = [f for f in downtimes if f['machine']=="I"]

    R3_N: int = len(R3_faults)
    R1_N: int = len(R1_faults)
    I_N: int = len(I_faults)

    R3_downtime: float = sum(d['duration'] for d in R3_faults) / 60
    R1_downtime: float = sum(d['duration'] for d in R1_faults) / 60
    I_downtime: float = sum(d['duration'] for d in I_faults) / 60

    R3_plan: list[int] = [d['R3plan'] for d in delivery]
    R1_plan: list[int] = [d['R1plan'] for d in delivery]
    I_plan: list[int] = [d['SPFplan'] for d in delivery]

    R3_del: int = sum(R3_plan)
    R1_del: int = sum(R1_plan)
    I_del: int = sum(I_plan)

    R3_uptime: float = (R3_del - R3_downtime) / R3_del
    R1_uptime: float = (R1_del - R1_downtime) / R1_del
    I_uptime: float = (I_del - I_downtime) / I_del

    R3_mtbf: float = R3_del / R3_N
    R1_mtbf: float = R1_del / R1_N
    I_mtbf: float = I_del / I_N

    R3_mttr: float = R3_downtime / R3_N
    R1_mttr: float = R1_downtime / R1_N
    I_mttr: float = I_downtime / I_N

    sink: typing.TextIO = sys.stdout

    title: str = f"Ops report for {starttime.date()} to {endtime.date()}"
    markdown_header(title=title, author="Stephen Molloy", sink=sink)

    print(f"# Summary", file=sink)
    print("")
    print(f"| Machine | Delivery | Downtime | Uptime | MTTR | MTBF (days) |")
    print(f"|---------|:--------:|:--------:|:------:|:----:|:-----------:|")
    print(f"| R3 | {R3_del} | {R3_downtime:0.2f} | {R3_uptime*100:0.2f} | {R3_mttr:0.2f} | {R3_mtbf/24:0.2f} |")
    print(f"| R1 | {R1_del} | {R1_downtime:0.2f} | {R1_uptime*100:0.2f} | {R1_mttr:0.2f} | {R1_mtbf/24:0.2f} |")
    print(f"| SPF| {I_del}  | {I_downtime:0.2f}  | {I_uptime*100:0.2f}  | {I_mttr:0.2f}  | {I_mtbf/24:0.2f}  |")

    print(f"- R3: MTBF = {R3_mtbf} hours", file=sink)
    print(f"- R1: MTBF = {R1_mtbf} hours", file=sink)
    print(f"- I: MTBF = {I_mtbf} hours", file=sink)
    print("", file=sink)

    R3_Ib_fname: str = "r3_ib.png"
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3, 4, 5, 6])
    plt.savefig(R3_Ib_fname)

    R1_Ib_fname: str = "r3_ib.png"
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3, 4, 5, 6])
    plt.savefig(R1_Ib_fname)

    I_Ib_fname: str = "r3_ib.png"
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3, 4, 5, 6])
    plt.savefig(I_Ib_fname)

    print(f"# R3 current", file=sink)
    print("", file=sink)
    print(f"![]({R3_Ib_fname})", file=sink)
    print("", file=sink)
    
    print(f"# R1 current", file=sink)
    print("", file=sink)
    print(f"![]({R1_Ib_fname})", file=sink)
    print("", file=sink)
    
    print(f"# I current", file=sink)
    print("", file=sink)
    print(f"![]({I_Ib_fname})", file=sink)
    print("", file=sink)
    
if __name__=="__main__":
    main(dt.now() - timedelta(days=28), dt.now())

