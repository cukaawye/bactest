"""Vapor paper-trade stats. Run: python stats.py [path-to-db]
Prints closed-trade P&L by exit reason and config, plus gate-eval tallies.
"""
import json
import os
import sqlite3
import sys
from collections import defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "vapor.db")

db = sqlite3.connect(PATH)
q = lambda s, *a: db.execute(s, a).fetchall()

closed = q("""SELECT exit_reason, gross_ret, net_ret, config_id  FROM paper_trades WHERE state='CLOSED'""")
open_n = q("SELECT COUNT(*) FROM paper_trades WHERE state='OPEN'")[0][0]

print("== closed trades ==")
by_reason = defaultdict(list)
for why, gross, net, cid in closed:
    by_reason[why].append((gross, net, cid))
for why in ("sl", "rec", "tp", "end"):
    rows = by_reason.get(why, [])
    if not rows:
        print(f"{why:>6}: 0")
        continue
    nets, grosses = [r[1] for r in rows], [r[0] for r in rows]
    wr = sum(1 for n in nets if n > 0) / len(nets)
    print(f"{why:>6}: n={len(rows):4d}  wr={wr:4.0%}  avg_net={sum(nets)/len(nets):+8.3f}  sum_net={sum(nets):+9.2f}")

total_net = sum(r[1] for r in closed)
print(f"TOTAL closed: n={len(closed)}  sum_net={total_net:+.2f}")

print("\n== by config ==")
by_cfg = defaultdict(list)
for why, gross, net, cid in closed:
    by_cfg[cid].append(net)
for cid in sorted(by_cfg):
    nets = by_cfg[cid]
    wr = sum(1 for n in nets if n > 0) / len(nets)
    print(f"cfg {cid:>3}: n={len(nets):4d}  wr={wr:4.0%}  sum_net={sum(nets):+9.2f}")

print("\n== open ==")
print(f"{open_n} trades still OPEN (hold=0 trades only close via sl/rec/tp)")

print("\n== gate evaluations ==")
rows = q("""SELECT COALESCE(reason,'') , COUNT(*) FROM evaluations GROUP BY reason ORDER BY 2 DESC""")
for reason, n in rows:
    print(f"{reason:>14}: {n:5d}")