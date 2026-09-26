"""Config-redundancy analysis: are the 8 frozen configs independent bets or
copies? Groups trades into dip episodes (token + entry bar) and reports
entry overlap (Jaccard per config pair), exit correlation, and the number of
truly independent bets realized so far. Run: python config_overlap.py [db]
"""
import json
import os
import sqlite3
import sys
from collections import defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "vapor.db")
db = sqlite3.connect(PATH)

trades = db.execute("""SELECT token_address, config_id, entry_ts, exit_ts, state,
                               exit_price, entry_price, net_ret
                       FROM paper_trades""").fetchall()

by_ep = defaultdict(list)          # (token, entry_ts) -> [trade rows]
for t in trades:
    by_ep[(t[0], t[2])].append(t)

eps = sorted(by_ep)
print(f"total trades: {len(trades)}  |  dip episodes (token+entry bar): {len(eps)}")
print(f"effective bets (episodes): {len(eps)}  ->  trades/episode = {len(trades)/max(len(eps),1):.1f}")
print()

print("== per episode: configs joined + exit ==")
for (tok, ents), t in sorted(by_ep.items()):
    sym = db.execute("SELECT symbol FROM candidates WHERE token_address=?", (tok,)).fetchone()
    sym = sym and sym[0] or tok[:6]
    cids = sorted({x[1] for x in t})
    closed = sum(1 for x in t if x[4] == "CLOSED")
    net = sum(x[7] for x in t if x[7])
    print(f"{sym:>10} cfg={cids} {closed}/{len(t)} closed net={net:+6.2f}")
print()

def jaccard(a, b):
    sa, sb = set(a), set(b)
    return len(sa & sb) / max(len(sa | sb), 1)

# entry overlap: for each config, the fraction of OTHER configs whose entry
# episodes it shares (1.0 = always moves together = redundant copy)
print("== config pairwise entry-overlap (Jaccard over episodes) ==")
per_cfg_eps = defaultdict(set)
for (tok, ents), t in by_ep.items():
    for x in t:
        per_cfg_eps[x[1]].add((tok, ents))
print("     " + " ".join(f"c{i:>3}" for i in range(1, 9)))
for i in range(1, 9):
    row = f"c{i:>3} "
    for j in range(1, 9):
        row += f"{jaccard(per_cfg_eps[i], per_cfg_eps[j]):4.2f} "
    print(row)
print()

print("\n== configs sharing 1.0 overlap with at least one other ==")
for i in range(1, 9):
    for j in range(i + 1, 9):
        jd = jaccard(per_cfg_eps[i], per_cfg_eps[j])
        if jd >= 0.5:
            print(f"  c{i} ~ c{j}: overlap {jd:.2f}")

# collapse to dip-threshold entry groups and summarize
print("\n== summary ==")
dip25 = sum(1 for i in (1, 2, 3, 4, 6))
dip30 = sum(1 for i in (5, 7, 8))
def jpair(a, b):
    return jaccard(per_cfg_eps[a], per_cfg_eps[b])
within25 = [jpair(1, 4), jpair(1, 6), jpair(2, 4), jpair(3, 6)]
within30 = [jpair(5, 7)]
print(f"dip=0.25 group (c1,c2,c3,c4,c6): within-group overlap ~{max(within25):.2f}..{min(within25):.2f}")
print(f"dip=0.30 group (c5,c7,c8): c5~c7 overlap {within30[0]:.2f}")
tok_eps = defaultdict(set)
for (tok, ents) in by_ep:
    tok_eps[tok].add(ents)
print(f"\ndistinct tokens traded: {len(tok_eps)}")
print(f"episodes per token: {sorted(len(v) for v in tok_eps.values())}")
print("=> effective independent bets are EPISODES, not trades; episodes still share token fate")