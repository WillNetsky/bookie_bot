"""Eval harness for bot/services/kalshi_taxonomy.py.

Runs the classifier over every ticker the bot has ever seen
(unknown_series.txt) and prints the distribution across bet_type / subtype,
with samples, so we can judge coverage before wiring any UI.

    python test_taxonomy.py            # summary + samples
    python test_taxonomy.py FUTURES    # dump everything in a bet_type
    python test_taxonomy.py fallback   # dump only the catch-all bucket
"""

import sys
from collections import defaultdict
from pathlib import Path

from bot.services import kalshi_taxonomy as tax

ROWS = []
for line in Path("unknown_series.txt").read_text().splitlines():
    if "|" not in line:
        continue
    ticker, title = (p.strip() for p in line.split("|", 1))
    if ticker:
        ROWS.append((ticker, title))

by_type = defaultdict(list)
by_sub = defaultdict(list)
for ticker, title in ROWS:
    c = tax.classify(ticker, title=title)
    by_type[c.bet_type].append((ticker, title, c))
    by_sub[(c.bet_type, c.subtype)].append((ticker, title))

arg = sys.argv[1] if len(sys.argv) > 1 else ""

if arg == "fallback":
    # The catch-all: undated futures that hit no token rule.
    rows = [
        (tk, ti) for tk, ti, c in by_type[tax.FUTURES]
        if c.subtype == tax.SUB_OUTRIGHT
    ]
    print(f"Catch-all FUTURES/Tournament Winner bucket: {len(rows)} series\n")
    for tk, ti in sorted(rows):
        print(f"  {tk:32} {ti}")
    sys.exit(0)

if arg.upper() in (tax.GAME, tax.PROP, tax.FUTURES, tax.SPECIAL):
    want = arg.lower()
    for tk, ti, c in sorted(by_type[want]):
        print(f"  [{c.subtype:22}] {tk:30} {ti}")
    sys.exit(0)

total = len(ROWS)
print(f"Classified {total} series\n")
print("By bet_type:")
for bt in tax.BET_TYPE_ORDER:
    rows = by_type[bt]
    print(f"  {tax.BET_TYPE_LABEL[bt]:14} {len(rows):4}  ({100*len(rows)//total}%)")

print("\nBy subtype:")
for (bt, sub), rows in sorted(by_sub.items(), key=lambda kv: -len(kv[1])):
    sample = ", ".join(tk for tk, _ in rows[:3])
    print(f"  {bt:8} {sub:24} {len(rows):4}   e.g. {sample}")

fallback = [
    (tk, ti) for tk, ti, c in by_type[tax.FUTURES] if c.subtype == tax.SUB_OUTRIGHT
]
print(f"\nCatch-all (FUTURES/Tournament Winner): {len(fallback)} "
      f"({100*len(fallback)//total}%) — run `python test_taxonomy.py fallback` to inspect")
