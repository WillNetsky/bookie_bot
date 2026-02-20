#!/usr/bin/env python3
"""
Kalshi Series Categorizer

Interactive CLI to assign labels, categories, and exclusion status to Kalshi
market series. Stores results in bookie_bot.db.

Usage:
    python -m tools.categorize           # review uncategorized series only
    python -m tools.categorize --all     # re-review all (including already categorized)
    python -m tools.categorize --show    # print current categorizations and exit
    python -m tools.categorize --ticker KXNBAGAME  # review a specific series
"""

import argparse
import asyncio
import base64
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DB_PATH = "bookie_bot.db"

CATEGORIES = [
    ("american",      "American Sports"),
    ("soccer",        "Soccer"),
    ("combat",        "Combat Sports"),
    ("esports",       "Esports"),
    ("international", "International"),
    ("other",         "Other"),
]
CAT_KEYS = [c[0] for c in CATEGORIES]


# ── Auth ──────────────────────────────────────────────────────────────

def _load_key():
    path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
    if not path or not Path(path).exists():
        return None
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        return load_pem_private_key(Path(path).read_bytes(), password=None)
    except Exception as e:
        print(f"  Warning: could not load private key: {e}", file=sys.stderr)
        return None


def _auth_headers(method: str, url: str) -> dict:
    key_id = os.environ.get("KALSHI_API_KEY_ID", "")
    if not key_id:
        return {}
    key = _load_key()
    if not key:
        return {}
    ts = int(time.time() * 1000)
    path = urlparse(url).path
    try:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        sig = key.sign(
            f"{ts}{method.upper()}{path}".encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(ts),
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }
    except Exception as e:
        print(f"  Warning: signing failed: {e}", file=sys.stderr)
        return {}


# ── DB ────────────────────────────────────────────────────────────────

def open_db() -> sqlite3.Connection:
    if not Path(DB_PATH).exists():
        print(f"Database not found at {DB_PATH}. Run the bot at least once first.")
        sys.exit(1)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE IF NOT EXISTS kalshi_series (
            ticker          TEXT PRIMARY KEY,
            kalshi_title    TEXT,
            label           TEXT,
            category        TEXT,
            is_excluded     INTEGER NOT NULL DEFAULT 0,
            is_derivative   INTEGER NOT NULL DEFAULT 0,
            parent_ticker   TEXT,
            notes           TEXT,
            last_seen       TEXT
        )
    """)
    con.commit()
    return con


def load_known(con: sqlite3.Connection) -> dict[str, dict]:
    rows = con.execute("SELECT * FROM kalshi_series").fetchall()
    return {r["ticker"]: dict(r) for r in rows}


def upsert(con: sqlite3.Connection, row: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO kalshi_series
            (ticker, kalshi_title, label, category,
             is_excluded, is_derivative, parent_ticker, notes, last_seen)
        VALUES
            (:ticker, :kalshi_title, :label, :category,
             :is_excluded, :is_derivative, :parent_ticker, :notes, :last_seen)
        """,
        row,
    )
    con.commit()


# ── Kalshi API ────────────────────────────────────────────────────────

async def fetch_series(session: aiohttp.ClientSession) -> list[dict]:
    url = f"{BASE_URL}/series"
    async with session.get(url, headers=_auth_headers("GET", url)) as resp:
        if resp.status != 200:
            text = await resp.text()
            print(f"Error fetching series: HTTP {resp.status} — {text[:200]}")
            return []
        data = await resp.json()
    return data.get("series", [])


async def fetch_samples(session: aiohttp.ClientSession, ticker: str) -> list[str]:
    """Fetch a few sample market titles for a series (to give context)."""
    url = f"{BASE_URL}/markets"
    params = {"series_ticker": ticker, "limit": 4, "status": "open"}
    async with session.get(url, params=params, headers=_auth_headers("GET", url)) as resp:
        if resp.status != 200:
            return []
        data = await resp.json()
    return [m.get("title", "") for m in data.get("markets", []) if m.get("title")]


# ── Interactive prompt ────────────────────────────────────────────────

def _cat_menu() -> str:
    parts = []
    for slug, name in CATEGORIES:
        parts.append(f"[{slug[0]}]{slug[1:]}")
    return "  " + "  ".join(parts) + "  [x]=exclude  [d]=derivative  [?]=skip  [q]=quit"


def prompt(ticker: str, kalshi_title: str, existing: dict | None, samples: list[str]) -> dict | None:
    """
    Prompt the user to categorize a series.
    Returns a row dict, or None to skip.
    Raises SystemExit on quit.
    """
    sep = "─" * 60
    print(f"\n{sep}")
    print(f"  Ticker : {ticker}")
    print(f"  Title  : {kalshi_title}")
    if existing:
        flags = []
        if existing.get("is_excluded"):    flags.append("EXCLUDED")
        if existing.get("is_derivative"): flags.append(f"DERIVATIVE→{existing.get('parent_ticker') or '?'}")
        cur = existing.get("category") or ""
        if flags: cur = ", ".join(flags)
        print(f"  Current: {cur or '(none)'}  label={existing.get('label') or '(none)'}")
    if samples:
        print(f"  Markets:")
        for s in samples:
            print(f"    · {s}")
    print()
    print(_cat_menu())

    while True:
        try:
            raw = input("  > ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            raise SystemExit(0)

        if raw == "q":
            raise SystemExit(0)
        if raw == "?":
            return None

        if raw == "x":
            label = _ask_label(kalshi_title)
            notes = _ask_notes()
            return _row(ticker, kalshi_title, label, None, is_excluded=1, notes=notes)

        if raw == "d":
            parent = input("  Parent ticker: ").strip().upper() or None
            label  = _ask_label(kalshi_title)
            notes  = _ask_notes()
            return _row(ticker, kalshi_title, label, None, is_derivative=1, parent=parent, notes=notes)

        match = next((c for c in CAT_KEYS if c[0] == raw), None)
        if match:
            label = _ask_label(kalshi_title)
            notes = _ask_notes()
            return _row(ticker, kalshi_title, label, match, notes=notes)

        print("  Unrecognized. Try again.")


def _ask_label(default: str) -> str:
    val = input(f"  Label [{default}]: ").strip()
    return val if val else default


def _ask_notes() -> str | None:
    val = input("  Notes (optional): ").strip()
    return val or None


def _row(
    ticker: str, kalshi_title: str, label: str, category: str | None,
    is_excluded: int = 0, is_derivative: int = 0,
    parent: str | None = None, notes: str | None = None,
) -> dict:
    return {
        "ticker":        ticker,
        "kalshi_title":  kalshi_title,
        "label":         label,
        "category":      category,
        "is_excluded":   is_excluded,
        "is_derivative": is_derivative,
        "parent_ticker": parent,
        "notes":         notes,
        "last_seen":     datetime.now(timezone.utc).isoformat(),
    }


# ── Show command ──────────────────────────────────────────────────────

def cmd_show(con: sqlite3.Connection) -> None:
    rows = con.execute(
        "SELECT * FROM kalshi_series ORDER BY category NULLS LAST, label COLLATE NOCASE"
    ).fetchall()
    if not rows:
        print("No series categorized yet.")
        return

    print(f"\n{'Ticker':<35} {'Label':<32} {'Category':<15} Flags")
    print("─" * 100)
    last_cat = object()
    for r in rows:
        cat = r["category"] or ""
        if cat != last_cat:
            if last_cat is not object():
                print()
            print(f"  {'── ' + (cat.upper() if cat else 'UNCATEGORIZED/EXCLUDED') + ' ──'}")
            last_cat = cat
        flags = []
        if r["is_excluded"]:    flags.append("EXCLUDED")
        if r["is_derivative"]: flags.append(f"→{r['parent_ticker'] or '?'}")
        flag_str = "  " + ", ".join(flags) if flags else ""
        print(f"  {r['ticker']:<33} {(r['label'] or ''):<32} {cat:<15}{flag_str}")

    print(f"\n  Total: {len(rows)} series")


# ── Main ──────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    con = open_db()

    if args.show:
        cmd_show(con)
        con.close()
        return

    async with aiohttp.ClientSession() as session:
        print("Fetching series from Kalshi API...")
        all_series = await fetch_series(session)
        if not all_series:
            print("No series returned. Check KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH in .env")
            con.close()
            return

        print(f"Got {len(all_series)} series from Kalshi.")
        known = load_known(con)

        if args.ticker:
            # Single-series mode
            match = next((s for s in all_series if s.get("ticker") == args.ticker.upper()), None)
            if not match:
                match = {"ticker": args.ticker.upper(), "title": args.ticker.upper()}
            to_review = [match]
        elif args.all:
            to_review = all_series
        else:
            to_review = [s for s in all_series if s.get("ticker") not in known]

        if not to_review:
            print("All series are already categorized. Use --all to re-review.")
            con.close()
            return

        print(f"{len(to_review)} series to review.\n")
        print("Keys: " + "  ".join(f"[{c[0]}]={c[0]}" for c in CATEGORIES))
        print("      [x]=exclude  [d]=derivative (spread/total/sub-stat)  [?]=skip  [q]=quit\n")

        saved = skipped = 0
        for i, s in enumerate(to_review, 1):
            ticker      = s.get("ticker", "")
            kalshi_title = s.get("title", ticker)
            existing    = known.get(ticker)

            print(f"[{i}/{len(to_review)}]", end="", flush=True)
            samples = await fetch_samples(session, ticker)

            row = prompt(ticker, kalshi_title, existing, samples)
            if row is None:
                print("  Skipped.")
                skipped += 1
            else:
                upsert(con, row)
                flag = (
                    "EXCLUDED" if row["is_excluded"]
                    else f"DERIVATIVE→{row['parent_ticker'] or '?'}" if row["is_derivative"]
                    else row["category"]
                )
                print(f"  Saved: {ticker} → {flag} / \"{row['label']}\"")
                saved += 1

    print(f"\nDone. {saved} saved, {skipped} skipped.")
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi series categorization tool")
    parser.add_argument("--all",    action="store_true", help="Re-review all series, not just uncategorized")
    parser.add_argument("--show",   action="store_true", help="Print current categorizations and exit")
    parser.add_argument("--ticker", metavar="TICKER",   help="Review a single specific series")
    args = parser.parse_args()
    asyncio.run(main(args))
