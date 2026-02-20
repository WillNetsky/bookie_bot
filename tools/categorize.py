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

# (key, slug, display_name)
CATEGORIES = [
    ("p", "politics",    "Politics"),
    ("s", "sports",      "Sports"),
    ("u", "culture",     "Culture"),
    ("c", "crypto",      "Crypto"),
    ("l", "climate",     "Climate"),
    ("e", "economics",   "Economics"),
    ("m", "mentions",    "Mentions"),
    ("o", "companies",   "Companies"),
    ("f", "financials",  "Financials"),
    ("t", "tech_science","Tech & Science"),
]

# Suggested subcategories per top-level category
SUBCATEGORY_SUGGESTIONS: dict[str, list[str]] = {
    "politics":    ["us", "international"],
    "sports":      ["american", "soccer", "combat", "esports", "international", "other"],
    "culture":     ["entertainment", "awards", "gaming", "other"],
    "crypto":      ["bitcoin", "ethereum", "altcoins", "other"],
    "climate":     ["weather", "environment", "other"],
    "economics":   ["us", "international", "other"],
    "mentions":    ["social_media", "news", "other"],
    "companies":   ["tech", "finance", "energy", "retail", "other"],
    "financials":  ["stocks", "indices", "commodities", "rates", "other"],
    "tech_science":["ai", "space", "tech", "other"],
}

CAT_BY_KEY  = {k: (slug, name) for k, slug, name in CATEGORIES}
CAT_BY_SLUG = {slug: (k, name) for k, slug, name in CATEGORIES}

# Map Kalshi's category strings → our slugs
KALSHI_CATEGORY_MAP: dict[str, str] = {
    "Sports":                "sports",
    "Politics":              "politics",
    "Culture":               "culture",
    "Crypto":                "crypto",
    "Climate and Weather":   "climate",
    "Economics":             "economics",
    "Mentions":              "mentions",
    "Companies":             "companies",
    "Financials":            "financials",
    "Tech and Science":      "tech_science",
    "Technology":            "tech_science",
    "Tech & Science":        "tech_science",
}


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
            subcategory     TEXT,
            is_excluded     INTEGER NOT NULL DEFAULT 0,
            is_derivative   INTEGER NOT NULL DEFAULT 0,
            parent_ticker   TEXT,
            notes           TEXT,
            last_seen       TEXT
        )
    """)
    # migration for existing tables
    try:
        con.execute("ALTER TABLE kalshi_series ADD COLUMN subcategory TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists
    con.commit()
    return con


def load_known(con: sqlite3.Connection) -> dict[str, dict]:
    rows = con.execute("SELECT * FROM kalshi_series").fetchall()
    return {r["ticker"]: dict(r) for r in rows}


def upsert(con: sqlite3.Connection, row: dict) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO kalshi_series
            (ticker, kalshi_title, label, category, subcategory,
             is_excluded, is_derivative, parent_ticker, notes, last_seen)
        VALUES
            (:ticker, :kalshi_title, :label, :category, :subcategory,
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


async def fetch_all_open_markets(session: aiohttp.ClientSession) -> dict[str, list[dict]]:
    """
    Fetch ALL open markets in paginated calls and return them grouped by series_ticker.
    One bulk fetch instead of one call per series.
    """
    url = f"{BASE_URL}/markets"
    by_series: dict[str, list[dict]] = {}
    cursor = None
    page = 0
    while True:
        params: dict = {"limit": 1000, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        async with session.get(url, params=params, headers=_auth_headers("GET", url)) as resp:
            if resp.status != 200:
                print(f"  Warning: markets fetch returned HTTP {resp.status}, stopping pagination")
                break
            data = await resp.json()
        markets = data.get("markets", [])
        for m in markets:
            st = m.get("series_ticker") or ""
            if not st:
                # Kalshi v2 API often omits series_ticker — derive it from event_ticker
                et = m.get("event_ticker") or ""
                st = et.split("-")[0] if "-" in et else et
            if st:
                by_series.setdefault(st, []).append(m)
        page += 1
        cursor = data.get("cursor")
        if not cursor or not markets:
            break
        if page % 5 == 0:
            print(f"  Fetched {sum(len(v) for v in by_series.values())} markets so far...")
    return by_series


# ── Interactive prompt ────────────────────────────────────────────────

def _cat_menu() -> str:
    parts = [f"[{k}] {name}" for k, slug, name in CATEGORIES]
    parts += ["[x] Exclude", "[d] Derivative", "[?] Skip", "[q] Quit"]
    # Wrap to two lines
    half = len(parts) // 2
    line1 = "  ".join(parts[:half])
    line2 = "  ".join(parts[half:])
    return f"  {line1}\n  {line2}"


def _subcat_prompt(category_slug: str, existing_sub: str | None) -> str | None:
    """Prompt for an optional subcategory, showing suggestions for this category."""
    suggestions = SUBCATEGORY_SUGGESTIONS.get(category_slug, [])
    default = existing_sub or ""
    if suggestions:
        hint = "/".join(suggestions)
        display_default = f" [{default}]" if default else f" (suggestions: {hint})"
    else:
        display_default = f" [{default}]" if default else " (optional)"
    val = input(f"  Subcategory{display_default}: ").strip().lower().replace(" ", "_")
    if not val:
        return default or None
    return val


def _fmt_prob(market: dict) -> str:
    """Format YES probability as a percentage."""
    p = market.get("yes_ask_dollars") or market.get("last_price_dollars")
    if p is None:
        return "  ?%"
    try:
        return f"{round(float(p) * 100):>3}%"
    except (ValueError, TypeError):
        return "  ?%"


def _fmt_close(market: dict) -> str:
    """Format close time as a short human-readable string."""
    raw = market.get("close_time") or market.get("expected_expiration_time") or ""
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = dt - now
        days = delta.days
        if days < 0:
            return "CLOSED"
        if days == 0:
            hrs = delta.seconds // 3600
            return f"closes in {hrs}h"
        return f"closes {dt.strftime('%b %d')}"
    except (ValueError, TypeError):
        return ""


def prompt(
    ticker: str, kalshi_title: str, kalshi_category: str,
    existing: dict | None, market_count: int, samples: list[dict],
) -> dict | None:
    """
    Prompt the user to categorize a series.
    Category is auto-assigned from Kalshi's own category field.
    User only needs to confirm/override label and pick a subcategory.
    Returns a row dict, or None to skip. Raises SystemExit on quit.
    """
    auto_slug = KALSHI_CATEGORY_MAP.get(kalshi_category, "")
    _, auto_name = CAT_BY_SLUG.get(auto_slug, ("?", kalshi_category or "unknown"))

    sep = "─" * 65
    print(f"\n{sep}")
    print(f"  Ticker   : {ticker}")
    print(f"  Title    : {kalshi_title}")
    print(f"  Kalshi   : {kalshi_category or '(none)'}  →  {auto_slug or '(unmapped)'}  |  {market_count} open market{'s' if market_count != 1 else ''}")
    if existing:
        flags = []
        if existing.get("is_excluded"):    flags.append("EXCLUDED")
        if existing.get("is_derivative"): flags.append(f"DERIVATIVE→{existing.get('parent_ticker') or '?'}")
        cat = existing.get("category") or ""
        sub = existing.get("subcategory") or ""
        cur = f"{cat}/{sub}" if sub else cat
        if flags: cur = ", ".join(flags)
        print(f"  Current  : {cur or '(none)'}  label={existing.get('label') or '(none)'}")
    if samples:
        print("  Markets  :")
        for m in samples:
            title  = m.get("title") or "?"
            sub_t  = m.get("yes_sub_title") or ""
            prob   = _fmt_prob(m)
            close  = _fmt_close(m)
            detail = f"  [{prob} YES]" + (f"  {close}" if close else "")
            print(f"    · {title}{detail}")
            if sub_t and sub_t.lower() not in title.lower():
                print(f"        YES = {sub_t}")
    print()

    if not auto_slug:
        # Kalshi category not in our map — fall back to manual pick
        print(f"  ⚠  Unmapped Kalshi category: '{kalshi_category}'")
        print(_cat_menu())
        while True:
            try:
                raw = input("  category > ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print(); raise SystemExit(0)
            if raw == "q": raise SystemExit(0)
            if raw == "?": return None
            if raw == "x":
                return _row(ticker, kalshi_title, _ask_label(kalshi_title), None, None, is_excluded=1, notes=_ask_notes())
            if raw == "d":
                parent = input("  Parent ticker: ").strip().upper() or None
                return _row(ticker, kalshi_title, _ask_label(kalshi_title), None, None, is_derivative=1, parent=parent, notes=_ask_notes())
            if raw in CAT_BY_KEY:
                auto_slug = CAT_BY_KEY[raw][0]
                break
            print("  Unrecognized. Try again.")

    # Category is known — just ask for label, subcategory, and optional flags
    print(f"  [Enter]=accept  [x]=exclude  [d]=derivative  [?]=skip  [q]=quit")
    try:
        raw = input("  > ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print(); raise SystemExit(0)

    if raw == "q": raise SystemExit(0)
    if raw == "?": return None
    if raw == "x":
        return _row(ticker, kalshi_title, _ask_label(kalshi_title), None, None, is_excluded=1, notes=_ask_notes())
    if raw == "d":
        parent = input("  Parent ticker: ").strip().upper() or None
        return _row(ticker, kalshi_title, _ask_label(kalshi_title), None, None, is_derivative=1, parent=parent, notes=_ask_notes())

    # Accept (any other input including empty string)
    label  = _ask_label(kalshi_title)
    subcat = _subcat_prompt(auto_slug, existing.get("subcategory") if existing else None)
    notes  = _ask_notes()
    return _row(ticker, kalshi_title, label, auto_slug, subcat, notes=notes)


def _ask_label(default: str) -> str:
    val = input(f"  Label [{default}]: ").strip()
    return val if val else default


def _ask_notes() -> str | None:
    val = input("  Notes (optional): ").strip()
    return val or None


def _row(
    ticker: str, kalshi_title: str, label: str,
    category: str | None, subcategory: str | None,
    is_excluded: int = 0, is_derivative: int = 0,
    parent: str | None = None, notes: str | None = None,
) -> dict:
    return {
        "ticker":        ticker,
        "kalshi_title":  kalshi_title,
        "label":         label,
        "category":      category,
        "subcategory":   subcategory,
        "is_excluded":   is_excluded,
        "is_derivative": is_derivative,
        "parent_ticker": parent,
        "notes":         notes,
        "last_seen":     datetime.now(timezone.utc).isoformat(),
    }


# ── Show command ──────────────────────────────────────────────────────

def cmd_show(con: sqlite3.Connection) -> None:
    rows = con.execute(
        "SELECT * FROM kalshi_series ORDER BY category NULLS LAST, subcategory NULLS LAST, label COLLATE NOCASE"
    ).fetchall()
    if not rows:
        print("No series categorized yet.")
        return

    print(f"\n  {'Ticker':<35} {'Label':<30} {'Category':<15} {'Subcategory':<15} Flags")
    print("  " + "─" * 105)
    last_cat = object()
    for r in rows:
        cat = r["category"] or ""
        sub = r["subcategory"] or ""
        if cat != last_cat:
            if last_cat is not object():
                print()
            _, cat_name = CAT_BY_SLUG.get(cat, ("?", cat.upper() if cat else "UNCATEGORIZED"))
            print(f"\n  ── {cat_name} ──")
            last_cat = cat
        flags = []
        if r["is_excluded"]:    flags.append("EXCL")
        if r["is_derivative"]: flags.append(f"→{r['parent_ticker'] or '?'}")
        flag_str = "  " + ", ".join(flags) if flags else ""
        print(f"  {r['ticker']:<35} {(r['label'] or ''):<30} {cat:<15} {sub:<15}{flag_str}")

    total = len(rows)
    excl  = sum(1 for r in rows if r["is_excluded"])
    deriv = sum(1 for r in rows if r["is_derivative"])
    print(f"\n  {total} total  ({excl} excluded, {deriv} derivatives)")


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
        print("Fetching all open markets (one bulk call)...")
        markets_by_series = await fetch_all_open_markets(session)
        total_markets = sum(len(v) for v in markets_by_series.values())
        print(f"Got {total_markets} open markets across {len(markets_by_series)} active series.")
        known = load_known(con)

        if args.ticker:
            match = next((s for s in all_series if s.get("ticker") == args.ticker.upper()), None)
            if not match:
                match = {"ticker": args.ticker.upper(), "title": args.ticker.upper()}
            to_review = [match]
        elif args.all:
            # Re-review all series that have open markets
            active = {s.get("ticker") for s in all_series if s.get("ticker") in markets_by_series}
            to_review = [s for s in all_series if s.get("ticker") in active]
        else:
            # Only uncategorized series that have open markets
            to_review = [
                s for s in all_series
                if s.get("ticker") not in known and s.get("ticker") in markets_by_series
            ]

        if not to_review:
            print("Nothing to review. Use --all to re-review categorized series.")
            con.close()
            return

        print(f"\n{len(to_review)} series to review.\n")
        print(_cat_menu())
        print()

        saved = skipped = 0
        for i, s in enumerate(to_review, 1):
            ticker       = s.get("ticker", "")
            kalshi_title = s.get("title", ticker)
            existing     = known.get(ticker)
            kalshi_category = s.get("category", "")
            series_markets  = markets_by_series.get(ticker, [])
            market_count    = len(series_markets)
            samples         = series_markets[:4]

            row = prompt(ticker, kalshi_title, kalshi_category, existing, market_count, samples)
            if row is None:
                print("  Skipped.")
                skipped += 1
            else:
                upsert(con, row)
                cat_display = row["category"] or ("EXCLUDED" if row["is_excluded"] else "DERIVATIVE")
                sub_display = f"/{row['subcategory']}" if row.get("subcategory") else ""
                print(f"  Saved: {ticker} → {cat_display}{sub_display} / \"{row['label']}\"")
                saved += 1

    print(f"\nDone. {saved} saved, {skipped} skipped.")
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi series categorization tool")
    parser.add_argument("--all",    action="store_true", help="Re-review all series, not just uncategorized")
    parser.add_argument("--show",   action="store_true", help="Print current categorizations and exit")
    parser.add_argument("--ticker", metavar="TICKER",    help="Review a single specific series")
    args = parser.parse_args()
    asyncio.run(main(args))
