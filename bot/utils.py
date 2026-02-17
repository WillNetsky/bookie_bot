from datetime import datetime, timezone
from bot.constants import TZ_PT, TZ_ET, PICK_LABELS

def format_game_time(commence_str: str) -> str:
    """Format a commence_time string into PT / ET display."""
    try:
        ct = datetime.fromisoformat(commence_str.replace("Z", "+00:00"))
        pt_dt = ct.astimezone(TZ_PT)
        et_dt = ct.astimezone(TZ_ET)
        pt_str = pt_dt.strftime("%-m/%-d %-I:%M %p PT")
        if pt_dt.date() == et_dt.date():
            et_str = et_dt.strftime("%-I:%M %p ET")
        else:
            et_str = et_dt.strftime("%-m/%-d %-I:%M %p ET")
        return f"{pt_str} / {et_str}"
    except (ValueError, TypeError):
        return "TBD"


def format_pick_label(bet: dict) -> str:
    """Format a bet's pick into a display label including point info."""
    pick = bet.get("pick", "")
    label = PICK_LABELS.get(pick, pick.capitalize())
    point = bet.get("point")
    if point is not None:
        if pick in ("spread_home", "spread_away"):
            label += f" {point:+g}"
        else:
            label += f" {point:g}"
    return label


def format_matchup(home: str, away: str) -> str:
    """Format a matchup as 'Away @ Home'."""
    return f"{away} @ {home}"


def decimal_to_american(decimal_odds: float) -> int:
    """Convert decimal odds to American odds."""
    if decimal_odds <= 1.0:
        return -10000  # Extreme favorite / essentially no odds
    if decimal_odds >= 2.0:
        return round((decimal_odds - 1) * 100)
    else:
        return round(-100 / (decimal_odds - 1))


def format_american(odds: int) -> str:
    """Format American odds with +/- prefix."""
    return f"{odds:+d}" if odds else "?"
