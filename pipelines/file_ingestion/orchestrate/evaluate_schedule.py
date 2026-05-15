"""Schedule evaluation: Quartz 6-field cron parsing and next-fire-time calculation.

Format: sec min hour dom month dow
Supports: *, ?, */N, N-M, N-M/S, N/S, N,M, L, NL, N#M, NW, LW, L-N, DOW/N, DOW/N+offset
Day names: SUN, MON, TUE, WED, THU, FRI, SAT
Month names: JAN-DEC (optional)
Multi-cron: pipe-separated expressions (e.g., "expr1 | expr2") — earliest next-fire wins
Timezone: optional tz_name parameter for DST-aware local-time computation

Dependencies: re, calendar, datetime, zoneinfo (stdlib only)
"""
from __future__ import annotations

import calendar
import re
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python 3.8 compat

UTC = timezone.utc

# --- Day name mappings (Quartz convention: 1=SUN ... 7=SAT) ---
_DAY_NAMES = {
    'SUN': '1', 'MON': '2', 'TUE': '3', 'WED': '4',
    'THU': '5', 'FRI': '6', 'SAT': '7',
}
_MONTH_NAMES = {
    'JAN': '1', 'FEB': '2', 'MAR': '3', 'APR': '4', 'MAY': '5', 'JUN': '6',
    'JUL': '7', 'AUG': '8', 'SEP': '9', 'OCT': '10', 'NOV': '11', 'DEC': '12',
}


# =============================================================================
# Field Parsing
# =============================================================================


def _normalize_field(field: str, name_map: dict[str, str] | None = None) -> str:
    """Uppercase and replace named tokens (MON, JAN, etc.) with numeric equivalents."""
    field = field.strip().upper()
    if name_map:
        for name, num in name_map.items():
            field = field.replace(name, num)
    return field


def _parse_cron_field(field: str, min_val: int, max_val: int) -> set[int] | None:
    """Parse a cron field into a set of valid integers.

    Returns None if the field contains special operators (L, #, W) needing dynamic resolution.
    """
    field = field.strip()
    if field in ('*', '?'):
        return set(range(min_val, max_val + 1))
    if any(c in field for c in ('L', '#', 'W')):
        return None  # Needs dynamic resolution

    values: set[int] = set()
    for part in field.split(','):
        part = part.strip()
        if '/' in part:
            base, step_str = part.split('/', 1)
            step = int(step_str)
            if base in ('*', '?'):
                start, end = min_val, max_val
            elif '-' in base:
                start, end = (int(x) for x in base.split('-', 1))
            else:
                start, end = int(base), max_val
            values.update(range(start, end + 1, step))
        elif '-' in part:
            start, end = (int(x) for x in part.split('-', 1))
            values.update(range(start, end + 1))
        else:
            values.add(int(part))
    return values


# =============================================================================
# Day-of-Month Resolution (L, W, LW, L-N)
# =============================================================================


def _resolve_dom(field: str, year: int, month: int) -> set[int]:
    """Resolve day-of-month field dynamically, handling L, W, LW, L-N operators."""
    field = field.strip().upper()
    last_day = calendar.monthrange(year, month)[1]

    if field in ('*', '?'):
        return set(range(1, last_day + 1))

    # L = last day of month
    if field == 'L':
        return {last_day}

    # LW = last weekday of month
    if field == 'LW':
        d = last_day
        wd = calendar.weekday(year, month, d)  # 0=Mon ... 6=Sun
        if wd == 5:   # Saturday → Friday
            d -= 1
        elif wd == 6:  # Sunday → Friday
            d -= 2
        return {max(d, 1)}

    # L-N = Nth day before last day
    m = re.fullmatch(r'L-(\d+)', field)
    if m:
        offset = int(m.group(1))
        return {max(last_day - offset, 1)}

    # NW = nearest weekday to day N
    m = re.fullmatch(r'(\d+)W', field)
    if m:
        target = min(int(m.group(1)), last_day)
        wd = calendar.weekday(year, month, target)
        if wd == 5:   # Saturday → nearest weekday
            d = target - 1 if target > 1 else target + 2
        elif wd == 6:  # Sunday → nearest weekday
            d = target + 1 if target < last_day else target - 2
        else:
            d = target
        return {max(min(d, last_day), 1)}

    # Standard field parsing (ranges, lists, steps)
    result = _parse_cron_field(field, 1, 31)
    if result is None:
        return set()
    return {d for d in result if d <= last_day}


# =============================================================================
# Day-of-Week Resolution (NL, N#M, named days)
# =============================================================================


def _resolve_dow(field: str, year: int, month: int) -> set[int] | None:
    """Resolve day-of-week field into set of day-of-month values for the given month.

    Returns None if field is * or ? (all days match).
    Quartz DOW: 1=SUN, 2=MON, 3=TUE, 4=WED, 5=THU, 6=FRI, 7=SAT
    """
    field = _normalize_field(field, _DAY_NAMES)
    if field in ('*', '?'):
        return None  # All days valid

    last_day = calendar.monthrange(year, month)[1]

    # NL = last Nth weekday of month (e.g., 6L = last Friday)
    m = re.fullmatch(r'(\d)L', field)
    if m:
        quartz_dow = int(m.group(1))
        py_dow = (quartz_dow - 2) % 7  # Quartz→Python: 2=MON→0, 1=SUN→6, 7=SAT→5
        for d in range(last_day, 0, -1):
            if calendar.weekday(year, month, d) == py_dow:
                return {d}
        return set()

    # N#M = Mth occurrence of weekday N (e.g., 2#2 = 2nd Monday)
    m = re.fullmatch(r'(\d)#(\d)', field)
    if m:
        quartz_dow = int(m.group(1))
        occurrence = int(m.group(2))
        py_dow = (quartz_dow - 2) % 7
        count = 0
        for d in range(1, last_day + 1):
            if calendar.weekday(year, month, d) == py_dow:
                count += 1
                if count == occurrence:
                    return {d}
        return set()  # Occurrence doesn't exist this month

    # DOW/N = every Nth week for a given weekday (e.g., MON/2 = every other Monday)
    # Uses ISO week parity: fires when iso_week % N == offset
    # Syntax: DOW/N (offset=0) or DOW/N+offset
    m = re.fullmatch(r'(\d)/(\d+)(?:\+(\d+))?', field)
    if m:
        quartz_dow = int(m.group(1))
        step = int(m.group(2))
        offset = int(m.group(3)) if m.group(3) else 0
        py_dow = (quartz_dow - 2) % 7
        matching_days = set()
        for d in range(1, last_day + 1):
            if calendar.weekday(year, month, d) == py_dow:
                iso_week = datetime(year, month, d).isocalendar()[1]
                if iso_week % step == offset % step:
                    matching_days.add(d)
        return matching_days if matching_days else set()

    # Standard DOW parsing → convert to matching days-of-month
    dow_set = _parse_cron_field(field, 1, 7)
    if dow_set is None:
        return None
    py_dows = {(q - 2) % 7 for q in dow_set}  # Convert all to Python weekday
    return {d for d in range(1, last_day + 1) if calendar.weekday(year, month, d) in py_dows}


# =============================================================================
# Core: Next Fire Time Calculation
# =============================================================================


def _next_fire_time(sched_cron: str, after: datetime, tz_name: str | None = None) -> datetime | None:
    """Compute the exact next fire time for a Quartz 6-field cron expression.

    Args:
        sched_cron: Quartz cron string (sec min hour dom month dow).
                    Supports pipe-separated multi-cron: "expr1 | expr2 | expr3"
                    — evaluates each, returns the earliest next-fire.
        after: Reference datetime — next fire will be strictly AFTER this time.
        tz_name: Optional timezone name (e.g., "America/New_York"). When provided,
                 fire times are computed in local time then converted to UTC.

    Returns:
        Next fire datetime, or None if invalid/not found.
    """
    # --- Multi-cron: pipe separator ---
    if '|' in sched_cron:
        candidates = []
        for sub_expr in sched_cron.split('|'):
            sub_expr = sub_expr.strip()
            if sub_expr:
                result = _next_fire_time(sub_expr, after, tz_name=tz_name)
                if result is not None:
                    candidates.append(result)
        return min(candidates) if candidates else None

    # --- Timezone-aware computation ---
    if tz_name:
        tz = ZoneInfo(tz_name)
        # Convert 'after' to local time for grid computation
        if after.tzinfo is None:
            after_utc = after.replace(tzinfo=UTC)
        else:
            after_utc = after
        after_local = after_utc.astimezone(tz).replace(tzinfo=None)
        # Compute next fire in local time (naive)
        local_fire = _next_fire_time_single(sched_cron, after_local)
        if local_fire is None:
            return None
        # Convert back to UTC
        local_aware = local_fire.replace(tzinfo=tz)
        return local_aware.astimezone(UTC).replace(tzinfo=UTC)

    return _next_fire_time_single(sched_cron, after)


def _next_fire_time_single(sched_cron: str, after: datetime) -> datetime | None:
    """Core single-expression next-fire-time computation (no pipes, no tz)."""
    parts = sched_cron.strip().split()
    if len(parts) != 6:
        return None

    sec_field, min_field, hour_field, dom_field, month_field, dow_field = parts

    # Normalize month names (JAN→1, etc.)
    month_field = _normalize_field(month_field, _MONTH_NAMES)

    # Parse static fields (seconds, minutes, hours, months)
    valid_seconds = _parse_cron_field(sec_field, 0, 59) or {0}
    valid_minutes = _parse_cron_field(min_field, 0, 59)
    valid_hours = _parse_cron_field(hour_field, 0, 23)
    valid_months = _parse_cron_field(month_field, 1, 12)

    if valid_minutes is None or valid_hours is None or valid_months is None:
        return None  # Invalid: these fields cannot have L/W/# operators

    # Start search from next second after reference
    t = after.replace(microsecond=0) + timedelta(seconds=1)
    end_search = after + timedelta(days=366 * 4)  # 4-year search window

    sorted_months = sorted(valid_months)
    sorted_hours = sorted(valid_hours)
    sorted_minutes = sorted(valid_minutes)
    sorted_seconds = sorted(valid_seconds)

    while t <= end_search:
        # --- MONTH ---
        if t.month not in valid_months:
            next_m = next((m for m in sorted_months if m > t.month), None)
            if next_m is None:
                t = t.replace(year=t.year + 1, month=sorted_months[0], day=1, hour=0, minute=0, second=0)
            else:
                t = t.replace(month=next_m, day=1, hour=0, minute=0, second=0)
            continue

        # --- DAY (DOM + DOW interaction) ---
        valid_doms_set = _resolve_dom(dom_field, t.year, t.month)
        valid_dow_days = _resolve_dow(dow_field, t.year, t.month)

        dom_restricted = dom_field.strip().upper() not in ('*', '?')
        dow_restricted = _normalize_field(dow_field, _DAY_NAMES) not in ('*', '?')

        if dom_restricted and dow_restricted:
            # Quartz behavior: if BOTH restricted, fire on EITHER (OR logic)
            valid_days = valid_doms_set | (valid_dow_days or set())
        elif dow_restricted and valid_dow_days is not None:
            valid_days = valid_dow_days
        else:
            valid_days = valid_doms_set

        if t.day not in valid_days:
            t = (t + timedelta(days=1)).replace(hour=0, minute=0, second=0)
            continue

        # --- HOUR ---
        if t.hour not in valid_hours:
            next_h = next((h for h in sorted_hours if h > t.hour), None)
            if next_h is None:
                t = (t + timedelta(days=1)).replace(hour=0, minute=0, second=0)
            else:
                t = t.replace(hour=next_h, minute=0, second=0)
            continue

        # --- MINUTE ---
        if t.minute not in valid_minutes:
            next_min = next((m for m in sorted_minutes if m > t.minute), None)
            if next_min is None:
                t = (t + timedelta(hours=1)).replace(minute=0, second=0)
            else:
                t = t.replace(minute=next_min, second=0)
            continue

        # --- SECOND ---
        if t.second not in valid_seconds:
            next_sec = next((s for s in sorted_seconds if s > t.second), None)
            if next_sec is None:
                t = (t + timedelta(minutes=1)).replace(second=0)
            else:
                t = t.replace(second=next_sec)
            continue

        # All fields match — this is the next fire time
        return t

    return None  # Not found within 4-year search window


# =============================================================================
# Public API
# =============================================================================


def is_dispatch_due(sched_cron: str, last_dispatched_at: datetime | None, now_utc: datetime | None = None, tz_name: str | None = None) -> bool:
    """Determine if a feed is due for dispatch based on Quartz 6-field cron.

    Returns True if the next fire time after last_dispatched_at has been reached.
    Returns True immediately if last_dispatched_at is None (never dispatched).
    """
    if not (sched_cron or "").strip():
        return False
    now_utc = now_utc or datetime.now(UTC)
    if last_dispatched_at is None:
        return True  # Never dispatched → due immediately
    if last_dispatched_at.tzinfo is None:
        last_dispatched_at = last_dispatched_at.replace(tzinfo=UTC)
    next_fire = _next_fire_time(sched_cron, last_dispatched_at, tz_name=tz_name)
    if next_fire is None:
        return False
    next_fire_utc = next_fire.replace(tzinfo=UTC) if next_fire.tzinfo is None else next_fire
    return now_utc >= next_fire_utc


def compute_next_dispatched_at(sched_cron: str, now_utc: datetime | None = None, tz_name: str | None = None) -> datetime | None:
    """Compute the next_dispatched_at value for the MERGE in dispatch_feeds.py.

    Returns the exact next fire time after now, for writing into ops_dispatch_state.
    """
    now_utc = now_utc or datetime.now(UTC)
    return _next_fire_time(sched_cron, now_utc, tz_name=tz_name)


def should_auto_trigger_row(
    row: dict, now_utc: datetime | None = None,
    honor_config_auto_trigger: bool = True, honor_maintenance_hold: bool = True,
) -> bool:
    """Full eligibility check: ctl_active + ctl_auto_trigger + maintenance_hold + schedule.

    Evaluates all scheduling gates before checking the cron schedule.
    """
    now_utc = now_utc or datetime.now(UTC)
    if str(row.get("ctl_active", "Y")).strip().upper() != "Y":
        return False
    if honor_config_auto_trigger and str(row.get("ctl_auto_trigger", "Y")).strip().upper() != "Y":
        return False
    if honor_maintenance_hold:
        hold_until = _parse_utc_timestamp(row.get("ctl_maintenance_hold_until"))
        if hold_until is not None and now_utc < hold_until:
            return False
    return is_dispatch_due(
        str(row.get("sched_cron") or ""),
        _parse_utc_timestamp(row.get("last_dispatched_at")),
        now_utc=now_utc,
        tz_name=str(row.get("sched_timezone") or "").strip() or None,
    )


# =============================================================================
# Helpers
# =============================================================================


def _parse_utc_timestamp(value) -> datetime | None:
    """Parse a timestamp value (string or datetime) into UTC-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
