"""Tests for evaluate_schedule.py — Quartz 6-field cron parser and next-fire-time calculation.

Validates _next_fire_time against all 46 cases from docs/CRON_QUARTZ_REFERENCE.md.
Reference time: 2026-05-04 14:03:27 (Monday) — chosen to exercise hour/minute/dow logic.

Run: pytest tests/unit/test_evaluate_schedule.py -v
"""
import pytest
from datetime import datetime, timedelta, timezone

from pipelines.file_ingestion.orchestrate.evaluate_schedule import (
    _next_fire_time,
    _parse_cron_field,
    _resolve_dom,
    _resolve_dow,
    is_dispatch_due,
    compute_next_dispatched_at,
    should_auto_trigger_row,
)

UTC = timezone.utc

# Reference time: Monday 2026-05-04 at 14:03:27
REF = datetime(2026, 5, 4, 14, 3, 27)


# =============================================================================
# SECTION 1: _parse_cron_field unit tests
# =============================================================================


class TestParseCronField:
    """Unit tests for static cron field parsing."""

    def test_wildcard_star(self):
        assert _parse_cron_field("*", 0, 59) == set(range(0, 60))

    def test_wildcard_question(self):
        assert _parse_cron_field("?", 1, 31) == set(range(1, 32))

    def test_single_value(self):
        assert _parse_cron_field("5", 0, 59) == {5}

    def test_list(self):
        assert _parse_cron_field("1,3,5", 1, 7) == {1, 3, 5}

    def test_range(self):
        assert _parse_cron_field("2-5", 0, 23) == {2, 3, 4, 5}

    def test_step_from_star(self):
        assert _parse_cron_field("*/5", 0, 59) == {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}

    def test_step_from_value(self):
        assert _parse_cron_field("2/6", 0, 23) == {2, 8, 14, 20}

    def test_range_with_step(self):
        assert _parse_cron_field("8-22/2", 0, 23) == {8, 10, 12, 14, 16, 18, 20, 22}

    def test_step_from_star_hours(self):
        assert _parse_cron_field("*/6", 0, 23) == {0, 6, 12, 18}

    def test_returns_none_for_L(self):
        assert _parse_cron_field("L", 1, 31) is None

    def test_returns_none_for_W(self):
        assert _parse_cron_field("15W", 1, 31) is None

    def test_returns_none_for_hash(self):
        assert _parse_cron_field("2#3", 1, 7) is None


# =============================================================================
# SECTION 2: _resolve_dom unit tests
# =============================================================================


class TestResolveDom:
    """Unit tests for day-of-month resolution including L, W, LW operators."""

    def test_last_day_may(self):
        assert _resolve_dom("L", 2026, 5) == {31}

    def test_last_day_february_non_leap(self):
        assert _resolve_dom("L", 2025, 2) == {28}

    def test_last_day_february_leap(self):
        assert _resolve_dom("L", 2028, 2) == {29}

    def test_last_weekday_may_2026(self):
        # May 31, 2026 = Sunday → nearest weekday = Friday May 29
        assert _resolve_dom("LW", 2026, 5) == {29}

    def test_last_weekday_jan_2026(self):
        # Jan 31, 2026 = Saturday → nearest weekday = Friday Jan 30
        assert _resolve_dom("LW", 2026, 1) == {30}

    def test_l_minus_3(self):
        # May: last day = 31, so L-3 = 28
        assert _resolve_dom("L-3", 2026, 5) == {28}

    def test_nearest_weekday_15_is_thursday(self):
        # May 15, 2026 = Friday → already weekday
        assert _resolve_dom("15W", 2026, 5) == {15}

    def test_nearest_weekday_saturday(self):
        # May 16, 2026 = Saturday → nearest weekday = Friday May 15
        assert _resolve_dom("16W", 2026, 5) == {15}

    def test_nearest_weekday_sunday(self):
        # May 17, 2026 = Sunday → nearest weekday = Monday May 18
        assert _resolve_dom("17W", 2026, 5) == {18}

    def test_specific_days(self):
        assert _resolve_dom("1,15", 2026, 5) == {1, 15}

    def test_range(self):
        assert _resolve_dom("1-5", 2026, 5) == {1, 2, 3, 4, 5}

    def test_filters_invalid_days(self):
        # February only has 28 days in 2025, so 29-31 filtered out
        result = _resolve_dom("28,29,30,31", 2025, 2)
        assert result == {28}


# =============================================================================
# SECTION 3: _resolve_dow unit tests
# =============================================================================


class TestResolveDow:
    """Unit tests for day-of-week resolution including NL and N#M operators."""

    def test_wildcard_returns_none(self):
        assert _resolve_dow("?", 2026, 5) is None
        assert _resolve_dow("*", 2026, 5) is None

    def test_mon_wed_fri(self):
        # May 2026: Mon=4,11,18,25; Wed=6,13,20,27; Fri=1,8,15,22,29
        result = _resolve_dow("MON,WED,FRI", 2026, 5)
        expected = {4, 11, 18, 25, 6, 13, 20, 27, 1, 8, 15, 22, 29}
        assert result == expected

    def test_sat_sun(self):
        # May 2026: Sat=2,9,16,23,30; Sun=3,10,17,24,31
        result = _resolve_dow("SAT,SUN", 2026, 5)
        expected = {2, 9, 16, 23, 30, 3, 10, 17, 24, 31}
        assert result == expected

    def test_mon_to_fri_range(self):
        # May 2026 weekdays
        result = _resolve_dow("MON-FRI", 2026, 5)
        # Should include all weekdays in May 2026
        assert 4 in result   # Monday
        assert 5 in result   # Tuesday
        assert 2 not in result  # Saturday
        assert 3 not in result  # Sunday

    def test_second_tuesday(self):
        # May 2026: Tuesdays = 5, 12, 19, 26 → 2nd Tuesday = 12
        result = _resolve_dow("3#2", 2026, 5)  # Quartz: 3=TUE
        assert result == {12}

    def test_last_friday(self):
        # May 2026: Fridays = 1, 8, 15, 22, 29 → last Friday = 29
        result = _resolve_dow("6L", 2026, 5)  # Quartz: 6=FRI
        assert result == {29}

    def test_last_monday(self):
        # May 2026: Mondays = 4, 11, 18, 25 → last Monday = 25
        result = _resolve_dow("2L", 2026, 5)  # Quartz: 2=MON
        assert result == {25}

    def test_nth_occurrence_not_found(self):
        # May 2026 has only 4 Mondays — 5th Monday doesn't exist
        result = _resolve_dow("2#5", 2026, 5)
        assert result == set()


# =============================================================================
# SECTION 4: _next_fire_time — All 46 cases from CRON_QUARTZ_REFERENCE.md
# =============================================================================


class TestNextFireTimeIntervalBased:
    """Cases 01–09: Interval-based (every N minutes)."""

    def test_case_01_every_5_min_daily(self):
        assert _next_fire_time("0 */5 * * * ?", REF) == datetime(2026, 5, 4, 14, 5, 0)

    def test_case_02_every_5_min_daily_02_to_23(self):
        assert _next_fire_time("0 */5 2-23 * * ?", REF) == datetime(2026, 5, 4, 14, 5, 0)

    def test_case_03_every_10_min_daily(self):
        assert _next_fire_time("0 */10 * * * ?", REF) == datetime(2026, 5, 4, 14, 10, 0)

    def test_case_04_every_15_min_daily_02_to_23(self):
        assert _next_fire_time("0 */15 2-23 * * ?", REF) == datetime(2026, 5, 4, 14, 15, 0)

    def test_case_05_every_15_min_biz_hours_weekdays(self):
        # Monday 14:03 → next 15-min mark in 9-17 range
        assert _next_fire_time("0 */15 9-17 ? * MON-FRI", REF) == datetime(2026, 5, 4, 14, 15, 0)

    def test_case_06_every_30_min_daily_02_to_23(self):
        assert _next_fire_time("0 */30 2-23 * * ?", REF) == datetime(2026, 5, 4, 14, 30, 0)

    def test_case_07_every_5_min_weekends(self):
        # Monday → next Saturday = May 9
        assert _next_fire_time("0 */5 * ? * SAT,SUN", REF) == datetime(2026, 5, 9, 0, 0, 0)

    def test_case_08_every_5_min_mon_wed_fri(self):
        # Today is Monday → next 5-min mark
        assert _next_fire_time("0 */5 * ? * MON,WED,FRI", REF) == datetime(2026, 5, 4, 14, 5, 0)

    def test_case_09_every_5_min_mon_wed_fri_02_to_23(self):
        assert _next_fire_time("0 */5 2-23 ? * MON,WED,FRI", REF) == datetime(2026, 5, 4, 14, 5, 0)


class TestNextFireTimeHourly:
    """Cases 10–20: Hourly schedules."""

    def test_case_10_every_hour_daily(self):
        assert _next_fire_time("0 0 * * * ?", REF) == datetime(2026, 5, 4, 15, 0, 0)

    def test_case_11_every_2_hours_daily(self):
        # */2 from 0 → 0,2,4,6,8,10,12,14,16... next after 14:03 = 16:00
        assert _next_fire_time("0 0 */2 * * ?", REF) == datetime(2026, 5, 4, 16, 0, 0)

    def test_case_12_every_2_hours_06_to_22(self):
        # 6-22/2 → 6,8,10,12,14,16,18,20,22; next after 14:03 = 16:00
        assert _next_fire_time("0 0 6-22/2 * * ?", REF) == datetime(2026, 5, 4, 16, 0, 0)

    def test_case_13_every_3_hours_daily(self):
        # */3 → 0,3,6,9,12,15,18,21; next after 14:03 = 15:00
        assert _next_fire_time("0 0 */3 * * ?", REF) == datetime(2026, 5, 4, 15, 0, 0)

    def test_case_14_every_4_hours_daily(self):
        # */4 → 0,4,8,12,16,20; next after 14:03 = 16:00
        assert _next_fire_time("0 0 */4 * * ?", REF) == datetime(2026, 5, 4, 16, 0, 0)

    def test_case_15_every_6_hours_daily(self):
        # */6 → 0,6,12,18; next after 14:03 = 18:00
        assert _next_fire_time("0 0 */6 * * ?", REF) == datetime(2026, 5, 4, 18, 0, 0)

    def test_case_16_every_6_hours_offset_from_02(self):
        # 2/6 → 2,8,14,20; next after 14:03 = 20:00
        assert _next_fire_time("0 0 2/6 * * ?", REF) == datetime(2026, 5, 4, 20, 0, 0)

    def test_case_17_every_hour_02_to_23(self):
        assert _next_fire_time("0 0 2-23 * * ?", REF) == datetime(2026, 5, 4, 15, 0, 0)

    def test_case_18_every_hour_weekends(self):
        # Monday → next Saturday = May 9
        assert _next_fire_time("0 0 * ? * SAT,SUN", REF) == datetime(2026, 5, 9, 0, 0, 0)

    def test_case_19_every_hour_mon_wed_fri(self):
        assert _next_fire_time("0 0 * ? * MON,WED,FRI", REF) == datetime(2026, 5, 4, 15, 0, 0)

    def test_case_20_every_hour_mon_wed_fri_02_to_23(self):
        assert _next_fire_time("0 0 2-23 ? * MON,WED,FRI", REF) == datetime(2026, 5, 4, 15, 0, 0)


class TestNextFireTimeFixedDaily:
    """Cases 21–26: Fixed daily times."""

    def test_case_21_daily_at_0530(self):
        # 14:03 past 05:30 → next day
        assert _next_fire_time("0 30 5 * * ?", REF) == datetime(2026, 5, 5, 5, 30, 0)

    def test_case_22_daily_at_0530_and_1730(self):
        # 14:03 → next is 17:30 today
        assert _next_fire_time("0 30 5,17 * * ?", REF) == datetime(2026, 5, 4, 17, 30, 0)

    def test_case_23_daily_at_0600_1200_1800(self):
        # 14:03 → next is 18:00 today
        assert _next_fire_time("0 0 6,12,18 * * ?", REF) == datetime(2026, 5, 4, 18, 0, 0)

    def test_case_24_daily_at_0530_weekdays(self):
        # Monday 14:03 → next weekday 05:30 = Tuesday May 5
        assert _next_fire_time("0 30 5 ? * MON-FRI", REF) == datetime(2026, 5, 5, 5, 30, 0)

    def test_case_25_daily_at_0530_1730_weekdays(self):
        # Monday 14:03 → 17:30 today (still a weekday)
        assert _next_fire_time("0 30 5,17 ? * MON-FRI", REF) == datetime(2026, 5, 4, 17, 30, 0)

    def test_case_26_every_2hrs_from_0600_mon_wed(self):
        # 6/2 → 6,8,10,12,14,16,18,20,22; Monday 14:03 → 16:00
        assert _next_fire_time("0 0 6/2 ? * MON,WED", REF) == datetime(2026, 5, 4, 16, 0, 0)


class TestNextFireTimeWeekly:
    """Cases 27–30: Weekly schedules."""

    def test_case_27_every_monday_at_0800(self):
        # Monday 14:03 — already past 08:00 → next Monday = May 11
        assert _next_fire_time("0 0 8 ? * MON", REF) == datetime(2026, 5, 11, 8, 0, 0)

    def test_case_28_every_monday_at_0800_and_2000(self):
        # Monday 14:03 → 20:00 today (still Monday)
        assert _next_fire_time("0 0 8,20 ? * MON", REF) == datetime(2026, 5, 4, 20, 0, 0)

    def test_case_29_every_tue_thu_at_0800(self):
        # Monday → next Tuesday = May 5
        assert _next_fire_time("0 0 8 ? * TUE,THU", REF) == datetime(2026, 5, 5, 8, 0, 0)

    def test_case_30_mon_wed_fri_at_0800_and_2000(self):
        # Monday 14:03 → 20:00 today
        assert _next_fire_time("0 0 8,20 ? * MON,WED,FRI", REF) == datetime(2026, 5, 4, 20, 0, 0)


class TestNextFireTimeMonthly:
    """Cases 31–38: Monthly schedules."""

    def test_case_31_every_month_1st_at_0800(self):
        # May 4 → next 1st = June 1
        assert _next_fire_time("0 0 8 1 * ?", REF) == datetime(2026, 6, 1, 8, 0, 0)

    def test_case_32_first_day_of_month_at_0800(self):
        # Same as case 31
        assert _next_fire_time("0 0 8 1 * ?", REF) == datetime(2026, 6, 1, 8, 0, 0)

    def test_case_33_last_day_of_month_at_0800(self):
        # May has 31 days → May 31 at 08:00
        assert _next_fire_time("0 0 8 L * ?", REF) == datetime(2026, 5, 31, 8, 0, 0)

    def test_case_34_second_tuesday_of_month_at_0800(self):
        # May 2026: Tuesdays = 5, 12, 19, 26 → 2nd Tue = 12th
        # Quartz: TUE = 3
        assert _next_fire_time("0 0 8 ? * 3#2", REF) == datetime(2026, 5, 12, 8, 0, 0)

    def test_case_35_nearest_weekday_to_15th_at_0800(self):
        # May 15, 2026 = Friday → already a weekday
        assert _next_fire_time("0 0 8 15W * ?", REF) == datetime(2026, 5, 15, 8, 0, 0)

    def test_case_36_bimonthly_1st_and_15th_at_0800(self):
        # May 4 → next is May 15
        assert _next_fire_time("0 0 8 1,15 * ?", REF) == datetime(2026, 5, 15, 8, 0, 0)

    def test_case_37_bimonthly_08_to_23_hourly(self):
        # May 4 → next 1st or 15th with hour 8-23 = May 15 at 08:00
        assert _next_fire_time("0 0 8-23 1,15 * ?", REF) == datetime(2026, 5, 15, 8, 0, 0)

    def test_case_38_15th_of_month_08_to_23_hourly(self):
        # May 4 → May 15 at 08:00
        assert _next_fire_time("0 0 8-23 15 * ?", REF) == datetime(2026, 5, 15, 8, 0, 0)


class TestNextFireTimeQuarterly:
    """Cases 39–42: Quarterly schedules."""

    def test_case_39_first_day_of_quarter_at_0000(self):
        # May 4 → next quarter start: Jul 1 (months 1,4,7,10)
        assert _next_fire_time("0 0 0 1 1,4,7,10 ?", REF) == datetime(2026, 7, 1, 0, 0, 0)

    def test_case_40_last_day_of_quarter_at_0000(self):
        # May 4 → next quarter end: Jun 30 (months 3,6,9,12)
        assert _next_fire_time("0 0 0 L 3,6,9,12 ?", REF) == datetime(2026, 6, 30, 0, 0, 0)

    def test_case_41_first_day_of_quarter_08_to_23_hourly(self):
        # May 4 → Jul 1 at 08:00
        assert _next_fire_time("0 0 8-23 1 1,4,7,10 ?", REF) == datetime(2026, 7, 1, 8, 0, 0)

    def test_case_42_last_day_of_quarter_08_to_23_hourly(self):
        # May 4 → Jun 30 at 08:00
        assert _next_fire_time("0 0 8-23 L 3,6,9,12 ?", REF) == datetime(2026, 6, 30, 8, 0, 0)


class TestNextFireTimeYearly:
    """Cases 43–46: Yearly schedules."""

    def test_case_43_first_day_of_year_at_0000(self):
        # May 4, 2026 → Jan 1, 2027
        assert _next_fire_time("0 0 0 1 1 ?", REF) == datetime(2027, 1, 1, 0, 0, 0)

    def test_case_44_last_day_of_year_at_0000(self):
        # May 4 → Dec 31, 2026
        assert _next_fire_time("0 0 0 31 12 ?", REF) == datetime(2026, 12, 31, 0, 0, 0)

    def test_case_45_first_day_of_year_08_to_23_hourly(self):
        # May 4, 2026 → Jan 1, 2027 at 08:00
        assert _next_fire_time("0 0 8-23 1 1 ?", REF) == datetime(2027, 1, 1, 8, 0, 0)

    def test_case_46_last_day_of_year_08_to_23_hourly(self):
        # May 4 → Dec 31, 2026 at 08:00
        assert _next_fire_time("0 0 8-23 31 12 ?", REF) == datetime(2026, 12, 31, 8, 0, 0)


# =============================================================================
# SECTION 5: _next_fire_time — Edge cases and boundary conditions
# =============================================================================


class TestNextFireTimeEdgeCases:
    """Edge cases, boundary conditions, and error handling."""

    def test_invalid_field_count_returns_none(self):
        assert _next_fire_time("*/5 * * * *", REF) is None  # 5-field (old format)
        assert _next_fire_time("", REF) is None
        assert _next_fire_time("0 0 0 1 1 ? 2026", REF) is None  # 7-field

    def test_midnight_boundary(self):
        # At 23:59:30 → next minute 0 fires at 00:00 next day
        ref = datetime(2026, 5, 4, 23, 59, 30)
        assert _next_fire_time("0 0 * * * ?", ref) == datetime(2026, 5, 5, 0, 0, 0)

    def test_month_boundary(self):
        # May 31 at 23:00 → next fire for "every hour" = May 31 at 00:00? No — 23:00 + next = Jun 1 00:00
        ref = datetime(2026, 5, 31, 23, 30, 0)
        assert _next_fire_time("0 0 * * * ?", ref) == datetime(2026, 6, 1, 0, 0, 0)

    def test_year_boundary(self):
        # Dec 31 at 23:30 with monthly-1st cron → Jan 1 next year
        ref = datetime(2026, 12, 31, 23, 30, 0)
        assert _next_fire_time("0 0 8 1 * ?", ref) == datetime(2027, 1, 1, 8, 0, 0)

    def test_february_last_day_leap_year(self):
        ref = datetime(2028, 2, 1, 0, 0, 0)
        assert _next_fire_time("0 0 8 L * ?", ref) == datetime(2028, 2, 29, 8, 0, 0)

    def test_february_last_day_non_leap_year(self):
        ref = datetime(2025, 2, 1, 0, 0, 0)
        assert _next_fire_time("0 0 8 L * ?", ref) == datetime(2025, 2, 28, 8, 0, 0)

    def test_exact_fire_time_returns_next(self):
        # If 'after' is exactly on a fire time, should return the NEXT one
        ref = datetime(2026, 5, 4, 14, 0, 0)  # Exactly on the hour
        result = _next_fire_time("0 0 * * * ?", ref)
        assert result == datetime(2026, 5, 4, 15, 0, 0)

    def test_dow_wraps_to_next_week(self):
        # Tuesday ref, cron fires only on Mondays at 08:00 → next Monday
        ref = datetime(2026, 5, 5, 10, 0, 0)  # Tuesday
        assert _next_fire_time("0 0 8 ? * MON", ref) == datetime(2026, 5, 11, 8, 0, 0)

    def test_hour_range_skips_outside_window(self):
        # At 01:00, cron is 2-23 → should fire at 02:00
        ref = datetime(2026, 5, 4, 1, 0, 0)
        assert _next_fire_time("0 0 2-23 * * ?", ref) == datetime(2026, 5, 4, 2, 0, 0)

    def test_hour_range_wraps_to_next_day(self):
        # At 23:30, cron is 2-22/2 → next valid = tomorrow 02:00... wait no.
        # 2-22/2 → 2,4,6,8,10,12,14,16,18,20,22. At 23:30 nothing left → next day 02:00
        ref = datetime(2026, 5, 4, 23, 30, 0)
        assert _next_fire_time("0 0 2-22/2 * * ?", ref) == datetime(2026, 5, 5, 2, 0, 0)

    def test_month_names_supported(self):
        # JAN,APR,JUL,OCT equivalent to 1,4,7,10
        ref = datetime(2026, 5, 4, 14, 0, 0)
        assert _next_fire_time("0 0 0 1 JAN,APR,JUL,OCT ?", ref) == datetime(2026, 7, 1, 0, 0, 0)


# =============================================================================
# SECTION 6: is_dispatch_due — Integration tests
# =============================================================================


class TestIsDispatchDue:
    """Tests for the public is_dispatch_due function with Quartz 6-field cron."""

    def test_empty_cron_returns_false(self):
        assert is_dispatch_due("", None) is False

    def test_none_cron_returns_false(self):
        assert is_dispatch_due(None, None) is False

    def test_never_dispatched_returns_true(self):
        assert is_dispatch_due("0 */5 * * * ?", None) is True

    def test_due_after_fire_time_passed(self):
        # Cron: every hour at :00. Last dispatch 14:00, now 15:01 → due (next fire was 15:00)
        now = datetime(2026, 5, 4, 15, 1, 0, tzinfo=UTC)
        last = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)
        assert is_dispatch_due("0 0 * * * ?", last, now_utc=now) is True

    def test_not_due_before_fire_time(self):
        # Cron: every hour at :00. Last dispatch 14:00, now 14:30 → not due (next fire 15:00)
        now = datetime(2026, 5, 4, 14, 30, 0, tzinfo=UTC)
        last = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)
        assert is_dispatch_due("0 0 * * * ?", last, now_utc=now) is False

    def test_due_exactly_at_fire_time(self):
        # Now is exactly at next fire time → due
        now = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
        last = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)
        assert is_dispatch_due("0 0 * * * ?", last, now_utc=now) is True

    def test_monthly_not_due_wrong_day(self):
        # Cron: 1st of month at 08:00. Last dispatch May 1, now May 4 → not due until Jun 1
        now = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)
        last = datetime(2026, 5, 1, 8, 0, 0, tzinfo=UTC)
        assert is_dispatch_due("0 0 8 1 * ?", last, now_utc=now) is False

    def test_monthly_due_on_correct_day(self):
        # Cron: 1st of month at 08:00. Last dispatch May 1, now Jun 1 09:00 → due
        now = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
        last = datetime(2026, 5, 1, 8, 0, 0, tzinfo=UTC)
        assert is_dispatch_due("0 0 8 1 * ?", last, now_utc=now) is True

    def test_naive_last_dispatched_at_treated_as_utc(self):
        # Naive datetime should be treated as UTC
        now = datetime(2026, 5, 4, 15, 1, 0, tzinfo=UTC)
        last = datetime(2026, 5, 4, 14, 0, 0)  # naive
        assert is_dispatch_due("0 0 * * * ?", last, now_utc=now) is True


# =============================================================================
# SECTION 7: compute_next_dispatched_at — for MERGE statement
# =============================================================================


class TestComputeNextDispatchedAt:
    """Tests for the MERGE helper function."""

    def test_returns_next_fire_from_now(self):
        now = datetime(2026, 5, 4, 14, 3, 27, tzinfo=UTC)
        result = compute_next_dispatched_at("0 0 * * * ?", now_utc=now)
        assert result == datetime(2026, 5, 4, 15, 0, 0)

    def test_monthly_returns_next_month(self):
        now = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)
        result = compute_next_dispatched_at("0 0 8 1 * ?", now_utc=now)
        assert result == datetime(2026, 6, 1, 8, 0, 0)

    def test_invalid_cron_returns_none(self):
        now = datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC)
        assert compute_next_dispatched_at("", now_utc=now) is None


# =============================================================================
# SECTION 8: should_auto_trigger_row — with Quartz format
# =============================================================================


class TestShouldAutoTriggerRow:
    """Integration tests for the full row eligibility check."""

    def test_inactive_row_returns_false(self):
        now = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
        row = {"ctl_active": "N", "sched_cron": "0 0 * * * ?", "last_dispatched_at": None}
        assert should_auto_trigger_row(row, now_utc=now) is False

    def test_auto_trigger_disabled_returns_false(self):
        now = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
        row = {"ctl_active": "Y", "ctl_auto_trigger": "N", "sched_cron": "0 0 * * * ?", "last_dispatched_at": None}
        assert should_auto_trigger_row(row, now_utc=now) is False

    def test_maintenance_hold_active_returns_false(self):
        now = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
        row = {
            "ctl_active": "Y",
            "ctl_auto_trigger": "Y",
            "sched_cron": "0 0 * * * ?",
            "last_dispatched_at": None,
            "ctl_maintenance_hold_until": "2026-05-05T00:00:00Z",
        }
        assert should_auto_trigger_row(row, now_utc=now) is False

    def test_happy_path_due(self):
        now = datetime(2026, 5, 4, 15, 1, 0, tzinfo=UTC)
        row = {
            "ctl_active": "Y",
            "ctl_auto_trigger": "Y",
            "sched_cron": "0 0 * * * ?",
            "last_dispatched_at": datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC),
            "ctl_maintenance_hold_until": "",
        }
        assert should_auto_trigger_row(row, now_utc=now) is True

    def test_happy_path_not_yet_due(self):
        now = datetime(2026, 5, 4, 14, 30, 0, tzinfo=UTC)
        row = {
            "ctl_active": "Y",
            "ctl_auto_trigger": "Y",
            "sched_cron": "0 0 * * * ?",
            "last_dispatched_at": datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC),
            "ctl_maintenance_hold_until": "",
        }
        assert should_auto_trigger_row(row, now_utc=now) is False

    def test_honor_flags_can_be_overridden(self):
        now = datetime(2026, 5, 4, 15, 1, 0, tzinfo=UTC)
        row = {
            "ctl_active": "Y",
            "ctl_auto_trigger": "N",
            "sched_cron": "0 0 * * * ?",
            "last_dispatched_at": datetime(2026, 5, 4, 14, 0, 0, tzinfo=UTC),
            "ctl_maintenance_hold_until": "2026-12-31T00:00:00Z",
        }
        assert should_auto_trigger_row(row, now_utc=now, honor_config_auto_trigger=False, honor_maintenance_hold=False) is True


# =============================================================================
# SECTION 9: DOW/N — Biweekly / Week-Step scheduling
# =============================================================================


class TestNextFireTimeDowStep:
    """Tests for DOW/N week-step syntax (every Nth week for a given weekday)."""

    def test_every_other_monday_even_weeks(self):
        # REF=Mon May 4 (ISO wk19, odd) → next even-week Mon = May 11 (wk20)
        assert _next_fire_time("0 0 8 ? * 2/2", REF) == datetime(2026, 5, 11, 8, 0, 0)

    def test_every_other_monday_odd_weeks(self):
        # REF=Mon May 4 14:03 (wk19, odd) → past 08:00 → next odd-week Mon = May 18 (wk21)
        assert _next_fire_time("0 0 8 ? * 2/2+1", REF) == datetime(2026, 5, 18, 8, 0, 0)

    def test_every_3rd_friday(self):
        # May Fridays: 1(wk18), 8(wk19), 15(wk20), 22(wk21), 29(wk22)
        # wk%3==0: wk18=May1(passed), wk21=May22
        result = _next_fire_time("0 0 8 ? * 6/3", REF)
        assert result == datetime(2026, 5, 22, 8, 0, 0)

    def test_biweekly_tuesday(self):
        # REF=Mon May 4 → next Tue = May 5 (wk19). TUE/2 even weeks: wk20=May 12
        result = _next_fire_time("0 0 8 ? * 3/2", REF)
        # May 5 = wk19 (odd, skip), May 12 = wk20 (even, match)
        assert result == datetime(2026, 5, 12, 8, 0, 0)

    def test_every_other_monday_at_2000(self):
        # REF=Mon May 4 14:03, MON/2 even weeks. May 4=wk19(odd) → skip.
        # Next even-week Mon = May 11 at 20:00
        assert _next_fire_time("0 0 20 ? * 2/2", REF) == datetime(2026, 5, 11, 20, 0, 0)


# =============================================================================
# SECTION 10: Multi-cron pipe separator
# =============================================================================


class TestNextFireTimeMultiCron:
    """Tests for pipe-separated multi-cron expressions."""

    def test_three_fixed_times(self):
        # 7:30 | 11:00 | 16:45 — at 14:03, next is 16:45
        multi = "0 30 7 * * ? | 0 0 11 * * ? | 0 45 16 * * ?"
        assert _next_fire_time(multi, REF) == datetime(2026, 5, 4, 16, 45, 0)

    def test_two_times_picks_earlier(self):
        # 6:00 | 18:00 — at 14:03, next is 18:00 (6:00 tomorrow is later)
        multi = "0 0 6 * * ? | 0 0 18 * * ?"
        assert _next_fire_time(multi, REF) == datetime(2026, 5, 4, 18, 0, 0)

    def test_single_expr_no_pipe(self):
        # No pipe — should behave normally
        assert _next_fire_time("0 0 * * * ?", REF) == datetime(2026, 5, 4, 15, 0, 0)

    def test_all_past_wraps_to_next_day(self):
        # All times earlier today → picks earliest tomorrow
        ref_late = datetime(2026, 5, 4, 23, 0, 0)
        multi = "0 0 6 * * ? | 0 0 12 * * ? | 0 0 18 * * ?"
        result = _next_fire_time(multi, ref_late)
        assert result == datetime(2026, 5, 5, 6, 0, 0)

    def test_pipe_with_invalid_expr_skips_it(self):
        # One valid, one invalid (5-field) — only valid one fires
        multi = "bad expr | 0 0 18 * * ?"
        result = _next_fire_time(multi, REF)
        assert result == datetime(2026, 5, 4, 18, 0, 0)


# =============================================================================
# SECTION 11: Timezone-aware scheduling
# =============================================================================


class TestNextFireTimeTimezone:
    """Tests for timezone-aware fire time computation."""

    def test_hourly_new_york(self):
        # 14:03 UTC = 10:03 ET (EDT, UTC-4 in May)
        # Next :00 ET = 11:00 ET = 15:00 UTC
        ref_utc = datetime(2026, 5, 4, 14, 3, 27, tzinfo=UTC)
        result = _next_fire_time("0 0 * * * ?", ref_utc, tz_name="America/New_York")
        assert result.hour == 15 and result.minute == 0

    def test_daily_0530_new_york(self):
        # 14:03 UTC = 10:03 ET → past 05:30 ET → next day 05:30 ET = 09:30 UTC
        ref_utc = datetime(2026, 5, 4, 14, 3, 27, tzinfo=UTC)
        result = _next_fire_time("0 30 5 * * ?", ref_utc, tz_name="America/New_York")
        assert result == datetime(2026, 5, 5, 9, 30, 0, tzinfo=UTC)

    def test_no_timezone_unchanged(self):
        # Without tz_name, behaves exactly as before
        result = _next_fire_time("0 0 * * * ?", REF, tz_name=None)
        assert result == datetime(2026, 5, 4, 15, 0, 0)

    def test_is_dispatch_due_with_timezone(self):
        # Hourly ET. Last dispatch at 11:00 ET (15:00 UTC). Next fire = 12:00 ET (16:00 UTC).
        # Now = 15:30 UTC (11:30 ET) → not due
        now = datetime(2026, 5, 4, 15, 30, 0, tzinfo=UTC)
        last = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
        assert is_dispatch_due("0 0 * * * ?", last, now_utc=now, tz_name="America/New_York") is False

    def test_is_dispatch_due_with_timezone_due(self):
        # Same as above but now = 16:01 UTC (12:01 ET) → due
        now = datetime(2026, 5, 4, 16, 1, 0, tzinfo=UTC)
        last = datetime(2026, 5, 4, 15, 0, 0, tzinfo=UTC)
        assert is_dispatch_due("0 0 * * * ?", last, now_utc=now, tz_name="America/New_York") is True
