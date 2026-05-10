"""
Unit tests for `choose_update_mode`.

These tests exercise the underlying function (via `.function`) and mock
`get_current_context` directly, so they don't require a running Airflow
scheduler or a DagRun.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pendulum
import pytest
from loci.collectors.base_spec import DatasetSpec
from loci.sources.update_configs import DatasetUpdateConfig
from loci.tasks.task_utils import choose_update_mode

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


class _FakeSpec(DatasetSpec):
    """Minimal DatasetSpec stand-in. choose_update_mode never touches the spec."""

    source = "fake"
    name = "fake"
    target_table = "fake"
    target_schema = "raw_data"
    entity_key = None


@pytest.fixture
def spec() -> DatasetSpec:
    return _FakeSpec()


@pytest.fixture
def task_logger() -> logging.Logger:
    return logging.getLogger("test_choose_update_mode")


def _make_context(
    logical_date: pendulum.DateTime,
    *,
    task_id: str = "tg.choose_update_mode",
    force_full_refresh: bool = False,
    run_type: str = "scheduled",
) -> dict:
    """Build a fake Airflow context dict with the fields the task reads."""
    ti = MagicMock()
    ti.task_id = task_id
    dag_run = MagicMock()
    dag_run.run_type = run_type
    return {
        "ti": ti,
        "logical_date": logical_date,
        "params": {"force_full_refresh": force_full_refresh},
        "dag_run": dag_run,
    }


def _run(update_config: DatasetUpdateConfig, context: dict, task_logger: logging.Logger) -> str:
    """Invoke the underlying function with a patched get_current_context."""
    with patch("loci.tasks.task_utils.get_current_context", return_value=context):
        return choose_update_mode.function(update_config=update_config, task_logger=task_logger)


# ---------------------------------------------------------------------------
# Core branching: full vs incremental based on date + config
# ---------------------------------------------------------------------------


class TestCoreBranching:
    """All three of (month, week_of_month, day_of_week) must match for a full update."""

    def test_full_update_when_all_three_match(self, spec, task_logger):
        # First Tuesday of Jan 2026 is Jan 6. Pendulum Tuesday = 1.
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 6))

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"

    def test_incremental_when_week_does_not_match(self, spec, task_logger):
        # Jan 13, 2026 is a Tuesday in week 2. Config wants week 1.
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 13))

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"

    def test_incremental_when_day_does_not_match(self, spec, task_logger):
        # Jan 7, 2026 is a Wednesday (Pendulum 2) in week 1. Config wants Tuesday (1).
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 7))

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"

    def test_task_group_id_prefix_is_preserved(self, spec, task_logger):
        """The branch decision should include the calling task group's prefix."""
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(
            pendulum.datetime(2026, 1, 6),
            task_id="update_arcgishub_table.choose_update_mode",
        )

        result = _run(config, context, task_logger)

        assert result == "update_arcgishub_table.run_full_update"

    def test_no_task_group_prefix(self, spec, task_logger):
        """A bare task_id (no group) should produce an unprefixed return value."""
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 6), task_id="choose_update_mode")

        result = _run(config, context, task_logger)

        assert result == "run_full_update"


# ---------------------------------------------------------------------------
# force_full_refresh override
# ---------------------------------------------------------------------------


class TestForceFullRefresh:
    """force_full_refresh in DAG run conf should short-circuit the date checks."""

    def test_force_overrides_date_mismatch(self, spec, task_logger):
        # Jan 13 wouldn't otherwise qualify (week 2, config wants week 1).
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 13), force_full_refresh=True)

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"

    def test_force_overrides_month_filter(self, spec, task_logger):
        """Even when the month is excluded, force_full_refresh wins."""
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
            full_update_months=(6,),  # June only
        )
        context = _make_context(pendulum.datetime(2026, 1, 6), force_full_refresh=True)

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"

    def test_force_false_falls_through_to_normal_logic(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 13), force_full_refresh=False)

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"


# ---------------------------------------------------------------------------
# Optional full_update_day_of_week (the new behavior)
# ---------------------------------------------------------------------------


class TestOptionalDayOfWeek:
    """When full_update_day_of_week is None, the day check is skipped."""

    def test_none_with_matching_week_runs_full_update(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            # full_update_day_of_week omitted -> defaults to None
        )
        # Jan 6, 2026: Tuesday in week 1. Day-of-week is irrelevant here.
        context = _make_context(pendulum.datetime(2026, 1, 6))

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"

    def test_none_runs_full_on_any_weekday_in_target_week(self, spec, task_logger):
        """With day_of_week=None, any DAG run inside week 1 triggers a full update.

        This isn't how the cron above is actually configured (Tuesdays only), but
        the function shouldn't enforce a day when the config doesn't specify one.
        """
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
        )
        # Jan 5, 2026 is a Monday in week 1.
        context = _make_context(pendulum.datetime(2026, 1, 5))

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"

    def test_none_still_respects_week_of_month(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
        )
        # Jan 13 is in week 2 -- should still go incremental.
        context = _make_context(pendulum.datetime(2026, 1, 13))

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"

    def test_none_still_respects_full_update_months(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_months=(6,),  # June only
        )
        # Jan 6 is week 1 but January isn't in full_update_months.
        context = _make_context(pendulum.datetime(2026, 1, 6))

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"


# ---------------------------------------------------------------------------
# full_update_months filtering
# ---------------------------------------------------------------------------


class TestFullUpdateMonths:
    """full_update_months gates which months can have full refreshes at all."""

    def test_excluded_month_routes_incremental(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
            full_update_months=(6, 12),  # June and December only
        )
        # Jan 6, 2026: matches week and day, but Jan is excluded.
        context = _make_context(pendulum.datetime(2026, 1, 6))

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"

    def test_included_month_runs_full_update(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
            full_update_months=(6, 12),
        )
        # Dec 1, 2026 is a Tuesday in week 1.
        context = _make_context(pendulum.datetime(2026, 12, 1))

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"

    def test_default_months_includes_all(self, spec, task_logger):
        """The default tuple covers every month."""
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        # Pick a month-1 date that satisfies week and day.
        context = _make_context(pendulum.datetime(2026, 1, 6))

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"


# ---------------------------------------------------------------------------
# Manual / non-scheduled runs
# ---------------------------------------------------------------------------


class TestNonScheduledRuns:
    """Manually triggered, backfill, and asset-triggered runs should default to
    incremental, regardless of the date. Only force_full_refresh overrides this."""

    def test_manual_run_on_full_update_date_routes_incremental(self, spec, task_logger):
        # Jan 6, 2026 would qualify for a full update on a scheduled run.
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 6), run_type="manual")

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"

    def test_manual_run_with_force_full_refresh_routes_full(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        # Date doesn't qualify, but force_full_refresh wins on manual runs too.
        context = _make_context(
            pendulum.datetime(2026, 1, 13),
            run_type="manual",
            force_full_refresh=True,
        )

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"

    def test_backfill_run_routes_incremental(self, spec, task_logger):
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 6), run_type="backfill")

        result = _run(config, context, task_logger)

        assert result == "tg.run_incremental_update"

    def test_scheduled_run_still_uses_date_logic(self, spec, task_logger):
        """Sanity check that the default run_type='scheduled' still triggers full updates."""
        config = DatasetUpdateConfig(
            spec=spec,
            update_cron="7 3 * * 2",
            full_update_week_of_month=1,
            full_update_day_of_week=1,
        )
        context = _make_context(pendulum.datetime(2026, 1, 6))  # default run_type='scheduled'

        result = _run(config, context, task_logger)

        assert result == "tg.run_full_update"
