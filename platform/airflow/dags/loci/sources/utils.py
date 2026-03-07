from __future__ import annotations

import inspect
import types

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

SOURCE_COLORS = [
    "#e6194b",
    "#3cb44b",
    "#4363d8",
    "#f58231",
    "#911eb4",
    "#42d4f4",
    "#f032e6",
    "#bfef45",
    "#fabed4",
    "#469990",
    "#dcbeff",
    "#9A6324",
    "#800000",
    "#aaffc3",
    "#808000",
]


class ScheduleVisualizer:
    """Visualize DatasetUpdateConfig schedules in a Jupyter notebook.

    Usage:
        from loci.sources import update_configs
        from loci.db.af_utils import get_postgres_engine

        engine = get_postgres_engine(conn_id="gis_dwh_db")
        viz = ScheduleVisualizer.from_module(update_configs, engine=engine)

        viz.plot_incremental()                     # dots only, color-coded by source
        viz.plot_incremental(source="socrata")     # numbered markers + printed legend
        viz.plot_gantt_incremental(source="socrata", alpha=0.6)
    """

    def __init__(self, configs: list, engine=None) -> None:
        self.configs = configs
        self.engine = engine
        self._source_color_map: dict[str, str] = {}
        self._runtimes: dict[str, dict[str, float]] = {}
        self._assign_colors()
        if self.engine is not None:
            self._load_runtimes()

    @classmethod
    def from_module(cls, module: types.ModuleType, engine=None) -> ScheduleVisualizer:
        """Auto-discover all DatasetUpdateConfig instances in a module."""
        from loci.sources.update_configs import DatasetUpdateConfig

        configs = [
            obj for name, obj in inspect.getmembers(module) if isinstance(obj, DatasetUpdateConfig)
        ]
        return cls(configs, engine=engine)

    def _assign_colors(self) -> None:
        sources = sorted(set(cfg.spec.source for cfg in self.configs))
        for i, source in enumerate(sources):
            self._source_color_map[source] = SOURCE_COLORS[i % len(SOURCE_COLORS)]

    @property
    def sources(self) -> list[str]:
        return sorted(self._source_color_map.keys())

    def _filter_configs(self, source: str | None = None) -> list:
        if source is None:
            return self.configs
        return [cfg for cfg in self.configs if cfg.spec.source == source]

    # ── Runtime loading ──────────────────────────────────────────────────

    def _load_runtimes(self) -> None:
        """Query meta.ingest_log for average runtimes per dataset and mode."""
        df = self.engine.query("""
            select
                dataset_id,
                case
                    when metadata->>'mode' = 'incremental' then 'incremental'
                    else 'full_refresh'
                end as mode_group,
                avg(extract(epoch from (completed_at - started_at)) / 60.0) as avg_minutes
            from meta.ingest_log
            where status = 'success'
            group by dataset_id, mode_group
        """)
        self._runtimes = {}
        for _, row in df.iterrows():
            did = row["dataset_id"]
            if did not in self._runtimes:
                self._runtimes[did] = {}
            self._runtimes[did][row["mode_group"]] = float(row["avg_minutes"])

    def _get_runtime_minutes(self, dataset_id: str, mode: str) -> float:
        """Get average runtime in minutes. Returns a small default if unknown."""
        return self._runtimes.get(dataset_id, {}).get(mode, 5.0)

    # ── Labeling helpers ─────────────────────────────────────────────────

    @staticmethod
    def _build_number_map(configs: list) -> dict[str, int]:
        """Assign a stable number to each dataset name, sorted alphabetically."""
        names = sorted(set(cfg.spec.name for cfg in configs))
        return {name: i + 1 for i, name in enumerate(names)}

    @staticmethod
    def _print_number_legend(number_map: dict[str, int]) -> None:
        """Print the number-to-name mapping below the plot."""
        print("\nDataset Legend:")
        for name, num in sorted(number_map.items(), key=lambda x: x[1]):
            print(f"  {num:>3}: {name}")

    def _annotate_point(self, ax, x, y, cfg, number_map: dict[str, int]) -> None:
        num = number_map[cfg.spec.name]
        ax.annotate(
            str(num),
            (x, y),
            fontsize=6,
            fontweight="bold",
            ha="center",
            va="center",
        )

    def _annotate_bar(self, ax, x, y, cfg, number_map: dict[str, int]) -> None:
        num = number_map[cfg.spec.name]
        ax.text(x, y, f" {num}", fontsize=6, fontweight="bold", va="center")

    def _finish_plot(self, ax, configs, title, source, number_map: dict[str, int] | None) -> None:
        if source:
            title += f" — {source}"
        ax.set_title(title, fontsize=14, fontweight="bold")
        self._add_source_legend(ax, configs)
        plt.tight_layout()
        plt.show()
        if number_map is not None:
            self._print_number_legend(number_map)

    def _add_source_legend(self, ax, configs: list) -> None:
        sources_in_plot = sorted(set(cfg.spec.source for cfg in configs))
        patches = [
            mpatches.Patch(color=self._source_color_map[s], label=s) for s in sources_in_plot
        ]
        ax.legend(handles=patches, loc="upper right", fontsize=8)

    # ── Scatter plots ────────────────────────────────────────────────────

    def plot_incremental(self, source: str | None = None, alpha: float = 0.8) -> None:
        """Plot incremental update schedule on a weekly grid (days x hours).

        When source is None: unlabeled dots, color-coded by source.
        When source is specified: numbered markers with a printed legend.
        """
        configs = self._filter_configs(source)
        if not configs:
            print(f"No configs found for source: {source}")
            return

        number_map = self._build_number_map(configs) if source else None
        fig, ax = plt.subplots(figsize=(14, 8))

        for cfg in configs:
            color = self._source_color_map[cfg.spec.source]
            slots = self._parse_weekly_slots(cfg.update_cron)
            for day_index, hour, minute in slots:
                time_val = hour + minute / 60
                ax.scatter(
                    day_index,
                    time_val,
                    color=color,
                    s=60,
                    zorder=3,
                    alpha=alpha,
                    edgecolors="black",
                    linewidths=0.5,
                )
                if number_map is not None:
                    self._annotate_point(ax, day_index, time_val, cfg, number_map)

        self._style_weekly_grid(ax)
        self._finish_plot(ax, configs, "Incremental Update Schedule", source, number_map)

    def plot_full_update(self, source: str | None = None, alpha: float = 0.8) -> None:
        """Plot full update schedule on a monthly grid (day-of-month x hours).

        When source is None: unlabeled dots, color-coded by source.
        When source is specified: numbered markers with a printed legend.
        """
        configs = self._filter_configs(source)
        if not configs:
            print(f"No configs found for source: {source}")
            return

        number_map = self._build_number_map(configs) if source else None
        fig, ax = plt.subplots(figsize=(16, 8))

        for cfg in configs:
            color = self._source_color_map[cfg.spec.source]
            day_mid = (cfg.full_update_week_of_month - 1) * 7 + 1 + cfg.full_update_day_of_week

            slots = self._parse_weekly_slots(cfg.update_cron)
            if slots:
                _, hour, minute = slots[0]
            else:
                hour, minute = 0, 0

            time_val = hour + minute / 60
            ax.scatter(
                day_mid,
                time_val,
                color=color,
                s=80,
                zorder=3,
                alpha=alpha,
                edgecolors="black",
                linewidths=0.5,
                marker="D",
            )
            if number_map is not None:
                self._annotate_point(ax, day_mid, time_val, cfg, number_map)

        self._style_monthly_grid(ax)
        self._finish_plot(ax, configs, "Full Update Schedule", source, number_map)

    # ── Gantt plots ──────────────────────────────────────────────────────

    def plot_gantt_incremental(self, source: str | None = None, alpha: float = 0.8) -> None:
        """Gantt chart of incremental updates on a weekly grid.

        Requires engine to have been passed to the constructor.
        """
        configs = self._filter_configs(source)
        if not configs:
            print(f"No configs found for source: {source}")
            return
        if not self._runtimes:
            print("No runtimes available. Pass engine to the constructor.")
            return

        number_map = self._build_number_map(configs) if source else None
        fig, ax = plt.subplots(figsize=(14, 8))

        for cfg in configs:
            color = self._source_color_map[cfg.spec.source]
            duration_hours = self._get_runtime_minutes(cfg.spec.dataset_id, "incremental") / 60

            slots = self._parse_weekly_slots(cfg.update_cron)
            for day_index, hour, minute in slots:
                start_time = hour + minute / 60
                ax.barh(
                    day_index,
                    duration_hours,
                    left=start_time,
                    height=0.3,
                    color=color,
                    alpha=alpha,
                    edgecolor="black",
                    linewidth=0.5,
                    zorder=3,
                )
                if number_map is not None:
                    label_x = start_time + duration_hours + 0.05
                    self._annotate_bar(ax, label_x, day_index, cfg, number_map)

        self._style_gantt_weekly(ax)
        self._finish_plot(ax, configs, "Incremental Update Gantt", source, number_map)

    def plot_gantt_full_update(self, source: str | None = None, alpha: float = 0.8) -> None:
        """Gantt chart of full updates on a monthly grid.

        Requires engine to have been passed to the constructor.
        """
        configs = self._filter_configs(source)
        if not configs:
            print(f"No configs found for source: {source}")
            return
        if not self._runtimes:
            print("No runtimes available. Pass engine to the constructor.")
            return

        number_map = self._build_number_map(configs) if source else None
        fig, ax = plt.subplots(figsize=(16, 8))

        for cfg in configs:
            color = self._source_color_map[cfg.spec.source]
            duration_hours = self._get_runtime_minutes(cfg.spec.dataset_id, "full_refresh") / 60

            day_mid = (cfg.full_update_week_of_month - 1) * 7 + 1 + cfg.full_update_day_of_week

            slots = self._parse_weekly_slots(cfg.update_cron)
            if slots:
                _, hour, minute = slots[0]
            else:
                hour, minute = 0, 0

            start_time = hour + minute / 60
            ax.barh(
                day_mid,
                duration_hours,
                left=start_time,
                height=0.5,
                color=color,
                alpha=alpha,
                edgecolor="black",
                linewidth=0.5,
                zorder=3,
            )
            if number_map is not None:
                label_x = start_time + duration_hours + 0.05
                self._annotate_bar(ax, label_x, day_mid, cfg, number_map)

        self._style_gantt_monthly(ax)
        self._finish_plot(ax, configs, "Full Update Gantt", source, number_map)

    # ── Run history plot ─────────────────────────────────────────────────

    MODE_COLORS = {
        "incremental": "#3cb44b",
        "full_refresh_api": "#4363d8",
        "full_refresh_file": "#e6194b",
    }

    def plot_run_history(self, target_table: str, alpha: float = 0.8) -> None:
        """Gantt chart of all historical runs for a single target table.

        Y-axis is the date of each run, x-axis is time of day.
        Bar length shows actual duration. Color-coded by mode.
        Requires engine to have been passed to the constructor.
        """
        if self.engine is None:
            print("No engine available. Pass engine to the constructor.")
            return

        df = self.engine.query(
            """
            select
                started_at,
                completed_at,
                metadata->>'mode' as mode,
                status
            from meta.ingest_log
            where target_table = %(target_table)s
            order by started_at
            """,
            {"target_table": target_table},
        )

        if df.empty:
            print(f"No ingest_log records found for target_table: {target_table}")
            return

        fig, ax = plt.subplots(figsize=(14, max(6, len(df) * 0.35)))

        date_labels = []
        for i, (_, row) in enumerate(df.iterrows()):
            started = row["started_at"]
            completed = row["completed_at"]
            mode = row["mode"] or "unknown"
            status = row["status"]

            start_hours = started.hour + started.minute / 60 + started.second / 3600
            duration_hours = (completed - started).total_seconds() / 3600

            color = self.MODE_COLORS.get(mode, "#999999")
            edge_color = "red" if status != "success" else "black"

            ax.barh(
                i,
                duration_hours,
                left=start_hours,
                height=0.6,
                color=color,
                alpha=alpha,
                edgecolor=edge_color,
                linewidth=0.5,
                zorder=3,
            )

            date_labels.append(started.strftime("%Y-%m-%d"))

        ax.set_yticks(range(len(date_labels)))
        ax.set_yticklabels(date_labels, fontsize=7)
        ax.set_xlim(0, 24)
        ax.set_xticks(range(0, 25))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8, rotation=45)
        ax.set_xlabel("Time of Day", fontsize=11)
        ax.set_ylabel("Run Date", fontsize=11)
        ax.invert_yaxis()
        ax.grid(True, axis="x", alpha=0.3)

        patches = [mpatches.Patch(color=c, label=m) for m, c in self.MODE_COLORS.items()]
        patches.append(
            mpatches.Patch(
                facecolor="white",
                edgecolor="red",
                linewidth=1.5,
                label="failed",
            )
        )
        ax.legend(handles=patches, loc="upper right", fontsize=8)

        ax.set_title(f"Run History — {target_table}", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.show()

    # ── Parsing ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_weekly_slots(cron_expr: str) -> list[tuple[int, int, int]]:
        """Parse a cron expression and return (day_index, hour, minute) tuples.

        day_index: 0=Monday .. 6=Sunday
        """
        parts = cron_expr.split()
        if len(parts) != 5:
            return []

        minute_str, hour_str, _, _, dow_str = parts

        minutes = _expand_cron_field(minute_str, 0, 59)
        hours = _expand_cron_field(hour_str, 0, 23)
        dows = _expand_cron_field(dow_str, 0, 6)

        # Cron uses 0=Sunday, convert to 0=Monday
        slots = []
        for dow in dows:
            day_index = (dow - 1) % 7  # cron 1=Mon->0, 0=Sun->6
            for h in hours:
                for m in minutes:
                    slots.append((day_index, h, m))
        return slots

    # ── Grid styling ─────────────────────────────────────────────────────

    @staticmethod
    def _style_weekly_grid(ax) -> None:
        ax.set_xlim(-0.5, 6.5)
        ax.set_xticks(range(7))
        ax.set_xticklabels(DAYS_OF_WEEK, fontsize=9)
        ax.set_ylim(-0.5, 24)
        ax.set_yticks(range(0, 25))
        ax.set_yticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8)
        ax.set_ylabel("Time of Day", fontsize=11)
        ax.set_xlabel("Day of Week", fontsize=11)
        ax.invert_yaxis()
        ax.grid(True, alpha=0.3)

    @staticmethod
    def _style_monthly_grid(ax) -> None:
        ax.set_xlim(0.5, 31.5)
        ax.set_xticks(range(1, 32))
        ax.set_xlabel("Day of Month", fontsize=11)
        ax.set_ylim(-0.5, 24)
        ax.set_yticks(range(0, 25))
        ax.set_yticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8)
        ax.set_ylabel("Time of Day", fontsize=11)
        ax.invert_yaxis()
        ax.grid(True, alpha=0.3)

    @staticmethod
    def _style_gantt_weekly(ax) -> None:
        ax.set_ylim(-0.5, 6.5)
        ax.set_yticks(range(7))
        ax.set_yticklabels(DAYS_OF_WEEK, fontsize=9)
        ax.set_xlim(0, 24)
        ax.set_xticks(range(0, 25))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8, rotation=45)
        ax.set_xlabel("Time of Day", fontsize=11)
        ax.set_ylabel("Day of Week", fontsize=11)
        ax.invert_yaxis()
        ax.grid(True, axis="x", alpha=0.3)

    @staticmethod
    def _style_gantt_monthly(ax) -> None:
        ax.set_ylim(0.5, 31.5)
        ax.set_yticks(range(1, 32))
        ax.set_ylabel("Day of Month", fontsize=11)
        ax.set_xlim(0, 24)
        ax.set_xticks(range(0, 25))
        ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8, rotation=45)
        ax.set_xlabel("Time of Day", fontsize=11)
        ax.invert_yaxis()
        ax.grid(True, axis="x", alpha=0.3)


def _expand_cron_field(field: str, min_val: int, max_val: int) -> list[int]:
    """Expand a single cron field into a list of integer values."""
    if field == "*":
        return list(range(min_val, max_val + 1))

    values = set()
    for part in field.split(","):
        if "/" in part:
            range_part, step = part.split("/")
            step = int(step)
            if range_part == "*":
                start, end = min_val, max_val
            elif "-" in range_part:
                start, end = map(int, range_part.split("-"))
            else:
                start, end = int(range_part), max_val
            values.update(range(start, end + 1, step))
        elif "-" in part:
            start, end = map(int, part.split("-"))
            values.update(range(start, end + 1))
        else:
            values.add(int(part))
    return sorted(values)


# from __future__ import annotations

# import inspect
# import types
# from typing import Optional

# import matplotlib.pyplot as plt
# import matplotlib.patches as mpatches


# DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# SOURCE_COLORS = [
#     "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
#     "#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
#     "#dcbeff", "#9A6324", "#800000", "#aaffc3", "#808000",
# ]


# class ScheduleVisualizer:
#     """Visualize DatasetUpdateConfig schedules in a Jupyter notebook.

#     Usage:
#         from loci.sources import update_configs
#         from loci.db.af_utils import get_postgres_engine

#         engine = get_postgres_engine(conn_id="gis_dwh_db")
#         viz = ScheduleVisualizer.from_module(update_configs, engine=engine)

#         viz.plot_incremental()                     # dots only, color-coded by source
#         viz.plot_incremental(source="socrata")     # numbered markers + printed legend
#         viz.plot_gantt_incremental(source="socrata", alpha=0.6)
#     """

#     def __init__(self, configs: list, engine=None) -> None:
#         self.configs = configs
#         self.engine = engine
#         self._source_color_map: dict[str, str] = {}
#         self._runtimes: dict[str, dict[str, float]] = {}
#         self._assign_colors()
#         if self.engine is not None:
#             self._load_runtimes()

#     @classmethod
#     def from_module(cls, module: types.ModuleType, engine=None) -> "ScheduleVisualizer":
#         """Auto-discover all DatasetUpdateConfig instances in a module."""
#         from loci.sources.update_configs import DatasetUpdateConfig

#         configs = [
#             obj
#             for name, obj in inspect.getmembers(module)
#             if isinstance(obj, DatasetUpdateConfig)
#         ]
#         return cls(configs, engine=engine)

#     def _assign_colors(self) -> None:
#         sources = sorted(set(cfg.spec.source for cfg in self.configs))
#         for i, source in enumerate(sources):
#             self._source_color_map[source] = SOURCE_COLORS[i % len(SOURCE_COLORS)]

#     @property
#     def sources(self) -> list[str]:
#         return sorted(self._source_color_map.keys())

#     def _filter_configs(self, source: Optional[str] = None) -> list:
#         if source is None:
#             return self.configs
#         return [cfg for cfg in self.configs if cfg.spec.source == source]

#     # ── Runtime loading ──────────────────────────────────────────────────

#     def _load_runtimes(self) -> None:
#         """Query meta.ingest_log for average runtimes per dataset and mode."""
#         df = self.engine.query("""
#             select
#                 dataset_id,
#                 case
#                     when metadata->>'mode' = 'incremental' then 'incremental'
#                     else 'full_refresh'
#                 end as mode_group,
#                 avg(extract(epoch from (completed_at - started_at)) / 60.0) as avg_minutes
#             from meta.ingest_log
#             where status = 'success'
#             group by dataset_id, mode_group
#         """)
#         self._runtimes = {}
#         for _, row in df.iterrows():
#             did = row["dataset_id"]
#             if did not in self._runtimes:
#                 self._runtimes[did] = {}
#             self._runtimes[did][row["mode_group"]] = float(row["avg_minutes"])

#     def _get_runtime_minutes(self, dataset_id: str, mode: str) -> float:
#         """Get average runtime in minutes. Returns a small default if unknown."""
#         return self._runtimes.get(dataset_id, {}).get(mode, 5.0)

#     # ── Labeling helpers ─────────────────────────────────────────────────

#     @staticmethod
#     def _build_number_map(configs: list) -> dict[str, int]:
#         """Assign a stable number to each dataset name, sorted alphabetically."""
#         names = sorted(set(cfg.spec.name for cfg in configs))
#         return {name: i + 1 for i, name in enumerate(names)}

#     @staticmethod
#     def _print_number_legend(number_map: dict[str, int]) -> None:
#         """Print the number-to-name mapping below the plot."""
#         print("\nDataset Legend:")
#         for name, num in sorted(number_map.items(), key=lambda x: x[1]):
#             print(f"  {num:>3}: {name}")

#     def _annotate_point(self, ax, x, y, cfg, number_map: dict[str, int]) -> None:
#         num = number_map[cfg.spec.name]
#         ax.annotate(
#             str(num), (x, y),
#             fontsize=6, fontweight="bold", ha="center", va="center",
#         )

#     def _annotate_bar(self, ax, x, y, cfg, number_map: dict[str, int]) -> None:
#         num = number_map[cfg.spec.name]
#         ax.text(x, y, f" {num}", fontsize=6, fontweight="bold", va="center")

#     def _finish_plot(
#         self, ax, configs, title, source, number_map: Optional[dict[str, int]]
#     ) -> None:
#         if source:
#             title += f" — {source}"
#         ax.set_title(title, fontsize=14, fontweight="bold")
#         self._add_source_legend(ax, configs)
#         plt.tight_layout()
#         plt.show()
#         if number_map is not None:
#             self._print_number_legend(number_map)

#     def _add_source_legend(self, ax, configs: list) -> None:
#         sources_in_plot = sorted(set(cfg.spec.source for cfg in configs))
#         patches = [
#             mpatches.Patch(color=self._source_color_map[s], label=s)
#             for s in sources_in_plot
#         ]
#         ax.legend(handles=patches, loc="upper right", fontsize=8)

#     # ── Scatter plots ────────────────────────────────────────────────────

#     def plot_incremental(self, source: Optional[str] = None, alpha: float = 0.8) -> None:
#         """Plot incremental update schedule on a weekly grid (days x hours).

#         When source is None: unlabeled dots, color-coded by source.
#         When source is specified: numbered markers with a printed legend.
#         """
#         configs = self._filter_configs(source)
#         if not configs:
#             print(f"No configs found for source: {source}")
#             return

#         number_map = self._build_number_map(configs) if source else None
#         fig, ax = plt.subplots(figsize=(14, 8))

#         for cfg in configs:
#             color = self._source_color_map[cfg.spec.source]
#             slots = self._parse_weekly_slots(cfg.update_cron)
#             for day_index, hour, minute in slots:
#                 time_val = hour + minute / 60
#                 ax.scatter(
#                     day_index, time_val,
#                     color=color, s=60, zorder=3, alpha=alpha,
#                     edgecolors="black", linewidths=0.5,
#                 )
#                 if number_map is not None:
#                     self._annotate_point(ax, day_index, time_val, cfg, number_map)

#         self._style_weekly_grid(ax)
#         self._finish_plot(ax, configs, "Incremental Update Schedule", source, number_map)

#     def plot_full_update(self, source: Optional[str] = None, alpha: float = 0.8) -> None:
#         """Plot full update schedule on a monthly grid (day-of-month x hours).

#         When source is None: unlabeled dots, color-coded by source.
#         When source is specified: numbered markers with a printed legend.
#         """
#         configs = self._filter_configs(source)
#         if not configs:
#             print(f"No configs found for source: {source}")
#             return

#         number_map = self._build_number_map(configs) if source else None
#         fig, ax = plt.subplots(figsize=(16, 8))

#         for cfg in configs:
#             color = self._source_color_map[cfg.spec.source]
#             day_mid = (cfg.full_update_week_of_month - 1) * 7 + 1 + cfg.full_update_day_of_week

#             slots = self._parse_weekly_slots(cfg.update_cron)
#             if slots:
#                 _, hour, minute = slots[0]
#             else:
#                 hour, minute = 0, 0

#             time_val = hour + minute / 60
#             ax.scatter(
#                 day_mid, time_val,
#                 color=color, s=80, zorder=3, alpha=alpha,
#                 edgecolors="black", linewidths=0.5,
#                 marker="D",
#             )
#             if number_map is not None:
#                 self._annotate_point(ax, day_mid, time_val, cfg, number_map)

#         self._style_monthly_grid(ax)
#         self._finish_plot(ax, configs, "Full Update Schedule", source, number_map)

#     # ── Gantt plots ──────────────────────────────────────────────────────

#     def plot_gantt_incremental(self, source: Optional[str] = None, alpha: float = 0.8) -> None:
#         """Gantt chart of incremental updates on a weekly grid.

#         Requires engine to have been passed to the constructor.
#         """
#         configs = self._filter_configs(source)
#         if not configs:
#             print(f"No configs found for source: {source}")
#             return
#         if not self._runtimes:
#             print("No runtimes available. Pass engine to the constructor.")
#             return

#         number_map = self._build_number_map(configs) if source else None
#         fig, ax = plt.subplots(figsize=(14, 8))

#         for cfg in configs:
#             color = self._source_color_map[cfg.spec.source]
#             duration_hours = self._get_runtime_minutes(cfg.spec.dataset_id, "incremental") / 60

#             slots = self._parse_weekly_slots(cfg.update_cron)
#             for day_index, hour, minute in slots:
#                 start_time = hour + minute / 60
#                 ax.barh(
#                     day_index, duration_hours, left=start_time, height=0.3,
#                     color=color, alpha=alpha, edgecolor="black", linewidth=0.5,
#                     zorder=3,
#                 )
#                 if number_map is not None:
#                     label_x = start_time + duration_hours + 0.05
#                     self._annotate_bar(ax, label_x, day_index, cfg, number_map)

#         self._style_gantt_weekly(ax)
#         self._finish_plot(ax, configs, "Incremental Update Gantt", source, number_map)

#     def plot_gantt_full_update(self, source: Optional[str] = None, alpha: float = 0.8) -> None:
#         """Gantt chart of full updates on a monthly grid.

#         Requires engine to have been passed to the constructor.
#         """
#         configs = self._filter_configs(source)
#         if not configs:
#             print(f"No configs found for source: {source}")
#             return
#         if not self._runtimes:
#             print("No runtimes available. Pass engine to the constructor.")
#             return

#         number_map = self._build_number_map(configs) if source else None
#         fig, ax = plt.subplots(figsize=(16, 8))

#         for cfg in configs:
#             color = self._source_color_map[cfg.spec.source]
#             duration_hours = self._get_runtime_minutes(cfg.spec.dataset_id, "full_refresh") / 60

#             day_mid = (cfg.full_update_week_of_month - 1) * 7 + 1 + cfg.full_update_day_of_week

#             slots = self._parse_weekly_slots(cfg.update_cron)
#             if slots:
#                 _, hour, minute = slots[0]
#             else:
#                 hour, minute = 0, 0

#             start_time = hour + minute / 60
#             ax.barh(
#                 day_mid, duration_hours, left=start_time, height=0.5,
#                 color=color, alpha=alpha, edgecolor="black", linewidth=0.5,
#                 zorder=3,
#             )
#             if number_map is not None:
#                 label_x = start_time + duration_hours + 0.05
#                 self._annotate_bar(ax, label_x, day_mid, cfg, number_map)

#         self._style_gantt_monthly(ax)
#         self._finish_plot(ax, configs, "Full Update Gantt", source, number_map)

#     # ── Parsing ──────────────────────────────────────────────────────────

#     @staticmethod
#     def _parse_weekly_slots(cron_expr: str) -> list[tuple[int, int, int]]:
#         """Parse a cron expression and return (day_index, hour, minute) tuples.

#         day_index: 0=Monday .. 6=Sunday
#         """
#         parts = cron_expr.split()
#         if len(parts) != 5:
#             return []

#         minute_str, hour_str, _, _, dow_str = parts

#         minutes = _expand_cron_field(minute_str, 0, 59)
#         hours = _expand_cron_field(hour_str, 0, 23)
#         dows = _expand_cron_field(dow_str, 0, 6)

#         # Cron uses 0=Sunday, convert to 0=Monday
#         slots = []
#         for dow in dows:
#             day_index = (dow - 1) % 7  # cron 1=Mon->0, 0=Sun->6
#             for h in hours:
#                 for m in minutes:
#                     slots.append((day_index, h, m))
#         return slots

#     # ── Grid styling ─────────────────────────────────────────────────────

#     @staticmethod
#     def _style_weekly_grid(ax) -> None:
#         ax.set_xlim(-0.5, 6.5)
#         ax.set_xticks(range(7))
#         ax.set_xticklabels(DAYS_OF_WEEK, fontsize=9)
#         ax.set_ylim(-0.5, 24)
#         ax.set_yticks(range(0, 25))
#         ax.set_yticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8)
#         ax.set_ylabel("Time of Day", fontsize=11)
#         ax.set_xlabel("Day of Week", fontsize=11)
#         ax.invert_yaxis()
#         ax.grid(True, alpha=0.3)

#     @staticmethod
#     def _style_monthly_grid(ax) -> None:
#         ax.set_xlim(0.5, 31.5)
#         ax.set_xticks(range(1, 32))
#         ax.set_xlabel("Day of Month", fontsize=11)
#         ax.set_ylim(-0.5, 24)
#         ax.set_yticks(range(0, 25))
#         ax.set_yticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8)
#         ax.set_ylabel("Time of Day", fontsize=11)
#         ax.invert_yaxis()
#         ax.grid(True, alpha=0.3)

#     @staticmethod
#     def _style_gantt_weekly(ax) -> None:
#         ax.set_ylim(-0.5, 6.5)
#         ax.set_yticks(range(7))
#         ax.set_yticklabels(DAYS_OF_WEEK, fontsize=9)
#         ax.set_xlim(0, 24)
#         ax.set_xticks(range(0, 25))
#         ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8, rotation=45)
#         ax.set_xlabel("Time of Day", fontsize=11)
#         ax.set_ylabel("Day of Week", fontsize=11)
#         ax.invert_yaxis()
#         ax.grid(True, axis="x", alpha=0.3)

#     @staticmethod
#     def _style_gantt_monthly(ax) -> None:
#         ax.set_ylim(0.5, 31.5)
#         ax.set_yticks(range(1, 32))
#         ax.set_ylabel("Day of Month", fontsize=11)
#         ax.set_xlim(0, 24)
#         ax.set_xticks(range(0, 25))
#         ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8, rotation=45)
#         ax.set_xlabel("Time of Day", fontsize=11)
#         ax.invert_yaxis()
#         ax.grid(True, axis="x", alpha=0.3)


# def _expand_cron_field(field: str, min_val: int, max_val: int) -> list[int]:
#     """Expand a single cron field into a list of integer values."""
#     if field == "*":
#         return list(range(min_val, max_val + 1))

#     values = set()
#     for part in field.split(","):
#         if "/" in part:
#             range_part, step = part.split("/")
#             step = int(step)
#             if range_part == "*":
#                 start, end = min_val, max_val
#             elif "-" in range_part:
#                 start, end = map(int, range_part.split("-"))
#             else:
#                 start, end = int(range_part), max_val
#             values.update(range(start, end + 1, step))
#         elif "-" in part:
#             start, end = map(int, part.split("-"))
#             values.update(range(start, end + 1))
#         else:
#             values.add(int(part))
#     return sorted(values)

# # from __future__ import annotations

# # import inspect
# # import types
# # from typing import Optional

# # import matplotlib.pyplot as plt
# # import matplotlib.patches as mpatches
# # import pandas as pd

# # from loci.db.core import PostgresEngine


# # DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# # SOURCE_COLORS = [
# #     "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
# #     "#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
# #     "#dcbeff", "#9A6324", "#800000", "#aaffc3", "#808000",
# # ]


# # class ScheduleVisualizer:
# #     """Visualize DatasetUpdateConfig schedules in a Jupyter notebook.

# #     Usage:
# #         from loci.sources import update_configs
# #         viz = ScheduleVisualizer.from_module(update_configs)
# #         viz.plot_incremental()                # numbered markers, all sources
# #         viz.plot_incremental(source="socrata") # named labels, one source
# #         viz.plot_full_update()

# #         # For gantt charts, pass a sqlalchemy engine:
# #         viz.load_runtimes(engine)
# #         viz.plot_gantt_incremental()
# #         viz.plot_gantt_full_update(source="socrata")
# #     """

# #     def __init__(self, configs: list) -> None:
# #         self.configs = configs
# #         self._source_color_map: dict[str, str] = {}
# #         self._runtimes: dict[str, dict[str, float]] = {}
# #         self._assign_colors()

# #     @classmethod
# #     def from_module(cls, module: types.ModuleType) -> "ScheduleVisualizer":
# #         """Auto-discover all DatasetUpdateConfig instances in a module."""
# #         from loci.sources.update_configs import DatasetUpdateConfig

# #         configs = [
# #             obj
# #             for name, obj in inspect.getmembers(module)
# #             if isinstance(obj, DatasetUpdateConfig)
# #         ]
# #         return cls(configs)

# #     def _assign_colors(self) -> None:
# #         sources = sorted(set(cfg.spec.source for cfg in self.configs))
# #         for i, source in enumerate(sources):
# #             self._source_color_map[source] = SOURCE_COLORS[i % len(SOURCE_COLORS)]

# #     @property
# #     def sources(self) -> list[str]:
# #         return sorted(self._source_color_map.keys())

# #     def _filter_configs(self, source: Optional[str] = None) -> list:
# #         if source is None:
# #             return self.configs
# #         return [cfg for cfg in self.configs if cfg.spec.source == source]

# #     # ── Runtime loading ──────────────────────────────────────────────────

# #     def load_runtimes(self, engine: PostgresEngine) -> None:
# #         """Query meta.ingest_log for average runtimes per dataset and mode."""
# #         query = """
# #             select
# #                 dataset_id,
# #                 case
# #                     when metadata->>'mode' = 'incremental' then 'incremental'
# #                     else 'full_refresh'
# #                 end as mode_group,
# #                 avg(extract(epoch from (completed_at - started_at)) / 60.0) as avg_minutes
# #             from meta.ingest_log
# #             where status = 'success'
# #             group by dataset_id, mode_group
# #         """
# #         df = engine.query(query)
# #         self._runtimes = {}
# #         for _, row in df.iterrows():
# #             did = row["dataset_id"]
# #             if did not in self._runtimes:
# #                 self._runtimes[did] = {}
# #             self._runtimes[did][row["mode_group"]] = float(row["avg_minutes"])

# #     def _get_runtime_minutes(self, dataset_id: str, mode: str) -> float:
# #         """Get average runtime in minutes. Returns a small default if unknown."""
# #         return self._runtimes.get(dataset_id, {}).get(mode, 5.0)

# #     # ── Labeling helpers ─────────────────────────────────────────────────

# #     @staticmethod
# #     def _build_number_map(configs: list) -> dict[str, int]:
# #         """Assign a stable number to each dataset name, sorted alphabetically."""
# #         names = sorted(set(cfg.spec.name for cfg in configs))
# #         return {name: i + 1 for i, name in enumerate(names)}

# #     @staticmethod
# #     def _print_number_legend(number_map: dict[str, int]) -> None:
# #         """Print the number-to-name mapping below the plot."""
# #         print("\nDataset Legend:")
# #         for name, num in sorted(number_map.items(), key=lambda x: x[1]):
# #             print(f"  {num:>3}: {name}")

# #     def _annotate_point(
# #         self, ax, x, y, cfg, number_map: Optional[dict[str, int]], source_filter: bool
# #     ) -> None:
# #         """Add a label to a point: dataset name if filtered, number if not."""
# #         if source_filter:
# #             ax.annotate(
# #                 cfg.spec.name,
# #                 (x, y),
# #                 fontsize=6, rotation=30,
# #                 xytext=(4, 4), textcoords="offset points",
# #             )
# #         else:
# #             num = number_map[cfg.spec.name]
# #             ax.annotate(
# #                 str(num),
# #                 (x, y),
# #                 fontsize=6, fontweight="bold",
# #                 ha="center", va="center",
# #             )

# #     def _annotate_bar(
# #         self, ax, x, y, cfg, number_map: Optional[dict[str, int]], source_filter: bool
# #     ) -> None:
# #         """Add a label to a bar: dataset name if filtered, number if not."""
# #         if source_filter:
# #             ax.text(x, y, f" {cfg.spec.name}", fontsize=6, va="center")
# #         else:
# #             num = number_map[cfg.spec.name]
# #             ax.text(x, y, f" {num}", fontsize=6, fontweight="bold", va="center")

# #     def _finish_plot(
# #         self, ax, configs, title, source, number_map: Optional[dict[str, int]]
# #     ) -> None:
# #         """Add title, legend, and show. Print number legend if applicable."""
# #         if source:
# #             title += f" — {source}"
# #         ax.set_title(title, fontsize=14, fontweight="bold")
# #         self._add_source_legend(ax, configs)
# #         plt.tight_layout()
# #         plt.show()
# #         if number_map and not source:
# #             self._print_number_legend(number_map)

# #     def _add_source_legend(self, ax, configs: list) -> None:
# #         sources_in_plot = sorted(set(cfg.spec.source for cfg in configs))
# #         patches = [
# #             mpatches.Patch(color=self._source_color_map[s], label=s)
# #             for s in sources_in_plot
# #         ]
# #         ax.legend(handles=patches, loc="upper right", fontsize=8)

# #     # ── Scatter plots ────────────────────────────────────────────────────

# #     def plot_incremental(self, source: Optional[str] = None) -> None:
# #         """Plot incremental update schedule on a weekly grid (days x hours).

# #         When source is None: numbered markers with a printed legend.
# #         When source is specified: dataset name labels.
# #         """
# #         configs = self._filter_configs(source)
# #         if not configs:
# #             print(f"No configs found for source: {source}")
# #             return

# #         number_map = None if source else self._build_number_map(configs)
# #         fig, ax = plt.subplots(figsize=(14, 8))

# #         for cfg in configs:
# #             color = self._source_color_map[cfg.spec.source]
# #             slots = self._parse_weekly_slots(cfg.update_cron)
# #             for day_index, hour, minute in slots:
# #                 time_val = hour + minute / 60
# #                 ax.scatter(
# #                     day_index, time_val,
# #                     color=color, s=60, zorder=3, alpha=0.8,
# #                     edgecolors="black", linewidths=0.5,
# #                 )
# #                 self._annotate_point(ax, day_index, time_val, cfg, number_map, bool(source))

# #         self._style_weekly_grid(ax)
# #         self._finish_plot(ax, configs, "Incremental Update Schedule", source, number_map)

# #     def plot_full_update(self, source: Optional[str] = None) -> None:
# #         """Plot full update schedule on a monthly grid (day-of-month x hours).

# #         When source is None: numbered markers with a printed legend.
# #         When source is specified: dataset name labels.
# #         """
# #         configs = self._filter_configs(source)
# #         if not configs:
# #             print(f"No configs found for source: {source}")
# #             return

# #         number_map = None if source else self._build_number_map(configs)
# #         fig, ax = plt.subplots(figsize=(16, 8))

# #         for cfg in configs:
# #             color = self._source_color_map[cfg.spec.source]
# #             day_mid = (cfg.full_update_week_of_month - 1) * 7 + 1 + cfg.full_update_day_of_week

# #             slots = self._parse_weekly_slots(cfg.update_cron)
# #             if slots:
# #                 _, hour, minute = slots[0]
# #             else:
# #                 hour, minute = 0, 0

# #             time_val = hour + minute / 60
# #             ax.scatter(
# #                 day_mid, time_val,
# #                 color=color, s=80, zorder=3, alpha=0.8,
# #                 edgecolors="black", linewidths=0.5,
# #                 marker="D",
# #             )
# #             self._annotate_point(ax, day_mid, time_val, cfg, number_map, bool(source))

# #         self._style_monthly_grid(ax)
# #         self._finish_plot(ax, configs, "Full Update Schedule", source, number_map)

# #     # ── Gantt plots ──────────────────────────────────────────────────────

# #     def plot_gantt_incremental(self, source: Optional[str] = None) -> None:
# #         """Gantt chart of incremental updates on a weekly grid.

# #         Requires load_runtimes() to have been called first.
# #         """
# #         configs = self._filter_configs(source)
# #         if not configs:
# #             print(f"No configs found for source: {source}")
# #             return
# #         if not self._runtimes:
# #             print("No runtimes loaded. Call load_runtimes(engine) first.")
# #             return

# #         number_map = None if source else self._build_number_map(configs)
# #         fig, ax = plt.subplots(figsize=(14, 8))

# #         for cfg in configs:
# #             color = self._source_color_map[cfg.spec.source]
# #             duration_hours = self._get_runtime_minutes(cfg.spec.dataset_id, "incremental") / 60

# #             slots = self._parse_weekly_slots(cfg.update_cron)
# #             for day_index, hour, minute in slots:
# #                 start_time = hour + minute / 60
# #                 ax.barh(
# #                     day_index, duration_hours, left=start_time, height=0.3,
# #                     color=color, alpha=0.8, edgecolor="black", linewidth=0.5,
# #                     zorder=3,
# #                 )
# #                 label_x = start_time + duration_hours + 0.05
# #                 self._annotate_bar(ax, label_x, day_index, cfg, number_map, bool(source))

# #         self._style_gantt_weekly(ax)
# #         self._finish_plot(ax, configs, "Incremental Update Gantt", source, number_map)

# #     def plot_gantt_full_update(self, source: Optional[str] = None) -> None:
# #         """Gantt chart of full updates on a monthly grid.

# #         Requires load_runtimes() to have been called first.
# #         """
# #         configs = self._filter_configs(source)
# #         if not configs:
# #             print(f"No configs found for source: {source}")
# #             return
# #         if not self._runtimes:
# #             print("No runtimes loaded. Call load_runtimes(engine) first.")
# #             return

# #         number_map = None if source else self._build_number_map(configs)
# #         fig, ax = plt.subplots(figsize=(16, 8))

# #         for cfg in configs:
# #             color = self._source_color_map[cfg.spec.source]
# #             duration_hours = self._get_runtime_minutes(cfg.spec.dataset_id, "full_refresh") / 60

# #             day_mid = (cfg.full_update_week_of_month - 1) * 7 + 1 + cfg.full_update_day_of_week

# #             slots = self._parse_weekly_slots(cfg.update_cron)
# #             if slots:
# #                 _, hour, minute = slots[0]
# #             else:
# #                 hour, minute = 0, 0

# #             start_time = hour + minute / 60
# #             ax.barh(
# #                 day_mid, duration_hours, left=start_time, height=0.5,
# #                 color=color, alpha=0.8, edgecolor="black", linewidth=0.5,
# #                 zorder=3,
# #             )
# #             label_x = start_time + duration_hours + 0.05
# #             self._annotate_bar(ax, label_x, day_mid, cfg, number_map, bool(source))

# #         self._style_gantt_monthly(ax)
# #         self._finish_plot(ax, configs, "Full Update Gantt", source, number_map)

# #     # ── Parsing ──────────────────────────────────────────────────────────

# #     @staticmethod
# #     def _parse_weekly_slots(cron_expr: str) -> list[tuple[int, int, int]]:
# #         """Parse a cron expression and return (day_index, hour, minute) tuples.

# #         day_index: 0=Monday .. 6=Sunday
# #         """
# #         parts = cron_expr.split()
# #         if len(parts) != 5:
# #             return []

# #         minute_str, hour_str, _, _, dow_str = parts

# #         minutes = _expand_cron_field(minute_str, 0, 59)
# #         hours = _expand_cron_field(hour_str, 0, 23)
# #         dows = _expand_cron_field(dow_str, 0, 6)

# #         # Cron uses 0=Sunday, convert to 0=Monday
# #         slots = []
# #         for dow in dows:
# #             day_index = (dow - 1) % 7  # cron 1=Mon->0, 0=Sun->6
# #             for h in hours:
# #                 for m in minutes:
# #                     slots.append((day_index, h, m))
# #         return slots

# #     # ── Grid styling ─────────────────────────────────────────────────────

# #     @staticmethod
# #     def _style_weekly_grid(ax) -> None:
# #         ax.set_xlim(-0.5, 6.5)
# #         ax.set_xticks(range(7))
# #         ax.set_xticklabels(DAYS_OF_WEEK, fontsize=9)
# #         ax.set_ylim(-0.5, 24)
# #         ax.set_yticks(range(0, 25))
# #         ax.set_yticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8)
# #         ax.set_ylabel("Time of Day", fontsize=11)
# #         ax.set_xlabel("Day of Week", fontsize=11)
# #         ax.invert_yaxis()
# #         ax.grid(True, alpha=0.3)

# #     @staticmethod
# #     def _style_monthly_grid(ax) -> None:
# #         ax.set_xlim(0.5, 31.5)
# #         ax.set_xticks(range(1, 32))
# #         ax.set_xlabel("Day of Month", fontsize=11)
# #         ax.set_ylim(-0.5, 24)
# #         ax.set_yticks(range(0, 25))
# #         ax.set_yticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8)
# #         ax.set_ylabel("Time of Day", fontsize=11)
# #         ax.invert_yaxis()
# #         ax.grid(True, alpha=0.3)

# #     @staticmethod
# #     def _style_gantt_weekly(ax) -> None:
# #         ax.set_ylim(-0.5, 6.5)
# #         ax.set_yticks(range(7))
# #         ax.set_yticklabels(DAYS_OF_WEEK, fontsize=9)
# #         ax.set_xlim(0, 24)
# #         ax.set_xticks(range(0, 25))
# #         ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8, rotation=45)
# #         ax.set_xlabel("Time of Day", fontsize=11)
# #         ax.set_ylabel("Day of Week", fontsize=11)
# #         ax.invert_yaxis()
# #         ax.grid(True, axis="x", alpha=0.3)

# #     @staticmethod
# #     def _style_gantt_monthly(ax) -> None:
# #         ax.set_ylim(0.5, 31.5)
# #         ax.set_yticks(range(1, 32))
# #         ax.set_ylabel("Day of Month", fontsize=11)
# #         ax.set_xlim(0, 24)
# #         ax.set_xticks(range(0, 25))
# #         ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25)], fontsize=8, rotation=45)
# #         ax.set_xlabel("Time of Day", fontsize=11)
# #         ax.invert_yaxis()
# #         ax.grid(True, axis="x", alpha=0.3)


# # def _expand_cron_field(field: str, min_val: int, max_val: int) -> list[int]:
# #     """Expand a single cron field into a list of integer values."""
# #     if field == "*":
# #         return list(range(min_val, max_val + 1))

# #     values = set()
# #     for part in field.split(","):
# #         if "/" in part:
# #             range_part, step = part.split("/")
# #             step = int(step)
# #             if range_part == "*":
# #                 start, end = min_val, max_val
# #             elif "-" in range_part:
# #                 start, end = map(int, range_part.split("-"))
# #             else:
# #                 start, end = int(range_part), max_val
# #             values.update(range(start, end + 1, step))
# #         elif "-" in part:
# #             start, end = map(int, part.split("-"))
# #             values.update(range(start, end + 1))
# #         else:
# #             values.add(int(part))
# #     return sorted(values)
