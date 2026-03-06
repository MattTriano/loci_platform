"""Tests for BikeIndexCollector."""

import json
from unittest.mock import MagicMock

from loci.collectors.bike_index.client import BikeIndexSearchParams
from loci.collectors.bike_index.collector import BikeIndexCollector
from loci.collectors.bike_index.spec import BikeIndexDatasetSpec

from .conftest import make_detail_bike, make_search_bike

# ================================================================== #
#  Flattening — search
# ================================================================== #


class TestFlattenSearch:
    def test_unpacks_stolen_coordinates(self, collector):
        bike = make_search_bike(1, stolen_coordinates=[41.88, -87.63])
        row = collector._flatten_search(bike)
        assert row["stolen_coordinates_lat"] == 41.88
        assert row["stolen_coordinates_lon"] == -87.63

    def test_handles_missing_stolen_coordinates(self, collector):
        bike = make_search_bike(1, stolen_coordinates=None)
        row = collector._flatten_search(bike)
        assert row["stolen_coordinates_lat"] is None
        assert row["stolen_coordinates_lon"] is None

    def test_detail_fields_are_none(self, collector):
        row = collector._flatten_search(make_search_bike(1))
        assert row["latitude"] is None
        assert row["longitude"] is None
        assert row["theft_description"] is None
        assert row["components"] is None
        assert row["public_images"] is None
        assert row["frame_material_slug"] is None

    def test_frame_colors_serialized_as_json(self, collector):
        bike = make_search_bike(1, frame_colors=["Red", "Black"])
        row = collector._flatten_search(bike)
        assert row["frame_colors"] == '["Red", "Black"]'

    def test_summary_fields_populated(self, collector):
        bike = make_search_bike(42, date_stolen=1700000000)
        row = collector._flatten_search(bike)
        assert row["id"] == 42
        assert row["date_stolen"] == 1700000000
        assert row["stolen_location"] == "Chicago, IL"
        assert row["propulsion_type_slug"] == "foot-pedal"


# ================================================================== #
#  Flattening — detail
# ================================================================== #


class TestFlattenDetail:
    def test_stolen_record_fields_populated(self, collector):
        bike = make_detail_bike(1)
        row = collector._flatten_detail(bike)
        assert row["latitude"] == 41.89
        assert row["longitude"] == -87.62
        assert row["theft_description"] == "Locked outside"
        assert row["police_report_number"] == "CPD-12345"

    def test_detail_scalar_fields_populated(self, collector):
        bike = make_detail_bike(1)
        row = collector._flatten_detail(bike)
        assert row["frame_material_slug"] == "aluminum"
        assert row["manufacturer_id"] == 42
        assert row["frame_size"] == "56cm"
        assert row["registration_created_at"] == 1700000100

    def test_components_serialized_as_json(self, collector):
        bike = make_detail_bike(1)
        row = collector._flatten_detail(bike)
        parsed = json.loads(row["components"])
        assert parsed == [{"type": "wheel", "brand": "Shimano"}]

    def test_public_images_serialized_as_json(self, collector):
        bike = make_detail_bike(1)
        row = collector._flatten_detail(bike)
        assert json.loads(row["public_images"]) == []

    def test_stolen_coordinates_unpacked(self, collector):
        bike = make_detail_bike(1, stolen_coordinates=[41.88, -87.63])
        row = collector._flatten_detail(bike)
        assert row["stolen_coordinates_lat"] == 41.88
        assert row["stolen_coordinates_lon"] == -87.63

    def test_missing_stolen_record(self, collector):
        bike = make_detail_bike(1)
        bike["stolen_record"] = None
        row = collector._flatten_detail(bike)
        assert row["latitude"] is None
        assert row["longitude"] is None
        assert row["theft_description"] is None

    def test_search_and_detail_have_same_keys(self, collector):
        search_row = collector._flatten_search(make_search_bike(1))
        detail_row = collector._flatten_detail(make_detail_bike(1))
        assert set(search_row.keys()) == set(detail_row.keys())


# ================================================================== #
#  High water mark
# ================================================================== #


class TestHighWaterMark:
    def test_returns_max_date_stolen(self, collector, mock_engine):
        df = MagicMock()
        df.empty = False
        df.__getitem__ = lambda self, key: MagicMock(iloc=[1700000000])
        mock_engine.query.return_value = df

        hwm = collector._get_high_water_mark(
            BikeIndexDatasetSpec(name="t", target_table="t", target_schema="s")
        )
        assert hwm == 1700000000

    def test_returns_epoch_on_empty_table(self, collector, mock_engine):
        df = MagicMock()
        df.empty = False
        df.__getitem__ = lambda self, key: MagicMock(iloc=[None])
        mock_engine.query.return_value = df

        hwm = collector._get_high_water_mark(
            BikeIndexDatasetSpec(name="t", target_table="t", target_schema="s")
        )
        assert hwm == collector.EPOCH_HWM

    def test_returns_epoch_on_exception(self, collector, mock_engine):
        mock_engine.query.side_effect = Exception("table does not exist")

        hwm = collector._get_high_water_mark(
            BikeIndexDatasetSpec(name="t", target_table="t", target_schema="s")
        )
        assert hwm == collector.EPOCH_HWM


# ================================================================== #
#  collect_search
# ================================================================== #


class TestCollectSearch:
    def test_ingests_all_bikes(self, collector, mock_client, mock_stager, spec):
        bikes = [make_search_bike(i, date_stolen=1700000000 + i) for i in range(3)]
        mock_client.search_all.return_value = iter([bikes])

        result = collector.collect_search(spec, force=True, batch_size=50)

        assert result["total_rows_staged"] == 3
        assert result["rows_skipped"] == 0
        assert mock_stager.write_batch.call_count == 1

    def test_skips_bikes_at_or_before_hwm(self, collector, mock_client, mock_stager, spec):
        bikes = [
            make_search_bike(1, date_stolen=100),  # at hwm — skip
            make_search_bike(2, date_stolen=50),  # before hwm — skip
            make_search_bike(3, date_stolen=200),  # after hwm — keep
        ]
        mock_client.search_all.return_value = iter([bikes])

        result = collector.collect_search(spec, high_water_mark=100, batch_size=50)

        assert result["total_rows_staged"] == 1
        assert result["rows_skipped"] == 2

    def test_force_ignores_hwm(self, collector, mock_client, mock_stager, spec):
        bikes = [make_search_bike(1, date_stolen=50)]
        mock_client.search_all.return_value = iter([bikes])

        result = collector.collect_search(spec, force=True, batch_size=50)

        assert result["total_rows_staged"] == 1
        assert result["rows_skipped"] == 0

    def test_batching(self, collector, mock_client, mock_stager, spec):
        bikes = [make_search_bike(i, date_stolen=1700000000 + i) for i in range(5)]
        mock_client.search_all.return_value = iter([bikes])

        collector.collect_search(spec, force=True, batch_size=2)

        # 5 bikes at batch_size=2 → 2, 2, 1
        assert mock_stager.write_batch.call_count == 3

    def test_passes_search_params_to_client(self, collector, mock_client, mock_stager, spec):
        mock_client.search_all.return_value = iter([])

        collector.collect_search(spec, force=True)

        search_arg = mock_client.search_all.call_args[0][0]
        assert isinstance(search_arg, BikeIndexSearchParams)
        assert search_arg.location == "Chicago, IL"
        assert search_arg.distance == 10

    def test_staged_ingest_called_with_spec_params(self, collector, mock_client, mock_engine, spec):
        mock_client.search_all.return_value = iter([])

        collector.collect_search(spec, force=True)

        mock_engine.staged_ingest.assert_called_once_with(
            target_table="stolen_bikes",
            target_schema="raw_data",
            entity_key=["id"],
            metadata_columns=collector.METADATA_COLUMNS,
        )


# ================================================================== #
#  collect_detail
# ================================================================== #


class TestCollectDetail:
    def _setup_ids_query(self, mock_engine, ids):
        """Configure the engine to return a list of bike ids."""
        df = MagicMock()
        df.__getitem__ = lambda self, key: MagicMock(tolist=MagicMock(return_value=ids))
        mock_engine.query.return_value = df

    def test_fetches_and_ingests_detail(
        self,
        collector,
        mock_client,
        mock_engine,
        mock_stager,
        spec,
    ):
        self._setup_ids_query(mock_engine, [10, 20])
        mock_client.get_bike.side_effect = [
            make_detail_bike(10),
            make_detail_bike(20),
        ]

        result = collector.collect_detail(spec, force=True, batch_size=50)

        assert result["total_rows_staged"] == 2
        assert result["errors"] == []
        assert mock_client.get_bike.call_count == 2

    def test_records_errors_on_failed_detail_fetch(
        self,
        collector,
        mock_client,
        mock_engine,
        mock_stager,
        spec,
    ):
        self._setup_ids_query(mock_engine, [10, 20])
        mock_client.get_bike.side_effect = [
            Exception("timeout"),
            make_detail_bike(20),
        ]

        result = collector.collect_detail(spec, force=True, batch_size=50)

        assert result["total_rows_staged"] == 1
        assert len(result["errors"]) == 1
        assert result["errors"][0]["bike_id"] == 10

    def test_returns_early_when_no_ids(
        self,
        collector,
        mock_client,
        mock_engine,
        mock_stager,
        spec,
    ):
        self._setup_ids_query(mock_engine, [])

        result = collector.collect_detail(spec, force=True)

        assert result["total_rows_staged"] == 0
        mock_client.get_bike.assert_not_called()

    def test_batching(self, collector, mock_client, mock_engine, mock_stager, spec):
        self._setup_ids_query(mock_engine, [1, 2, 3, 4, 5])
        mock_client.get_bike.side_effect = [make_detail_bike(i) for i in range(1, 6)]

        collector.collect_detail(spec, force=True, batch_size=2)

        # 5 bikes at batch_size=2 → 2, 2, 1
        assert mock_stager.write_batch.call_count == 3

    def test_uses_hwm_param(self, collector, mock_client, mock_engine, mock_stager, spec):
        self._setup_ids_query(mock_engine, [])

        collector.collect_detail(spec, high_water_mark=999)

        # Check the query used hwm=999
        query_call = mock_engine.query.call_args
        assert query_call[0][1]["hwm"] == 999


# ================================================================== #
#  collect (both phases)
# ================================================================== #


class TestCollect:
    def test_captures_hwm_before_search_phase(
        self,
        collector,
        mock_client,
        mock_engine,
        mock_stager,
        spec,
    ):
        """The HWM should be captured once before collect_search runs,
        then passed to both phases."""
        # First call is _get_high_water_mark query → returns 500
        hwm_df = MagicMock()
        hwm_df.empty = False
        hwm_df.__getitem__ = lambda self, key: MagicMock(iloc=[500])

        # Second call is _get_ids_needing_detail query → returns []
        ids_df = MagicMock()
        ids_df.__getitem__ = lambda self, key: MagicMock(tolist=MagicMock(return_value=[]))

        mock_engine.query.side_effect = [hwm_df, ids_df]
        mock_client.search_all.return_value = iter([])

        result = collector.collect(spec)

        assert result["search"]["high_water_mark"] == 500
        assert result["detail"]["high_water_mark"] == 500

    def test_force_uses_epoch_hwm(
        self,
        collector,
        mock_client,
        mock_engine,
        mock_stager,
        spec,
    ):
        ids_df = MagicMock()
        ids_df.__getitem__ = lambda self, key: MagicMock(tolist=MagicMock(return_value=[]))
        mock_engine.query.return_value = ids_df
        mock_client.search_all.return_value = iter([])

        result = collector.collect(spec, force=True)

        assert result["search"]["high_water_mark"] == collector.EPOCH_HWM
        assert result["detail"]["high_water_mark"] == collector.EPOCH_HWM
        # _get_high_water_mark should NOT have been called
        # The only query call should be _get_ids_needing_detail
        assert mock_engine.query.call_count == 1


# ================================================================== #
#  generate_ddl
# ================================================================== #


class TestGenerateDDL:
    def test_creates_valid_sql(self, collector, spec):
        ddl = collector.generate_ddl(spec)
        assert ddl.startswith("create table raw_data.stolen_bikes (")
        assert '"id" integer not null' in ddl
        assert '"frame_colors" jsonb' in ddl
        assert '"components" jsonb' in ddl
        assert '"stolen_coordinates_lat" double precision' in ddl
        assert '"latitude" double precision' in ddl
        assert "uq_stolen_bikes_entity_hash" in ddl
        assert "ix_stolen_bikes_current" in ddl
        assert '"valid_to" is null' in ddl

    def test_uses_entity_key_in_constraints(self, collector):
        spec = BikeIndexDatasetSpec(
            name="t",
            target_table="t",
            entity_key=["id", "date_stolen"],
        )
        ddl = collector.generate_ddl(spec)
        assert '"id", "date_stolen", "record_hash"' in ddl


# ================================================================== #
#  Tracker integration
# ================================================================== #


class TestTrackerIntegration:
    def test_search_calls_tracker(self, mock_client, mock_engine, mock_stager, spec):
        tracker = MagicMock()
        run = MagicMock()
        tracker.track.return_value.__enter__ = MagicMock(return_value=run)
        tracker.track.return_value.__exit__ = MagicMock(return_value=False)

        collector = BikeIndexCollector(
            client=mock_client,
            engine=mock_engine,
            tracker=tracker,
        )
        mock_client.search_all.return_value = iter(
            [
                [make_search_bike(1, date_stolen=1700000000)],
            ]
        )

        collector.collect_search(spec, force=True)

        tracker.track.assert_called_once()
        call_kwargs = tracker.track.call_args[1]
        assert call_kwargs["source"] == "bike_index_api"
        assert call_kwargs["metadata"]["phase"] == "search"
        assert run.rows_staged == 1

    def test_detail_calls_tracker(self, mock_client, mock_engine, mock_stager, spec):
        tracker = MagicMock()
        run = MagicMock()
        tracker.track.return_value.__enter__ = MagicMock(return_value=run)
        tracker.track.return_value.__exit__ = MagicMock(return_value=False)

        collector = BikeIndexCollector(
            client=mock_client,
            engine=mock_engine,
            tracker=tracker,
        )

        ids_df = MagicMock()
        ids_df.__getitem__ = lambda self, key: MagicMock(tolist=MagicMock(return_value=[10]))
        mock_engine.query.return_value = ids_df
        mock_client.get_bike.return_value = make_detail_bike(10)

        collector.collect_detail(spec, force=True)

        tracker.track.assert_called_once()
        call_kwargs = tracker.track.call_args[1]
        assert call_kwargs["metadata"]["phase"] == "detail"
        assert run.rows_staged == 1
