from loci.exports.bike_map_layers import LayerDisplayConfig, PopupField
from loci.exports.geojson_export import GeoJSONExportConfig

BIKEINDEX_BIKE_THEFTS_LAYER_CONFIG = LayerDisplayConfig(
    export_name="thefts",
    label="Thefts",
    color="#f59e0b",
    popup_title="Thefts",
    date_field="theft_date",
    popup_fields=[
        PopupField(key="source", label="Source"),
        PopupField(key="theft_date", label="Theft Date"),
        PopupField(key="theft_year", label="Theft Year"),
        PopupField(key="theft_hour", label="Theft Hour", fmt="hour"),
        PopupField(key="bike_title", label="Bike Title"),
        PopupField(key="bike_description", label="Bike Description"),
        PopupField(key="theft_description", label="Theft Description"),
        PopupField(key="locking_description", label="Locking Description"),
        PopupField(key="lock_defeat_description", label="Lock Defeat Description"),
        PopupField(key="theft_status", label="Theft Status"),
    ],
)

OSM_BIKE_PARKING_LAYER_CONFIG = LayerDisplayConfig(
    export_name="parking",
    label="Parking",
    color="#22c55e",
    popup_title="Parking",
    popup_fields=[
        PopupField(key="type", label="Type"),
        PopupField(key="capacity", label="Capacity"),
        PopupField(key="covered", label="Covered", fmt="yes_no_bool"),
        PopupField(key="indoor", label="Indoor", fmt="yes_no_bool"),
        PopupField(key="lit", label="Lit", fmt="yes_no_bool"),
        PopupField(key="fee", label="Fee", fmt="yes_no_bool"),
        PopupField(key="operator", label="Operator"),
        PopupField(key="access", label="Access"),
    ],
)


def bikeindex_bike_theft_geojson_config_factory(city: str) -> GeoJSONExportConfig:
    return GeoJSONExportConfig(
        name="thefts",
        table=f"{city}_bikeindex_bike_thefts",
        latitude_column="latitude",
        longitude_column="longitude",
        properties=[
            "source",
            "source_id",
            "theft_date",
            "theft_year",
            "theft_hour",
            "bike_title",
            "bike_description",
            "theft_description",
            "locking_description",
            "lock_defeat_description",
            "theft_status",
            "latitude",
            "longitude",
            "location",
        ],
    )


def osm_bike_parking_geojson_config_factory(city: str) -> GeoJSONExportConfig:
    return GeoJSONExportConfig(
        name="parking",
        table=f"{city}_osm_bike_parking",
        geometry_column="geom",
        properties=[
            "osm_id",
            "type",
            "capacity",
            "covered",
            "indoor",
            "fee",
            "lit",
            "operator",
            "access",
        ],
    )
