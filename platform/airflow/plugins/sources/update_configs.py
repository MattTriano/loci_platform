from dataclasses import dataclass, field

from sources.dataset_specs import ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_SPEC


@dataclass
class DatasetUpdateConfig:
    dataset_id: str
    dataset_name: str
    full_update_cron: str
    update_cron: str
    entity_key: list[str] = field(default_factory=list)
    full_update_mode: str = "api"  # "api" or "file_download"


ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_UPDATE_CONFIG = DatasetUpdateConfig(
    dataset_id=ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_SPEC.target_table,
    dataset_name=ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_SPEC.target_table,
    full_update_cron="0 10 21-28 3,6,9,12 3",
    update_cron="0 10 21-28 * 3",
    entity_key=["state", "county", "tract", "vintage"],
    full_update_mode="api",
)

CHICAGO_BUILDING_PERMITS = DatasetUpdateConfig(
    dataset_id="ydr8-5enu",
    dataset_name="chicago_building_permits",
    full_update_cron="0 6 1-7 * 2",
    update_cron="0 6 * * 2,5",
    entity_key=["permit_"],
    full_update_mode="api",
)

CHICAGO_FOOD_INSPECTIONS = DatasetUpdateConfig(
    dataset_id="4ijn-s7e5",
    dataset_name="chicago_food_inspections",
    full_update_cron="0 5 1-7 * 0",
    update_cron="0 5 * * *",
    entity_key=["inspection_id"],
    full_update_mode="api",
)

CHICAGO_SIDEWALK_CAFE_PERMITS = DatasetUpdateConfig(
    dataset_id="nxj5-ix6z",
    dataset_name="chicago_sidewalk_cafe_permits",
    full_update_cron="5 5 1-7 * 0",
    update_cron="5 5 * * *",
    entity_key=["permit_number"],
    full_update_mode="api",
)

CHICAGO_SPEED_CAMERA_VIOLATION_CONFIG = DatasetUpdateConfig(
    dataset_id="hhkd-xvj4",
    dataset_name="chicago_speed_camera_violations",
    full_update_cron="10 4 1-7 * 0",
    update_cron="5 4 * * 1,4",
)

CHICAGO_DIVVY_BICYCLE_STATIONS = DatasetUpdateConfig(
    dataset_id="bbyy-e7gq",
    dataset_name="chicago_divvy_bicycle_stations",
    full_update_cron="15 4 1-7 * 0",
    update_cron="15 4 * * 1,4",
)

CHICAGO_RED_LIGHT_CAMERA_VIOLATION_CONFIG = DatasetUpdateConfig(
    dataset_id="spqx-js37",
    dataset_name="chicago_red_light_camera_violations",
    full_update_cron="20 4 1-7 * 0",
    update_cron="20 4 * * 1,4",
)

CHICAGO_311_SERVICE_REQUESTS = DatasetUpdateConfig(
    dataset_id="v6vf-nfxy",
    dataset_name="chicago_311_service_requests",
    full_update_cron="0 0 1-7 1,4,7,10 0",
    update_cron="30 4 * * 1,4",
    entity_key=["sr_number"],
    full_update_mode="file_download",
)

CHICAGO_TOWED_VEHICLES = DatasetUpdateConfig(
    dataset_id="ygr5-vcbg",
    dataset_name="chicago_towed_vehicles",
    full_update_cron="0 4 1-7 * 0",
    update_cron="0 4 * * 1,4",
)

CHICAGO_TRAFFIC_CRASHES_CRASHES = DatasetUpdateConfig(
    dataset_id="85ca-t3if",
    dataset_name="chicago_traffic_crashes_crashes",
    full_update_cron="10 3 1-7 * 0",
    update_cron="10 3 * * 1,4",
    entity_key=["crash_record_id"],
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_PEOPLE = DatasetUpdateConfig(
    dataset_id="u6pd-qa9d",
    dataset_name="chicago_traffic_crashes_people",
    full_update_cron="40 2 1-7 * 0",
    update_cron="40 2 * * 1,4",
    entity_key=["person_id"],
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_VEHICLES = DatasetUpdateConfig(
    dataset_id="68nd-jvt3",
    dataset_name="chicago_traffic_crashes_vehicles",
    full_update_cron="50 2 1-7 * 0",
    update_cron="50 2 * * 1,4",
    entity_key=["crash_unit_id"],
    full_update_mode="api",
)

CTA_RIDERSHIP_DAILY_BOARDING_TOTALS = DatasetUpdateConfig(
    dataset_id="6iiy-9s97",
    dataset_name="cta_ridership_daily_boarding_totals",
    full_update_cron="30 22 1-7 * 0",
    update_cron="30 22 * * *",
)

CHICAGO_LENDING_EQUITY_RESIDENTIAL_LENDING = DatasetUpdateConfig(
    dataset_id="b77m-uuhb",
    dataset_name="chicago_lending_equity_residential_lending",
    full_update_cron="40 4 1-7 * 0",
    update_cron="40 4 * * *",
)

CHICAGO_ADDITIONAL_DWELLING_UNIT_PREAPPROVAL_APPLICATIONS = DatasetUpdateConfig(
    dataset_id="xbwc-ntpx",
    dataset_name="chicago_additional_dwelling_unit_preapproval_applications",
    full_update_cron="50 4 1-7 * 0",
    update_cron="50 4 * * *",
    entity_key=["id"],
    full_update_mode="api",
)

COOK_COUNTY_RESIDENTIAL_CONDOMINIUM_UNIT_CHARACTERISTICS = DatasetUpdateConfig(
    dataset_id="3r7i-mrz4",
    dataset_name="cook_county_residential_condominium_unit_characteristics",
    full_update_cron="10 5 1-7 * 6",
    update_cron="10 5 * * *",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

COOK_COUNTY_SINGLE_AND_MULTI_FAMILY_IMPROVEMENT_CHARACTERISTICS = DatasetUpdateConfig(
    dataset_id="x54s-btds",
    dataset_name="cook_county_single_and_multi_family_improvement_characteristics",
    full_update_cron="20 5 1-7 * 6",
    update_cron="20 5 * * *",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

COOK_COUNTY_COMMERCIAL_VALUATION_DATA = DatasetUpdateConfig(
    dataset_id="csik-bsws",
    dataset_name="cook_county_commercial_valuation_data",
    full_update_cron="30 5 1-7 * 6",
    update_cron="30 5 * * *",
)

COOK_COUNTY_PARCEL_SALES = DatasetUpdateConfig(
    dataset_id="wvhk-k5uv",
    dataset_name="cook_county_parcel_sales",
    full_update_cron="40 5 1-7 * 0",
    update_cron="40 5 * * *",
    entity_key=["row_id"],
    full_update_mode="api",
)

COOK_COUNTY_ASSESSED_PARCEL_VALUES = DatasetUpdateConfig(
    dataset_id="uzyt-m557",
    dataset_name="cook_county_assessed_parcel_values",
    full_update_cron="0 2 1-7 * 1",
    update_cron="30 1 * * 1,4",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES = DatasetUpdateConfig(
    dataset_id="pcdw-pxtg",
    dataset_name="cook_county_neighborhood_boundaries",
    full_update_cron="50 5 1-7 * 0",
    update_cron="50 5 * * *",
)

COOK_COUNTY_PARCEL_ADDRESSES = DatasetUpdateConfig(
    dataset_id="3723-97qp",
    dataset_name="cook_county_parcel_addresses",
    full_update_cron="50 5 1-7 * 0",
    update_cron="50 5 * * *",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

CHICAGO_ARRESTS = DatasetUpdateConfig(
    dataset_id="dpt3-jri9",
    dataset_name="chicago_arrests",
    full_update_cron="0 2 1-7 * 1",
    update_cron="20 1 * * *",
    entity_key=["cb_no"],
    full_update_mode="api",
)

CHICAGO_CRIMES = DatasetUpdateConfig(
    dataset_id="ijzp-q8t2",
    dataset_name="chicago_crimes",
    full_update_cron="0 1 1-7 * 1",
    update_cron="10 1 * * *",
    entity_key=["id"],
    full_update_mode="file_download",
)

CHICAGO_HOMICIDE_AND_NON_FATAL_SHOOTING_VICTIMIZATIONS = DatasetUpdateConfig(
    dataset_id="gumc-mgzr",
    dataset_name="chicago_homicide_and_non_fatal_shooting_victimizations",
    full_update_cron="0 1 1-7 * 0",
    update_cron="0 1 * * *",
    entity_key=["unique_id"],
    full_update_mode="api",
)
