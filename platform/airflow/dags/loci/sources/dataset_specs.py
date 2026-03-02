from loci.collectors.census.spec import CensusDatasetSpec
from loci.collectors.osm.spec import OsmDatasetSpec
from loci.collectors.socrata.spec import SocrataDatasetSpec
from loci.collectors.tiger.spec import TigerDatasetSpec

STATE_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_states",
    layer="STATE",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="tiger_states",
)

COUNTY_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_counties",
    layer="COUNTY",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="tiger_counties",
)

ZCTA_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_zip_code_tabulation_areas",
    layer="ZCTA520",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="tiger_zcta",
)

TRACT_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_tracts",
    layer="TRACT",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="tiger_tracts",
)

BLOCK_GROUP_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_block_groups",
    layer="BG",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="tiger_block_groups",
)

ADDR_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_addrs",
    layer="ADDR",
    vintages=[2024, 2022, 2020],
    target_table="tiger_addrs",
    state_fips=["17"],
)

COASTLINE_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_coastline",
    layer="COASTLINE",
    vintages=[2024, 2023, 2022, 2021, 2020],
    target_table="tiger_coastline",
)

AREAWATER_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_areawater",
    layer="AREAWATER",
    vintages=[2024],
    target_table="tiger_areawater",
    state_fips=["17"],
    entity_key=["hydroid", "vintage"],
)

LINEARWATER_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_linear_water",
    layer="LINEARWATER",
    vintages=[2024],
    target_table="tiger_linear_water",
    state_fips=["17"],
    entity_key=["linearid", "vintage"],
)

RAILS_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_railroads",
    layer="RAILS",
    vintages=[2024, 2023, 2022, 2021, 2020],
    target_table="tiger_railroads",
    entity_key=["linearid", "vintage"],  # RAILS uses LINEARID, not GEOID
)

# National primary roads (interstates, US highways)
PRIMARY_ROADS_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_primary_roads",
    layer="PRIMARYROADS",
    vintages=[2024],
    target_table="tiger_primary_roads",
    entity_key=["linearid", "vintage"],
)

# State-based primary + secondary roads
PRIMARY_SECONDARY_ROADS_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_primary_secondary_roads",
    layer="PRISECROADS",
    vintages=[2024],
    target_table="tiger_primary_secondary_roads",
    state_fips=["17"],
    entity_key=["linearid", "vintage"],
)

# County-based all roads — this is the big one
ALL_ROADS_TIGER_SPEC = TigerDatasetSpec(
    name="tiger_all_roads",
    layer="ROADS",
    vintages=[2024],
    target_table="tiger_all_roads",
    state_fips=["17"],  # strongly recommend limiting states for roads
    entity_key=["linearid", "vintage"],
)


ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_SPEC = CensusDatasetSpec(
    name="acs5__housing_characteristics",
    dataset="acs/acs5/subject",
    vintages=[2024, 2023, 2022, 2021, 2020],
    geography_level="tract",
    groups=["S2504"],
    state_fips=["17"],
    target_schema="raw_data",
    target_table="acs5__housing_characteristics_by_tract",
)

ACS5__OCCUPATIONS_BY_SEX_BY_TRACT_SPEC = CensusDatasetSpec(
    name="acs5__occupations_by_sex",
    dataset="acs/acs5",
    vintages=[2023],
    geography_level="tract",
    groups=["B24012"],
    state_fips=["17"],
    target_schema="raw_data",
    target_table="acs5__occupations_by_sex_by_tract",
)

ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_AGE_SEX_RACE_BY_TRACT_SPEC = CensusDatasetSpec(
    name="acs5__means_of_transportation_to_work_by_age_sex_race",
    dataset="acs/acs5",
    vintages=[2024, 2023, 2022, 2021, 2020],
    geography_level="tract",
    groups=[
        "B08006",
        "B08101",
        "B08103",
        "B08105A",
        "B08105B",
        "B08105C",
        "B08105D",
        "B08105E",
        "B08105F",
        "B08105G",
        "B08105H",
        "B08105I",
        "B08301",
    ],
    state_fips=["17"],
    target_schema="raw_data",
    target_table="acs5__means_of_transportation_to_work_by_age_sex_race_by_tract",
)

ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_ECON_CHARS_BY_TRACT_SPEC = CensusDatasetSpec(
    name="acs5__means_of_transportation_to_work_by_econ_chars",
    dataset="acs/acs5",
    vintages=[2024, 2023, 2022, 2021, 2020],
    geography_level="tract",
    groups=[
        "B08111",
        "B08113",
        "B08119",
        "B08121",
        "B08122",
        "B08124",
        "B08126",
        "B08128",
        "B08130",
        "B08132",
        "B08134",
        "B08136",
        "B08137",
        "B08141",
    ],
    state_fips=["17"],
    target_schema="raw_data",
    target_table="acs5__means_of_transportation_to_work_by_econ_chars_by_tract",
)


ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_GEOGRAPHY_BY_TRACT_SPEC = CensusDatasetSpec(
    name="acs5__means_of_transportation_to_work_by_geography",
    dataset="acs/acs5",
    vintages=[2024, 2023, 2022, 2021, 2020],
    geography_level="tract",
    groups=[
        "B08406",
        "B08501",
        "B08503",
        "B08505A",
        "B08505B",
        "B08505C",
        "B08505D",
        "B08505E",
        "B08505F",
        "B08505G",
        "B08505H",
        "B08505I",
        "B08511",
        "B08513",
        "B08519",
        "B08521",
        "B08522",
        "B08524",
        "B08526",
        "B08528",
        "B08532",
        "B08534",
        "B08536",
        "B08537",
        "B08541",
        "B08601",
    ],
    state_fips=["17"],
    target_schema="raw_data",
    target_table="acs5__means_of_transportation_to_work_by_geography_by_tract",
)


ACS5__INTERNET_UTILIZATION_BY_TRACT_SPEC = CensusDatasetSpec(
    name="acs5__internet_utilization",
    dataset="acs/acs5",
    vintages=[2024, 2023, 2022, 2021, 2020],
    geography_level="tract",
    groups=[
        "B28002",
        "B28003",
        "B28004",
        "B28005",
        "B28006",
        "B28007",
        "B28008",
        "B28009A",
        "B28009B",
        "B28009C",
        "B28009D",
        "B28009E",
        "B28009F",
        "B28009G",
        "B28009H",
        "B28009I",
        "B28011",
        "B28012",
        "B99281",
        "B99283",
    ],
    state_fips=["17"],
    target_schema="raw_data",
    target_table="acs5__internet_utilization_by_tract",
)


ACS5__SEX_BY_AGE_RACE_AND_CITIZENSHIP_BY_TRACT = CensusDatasetSpec(
    name="acs5__sex_by_age_race_and_citizenship",
    dataset="acs/acs5",
    vintages=[2024, 2023, 2022, 2021, 2020],
    geography_level="tract",
    groups=[
        "B01001",
        "B01001A",
        "B01001B",
        "B01001C",
        "B01001D",
        "B01001E",
        "B01001F",
        "B01001G",
        "B01001H",
        "B01001I",
        "B05003",
        "B05003A",
        "B05003B",
        "B05003C",
        "B05003D",
        "B05003E",
        "B05003F",
        "B05003G",
        "B05003H",
        "B05003I",
        "B05013",
        "B05014",
    ],
    state_fips=["17"],
    target_schema="raw_data",
    target_table="acs5__sex_by_age_race_and_citizenship_by_tract",
)

#######################################################################################
#                                    OSM                                              #
#######################################################################################

OSM_NODES_SPEC = OsmDatasetSpec(
    name="osm_nodes",
    region_ids=["us/illinois"],
    element_type="nodes",
    target_table="osm_nodes",
    target_schema="raw_data",
)

OSM_WAYS_SPEC = OsmDatasetSpec(
    name="osm_ways",
    region_ids=["us/illinois"],
    element_type="ways",
    target_table="osm_ways",
    target_schema="raw_data",
)

OSM_RELATIONS_SPEC = OsmDatasetSpec(
    name="osm_relations",
    region_ids=["us/illinois"],
    element_type="relations",
    target_table="osm_relations",
    target_schema="raw_data",
)

#######################################################################################
#                                    Socrata                                          #
#######################################################################################

CHICAGO_BUILDING_PERMITS_SPEC = SocrataDatasetSpec(
    name="chicago_building_permits",
    dataset_id="ydr8-5enu",
    target_table="chicago_building_permits",
    target_schema="raw_data",
    entity_key=["permit_"],
    full_update_mode="api",
)

CHICAGO_FOOD_INSPECTIONS_SPEC = SocrataDatasetSpec(
    name="chicago_food_inspections",
    dataset_id="4ijn-s7e5",
    target_table="chicago_food_inspections",
    target_schema="raw_data",
    entity_key=["inspection_id"],
    full_update_mode="api",
)

CHICAGO_SIDEWALK_CAFE_PERMITS_SPEC = SocrataDatasetSpec(
    name="chicago_sidewalk_cafe_permits",
    dataset_id="nxj5-ix6z",
    target_table="chicago_sidewalk_cafe_permits",
    target_schema="raw_data",
    entity_key=["permit_number"],
    full_update_mode="api",
)

CHICAGO_SPEED_CAMERA_VIOLATIONS_SPEC = SocrataDatasetSpec(
    name="chicago_speed_camera_violations",
    dataset_id="hhkd-xvj4",
    target_table="chicago_speed_camera_violations",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

CHICAGO_DIVVY_BICYCLE_STATIONS_SPEC = SocrataDatasetSpec(
    name="chicago_divvy_bicycle_stations",
    dataset_id="bbyy-e7gq",
    target_table="chicago_divvy_bicycle_stations",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

CHICAGO_RED_LIGHT_CAMERA_VIOLATIONS_SPEC = SocrataDatasetSpec(
    name="chicago_red_light_camera_violations",
    dataset_id="spqx-js37",
    target_table="chicago_red_light_camera_violations",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

CHICAGO_311_SERVICE_REQUESTS_SPEC = SocrataDatasetSpec(
    name="chicago_311_service_requests",
    dataset_id="v6vf-nfxy",
    target_table="chicago_311_service_requests",
    target_schema="raw_data",
    entity_key=["sr_number"],
    full_update_mode="file_download",
)

CHICAGO_TOWED_VEHICLES_SPEC = SocrataDatasetSpec(
    name="chicago_towed_vehicles",
    dataset_id="ygr5-vcbg",
    target_table="chicago_towed_vehicles",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_CRASHES_SPEC = SocrataDatasetSpec(
    name="chicago_traffic_crashes_crashes",
    dataset_id="85ca-t3if",
    target_table="chicago_traffic_crashes_crashes",
    target_schema="raw_data",
    entity_key=["crash_record_id"],
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_PEOPLE_SPEC = SocrataDatasetSpec(
    name="chicago_traffic_crashes_people",
    dataset_id="u6pd-qa9d",
    target_table="chicago_traffic_crashes_people",
    target_schema="raw_data",
    entity_key=["person_id"],
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_VEHICLES_SPEC = SocrataDatasetSpec(
    name="chicago_traffic_crashes_vehicles",
    dataset_id="68nd-jvt3",
    target_table="chicago_traffic_crashes_vehicles",
    target_schema="raw_data",
    entity_key=["crash_unit_id"],
    full_update_mode="api",
)

CTA_RIDERSHIP_DAILY_BOARDING_TOTALS_SPEC = SocrataDatasetSpec(
    name="cta_ridership_daily_boarding_totals",
    dataset_id="6iiy-9s97",
    target_table="cta_ridership_daily_boarding_totals",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

CHICAGO_LENDING_EQUITY_RESIDENTIAL_LENDING_SPEC = SocrataDatasetSpec(
    name="chicago_lending_equity_residential_lending",
    dataset_id="b77m-uuhb",
    target_table="chicago_lending_equity_residential_lending",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

CHICAGO_ADDITIONAL_DWELLING_UNIT_PREAPPROVAL_APPLICATIONS_SPEC = SocrataDatasetSpec(
    name="chicago_additional_dwelling_unit_preapproval_applications",
    dataset_id="xbwc-ntpx",
    target_table="chicago_additional_dwelling_unit_preapproval_applications",
    target_schema="raw_data",
    entity_key=["id"],
    full_update_mode="api",
)

COOK_COUNTY_RESIDENTIAL_CONDOMINIUM_UNIT_CHARACTERISTICS_SPEC = SocrataDatasetSpec(
    name="cook_county_residential_condominium_unit_characteristics",
    dataset_id="3r7i-mrz4",
    target_table="cook_county_residential_condominium_unit_characteristics",
    target_schema="raw_data",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

COOK_COUNTY_SINGLE_AND_MULTI_FAMILY_IMPROVEMENT_CHARACTERISTICS_SPEC = SocrataDatasetSpec(
    name="cook_county_single_and_multi_family_improvement_characteristics",
    dataset_id="x54s-btds",
    target_table="cook_county_single_and_multi_family_improvement_characteristics",
    target_schema="raw_data",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

COOK_COUNTY_COMMERCIAL_VALUATION_DATA_SPEC = SocrataDatasetSpec(
    name="cook_county_commercial_valuation_data",
    dataset_id="csik-bsws",
    target_table="cook_county_commercial_valuation_data",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

COOK_COUNTY_PARCEL_SALES_SPEC = SocrataDatasetSpec(
    name="cook_county_parcel_sales",
    dataset_id="wvhk-k5uv",
    target_table="cook_county_parcel_sales",
    target_schema="raw_data",
    entity_key=["row_id"],
    full_update_mode="api",
)

COOK_COUNTY_ASSESSED_PARCEL_VALUES_SPEC = SocrataDatasetSpec(
    name="cook_county_assessed_parcel_values",
    dataset_id="uzyt-m557",
    target_table="cook_county_assessed_parcel_values",
    target_schema="raw_data",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES_SPEC = SocrataDatasetSpec(
    name="cook_county_neighborhood_boundaries",
    dataset_id="pcdw-pxtg",
    target_table="cook_county_neighborhood_boundaries",
    target_schema="raw_data",
    entity_key=None,
    full_update_mode="api",
)

COOK_COUNTY_PARCEL_ADDRESSES_SPEC = SocrataDatasetSpec(
    name="cook_county_parcel_addresses",
    dataset_id="3723-97qp",
    target_table="cook_county_parcel_addresses",
    target_schema="raw_data",
    entity_key=["row_id"],
    full_update_mode="file_download",
)

CHICAGO_ARRESTS_SPEC = SocrataDatasetSpec(
    name="chicago_arrests",
    dataset_id="dpt3-jri9",
    target_table="chicago_arrests",
    target_schema="raw_data",
    entity_key=["cb_no"],
    full_update_mode="api",
)

CHICAGO_CRIMES_SPEC = SocrataDatasetSpec(
    name="chicago_crimes",
    dataset_id="ijzp-q8t2",
    target_table="chicago_crimes",
    target_schema="raw_data",
    entity_key=["id"],
    full_update_mode="file_download",
)

CHICAGO_HOMICIDE_AND_NON_FATAL_SHOOTING_VICTIMIZATIONS_SPEC = SocrataDatasetSpec(
    name="chicago_homicide_and_non_fatal_shooting_victimizations",
    dataset_id="gumc-mgzr",
    target_table="chicago_homicide_and_non_fatal_shooting_victimizations",
    target_schema="raw_data",
    entity_key=["unique_id"],
    full_update_mode="api",
)
