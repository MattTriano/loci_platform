from loci.collectors.census.spec import CensusDatasetSpec
from loci.collectors.tiger.spec import TigerDatasetSpec

TRACT_TIGER_SPEC = TigerDatasetSpec(
    name="census_tracts",
    layer="TRACT",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="census_tracts",
    state_fips=["17"],  # Illinois only; omit for all states
)

BLOCK_GROUP_TIGER_SPEC = TigerDatasetSpec(
    name="block_groups",
    layer="BG",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="block_groups",
)

COUNTY_TIGER_SPEC = TigerDatasetSpec(
    name="counties",
    layer="COUNTY",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="counties",
)

ZCTA_TIGER_SPEC = TigerDatasetSpec(
    name="zip_code_tabulation_areas",
    layer="ZCTA520",
    vintages=[2020, 2021, 2022, 2023, 2024],
    target_table="zcta",
)

RAILS_TIGER_SPEC = TigerDatasetSpec(
    name="railroads",
    layer="RAILS",
    vintages=[2024],
    target_table="railroads",
    entity_key=["linearid", "vintage"],  # RAILS uses LINEARID, not GEOID
)

# National primary roads (interstates, US highways)
PRIMARY_ROADS_TIGER_SPEC = TigerDatasetSpec(
    name="primary_roads",
    layer="PRIMARYROADS",
    vintages=[2024],
    target_table="primary_roads",
    entity_key=["linearid", "vintage"],
)

# State-based primary + secondary roads
PRIMARY_SECONDARY_ROADS_TIGER_SPEC = TigerDatasetSpec(
    name="primary_secondary_roads",
    layer="PRISECROADS",
    vintages=[2024],
    target_table="primary_secondary_roads",
    state_fips=["17"],
    entity_key=["linearid", "vintage"],
)

# County-based all roads — this is the big one
ALL_ROADS_TIGER_SPEC = TigerDatasetSpec(
    name="all_roads",
    layer="ROADS",
    vintages=[2024],
    target_table="all_roads",
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
