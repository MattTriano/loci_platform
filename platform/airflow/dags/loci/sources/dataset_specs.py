# loci_platform/platform/airflow/dags/loci/sources/dataset_specs.py
from loci.collectors.arcgishub.spec import ArcGISHubDatasetSpec
from loci.collectors.bike_index.spec import BikeIndexDatasetSpec
from loci.collectors.census.spec import CensusDatasetSpec
from loci.collectors.ckan.spec import CKANDatasetSpec
from loci.collectors.cms.spec import CMSDatasetSpec
from loci.collectors.dkan.spec import DKANDatasetSpec
from loci.collectors.osm.query import OverpassAPIQuery
from loci.collectors.osm.spec import OSMDatasetSpec
from loci.collectors.osmnx.spec import OsmnxDatasetSpec
from loci.collectors.socrata.spec import SocrataDatasetSpec
from loci.collectors.static.spec import FileRef, StaticFileDatasetSpec
from loci.collectors.threedep.spec import ThreeDEPDatasetSpec
from loci.collectors.tiger.spec import TigerDatasetSpec
from loci.geo import BBox

#######################################################################################
#    ArcGIS Hub                                                                       #
#######################################################################################


TORONTO_TRAFFIC_COLLISIONS_SPEC = ArcGISHubDatasetSpec(
    name="toronto_traffic_collisions",
    base_url="https://data.tps.ca",
    item_id="bc4c72a793014a55a674984ef175a6f3",
    target_table="toronto_traffic_collisions",
    entity_key=["event_unique_id"],
)

DETROIT_BIKE_LANES_SPEC = ArcGISHubDatasetSpec(
    name="detroit_bike_lanes",
    base_url="https://data.detroitmi.gov",
    item_id="1a461925a1a242b9b4512380e32516c1",
    target_table="detroit_bike_lanes",
    entity_key=["bike_route_id"],
)

DETROIT_BIKE_PARKING_SPEC = ArcGISHubDatasetSpec(
    name="detroit_bike_parking",
    base_url="https://data.detroitmi.gov",
    item_id="88f5f270fbf64e818dc391e143cd96ab",
    target_table="detroit_bike_parking",
    entity_key=["geom"],
    layer_index=1,
)

DETROIT_BOUNDARY_SPEC = ArcGISHubDatasetSpec(
    name="detroit_boundary",
    base_url="https://data.detroitmi.gov",
    item_id="86b221bb68ca4364afe81d156e54f95c",
    target_table="detroit_boundary",
    entity_key=["fid"],
)

DETROIT_TRAFFIC_CRASHES_SPEC = ArcGISHubDatasetSpec(
    name="detroit_traffic_crashes",
    base_url="https://data.detroitmi.gov",
    item_id="d837b05bdd9643698be30dfedbab0272",
    target_table="detroit_traffic_crashes",
    layer_index="all",
    layer_column="source_layer",
    entity_key=["crash_id"],
)


#######################################################################################
#    Bike Index                                                                       #
#######################################################################################

BOSTON_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="boston_bikeindex_bike_thefts",
    target_table="boston_bikeindex_bike_thefts",
    entity_key=["id"],
    location="42.3125,-71.11",
    distance=12.5,
    stolenness="proximity",
)

CHICAGO_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="chicago_bikeindex_bike_thefts",
    target_table="chicago_bikeindex_bike_thefts",
    entity_key=["id"],
    location="Chicago, IL",
    distance=10,
    stolenness="proximity",
)

DENVER_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="denver_bikeindex_bike_thefts",
    target_table="denver_bikeindex_bike_thefts",
    entity_key=["id"],
    location="39.745,-104.875",
    distance=12,
    stolenness="proximity",
)

DETROIT_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="detroit_bikeindex_bike_thefts",
    target_table="detroit_bikeindex_bike_thefts",
    entity_key=["id"],
    location="42.380,-83.099",
    distance=15,
    stolenness="proximity",
)

MADISON_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="madison_bikeindex_bike_thefts",
    target_table="madison_bikeindex_bike_thefts",
    entity_key=["id"],
    location="Madison, WI",
    distance=15,
    stolenness="proximity",
)

NEW_ORLEANS_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="nola_bikeindex_bike_thefts",
    target_table="nola_bikeindex_bike_thefts",
    entity_key=["id"],
    location="30.02,-89.98",
    distance=25,
    stolenness="proximity",
)

NYC_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="nyc_bikeindex_bike_thefts",
    target_table="nyc_bikeindex_bike_thefts",
    entity_key=["id"],
    location="40.67,-74.00",
    distance=18,
    stolenness="proximity",
)

PORTLAND_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="portland_bikeindex_bike_thefts",
    target_table="portland_bikeindex_bike_thefts",
    entity_key=["id"],
    location="45.535,-122.66",
    distance=12,
    stolenness="proximity",
)

SAN_FRANCISCO_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="sf_bikeindex_bike_thefts",
    target_table="sf_bikeindex_bike_thefts",
    entity_key=["id"],
    location="37.59,-122.16",
    distance=35,
    stolenness="proximity",
)

TORONTO_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="toronto_bikeindex_bike_thefts",
    target_table="toronto_bikeindex_bike_thefts",
    entity_key=["id"],
    location="43.67,-79.315",
    distance=25,
    stolenness="proximity",
)

WASHINGTON_DC_BIKEINDEX_BIKE_THEFTS_SPEC = BikeIndexDatasetSpec(
    name="dc_bikeindex_bike_thefts",
    target_table="dc_bikeindex_bike_thefts",
    entity_key=["id"],
    location="38.89,-77.01",
    distance=10,
    stolenness="proximity",
)


#######################################################################################
#    Census TIGER Data                                                                #
#######################################################################################

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


#######################################################################################
#    Census                                                                           #
#######################################################################################

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
#    CKAN                                                                             #
#######################################################################################

TORONTO_SERIOUS_MOTOR_VEHICLE_COLLISIONS_SPEC = CKANDatasetSpec(
    name="toronto_serious_motor_vehicle_collisions",
    base_url="https://ckan0.cf.opendata.inter.prod-toronto.ca",
    dataset_id="motor-vehicle-collisions-involving-killed-or-seriously-injured-persons",
    target_table="toronto_serious_motor_vehicle_collisions",
    target_schema="raw_data",
    entity_key=["collision_id", "per_no"],
    resource_ids=["f8faf384-a96d-4dff-af59-db40f777c7d3"],
)

TORONTO_BICYCLE_PARKING_RACKS_SPEC = CKANDatasetSpec(
    name="toronto_bicycle_parking_racks",
    base_url="https://ckan0.cf.opendata.inter.prod-toronto.ca",
    dataset_id="bicycle-parking-racks",
    target_table="toronto_bicycle_parking_racks",
    target_schema="raw_data",
    entity_key=["objectid"],
    resource_ids=["4d105465-6e64-4a69-957b-6e0eee5bca8b"],
)


#######################################################################################
#    CMS                                                                              #
#######################################################################################


# Medicare fee-for-service inpatient payments by hospital x DRG x year.
# ~150-210k rows per vintage, so the paged API is fine.
# Grain: one row per (provider CCN, DRG code) per vintage.
MEDICARE_INPATIENT_BY_PROVIDER_AND_SERVICE_SPEC = CMSDatasetSpec(
    name="medicare_inpatient_by_provider_and_service",
    dataset_title="Medicare Inpatient Hospitals - by Provider and Service",
    target_table="medicare_inpatient_by_provider_and_service",
    entity_key=["rndrng_prvdr_ccn", "drg_cd", "vintage"],
    retrieval="api",
)

# Medicare payments by clinician NPI x HCPCS code x place of service x year.
# ~9-10M rows per vintage — use the CSV distribution, not the paged API.
# NOTE: verify the entity key grain before the first run (documented grain
# is NPI x HCPCS x place of service; confirm no duplicates with a sample
# query against one vintage).
MEDICARE_PHYSICIANS_BY_PROVIDER_AND_SERVICE_SPEC = CMSDatasetSpec(
    name="medicare_physicians_by_provider_and_service",
    dataset_title="Medicare Physician & Other Practitioners - by Provider and Service",
    target_table="medicare_physicians_by_provider_and_service",
    entity_key=["rndrng_npi", "hcpcs_cd", "place_of_srvc", "vintage"],
    retrieval="csv",
)


#######################################################################################
#    DKAN                                                                             #
#######################################################################################


# Care Compare hospital dimension: one row per facility CCN. Joins to
# raw_data.medicare_inpatient_by_provider_and_service on
# facility_id = rndrng_prvdr_ccn. ~5.4k rows, refreshed in place
# (roughly quarterly). invalidate_missing=False for now (project
# default); flipping it to True is the correct semantics here —
# a facility absent from a refresh has been delisted from Care
# Compare — once you're comfortable with the behavior.
PDC_HOSPITAL_GENERAL_INFORMATION_SPEC = DKANDatasetSpec(
    name="pdc_hospital_general_information",
    base_url="https://data.cms.gov/provider-data",
    dataset_identifiers=["xubh-q36u"],
    target_table="pdc_hospital_general_information",
    entity_key=["facility_id"],
    retrieval="datastore",
)

# Open Payments General Payments: one metastore dataset per program
# year, ~11M rows / ~6 GB each, all republished together every January
# (so expect an annual recollection of every year listed). Identifiers
# verified against the live catalog 2026-06.
OPENPAYMENTS_GENERAL_PAYMENT_DATASETS = {
    "2018": "f003634c-c103-568f-876c-73017fa83be0",
    "2019": "4e54dd6c-30f8-4f86-86a7-3c109a89528e",
    "2020": "a08c4b30-5cf3-4948-ad40-36f404619019",
    "2021": "0380bbeb-aea1-58b6-b708-829f92a48202",
    "2022": "df01c2f8-dc1f-4e79-96cb-8208beaf143c",
    "2023": "fb3a65aa-c901-4a38-a813-b04b00dfa2a9",
    "2024": "e6b17c6a-2534-4207-a4a1-6746a14911ff",
}

# Start with the two most recent program years (~22M rows, ~12 GB of
# downloads); widen the year list once the first load looks right.
# program_year is in the entity key because record_id uniqueness is
# only guaranteed within a program year's dataset.
OPENPAYMENTS_GENERAL_PAYMENTS_SPEC = DKANDatasetSpec(
    name="openpayments_general_payments",
    base_url="https://openpaymentsdata.cms.gov",
    dataset_identifiers=[
        OPENPAYMENTS_GENERAL_PAYMENT_DATASETS["2023"],
        OPENPAYMENTS_GENERAL_PAYMENT_DATASETS["2024"],
    ],
    target_table="openpayments_general_payments",
    entity_key=["record_id", "program_year"],
    retrieval="file",
)


#######################################################################################
#    OpenStreetMaps                                                                   #
#######################################################################################

BOSTON_BBOX = BBox(42.20, -71.26, 42.45, -70.96)
CHICAGO_BBOX = BBox(41.62, -87.97, 42.05, -87.5)
DENVER_BBOX = BBox(39.55, -105.19, 39.94, -104.56)
DETROIT_BBOX = BBox(42.18, -83.40, 42.56, -82.84)
LOS_ANGELES_BBOX = BBox(33.69, -118.72, 34.38, -118.02)
MADISON_BBOX = BBox(42.96, -89.60, 43.21, -89.21)
NEW_ORLEANS_BBOX = BBox(29.79, -90.34, 30.19, -89.60)
NEW_YORK_CITY_BBOX = BBox(40.48, -74.26, 40.93, -73.70)
PORTLAND_BBOX = BBox(45.41, -122.86, 45.66, -122.46)
SAN_FRANCISCO_BBOX = BBox(37.18, -122.59, 38.00, -121.73)
SEATTLE_BBOX = BBox(47.47, -122.48, 47.78, -122.04)
TORONTO_BBOX = BBox(43.48, -79.79, 43.95, -78.84)
WASHINGTON_DC_BBOX = BBox(38.75, -77.22, 39.04, -76.83)


# ----------------------------------------------------------------------
# OSM Spec Generators
# ----------------------------------------------------------------------


def generate_osm_bike_network_edges_spec(city: str, bbox: BBox) -> OSMDatasetSpec:
    """Build a bikeable-ways spec for a given city."""
    return OSMDatasetSpec(
        name=f"{city}_osm_bike_network_edges",
        target_table=f"{city}_osm_bike_network_edges",
        target_schema="raw_data",
        query=OverpassAPIQuery(
            element_types=["way"],
            tag_filters=[
                {
                    "highway": [
                        "residential",
                        "tertiary",
                        "secondary",
                        "primary",
                        "unclassified",
                        "service",
                        "living_street",
                        "unclassified",
                        "pedestrian",
                        "track",
                        "path",
                        "cycleway",
                        "footway",
                        "bridleway",
                        "road",
                        "busway",
                    ]
                },
                {"bicycle": ["yes", "designated", "permissive"]},
            ],
            bbox=bbox,
        ),
        promoted_tags=[
            "highway",
            "name",
            "bicycle",
            "cycleway",
            "cycleway:left",
            "cycleway:right",
            "surface",
            "maxspeed",
            "oneway",
        ],
    )


def generate_osm_bike_network_nodes_spec(city: str, bbox: BBox) -> OSMDatasetSpec:
    """Build a bike-routing point-features spec for a given city."""
    return OSMDatasetSpec(
        name=f"{city}_osm_bike_network_nodes",
        target_table=f"{city}_osm_bike_network_nodes",
        target_schema="raw_data",
        query=OverpassAPIQuery(
            element_types=["node"],
            tag_filters=[
                {
                    "highway": [
                        "traffic_signals",
                        "stop",
                        "give_way",
                        "crossing",
                        "mini_roundabout",
                        "turning_circle",
                        "turning_loop",
                    ]
                },
                {"crossing": None},
                {"traffic_calming": None},
                {"barrier": None},
            ],
            bbox=bbox,
        ),
        promoted_tags=[
            "highway",
            "crossing",
            "traffic_calming",
            "barrier",
            "bicycle",
            "name",
            "button_operated",
            "tactile_paving",
        ],
    )


def generate_osm_bike_parking_spec(city: str, bbox: BBox) -> OSMDatasetSpec:
    """Build a bike-routing point-features spec for a given city."""
    return OSMDatasetSpec(
        name=f"{city}_osm_bike_parking",
        target_table=f"{city}_osm_bike_parking",
        target_schema="raw_data",
        query=OverpassAPIQuery(
            element_types=["node"],
            tag_filters=[{"amenity": "bicycle_parking"}],
            bbox=bbox,
        ),
        promoted_tags=["name", "bicycle_parking", "capacity", "covered", "access"],
        entity_key=["osm_type", "osm_id"],
    )


# ====================================================================================
#    OSM Overpass Queries
# ====================================================================================

# ----------------------------------------------------------------------
# Clothing & accessories
# ----------------------------------------------------------------------
# `second_hand` is promoted because thrift/vintage is a strong
# neighborhood-character signal you specifically mentioned.
OSM_CLOTHING_QUERY = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[
        {
            "shop": [
                "clothes",
                "shoes",
                "jewelry",
                "bag",
                "boutique",
                "fashion_accessories",
                "tailor",
                "fabric",
                "second_hand",
                "charity",  # often functions as thrift
                "watches",
                "leather",
            ]
        },
    ],
)


# ----------------------------------------------------------------------
# Culture: galleries, museums, performance, libraries, community spaces
# ----------------------------------------------------------------------
# Mixes amenity=*, tourism=*, shop=*, and craft=* under a common roof.
# A synthetic `category` column would be ideal here, but since promoted
# tags map 1:1 to OSM keys, we promote both `amenity` and `tourism` and
# `shop` and let the consumer COALESCE them at query time. Document
# this pattern in the README.
OSM_CULTURE_QUERY = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[
        {
            "amenity": [
                "library",
                "theatre",
                "cinema",
                "arts_centre",
                "community_centre",
                "music_venue",
                "nightclub",  # live music / cultural overlap
                "studio",
                "planetarium",
            ]
        },
        {"tourism": ["gallery", "museum"]},
        {"shop": ["books", "art", "music"]},
        {"craft": ["sculptor", "painter", "photographer"]},
    ],
)

# Fitness: gyms, yoga, classes, sports facilities
# ----------------------------------------------------------------------
# Most of this lives under leisure=fitness_centre / leisure=sports_centre,
# with `sport=*` carrying the specifics (yoga, climbing, swimming, etc.).
# shop=sports is gear, not activity — excluded.
OSM_FITNESS_QUERY = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[
        {
            "leisure": [
                "fitness_centre",
                "sports_centre",
                "fitness_station",  # outdoor exercise equipment
                "swimming_pool",
                "dance",
                "pitch",  # courts/fields — high volume, may want to split
                "track",
            ]
        },
        {"shop": "yoga"},  # rare but real
    ],
)


# Food & drink: anywhere you'd go to eat or drink ready-to-consume things
# ----------------------------------------------------------------------
# `amenity` is promoted so you can filter by subtype:
#   amenity = 'cafe'         -> coffee shops
#   amenity = 'restaurant'   -> sit-down restaurants
#   amenity = 'fast_food'    -> quick-service
#   amenity = 'bar'/'pub'    -> bars
#   amenity = 'ice_cream'    -> ice cream shops
#   amenity = 'biergarten'   -> beer gardens
#   amenity = 'food_court'   -> food courts
OSM_FOOD_AND_DRINK_QUERY = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[
        {
            "amenity": [
                "cafe",
                "restaurant",
                "fast_food",
                "bar",
                "pub",
                "biergarten",
                "ice_cream",
                "food_court",
            ]
        }
    ],
)

# Food retail: anywhere you'd buy food/drink to take home
# ----------------------------------------------------------------------
# `shop` is promoted as the subtype column.
# Bakeries live here (vs. food_and_drink) on the assumption that the
# more common query is "where can I buy bread"; some bakeries are
# eat-in spots that you'll miss if querying food_and_drink only.
OSM_FOOD_RETAIL_QUERY = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[
        {
            "shop": [
                "supermarket",
                "convenience",
                "greengrocer",
                "grocery",
                "butcher",
                "bakery",
                "deli",
                "cheese",
                "seafood",
                "wine",
                "alcohol",
                "coffee",  # whole-bean / coffee retail (vs. amenity=cafe)
                "tea",
                "chocolate",
                "confectionery",
                "pastry",
                "spices",
                "health_food",
            ]
        },
        {"amenity": "marketplace"},  # farmers markets, public markets
    ],
)

OSM_TRANSIT_QUERY = OverpassAPIQuery(
    element_types=["node", "way"],
    tag_filters=[
        {"public_transport": ["station", "stop_position", "platform"]},
        {"railway": "station"},
        {"highway": "bus_stop"},
        {"amenity": "bicycle_rental"},  # e.g. Divvy stations
    ],
)

# *****************
#    OSM Specs
# *****************

BOSTON_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="boston", bbox=BOSTON_BBOX
)
BOSTON_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="boston", bbox=BOSTON_BBOX
)

CHICAGO_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="chicago", bbox=CHICAGO_BBOX
)
CHICAGO_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="chicago", bbox=CHICAGO_BBOX
)

DENVER_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="denver", bbox=DENVER_BBOX
)
DENVER_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="denver", bbox=DENVER_BBOX
)

DETROIT_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="detroit", bbox=DETROIT_BBOX
)
DETROIT_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="detroit", bbox=DETROIT_BBOX
)

MADISON_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="madison", bbox=MADISON_BBOX
)
MADISON_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="madison", bbox=MADISON_BBOX
)

NEW_ORLEANS_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="nola", bbox=NEW_ORLEANS_BBOX
)
NEW_ORLEANS_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="nola", bbox=NEW_ORLEANS_BBOX
)

NYC_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="nyc", bbox=NEW_YORK_CITY_BBOX
)
NYC_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="nyc", bbox=NEW_YORK_CITY_BBOX
)

PORTLAND_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="portland", bbox=PORTLAND_BBOX
)
PORTLAND_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="portland", bbox=PORTLAND_BBOX
)

SAN_FRANCISCO_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="sf", bbox=SAN_FRANCISCO_BBOX
)
SAN_FRANCISCO_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="sf", bbox=SAN_FRANCISCO_BBOX
)

TORONTO_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="toronto", bbox=TORONTO_BBOX
)
TORONTO_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="toronto", bbox=TORONTO_BBOX
)

WASHINGTON_DC_OSM_BIKE_NETWORK_EDGES_SPEC = generate_osm_bike_network_edges_spec(
    city="dc", bbox=WASHINGTON_DC_BBOX
)
WASHINGTON_DC_OSM_BIKE_NETWORK_NODES_SPEC = generate_osm_bike_network_nodes_spec(
    city="dc", bbox=WASHINGTON_DC_BBOX
)


CHICAGO_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(city="chicago", bbox=CHICAGO_BBOX)

DETROIT_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(city="detroit", bbox=DETROIT_BBOX)

DENVER_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(city="denver", bbox=DENVER_BBOX)

MADISON_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(city="madison", bbox=MADISON_BBOX)

NEW_ORLEANS_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(
    city="nola", bbox=NEW_ORLEANS_BBOX
)

NYC_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(city="nyc", bbox=NEW_YORK_CITY_BBOX)

PORTLAND_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(city="portland", bbox=PORTLAND_BBOX)

SAN_FRANCISCO_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(
    city="sf", bbox=SAN_FRANCISCO_BBOX
)

TORONTO_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(city="toronto", bbox=TORONTO_BBOX)

WASHINGTON_DC_OSM_BIKE_PARKING_SPEC = generate_osm_bike_parking_spec(
    city="dc", bbox=WASHINGTON_DC_BBOX
)

BOSTON_OSM_BIKE_PARKING_SPEC = OSMDatasetSpec(
    name="boston_osm_bike_parking",
    target_table="boston_osm_bike_parking",
    query=OverpassAPIQuery(
        element_types=["node", "way", "relation"],
        tag_filters=[{"amenity": "bicycle_parking"}],
        bbox=BOSTON_BBOX,
    ),
    promoted_tags=["name", "bicycle_parking", "capacity", "covered", "access"],
    entity_key=["osm_type", "osm_id"],
)

BOSTON_OSM_TRANSIT_SPEC = OSMDatasetSpec(
    name="boston_osm_transit",
    target_table="boston_osm_transit",
    query=OSM_TRANSIT_QUERY.for_bbox(BOSTON_BBOX),
    promoted_tags=[
        "name",
        "public_transport",
        "railway",
        "highway",
        "amenity",
        "network",
        "operator",
        "ref",
        "wheelchair",
    ],
)

CHICAGO_OSM_BARS_SPEC = OSMDatasetSpec(
    name="chicago_osm_bars",
    target_table="chicago_osm_bars",
    query=OverpassAPIQuery(
        element_types=["node", "way"],
        tag_filters=[
            {"amenity": ["bar", "pub", "biergarten"]},
        ],
        bbox=CHICAGO_BBOX,
    ),
    promoted_tags=[
        "name",
        "amenity",
        "brand",
        "cuisine",
        "microbrewery",
        "craft_beer",
        "food",
        "outdoor_seating",
        "smoking",
        "opening_hours",
        "website",
        "phone",
        "addr:housenumber",
        "addr:street",
        "addr:city",
        "addr:postcode",
    ],
)


CHICAGO_OSM_CAFES_SPEC = OSMDatasetSpec(
    name="chicago_osm_cafes",
    target_table="chicago_osm_cafes",
    query=OverpassAPIQuery(
        element_types=["node", "way"],
        tag_filters=[{"amenity": "cafe"}],
        bbox=CHICAGO_BBOX,
    ),
    promoted_tags=[
        "name",
        "brand",
        "cuisine",
        "takeaway",
        "outdoor_seating",
        "internet_access",
        "opening_hours",
        "website",
        "phone",
        "addr:housenumber",
        "addr:street",
        "addr:city",
        "addr:postcode",
    ],
)

CHICAGO_OSM_CLOTHING_SPEC = OSMDatasetSpec(
    name="chicago_osm_clothing",
    target_table="chicago_osm_clothing",
    query=OSM_CLOTHING_QUERY.for_bbox(CHICAGO_BBOX),
    promoted_tags=[
        "name",
        "shop",  # subtype
        "brand",
        "second_hand",  # yes/only/no — thrift signal
        "clothes",  # men/women/children/etc.
        "opening_hours",
        "website",
        "phone",
        "addr:housenumber",
        "addr:street",
        "addr:city",
        "addr:postcode",
    ],
)

CHICAGO_OSM_CULTURE_SPEC = OSMDatasetSpec(
    name="chicago_osm_culture",
    target_table="chicago_osm_culture",
    query=OSM_CULTURE_QUERY.for_bbox(CHICAGO_BBOX),
    promoted_tags=[
        "name",
        "amenity",  # subtype for amenity=* rows
        "tourism",  # subtype for gallery/museum
        "shop",  # subtype for bookshop/art-shop
        "craft",  # subtype for craft=* rows
        "fee",  # free vs. paid admission
        "opening_hours",
        "website",
        "phone",
        "wheelchair",
        "addr:housenumber",
        "addr:street",
        "addr:city",
        "addr:postcode",
    ],
)

CHICAGO_OSM_FITNESS_SPEC = OSMDatasetSpec(
    name="chicago_osm_fitness",
    target_table="chicago_osm_fitness",
    query=OSM_FITNESS_QUERY.for_bbox(CHICAGO_BBOX),
    promoted_tags=[
        "name",
        "leisure",  # subtype
        "sport",  # the most important filter — yoga, climbing, etc.
        "fitness_station",  # outdoor equipment type
        "access",  # public/private/customers
        "fee",
        "opening_hours",
        "website",
        "phone",
        "addr:housenumber",
        "addr:street",
        "addr:city",
        "addr:postcode",
    ],
)


CHICAGO_OSM_FOOD_AND_DRINK_SPEC = OSMDatasetSpec(
    name="chicago_osm_food_and_drink",
    target_table="chicago_osm_food_and_drink",
    query=OSM_FOOD_AND_DRINK_QUERY.for_bbox(CHICAGO_BBOX),
    promoted_tags=[
        "name",
        "amenity",  # subtype filter — the key column for this table
        "brand",
        "cuisine",
        "takeaway",
        "delivery",
        "outdoor_seating",
        "internet_access",  # essential for the "good laptop spot" query
        "opening_hours",
        "website",
        "phone",
        # Bar-flavored tags — sparse on cafes/restaurants, but cheap to carry:
        "microbrewery",
        "craft_beer",
        "food",  # yes/no: does this bar serve food
        "addr:housenumber",
        "addr:street",
        "addr:city",
        "addr:postcode",
    ],
)


CHICAGO_OSM_FOOD_RETAIL_SPEC = OSMDatasetSpec(
    name="chicago_osm_food_retail",
    target_table="chicago_osm_food_retail",
    query=OSM_FOOD_RETAIL_QUERY.for_bbox(CHICAGO_BBOX),
    promoted_tags=[
        "name",
        "shop",  # primary subtype — null for marketplaces
        "amenity",  # only set for marketplaces
        "brand",
        "organic",
        "opening_hours",
        "website",
        "phone",
        "addr:housenumber",
        "addr:street",
        "addr:city",
        "addr:postcode",
    ],
)

CHICAGO_OSM_TRANSIT_SPEC = OSMDatasetSpec(
    name="chicago_osm_transit",
    target_table="chicago_osm_transit",
    query=OSM_TRANSIT_QUERY.for_bbox(CHICAGO_BBOX),
    promoted_tags=[
        "name",
        "public_transport",
        "railway",
        "highway",
        "amenity",
        "network",  # CTA, Metra, Divvy
        "operator",
        "ref",  # stop number / station code
        "wheelchair",
    ],
)

# Tree coverage in OSM is patchy — expect lots of gaps relative to the
# actual urban canopy. Still useful as a relative signal across neighborhoods.
CHICAGO_OSM_TREES_SPEC = OSMDatasetSpec(
    name="chicago_osm_trees",
    target_table="chicago_osm_trees",
    query=OverpassAPIQuery(
        element_types=["node"],
        tag_filters=[{"natural": "tree"}],
        bbox=CHICAGO_BBOX,
    ),
    promoted_tags=[
        "species",
        "species:en",
        "genus",
        "genus:en",
        "leaf_type",  # broadleaved / needleleaved
        "leaf_cycle",  # deciduous / evergreen
        "height",
        "circumference",
        "diameter_crown",
        "denotation",  # urban / avenue / park / etc.
        "ref",  # municipal tree ID, when present
    ],
)

# DETROIT_OSM_BIKE_PARKING_SPEC = OSMDatasetSpec(
#     name="detroit_osm_bike_parking",
#     target_table="detroit_osm_bike_parking",
#     query=OverpassAPIQuery(
#         element_types=["node", "way", "relation"],
#         tag_filters=[{"amenity": "bicycle_parking"}],
#         bbox=DETROIT_BBOX,
#     ),
#     promoted_tags=["name", "bicycle_parking", "capacity", "covered", "access"],
#     entity_key=["osm_type", "osm_id"],
# )

DETROIT_OSM_TRANSIT_SPEC = OSMDatasetSpec(
    name="detroit_osm_transit",
    target_table="detroit_osm_transit",
    query=OSM_TRANSIT_QUERY.for_bbox(DETROIT_BBOX),
    promoted_tags=[
        "name",
        "public_transport",
        "railway",
        "highway",
        "amenity",
        "network",  # CTA, Metra, Divvy
        "operator",
        "ref",  # stop number / station code
        "wheelchair",
    ],
    entity_key=["osm_type", "osm_id"],
)

MADISON_OSM_BIKE_PARKING_SPEC = OSMDatasetSpec(
    name="madison_osm_bike_parking",
    target_table="madison_osm_bike_parking",
    query=OverpassAPIQuery(
        element_types=["node", "way", "relation"],
        tag_filters=[{"amenity": "bicycle_parking"}],
        bbox=MADISON_BBOX,
    ),
    promoted_tags=["name", "bicycle_parking", "capacity", "covered", "access"],
    entity_key=["osm_type", "osm_id"],
)

MADISON_OSM_TRANSIT_SPEC = OSMDatasetSpec(
    name="madison_osm_transit",
    target_table="madison_osm_transit",
    query=OSM_TRANSIT_QUERY.for_bbox(MADISON_BBOX),
    promoted_tags=[
        "name",
        "public_transport",
        "railway",
        "highway",
        "amenity",
        "network",
        "operator",
        "ref",  # stop number / station code
        "wheelchair",
    ],
)

#######################################################################################
#    OSMnx                                                                            #
#######################################################################################

OSMNX_CHICAGO_BIKE_NETWORK_SPEC = OsmnxDatasetSpec(
    name="chicagoland_bike_network",
    target_table_nodes="chicago_osmnx_bike_network_nodes",
    target_table_edges="chicago_osmnx_bike_network_edges",
    target_schema="raw_data",
    bbox=BBox(41.62, -87.97, 42.05, -87.5),
    network_type="bike",
    entity_key_nodes=["osmid"],
    entity_key_edges=["u", "v", "key"],
    source="osmnx",
)

OSMNX_DETROIT_BIKE_NETWORK_SPEC = OsmnxDatasetSpec(
    name="detroit_bike_network",
    target_table_nodes="osmnx_detroit_bike_network_nodes",
    target_table_edges="osmnx_detroit_bike_network_edges",
    target_schema="raw_data",
    bbox=BBox(42.24, -83.29, 42.46, -82.89),
    network_type="bike",
    entity_key_nodes=["osmid"],
    entity_key_edges=["u", "v", "key"],
    source="osmnx",
)

#######################################################################################
#    Socrata                                                                          #
#######################################################################################

CHICAGO_CITY_BOUNDARY_SPEC = SocrataDatasetSpec(
    name="chicago_city_boundary",
    dataset_id="qqq8-j68g",
    target_table="chicago_city_boundary",
    target_schema="raw_data",
    entity_key=["objectid"],
    full_update_mode="api",
)

CHICAGO_BIKE_RACKS_SPEC = SocrataDatasetSpec(
    name="chicago_bike_racks",
    dataset_id="hgdw-64h3",
    target_table="chicago_bike_racks",
    target_schema="raw_data",
    entity_key=["socrata_id"],
    full_update_mode="api",
)

CHICAGO_COMMUNITY_AREAS_SPEC = SocrataDatasetSpec(
    name="chicago_community_areas",
    dataset_id="igwz-8jzy",
    target_table="chicago_community_areas",
    target_schema="raw_data",
    entity_key=["area_numbe"],
    full_update_mode="api",
)

CHICAGO_POLICE_DISTRICT_SPEC = SocrataDatasetSpec(
    name="chicago_police_districts",
    dataset_id="9vmg-9p8p",
    target_table="chicago_police_districts",
    target_schema="raw_data",
    entity_key=["dist_num"],
    full_update_mode="api",
)

CHICAGO_WARD_PRECINCTS_SPEC = SocrataDatasetSpec(
    name="chicago_ward_precincts",
    dataset_id="6piy-vbxa",
    target_table="chicago_ward_precincts",
    target_schema="raw_data",
    entity_key=["ward_precinct"],
    full_update_mode="api",
)

CHICAGO_PEDWAY_ROUTE_SPEC = SocrataDatasetSpec(
    name="chicago_pedway_routes",
    dataset_id="xkur-4g6u",
    target_table="chicago_pedway_routes",
    target_schema="raw_data",
    entity_key=["objectid"],
    full_update_mode="api",
)

CHICAGO_LIBRARIES_SPEC = SocrataDatasetSpec(
    name="chicago_libraries",
    dataset_id="x8fc-8rcq",
    target_table="chicago_libraries",
    target_schema="raw_data",
    entity_key=["branch_"],
    full_update_mode="api",
)

CHICAGO_BIKE_ROUTES_SPEC = SocrataDatasetSpec(
    name="chicago_bike_routes",
    dataset_id="hvv9-38ut",
    target_table="chicago_bike_routes",
    target_schema="raw_data",
    entity_key=["socrata_id"],
    full_update_mode="api",
)

CHICAGO_BUILDING_FOOTPRINTS_SPEC = SocrataDatasetSpec(
    name="chicago_building_footprints",
    dataset_id="syp8-uezg",
    target_table="chicago_building_footprints",
    target_schema="raw_data",
    entity_key=["bldg_id"],
    full_update_mode="api",
)

CTA_BUS_STOP_SPEC = SocrataDatasetSpec(
    name="cta_bus_stops",
    dataset_id="qs84-j7wh",
    target_table="cta_bus_stops",
    target_schema="raw_data",
    entity_key=["systemstop"],
    full_update_mode="api",
)

CTA_BUS_ROUTES_SPEC = SocrataDatasetSpec(
    name="cta_bus_routes",
    dataset_id="6uva-a5ei",
    target_table="cta_bus_routes",
    target_schema="raw_data",
    entity_key=["route"],
    full_update_mode="api",
)

CHICAGO_STREET_CENTER_LINES_SPEC = SocrataDatasetSpec(
    name="chicago_street_center_lines",
    dataset_id="pr57-gg9e",
    target_table="chicago_street_center_lines",
    target_schema="raw_data",
    entity_key=["objectid"],
    full_update_mode="api",
)

CTA_STATIONS_SPEC = SocrataDatasetSpec(
    name="cta_stations",
    dataset_id="3tzw-cg4m",
    target_table="cta_stations",
    target_schema="raw_data",
    entity_key=["station_id"],
    full_update_mode="api",
)

CHICAGO_PARKS_SPEC = SocrataDatasetSpec(
    name="chicago_parks",
    dataset_id="ejsh-fztr",
    target_table="chicago_parks",
    target_schema="raw_data",
    entity_key=["park_no"],
    full_update_mode="api",
)

CHICAGO_MURAL_REGISTRY_SPEC = SocrataDatasetSpec(
    name="chicago_mural_registry",
    dataset_id="we8h-apcf",
    target_table="chicago_mural_registry",
    target_schema="raw_data",
    entity_key=["mural_registration_id"],
    full_update_mode="api",
)

CHICAGO_LANDMARK_SPEC = SocrataDatasetSpec(
    name="chicago_landmarks",
    dataset_id="uct4-hrvh",
    target_table="chicago_landmarks",
    target_schema="raw_data",
    entity_key=["id"],
    full_update_mode="api",
)

CHICAGO_VACANT_ABANDONED_BUILDINGS_SPEC = SocrataDatasetSpec(
    name="chicago_vacant_abandoned_buildings",
    dataset_id="kc9i-wq85",
    target_table="chicago_vacant_abandoned_buildings",
    target_schema="raw_data",
    entity_key=["violation_number"],
    full_update_mode="api",
)

CHICAGO_BUILDING_SCOFFLAW_LIST_SPEC = SocrataDatasetSpec(
    name="chicago_building_scofflaw_list",
    dataset_id="crg5-4zyp",
    target_table="chicago_building_scofflaw_list",
    target_schema="raw_data",
    entity_key=["record_id"],
    full_update_mode="api",
)

CHICAGO_POTHOLES_PATCHED_SPEC = SocrataDatasetSpec(
    name="chicago_potholes_patched",
    dataset_id="wqdh-9gek",
    target_table="chicago_potholes_patched",
    target_schema="raw_data",
    full_update_mode="api",
)

CHICAGO_RELOCATED_VEHICLES_SPEC = SocrataDatasetSpec(
    name="chicago_relocated_vehicles",
    dataset_id="5k2z-suxx",
    target_table="chicago_relocated_vehicles",
    target_schema="raw_data",
    entity_key=["service_request_number"],
    full_update_mode="api",
)

CHICAGO_LIBRARY_EVENTS_SPEC = SocrataDatasetSpec(
    name="chicago_library_events",
    dataset_id="vsdy-d8k7",
    target_table="chicago_library_events",
    target_schema="raw_data",
    entity_key=["event_id"],
    full_update_mode="api",
)

CHICAGO_HOUSE_SHARE_RESTRICTED_ZONES_SPEC = SocrataDatasetSpec(
    name="chicago_house_share_restricted_zones",
    dataset_id="8eww-pamb",
    target_table="chicago_house_share_restricted_zones",
    target_schema="raw_data",
    entity_key=["precinct"],
    full_update_mode="api",
)

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

OPEN_AIR_CHICAGO_INDIVIDUAL_MEASUREMENTS_SPEC = SocrataDatasetSpec(
    name="open_air_chicago_individual_measurements",
    dataset_id="xfya-dxtq",
    target_table="open_air_chicago_individual_measurements",
    target_schema="raw_data",
    entity_key=["record_id"],
    full_update_mode="file_download",
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
    entity_key=["town_nbhd"],
    full_update_mode="api",
)

COOK_COUNTY_PARCEL_ADDRESSES_SPEC = SocrataDatasetSpec(
    name="cook_county_parcel_addresses",
    dataset_id="3723-97qp",
    target_table="cook_county_parcel_addresses",
    target_schema="raw_data",
    entity_key=["row_id"],
    max_rows=3_000_000,
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


#######################################################################################
#    Static Files                                                                     #
#######################################################################################

AHRQ_BASE = "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium"

# CCN -> health system membership, one row per hospital.
# entity_key includes vintage because each edition is a distinct annual
# snapshot, not an update to the prior edition's records.
# TODO: confirm the CCN column is named "ccn" after the first download
# (generate_ddl will show the real header); the technical documentation
# is at {_BASE}/2023-hospital-linkage-techdoc.pdf
AHRQ_HOSPITAL_LINKAGE_SPEC = StaticFileDatasetSpec(
    name="ahrq_chsp_hospital_linkage",
    target_table="ahrq_chsp_hospital_linkage",
    entity_key=["ccn", "vintage"],
    files=[
        FileRef(
            url=f"{AHRQ_BASE}/chsp-hospital-linkage-2023.csv", vintage="2023", encoding="cp1252"
        ),
    ],
)

# One row per health system (~639 in 2023): AHRQ system id, name, home
# office location, size counts, teaching/safety-net/insurance-product
# flags, ownership, and hospital revenue totals.
AHRQ_HEALTH_SYSTEMS_SPEC = StaticFileDatasetSpec(
    name="ahrq_chsp_health_systems",
    target_table="ahrq_chsp_health_systems",
    entity_key=["health_sys_id", "vintage"],
    files=[
        FileRef(url=f"{AHRQ_BASE}/chsp-compendium-2023-rev.csv", vintage="2023", encoding="cp1252"),
    ],
)


#######################################################################################
#    USGS 3DEP Elevation                                                              #
#######################################################################################


def generate_3dep_elevation_spec(city: str, bbox: BBox, product: str = "13") -> ThreeDEPDatasetSpec:
    """Build a USGS 3DEP elevation spec for a given US city."""
    return ThreeDEPDatasetSpec(
        name=f"{city}_3dep_elevation",
        target_table=f"{city}_3dep_elevation",
        target_schema="raw_data",
        bbox=bbox,
        product=product,
    )


BOSTON_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="boston", bbox=BOSTON_BBOX)
CHICAGO_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="chicago", bbox=CHICAGO_BBOX)
DC_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="dc", bbox=WASHINGTON_DC_BBOX)
DENVER_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="denver", bbox=DENVER_BBOX)
DETROIT_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="detroit", bbox=DENVER_BBOX)
MADISON_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="madison", bbox=MADISON_BBOX)
NOLA_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="nola", bbox=NEW_ORLEANS_BBOX)
PORTLAND_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="portland", bbox=PORTLAND_BBOX)
SF_3DEP_ELEVATION_SPEC = generate_3dep_elevation_spec(city="sf", bbox=SAN_FRANCISCO_BBOX)
