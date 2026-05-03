from loci.collectors.arcgishub.spec import ArcGISHubDatasetSpec
from loci.collectors.bike_index.spec import BikeIndexDatasetSpec
from loci.collectors.census.spec import CensusDatasetSpec
from loci.collectors.ckan.spec import CKANDatasetSpec
from loci.collectors.osm.query import OverpassAPIQuery
from loci.collectors.osm.spec import OSMDatasetSpec
from loci.collectors.osmnx.spec import OsmnxDatasetSpec
from loci.collectors.socrata.spec import SocrataDatasetSpec
from loci.collectors.tiger.spec import TigerDatasetSpec
from loci.geo import BBox

#######################################################################################
#    Bike Index                                                                       #
#######################################################################################

BIKEINDEX_CHICAGO_STOLEN_BIKES_SPEC = BikeIndexDatasetSpec(
    name="bikeindex_chicago_stolen_bikes",
    target_table="bikeindex_chicago_stolen_bikes",
    entity_key=["id"],
    location="Chicago, IL",
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
#    OpenStreetMaps                                                                   #
#######################################################################################

BOSTON_BBOX = BBox(42.20, -71.26, 42.45, -70.96)
CHICAGO_BBOX = BBox(41.62, -87.97, 42.05, -87.5)
DENVER_BBOX = BBox(39.55, -105.19, 39.94, -104.56)
DETROIT_BBOX = BBox(42.18, -83.40, 42.56, -82.84)
LOS_ANGELES_BBOX = BBox(33.69, -118.72, 34.38, -118.02)
MADISON_BBOX = BBox(42.96, -89.60, 43.21, -89.21)
NEW_ORLEANS_BBOX = BBox(29.83, -90.22, 30.19, -89.60)
NEW_YORK_CITY_BBOX = BBox(40.52, -74.05, 40.93, -73.67)
PORTLAND_BBOX = BBox(45.41, -122.86, 45.66, -122.46)
SAN_FRANCISCO_BBOX = BBox(37.61, -122.53, 37.84, -122.35)
SF_BAY_AREA_BBOX = BBox(37.18, -122.59, 37.88, -121.73)
SEATTLE_BBOX = BBox(47.47, -122.48, 47.78, -122.04)
TORONTO_BBOX = BBox(43.48, -79.79, 43.95, -78.84)


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

CHICAGO_OSM_BIKE_PARKING_SPEC = OSMDatasetSpec(
    name="chicago_osm_bike_parking",
    target_table="chicago_osm_bike_parking",
    query=OverpassAPIQuery(
        element_types=["node", "way", "relation"],
        tag_filters=[{"amenity": "bicycle_parking"}],
        bbox=CHICAGO_BBOX,
    ),
    promoted_tags=["name", "bicycle_parking", "capacity", "covered", "access"],
    entity_key=["osm_type", "osm_id"],
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

DETROIT_OSM_BIKE_PARKING_SPEC = OSMDatasetSpec(
    name="detroit_osm_bike_parking",
    target_table="detroit_osm_bike_parking",
    query=OverpassAPIQuery(
        element_types=["node", "way", "relation"],
        tag_filters=[{"amenity": "bicycle_parking"}],
        bbox=DETROIT_BBOX,
    ),
    promoted_tags=["name", "bicycle_parking", "capacity", "covered", "access"],
    entity_key=["osm_type", "osm_id"],
)

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

# OSM_NODES_SPEC = OsmDatasetSpec(
#     name="osm_nodes",
#     region_ids=["us/illinois"],
#     element_type="nodes",
#     target_table="osm_nodes",
#     target_schema="raw_data",
# )

# OSM_WAYS_SPEC = OsmDatasetSpec(
#     name="osm_ways",
#     region_ids=["us/illinois"],
#     element_type="ways",
#     target_table="osm_ways",
#     target_schema="raw_data",
# )

# OSM_RELATIONS_SPEC = OsmDatasetSpec(
#     name="osm_relations",
#     region_ids=["us/illinois"],
#     element_type="relations",
#     target_table="osm_relations",
#     target_schema="raw_data",
# )

#######################################################################################
#    OSMnx                                                                            #
#######################################################################################

OSMNX_CHICAGO_BIKE_NETWORK_SPEC = OsmnxDatasetSpec(
    name="chicagoland_bike_network",
    target_table_nodes="osmnx_bike_network_nodes",
    target_table_edges="osmnx_bike_network_edges",
    target_schema="raw_data",
    bbox=(-87.97, 41.62, -87.5, 42.05),
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
    bbox=(-83.29, 42.24, -82.89, 42.46),
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
    max_rows=1_500_000,
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
