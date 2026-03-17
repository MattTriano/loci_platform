from dataclasses import dataclass

from loci.collectors.base_spec import DatasetSpec
from loci.sources import dataset_specs as specs


@dataclass
class DatasetUpdateConfig:
    spec: DatasetSpec
    update_cron: str
    full_update_week_of_month: int
    full_update_day_of_week: int
    full_update_months: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    full_update_mode: str = "api"  # "api" or "file_download"


###############################################################################
#    Bike Index                                                               #
###############################################################################

BIKEINDEX_CHICAGO_STOLEN_BIKES_UC = DatasetUpdateConfig(
    spec=specs.BIKEINDEX_CHICAGO_STOLEN_BIKES_SPEC,
    update_cron="0 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=3,
    full_update_mode="api",
)

###############################################################################
#    Census TIGER Data                                                        #
###############################################################################

STATE_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.STATE_TIGER_SPEC,
    update_cron="0 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

COUNTY_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.COUNTY_TIGER_SPEC,
    update_cron="5 2 22 10 2",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

ZCTA_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ZCTA_TIGER_SPEC,
    update_cron="10 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

TRACT_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.TRACT_TIGER_SPEC,
    update_cron="15 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

BLOCK_GROUP_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.BLOCK_GROUP_TIGER_SPEC,
    update_cron="25 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

ADDR_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ADDR_TIGER_SPEC,
    update_cron="20 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

COASTLINE_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.COASTLINE_TIGER_SPEC,
    update_cron="40 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

RAILS_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.RAILS_TIGER_SPEC,
    update_cron="0 3 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

PRIMARY_ROADS_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.PRIMARY_ROADS_TIGER_SPEC,
    update_cron="20 3 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

PRIMARY_SECONDARY_ROADS_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.PRIMARY_SECONDARY_ROADS_TIGER_SPEC,
    update_cron="40 3 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

ALL_ROADS_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ALL_ROADS_TIGER_SPEC,
    update_cron="0 4 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

AREAWATER_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.AREAWATER_TIGER_SPEC,
    update_cron="20 4 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

LINEARWATER_TIGER_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.LINEARWATER_TIGER_SPEC,
    update_cron="40 4 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
    full_update_mode="file_download",
)

###############################################################################
#                                 CENSUS                                      #
###############################################################################

ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_SPEC,
    update_cron="0 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
    full_update_mode="api",
)

ACS5__OCCUPATIONS_BY_SEX_BY_TRACT_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ACS5__OCCUPATIONS_BY_SEX_BY_TRACT_SPEC,
    update_cron="5 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
    full_update_mode="api",
)

ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_AGE_SEX_RACE_BY_TRACT_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_AGE_SEX_RACE_BY_TRACT_SPEC,
    update_cron="10 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
    full_update_mode="api",
)

ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_ECON_CHARS_BY_TRACT_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_ECON_CHARS_BY_TRACT_SPEC,
    update_cron="15 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
    full_update_mode="api",
)

ACS5__INTERNET_UTILIZATION_BY_TRACT_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ACS5__INTERNET_UTILIZATION_BY_TRACT_SPEC,
    update_cron="25 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
    full_update_mode="api",
)

ACS5__SEX_BY_AGE_RACE_AND_CITIZENSHIP_BY_TRACT_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.ACS5__SEX_BY_AGE_RACE_AND_CITIZENSHIP_BY_TRACT,
    update_cron="35 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
    full_update_mode="api",
)

###############################################################################
#                                 SOCRATA                                     #
###############################################################################

CHICAGO_CITY_BOUNDARY_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_CITY_BOUNDARY_SPEC,
    update_cron="0 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_BIKE_RACKS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BIKE_RACKS_SPEC,
    update_cron="1 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_COMMUNITY_AREAS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_COMMUNITY_AREAS_SPEC,
    update_cron="2 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_POLICE_DISTRICT_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_POLICE_DISTRICT_SPEC,
    update_cron="3 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_WARD_PRECINCTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_WARD_PRECINCTS_SPEC,
    update_cron="4 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_PEDWAY_ROUTE_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_PEDWAY_ROUTE_SPEC,
    update_cron="5 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_LIBRARIES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_LIBRARIES_SPEC,
    update_cron="6 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_BIKE_ROUTES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BIKE_ROUTES_SPEC,
    update_cron="7 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_BUILDING_FOOTPRINTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BUILDING_FOOTPRINTS_SPEC,
    update_cron="8 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CTA_BUS_STOP_UC = DatasetUpdateConfig(
    spec=specs.CTA_BUS_STOP_SPEC,
    update_cron="15 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CTA_BUS_ROUTES_UC = DatasetUpdateConfig(
    spec=specs.CTA_BUS_ROUTES_SPEC,
    update_cron="16 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_STREET_CENTER_LINES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_STREET_CENTER_LINES_SPEC,
    update_cron="0 3 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=2,
    full_update_mode="api",
)

CTA_STATIONS_UC = DatasetUpdateConfig(
    spec=specs.CTA_STATIONS_SPEC,
    update_cron="17 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_PARKS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_PARKS_SPEC,
    update_cron="18 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_MURAL_REGISTRY_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_MURAL_REGISTRY_SPEC,
    update_cron="19 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_LANDMARK_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_LANDMARK_SPEC,
    update_cron="20 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_VACANT_ABANDONED_BUILDINGS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_VACANT_ABANDONED_BUILDINGS_SPEC,
    update_cron="21 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_BUILDING_SCOFFLAW_LIST_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BUILDING_SCOFFLAW_LIST_SPEC,
    update_cron="22 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_POTHOLES_PATCHED_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_POTHOLES_PATCHED_SPEC,
    update_cron="23 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_RELOCATED_VEHICLES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_RELOCATED_VEHICLES_SPEC,
    update_cron="24 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_LIBRARY_EVENTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_LIBRARY_EVENTS_SPEC,
    update_cron="25 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_HOUSE_SHARE_RESTRICTED_ZONES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_HOUSE_SHARE_RESTRICTED_ZONES_SPEC,
    update_cron="26 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_BUILDING_PERMITS = DatasetUpdateConfig(
    spec=specs.CHICAGO_BUILDING_PERMITS_SPEC,
    update_cron="0 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
    full_update_mode="api",
)

CHICAGO_FOOD_INSPECTIONS = DatasetUpdateConfig(
    spec=specs.CHICAGO_FOOD_INSPECTIONS_SPEC,
    update_cron="0 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_SIDEWALK_CAFE_PERMITS = DatasetUpdateConfig(
    spec=specs.CHICAGO_SIDEWALK_CAFE_PERMITS_SPEC,
    update_cron="5 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_SPEED_CAMERA_VIOLATION_CONFIG = DatasetUpdateConfig(
    spec=specs.CHICAGO_SPEED_CAMERA_VIOLATIONS_SPEC,
    update_cron="5 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_DIVVY_BICYCLE_STATIONS = DatasetUpdateConfig(
    spec=specs.CHICAGO_DIVVY_BICYCLE_STATIONS_SPEC,
    update_cron="15 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_RED_LIGHT_CAMERA_VIOLATION_CONFIG = DatasetUpdateConfig(
    spec=specs.CHICAGO_RED_LIGHT_CAMERA_VIOLATIONS_SPEC,
    update_cron="20 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

OPEN_AIR_CHICAGO_INDIVIDUAL_MEASUREMENTS_UC = DatasetUpdateConfig(
    spec=specs.OPEN_AIR_CHICAGO_INDIVIDUAL_MEASUREMENTS_SPEC,
    update_cron="5 2 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_311_SERVICE_REQUESTS = DatasetUpdateConfig(
    spec=specs.CHICAGO_311_SERVICE_REQUESTS_SPEC,
    update_cron="30 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="file_download",
)

CHICAGO_TOWED_VEHICLES = DatasetUpdateConfig(
    spec=specs.CHICAGO_TOWED_VEHICLES_SPEC,
    update_cron="0 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_CRASHES = DatasetUpdateConfig(
    spec=specs.CHICAGO_TRAFFIC_CRASHES_CRASHES_SPEC,
    update_cron="10 3 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_PEOPLE = DatasetUpdateConfig(
    spec=specs.CHICAGO_TRAFFIC_CRASHES_PEOPLE_SPEC,
    update_cron="40 2 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_TRAFFIC_CRASHES_VEHICLES = DatasetUpdateConfig(
    spec=specs.CHICAGO_TRAFFIC_CRASHES_VEHICLES_SPEC,
    update_cron="50 2 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CTA_RIDERSHIP_DAILY_BOARDING_TOTALS = DatasetUpdateConfig(
    spec=specs.CTA_RIDERSHIP_DAILY_BOARDING_TOTALS_SPEC,
    update_cron="30 22 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_LENDING_EQUITY_RESIDENTIAL_LENDING = DatasetUpdateConfig(
    spec=specs.CHICAGO_LENDING_EQUITY_RESIDENTIAL_LENDING_SPEC,
    update_cron="40 4 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

CHICAGO_ADDITIONAL_DWELLING_UNIT_PREAPPROVAL_APPLICATIONS = DatasetUpdateConfig(
    spec=specs.CHICAGO_ADDITIONAL_DWELLING_UNIT_PREAPPROVAL_APPLICATIONS_SPEC,
    update_cron="50 4 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

COOK_COUNTY_RESIDENTIAL_CONDOMINIUM_UNIT_CHARACTERISTICS = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_RESIDENTIAL_CONDOMINIUM_UNIT_CHARACTERISTICS_SPEC,
    update_cron="10 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=5,
    full_update_mode="file_download",
)

COOK_COUNTY_SINGLE_AND_MULTI_FAMILY_IMPROVEMENT_CHARACTERISTICS = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_SINGLE_AND_MULTI_FAMILY_IMPROVEMENT_CHARACTERISTICS_SPEC,
    update_cron="20 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=5,
    full_update_mode="file_download",
)

COOK_COUNTY_COMMERCIAL_VALUATION_DATA = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_COMMERCIAL_VALUATION_DATA_SPEC,
    update_cron="30 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=5,
    full_update_mode="api",
)

COOK_COUNTY_PARCEL_SALES = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_PARCEL_SALES_SPEC,
    update_cron="40 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

COOK_COUNTY_ASSESSED_PARCEL_VALUES = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_ASSESSED_PARCEL_VALUES_SPEC,
    update_cron="30 1 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=0,
    full_update_mode="file_download",
)

COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES_SPEC,
    update_cron="50 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

COOK_COUNTY_PARCEL_ADDRESSES = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_PARCEL_ADDRESSES_SPEC,
    update_cron="50 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="file_download",
)

CHICAGO_ARRESTS = DatasetUpdateConfig(
    spec=specs.CHICAGO_ARRESTS_SPEC,
    update_cron="20 1 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=0,
    full_update_mode="api",
)

CHICAGO_CRIMES = DatasetUpdateConfig(
    spec=specs.CHICAGO_CRIMES_SPEC,
    update_cron="10 1 * * 1",
    full_update_week_of_month=1,
    full_update_day_of_week=0,
    full_update_mode="file_download",
)

CHICAGO_HOMICIDE_AND_NON_FATAL_SHOOTING_VICTIMIZATIONS = DatasetUpdateConfig(
    spec=specs.CHICAGO_HOMICIDE_AND_NON_FATAL_SHOOTING_VICTIMIZATIONS_SPEC,
    update_cron="0 1 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
    full_update_mode="api",
)

#######################################################################################
#    OpenStreetMaps                                                                   #
#######################################################################################

OSM_NODES_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.OSM_NODES_SPEC,
    update_cron="0 3 1-7 2,5,8,11 *",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
    full_update_months=(2, 5, 8, 11),
    full_update_mode="file_download",
)

OSM_WAYS_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.OSM_WAYS_SPEC,
    update_cron="0 5 1-7 2,5,8,11 *",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
    full_update_months=(2, 5, 8, 11),
    full_update_mode="file_download",
)

OSM_RELATIONS_UPDATE_CONFIG = DatasetUpdateConfig(
    spec=specs.OSM_RELATIONS_SPEC,
    update_cron="30 5 1-7 2,5,8,11 *",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
    full_update_months=(2, 5, 8, 11),
    full_update_mode="file_download",
)

#######################################################################################
#    OSMnx                                                                            #
#######################################################################################

OSMNX_CHICAGO_BIKE_NETWORK_UC = DatasetUpdateConfig(
    spec=specs.OSMNX_CHICAGO_BIKE_NETWORK_SPEC,
    update_cron="45 5 1-7 2,5,8,11 *",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
    full_update_months=(2, 5, 8, 11),
    full_update_mode="api",
)
