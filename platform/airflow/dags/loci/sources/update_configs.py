# /loci_platform/platform/airflow/dags/loci/sources/update_configs.py
from dataclasses import dataclass

from loci.collectors.base_spec import DatasetSpec
from loci.sources import dataset_specs as specs


@dataclass
class DatasetUpdateConfig:
    """
    Configuration for scheduling updates of a single dataset.

    Parameters
    ----------
    spec : DatasetSpec
        The dataset to collect.
    update_cron : str
        Cron expression controlling when the DAG runs.
        Cron's day-of-week field uses Sunday=0, Monday=1, ..., Saturday=6.
    full_update_week_of_month : int
        Which week of the month (1-5) a full refresh should run in.
        Weeks are calendar weeks within the month: days 1-7 are week 1,
        days 8-14 are week 2, etc.
    full_update_day_of_week : int | None
        Which day of the week within `full_update_week_of_month` should
        trigger a full refresh. Uses Pendulum's `day_of_week` convention,
        which on Pendulum 3 (Airflow 3) is Monday=0, Tuesday=1, ...,
        Sunday=6 -- this is OFFSET BY ONE from cron's day-of-week.

        Example: to run a full refresh on the first Tuesday of the month,
        set `update_cron="... * * 2"` (cron Tuesday=2) and
        `full_update_day_of_week=1` (Pendulum Tuesday=1).

        If None, any DAG run that falls in `full_update_week_of_month`
        triggers a full refresh. This is useful when `update_cron`
        already pins the DAG to a single weekday -- the cron's
        day-of-week filter is doing the work, so there's no need to
        specify it again here (and risk the cron/Pendulum off-by-one).
    full_update_months : tuple[int, ...]
        Which months full refreshes are allowed in. Defaults to all 12.
    """

    spec: DatasetSpec
    update_cron: str
    full_update_week_of_month: int
    full_update_day_of_week: int | None = None
    full_update_months: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)


#######################################################################################
#    ArcGIS Hub                                                                       #
#######################################################################################

TORONTO_TRAFFIC_COLLISIONS_UC = DatasetUpdateConfig(
    spec=specs.TORONTO_TRAFFIC_COLLISIONS_SPEC,
    update_cron="0 3 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

DETROIT_BIKE_LANES_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_BIKE_LANES_SPEC,
    update_cron="4 3 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

DETROIT_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_BIKE_PARKING_SPEC,
    update_cron="7 3 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

DETROIT_BOUNDARY_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_BOUNDARY_SPEC,
    update_cron="6 3 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

DETROIT_TRAFFIC_CRASHES_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_TRAFFIC_CRASHES_SPEC,
    update_cron="10 3 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)


###############################################################################
#    Bike Index                                                               #
###############################################################################

BOSTON_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.BOSTON_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 1 * * 0",
    full_update_week_of_month=1,
)

CHICAGO_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 1 * * 1",
    full_update_week_of_month=1,
)

DENVER_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.DENVER_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 2 * * 0",
    full_update_week_of_month=1,
)

DETROIT_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 2 * * 1",
    full_update_week_of_month=1,
)

MADISON_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.MADISON_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 3 * * 0",
    full_update_week_of_month=1,
)

NEW_ORLEANS_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.NEW_ORLEANS_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 3 * * 1",
    full_update_week_of_month=1,
)

NYC_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.NYC_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 2 * * 2",
    full_update_week_of_month=1,
)

PORTLAND_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.PORTLAND_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 4 * * 0",
    full_update_week_of_month=1,
)

SAN_FRANCISCO_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.SAN_FRANCISCO_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 4 * * 1",
    full_update_week_of_month=1,
)

TORONTO_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.TORONTO_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 5 * * 0",
    full_update_week_of_month=1,
)

WASHINGTON_DC_BIKEINDEX_BIKE_THEFTS_UC = DatasetUpdateConfig(
    spec=specs.WASHINGTON_DC_BIKEINDEX_BIKE_THEFTS_SPEC,
    update_cron="0 5 * * 1",
    full_update_week_of_month=1,
)

###############################################################################
#    Census TIGER Data                                                        #
###############################################################################

STATE_TIGER_UC = DatasetUpdateConfig(
    spec=specs.STATE_TIGER_SPEC,
    update_cron="0 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

COUNTY_TIGER_UC = DatasetUpdateConfig(
    spec=specs.COUNTY_TIGER_SPEC,
    update_cron="5 2 22 10 2",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

ZCTA_TIGER_UC = DatasetUpdateConfig(
    spec=specs.ZCTA_TIGER_SPEC,
    update_cron="10 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

TRACT_TIGER_UC = DatasetUpdateConfig(
    spec=specs.TRACT_TIGER_SPEC,
    update_cron="15 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

BLOCK_GROUP_UC = DatasetUpdateConfig(
    spec=specs.BLOCK_GROUP_TIGER_SPEC,
    update_cron="25 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

ADDR_TIGER_UC = DatasetUpdateConfig(
    spec=specs.ADDR_TIGER_SPEC,
    update_cron="20 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

COASTLINE_TIGER_UC = DatasetUpdateConfig(
    spec=specs.COASTLINE_TIGER_SPEC,
    update_cron="40 2 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

RAILS_TIGER_UC = DatasetUpdateConfig(
    spec=specs.RAILS_TIGER_SPEC,
    update_cron="0 3 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

PRIMARY_ROADS_TIGER_UC = DatasetUpdateConfig(
    spec=specs.PRIMARY_ROADS_TIGER_SPEC,
    update_cron="20 3 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

PRIMARY_SECONDARY_ROADS_TIGER_UC = DatasetUpdateConfig(
    spec=specs.PRIMARY_SECONDARY_ROADS_TIGER_SPEC,
    update_cron="40 3 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

ALL_ROADS_TIGER_UC = DatasetUpdateConfig(
    spec=specs.ALL_ROADS_TIGER_SPEC,
    update_cron="0 4 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

AREAWATER_TIGER_UC = DatasetUpdateConfig(
    spec=specs.AREAWATER_TIGER_SPEC,
    update_cron="20 4 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

LINEARWATER_TIGER_UC = DatasetUpdateConfig(
    spec=specs.LINEARWATER_TIGER_SPEC,
    update_cron="40 4 22 10 *",
    full_update_week_of_month=4,
    full_update_day_of_week=3,
)

###############################################################################
#    Census                                                                   #
###############################################################################

ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_UC = DatasetUpdateConfig(
    spec=specs.ACS5__HOUSING_CHARACTERISTICS_BY_TRACT_SPEC,
    update_cron="0 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
)

ACS5__OCCUPATIONS_BY_SEX_BY_TRACT_UC = DatasetUpdateConfig(
    spec=specs.ACS5__OCCUPATIONS_BY_SEX_BY_TRACT_SPEC,
    update_cron="5 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
)

ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_AGE_SEX_RACE_BY_TRACT_UC = DatasetUpdateConfig(
    spec=specs.ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_AGE_SEX_RACE_BY_TRACT_SPEC,
    update_cron="10 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
)

ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_ECON_CHARS_BY_TRACT_UC = DatasetUpdateConfig(
    spec=specs.ACS5__MEANS_OF_TRANSPO_TO_WORK_BY_ECON_CHARS_BY_TRACT_SPEC,
    update_cron="15 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
)

ACS5__INTERNET_UTILIZATION_BY_TRACT_UC = DatasetUpdateConfig(
    spec=specs.ACS5__INTERNET_UTILIZATION_BY_TRACT_SPEC,
    update_cron="25 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
)

ACS5__SEX_BY_AGE_RACE_AND_CITIZENSHIP_BY_TRACT_UC = DatasetUpdateConfig(
    spec=specs.ACS5__SEX_BY_AGE_RACE_AND_CITIZENSHIP_BY_TRACT,
    update_cron="35 10 21-28 * *",
    full_update_week_of_month=4,
    full_update_day_of_week=4,
    full_update_months=(3, 6, 9, 12),
)


#######################################################################################
#    CKAN                                                                             #
#######################################################################################

TORONTO_SERIOUS_MOTOR_VEHICLE_COLLISIONS_UC = DatasetUpdateConfig(
    spec=specs.TORONTO_SERIOUS_MOTOR_VEHICLE_COLLISIONS_SPEC,
    update_cron="0 6 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

TORONTO_BICYCLE_PARKING_RACKS_UC = DatasetUpdateConfig(
    spec=specs.TORONTO_BICYCLE_PARKING_RACKS_SPEC,
    update_cron="3 6 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)


#######################################################################################
#    CMS                                                                              #
#######################################################################################

MEDICARE_INPATIENT_BY_PROVIDER_AND_SERVICE_UC = DatasetUpdateConfig(
    spec=specs.MEDICARE_INPATIENT_BY_PROVIDER_AND_SERVICE_SPEC,
    update_cron="1 0 1 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=2,
)

MEDICARE_PHYSICIANS_BY_PROVIDER_AND_SERVICE_UC = DatasetUpdateConfig(
    spec=specs.MEDICARE_PHYSICIANS_BY_PROVIDER_AND_SERVICE_SPEC,
    update_cron="2 0 1 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=2,
)


#######################################################################################
#    DKAN                                                                             #
#######################################################################################

PDC_HOSPITAL_GENERAL_INFORMATION_UC = DatasetUpdateConfig(
    spec=specs.PDC_HOSPITAL_GENERAL_INFORMATION_SPEC,
    update_cron="1 0 2 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=2,
)

OPENPAYMENTS_GENERAL_PAYMENTS_UC = DatasetUpdateConfig(
    spec=specs.OPENPAYMENTS_GENERAL_PAYMENTS_SPEC,
    update_cron="2 0 2 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=2,
)


#######################################################################################
#    OpenStreetMaps                                                                   #
#######################################################################################

BOSTON_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.BOSTON_OSM_BIKE_PARKING_SPEC,
    update_cron="50 1 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

BOSTON_OSM_TRANSIT_UC = DatasetUpdateConfig(
    spec=specs.BOSTON_OSM_TRANSIT_SPEC,
    update_cron="52 1 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_BARS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_BARS_SPEC,
    update_cron="0 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_BIKE_PARKING_SPEC,
    update_cron="1 6 * * 3",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_CAFES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_CAFES_SPEC,
    update_cron="2 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_CLOTHING_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_CLOTHING_SPEC,
    update_cron="3 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_CULTURE_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_CULTURE_SPEC,
    update_cron="4 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_FITNESS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_FITNESS_SPEC,
    update_cron="5 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_FOOD_AND_DRINK_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_FOOD_AND_DRINK_SPEC,
    update_cron="6 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_FOOD_RETAIL_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_FOOD_RETAIL_SPEC,
    update_cron="7 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_TREES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_TREES_SPEC,
    update_cron="8 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

CHICAGO_OSM_TRANSIT_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_TRANSIT_SPEC,
    update_cron="9 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

DENVER_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.DENVER_OSM_BIKE_PARKING_SPEC,
    update_cron="35 2 * * 2",
    full_update_week_of_month=1,
)

DETROIT_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_OSM_BIKE_PARKING_SPEC,
    update_cron="10 2 * * 2",
    full_update_week_of_month=1,
)

DETROIT_OSM_TRANSIT_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_OSM_TRANSIT_SPEC,
    update_cron="12 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

MADISON_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.MADISON_OSM_BIKE_PARKING_SPEC,
    update_cron="20 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

MADISON_OSM_TRANSIT_UC = DatasetUpdateConfig(
    spec=specs.MADISON_OSM_TRANSIT_SPEC,
    update_cron="22 2 * * 2",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

NEW_ORLEANS_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.NEW_ORLEANS_OSM_BIKE_PARKING_SPEC,
    update_cron="41 2 * * 2",
    full_update_week_of_month=1,
)

NYC_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.NYC_OSM_BIKE_PARKING_SPEC,
    update_cron="37 2 * * 2",
    full_update_week_of_month=1,
)

PORTLAND_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.PORTLAND_OSM_BIKE_PARKING_SPEC,
    update_cron="25 2 * * 2",
    full_update_week_of_month=1,
)

SAN_FRANCISCO_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.SAN_FRANCISCO_OSM_BIKE_PARKING_SPEC,
    update_cron="28 2 * * 2",
    full_update_week_of_month=1,
)

TORONTO_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.TORONTO_OSM_BIKE_PARKING_SPEC,
    update_cron="31 2 * * 2",
    full_update_week_of_month=1,
)

WASHINGTON_DC_OSM_BIKE_PARKING_UC = DatasetUpdateConfig(
    spec=specs.WASHINGTON_DC_OSM_BIKE_PARKING_SPEC,
    update_cron="34 2 * * 2",
    full_update_week_of_month=1,
)


# ------------------------------------------------------------------------------------#
#    OSM: bike networks                                                               #
# ------------------------------------------------------------------------------------#

BOSTON_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.BOSTON_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="6 1 * * 3",
    full_update_week_of_month=1,
)

BOSTON_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.BOSTON_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="8 1 * * 3",
    full_update_week_of_month=1,
)

CHICAGO_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="10 0 * * 3",
    full_update_week_of_month=1,
)

CHICAGO_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="12 0 * * 3",
    full_update_week_of_month=1,
)

DENVER_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.DENVER_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="14 0 * * 3",
    full_update_week_of_month=1,
)

DENVER_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.DENVER_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="16 0 * * 3",
    full_update_week_of_month=1,
)

DETROIT_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="2 1 * * 3",
    full_update_week_of_month=1,
)

DETROIT_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="4 1 * * 3",
    full_update_week_of_month=1,
)

MADISON_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.MADISON_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="10 1 * * 3",
    full_update_week_of_month=1,
)

MADISON_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.MADISON_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="12 1 * * 3",
    full_update_week_of_month=1,
)

NEW_ORLEANS_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.NEW_ORLEANS_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="14 1 * * 3",
    full_update_week_of_month=1,
)

NEW_ORLEANS_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.NEW_ORLEANS_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="16 1 * * 3",
    full_update_week_of_month=1,
)

NYC_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.NYC_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="32 1 * * 3",
    full_update_week_of_month=1,
)

NYC_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.NYC_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="35 1 * * 3",
    full_update_week_of_month=1,
)

PORTLAND_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.PORTLAND_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="18 1 * * 3",
    full_update_week_of_month=1,
)

PORTLAND_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.PORTLAND_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="20 1 * * 3",
    full_update_week_of_month=1,
)

SAN_FRANCISCO_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.SAN_FRANCISCO_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="30 1 * * 3",
    full_update_week_of_month=1,
)

SAN_FRANCISCO_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.SAN_FRANCISCO_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="32 1 * * 3",
    full_update_week_of_month=1,
)

TORONTO_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.TORONTO_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="22 1 * * 3",
    full_update_week_of_month=1,
)

TORONTO_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.TORONTO_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="24 1 * * 3",
    full_update_week_of_month=1,
)

WASHINGTON_DC_OSM_BIKE_NETWORK_EDGES_UC = DatasetUpdateConfig(
    spec=specs.WASHINGTON_DC_OSM_BIKE_NETWORK_EDGES_SPEC,
    update_cron="26 1 * * 3",
    full_update_week_of_month=1,
)

WASHINGTON_DC_OSM_BIKE_NETWORK_NODES_UC = DatasetUpdateConfig(
    spec=specs.WASHINGTON_DC_OSM_BIKE_NETWORK_NODES_SPEC,
    update_cron="28 1 * * 3",
    full_update_week_of_month=1,
)


###############################################################################
#    Socrata                                                                  #
###############################################################################

CHICAGO_CITY_BOUNDARY_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_CITY_BOUNDARY_SPEC,
    update_cron="0 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_BIKE_RACKS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BIKE_RACKS_SPEC,
    update_cron="1 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_COMMUNITY_AREAS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_COMMUNITY_AREAS_SPEC,
    update_cron="2 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_POLICE_DISTRICT_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_POLICE_DISTRICT_SPEC,
    update_cron="3 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_WARD_PRECINCTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_WARD_PRECINCTS_SPEC,
    update_cron="4 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_PEDWAY_ROUTE_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_PEDWAY_ROUTE_SPEC,
    update_cron="5 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_LIBRARIES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_LIBRARIES_SPEC,
    update_cron="6 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_BIKE_ROUTES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BIKE_ROUTES_SPEC,
    update_cron="7 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_BUILDING_FOOTPRINTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BUILDING_FOOTPRINTS_SPEC,
    update_cron="8 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CTA_BUS_STOP_UC = DatasetUpdateConfig(
    spec=specs.CTA_BUS_STOP_SPEC,
    update_cron="15 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CTA_BUS_ROUTES_UC = DatasetUpdateConfig(
    spec=specs.CTA_BUS_ROUTES_SPEC,
    update_cron="16 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_STREET_CENTER_LINES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_STREET_CENTER_LINES_SPEC,
    update_cron="0 3 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=2,
)

CTA_STATIONS_UC = DatasetUpdateConfig(
    spec=specs.CTA_STATIONS_SPEC,
    update_cron="17 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_PARKS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_PARKS_SPEC,
    update_cron="18 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_MURAL_REGISTRY_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_MURAL_REGISTRY_SPEC,
    update_cron="19 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_LANDMARK_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_LANDMARK_SPEC,
    update_cron="20 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_VACANT_ABANDONED_BUILDINGS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_VACANT_ABANDONED_BUILDINGS_SPEC,
    update_cron="21 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_BUILDING_SCOFFLAW_LIST_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BUILDING_SCOFFLAW_LIST_SPEC,
    update_cron="22 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_POTHOLES_PATCHED_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_POTHOLES_PATCHED_SPEC,
    update_cron="23 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_RELOCATED_VEHICLES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_RELOCATED_VEHICLES_SPEC,
    update_cron="24 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_LIBRARY_EVENTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_LIBRARY_EVENTS_SPEC,
    update_cron="25 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_HOUSE_SHARE_RESTRICTED_ZONES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_HOUSE_SHARE_RESTRICTED_ZONES_SPEC,
    update_cron="26 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_BUILDING_PERMITS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_BUILDING_PERMITS_SPEC,
    update_cron="0 6 * * 2,5",
    full_update_week_of_month=1,
    full_update_day_of_week=1,
)

CHICAGO_FOOD_INSPECTIONS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_FOOD_INSPECTIONS_SPEC,
    update_cron="0 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_SIDEWALK_CAFE_PERMITS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_SIDEWALK_CAFE_PERMITS_SPEC,
    update_cron="5 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_SPEED_CAMERA_VIOLATION_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_SPEED_CAMERA_VIOLATIONS_SPEC,
    update_cron="5 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_DIVVY_BICYCLE_STATIONS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_DIVVY_BICYCLE_STATIONS_SPEC,
    update_cron="15 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_RED_LIGHT_CAMERA_VIOLATION_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_RED_LIGHT_CAMERA_VIOLATIONS_SPEC,
    update_cron="20 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

OPEN_AIR_CHICAGO_INDIVIDUAL_MEASUREMENTS_UC = DatasetUpdateConfig(
    spec=specs.OPEN_AIR_CHICAGO_INDIVIDUAL_MEASUREMENTS_SPEC,
    update_cron="5 2 12 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_311_SERVICE_REQUESTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_311_SERVICE_REQUESTS_SPEC,
    update_cron="30 4 10 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_TOWED_VEHICLES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_TOWED_VEHICLES_SPEC,
    update_cron="0 4 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_TRAFFIC_CRASHES_CRASHES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_TRAFFIC_CRASHES_CRASHES_SPEC,
    update_cron="10 3 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_TRAFFIC_CRASHES_PEOPLE_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_TRAFFIC_CRASHES_PEOPLE_SPEC,
    update_cron="40 2 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_TRAFFIC_CRASHES_VEHICLES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_TRAFFIC_CRASHES_VEHICLES_SPEC,
    update_cron="50 2 * * 1,4",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CTA_RIDERSHIP_DAILY_BOARDING_TOTALS_UC = DatasetUpdateConfig(
    spec=specs.CTA_RIDERSHIP_DAILY_BOARDING_TOTALS_SPEC,
    update_cron="30 22 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_LENDING_EQUITY_RESIDENTIAL_LENDING_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_LENDING_EQUITY_RESIDENTIAL_LENDING_SPEC,
    update_cron="40 4 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_ADDITIONAL_DWELLING_UNIT_PREAPPROVAL_APPLICATIONS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_ADDITIONAL_DWELLING_UNIT_PREAPPROVAL_APPLICATIONS_SPEC,
    update_cron="50 4 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

COOK_COUNTY_RESIDENTIAL_CONDOMINIUM_UNIT_CHARACTERISTICS_UC = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_RESIDENTIAL_CONDOMINIUM_UNIT_CHARACTERISTICS_SPEC,
    update_cron="10 5 1 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=5,
)

COOK_COUNTY_SINGLE_AND_MULTI_FAMILY_IMPROVEMENT_CHARACTERISTICS_UC = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_SINGLE_AND_MULTI_FAMILY_IMPROVEMENT_CHARACTERISTICS_SPEC,
    update_cron="20 5 2 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=5,
)

COOK_COUNTY_COMMERCIAL_VALUATION_DATA_UC = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_COMMERCIAL_VALUATION_DATA_SPEC,
    update_cron="30 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=5,
)

COOK_COUNTY_PARCEL_SALES_UC = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_PARCEL_SALES_SPEC,
    update_cron="40 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

COOK_COUNTY_ASSESSED_PARCEL_VALUES_UC = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_ASSESSED_PARCEL_VALUES_SPEC,
    update_cron="30 1 4 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=0,
)

COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES_UC = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES_SPEC,
    update_cron="50 5 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

COOK_COUNTY_PARCEL_ADDRESSES_UC = DatasetUpdateConfig(
    spec=specs.COOK_COUNTY_PARCEL_ADDRESSES_SPEC,
    update_cron="50 5 5 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_ARRESTS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_ARRESTS_SPEC,
    update_cron="20 1 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=0,
)

CHICAGO_CRIMES_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_CRIMES_SPEC,
    update_cron="10 1 7 * *",
    full_update_week_of_month=1,
    full_update_day_of_week=0,
)

CHICAGO_HOMICIDE_AND_NON_FATAL_SHOOTING_VICTIMIZATIONS_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_HOMICIDE_AND_NON_FATAL_SHOOTING_VICTIMIZATIONS_SPEC,
    update_cron="0 1 * * *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)


#######################################################################################
#    Static File                                                                      #
#######################################################################################

AHRQ_HOSPITAL_LINKAGE_UC = DatasetUpdateConfig(
    spec=specs.AHRQ_HOSPITAL_LINKAGE_SPEC,
    update_cron="0 1 20 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

AHRQ_HEALTH_SYSTEMS_UC = DatasetUpdateConfig(
    spec=specs.AHRQ_HEALTH_SYSTEMS_SPEC,
    update_cron="0 2 20 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)


#######################################################################################
#    USGS 3DEP Elevation                                                              #
#######################################################################################


BOSTON_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.BOSTON_3DEP_ELEVATION_SPEC,
    update_cron="0 3 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

CHICAGO_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.CHICAGO_3DEP_ELEVATION_SPEC,
    update_cron="10 3 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

DC_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.DC_3DEP_ELEVATION_SPEC,
    update_cron="20 3 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

DENVER_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.DENVER_3DEP_ELEVATION_SPEC,
    update_cron="30 3 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

DETROIT_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.DETROIT_3DEP_ELEVATION_SPEC,
    update_cron="40 3 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

MADISON_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.MADISON_3DEP_ELEVATION_SPEC,
    update_cron="50 3 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

NOLA_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.NOLA_3DEP_ELEVATION_SPEC,
    update_cron="0 4 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

PORTLAND_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.PORTLAND_3DEP_ELEVATION_SPEC,
    update_cron="10 4 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)

SF_3DEP_ELEVATION_UC = DatasetUpdateConfig(
    spec=specs.SF_3DEP_ELEVATION_SPEC,
    update_cron="20 4 10 1 *",
    full_update_week_of_month=1,
    full_update_day_of_week=6,
)


#######################################################################################
#    OSMnx                                                                            #
#######################################################################################

OSMNX_CHICAGO_BIKE_NETWORK_UC = DatasetUpdateConfig(
    spec=specs.OSMNX_CHICAGO_BIKE_NETWORK_SPEC,
    update_cron="45 6 * * 4",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)

OSMNX_DETROIT_BIKE_NETWORK_UC = DatasetUpdateConfig(
    spec=specs.OSMNX_DETROIT_BIKE_NETWORK_SPEC,
    update_cron="55 6 * * 4",
    full_update_week_of_month=1,
    full_update_day_of_week=4,
)
