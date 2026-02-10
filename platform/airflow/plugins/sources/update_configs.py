from dataclasses import dataclass


@dataclass
class DatasetUpdateConfig:
    dataset_id: str
    dataset_name: str
    full_update_cron: str
    update_cron: str


CHICAGO_SPEED_CAMERA_VIOLATION_CONFIG = DatasetUpdateConfig(
    dataset_id="hhkd-xvj4",
    dataset_name="chicago_speed_camera_violations",
    full_update_cron="10 4 1-7 * 0",
    update_cron="5 4 * * 1,4",
)

CHICAGO_RED_LIGHT_CAMERA_VIOLATION_CONFIG = DatasetUpdateConfig(
    dataset_id="spqx-js37",
    dataset_name="chicago_red_light_camera_violations",
    full_update_cron="20 4 1-7 * 0",
    update_cron="20 4 * * 1,4",
)

CHICAGO_311_SERVICE_REQUESTS = DatasetUpdateConfig(
    dataset_id="spqx-js37",
    dataset_name="chicago_311_service_requests",
    full_update_cron="30 4 1-7 * 0",
    update_cron="30 4 * * 1,4",
)

CHICAGO_TOWED_VEHICLES = DatasetUpdateConfig(
    dataset_id="ygr5-vcbg",
    dataset_name="chicago_towed_vehicles",
    full_update_cron="0 4 1-7 * 0",
    update_cron="0 4 * * 1,4",
)
