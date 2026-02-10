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
