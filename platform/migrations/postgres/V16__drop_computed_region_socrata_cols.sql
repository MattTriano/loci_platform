alter table raw_data.chicago_building_permits
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu",
  drop column if exists ":@computed_region_awaf_s7ux";

alter table raw_data.chicago_park_district_activities
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_8hcu_yrd4";

alter table raw_data.chicago_311_service_requests
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu",
  drop column if exists ":@computed_region_du4m_ji7t";

alter table raw_data.chicago_relocated_vehicles
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu",
  drop column if exists ":@computed_region_awaf_s7ux";

alter table raw_data.open_air_chicago_individual_measurements
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_8hcu_yrd4";

alter table raw_data.chicago_homicide_and_non_fatal_shooting_victimizations
  drop column if exists ":@computed_region_d3ds_rm58",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_43wa_7qmu",
  drop column if exists ":@computed_region_d9mm_jgwp";

alter table raw_data.chicago_arrests
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_crimes
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu",
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_d9mm_jgwp",
  drop column if exists ":@computed_region_d3ds_rm58",
  drop column if exists ":@computed_region_8hcu_yrd4";

alter table raw_data.chicago_divvy_bicycle_stations
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu",
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_8hcu_yrd4";

alter table raw_data.chicago_speed_camera_locations
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_red_light_camera_locations
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_red_light_camera_violations
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_speed_camera_violations
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_potholes_patched
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_business_licenses
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_sidewalk_cafe_permits
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_food_inspections
  drop column if exists ":@computed_region_awaf_s7ux",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_additional_dwelling_unit_preapproval_applications
  drop column if exists ":@computed_region_rpca_8um6",
  drop column if exists ":@computed_region_vrxf_vc4k",
  drop column if exists ":@computed_region_6mkv_f3dw",
  drop column if exists ":@computed_region_bdys_3d7i",
  drop column if exists ":@computed_region_43wa_7qmu";

alter table raw_data.chicago_traffic_crashes_crashes
  drop column if exists ":@computed_region_rpca_8um6";
