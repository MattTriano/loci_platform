create schema if not exists meta;

create table meta.ingest_log (
    id              serial primary key,
    source          text not null,
    dataset         text not null,
    target_table    text not null,
    rows_ingested   int not null,
    high_water_mark text,
    started_at      timestamptz not null,
    completed_at    timestamptz not null,
    status          text not null default 'success',
    error_message   text,
    metadata        jsonb
);

create table if not exists raw_data.chicago_building_permits (
    "id" text,
    "permit_" text,
    "permit_status" text,
    "permit_milestone" text,
    "permit_type" text,
    "review_type" text,
    "application_start_date" timestamptz,
    "issue_date" timestamptz,
    "processing_time" numeric,
    "street_number" numeric,
    "street_direction" text,
    "street_name" text,
    "work_type" text,
    "work_description" text,
    "permit_condition" text,
    "building_fee_paid" numeric,
    "zoning_fee_paid" numeric,
    "other_fee_paid" numeric,
    "subtotal_paid" numeric,
    "building_fee_unpaid" numeric,
    "zoning_fee_unpaid" numeric,
    "other_fee_unpaid" numeric,
    "subtotal_unpaid" numeric,
    "building_fee_waived" numeric,
    "building_fee_subtotal" numeric,
    "zoning_fee_subtotal" numeric,
    "other_fee_subtotal" numeric,
    "zoning_fee_waived" numeric,
    "other_fee_waived" numeric,
    "subtotal_waived" numeric,
    "total_fee" numeric,
    "contact_1_type" text,
    "contact_1_name" text,
    "contact_1_city" text,
    "contact_1_state" text,
    "contact_1_zipcode" text,
    "contact_2_type" text,
    "contact_2_name" text,
    "contact_2_city" text,
    "contact_2_state" text,
    "contact_2_zipcode" text,
    "contact_3_type" text,
    "contact_3_name" text,
    "contact_3_city" text,
    "contact_3_state" text,
    "contact_3_zipcode" text,
    "contact_4_type" text,
    "contact_4_name" text,
    "contact_4_city" text,
    "contact_4_state" text,
    "contact_4_zipcode" text,
    "contact_5_type" text,
    "contact_5_name" text,
    "contact_5_city" text,
    "contact_5_state" text,
    "contact_5_zipcode" text,
    "contact_6_type" text,
    "contact_6_name" text,
    "contact_6_city" text,
    "contact_6_state" text,
    "contact_6_zipcode" text,
    "contact_7_type" text,
    "contact_7_name" text,
    "contact_7_city" text,
    "contact_7_state" text,
    "contact_7_zipcode" text,
    "contact_8_type" text,
    "contact_8_name" text,
    "contact_8_city" text,
    "contact_8_state" text,
    "contact_8_zipcode" text,
    "contact_9_type" text,
    "contact_9_name" text,
    "contact_9_city" text,
    "contact_9_state" text,
    "contact_9_zipcode" text,
    "contact_10_type" text,
    "contact_10_name" text,
    "contact_10_city" text,
    "contact_10_state" text,
    "contact_10_zipcode" text,
    "contact_11_type" text,
    "contact_11_name" text,
    "contact_11_city" text,
    "contact_11_state" text,
    "contact_11_zipcode" text,
    "contact_12_type" text,
    "contact_12_name" text,
    "contact_12_city" text,
    "contact_12_state" text,
    "contact_12_zipcode" text,
    "contact_13_type" text,
    "contact_13_name" text,
    "contact_13_city" text,
    "contact_13_state" text,
    "contact_13_zipcode" text,
    "contact_14_type" text,
    "contact_14_name" text,
    "contact_14_city" text,
    "contact_14_state" text,
    "contact_14_zipcode" text,
    "contact_15_type" text,
    "contact_15_name" text,
    "contact_15_city" text,
    "contact_15_state" text,
    "contact_15_zipcode" text,
    "reported_cost" numeric,
    "pin_list" text,
    "community_area" numeric,
    "census_tract" numeric,
    "ward" numeric,
    "xcoordinate" numeric,
    "ycoordinate" numeric,
    "latitude" numeric,
    "longitude" numeric,
    "location" geometry(Point, 4326),
    ":@computed_region_rpca_8um6" numeric,
    ":@computed_region_vrxf_vc4k" numeric,
    ":@computed_region_6mkv_f3dw" numeric,
    ":@computed_region_bdys_3d7i" numeric,
    ":@computed_region_43wa_7qmu" numeric,
    ":@computed_region_awaf_s7ux" numeric,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC')
);

comment on column raw_data.chicago_building_permits."id" is 'Unique database record identifier';
comment on column raw_data.chicago_building_permits."permit_" is 'Tracking number assigned at beginning of permit application process';
comment on column raw_data.chicago_building_permits."permit_status" is 'Current status of permit (not available for all permit types).';
comment on column raw_data.chicago_building_permits."permit_milestone" is 'Current milestone of permit (not available for all permit types).';
comment on column raw_data.chicago_building_permits."permit_type" is 'Type of permit';
comment on column raw_data.chicago_building_permits."review_type" is 'Process used to review permit application';
comment on column raw_data.chicago_building_permits."application_start_date" is 'Date when City began reviewing permit application';
comment on column raw_data.chicago_building_permits."issue_date" is 'For most permit types, date when City determined permit was ready to issue, subject to payment of permit fees; For Express Permit Program, date when permit issued based on full payment of applicable permit fee';
comment on column raw_data.chicago_building_permits."processing_time" is 'Number of days between APPLICATION_START_DATE and ISSUE_DATE';
comment on column raw_data.chicago_building_permits."street_number" is 'Address number - for buildings with address ranges, primary address is used';
comment on column raw_data.chicago_building_permits."street_direction" is 'Street direction';
comment on column raw_data.chicago_building_permits."street_name" is 'Street name';
comment on column raw_data.chicago_building_permits."work_type" is 'Classification of activity authorized by permit (not available for all permit types).';
comment on column raw_data.chicago_building_permits."work_description" is 'Description of work authorized by the permit';
comment on column raw_data.chicago_building_permits."permit_condition" is 'Conditions or comments added by reviewing department(s)';
comment on column raw_data.chicago_building_permits."building_fee_paid" is 'Amount of building permit fees received';
comment on column raw_data.chicago_building_permits."zoning_fee_paid" is 'Amount of zoning review fees received';
comment on column raw_data.chicago_building_permits."other_fee_paid" is 'Amount of other permit-related fees received';
comment on column raw_data.chicago_building_permits."subtotal_paid" is 'Sum of BUILDING_FEE_PAID, ZONING_FEE_PAID and OTHER_FEE_PAID';
comment on column raw_data.chicago_building_permits."building_fee_unpaid" is 'Amount of building permit fees due';
comment on column raw_data.chicago_building_permits."zoning_fee_unpaid" is 'Amount of zoning review fees due';
comment on column raw_data.chicago_building_permits."other_fee_unpaid" is 'Amount of other permit-related fees due';
comment on column raw_data.chicago_building_permits."subtotal_unpaid" is 'Sum of BUILDING_FEE_UNPAID, ZONING_FEE_UNPAID and OTHER_FEE_UNPAID';
comment on column raw_data.chicago_building_permits."building_fee_waived" is 'Amount of building permit fees waived';
comment on column raw_data.chicago_building_permits."building_fee_subtotal" is 'Sum of BUILDING_FEE_PAID, BUILDING_FEE_UNPAID and BUILDING_FEE_WAIVED';
comment on column raw_data.chicago_building_permits."zoning_fee_subtotal" is 'Sum of ZONING_FEE_PAID, ZONING _FEE_UNPAID and ZONING _FEE_WAIVED';
comment on column raw_data.chicago_building_permits."other_fee_subtotal" is 'Sum of OTHER_FEE_PAID, OTHER _FEE_UNPAID and OTHER _FEE_WAIVED';
comment on column raw_data.chicago_building_permits."zoning_fee_waived" is 'Amount of zoning review fees waived';
comment on column raw_data.chicago_building_permits."other_fee_waived" is 'Amount of other permit-related fees waived';
comment on column raw_data.chicago_building_permits."subtotal_waived" is 'Sum of BUILDING_FEE_WAIVED, ZONING_FEE_WAIVED and OTHER_FEE_WAIVED';
comment on column raw_data.chicago_building_permits."total_fee" is 'Sum of SUBTOTAL_PAID, SUBTOTAL_UNPAID and SUBTOTAL_WAIVED';
comment on column raw_data.chicago_building_permits."contact_1_type" is 'Contact 1''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_1_name" is 'Contact 1''s full name';
comment on column raw_data.chicago_building_permits."contact_1_city" is 'Contact 1''s primary address city';
comment on column raw_data.chicago_building_permits."contact_1_state" is 'Contact 1''s primary address state';
comment on column raw_data.chicago_building_permits."contact_1_zipcode" is 'Contact 1''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_2_type" is 'Contact 2''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_2_name" is 'Contact 2''s full name';
comment on column raw_data.chicago_building_permits."contact_2_city" is 'Contact 2''s primary address city';
comment on column raw_data.chicago_building_permits."contact_2_state" is 'Contact 2''s primary address state';
comment on column raw_data.chicago_building_permits."contact_2_zipcode" is 'Contact 2''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_3_type" is 'Contact 3''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_3_name" is 'Contact 3''s full name';
comment on column raw_data.chicago_building_permits."contact_3_city" is 'Contact 3''s primary address city';
comment on column raw_data.chicago_building_permits."contact_3_state" is 'Contact 3''s primary address state';
comment on column raw_data.chicago_building_permits."contact_3_zipcode" is 'Contact 3''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_4_type" is 'Contact 4''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_4_name" is 'Contact 4''s full name';
comment on column raw_data.chicago_building_permits."contact_4_city" is 'Contact 4''s primary address city';
comment on column raw_data.chicago_building_permits."contact_4_state" is 'Contact 4''s primary address state';
comment on column raw_data.chicago_building_permits."contact_4_zipcode" is 'Contact 4''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_5_type" is 'Contact 5''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_5_name" is 'Contact 5''s full name';
comment on column raw_data.chicago_building_permits."contact_5_city" is 'Contact 5''s primary address city';
comment on column raw_data.chicago_building_permits."contact_5_state" is 'Contact 5''s primary address state';
comment on column raw_data.chicago_building_permits."contact_5_zipcode" is 'Contact 5''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_6_type" is 'Contact 6''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_6_name" is 'Contact 6''s full name';
comment on column raw_data.chicago_building_permits."contact_6_city" is 'Contact 6''s primary address city';
comment on column raw_data.chicago_building_permits."contact_6_state" is 'Contact 6''s primary address state';
comment on column raw_data.chicago_building_permits."contact_6_zipcode" is 'Contact 6''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_7_type" is 'Contact 7''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_7_name" is 'Contact 7''s full name';
comment on column raw_data.chicago_building_permits."contact_7_city" is 'Contact 7''s primary address city';
comment on column raw_data.chicago_building_permits."contact_7_state" is 'Contact 7''s primary address state';
comment on column raw_data.chicago_building_permits."contact_7_zipcode" is 'Contact 7''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_8_type" is 'Contact 8''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_8_name" is 'Contact 8''s full name';
comment on column raw_data.chicago_building_permits."contact_8_city" is 'Contact 8''s primary address city';
comment on column raw_data.chicago_building_permits."contact_8_state" is 'Contact 8''s primary address state';
comment on column raw_data.chicago_building_permits."contact_8_zipcode" is 'Contact 8''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_9_type" is 'Contact 9''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_9_name" is 'Contact 9''s full name';
comment on column raw_data.chicago_building_permits."contact_9_city" is 'Contact 9''s primary address city';
comment on column raw_data.chicago_building_permits."contact_9_state" is 'Contact 9''s primary address state';
comment on column raw_data.chicago_building_permits."contact_9_zipcode" is 'Contact 9''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_10_type" is 'Contact 10''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_10_name" is 'Contact 10''s full name';
comment on column raw_data.chicago_building_permits."contact_10_city" is 'Contact 10''s primary address city';
comment on column raw_data.chicago_building_permits."contact_10_state" is 'Contact 10''s primary address state';
comment on column raw_data.chicago_building_permits."contact_10_zipcode" is 'Contact 10''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_11_type" is 'Contact 11''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_11_name" is 'Contact 11''s full name';
comment on column raw_data.chicago_building_permits."contact_11_city" is 'Contact 11''s primary address city';
comment on column raw_data.chicago_building_permits."contact_11_state" is 'Contact 11''s primary address state';
comment on column raw_data.chicago_building_permits."contact_11_zipcode" is 'Contact 11''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_12_type" is 'Contact 12''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_12_name" is 'Contact 12''s full name';
comment on column raw_data.chicago_building_permits."contact_12_city" is 'Contact 12''s primary address city';
comment on column raw_data.chicago_building_permits."contact_12_state" is 'Contact 12''s primary address state';
comment on column raw_data.chicago_building_permits."contact_12_zipcode" is 'Contact 12''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_13_type" is 'Contact 13''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_13_name" is 'Contact 13''s full name';
comment on column raw_data.chicago_building_permits."contact_13_city" is 'Contact 13''s primary address city';
comment on column raw_data.chicago_building_permits."contact_13_state" is 'Contact 13''s primary address state';
comment on column raw_data.chicago_building_permits."contact_13_zipcode" is 'Contact 13''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_14_type" is 'Contact 14''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_14_name" is 'Contact 14''s full name';
comment on column raw_data.chicago_building_permits."contact_14_city" is 'Contact 14''s primary address city';
comment on column raw_data.chicago_building_permits."contact_14_state" is 'Contact 14''s primary address state';
comment on column raw_data.chicago_building_permits."contact_14_zipcode" is 'Contact 14''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."contact_15_type" is 'Contact 15''s relationship to permit';
comment on column raw_data.chicago_building_permits."contact_15_name" is 'Contact 15''s full name';
comment on column raw_data.chicago_building_permits."contact_15_city" is 'Contact 15''s primary address city';
comment on column raw_data.chicago_building_permits."contact_15_state" is 'Contact 15''s primary address state';
comment on column raw_data.chicago_building_permits."contact_15_zipcode" is 'Contact 15''s primary address ZIP code';
comment on column raw_data.chicago_building_permits."reported_cost" is 'Estimated cost of work provided by applicant (not available for all permits)';
comment on column raw_data.chicago_building_permits."pin_list" is 'Property index number(s) (PINs) disclosed by applicant and verified by city staff as part of permit application review (not available for all permits); PINs are assigned to parcels of land for property tax purposes. This column replaces columns PIN1 through PIN10, which will be removed soon, no earlier than 6/1/2024.';
comment on column raw_data.chicago_building_permits."community_area" is 'The Chicago community area of the property''s primary address. The community area map is available in a separate dataset.';
comment on column raw_data.chicago_building_permits."census_tract" is 'The census tract of the property''s primary address. The census tract map is available in a separate dataset.';
comment on column raw_data.chicago_building_permits."ward" is 'The ward (city council district) of the property''s primary address at the time of permit issuance. The ward map is available in a separate dataset.';
comment on column raw_data.chicago_building_permits."xcoordinate" is 'The east-west coordinate of the property''s primary address, defined in feet, based on NAD 83 - State Plane Eastern IL';
comment on column raw_data.chicago_building_permits."ycoordinate" is 'The north-south coordinate of the property''s primary address, defined in feet, based on NAD 83 - State Plane Eastern IL';
comment on column raw_data.chicago_building_permits."latitude" is 'The latitude of the property''s primary address';
comment on column raw_data.chicago_building_permits."longitude" is 'The longitude of the property''s primary address';
comment on column raw_data.chicago_building_permits."location" is 'The location of the property''s primary address in a format that allows for creation of maps and other geographic operations on this data portal';
