create table if not exists raw_data.ahrq_chsp_health_systems (
    "health_sys_id" text,
    "health_sys_name" text,
    "health_sys_city" text,
    "health_sys_state" text,
    "in_onekey" text,
    "in_aha" text,
    "onekey_id" text,
    "aha_sysid" text,
    "total_mds" text,
    "prim_care_mds" text,
    "total_nps" text,
    "total_pas" text,
    "grp_cnt" text,
    "grp_cnt_restricted" text,
    "hosp_cnt" text,
    "acutehosp_cnt" text,
    "nh_cnt" text,
    "nh_cnt_restricted" text,
    "hhco_cnt" text,
    "hhco_cnt_restricted" text,
    "sys_multistate" text,
    "sys_beds" text,
    "sys_dsch" text,
    "sys_res" text,
    "deg_children" text,
    "sys_incl_majteachhosp" text,
    "sys_incl_vmajteachhosp" text,
    "sys_teachint" text,
    "sys_incl_highdpphosp" text,
    "sys_highucburden" text,
    "sys_incl_highuchosp" text,
    "sys_anyins_product" text,
    "sys_mcare_adv" text,
    "sys_mcaid_mngcare" text,
    "sys_healthins_mktplc" text,
    "sys_ma_plan_contracts" text,
    "sys_ma_plan_enroll" text,
    "sys_ownership" text,
    "hos_net_revenue" text,
    "hos_total_revenue" text,
    "vintage" text not null,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.ahrq_chsp_health_systems
    add constraint uq_ahrq_chsp_health_systems_entity_hash
    unique ("health_sys_id", "vintage", "record_hash");
create index if not exists ix_ahrq_chsp_health_systems_current
    on raw_data.ahrq_chsp_health_systems ("health_sys_id", "vintage")
    where valid_to is null;


create table if not exists raw_data.ahrq_chsp_hospital_linkage (
    "compendium_hospital_id" text,
    "ccn" text,
    "hospital_name" text,
    "hospital_street" text,
    "hospital_city" text,
    "hospital_state" text,
    "hospital_zip" text,
    "acutehosp_flag" text,
    "health_sys_id" text,
    "health_sys_name" text,
    "health_sys_city" text,
    "health_sys_state" text,
    "corp_parent_id" text,
    "corp_parent_name" text,
    "corp_parent_type" text,
    "hos_beds" text,
    "hos_dsch" text,
    "hos_res" text,
    "hos_children" text,
    "hos_majteach" text,
    "hos_vmajteach" text,
    "hos_teachint" text,
    "hos_highdpp" text,
    "hos_ucburden" text,
    "hos_highuc" text,
    "hos_ownership" text,
    "hos_net_revenue" text,
    "hos_total_revenue" text,
    "vintage" text not null,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.ahrq_chsp_hospital_linkage
    add constraint uq_ahrq_chsp_hospital_linkage_entity_hash
    unique ("ccn", "vintage", "record_hash");
create index if not exists ix_ahrq_chsp_hospital_linkage_current
    on raw_data.ahrq_chsp_hospital_linkage ("ccn", "vintage")
    where valid_to is null;

