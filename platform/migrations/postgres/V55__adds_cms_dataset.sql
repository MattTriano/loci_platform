create table raw_data.medicare_physicians_by_provider_and_service (
    "rndrng_npi" text,
    "rndrng_prvdr_last_org_name" text,
    "rndrng_prvdr_first_name" text,
    "rndrng_prvdr_mi" text,
    "rndrng_prvdr_crdntls" text,
    "rndrng_prvdr_ent_cd" text,
    "rndrng_prvdr_st1" text,
    "rndrng_prvdr_st2" text,
    "rndrng_prvdr_city" text,
    "rndrng_prvdr_state_abrvtn" text,
    "rndrng_prvdr_state_fips" text,
    "rndrng_prvdr_zip5" text,
    "rndrng_prvdr_ruca" text,
    "rndrng_prvdr_ruca_desc" text,
    "rndrng_prvdr_cntry" text,
    "rndrng_prvdr_type" text,
    "rndrng_prvdr_mdcr_prtcptg_ind" text,
    "hcpcs_cd" text,
    "hcpcs_desc" text,
    "hcpcs_drug_ind" text,
    "place_of_srvc" text,
    "tot_benes" text,
    "tot_srvcs" text,
    "tot_bene_day_srvcs" text,
    "avg_sbmtd_chrg" text,
    "avg_mdcr_alowd_amt" text,
    "avg_mdcr_pymt_amt" text,
    "avg_mdcr_stdzd_amt" text,
    "vintage" text not null,
    "_source_modified" text,
    "ingested_at" timestamptz not null default (now() at time zone 'UTC'),
    "record_hash" text not null,
    "valid_from" timestamptz not null default (now() at time zone 'UTC'),
    "valid_to" timestamptz
);
alter table raw_data.medicare_physicians_by_provider_and_service
    add constraint uq_medicare_physicians_by_provider_and_service_entity_hash
    unique ("rndrng_npi", "hcpcs_cd", "place_of_srvc", "vintage", "record_hash");
create index ix_medicare_physicians_by_provider_and_service_current
    on raw_data.medicare_physicians_by_provider_and_service ("rndrng_npi", "hcpcs_cd", "place_of_srvc", "vintage")
    where "valid_to" is null;
