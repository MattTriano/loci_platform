create schema if not exists marts;

create table if not exists marts.addresses (
    address_id bigint primary key,
    street_address varchar(255),
    city varchar(100),
    state varchar(64),
    postal_code varchar(16),
    country varchar(64),
    created_at timestamptz,
    updated_at timestamptz,
    ingested_to_postgres timestamptz not null default current_timestamp
);

create table if not exists marts.businesses (
    business_id bigint primary key,
    name varchar(64),
    legal_name varchar(64),
    tax_id varchar(16),
    industry varchar(32),
    description varchar(64),
    email varchar(64),
    phone varchar(32),
    website varchar(64),
    employee_count bigint,
    annual_revenue bigint,
    address_id bigint,
    is_active boolean,
    created_at timestamptz,
    updated_at timestamptz,
    ingested_to_postgres timestamptz not null default current_timestamp,
    foreign key (address_id) references marts.addresses(address_id)
);

create table if not exists marts.trading_partnership (
    partnership_id bigint primary key,
    business1_id bigint not null,
    business2_id bigint not null,
    is_active boolean,
    partnership_type varchar(32),
    start_date timestamp,
    end_date timestamp,
    contract_value bigint,
    notes varchar(255),
    created_at timestamptz,
    updated_at timestamptz,
    ingested_to_postgres timestamptz not null default current_timestamp,
    foreign key (business1_id) references marts.businesses(business_id),
    foreign key (business2_id) references marts.businesses(business_id)
);
