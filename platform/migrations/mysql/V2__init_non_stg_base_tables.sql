alter table loci.businesses_stg
drop foreign key fk_businesses_address;

alter table loci.businesses_stg
add constraint fk_business_stg__address_id
    foreign key (address_id)
    references addresses_stg(address_id);


alter table loci.trading_partnership_stg
drop foreign key fk_partnership_business1;

alter table loci.trading_partnership_stg
add constraint fk_trading_partnership__business1_id
    foreign key (business1_id)
    references businesses_stg(business_id);


alter table loci.trading_partnership_stg
drop foreign key fk_partnership_business2;

alter table loci.trading_partnership_stg
add constraint fk_trading_partnership__business2_id
    foreign key (business2_id)
    references businesses_stg(business_id);


create table if not exists addresses (
    address_id bigint primary key,
    street_address varchar(255),
    city varchar(100),
    state varchar(64),
    postal_code varchar(16),
    country varchar(64),
    created_at timestamp null,
    updated_at timestamp null,
    ingested_to_mysql timestamp not null default current_timestamp
);

create table if not exists businesses (
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
    created_at timestamp null,
    updated_at timestamp null,
    ingested_to_mysql timestamp not null default current_timestamp,
    constraint fk_businesses_address
        foreign key (address_id)
        references addresses(address_id)
);

create table if not exists trading_partnership (
    partnership_id bigint primary key,
    business1_id bigint not null,
    business2_id bigint not null,
    is_active boolean,
    partnership_type varchar(32),
    start_date timestamp null,
    end_date timestamp null,
    contract_value bigint,
    notes varchar(255),
    created_at timestamp null,
    updated_at timestamp null,
    ingested_to_mysql timestamp not null default current_timestamp,
    constraint fk_partnership_business1
        foreign key (business1_id)
        references businesses(business_id),
    constraint fk_partnership_business2
        foreign key (business2_id)
        references businesses(business_id)
);
