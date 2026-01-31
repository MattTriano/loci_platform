alter table raw_data.trading_partnership
drop constraint trading_partnership_business1_id_fkey;

alter table raw_data.trading_partnership
drop constraint trading_partnership_business2_id_fkey;

alter table raw_data.trading_partnership
drop constraint trading_partnership_pkey;

alter table raw_data.businesses
drop constraint businesses_address_id_fkey;

alter table raw_data.businesses drop constraint businesses_pkey;

alter table raw_data.addresses drop constraint addresses_pkey;
