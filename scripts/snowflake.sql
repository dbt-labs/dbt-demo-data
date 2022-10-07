---------------------------------------------------------------------
-- CONFIGURATION:
--
-- Change the values below to configure where source data is loaded
--
-- NOTE: Do not change these variables if you intend to run this dbt
--       project in dbt Cloud. Support for custom source data locations
--       will be added in a future release
---------------------------------------------------------------------

set DEST_DATABASE_NAME='dbt_demo_data';
set DEST_SCHEMA_NAME='ecommerce';

---------------------------------------------------------------------
-- DO NOT CHANGE ANYTHING BELOW THIS LINE
---------------------------------------------------------------------

begin;

---------------------------------------------------------------------
-- Setup database, schema, and stage
---------------------------------------------------------------------

create database if not exists identifier($DEST_DATABASE_NAME);
use database identifier($DEST_DATABASE_NAME);

create schema if not exists identifier($DEST_SCHEMA_NAME);
use schema identifier($DEST_SCHEMA_NAME);

create or replace stage s3_stage
  url='s3://dbt-demo-data-2022/jaffle-shop'
  file_format = ( type = parquet );

---------------------------------------------------------------------
-- Customers
---------------------------------------------------------------------


create or replace external table customers (
  id string as  (value:id::string), 
  name string as ( value:name::string)
)
with location = @s3_stage/customers
file_format = ( type = parquet );

---------------------------------------------------------------------
-- Orders
---------------------------------------------------------------------

create or replace external table orders (
  id string as  (value:id::string), 
  location_id string as (value:store_id::string),
  customer_id string as (value:customer::string),
  ordered_at timestamp as (value:ordered_at::timestamp),
  order_total int as (value:order_total::int),
  tax_paid int as (value:tax_paid::int)
)
with location = @s3_stage/orders
file_format = ( type = parquet );

---------------------------------------------------------------------
-- Order items
---------------------------------------------------------------------

create or replace external table order_items (
  id string as  (value:id::string), 
  order_id string as  (value:order_id::string), 
  sku string as (value:sku::string)
)
with location = @s3_stage/order_items
file_format = ( type = parquet );

---------------------------------------------------------------------
-- Locations
---------------------------------------------------------------------

create or replace external table locations (
  id string as (value:id::string),
  name string as  (value:name::string), 
  opened_at timestamp as (value:opened_at::timestamp),
  tax_rate float as (value:tax_rate::float)
)
with location = @s3_stage/locations
file_format = ( type = parquet );

---------------------------------------------------------------------
-- Products
---------------------------------------------------------------------

create or replace external table products (
  sku string as  (value:sku::string), 
  name string as (value:name::string),
  type string as (value:type::string),
  description string as (value:description::string),
  price int as (value:price::int)
)
with location = @s3_stage/products
file_format = ( type = parquet );

---------------------------------------------------------------------
-- Supplies
---------------------------------------------------------------------

create or replace external table supplies (
  id string as  (value:id::string),
  name string as (value:name::string),
  cost int as (value:cost::int),
  perishable boolean as (value:perishable::boolean),
  sku string as (value:sku::string)
)
with location = @s3_stage/supplies
file_format = ( type = parquet );

---------------------------------------------------------------------
-- Commit the transaction
---------------------------------------------------------------------

select 'Loading complete!'

commit;
