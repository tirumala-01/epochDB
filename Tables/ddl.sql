create table vehicles (
	vehicle_id integer primary key,
	vehicle_full_id text not null,
	vehicle_name text not null,
	vehicle_total_mileage numeric not null
);

create table vehicles_logs (
	vl_id integer primary key,
	vl_full_id text not null,
	vl_vehicle_id integer references vehicles(vehicle_id),
	vl_vehicle_full_id text not null,
	vl_trip_date date not null,
	vl_mileage numeric null,
	vl_fuel_used numeric null
);

create table past_vehicles_logs (
	vl_id integer primary key,
	vl_full_id text not null,
	vl_vehicle_id integer references vehicles(vehicle_id),
	vl_vehicle_full_id text not null,
	vl_trip_date date not null,
	vl_mileage numeric not null,
	vl_fuel_used numeric not null
);

create index idx_vlogs_vehicle_id on
vehicles_logs(vl_vehicle_id);

create index idx_pvl_vehicle_id on
past_vehicles_logs(vl_vehicle_id);

create table shipments (
	shipment_id integer primary key,
	shipment_full_id text not null,
	shipment_origin text not null,
	shipment_destination text not null,
	shipment_weight numeric not null,
	shipment_cost numeric not null,
	shipment_delivery_time int4 not null,
	shipment_log_id integer references vehicles_logs(vl_id),
	shipment_full_log_id text not null
);

create index idx_shipment_log_id on
shipments(shipment_log_id);

create table past_shipments (
	shipment_id integer primary key,
	shipment_full_id text not null,
	shipment_origin text not null,
	shipment_destination text not null,
	shipment_weight numeric not null,
	shipment_cost numeric not null,
	shipment_delivery_time int4 not null,
	shipment_log_id integer references vehicles_logs(vl_id),
	shipment_full_log_id text not null,
	shipment_route_id text generated always as (shipment_origin || '-' || shipment_destination) stored
);

insert into past_shipments
select s.*
from shipments s
inner join past_vehicles_logs pvl on
s.shipment_log_id = pvl.vl_id;

create index idx_pastshipment_log_id on
past_shipments(shipment_log_id);

create index idx_pastshipment_route_id on
past_shipments (shipment_route_id);