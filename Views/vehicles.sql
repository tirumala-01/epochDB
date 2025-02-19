CREATE MATERIALIZED VIEW FuelEfficiencyByVehicle AS
select
	pvl.vl_vehicle_id as vehicle_id,
	pvl.vl_vehicle_full_id as vehicle_full_id,
    v.vehicle_name as vehicle_name,
	ROUND(AVG(pvl.vl_mileage / pvl.vl_fuel_used ),2) as avg_mileage_per_liter,
	ROUND(MAX(pvl.vl_mileage / pvl.vl_fuel_used ),2) as max_mileage_per_liter,
	ROUND(MIN(pvl.vl_mileage / pvl.vl_fuel_used ),2) as min_mileage_per_liter,
	COUNT(*) as total_trips
from
	past_vehicles_logs pvl
join vehicles v on v.vehicle_id = pvl.vl_vehicle_id
group by
	pvl.vl_vehicle_id,
	pvl.vl_vehicle_full_id,
    v.vehicle_name;

create index idx_FuelEfficiencyByVehicle on
FuelEfficiencyByVehicle (vehicle_id);


CREATE MATERIALIZED VIEW PastTripsByVehicle AS
select
	pvl.vl_vehicle_id as vehicle_id,
    pvl.vl_vehicle_full_id as vehicle_full_id,
    v.vehicle_name as vehicle_name,
	SUM(pvl.vl_mileage) as total_mileage,
	SUM(pvl.vl_fuel_used) as total_fuel_used,
	COUNT(*) as total_trips
from
	past_vehicles_logs pvl
join vehicles v on v.vehicle_id = pvl.vl_vehicle_id
group by
	pvl.vl_vehicle_id,
	pvl.vl_vehicle_full_id,
    v.vehicle_name;


create index idx_PastTripsByVehicle on
PastTripsByVehicle (vehicle_id);