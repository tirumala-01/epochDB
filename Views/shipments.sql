CREATE MATERIALIZED VIEW DeliveryTimesByRoute AS
select
    s.shipment_route_id as route_id,
    s.shipment_origin as origin,
    s.shipment_destination as destination,
    ROUND(AVG(s.shipment_delivery_time), 2) as avg_delivery_time,
    MAX(s.shipment_delivery_time) as max_delivery_time,
    MIN(s.shipment_delivery_time) as min_delivery_time,
    SUM(s.shipment_delivery_time) as total_delivery_time,
    COUNT(*) as total_trips
from
    past_shipments s
group by
    s.shipment_route_id,
    s.shipment_origin,
    s.shipment_destination;

create index idx_DeliveryTimesByRoute on DeliveryTimesByRoute (route_id);

CREATE MATERIALIZED VIEW CostByRoute AS
select
    s.shipment_route_id as route_id,
    s.shipment_origin as origin,
    s.shipment_destination as destination,
    ROUND(AVG(s.shipment_cost), 2) as avg_shipment_cost,
    MAX(s.shipment_cost) as max_shipment_cost,
    MIN(s.shipment_cost) as min_shipment_cost,
    SUM(s.shipment_cost) as total_shipment_cost,
    COUNT(*) as total_trips
from
    past_shipments s
group by
    s.shipment_route_id,
    s.shipment_origin,
    s.shipment_destination;

create index idx_CostByRoute on CostByRoute (route_id);

CREATE MATERIALIZED VIEW HighestShipmentCost AS
select
    c.route_id,
    c.origin,
    c.destination,
    c.total_shipment_cost
from
    CostByRoute c
order by
    c.total_shipment_cost desc
limit
    5;

CREATE MATERIALIZED VIEW LowestShipmentCost AS
select
    c.route_id,
    c.origin,
    c.destination,
    c.total_shipment_cost
from
    CostByRoute c
order by
    c.total_shipment_cost asc
limit
    5;

CREATE MATERIALIZED VIEW DestinationCities AS
select
    DISTINCT s.shipment_destination
from
    past_shipments s WITH DATA;

CREATE MATERIALIZED VIEW origincities AS
SELECT
    DISTINCT s.shipment_origin
FROM
    past_shipments s;