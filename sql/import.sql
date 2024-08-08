SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '4g'; 
SET CLUSTER SETTING server.time_until_store_dead = '1m15s';

CREATE TABLE movr_demo.public.vehicle_location_histories (
    id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id UUID NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    city VARCHAR NULL,
    lat FLOAT8 NULL,
    long FLOAT8 NULL,
    INDEX hist_ride_ts_idx (ride_id ASC, "timestamp" ASC)
) LOCALITY REGIONAL BY ROW;
;
    -- CONSTRAINT fk_city_ref_rides FOREIGN KEY (city, ride_id) REFERENCES public.rides(city, id),
    -- FAMILY "primary" (city, ride_id, "timestamp", lat, long)


CREATE TABLE movr_demo.public.users
(
    id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    city STRING NOT NULL,
    name STRING,
    address STRING,
    credit_card STRING
)
;

IMPORT INTO movr_demo.public.users
CSV DATA (
    'workload:///csv/movr/users?infer-crdb-region-column=true&multi-region=false&num-histories=1000&num-promo-codes=1000&num-ranges=9&num-rides=100000&num-users=1000&num-vehicles=5000&row-end=1000&row-start=0&seed=1&survive=az&version=1.0.0'
) WITH "nullif" = 'NULL'
;

ALTER TABLE movr_demo.public.users SET LOCALITY GLOBAL;

CREATE TABLE movr_demo.public.vehicles
(
    id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    city STRING,
    type STRING,
    owner_id UUID,
    creation_time timestamp,
    status STRING,
    current_location STRING,
    ext JSONB
)
;

IMPORT INTO movr_demo.public.vehicles
CSV DATA (
    'workload:///csv/movr/vehicles?infer-crdb-region-column=true&multi-region=false&num-histories=1000&num-promo-codes=1000&num-ranges=9&num-rides=100000&num-users=1000&num-vehicles=5000&row-end=5000&row-start=0&seed=1&survive=az&version=1.0.0'
) WITH "nullif" = 'NULL'
;

ALTER TABLE movr_demo.public.vehicles SET LOCALITY GLOBAL;


CREATE TABLE movr_demo.public.rides
(
    id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    city STRING,
    vehicle_city STRING,
    rider_id uuid,
    vehicle_id uuid,
    start_address STRING,
    end_address STRING,
    start_time timestamp,
    end_time timestamp,
    revenue DECIMAL
)
;

IMPORT INTO movr_demo.public.rides
CSV DATA (
    'workload:///csv/movr/rides?infer-crdb-region-column=true&multi-region=false&num-histories=1000&num-promo-codes=1000&num-ranges=9&num-rides=100000&num-users=1000&num-vehicles=5000&row-end=100000&row-start=0&seed=1&survive=az&version=1.0.0'
) WITH "nullif" = 'NULL'
;

ALTER TABLE movr_demo.public.rides SET LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;