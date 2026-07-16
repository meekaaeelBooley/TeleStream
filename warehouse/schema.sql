-- TeleStream warehouse: star schema per docs/data-model.md.
-- Applied automatically by the postgres container on first init.

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

CREATE TABLE dim_subscriber (
    subscriber_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id    bigint NOT NULL UNIQUE,
    msisdn           varchar(11) NOT NULL UNIQUE,
    plan             varchar(16) NOT NULL,
    province         varchar(32) NOT NULL,
    created_at       timestamptz NOT NULL,
    is_active        boolean NOT NULL DEFAULT true
);

CREATE TABLE dim_tower (
    tower_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tower_id         varchar(16) NOT NULL UNIQUE,
    tower_name       varchar(64) NOT NULL,
    province         varchar(32) NOT NULL,
    technologies     varchar(16)[] NOT NULL
);

CREATE TABLE dim_bundle (
    bundle_key       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bundle_code      varchar(32) NOT NULL UNIQUE,
    bundle_name      varchar(64) NOT NULL,
    bundle_type      varchar(16) NOT NULL,
    price            numeric(10,2) NOT NULL
);

CREATE TABLE dim_date (
    date_key         int PRIMARY KEY,          -- YYYYMMDD
    full_date        date NOT NULL UNIQUE,
    day_of_week      smallint NOT NULL,
    month            smallint NOT NULL,
    quarter          smallint NOT NULL,
    year             smallint NOT NULL,
    is_weekend       boolean NOT NULL
);

-- ---------------------------------------------------------------------------
-- Facts (event_id unique => idempotent loads)
-- ---------------------------------------------------------------------------

CREATE TABLE fact_recharges (
    recharge_key     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    subscriber_key   bigint NOT NULL REFERENCES dim_subscriber (subscriber_key),
    date_key         int NOT NULL REFERENCES dim_date (date_key),
    event_timestamp  timestamptz NOT NULL,
    amount           numeric(10,2) NOT NULL,
    payment_method   varchar(16) NOT NULL
);
CREATE INDEX idx_fact_recharges_ts ON fact_recharges (event_timestamp);

CREATE TABLE fact_bundle_sales (
    sale_key         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    subscriber_key   bigint NOT NULL REFERENCES dim_subscriber (subscriber_key),
    bundle_key       bigint NOT NULL REFERENCES dim_bundle (bundle_key),
    date_key         int NOT NULL REFERENCES dim_date (date_key),
    event_timestamp  timestamptz NOT NULL,
    price            numeric(10,2) NOT NULL
);
CREATE INDEX idx_fact_bundle_sales_ts ON fact_bundle_sales (event_timestamp);

CREATE TABLE fact_calls (
    call_key         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    caller_key       bigint NOT NULL REFERENCES dim_subscriber (subscriber_key),
    tower_key        bigint NOT NULL REFERENCES dim_tower (tower_key),
    date_key         int NOT NULL REFERENCES dim_date (date_key),
    event_timestamp  timestamptz NOT NULL,
    receiver_msisdn  varchar(11) NOT NULL,
    duration_seconds int NOT NULL,
    dropped          boolean NOT NULL
);
CREATE INDEX idx_fact_calls_ts ON fact_calls (event_timestamp);

CREATE TABLE fact_sms (
    sms_key          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    sender_key       bigint NOT NULL REFERENCES dim_subscriber (subscriber_key),
    date_key         int NOT NULL REFERENCES dim_date (date_key),
    event_timestamp  timestamptz NOT NULL,
    receiver_msisdn  varchar(11) NOT NULL,
    length           int NOT NULL
);
CREATE INDEX idx_fact_sms_ts ON fact_sms (event_timestamp);

CREATE TABLE fact_data_usage (
    usage_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    subscriber_key   bigint NOT NULL REFERENCES dim_subscriber (subscriber_key),
    tower_key        bigint NOT NULL REFERENCES dim_tower (tower_key),
    date_key         int NOT NULL REFERENCES dim_date (date_key),
    event_timestamp  timestamptz NOT NULL,
    mb_used          numeric(12,2) NOT NULL,
    session_seconds  int NOT NULL,
    technology       varchar(8) NOT NULL
);
CREATE INDEX idx_fact_data_usage_ts ON fact_data_usage (event_timestamp);

-- ---------------------------------------------------------------------------
-- Operational tables (outside the star)
-- ---------------------------------------------------------------------------

CREATE TABLE dlq_records (
    dlq_key          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    source_topic     varchar(64) NOT NULL,
    rejection_reason varchar(128) NOT NULL,
    original_payload jsonb NOT NULL,
    rejected_at      timestamptz NOT NULL
);
CREATE INDEX idx_dlq_records_ts ON dlq_records (rejected_at);

CREATE TABLE tower_status_current (
    tower_id          varchar(16) PRIMARY KEY REFERENCES dim_tower (tower_id),
    technology        varchar(8) NOT NULL,
    signal_strength   int NOT NULL,
    connected_devices int NOT NULL,
    status            varchar(16) NOT NULL,
    updated_at        timestamptz NOT NULL
);

-- ---------------------------------------------------------------------------
-- Minute-level rollups (recomputed from facts per affected minute => idempotent)
-- ---------------------------------------------------------------------------

CREATE TABLE agg_revenue_minute (
    minute        timestamptz NOT NULL,
    province      varchar(32) NOT NULL,
    revenue_type  varchar(16) NOT NULL,      -- AIRTIME | BUNDLE
    total_amount  numeric(14,2) NOT NULL,
    txn_count     bigint NOT NULL,
    PRIMARY KEY (minute, province, revenue_type)
);

CREATE TABLE agg_calls_minute (
    minute          timestamptz NOT NULL,
    tower_key       bigint NOT NULL REFERENCES dim_tower (tower_key),
    call_count      bigint NOT NULL,
    dropped_count   bigint NOT NULL,
    total_duration  bigint NOT NULL,
    PRIMARY KEY (minute, tower_key)
);

CREATE TABLE agg_data_minute (
    minute         timestamptz NOT NULL,
    tower_key      bigint NOT NULL REFERENCES dim_tower (tower_key),
    technology     varchar(8) NOT NULL,
    total_mb       numeric(16,2) NOT NULL,
    session_count  bigint NOT NULL,
    PRIMARY KEY (minute, tower_key, technology)
);

CREATE TABLE agg_dlq_minute (
    minute           timestamptz NOT NULL,
    source_topic     varchar(64) NOT NULL,
    rejection_reason varchar(128) NOT NULL,
    record_count     bigint NOT NULL,
    PRIMARY KEY (minute, source_topic, rejection_reason)
);

-- ---------------------------------------------------------------------------
-- dim_date population: 2026 through 2027
-- ---------------------------------------------------------------------------

INSERT INTO dim_date (date_key, full_date, day_of_week, month, quarter, year, is_weekend)
SELECT
    (extract(year from d) * 10000 + extract(month from d) * 100 + extract(day from d))::int,
    d::date,
    extract(isodow from d)::smallint,
    extract(month from d)::smallint,
    extract(quarter from d)::smallint,
    extract(year from d)::smallint,
    extract(isodow from d) IN (6, 7)
FROM generate_series('2026-01-01'::date, '2027-12-31'::date, interval '1 day') AS d;
