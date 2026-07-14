# Event Schemas & Kafka Topic Map

Canonical contracts for every event in TeleStream. Producers, the Spark job, Pandera
schemas, and tests must all agree with this document. If code diverges, either fix the
code or update this doc in the same commit.

## Common Envelope

Every event carries these fields in addition to its domain payload:

| Field | Type | Rules |
|---|---|---|
| `event_id` | string (UUIDv4) | Globally unique; dedupe key |
| `event_type` | string | Matches the topic's event type |
| `timestamp` | string (ISO-8601, UTC) | Not in the future beyond 30s skew allowance |
| `schema_version` | int | Starts at 1; bump on breaking change |

Conventions: MSISDNs are synthetic South African numbers matching `27\d{9}`; monetary
amounts are ZAR with 2 decimal places; provinces come from the fixed list of 9 SA
provinces; towers come from the seeded tower registry.

## Topic Map

| Topic | Event type | Key | Approx. share of volume |
|---|---|---|---|
| `subscriber-created` | subscriber_created | `subscriber_id` | ~1% |
| `airtime-purchases` | airtime_purchase | `subscriber_id` | ~10% |
| `bundle-purchases` | bundle_purchase | `subscriber_id` | ~8% |
| `voice-calls` | voice_call | `caller_id` | ~25% |
| `sms` | sms | `sender_id` | ~15% |
| `data-usage` | data_usage | `subscriber_id` | ~30% |
| `tower-events` | tower_status | `tower_id` | ~6% |
| `failed-transactions` | (varies) | source key | DLQ + simulated payment failures |

## Event Contracts

### subscriber_created — `subscriber-created`

```json
{
  "event_id": "9f1c...",
  "event_type": "subscriber_created",
  "timestamp": "2026-07-14T08:30:00Z",
  "schema_version": 1,
  "subscriber_id": 10238,
  "msisdn": "27821234567",
  "plan": "Prepaid",
  "province": "Western Cape"
}
```

Rules: `msisdn` matches `27\d{9}` and is unique; `plan` ∈ {Prepaid, Contract, TopUp};
`province` ∈ the 9 SA provinces.

### airtime_purchase — `airtime-purchases`

```json
{
  "event_id": "...",
  "event_type": "airtime_purchase",
  "timestamp": "...",
  "schema_version": 1,
  "subscriber_id": 10238,
  "amount": 50.00,
  "payment_method": "Voucher"
}
```

Rules: `amount` > 0 and ≤ 1000; `payment_method` ∈ {Voucher, Card, EFT, USSD};
subscriber must exist.

### bundle_purchase — `bundle-purchases`

```json
{
  "event_id": "...",
  "event_type": "bundle_purchase",
  "timestamp": "...",
  "schema_version": 1,
  "subscriber_id": 10238,
  "bundle_code": "DATA_5GB",
  "price": 199.00
}
```

Rules: `bundle_code` must exist in `dim_bundle`; `price` > 0 and matches the catalog
price for the bundle; subscriber must exist.

### voice_call — `voice-calls`

```json
{
  "event_id": "...",
  "event_type": "voice_call",
  "timestamp": "...",
  "schema_version": 1,
  "caller_id": 10238,
  "receiver_msisdn": "27831112222",
  "duration_seconds": 212,
  "tower_id": "CPT-CBD-001",
  "dropped": false
}
```

Rules: `duration_seconds` ≥ 0 and ≤ 21600 (6h); caller must exist; tower must exist.
`dropped: true` with low duration feeds the dropped-calls KPI.

### sms — `sms`

```json
{
  "event_id": "...",
  "event_type": "sms",
  "timestamp": "...",
  "schema_version": 1,
  "sender_id": 10238,
  "receiver_msisdn": "27835550000",
  "length": 142
}
```

Rules: `length` between 1 and 1600 (concatenated SMS cap); sender must exist.

### data_usage — `data-usage`

```json
{
  "event_id": "...",
  "event_type": "data_usage",
  "timestamp": "...",
  "schema_version": 1,
  "subscriber_id": 10238,
  "mb_used": 315.4,
  "session_seconds": 1800,
  "tower_id": "CPT-BLV-003",
  "technology": "LTE"
}
```

Rules: `mb_used` > 0 and ≤ 10240 per session; `technology` ∈ {3G, LTE, 5G}; subscriber
and tower must exist.

### tower_status — `tower-events`

```json
{
  "event_id": "...",
  "event_type": "tower_status",
  "timestamp": "...",
  "schema_version": 1,
  "tower_id": "CPT-CC-002",
  "technology": "5G",
  "signal_strength": 87,
  "connected_devices": 1342,
  "status": "HEALTHY"
}
```

Rules: `signal_strength` 0–100; `status` ∈ {HEALTHY, DEGRADED, DOWN}; tower must exist.

### Dead letter record — `failed-transactions`

Written by the Spark job for any parse/schema/rule failure, and by generators for
simulated payment failures:

```json
{
  "event_id": "...",
  "event_type": "dlq_record",
  "timestamp": "...",
  "schema_version": 1,
  "source_topic": "airtime-purchases",
  "rejection_reason": "RULE_VIOLATION:amount_must_be_positive",
  "original_payload": "{...raw json string...}"
}
```

`rejection_reason` is machine-parseable: `PARSE_ERROR`, `SCHEMA_VIOLATION:<field>`, or
`RULE_VIOLATION:<rule_name>` — this powers the DLQ review dashboard's breakdown-by-reason
panel.

## Schema Evolution Policy

- Additive optional fields: allowed without a version bump.
- Renames, removals, type changes: bump `schema_version`; the Spark job must accept the
  previous version for at least one release.
- Stretch goal: move enforcement to Schema Registry + Avro (see architecture §9).
