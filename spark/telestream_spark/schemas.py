"""Field definitions per event type — the Spark-side mirror of
docs/event-schemas.md. Kept pyspark-free; the streaming job converts these to
StructTypes. A unit test asserts these stay in sync with the producer's
pandera contracts."""

ENVELOPE_FIELDS: dict[str, type] = {
    "event_id": str,
    "event_type": str,
    "timestamp": str,
    "schema_version": int,
}

EVENT_FIELDS: dict[str, dict[str, type]] = {
    "subscriber_created": {
        **ENVELOPE_FIELDS,
        "subscriber_id": int,
        "msisdn": str,
        "plan": str,
        "province": str,
    },
    "airtime_purchase": {
        **ENVELOPE_FIELDS,
        "subscriber_id": int,
        "amount": float,
        "payment_method": str,
    },
    "bundle_purchase": {
        **ENVELOPE_FIELDS,
        "subscriber_id": int,
        "bundle_code": str,
        "price": float,
    },
    "voice_call": {
        **ENVELOPE_FIELDS,
        "caller_id": int,
        "receiver_msisdn": str,
        "duration_seconds": int,
        "tower_id": str,
        "dropped": bool,
    },
    "sms": {
        **ENVELOPE_FIELDS,
        "sender_id": int,
        "receiver_msisdn": str,
        "length": int,
    },
    "data_usage": {
        **ENVELOPE_FIELDS,
        "subscriber_id": int,
        "mb_used": float,
        "session_seconds": int,
        "tower_id": str,
        "technology": str,
    },
    "tower_status": {
        **ENVELOPE_FIELDS,
        "tower_id": str,
        "technology": str,
        "signal_strength": int,
        "connected_devices": int,
        "status": str,
    },
    "dlq_record": {
        **ENVELOPE_FIELDS,
        "source_topic": str,
        "rejection_reason": str,
        "original_payload": str,
    },
}


def schema_violations(event_type: str, event: object) -> list[str]:
    """Structural check of a decoded JSON event against its field schema.

    Returns violation tags (empty = conforms). Types are checked strictly,
    except ints are acceptable where floats are expected (JSON round-trips
    `199.0` as `199`). bool is NOT acceptable for int (JSON true/false must
    not sneak into numeric fields).
    """
    fields = EVENT_FIELDS[event_type]
    if not isinstance(event, dict):
        return ["not_an_object"]
    violations = []
    for name, expected in fields.items():
        if name not in event:
            violations.append(f"missing:{name}")
            continue
        value = event[name]
        if expected is float:
            ok = isinstance(value, int | float) and not isinstance(value, bool)
        elif expected is int:
            ok = isinstance(value, int) and not isinstance(value, bool)
        else:
            ok = isinstance(value, expected)
        if not ok:
            violations.append(f"type:{name}")
    violations.extend(f"unexpected:{name}" for name in event if name not in fields)
    return violations


TOPIC_TO_EVENT_TYPE: dict[str, str] = {
    "subscriber-created": "subscriber_created",
    "airtime-purchases": "airtime_purchase",
    "bundle-purchases": "bundle_purchase",
    "voice-calls": "voice_call",
    "sms": "sms",
    "data-usage": "data_usage",
    "tower-events": "tower_status",
}

DLQ_TOPIC = "failed-transactions"
