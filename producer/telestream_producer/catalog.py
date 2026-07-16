"""Static reference data: provinces, towers, and the bundle catalog.

This is the producer-side mirror of the warehouse seed data (dim_tower,
dim_bundle). The warehouse seeds are generated from these definitions so the
two can never drift apart.
"""

from dataclasses import dataclass

PROVINCES: tuple[str, ...] = (
    "Eastern Cape",
    "Free State",
    "Gauteng",
    "KwaZulu-Natal",
    "Limpopo",
    "Mpumalanga",
    "Northern Cape",
    "North West",
    "Western Cape",
)

PLANS: tuple[str, ...] = ("Prepaid", "Contract", "TopUp")

PAYMENT_METHODS: tuple[str, ...] = ("Voucher", "Card", "EFT", "USSD")

TECHNOLOGIES: tuple[str, ...] = ("3G", "LTE", "5G")

TOWER_STATUSES: tuple[str, ...] = ("HEALTHY", "DEGRADED", "DOWN")


@dataclass(frozen=True)
class Tower:
    tower_id: str
    name: str
    province: str
    technologies: tuple[str, ...]


TOWERS: tuple[Tower, ...] = (
    Tower("CPT-CBD-001", "Cape Town CBD", "Western Cape", ("3G", "LTE", "5G")),
    Tower("CPT-BLV-003", "Bellville", "Western Cape", ("3G", "LTE")),
    Tower("CPT-CC-002", "Century City", "Western Cape", ("LTE", "5G")),
    Tower("CPT-STB-004", "Stellenbosch", "Western Cape", ("3G", "LTE")),
    Tower("JHB-SDT-001", "Sandton", "Gauteng", ("LTE", "5G")),
    Tower("JHB-CBD-002", "Johannesburg CBD", "Gauteng", ("3G", "LTE", "5G")),
    Tower("JHB-SWT-003", "Soweto", "Gauteng", ("3G", "LTE")),
    Tower("PTA-CBD-001", "Pretoria CBD", "Gauteng", ("3G", "LTE", "5G")),
    Tower("DBN-CBD-001", "Durban CBD", "KwaZulu-Natal", ("3G", "LTE", "5G")),
    Tower("DBN-UMH-002", "Umhlanga", "KwaZulu-Natal", ("LTE", "5G")),
    Tower("PMB-CBD-001", "Pietermaritzburg", "KwaZulu-Natal", ("3G", "LTE")),
    Tower("GQB-CBD-001", "Gqeberha Central", "Eastern Cape", ("3G", "LTE")),
    Tower("EL-CBD-001", "East London", "Eastern Cape", ("3G", "LTE")),
    Tower("BFN-CBD-001", "Bloemfontein", "Free State", ("3G", "LTE")),
    Tower("PLK-CBD-001", "Polokwane", "Limpopo", ("3G", "LTE")),
    Tower("NLP-CBD-001", "Mbombela", "Mpumalanga", ("3G", "LTE")),
    Tower("KIM-CBD-001", "Kimberley", "Northern Cape", ("3G",)),
    Tower("RTB-CBD-001", "Rustenburg", "North West", ("3G", "LTE")),
)

TOWER_IDS: tuple[str, ...] = tuple(t.tower_id for t in TOWERS)


@dataclass(frozen=True)
class Bundle:
    bundle_code: str
    name: str
    bundle_type: str
    price: float


BUNDLES: tuple[Bundle, ...] = (
    Bundle("DATA_1GB", "1GB Data", "DATA", 99.00),
    Bundle("DATA_5GB", "5GB Data", "DATA", 199.00),
    Bundle("DATA_20GB", "20GB Data", "DATA", 399.00),
    Bundle("VOICE_100MIN", "100 Minutes", "VOICE", 79.00),
    Bundle("VOICE_300MIN", "300 Minutes", "VOICE", 169.00),
    Bundle("SMS_500", "500 SMS", "SMS", 49.00),
    Bundle("COMBO_STARTER", "Starter Combo", "COMBO", 149.00),
    Bundle("COMBO_POWER", "Power Combo", "COMBO", 299.00),
)

BUNDLE_CODES: tuple[str, ...] = tuple(b.bundle_code for b in BUNDLES)
BUNDLE_PRICES: dict[str, float] = {b.bundle_code: b.price for b in BUNDLES}
