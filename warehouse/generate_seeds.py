"""Generate warehouse/seeds.sql from the producer catalog so dim_tower and
dim_bundle can never drift from what the generators emit.

Run from the repo root:  python warehouse/generate_seeds.py
A unit test asserts the committed seeds.sql matches this script's output.
"""

from pathlib import Path

from telestream_producer.catalog import BUNDLES, TOWERS

HEADER = (
    "-- GENERATED FILE — do not edit by hand.\n"
    "-- Regenerate with: python warehouse/generate_seeds.py\n\n"
)


def render_seeds() -> str:
    lines = [HEADER]
    lines.append("INSERT INTO dim_tower (tower_id, tower_name, province, technologies) VALUES\n")
    tower_rows = [
        f"    ('{t.tower_id}', '{t.name}', '{t.province}', "
        f"ARRAY[{', '.join(repr(x) for x in t.technologies)}])"
        for t in TOWERS
    ]
    lines.append(",\n".join(tower_rows) + ";\n\n")

    lines.append("INSERT INTO dim_bundle (bundle_code, bundle_name, bundle_type, price) VALUES\n")
    bundle_rows = [
        f"    ('{b.bundle_code}', '{b.name}', '{b.bundle_type}', {b.price:.2f})" for b in BUNDLES
    ]
    lines.append(",\n".join(bundle_rows) + ";\n")
    return "".join(lines)


def main() -> None:
    out = Path(__file__).parent / "seeds.sql"
    out.write_text(render_seeds(), newline="\n", encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
