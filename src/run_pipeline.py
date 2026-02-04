"""
Pipeline entrypoint.

Stages:
1. SEC ingestion
2. XBRL parsing (read-only, no persistence yet)
"""

from pathlib import Path

from sec_fetch import ingest_netflix_raw
from xbrl_parse import build_financial_fact_rows


def main() -> None:
    # 1. Ingest latest Netflix filing
    ingest_netflix_raw(limit=1)

    # 2. Parse XBRL into structured rows
    xbrl_path = Path(
        "data/raw/0001065280/000106528026000034/xbrl/nflx-20251231_htm.xml"
    )

    rows = build_financial_fact_rows(xbrl_path)

    print(f"Parsed {len(rows)} financial fact rows")
    print("Sample rows:")
    for row in rows[:5]:
        print(row)


if __name__ == "__main__":
    main()
