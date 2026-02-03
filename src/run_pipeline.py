"""
Pipeline entrypoint.

Currently:
- Runs SEC ingestion

Later:
- XBRL parsing
- SQL loading
- Analytics
"""

from src.sec_fetch import ingest_netflix_raw


def main() -> None:
    ingest_netflix_raw(limit=1)


if __name__ == "__main__":
    main()
