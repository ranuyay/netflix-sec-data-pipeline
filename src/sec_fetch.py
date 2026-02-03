"""
SEC ingestion for Netflix filings (10-K / 10-Q).

This script:
1. Resolves Netflix CIK from SEC company_tickers.json
2. Fetches the SEC submissions JSON
3. Identifies recent 10-K / 10-Q filings
4. Downloads index.json for each filing
5. Locates and downloads XBRL instance XML or XBRL ZIP
6. Stores raw artifacts under data/raw/

Ingestion only. Parsing comes later.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests


SEC_BASE = "https://data.sec.gov"
SEC_ARCHIVES = "https://www.sec.gov/Archives"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

DEFAULT_TICKER = "NFLX"
TARGET_FORMS = {"10-K", "10-Q"}


@dataclass(frozen=True)
class FilingRef:
    cik: str
    accession: str
    accession_nodashes: str
    form: str
    filing_date: str
    primary_doc: str


def _require_user_agent() -> str:
    ua = os.getenv("SEC_USER_AGENT", "").strip()
    if not ua:
        raise RuntimeError(
            "Missing SEC_USER_AGENT env var. Example:\n"
            'set SEC_USER_AGENT=YourName PortfolioProject (you@email.com)'
        )
    return ua


def _session(host: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": _require_user_agent(),
            "Accept-Encoding": "gzip, deflate",
            "Host": host,
        }
    )
    return s


def resolve_cik_from_ticker(ticker: str = DEFAULT_TICKER) -> str:
    s = _session("www.sec.gov")
    r = s.get(TICKERS_URL, timeout=30)
    r.raise_for_status()
    data = r.json()

    for row in data.values():
        if str(row.get("ticker", "")).upper() == ticker.upper():
            return f"{int(row['cik_str']):010d}"

    raise ValueError(f"Ticker not found: {ticker}")


def fetch_submissions(cik_10: str) -> dict:
    s = _session("data.sec.gov")
    url = f"{SEC_BASE}/submissions/CIK{cik_10}.json"
    r = s.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def list_recent_filings(
    submissions: dict,
    forms: Iterable[str] = TARGET_FORMS,
    limit: int = 3,
) -> List[FilingRef]:
    recent = submissions.get("filings", {}).get("recent", {})
    out: List[FilingRef] = []

    for form, acc, date, pdoc in zip(
        recent.get("form", []),
        recent.get("accessionNumber", []),
        recent.get("filingDate", []),
        recent.get("primaryDocument", []),
    ):
        if form in forms:
            out.append(
                FilingRef(
                    cik=submissions["cik"].zfill(10),
                    accession=acc,
                    accession_nodashes=acc.replace("-", ""),
                    form=form,
                    filing_date=date,
                    primary_doc=pdoc,
                )
            )

    out.sort(key=lambda x: x.filing_date, reverse=True)
    return out[:limit]


def fetch_index_json(cik_10: str, accession_nodashes: str) -> dict:
    cik_no_zeros = str(int(cik_10))
    url = f"{SEC_ARCHIVES}/edgar/data/{cik_no_zeros}/{accession_nodashes}/index.json"
    s = _session("www.sec.gov")
    r = s.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def pick_xbrl_files(index_json: dict) -> Tuple[Optional[str], Optional[str]]:
    items = index_json.get("directory", {}).get("item", [])
    names = [i["name"] for i in items if i.get("type") == "file"]

    xmls = [n for n in names if n.lower().endswith(".xml")]
    zips = [n for n in names if n.lower().endswith(".zip")]

    instance_xml = None
    if xmls:
        xmls.sort(key=lambda n: ("ins" not in n.lower(), n))
        instance_xml = xmls[0]

    xbrl_zip = None
    if zips:
        zips.sort(key=lambda n: ("xbrl" not in n.lower(), n))
        xbrl_zip = zips[0]

    return instance_xml, xbrl_zip


def download_file(url: str, out_path: Path, host: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    s = _session(host)
    with s.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)


def ingest_netflix_raw(limit: int = 1, out_root: Path = Path("data/raw")) -> None:
    cik_10 = resolve_cik_from_ticker()
    subs = fetch_submissions(cik_10)
    filings = list_recent_filings(subs, limit=limit)

    for f in filings:
        out_dir = out_root / cik_10 / f.accession_nodashes
        index_json = fetch_index_json(cik_10, f.accession_nodashes)

        index_path = out_dir / "index.json"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(json.dumps(index_json, indent=2), encoding="utf-8")

        instance_xml, xbrl_zip = pick_xbrl_files(index_json)

        base_url = f"{SEC_ARCHIVES}/edgar/data/{int(cik_10)}/{f.accession_nodashes}"

        if instance_xml:
            download_file(
                f"{base_url}/{instance_xml}",
                out_dir / instance_xml,
                "www.sec.gov",
            )

        if xbrl_zip:
            download_file(
                f"{base_url}/{xbrl_zip}",
                out_dir / xbrl_zip,
                "www.sec.gov",
            )

        print(f"Downloaded {f.form} {f.filing_date} ({f.accession})")


def main(argv: List[str]) -> int:
    limit = 1
    if "--limit" in argv:
        limit = int(argv[argv.index("--limit") + 1])

    ingest_netflix_raw(limit=limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
