"""
SQL Server loader for SEC XBRL financial facts.

Responsibilities:
- Accept parsed financial fact rows
- Insert into analytics.financial_facts
- Enforce idempotency via database constraints
"""

import pyodbc
from typing import Iterable


def load_financial_facts(
    rows: Iterable[dict],
    connection_string: str,
) -> int:
    """
    Load financial fact rows into SQL Server.

    Returns number of rows successfully inserted.
    """

    insert_sql = """
        INSERT INTO analytics.financial_facts (
            cik,
            accession_number,
            filing_type,
            filing_date,
            concept_namespace,
            concept_name,
            value,
            unit,
            period_start,
            period_end,
            period_type,
            context_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    inserted = 0

    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()

        for row in rows:
            try:
                cursor.execute(
                    insert_sql,
                    row["entity_cik"],
                    row.get("accession_number"),
                    row.get("filing_type"),
                    row.get("filing_date"),
                    "us-gaap",
                    row["concept"],
                    row["value"],
                    row["unit"],
                    row["period_start"],
                    row["period_end"],
                    row["period_type"],
                    row["context_id"],
                )
                inserted += 1
            except pyodbc.IntegrityError:
                # Duplicate row (safe to ignore)
                continue

        conn.commit()

    return inserted
