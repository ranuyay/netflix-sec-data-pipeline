"""
XBRL parsing for Netflix SEC filings.

This module extracts:
- contexts (entity + period semantics)
- units
- numeric US-GAAP facts

"""

from pathlib import Path
import xml.etree.ElementTree as ET


# Default path for standalone testing
XBRL_PATH = Path(
    "data/raw/0001065280/000106528026000034/xbrl/nflx-20251231_htm.xml"
)

NAMESPACES = {
    "xbrli": "http://www.xbrl.org/2003/instance",
}


# ---------------------------------------------------------------------
# Contexts
# ---------------------------------------------------------------------
def parse_contexts(xbrl_file: Path) -> list[dict]:
    tree = ET.parse(xbrl_file)
    root = tree.getroot()

    contexts = []

    for ctx in root.findall(".//xbrli:context", NAMESPACES):
        context_id = ctx.attrib.get("id")

        # ---- Entity (CIK) ----
        entity_elem = ctx.find("xbrli:entity/xbrli:identifier", NAMESPACES)
        entity_cik = entity_elem.text if entity_elem is not None else None

        # ---- Period ----
        period_elem = ctx.find("xbrli:period", NAMESPACES)

        start_date = None
        end_date = None
        period_type = None

        if period_elem is not None:
            instant = period_elem.find("xbrli:instant", NAMESPACES)
            start = period_elem.find("xbrli:startDate", NAMESPACES)
            end = period_elem.find("xbrli:endDate", NAMESPACES)

            if instant is not None:
                period_type = "instant"
                end_date = instant.text
            elif start is not None and end is not None:
                period_type = "duration"
                start_date = start.text
                end_date = end.text

        contexts.append(
            {
                "context_id": context_id,
                "entity_cik": entity_cik,
                "period_type": period_type,
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    return contexts


# ---------------------------------------------------------------------
# Units
# ---------------------------------------------------------------------
def parse_units(xbrl_file: Path) -> list[dict]:
    tree = ET.parse(xbrl_file)
    root = tree.getroot()

    units = []

    for unit in root.findall(".//xbrli:unit", NAMESPACES):
        unit_id = unit.attrib.get("id")

        measures = [
            m.text for m in unit.findall("xbrli:measure", NAMESPACES)
        ]

        units.append(
            {
                "unit_id": unit_id,
                "measures": measures,
            }
        )

    return units


# ---------------------------------------------------------------------
# US-GAAP Facts
# ---------------------------------------------------------------------
def parse_us_gaap_facts(xbrl_file: Path) -> list[dict]:
    tree = ET.parse(xbrl_file)
    root = tree.getroot()

    facts = []

    for elem in root.iter():
        if not elem.tag.startswith("{http://fasb.org/us-gaap/"):
            continue

        facts.append(
            {
                "concept": elem.tag.split("}")[1],
                "context_id": elem.attrib.get("contextRef"),
                "unit_id": elem.attrib.get("unitRef"),
                "value": elem.text.strip() if elem.text else None,
                "decimals": elem.attrib.get("decimals"),
            }
        )

    return facts


def filter_numeric_facts(facts: list[dict]) -> list[dict]:
    numeric_facts = []

    for fact in facts:
        if fact["unit_id"] is None:
            continue

        try:
            float(fact["value"])
        except (TypeError, ValueError):
            continue

        numeric_facts.append(fact)

    return numeric_facts


# ---------------------------------------------------------------------
# Pipeline-facing API
# ---------------------------------------------------------------------
def build_financial_fact_rows(xbrl_file: Path) -> list[dict]:
    """
    Parse a single XBRL instance document and return numeric US-GAAP facts
    enriched with context and unit information.
    """

    contexts = parse_contexts(xbrl_file)
    units = parse_units(xbrl_file)
    facts = parse_us_gaap_facts(xbrl_file)
    numeric_facts = filter_numeric_facts(facts)

    context_index = {c["context_id"]: c for c in contexts}
    unit_index = {u["unit_id"]: u for u in units}

    rows = []

    for fact in numeric_facts:
        ctx = context_index.get(fact["context_id"])
        if not ctx:
            continue

        rows.append(
            {
                "concept": fact["concept"],
                "value": fact["value"],
                "unit": fact["unit_id"],
                "decimals": fact["decimals"],
                "period_type": ctx["period_type"],
                "period_start": ctx["start_date"],
                "period_end": ctx["end_date"],
                "context_id": fact["context_id"],
                "entity_cik": ctx["entity_cik"],
            }
        )

    return rows


# ---------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------
if __name__ == "__main__":
    contexts = parse_contexts(XBRL_PATH)
    units = parse_units(XBRL_PATH)
    facts = parse_us_gaap_facts(XBRL_PATH)
    numeric_facts = filter_numeric_facts(facts)

    print(f"Parsed {len(contexts)} contexts")
    print(f"Parsed {len(units)} units")
    print(f"Parsed {len(facts)} total us-gaap facts")
    print(f"Parsed {len(numeric_facts)} numeric us-gaap facts")

    print("\nSample numeric facts:")
    for fact in numeric_facts[:5]:
        print(fact)
