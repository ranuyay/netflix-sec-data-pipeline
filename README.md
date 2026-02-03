# Netflix SEC Data Pipeline

End-to-end data pipeline ingesting Netflix financial disclosures from SEC XML filings,
storing curated time-series data in Microsoft SQL Server, and supporting exploratory
analysis in Python and data storytelling in Tableau.

## Project Overview

This project demonstrates a full analytics workflow:
- Programmatic ingestion of authoritative financial disclosures (SEC XML/XBRL)
- Data staging and transformation in SQL Server
- Exploratory data analysis in Python
- Business-facing data storytelling in Tableau

The pipeline is designed to be reproducible and extensible as new filings are published.

## Architecture

SEC XML filings  
→ Python ingestion & parsing  
→ SQL Server (staging → data warehouse)  
→ Python EDA  
→ Tableau dashboards & story

## Repository Structure

```text
src/        Pipeline code
sql/        Database schema and tables
notebooks/  Exploratory analysis
data/       Raw and curated datasets
tableau/    Tableau story design
