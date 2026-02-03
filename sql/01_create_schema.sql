-- Create project database
CREATE DATABASE NetflixSecAnalytics;
GO

USE NetflixSecAnalytics;
GO

-- Logical namespace for analyst-ready data
CREATE SCHEMA analytics;
GO

-- Canonical financial facts table
CREATE TABLE analytics.financial_facts (
    fact_id            BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Filing identity & lineage
    cik                CHAR(10)       NOT NULL,
    accession_number   VARCHAR(20)    NOT NULL,
    filing_type        VARCHAR(10)    NOT NULL,
    filing_date        DATE            NOT NULL,

    -- XBRL concept
    concept_namespace  VARCHAR(50)    NOT NULL,
    concept_name       VARCHAR(100)   NOT NULL,

    -- Value
    value              DECIMAL(20, 4)  NULL,
    unit               VARCHAR(20)     NULL,

    -- Time semantics
    period_start       DATE            NULL,
    period_end         DATE            NOT NULL,
    period_type        VARCHAR(20)     NOT NULL,

    -- Traceability
    context_id         VARCHAR(50)     NOT NULL,

    -- Audit metadata
    created_at         DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Prevent duplicate facts within a filing
CREATE UNIQUE INDEX ux_financial_facts_nodup
ON analytics.financial_facts (
    accession_number,
    concept_namespace,
    concept_name,
    context_id
);
GO

-- Speed up time-series queries
CREATE INDEX ix_financial_facts_concept_time
ON analytics.financial_facts (
    concept_name,
    period_end
);
GO
