-- FinDER 8-category schema (Accounting, Company Overview, Financials, Footnotes,
-- Governance, Legal, Risk, Shareholder Return) over SEC EDGAR data.
--
-- Every fact row points at the filing it came from (filings.filing_id), so any
-- answer can be traced back to an accession number. Every FK column has a
-- B-tree index: the Postgres/Neo4j comparison is only fair if neither side is
-- left doing full scans on join keys.

CREATE TABLE companies (
    company_id              SERIAL PRIMARY KEY,
    cik                     INTEGER UNIQUE,            -- NULL for non-SEC counterparties (Panasonic, CATL, ...)
    ticker                  TEXT,
    name                    TEXT NOT NULL UNIQUE,
    sic_code                TEXT,
    sic_description         TEXT,
    state_of_incorporation  TEXT,
    country                 TEXT,
    fiscal_year_end         TEXT,                      -- MMDD as reported by EDGAR
    is_sec_filer            BOOLEAN NOT NULL,
    annual_report_form      TEXT,                      -- 10-K / 20-F / S-1 / NULL
    entity_type             TEXT NOT NULL DEFAULT 'company' CHECK (entity_type IN ('company', 'government'))
);
COMMENT ON TABLE companies IS 'Company Overview: issuers that file with the SEC plus counterparties named in their filings.';

CREATE TABLE filings (
    filing_id      SERIAL PRIMARY KEY,
    accession_no   TEXT NOT NULL UNIQUE,
    filer_cik      INTEGER NOT NULL,
    company_id     INTEGER REFERENCES companies(company_id),   -- subject company, when it is one of ours
    form_type      TEXT NOT NULL,
    filing_date    DATE NOT NULL,
    report_date    DATE,
    primary_doc    TEXT
);
CREATE INDEX filings_company_id_idx ON filings(company_id);
CREATE INDEX filings_form_date_idx ON filings(form_type, filing_date);
COMMENT ON TABLE filings IS 'Source documents (provenance) for every fact row.';

-- Governance -----------------------------------------------------------------

CREATE TABLE persons (
    person_id   SERIAL PRIMARY KEY,
    owner_cik   INTEGER UNIQUE,            -- EDGAR reporting-owner CIK
    full_name   TEXT NOT NULL
);
COMMENT ON TABLE persons IS 'Governance: directors, officers and 10% owners reported on Forms 3/4/5.';

CREATE TABLE company_executives (
    company_executive_id  SERIAL PRIMARY KEY,
    company_id            INTEGER NOT NULL REFERENCES companies(company_id),
    person_id             INTEGER NOT NULL REFERENCES persons(person_id),
    is_director           BOOLEAN NOT NULL DEFAULT FALSE,
    is_officer            BOOLEAN NOT NULL DEFAULT FALSE,
    is_ten_percent_owner  BOOLEAN NOT NULL DEFAULT FALSE,
    officer_title         TEXT,
    first_seen_date       DATE NOT NULL,
    last_seen_date        DATE NOT NULL,
    source_filing_id      INTEGER REFERENCES filings(filing_id),
    UNIQUE (company_id, person_id)
);
CREATE INDEX company_executives_company_id_idx ON company_executives(company_id);
CREATE INDEX company_executives_person_id_idx ON company_executives(person_id);
CREATE INDEX company_executives_source_filing_id_idx ON company_executives(source_filing_id);
COMMENT ON TABLE company_executives IS 'Governance: a person''s board/officer role at a company. A person with rows at two companies is a board interlock.';

-- Shareholder Return / ownership ---------------------------------------------

CREATE TABLE institutional_holders (
    institution_id  SERIAL PRIMARY KEY,
    cik             INTEGER NOT NULL UNIQUE,
    name            TEXT NOT NULL
);
COMMENT ON TABLE institutional_holders IS 'Shareholder Return: investment managers that file Form 13F-HR.';

CREATE TABLE institutional_holdings (
    holding_id        SERIAL PRIMARY KEY,
    institution_id    INTEGER NOT NULL REFERENCES institutional_holders(institution_id),
    company_id        INTEGER NOT NULL REFERENCES companies(company_id),
    report_period     DATE NOT NULL,
    shares            BIGINT NOT NULL,
    value_usd         NUMERIC(20, 2) NOT NULL,
    source_filing_id  INTEGER REFERENCES filings(filing_id),
    UNIQUE (institution_id, company_id, report_period)
);
CREATE INDEX institutional_holdings_institution_id_idx ON institutional_holdings(institution_id);
CREATE INDEX institutional_holdings_company_id_idx ON institutional_holdings(company_id);
CREATE INDEX institutional_holdings_source_filing_id_idx ON institutional_holdings(source_filing_id);
COMMENT ON TABLE institutional_holdings IS 'Shareholder Return: position of one 13F filer in one company at a quarter end.';

-- Company Overview: supply chain ---------------------------------------------

CREATE TABLE supply_chain_relations (
    relation_id          SERIAL PRIMARY KEY,
    supplier_company_id  INTEGER NOT NULL REFERENCES companies(company_id),
    customer_company_id  INTEGER NOT NULL REFERENCES companies(company_id),
    relation_type        TEXT NOT NULL CHECK (relation_type IN ('supplier', 'manufacturing_partner', 'joint_venture', 'licensor', 'launch_provider')),
    product_category     TEXT NOT NULL,
    contract_value_usd   NUMERIC(20, 2),
    since_year           INTEGER,
    evidence             TEXT NOT NULL,            -- quoted sentence from the filing
    source_filing_id     INTEGER REFERENCES filings(filing_id),
    CHECK (supplier_company_id <> customer_company_id)
);
CREATE INDEX supply_chain_relations_supplier_idx ON supply_chain_relations(supplier_company_id);
CREATE INDEX supply_chain_relations_customer_idx ON supply_chain_relations(customer_company_id);
CREATE INDEX supply_chain_relations_source_filing_id_idx ON supply_chain_relations(source_filing_id);
COMMENT ON TABLE supply_chain_relations IS 'Company Overview: supplier_company_id supplies customer_company_id. Chains of these rows are multi-hop supply paths.';

-- Financials / Accounting / Shareholder Return: XBRL facts --------------------

CREATE TABLE metric_definitions (
    metric_id    SERIAL PRIMARY KEY,
    metric_code  TEXT NOT NULL UNIQUE,      -- normalized name, e.g. revenue, net_income
    name         TEXT NOT NULL,
    category     TEXT NOT NULL CHECK (category IN ('Financials', 'Accounting', 'Shareholder Return')),
    unit         TEXT NOT NULL,
    description  TEXT NOT NULL,
    us_gaap_tags TEXT[] NOT NULL,           -- source tags, first match wins
    ifrs_tags    TEXT[] NOT NULL
);
COMMENT ON TABLE metric_definitions IS 'Normalized metric catalog that unifies US-GAAP (10-K) and IFRS (20-F) XBRL tags.';

CREATE TABLE financial_metrics (
    financial_metric_id  SERIAL PRIMARY KEY,
    company_id           INTEGER NOT NULL REFERENCES companies(company_id),
    metric_id            INTEGER NOT NULL REFERENCES metric_definitions(metric_id),
    fiscal_year          INTEGER NOT NULL,
    fiscal_period        TEXT NOT NULL CHECK (fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')),
    period_start         DATE,                      -- NULL for instant (balance-sheet) facts
    period_end           DATE NOT NULL,
    value                NUMERIC(24, 4) NOT NULL,
    unit                 TEXT NOT NULL,             -- USD, USD/shares, shares, TWD, EUR, ...
    xbrl_tag             TEXT NOT NULL,
    source_filing_id     INTEGER REFERENCES filings(filing_id),
    UNIQUE (company_id, metric_id, fiscal_year, fiscal_period, unit)
);
CREATE INDEX financial_metrics_company_id_idx ON financial_metrics(company_id);
CREATE INDEX financial_metrics_metric_id_idx ON financial_metrics(metric_id);
CREATE INDEX financial_metrics_source_filing_id_idx ON financial_metrics(source_filing_id);
CREATE INDEX financial_metrics_period_idx ON financial_metrics(fiscal_year, fiscal_period);
COMMENT ON TABLE financial_metrics IS 'Financials: one reported XBRL value of one metric for one company and fiscal period. Values are in the reporting currency (unit).';

-- Risk -----------------------------------------------------------------------

CREATE TABLE risk_categories (
    risk_category_id  SERIAL PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    description       TEXT NOT NULL
);
COMMENT ON TABLE risk_categories IS 'Risk: shared taxonomy; two companies citing the same category share exposure.';

CREATE TABLE risk_factors (
    risk_factor_id    SERIAL PRIMARY KEY,
    company_id        INTEGER NOT NULL REFERENCES companies(company_id),
    risk_category_id  INTEGER NOT NULL REFERENCES risk_categories(risk_category_id),
    filing_id         INTEGER NOT NULL REFERENCES filings(filing_id),
    title             TEXT NOT NULL,
    summary           TEXT NOT NULL,
    mentioned_company_id INTEGER REFERENCES companies(company_id)  -- a named counterparty the risk depends on
);
CREATE INDEX risk_factors_company_id_idx ON risk_factors(company_id);
CREATE INDEX risk_factors_risk_category_id_idx ON risk_factors(risk_category_id);
CREATE INDEX risk_factors_filing_id_idx ON risk_factors(filing_id);
CREATE INDEX risk_factors_mentioned_company_id_idx ON risk_factors(mentioned_company_id);
COMMENT ON TABLE risk_factors IS 'Risk: one Item 1A (10-K) / Item 3.D (20-F) / S-1 risk factor.';

-- Legal ----------------------------------------------------------------------

CREATE TABLE legal_proceedings (
    legal_proceeding_id      SERIAL PRIMARY KEY,
    company_id               INTEGER NOT NULL REFERENCES companies(company_id),
    filing_id                INTEGER NOT NULL REFERENCES filings(filing_id),
    case_name                TEXT NOT NULL,
    proceeding_type          TEXT NOT NULL CHECK (proceeding_type IN ('securities', 'regulatory', 'product_liability', 'contract', 'intellectual_property', 'employment', 'environmental', 'government_investigation', 'other')),
    forum                    TEXT,             -- court or agency
    counterparty             TEXT,
    counterparty_company_id  INTEGER REFERENCES companies(company_id),
    status                   TEXT NOT NULL CHECK (status IN ('pending', 'settled', 'dismissed', 'judgment', 'appeal', 'unknown')),
    amount_usd               NUMERIC(20, 2),
    description              TEXT NOT NULL
);
CREATE INDEX legal_proceedings_company_id_idx ON legal_proceedings(company_id);
CREATE INDEX legal_proceedings_filing_id_idx ON legal_proceedings(filing_id);
CREATE INDEX legal_proceedings_counterparty_company_id_idx ON legal_proceedings(counterparty_company_id);
COMMENT ON TABLE legal_proceedings IS 'Legal: proceedings disclosed in Item 3 / contingencies notes.';

-- Footnotes ------------------------------------------------------------------

CREATE TABLE footnote_related_parties (
    related_party_id     SERIAL PRIMARY KEY,
    company_id           INTEGER NOT NULL REFERENCES companies(company_id),
    filing_id            INTEGER NOT NULL REFERENCES filings(filing_id),
    related_party_name   TEXT NOT NULL,
    related_company_id   INTEGER REFERENCES companies(company_id),
    related_person_id    INTEGER REFERENCES persons(person_id),
    relationship         TEXT NOT NULL,     -- e.g. 'entity controlled by CEO'
    transaction_type     TEXT NOT NULL,     -- e.g. 'purchase of services', 'lease'
    amount_usd           NUMERIC(20, 2),
    fiscal_year          INTEGER,
    description          TEXT NOT NULL
);
CREATE INDEX footnote_related_parties_company_id_idx ON footnote_related_parties(company_id);
CREATE INDEX footnote_related_parties_filing_id_idx ON footnote_related_parties(filing_id);
CREATE INDEX footnote_related_parties_related_company_id_idx ON footnote_related_parties(related_company_id);
CREATE INDEX footnote_related_parties_related_person_id_idx ON footnote_related_parties(related_person_id);
COMMENT ON TABLE footnote_related_parties IS 'Footnotes: related-party transactions disclosed in the notes.';
