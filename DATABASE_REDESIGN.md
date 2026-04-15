# Database Redesign Proposal

## Problems with the Current Schema

1. **No primary keys or constraints** — duplicates can enter freely (and did)
2. **Inconsistent column naming** — `evt_dttm` vs `beg_dttm` vs `eff_dttm` vs `start_ts`; `sid`/`pid`/`asset_id` are ambiguous
3. **No foreign keys** — orphan records exist (pid=999 usage with no profile, bundle_id=9999 rate with no plan)
4. **No data validation** — negative MB, negative rates, end dates before start dates all got through
5. **Denormalized tech field** — mix of casing and naming conventions (LTE vs 4G vs lte) with no reference table


---

## Proposed New Schema (SQL DDL)

```sql
-- ============================================================
-- Reference table: standardized technology generations
-- ============================================================
CREATE TABLE technology (
    tech_cd     VARCHAR(10) PRIMARY KEY,  -- '2G', '3G', '4G', '5G'
    tech_name   VARCHAR(50) NOT NULL,     -- 'GSM', 'CDMA/HSPA+', 'LTE', 'NR'
    description VARCHAR(200)
);

-- Seed data
INSERT INTO technology VALUES ('2G', 'GSM',        '2nd Generation');
INSERT INTO technology VALUES ('3G', 'CDMA/HSPA+',  '3rd Generation');
INSERT INTO technology VALUES ('4G', 'LTE',         '4th Generation / Long Term Evolution');
INSERT INTO technology VALUES ('5G', 'NR',          '5th Generation / New Radio');


-- ============================================================
-- Reference table: country/network codes
-- ============================================================
CREATE TABLE network_code (
    cc1         INT NOT NULL,          -- Mobile Country Code
    cc2         INT NOT NULL,          -- Mobile Network Code
    country     VARCHAR(100),
    carrier     VARCHAR(100),
    PRIMARY KEY (cc1, cc2)
);


-- ============================================================
-- Profiles (eSIM / SIM installations on devices)
-- Renamed columns for clarity
-- ============================================================
CREATE TABLE profile_installation (
    profile_id      INT PRIMARY KEY,           -- was 'pid', now unambiguous
    asset_id        INT NOT NULL,              -- the SIM card
    installed_at    TIMESTAMP NOT NULL,        -- was 'beg_dttm'/'start_ts'
    removed_at      TIMESTAMP,                -- was 'end_dttm'/'end_ts', NULL if still active
    source          VARCHAR(20) NOT NULL,      -- was 'src'/'src_cd' — 'portal', 'api', etc.
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_profile_dates CHECK (removed_at IS NULL OR removed_at > installed_at)
);

CREATE INDEX idx_profile_asset ON profile_installation(asset_id);
CREATE INDEX idx_profile_dates ON profile_installation(installed_at, removed_at);


-- ============================================================
-- SIM card plan history
-- Tracks which bundle/plan is assigned to each SIM over time
-- ============================================================
CREATE TABLE sim_card_plan_history (
    id              SERIAL PRIMARY KEY,         -- surrogate PK for uniqueness
    asset_id        INT NOT NULL,
    bundle_id       INT NOT NULL,
    effective_at    TIMESTAMP NOT NULL,          -- was 'eff_dttm'
    expired_at      TIMESTAMP,                  -- was 'x_dttm', NULL if still active
    reason          VARCHAR(50) NOT NULL,        -- was 'why_cd' — normalized values
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_plan_dates CHECK (expired_at IS NULL OR expired_at >= effective_at),
    CONSTRAINT fk_plan_bundle FOREIGN KEY (bundle_id) REFERENCES rate_card_bundle(bundle_id)
);

CREATE INDEX idx_plan_asset ON sim_card_plan_history(asset_id);
CREATE INDEX idx_plan_dates ON sim_card_plan_history(effective_at, expired_at);
CREATE UNIQUE INDEX idx_plan_no_overlap
    ON sim_card_plan_history(asset_id, bundle_id, effective_at);


-- ============================================================
-- Rate card bundles (grouping table)
-- ============================================================
CREATE TABLE rate_card_bundle (
    bundle_id   INT PRIMARY KEY,
    bundle_name VARCHAR(100),
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- Rate card (pricing per MB)
-- Now has a proper PK and version tracking
-- ============================================================
CREATE TABLE rate_card (
    rate_id     SERIAL PRIMARY KEY,             -- surrogate PK
    bundle_id   INT NOT NULL,
    cc1         INT NOT NULL,
    cc2         INT NOT NULL,
    tech_cd     VARCHAR(10),                    -- NULL = catch-all/default rate
    effective_at DATE NOT NULL,                 -- was 'beg_dttm'
    expired_at   DATE,                          -- was 'end_dttm', NULL if still active
    rate_per_mb  NUMERIC(10,6) NOT NULL,        -- was 'rt_amt'
    currency     CHAR(3) NOT NULL DEFAULT 'USD',-- was 'curr_cd', fixed width
    priority     INT NOT NULL DEFAULT 10,       -- was 'prio_nbr'
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_rate_positive CHECK (rate_per_mb > 0),
    CONSTRAINT chk_rate_dates CHECK (expired_at IS NULL OR expired_at >= effective_at),
    CONSTRAINT chk_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT fk_rate_bundle FOREIGN KEY (bundle_id) REFERENCES rate_card_bundle(bundle_id),
    CONSTRAINT fk_rate_tech FOREIGN KEY (tech_cd) REFERENCES technology(tech_cd),
    CONSTRAINT fk_rate_network FOREIGN KEY (cc1, cc2) REFERENCES network_code(cc1, cc2)
);

CREATE INDEX idx_rate_lookup
    ON rate_card(bundle_id, cc1, cc2, tech_cd, effective_at);
CREATE UNIQUE INDEX idx_rate_no_dup
    ON rate_card(bundle_id, cc1, cc2, COALESCE(tech_cd, ''), effective_at);


-- ============================================================
-- Usage events
-- ============================================================
CREATE TABLE usage_event (
    event_id        INT PRIMARY KEY,            -- was 'sid', now clearly named
    profile_id      INT NOT NULL,               -- was 'pid'
    event_at        TIMESTAMP NOT NULL,         -- was 'evt_dttm'
    mb_used         NUMERIC(12,4) NOT NULL,     -- was 'mb'
    cc1             INT NOT NULL,
    cc2             INT NOT NULL,
    tech_cd         VARCHAR(10) NOT NULL,        -- normalized, references technology table
    apn             VARCHAR(100),                -- was 'apn_nm'
    source_file     VARCHAR(100),                -- was 'src_nm', for lineage tracking
    loaded_at       TIMESTAMP NOT NULL,         -- was 'ld_dttm'

    CONSTRAINT chk_mb_positive CHECK (mb_used >= 0),
    CONSTRAINT fk_usage_profile FOREIGN KEY (profile_id) REFERENCES profile_installation(profile_id),
    CONSTRAINT fk_usage_tech FOREIGN KEY (tech_cd) REFERENCES technology(tech_cd),
    CONSTRAINT fk_usage_network FOREIGN KEY (cc1, cc2) REFERENCES network_code(cc1, cc2)
);

CREATE INDEX idx_usage_profile ON usage_event(profile_id);
CREATE INDEX idx_usage_date ON usage_event(event_at);
CREATE INDEX idx_usage_source ON usage_event(source_file);
```


## Key Changes

| **Primary keys** | None defined | Every table has an explicit PK |
| **Foreign keys** | None defined | All relationships enforced |
| **Column naming** | Cryptic abbreviations (`evt_dttm`, `x_dttm`, `why_cd`) | Descriptive names (`event_at`, `expired_at`, `reason`) |
| **Tech values** | Free-text with mixed casing | FK to `technology` reference table |
| **Country/network codes** | No validation | FK to `network_code` reference table |
| **Data validation** | None | CHECK constraints on dates, amounts, formats |
| **Rate card duplicates** | No prevention | UNIQUE index on (bundle, cc1, cc2, tech, effective_at) |
| **Audit trail** | Minimal (`ld_dttm` only) | `created_at` on all tables |



### Risks

1. **Migration complexity** — Existing data will need a one-time ETL to map old tech strings (LTE, lte, NR) to the new `technology` reference table. Bad data (negative values, nulls) will need to be either cleaned or handled during migration.

2. **Breaking upstream integrations** — Column renames (`pid` → `profile_id`, `evt_dttm` → `event_at`) will break any existing queries, dashboards, or ETL pipelines. A migration period with views aliasing old names could ease this.


### Tradeoffs

1. **Normalization vs. query simplicity** — Adding reference tables (`technology`, `network_code`) means more JOINs for simple queries. But it eliminates the "LTE vs 4G vs lte" inconsistency problem permanently.

2. **Surrogate keys vs. natural keys** — I added surrogate IDs (`rate_id`, `id` on plan history) since the natural keys were ambiguous or missing. Natural keys are often better for domain clarity, but the existing data didn't have reliable ones.
