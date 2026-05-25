-- ============================================================
-- CONNEXA CORE MASTER DATA
-- Product Master Canonical Model - DDL v1.0
-- PostgreSQL
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- SCHEMAS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS master_data;
CREATE SCHEMA IF NOT EXISTS supplier;
CREATE SCHEMA IF NOT EXISTS logistics;
CREATE SCHEMA IF NOT EXISTS replenishment;
CREATE SCHEMA IF NOT EXISTS commercial;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================
-- MASTER DATA: PRODUCT
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data.product (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ext_code            varchar(50) NOT NULL,
    description         varchar(255) NOT NULL,
    short_description   varchar(120),
    product_type        varchar(50) DEFAULT 'MERCHANDISE',

    brand_code          varchar(50),
    brand_name          varchar(120),

    category_code       varchar(50),
    category_name       varchar(120),
    subcategory_code    varchar(50),
    subcategory_name    varchar(120),

    weight_variable     boolean DEFAULT false,
    own_brand           boolean DEFAULT false,
    active              boolean DEFAULT true,
    discontinued        boolean DEFAULT false,

    valid_from          date DEFAULT CURRENT_DATE,
    valid_to            date,

    source_system       varchar(50) DEFAULT 'CONNEXA',
    legacy_payload      jsonb,

    created_at          timestamp without time zone DEFAULT now(),
    updated_at          timestamp without time zone DEFAULT now(),
    created_by          uuid,
    updated_by          uuid,

    CONSTRAINT uq_product_ext_code UNIQUE (ext_code),
    CONSTRAINT chk_product_validity CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_product_description
    ON master_data.product (description);

CREATE INDEX IF NOT EXISTS idx_product_category
    ON master_data.product (category_code, subcategory_code);

CREATE INDEX IF NOT EXISTS idx_product_active
    ON master_data.product (active);

-- ============================================================
-- PRODUCT BARCODE
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data.product_barcode (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id          uuid NOT NULL REFERENCES master_data.product(id),
    barcode             varchar(50) NOT NULL,
    barcode_type        varchar(30) DEFAULT 'EAN13',
    primary_barcode     boolean DEFAULT false,
    active              boolean DEFAULT true,

    created_at          timestamp without time zone DEFAULT now(),
    updated_at          timestamp without time zone DEFAULT now(),

    CONSTRAINT uq_product_barcode UNIQUE (barcode)
);

CREATE INDEX IF NOT EXISTS idx_product_barcode_product
    ON master_data.product_barcode (product_id);

-- ============================================================
-- PRODUCT CLASSIFICATION
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data.product_classification (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id              uuid NOT NULL REFERENCES master_data.product(id),

    classification_type     varchar(50) NOT NULL,
    classification_value    varchar(100) NOT NULL,

    valid_from              date DEFAULT CURRENT_DATE,
    valid_to                date,
    active                  boolean DEFAULT true,

    created_at              timestamp without time zone DEFAULT now(),
    updated_at              timestamp without time zone DEFAULT now(),

    CONSTRAINT chk_product_classification_validity
        CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_product_classification_product
    ON master_data.product_classification (product_id);

CREATE INDEX IF NOT EXISTS idx_product_classification_type
    ON master_data.product_classification (classification_type, classification_value);

-- ============================================================
-- SITE
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data.site (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ext_code            varchar(50) NOT NULL,
    name                varchar(150) NOT NULL,
    site_type           varchar(50) NOT NULL,
    region_code         varchar(50),
    region_name         varchar(120),
    active              boolean DEFAULT true,

    created_at          timestamp without time zone DEFAULT now(),
    updated_at          timestamp without time zone DEFAULT now(),

    CONSTRAINT uq_site_ext_code UNIQUE (ext_code)
);

CREATE INDEX IF NOT EXISTS idx_site_type
    ON master_data.site (site_type);

-- ============================================================
-- PRODUCT SITE
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data.product_site (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    product_id              uuid NOT NULL REFERENCES master_data.product(id),
    site_id                 uuid NOT NULL REFERENCES master_data.site(id),
    supplying_site_id       uuid REFERENCES master_data.site(id),

    active_for_sale         boolean DEFAULT false,
    active_for_purchase     boolean DEFAULT false,
    active_for_transfer     boolean DEFAULT false,
    active_on_mix           boolean DEFAULT false,

    supply_type             varchar(50),
    min_stock_units         numeric(18,4),
    max_stock_units         numeric(18,4),
    shelf_capacity          numeric(18,4),
    linear_facing           numeric(18,4),
    gondola_capacity        numeric(18,4),

    valid_from              date DEFAULT CURRENT_DATE,
    valid_to                date,
    active                  boolean DEFAULT true,

    source_system           varchar(50) DEFAULT 'CONNEXA',
    legacy_payload          jsonb,

    created_at              timestamp without time zone DEFAULT now(),
    updated_at              timestamp without time zone DEFAULT now(),
    created_by              uuid,
    updated_by              uuid,

    CONSTRAINT uq_product_site UNIQUE (product_id, site_id),
    CONSTRAINT chk_product_site_validity CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_product_site_product
    ON master_data.product_site (product_id);

CREATE INDEX IF NOT EXISTS idx_product_site_site
    ON master_data.product_site (site_id);

CREATE INDEX IF NOT EXISTS idx_product_site_supply
    ON master_data.product_site (supply_type, supplying_site_id);

-- ============================================================
-- SITE CLUSTER
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data.site_cluster (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    code            varchar(50) NOT NULL,
    name            varchar(150) NOT NULL,
    cluster_type    varchar(50),
    active          boolean DEFAULT true,

    created_at      timestamp without time zone DEFAULT now(),
    updated_at      timestamp without time zone DEFAULT now(),

    CONSTRAINT uq_site_cluster_code UNIQUE (code)
);

-- ============================================================
-- PRODUCT SITE ASSORTMENT
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data.product_site_assortment (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    product_site_id     uuid NOT NULL REFERENCES master_data.product_site(id),
    site_cluster_id     uuid REFERENCES master_data.site_cluster(id),

    assortment_role     varchar(50),
    mandatory           boolean DEFAULT false,

    valid_from          date DEFAULT CURRENT_DATE,
    valid_to            date,
    active              boolean DEFAULT true,

    created_at          timestamp without time zone DEFAULT now(),
    updated_at          timestamp without time zone DEFAULT now(),

    CONSTRAINT chk_product_site_assortment_validity
        CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_product_site_assortment_product_site
    ON master_data.product_site_assortment (product_site_id);

CREATE INDEX IF NOT EXISTS idx_product_site_assortment_cluster
    ON master_data.product_site_assortment (site_cluster_id);

-- ============================================================
-- SUPPLIER
-- ============================================================

CREATE TABLE IF NOT EXISTS supplier.supplier (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ext_code                varchar(50) NOT NULL,
    name                    varchar(255) NOT NULL,
    tax_identification      varchar(50),
    active                  boolean DEFAULT true,

    created_at              timestamp without time zone DEFAULT now(),
    updated_at              timestamp without time zone DEFAULT now(),

    CONSTRAINT uq_supplier_ext_code UNIQUE (ext_code)
);

CREATE INDEX IF NOT EXISTS idx_supplier_name
    ON supplier.supplier (name);

-- ============================================================
-- SUPPLIER PRODUCT
-- ============================================================

CREATE TABLE IF NOT EXISTS supplier.supplier_product (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    supplier_id             uuid NOT NULL REFERENCES supplier.supplier(id),
    product_id              uuid NOT NULL REFERENCES master_data.product(id),

    supplier_product_code   varchar(80),
    primary_supplier        boolean DEFAULT false,
    active_for_purchase     boolean DEFAULT true,

    lead_time_days          numeric(10,2),
    min_purchase_qty        numeric(18,4),
    purchase_multiple       numeric(18,4),
    purchase_factor         numeric(18,4),
    cost_reference          numeric(18,4),

    valid_from              date DEFAULT CURRENT_DATE,
    valid_to                date,
    active                  boolean DEFAULT true,

    source_system           varchar(50) DEFAULT 'CONNEXA',
    legacy_payload          jsonb,

    created_at              timestamp without time zone DEFAULT now(),
    updated_at              timestamp without time zone DEFAULT now(),
    created_by              uuid,
    updated_by              uuid,

    CONSTRAINT uq_supplier_product UNIQUE (supplier_id, product_id),
    CONSTRAINT chk_supplier_product_validity CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_supplier_product_supplier
    ON supplier.supplier_product (supplier_id);

CREATE INDEX IF NOT EXISTS idx_supplier_product_product
    ON supplier.supplier_product (product_id);

CREATE INDEX IF NOT EXISTS idx_supplier_product_primary
    ON supplier.supplier_product (product_id, primary_supplier);

-- ============================================================
-- PRODUCT LOGISTICS
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics.product_logistics (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    product_id              uuid NOT NULL REFERENCES master_data.product(id),

    purchase_factor         numeric(18,4),
    sale_factor             numeric(18,4),
    transfer_factor         numeric(18,4),

    box_units               numeric(18,4),
    pallet_units            numeric(18,4),
    pallet_layers           numeric(18,4),
    boxes_per_layer         numeric(18,4),

    gross_weight            numeric(18,4),
    net_weight              numeric(18,4),
    volume                  numeric(18,6),

    refrigerated            boolean DEFAULT false,
    fragile                 boolean DEFAULT false,
    stackable               boolean DEFAULT true,
    shelf_life_days         integer,

    valid_from              date DEFAULT CURRENT_DATE,
    valid_to                date,
    active                  boolean DEFAULT true,

    created_at              timestamp without time zone DEFAULT now(),
    updated_at              timestamp without time zone DEFAULT now(),
    created_by              uuid,
    updated_by              uuid,

    CONSTRAINT chk_product_logistics_validity CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_product_logistics_product
    ON logistics.product_logistics (product_id);

-- ============================================================
-- REPLENISHMENT POLICY
-- ============================================================

CREATE TABLE IF NOT EXISTS replenishment.product_site_replenishment (
    id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    product_site_id             uuid NOT NULL REFERENCES master_data.product_site(id),

    forecast_algorithm_code     varchar(80),
    forecast_parameters         jsonb,

    replenishment_method        varchar(80),
    target_coverage_days        numeric(10,2),
    safety_stock_days           numeric(10,2),

    min_order_qty               numeric(18,4),
    order_multiple              numeric(18,4),

    lead_time_days              numeric(10,2),
    preparation_days            numeric(10,2),
    transit_days                numeric(10,2),

    replenishment_frequency     varchar(80),

    valid_from                  date DEFAULT CURRENT_DATE,
    valid_to                    date,
    active                      boolean DEFAULT true,

    created_at                  timestamp without time zone DEFAULT now(),
    updated_at                  timestamp without time zone DEFAULT now(),
    created_by                  uuid,
    updated_by                  uuid,

    CONSTRAINT chk_product_site_replenishment_validity
        CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_product_site_replenishment_product_site
    ON replenishment.product_site_replenishment (product_site_id);

CREATE INDEX IF NOT EXISTS idx_product_site_replenishment_algorithm
    ON replenishment.product_site_replenishment (forecast_algorithm_code);

-- ============================================================
-- COMMERCIAL POLICY
-- ============================================================

CREATE TABLE IF NOT EXISTS commercial.product_commercial_policy (
    id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    product_id                  uuid NOT NULL REFERENCES master_data.product(id),

    margin_target               numeric(10,4),
    price_sensitivity           varchar(50),
    promotional_enabled         boolean DEFAULT true,
    seasonal                    boolean DEFAULT false,
    competition_sensitive       boolean DEFAULT false,
    pricing_strategy            varchar(80),

    valid_from                  date DEFAULT CURRENT_DATE,
    valid_to                    date,
    active                      boolean DEFAULT true,

    created_at                  timestamp without time zone DEFAULT now(),
    updated_at                  timestamp without time zone DEFAULT now(),
    created_by                  uuid,
    updated_by                  uuid,

    CONSTRAINT chk_product_commercial_policy_validity
        CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX IF NOT EXISTS idx_product_commercial_policy_product
    ON commercial.product_commercial_policy (product_id);

-- ============================================================
-- AUDIT LOG
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.product_master_audit_log (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    entity_schema   varchar(80) NOT NULL,
    entity_name     varchar(120) NOT NULL,
    entity_id       uuid NOT NULL,

    operation       varchar(20) NOT NULL,
    field_name      varchar(120),
    old_value       text,
    new_value       text,

    changed_by      uuid,
    changed_at      timestamp without time zone DEFAULT now(),
    change_reason   text,
    source_system   varchar(50) DEFAULT 'CONNEXA',

    metadata        jsonb
);

CREATE INDEX IF NOT EXISTS idx_product_master_audit_entity
    ON audit.product_master_audit_log (entity_schema, entity_name, entity_id);

CREATE INDEX IF NOT EXISTS idx_product_master_audit_changed_at
    ON audit.product_master_audit_log (changed_at);

-- ============================================================
-- INTEGRATION / OUTBOX
-- ============================================================

CREATE SCHEMA IF NOT EXISTS integration;

CREATE TABLE IF NOT EXISTS integration.product_master_outbox (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    event_type          varchar(120) NOT NULL,
    entity_schema       varchar(80) NOT NULL,
    entity_name         varchar(120) NOT NULL,
    entity_id           uuid NOT NULL,

    payload             jsonb NOT NULL,

    status              varchar(30) DEFAULT 'PENDING',
    retry_count         integer DEFAULT 0,
    last_error          text,

    created_at          timestamp without time zone DEFAULT now(),
    processed_at        timestamp without time zone
);

CREATE INDEX IF NOT EXISTS idx_product_master_outbox_status
    ON integration.product_master_outbox (status, created_at);

CREATE INDEX IF NOT EXISTS idx_product_master_outbox_entity
    ON integration.product_master_outbox (entity_schema, entity_name, entity_id);