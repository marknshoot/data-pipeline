-- Raw warehouse layer: one table per OLTP source table, loaded from the lake
-- (Parquet) by `warehouse.load_raw`.
--
-- Engine choice: ReplacingMergeTree(updated_at) keyed by the business key.
-- A row that is updated after its extract interval is picked up again by a later
-- interval, so the same key can appear in several lake files. Replacing by the
-- newest `updated_at` makes repeated loads converge to one row per key. Merges are
-- asynchronous, so exact queries use FINAL (dbt staging does this explicitly).
--
-- Types mirror the Parquet schema written by ingestion/oltp_extract.py.

CREATE TABLE IF NOT EXISTS raw.sellers
(
    seller_id Int64,
    name String,
    email String,
    city String,
    country String,
    rating Decimal(3, 2),
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS raw.categories
(
    category_id Int32,
    name String,
    slug String,
    parent_category_id Nullable(Int32),
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS raw.users
(
    user_id Int64,
    email String,
    full_name String,
    phone Nullable(String),
    city String,
    country String,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS raw.products
(
    product_id Int64,
    seller_id Int64,
    category_id Int32,
    name String,
    sku String,
    price Decimal(14, 2),
    cost Decimal(14, 2),
    stock Int32,
    is_active Bool,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS raw.orders
(
    order_id Int64,
    user_id Int64,
    status String,
    currency String,
    shipping_city String,
    total_amount Decimal(14, 2),
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS raw.order_items
(
    order_item_id Int64,
    order_id Int64,
    product_id Int64,
    quantity Int32,
    unit_price Decimal(14, 2),
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS raw.payments
(
    payment_id Int64,
    order_id Int64,
    method String,
    amount Decimal(14, 2),
    status String,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY payment_id;
