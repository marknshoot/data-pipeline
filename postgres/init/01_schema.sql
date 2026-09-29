-- ShopStream source OLTP schema (database: shop).
-- Applied automatically on first Postgres volume init; safe to re-run by hand
-- (make schema) because every statement is IF NOT EXISTS.
--
-- Every table carries created_at / updated_at so the batch extractor can pull
-- increments by updated_at watermark. An UPDATE trigger keeps updated_at honest,
-- which is exactly the column the warehouse snapshot logic depends on.

-- ---------------------------------------------------------------- helpers
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ------------------------------------------------------------- categories
CREATE TABLE IF NOT EXISTS categories (
    category_id        integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name               text        NOT NULL,
    slug               text        NOT NULL UNIQUE,
    parent_category_id integer     REFERENCES categories (category_id),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- sellers
CREATE TABLE IF NOT EXISTS sellers (
    seller_id  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        NOT NULL,
    email      text        NOT NULL UNIQUE,
    city       text        NOT NULL,
    country    char(2)     NOT NULL DEFAULT 'ID',
    -- Seller reputation, 0.00 – 5.00.
    rating     numeric(3, 2) NOT NULL DEFAULT 0 CHECK (rating BETWEEN 0 AND 5),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------ users
CREATE TABLE IF NOT EXISTS users (
    user_id    bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email      text        NOT NULL UNIQUE,
    full_name  text        NOT NULL,
    phone      text,
    city       text        NOT NULL,
    country    char(2)     NOT NULL DEFAULT 'ID',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- --------------------------------------------------------------- products
CREATE TABLE IF NOT EXISTS products (
    product_id bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_id  bigint      NOT NULL REFERENCES sellers (seller_id),
    category_id integer    NOT NULL REFERENCES categories (category_id),
    name       text        NOT NULL,
    sku        text        NOT NULL UNIQUE,
    price      numeric(14, 2) NOT NULL CHECK (price > 0),
    cost       numeric(14, 2) NOT NULL CHECK (cost >= 0),
    stock      integer     NOT NULL DEFAULT 0 CHECK (stock >= 0),
    is_active  boolean     NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------- orders
-- shipping_city is a deliberate snapshot of the user's city at order time:
-- users move, and revenue-by-city must not be rewritten retroactively (SCD2 story).
CREATE TABLE IF NOT EXISTS orders (
    order_id      bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id       bigint      NOT NULL REFERENCES users (user_id),
    status        text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')),
    currency      char(3)     NOT NULL DEFAULT 'IDR',
    shipping_city text        NOT NULL,
    total_amount  numeric(14, 2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------ order_items
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id      bigint      NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id    bigint      NOT NULL REFERENCES products (product_id),
    quantity      integer     NOT NULL CHECK (quantity > 0),
    -- Price captured at purchase time; products.price keeps changing afterwards.
    unit_price    numeric(14, 2) NOT NULL CHECK (unit_price >= 0),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- --------------------------------------------------------------- payments
CREATE TABLE IF NOT EXISTS payments (
    payment_id bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id   bigint      NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    method     text        NOT NULL
        CHECK (method IN ('card', 'bank_transfer', 'va', 'ewallet', 'cod')),
    amount     numeric(14, 2) NOT NULL CHECK (amount >= 0),
    status     text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'authorized', 'captured', 'failed', 'refunded')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------ updated_at triggers
DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'categories', 'sellers', 'users', 'products', 'orders', 'order_items', 'payments'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%1$s_updated_at ON %1$s', t);
        EXECUTE format(
            'CREATE TRIGGER trg_%1$s_updated_at BEFORE UPDATE ON %1$s '
            'FOR EACH ROW EXECUTE FUNCTION set_updated_at()',
            t
        );
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------- indexes
-- The updated_at indexes exist for the incremental extractor (Phase 2).
CREATE INDEX IF NOT EXISTS idx_users_updated_at       ON users (updated_at);
CREATE INDEX IF NOT EXISTS idx_sellers_updated_at     ON sellers (updated_at);
CREATE INDEX IF NOT EXISTS idx_categories_updated_at  ON categories (updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated_at    ON products (updated_at);
CREATE INDEX IF NOT EXISTS idx_products_seller_id     ON products (seller_id);
CREATE INDEX IF NOT EXISTS idx_products_category_id   ON products (category_id);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at      ON orders (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_created_at      ON orders (created_at);
CREATE INDEX IF NOT EXISTS idx_orders_user_id         ON orders (user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status          ON orders (status);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at ON order_items (updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at    ON payments (updated_at);
CREATE INDEX IF NOT EXISTS idx_payments_order_id      ON payments (order_id);
