-- A small web shop: the example dataset of DataFarm (see datasets/README.md).

CREATE TABLE customers (
    id          INTEGER PRIMARY KEY,
    name        VARCHAR(64) NOT NULL,
    country     CHAR(2) NOT NULL,
    segment     VARCHAR(16) NOT NULL,
    signup_date DATE NOT NULL
);

CREATE TABLE products (
    id       INTEGER PRIMARY KEY,
    name     VARCHAR(64) NOT NULL,
    category VARCHAR(16) NOT NULL,
    price    DECIMAL(8, 2) NOT NULL,
    supplier VARCHAR(32)
);

CREATE TABLE orders (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers (id),
    order_date  DATE NOT NULL,
    status      VARCHAR(12) NOT NULL,
    note        TEXT
);

CREATE TABLE order_items (
    order_id      INTEGER NOT NULL REFERENCES orders (id),
    line_no       INTEGER NOT NULL,
    product_id    INTEGER NOT NULL REFERENCES products (id),
    quantity      INTEGER NOT NULL,
    discount_code VARCHAR(8),
    PRIMARY KEY (order_id, line_no)
);

-- customer_id is NULL for anonymous reviews
CREATE TABLE reviews (
    id          INTEGER PRIMARY KEY,
    product_id  INTEGER NOT NULL REFERENCES products (id),
    customer_id INTEGER REFERENCES customers (id),
    rating      INTEGER NOT NULL,
    body        TEXT
);
