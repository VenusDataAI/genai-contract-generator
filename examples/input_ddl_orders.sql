CREATE TABLE silver_orders (
    order_id        VARCHAR(36)     NOT NULL,
    user_id         BIGINT          NOT NULL,
    customer_email  VARCHAR(255)    NOT NULL,
    status          VARCHAR(50)     NOT NULL DEFAULT 'pending',
    total_amount    NUMERIC(12, 2)  NOT NULL,
    discount_amount NUMERIC(12, 2),
    tax_amount      NUMERIC(12, 2),
    currency_code   VARCHAR(3)      NOT NULL DEFAULT 'BRL',
    payment_method  VARCHAR(50),
    is_gift         BOOLEAN         NOT NULL DEFAULT FALSE,
    notes           TEXT,
    created_at      TIMESTAMP       NOT NULL,
    updated_at      TIMESTAMP,
    shipped_at      TIMESTAMP,
    PRIMARY KEY (order_id)
);
