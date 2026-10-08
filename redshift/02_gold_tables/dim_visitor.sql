CREATE TABLE IF NOT EXISTS gold.dim_visitor
(
    visitor_key  BIGINT IDENTITY(1,1) NOT NULL,
    visitor_id   VARCHAR(255) NOT NULL,
    email        VARCHAR(255),
    ip_address   VARCHAR(45),

    PRIMARY KEY (visitor_key)
);
