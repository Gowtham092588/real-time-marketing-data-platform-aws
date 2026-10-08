CREATE TABLE IF NOT EXISTS gold.dim_owner
(
    owner_key  BIGINT IDENTITY(1,1) NOT NULL,
    lead_owner VARCHAR(200) NOT NULL,

    PRIMARY KEY (owner_key)
);