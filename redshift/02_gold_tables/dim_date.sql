CREATE TABLE IF NOT EXISTS gold.dim_date
(
    date_key        INT NOT NULL,
    full_date       DATE NOT NULL,
    day_of_week     VARCHAR(20),
    week            INT,
    month           INT,
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN,

    PRIMARY KEY (date_key)
);