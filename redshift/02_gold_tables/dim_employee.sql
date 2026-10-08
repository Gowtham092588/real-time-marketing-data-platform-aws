CREATE TABLE IF NOT EXISTS gold.dim_employee
(
    employee_key    BIGINT IDENTITY(1,1) NOT NULL,
    employee_id     VARCHAR(100) NOT NULL,
    employee_name   VARCHAR(200),
    employee_email  VARCHAR(255),

    PRIMARY KEY (employee_key)
);