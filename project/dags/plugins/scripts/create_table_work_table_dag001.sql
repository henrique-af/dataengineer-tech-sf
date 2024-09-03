CREATE TABLE IF NOT EXISTS work_table_dag001 (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code VARCHAR(20),
    country VARCHAR(100),
    order_date DATE,
    delivery_date DATE,
    status VARCHAR(50)
);