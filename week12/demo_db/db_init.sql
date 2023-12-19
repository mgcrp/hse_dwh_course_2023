CREATE TABLE manufacturers (
manufacturer_id SERIAL PRIMARY KEY,
manufacturer_name VARCHAR(100) NOT NULL,
manufacturer_legal_entity VARCHAR(100) NOT NULL
);

CREATE TABLE categories (
category_id SERIAL PRIMARY KEY,
category_name VARCHAR(100) NOT NULL
);

CREATE TABLE products (
category_id BIGINT NOT NULL,
manufacturer_id BIGINT NOT NULL,
product_id SERIAL PRIMARY KEY,
product_name VARCHAR(255) NOT NULL,
product_picture_url VARCHAR(255) NOT NULL,
product_description VARCHAR(255) NOT NULL,
product_age_restriction INT NOT NULL,
CONSTRAINT category_fk FOREIGN KEY (category_id) REFERENCES categories (category_id),
CONSTRAINT manufacturer_fk FOREIGN KEY (manufacturer_id) REFERENCES manufacturers (manufacturer_id)
);

CREATE TABLE stores (
store_id SERIAL PRIMARY KEY,
store_name VARCHAR(100) NOT NULL,
store_country VARCHAR(255) NOT NULL,
store_city VARCHAR(255) NOT NULL,
store_address VARCHAR(255) NOT NULL
);

CREATE TABLE customers (
customer_id SERIAL PRIMARY KEY,
customer_fname VARCHAR(100) NOT NULL,
customer_lname VARCHAR(100) NOT NULL,
customer_gender VARCHAR(100) NOT NULL,
customer_phone VARCHAR(100) NOT NULL
);

CREATE TABLE price_change (
product_id BIGINT NOT NULL,
price_change_ts TIMESTAMP NOT NULL,
new_price BIGINT NOT NULL,
CONSTRAINT product_fk FOREIGN KEY (product_id) REFERENCES products (product_id),
PRIMARY KEY (product_id, price_change_ts)
);

CREATE TABLE deliveries (
delivery_id BIGINT PRIMARY KEY,
store_id BIGINT NOT NULL,
product_id BIGINT NOT NULL,
delivery_date DATE NOT NULL,
product_count INTEGER NOT NULL,
CONSTRAINT store_fk FOREIGN KEY (store_id) REFERENCES stores (store_id),
CONSTRAINT product_fk FOREIGN KEY (product_id) REFERENCES products (product_id)
);

CREATE TABLE purchases (
store_id BIGINT NOT NULL,
customer_id BIGINT NOT NULL,
purchase_id SERIAL PRIMARY KEY,
purchase_date TIMESTAMP NOT NULL,
purchase_payment_type VARCHAR(100) NOT NULL,
CONSTRAINT store_fk FOREIGN KEY (store_id) REFERENCES stores (store_id),
CONSTRAINT customer_fk FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

CREATE TABLE purchase_items (
product_id BIGINT NOT NULL,
purchase_id BIGINT NOT NULL,
product_count BIGINT NOT NULL,
product_price BIGINT NOT NULL,
CONSTRAINT product_fk FOREIGN KEY (product_id) REFERENCES products (product_id),
CONSTRAINT purchase_fk FOREIGN KEY (purchase_id) REFERENCES purchases (purchase_id),
PRIMARY KEY (product_id, purchase_id)
);