DROP DATABASE IF EXISTS project;

CREATE DATABASE project;

\c project;

START TRANSACTION;

CREATE TABLE restaurants(
	id INTEGER NOT NULL PRIMARY KEY,
	position INTEGER,
	name VARCHAR(100),
	score VARCHAR(20),
	ratings VARCHAR(20),
	category VARCHAR(10000),
	price_range VARCHAR(10),
	full_address VARCHAR(1000),
	zip_code VARCHAR(20),
	lat decimal(10, 6),
	lng decimal(10, 6)
);

CREATE TABLE menus(
	restaurant_id INTEGER,
	category VARCHAR(2000),
	name VARCHAR(2000),
	description VARCHAR(10000),
	price VARCHAR(20)
);

ALTER TABLE menus ADD CONSTRAINT fk_menus_restaurant_id_id FOREIGN KEY(restaurant_id) REFERENCES restaurants (id);

\COPY restaurants FROM 'data/restaurants.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

\COPY menus FROM 'data/restaurant-menus.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

COMMIT;
	