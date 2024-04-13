CREATE DATABASE bbdd_futbol;

\c bbdd_futbol;

CREATE TABLE ligas (id SERIAL PRIMARY KEY,
					Liga VARCHAR(255),
					Url VARCHAR(255),
					Siglas CHAR(3));