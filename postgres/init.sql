CREATE DATABASE bbdd_futbol;

\c bbdd_futbol;

CREATE TABLE ligas (Id SERIAL PRIMARY KEY,
					Liga VARCHAR(255),
					Url VARCHAR(255),
					Siglas CHAR(3));

CREATE TABLE equipos (Id SERIAL PRIMARY KEY,
						Equipo VARCHAR(255),
						Url VARCHAR(255),
						Equipo_Url VARCHAR(255),
						Id_Liga INTEGER,
						FOREIGN KEY (id_Liga) REFERENCES ligas (Id) ON DELETE CASCADE);