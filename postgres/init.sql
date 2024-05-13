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
						Url_Imagen VARCHAR(255) DEFAULT NULL,
						FOREIGN KEY (id_Liga) REFERENCES ligas (Id) ON DELETE CASCADE);

ALTER TABLE equipos ADD COLUMN Codigo_Url VARCHAR(255);

CREATE OR REPLACE FUNCTION obtener_codigo_url(url_texto VARCHAR)
RETURNS VARCHAR AS $$
DECLARE
    codigo VARCHAR;
BEGIN
    SELECT substring(url_texto FROM '/squads/([a-zA-Z0-9]+)/') INTO codigo;
    RETURN codigo;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION actualizar_codigo_url()
RETURNS TRIGGER AS $$
BEGIN
    NEW.Codigo_Url:=obtener_codigo_url(NEW.Url);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_actualizar_codigo_url
BEFORE INSERT ON equipos
FOR EACH ROW
EXECUTE FUNCTION actualizar_codigo_url();

CREATE TABLE partidos (Id SERIAL PRIMARY KEY,
    					Competicion VARCHAR(255),
						Ronda VARCHAR(255),
						Fecha DATE,
						Hora VARCHAR(255),
						Local VARCHAR(255),
						Marcador VARCHAR(255),
						Visitante VARCHAR(255),
						Publico INT,
						Sede VARCHAR(255),
						CodEquipo1 VARCHAR(255),
						CodEquipo2 VARCHAR(255));