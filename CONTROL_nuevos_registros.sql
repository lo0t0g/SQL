CREATE TABLE "FINANZAS"."CARTERA_CONTROL" (
	ISIN VARCHAR(60),
	FECHA DATE,
	ACTIVO VARCHAR(10),
	TIPO_DE_ACTIVO VARCHAR(30),
	TIPO_DE_OPERACION VARCHAR(30),
	CANTIDAD INTEGER,
	PRECIO DOUBLE PRECISION,
	DIVISA VARCHAR(10),
	TIPO_DE_CAMBIO DOUBLE PRECISION,
	PRECIO_EUR DOUBLE PRECISION,
	--DT_REFERENCE DATE, --Campo añadido al Excel por necesidad para distinguir del método FIFO.
	DT_LOAD TIMESTAMP DEFAULT CURRENT_TIMESTAMP --Este campo se va a rellenar en la base de datos. ALTER TABLE.
);

/* NOTA: Suponemos que no compramos la misma acción (mismo ISIN) dos veces en un mismo día...
por lo que ISIN+DT_LOAD forman campo PK
OBJETIVO: Cruzar esta tabla con una Point in Time (PIT) table.

La PIT_incremental debe almacenar DT_LOAD, DT_REFERENCE (puedo
hacer varias ventas en distintos días referidas al mismo LOTE de 
acciones)
*/

--ALTER TABLE "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
--ADD COLUMN IF NOT EXISTS beneficio_realizado INTEGER DEFAULT 0;

SELECT * FROM "FINANZAS"."CARTERA_CONTROL";



CREATE TABLE "FINANZAS"."ultima_fecha_de_carga" (
  id SERIAL PRIMARY KEY,
  delta_fecha_de_carga TIMESTAMP
);
-- Inicialmente, insertamos un valor predeterminado para inicializar:
INSERT INTO "FINANZAS"."ultima_fecha_de_carga" (delta_fecha_de_carga) VALUES ('1970-01-01 00:00:00');

/*
--OBJETIVO: Que PostgreSQL pueda leer directamente desde el archivo CSV usando la tabla extranjera file_cartera:
--Creamos previamente un SERVER que define la ubicación de los CSV (Excel):
El problema es que con file_fdw hay que configurar los PERMISOS DE LECTURA para que postgres pueda leerlos. 
Lo que lo hace un camino complejo...

CREATE EXTENSION file_fdw
    SCHEMA "FINANZAS";
--file_fdw = contenedor de datos externos para acceso a archivos planos (texto simple, texto sencillo o texto sin formato)
--SELECT * FROM pg_available_extensions ORDER BY name; --Para ver las extensiones en PostgreSQL

CREATE SERVER file_server
FOREIGN DATA WRAPPER file_fdw; --Pasa los datos de Excel (csv) al PostgreSQL. Permite acceder a una tabla o esquema en una base de datos desde otra.

--Creamos una tabla que lea el CSV:
CREATE FOREIGN TABLE "FINANZAS"."file_cartera" (
	ISIN VARCHAR(60),
	FECHA DATE,
	ACTIVO VARCHAR(10),
	TIPO_DE_ACTIVO VARCHAR(30),
	TIPO_DE_OPERACION VARCHAR(30),
	CANTIDAD INTEGER,
	PRECIO DOUBLE PRECISION,
	DIVISA VARCHAR(10),
	TIPO_DE_CAMBIO DOUBLE PRECISION,
	PRECIO_EUR DOUBLE PRECISION
) SERVER file_server
OPTIONS (filename 'C:/Users/tmaxi/Downloads/Curso PowerBI/PROYECTO_GESTION_FINANZAS/datasets/CARTERA_PRUEBAS.csv', format 'csv', header 'true');


--OTRA FORMA: Creamos la tabla TEMPORAL para cargar los datos del CSV:
CREATE TABLE "FINANZAS"."tmp_cartera" (
	ISIN VARCHAR(60),
	FECHA DATE,
	ACTIVO VARCHAR(10),
	TIPO_DE_ACTIVO VARCHAR(30),
	TIPO_DE_OPERACION VARCHAR(30),
	CANTIDAD INTEGER,
	PRECIO DOUBLE PRECISION,
	DIVISA VARCHAR(10),
	TIPO_DE_CAMBIO DOUBLE PRECISION,
	PRECIO_EUR DOUBLE PRECISION,
	--DT_REFERENCE DATE, --Campo añadido al Excel por necesidad para distinguir del método FIFO.
	DT_LOAD TIMESTAMP --Este campo se va a rellenar en la base de datos. ALTER TABLE.
);

COPY "FINANZAS"."tmp_cartera" 
FROM 'C:/Users/tmaxi/Downloads/Curso PowerBI/PROYECTO_GESTION_FINANZAS/datasets/CARTERA_PRUEBAS.csv'
WITH (FORMAT csv, HEADER true);

\copy "FINANZAS"."tmp_cartera" FROM 'C:/Users/User/Downloads/Curso PowerBI/PROYECTO_GESTION_FINANZAS/datasets/CARTERA_PRUEBAS.csv' WITH (FORMAT csv, HEADER true)
*/


--PROCEDIMIENTO DEL CONTROL DE ÚLTIMA CARGA:
CREATE TABLE "FINANZAS"."tmp_cartera" (
	ISIN VARCHAR(60),
	FECHA DATE,
	ACTIVO VARCHAR(10),
	TIPO_DE_ACTIVO VARCHAR(30),
	TIPO_DE_OPERACION VARCHAR(30),
	CANTIDAD INTEGER,
	PRECIO DOUBLE PRECISION,
	DIVISA VARCHAR(10),
	TIPO_DE_CAMBIO DOUBLE PRECISION,
	PRECIO_EUR DOUBLE PRECISION,
	--DT_REFERENCE DATE, --Campo añadido al Excel por necesidad para distinguir del método FIFO.
	DT_LOAD TIMESTAMP DEFAULT CURRENT_TIMESTAMP --Este campo se va a rellenar en la base de datos. ALTER TABLE.
);

-- SOLUCIÓN 1. 
--Paso 1: Obtener el último timestamp analizado
BEGIN;
DO $$
DECLARE
  ultima_fecha TIMESTAMP;
BEGIN
  -- Obtener el último timestamp de la tabla ultima_fecha_de_carga
  SELECT delta_fecha_de_carga INTO ultima_fecha FROM "FINANZAS"."ultima_fecha_de_carga" ORDER BY id DESC LIMIT 1;

  -- Paso 2: Insertar solo los registros nuevos en la tabla CARTERA
	  INSERT INTO "FINANZAS"."CARTERA_CONTROL"
  SELECT *
  FROM "FINANZAS"."tmp_cartera"
  WHERE DT_LOAD > ultima_fecha;
  --ON CONFLICT (ISIN, FECHA) DO NOTHING; --Control de duplicados si cambiamos FECHA por TIMESTAMP

  -- Paso 3: Actualizar el último timestamp analizado
  -- Primero, obtenemos el máximo timestamp de los datos insertados
  IF (SELECT COUNT(*) FROM "FINANZAS"."tmp_cartera" WHERE DT_LOAD > ultima_fecha) > 0 THEN
    UPDATE "FINANZAS"."ultima_fecha_de_carga"
    SET delta_fecha_de_carga = (SELECT MAX(DT_LOAD) FROM "FINANZAS"."tmp_cartera" WHERE DT_LOAD > ultima_fecha)
    WHERE id = (SELECT MAX(id) FROM "FINANZAS"."ultima_fecha_de_carga");
  END IF;
END $$;


SELECT * FROM "FINANZAS"."CARTERA_CONTROL";
SELECT * FROM "FINANZAS"."tmp_cartera";
SELECT * FROM "FINANZAS"."ultima_fecha_de_carga";


/* ARREGLAR FECHA DE LA TABLA DE CONTROL DE ULTIMA_FECHA_DE_CARGA:

--En la siguiente podemos añadir la última fecha de carga conocida/correcta, en vez del año 1970:
INSERT INTO "FINANZAS"."ultima_fecha_de_carga" (delta_fecha_de_carga) VALUES ('1970-01-01 00:00:00');
--Fechas de prueba: "2024-06-06 19:41:43.995249", "2024-06-07 19:41:43.995249"

--Actualizar el último timestamp analizado:
UPDATE "FINANZAS"."ultima_fecha_de_carga"
SET delta_fecha_de_carga = (SELECT MAX(DT_LOAD) FROM "FINANZAS"."tmp_cartera")
WHERE id = (SELECT MAX(id) FROM "FINANZAS"."ultima_fecha_de_carga");
*/



/*
FUNCIONA LO ANTERIOR: El problema es que hay que cargar previamente la tabla TMP_CARTERA con los nuevos registros, 
para luego mandar estos registros a la tabla CARTERA_CONTROL, por lo que no merece la pena... Sí que merece
la pena el proceso ETL.
*/



--CASO 1: EL CAMPO DT_LOAD LO RELLENAMOS A MANO EN LOS EXCEL (no está automatizado):

--Necesitamos un trigger para que cada vez que ocurra el EVENTO = alimentar la tabla temporal TEMP_CARTERA, 
--se ejecute el la función control_insercion_nuevos_registros:

--Primero creamos la función:
CREATE OR REPLACE FUNCTION "FINANZAS".control_insercion_nuevos_registros()
RETURNS TRIGGER AS $$
DECLARE
  ultima_fecha TIMESTAMP;
BEGIN
  -- Obtener el último timestamp de la tabla ultima_fecha_de_carga
  SELECT delta_fecha_de_carga INTO ultima_fecha FROM "FINANZAS"."ultima_fecha_de_carga" ORDER BY id DESC LIMIT 1;

  -- Paso 2: Insertar solo los registros nuevos en la tabla CARTERA
	  INSERT INTO "FINANZAS"."CARTERA_CONTROL"
  SELECT *
  FROM "FINANZAS"."tmp_cartera"
  WHERE DT_LOAD > ultima_fecha;
  --ON CONFLICT (NEW.ISIN, NEW.FECHA) DO NOTHING; --Control de duplicados si cambiamos FECHA por TIMESTAMP

  -- Paso 3: Actualizar el último timestamp analizado
  -- Primero, obtenemos el máximo timestamp de los datos insertados
  IF (SELECT COUNT(*) FROM "FINANZAS"."tmp_cartera" WHERE DT_LOAD > ultima_fecha) > 0 THEN
    UPDATE "FINANZAS"."ultima_fecha_de_carga"
    SET delta_fecha_de_carga = (SELECT MAX(DT_LOAD) FROM "FINANZAS"."tmp_cartera" WHERE DT_LOAD > ultima_fecha)
    WHERE id = (SELECT MAX(id) FROM "FINANZAS"."ultima_fecha_de_carga");
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


--Creamos el TRIGGER que se ejecutará automáticamente:
--BEGIN;
CREATE TRIGGER trigger_control_insercion_nuevos_registros
AFTER INSERT ON "FINANZAS"."tmp_cartera"
FOR EACH ROW
EXECUTE PROCEDURE "FINANZAS".control_insercion_nuevos_registros();

BEGIN;
TRUNCATE TABLE "FINANZAS"."tmp_cartera"




-- AUTOMATIZACIÓN DEL CAMPO DT_LOAD:   AUTOMATIZACIÓN DEL CAMPO DT_LOAD:    AUTOMATIZACIÓN DEL CAMPO DT_LOAD:


--AUTOMATIZACIÓN DEL CAMPO DT_LOAD:

CREATE TABLE "FINANZAS"."CARTERA_CONTROL_2" (
	ISIN VARCHAR(60),
	FECHA TIMESTAMP,
	ACTIVO VARCHAR(10),
	TIPO_DE_ACTIVO VARCHAR(30),
	TIPO_DE_OPERACION VARCHAR(30),
	CANTIDAD INTEGER,
	PRECIO DOUBLE PRECISION,
	DIVISA VARCHAR(10),
	TIPO_DE_CAMBIO DOUBLE PRECISION,
	PRECIO_EUR DOUBLE PRECISION,
	--DT_REFERENCE DATE, --Campo añadido al Excel por necesidad para distinguir del método FIFO.
	DT_LOAD TIMESTAMP DEFAULT CURRENT_TIMESTAMP --Este campo se va a rellenar en la base de datos. ALTER TABLE.
);

CREATE TABLE "FINANZAS"."tmp_cartera_2" (
	ISIN VARCHAR(60),
	FECHA TIMESTAMP,
	ACTIVO VARCHAR(10),
	TIPO_DE_ACTIVO VARCHAR(30),
	TIPO_DE_OPERACION VARCHAR(30),
	CANTIDAD INTEGER,
	PRECIO DOUBLE PRECISION,
	DIVISA VARCHAR(10),
	TIPO_DE_CAMBIO DOUBLE PRECISION,
	PRECIO_EUR DOUBLE PRECISION,
	--DT_REFERENCE DATE, --Campo añadido al Excel por necesidad para distinguir del método FIFO.
	DT_LOAD TIMESTAMP DEFAULT CURRENT_TIMESTAMP --Este campo se va a rellenar en la base de datos. ALTER TABLE.
);

DROP TABLE "FINANZAS"."CARTERA_CONTROL_2";
DROP TABLE "FINANZAS"."tmp_cartera_2";



SELECT * FROM "FINANZAS"."CARTERA_CONTROL_2";
SELECT * FROM "FINANZAS"."tmp_cartera_2";
SELECT * FROM "FINANZAS"."ultima_fecha_de_carga_2";


CREATE TABLE "FINANZAS"."ultima_fecha_de_carga_2" (
  id SERIAL PRIMARY KEY,
  delta_fecha_de_carga TIMESTAMP
);

-- Inicialmente, insertamos un valor predeterminado
INSERT INTO "FINANZAS"."ultima_fecha_de_carga_2" (delta_fecha_de_carga) VALUES ('1970-01-01 00:00:00');




CREATE OR REPLACE FUNCTION "FINANZAS".control_insercion_nuevos_registros_2()
RETURNS TRIGGER AS $$
DECLARE
  ultima_fecha TIMESTAMP;
BEGIN
  -- Obtener el último timestamp de la tabla ultima_fecha_de_carga
  SELECT delta_fecha_de_carga INTO ultima_fecha FROM "FINANZAS"."ultima_fecha_de_carga_2" ORDER BY id DESC LIMIT 1;

  -- Paso 2: Insertar solo los registros nuevos en la tabla CARTERA_CONTROL. SOLUCIÓN A LA AUTOMATIZACIÓN DE LA COLUMNA DT_LOAD
	INSERT INTO "FINANZAS"."CARTERA_CONTROL_2"
		SELECT * FROM "FINANZAS"."tmp_cartera_2"
		WHERE (ISIN, FECHA) NOT IN (
			SELECT ISIN, FECHA
			FROM "FINANZAS"."tmp_cartera_2"
			GROUP BY ISIN, FECHA
			HAVING COUNT(*) > 1
		) AND DT_LOAD > ultima_fecha;
	
  -- Paso 3: Actualizar el último timestamp analizado
  -- Primero, obtenemos el máximo timestamp de los datos insertados
  IF (SELECT COUNT(*) FROM "FINANZAS"."tmp_cartera_2" WHERE DT_LOAD > ultima_fecha) > 0 THEN
    UPDATE "FINANZAS"."ultima_fecha_de_carga_2"
    SET delta_fecha_de_carga = (SELECT MAX(DT_LOAD) FROM "FINANZAS"."tmp_cartera_2" WHERE DT_LOAD > ultima_fecha)
    WHERE id = (SELECT MAX(id) FROM "FINANZAS"."ultima_fecha_de_carga_2");
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;




--Creamos el TRIGGER que se ejecutará automáticamente:
BEGIN;
CREATE OR REPLACE TRIGGER trigger_control_insercion_nuevos_registros_2
AFTER INSERT ON "FINANZAS"."tmp_cartera_2"
FOR EACH ROW
EXECUTE PROCEDURE "FINANZAS".control_insercion_nuevos_registros_2();


SELECT * FROM "FINANZAS"."CARTERA_CONTROL_2";
SELECT * FROM "FINANZAS"."tmp_cartera_2";
SELECT * FROM "FINANZAS"."ultima_fecha_de_carga_2";

-- Inicialmente, insertamos un valor predeterminado
INSERT INTO "FINANZAS"."ultima_fecha_de_carga_2" (delta_fecha_de_carga) VALUES ('1970-01-01 00:00:00');


--BEGIN;
TRUNCATE TABLE "FINANZAS"."CARTERA_CONTROL_2";
TRUNCATE TABLE "FINANZAS"."tmp_cartera_2";
TRUNCATE TABLE "FINANZAS"."ultima_fecha_de_carga_2";





--SOLUCIÓN 2   SOLUCIÓN 2   SOLUCIÓN 2    SOLUCIÓN 2     SOLUCIÓN 2     SOLUCIÓN 2




/*

NOTA: Lo IDEAL sería cargar las tablas a través de Talend, pues en Talend podemos crear una consulta
de tal forma que cargue en las tablas SÓLO LOS REGISTROS ACTUALES.
Pero dado que no lo estamos haciendo así, vamos a tener que cargar el Excel en la tabla y luego
hacer un procedimiento que "limpie" la tabla.
*/
















