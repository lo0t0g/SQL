CREATE TABLE "FINANZAS"."CARTERA_REAL_NO_FIFO" (

    isin character varying(60),
    fecha timestamp without time zone,
    fecha_referencia timestamp without time zone DEFAULT '9999-01-01 00:00:00'::timestamp without time zone,
    activo character varying(10),
    tipo_de_activo character varying(30),
    tipo_de_operacion character varying(30),
    cantidad integer,
    precio double precision,
    divisa character varying(10),
    tipo_de_cambio double precision,
    precio_eur double precision
);

CREATE TABLE "FINANZAS"."CARTERA_DEFINITIVA" (

    isin character varying(60),
    fecha timestamp without time zone,
    fecha_referencia timestamp without time zone DEFAULT '9999-01-01 00:00:00'::timestamp without time zone,
    activo character varying(10),
    tipo_de_activo character varying(30),
    tipo_de_operacion character varying(30),
    cantidad integer,
    precio double precision,
    divisa character varying(10),
    tipo_de_cambio double precision,
    precio_eur double precision
);



SELECT * FROM "FINANZAS"."CARTERA_DEFINITIVA";





-- PRIMERO CREAMOS UN PROCEDURE PARA TRATAR LAS COMPRAS Y VENTAS DE LA TACLA CARTERA_DEFINITIVA, creando la nueva tabla
--CARTERA_REAL_NO_FIFO, donde hemos restado las COMPRAS y las VENTAS correspondientes para cada ISIN y FECHA_REFERENCIA:


-- CÓDIGO 1 (CORRECTO, FUNCIONA). PRIMERO ACTUALIZAMOS LA TABLA CARTERA_DEFINITIVA con las COMPRAS y VENTAS siguiendo la lógica
-- de dividendos_no_fifo.ods -> CARTERA_REAL_NO_FIFO:

--BEGIN;
CREATE OR REPLACE FUNCTION "FINANZAS".actualizar_cantidad_cartera_real_no_fifo()
RETURNS VOID AS $$
DECLARE
    rec_venta RECORD;
    cantidad_venta INT;
BEGIN
    -- Iterar sobre cada registro de ventas en CARTERA_DEFINITIVA
    FOR rec_venta IN
        SELECT * FROM "FINANZAS"."CARTERA_REAL_NO_FIFO" -- Solo queremos tomar los registros donde tipo_de_operacion = 'VENTA'
        WHERE tipo_de_operacion = 'VENTA' AND cantidad > 0 -- Esto lo hacemos dado que las VENTAS procesadas (i.e, con cantidad = 0)
        ORDER BY fecha									   -- se siguen guardando como registros y no los queremos volver a tomar en futuras
    LOOP												   -- ingestas de datos.
		--IF rec_venta.tipo_de_operacion = 'VENTA' THEN	
        	cantidad_venta := rec_venta.cantidad;
			
        	RAISE NOTICE 'Procesando VENTA: ISIN = %, Fecha = %, Cantidad = %, Fecha de Referencia = %',
            rec_venta.isin, rec_venta.fecha, rec_venta.cantidad, rec_venta.fecha_referencia;
		
        -- Reducimos cantidad de las COMPRAS correspondientes en CARTERA_REAL_NO_FIFO:
        UPDATE "FINANZAS"."CARTERA_REAL_NO_FIFO"
        SET cantidad = cantidad - cantidad_venta
        WHERE isin = rec_venta.isin
          AND fecha = rec_venta.fecha_referencia -- La FECHA (de COMPRA) sea igual a la FECHA_REFERENCIA (de VENTA)
          AND tipo_de_operacion = 'COMPRA'
          AND cantidad > 0;

        RAISE NOTICE 'Actualizando COMPRA: ISIN = %, Cantidad COMPRA restante después de la actualización = %', 
            rec_venta.isin, rec_venta.cantidad;
		
        -- Actualizamos la cantidad de las VENTAS, que pasa a ser 0:
        UPDATE "FINANZAS"."CARTERA_REAL_NO_FIFO"
        SET cantidad = 0 			   -- Dejamos la CANTIDAD de tipo_de_operación = VENTA en 0
		WHERE isin = rec_venta.isin
          AND fecha = rec_venta.fecha  -- Estamos procesando este registro de VENTA (rec_venta) en concreto. 
		  AND tipo_de_operacion = 'VENTA';
        
		cantidad_venta = 0; -- se consumen toda la CANTIDAD de ventas para ese ISIN y FECHA_REFERENCIA.

		RAISE NOTICE 'Actualizando cantidad VENTA: ISIN = %, Cantidad COMPRA restante después de la actualización = %', rec_venta.isin, cantidad_venta;

	END LOOP;
END;
$$ LANGUAGE plpgsql;

BEGIN;
SELECT * FROM "FINANZAS".actualizar_cantidad_cartera_real_no_fifo()




-- CÓDIGO 2 (CORRECTO, FUNCIONA). DEFINITIVO. El código anterior está bien, pero si queremos crear un TRIGGER que se ejecute utilizando
-- la función "actualizar_cantidad_cartera_real_no_fifo()" de manera automática, sin tener que ejecutar
/*
SELECT "FINANZAS".actualizar_cantidad_cartera_real_no_fifo();

manualmente para aplicar la lógica de la función, debemos modificar ligeramente el código anterior:
*/

--BEGIN;
CREATE OR REPLACE FUNCTION "FINANZAS".actualizar_cantidad_cartera_real_no_fifo_2()
RETURNS TRIGGER AS $$
DECLARE
    rec_venta RECORD;
    cantidad_venta INT;
BEGIN
    	-- Obtener la fila activadora (el registro de tipo 'VENTA' que disparó el trigger)
    	rec_venta := NEW; -- Al crear el TRIGGER, definiremos qué es NEW para tomar solo ventas con cantidad > 0 (DOS FORMAS)
		
	/*	Antes teníamos esto:
    FOR rec_venta IN
        SELECT * FROM "FINANZAS"."CARTERA_REAL_NO_FIFO"    -- Solo queremos tomar los registros donde tipo_de_operacion = 'VENTA'
        WHERE tipo_de_operacion = 'VENTA' AND cantidad > 0 -- Esto lo hacemos dado que las VENTAS procesadas (i.e, con cantidad = 0)
        ORDER BY fecha									   -- se siguen guardando como registros y no los queremos volver a tomar en futuras
    LOOP												   -- ingestas de datos.
	*/ 
	   IF rec_venta.tipo_de_operacion = 'VENTA' AND rec_venta.cantidad > 0 THEN
        cantidad_venta := rec_venta.cantidad;

		RAISE NOTICE 'Procesando VENTA: ISIN = %, Fecha = %, Cantidad = %, Fecha de Referencia = %',
		rec_venta.isin, rec_venta.fecha, rec_venta.cantidad, rec_venta.fecha_referencia;
		
        -- Reducimos cantidad de las COMPRAS correspondientes en CARTERA_REAL_NO_FIFO:
        UPDATE "FINANZAS"."CARTERA_REAL_NO_FIFO"
        SET cantidad = cantidad - cantidad_venta
        WHERE isin = rec_venta.isin
          AND fecha = rec_venta.fecha_referencia -- La FECHA (de COMPRA) sea igual a la FECHA_REFERENCIA (de VENTA)
          AND tipo_de_operacion = 'COMPRA'
          AND cantidad > 0;

        RAISE NOTICE 'Actualizando COMPRA: ISIN = %, Cantidad COMPRA restante después de la actualización = %', 
            rec_venta.isin, rec_venta.cantidad;
		
		-- Actualizamos la cantidad de la VENTA activadora a 0
		rec_venta.cantidad := 0; -- se consumen toda la CANTIDAD de ventas para ese ISIN y FECHA_REFERENCIA.

		RAISE NOTICE 'Actualizando cantidad VENTA: ISIN = %, Cantidad COMPRA restante después de la actualización = %', rec_venta.isin, cantidad;
		
	END IF;
END;
$$ LANGUAGE plpgsql;


-- CREAMOS EL TRIGGER:

BEGIN;
CREATE TRIGGER actualizar_cantidad_cartera_real_no_fifo_2
AFTER INSERT OR UPDATE ON "FINANZAS"."CARTERA_REAL_NO_FIFO"
FOR EACH ROW
EXECUTE FUNCTION "FINANZAS".actualizar_cantidad_cartera_real_no_fifo_2();















