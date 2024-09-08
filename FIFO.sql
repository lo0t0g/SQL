--NOTA: Hay que utilizar OVER PARTITION BY y NO GROUP BY porque es importante almacenar CADA FECHA.
--(ver CASO 2.2.1 Excalidraw RENDIMIENTO_PORTFOLIO.png). Con el GROUP BY perderíamos información
--como la fecha 01/04/2024...


--¡OJO! HAY QUE PONER fecha_operacion, NO fecha_actual. Aunque para el caso vamos a hacerlo con fecha_actual y luego
--ya aplicaremos este procedimiento a la tabla FINANZAS CARTERA.
/*
Lo malo de Transact-SQL es que trabajamos con bluces FOR que leen línea a línea.
*/
--Uso de RAISE NOTICE para comprobar que se ejecuta bien la lógica:

CREATE OR REPLACE FUNCTION "FINANZAS".aplicar_fifo()
RETURNS VOID AS $$
DECLARE
    rec RECORD; --Es de tipo FILA
    comp RECORD;
    ventas_pendientes INTEGER; -- Acumulador para controlar las CANTIDADES que se van vendiendo al aplicar FIFO
BEGIN
    -- Recorremos las operaciones por ISIN y fecha en orden ascendente
    FOR rec IN
        (SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS"
		WHERE tipo_de_operacion = 'VENTA'
        ORDER BY ISIN, fecha)
    LOOP
        IF rec.tipo_de_operacion = 'VENTA' THEN
            ventas_pendientes := rec.cantidad;
            RAISE NOTICE 'Processing VENTA: ISIN=%, fecha=%, cantidad=%', rec.ISIN, rec.fecha, rec.cantidad;

            -- Ajustamos las compras anteriores según el método FIFO
            FOR comp IN
                (SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS"
                WHERE ISIN = rec.ISIN AND tipo_de_operacion = 'COMPRA' AND cantidad > 0 --AND ventas_pendientes > 0
                ORDER BY fecha)
            LOOP
                IF ventas_pendientes <= comp.cantidad THEN
                    UPDATE "FINANZAS"."CARTERA_PRUEBAS"
                    SET cantidad = cantidad - ventas_pendientes
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha;
                    RAISE NOTICE 'Updated COMPRA: ISIN=%, fecha=%, cantidad=%', comp.ISIN, comp.fecha, comp.cantidad - ventas_pendientes;
                    ventas_pendientes := 0; --se consume el acumulador ventas_pendientes con un solo registro
                    EXIT;
                ELSE
                    ventas_pendientes := ventas_pendientes - comp.cantidad;
                    UPDATE "FINANZAS"."CARTERA_PRUEBAS"
                    SET cantidad = 0
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha; --AND ventas_pendientes > 0;
                    RAISE NOTICE 'Updated COMPRA to 0: ISIN=%, fecha=%, cantidad=0', comp.ISIN, comp.fecha;
                END IF;
				
				/*IF ventas_pendientes = 0 THEN
					EXIT;
				END IF;*/
            END LOOP;

            -- Registramos las ventas ajustadas, poniendo todas las cantidad = 0 SÓLO para 
			-- tipo_de_operacion = 'VENTA':
            UPDATE "FINANZAS"."CARTERA_PRUEBAS"
            SET cantidad = ventas_pendientes
            WHERE ISIN = rec.ISIN AND fecha = rec.fecha AND tipo_de_operacion = 'VENTA';
            RAISE NOTICE 'Updated VENTA: ISIN=%, fecha=%, cantidad=%', rec.ISIN, rec.fecha, ventas_pendientes;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql; --Indica que el lenguaje utilizado es PL/pgSQL una extensión de SQL que permite incluir estructuras de control 
                     --DE FLUJO (como bucles y condiciones) y UTILIZAR VARIABLES, algo que no es posible con SQL estándar.


BEGIN;
SELECT "FINANZAS".aplicar_fifo();
--COMMIT; --cuando esté seguro de que el resultado es
		  --correcto, lo haré definitivo en la tabla


SELECT "FINANZAS".aplicar_fifo();

SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS";
--SELECT * FROM "FINANZAS"."CARTERA";



/* 
--Comprobación de que la tabla se actualiza. ASÍ ES, dado que se han 
--modificado los privilegios de la tabla en Properties -> Security

CREATE OR REPLACE FUNCTION "FINANZAS".aplicar_prueba()
RETURNS VOID AS $$
DECLARE
    prueba_actualizacion_tablas RECORD;
BEGIN

	UPDATE "FINANZAS"."CARTERA_PRUEBAS"
    SET cantidad = 0;

END;
$$ LANGUAGE plpgsql;


SELECT "FINANZAS".aplicar_prueba();

SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS";
*/



/*
FASE 2: Introducir la variable BENEFICIO_REALIZADO para solucionar
el problema comentado en Excalidraw.
3. DIFERENCIA ENTRE plusvalias/minusvalias y BENEFICIOS REALIZADOS:
Lo que se muestra en las vistas RENDIMIENTOS_CURRENT_DATE son las plsvalías/minusvalías dados el PRECIO DE VENTA
y CIERRE. Sin embargo, si aplicamos el MÉTODO FIFO, el BENEFICIO REALIZADO VENDRÁ DADO por la resta entre 
PRECIO COMPRA y PRECIO VENTA, pues: 

	-> IMP: Si TIPO_DE_OPERACION = 'VENTA', entonces PRECIO = CIERRE (que proviene de la tabla de RENDIMIENTOS)

Así, implementando el siguiente código, se tiene que la lógica funciona:
TABLA CARTERA_PRUEBAS_BENEFICIOS (INPUT):
isin             fecha          activo   tipo_de_activo         
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	2	100
"US92936U1097"	"2024-04-06"	"WPC"	     "REIT"			"COMPRA"	3	150
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"		1	108
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	4	106
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	6	110
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"		2	112

Resultado esperado:
1. Se vende 1 acción de "ES0112501012" con fecha "2024-04-08". Por lo que el beneficio_realizado
   es igual a (108-100)*1=8.

Así, la tabla queda como sigue, donde "ES0112501012" con fecha"2024-04-01" tiene ahora cantidad=1:

isin             fecha          activo   tipo_de_activo                                        beneficio_realizado       
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	1	100           0
"US92936U1097"	"2024-04-06"	"WPC"	     "REIT"			"COMPRA"	3	150           0
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"		1	108           8
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	4	106           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	6	110           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"		2	112           

NOTA: En las "COMPRA" el beneficio_realizado=0 porque no se ha hecho el beneficio efectivo... -> SIGUIENTE FASE DEL CÓDIGO

2. Se venden 2 acciones de "ES0112501012" con fecha "2024-04-15", en este caso se venden acciones
de dos registros: la acción restante de "ES0112501012" con fecha "2024-04-01" y una acción de 
"ES0112501012"con fecha "2024-04-10": (112-100)*1 + (112-106)*1 = 12 + 6 = 18

Así, la tabla queda como sigue, donde "ES0112501012" con fecha "2024-04-10" tiene ahora cantidad=2:

isin             fecha          activo   tipo_de_activo                                        beneficio_realizado       
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	1	100           0
"US92936U1097"	"2024-04-06"	"WPC"	     "REIT"			"COMPRA"	3	150           0
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"		1	108           8
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	2	106           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	6	110           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"		2	112           18          


OUTPUT (Messages):

NOTICE:  Processing VENTA: ISIN=ES0112501012, fecha=2024-04-08, cantidad=1
NOTICE:  Updated COMPRA: ISIN=ES0112501012, fecha=2024-04-01, cantidad=1
NOTICE:  Updated BENEFICIO: ISIN=ES0112501012, fecha=2024-04-01, cantidad=1,  beneficio_realizado=8 -> BIEN
NOTICE:  Updated VENTA: ISIN=ES0112501012, fecha=2024-04-08, cantidad=0
NOTICE:  Processing VENTA: ISIN=ES0112501012, fecha=2024-04-15, cantidad=2
NOTICE:  Updated COMPRA to 0: ISIN=ES0112501012, fecha=2024-04-01, beneficio_realizado=12, cantidad=0 -> BIEN
NOTICE:  Updated COMPRA: ISIN=ES0112501012, fecha=2024-04-10, cantidad=3
NOTICE:  Updated BENEFICIO: ISIN=ES0112501012, fecha=2024-04-10, cantidad=3,  beneficio_realizado=6 -> BIEN
NOTICE:  Updated VENTA: ISIN=ES0112501012, fecha=2024-04-15, cantidad=0

Successfully run. Total query runtime: 141 msec.
1 rows affected.
*/

CREATE OR REPLACE FUNCTION "FINANZAS".aplicar_fifo_2()
RETURNS VOID AS $$
DECLARE
    rec RECORD; --Es de tipo FILA
    comp RECORD;
    ventas_pendientes INTEGER; -- Acumulador para controlar las CANTIDADES que se van vendiendo al aplicar FIFO
	beneficio_realizado INTEGER; -- Acumulador para controlar los BENEFICIOS que se van realizando al aplicar FIFO
BEGIN
    -- Recorremos las operaciones por ISIN y fecha en orden ascendente
    FOR rec IN
        (SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
		WHERE tipo_de_operacion = 'VENTA'
        ORDER BY ISIN, fecha)
    LOOP
        IF rec.tipo_de_operacion = 'VENTA' THEN
            ventas_pendientes := rec.cantidad;
            RAISE NOTICE 'Processing VENTA: ISIN=%, fecha=%, cantidad=%', rec.ISIN, rec.fecha, rec.cantidad;

            -- Ajustamos las compras anteriores según el método FIFO
            FOR comp IN
                (SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
                WHERE ISIN = rec.ISIN AND tipo_de_operacion = 'COMPRA' AND cantidad > 0 --AND ventas_pendientes > 0
                ORDER BY fecha)
            LOOP
                IF ventas_pendientes <= comp.cantidad THEN
                    UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
                    SET cantidad = cantidad - ventas_pendientes
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha;
                    RAISE NOTICE 'Updated COMPRA: ISIN=%, fecha=%, cantidad=%', comp.ISIN, comp.fecha, comp.cantidad - ventas_pendientes;
					beneficio_realizado := (rec.PRECIO - comp.PRECIO)*ventas_pendientes; --rec.PRECIO - comp.PRECIO = PRECIO VENTA - PRECIO COMPRA
                    RAISE NOTICE 'Updated BENEFICIO: ISIN=%, fecha=%, cantidad=%,  beneficio_realizado=%', comp.ISIN, comp.fecha, comp.cantidad - ventas_pendientes, beneficio_realizado;
                    ventas_pendientes := 0; --se consume el acumulador ventas_pendientes con un solo registro
                    EXIT;
                ELSE
                    ventas_pendientes := ventas_pendientes - comp.cantidad;
					beneficio_realizado := (rec.PRECIO - comp.PRECIO)*ventas_pendientes;
                    UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
                    SET cantidad = 0
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha;
                    RAISE NOTICE 'Updated COMPRA to 0: ISIN=%, fecha=%, beneficio_realizado=%, cantidad=0', comp.ISIN, comp.fecha, beneficio_realizado;

                END IF;
				
				/*IF ventas_pendientes = 0 THEN
					EXIT;
				END IF;*/
            END LOOP;

            -- Registramos las ventas ajustadas, poniendo todas las cantidad = 0 SÓLO para 
			-- tipo_de_operacion = 'VENTA':
            UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
            SET cantidad = ventas_pendientes
            WHERE ISIN = rec.ISIN AND fecha = rec.fecha AND tipo_de_operacion = 'VENTA';
            RAISE NOTICE 'Updated VENTA: ISIN=%, fecha=%, cantidad=%', rec.ISIN, rec.fecha, ventas_pendientes;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


BEGIN;
SELECT "FINANZAS".aplicar_fifo_2();



SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS";



/*
FASE 2 (FINAL): Introducir la variable BENEFICIO_REALIZADO para solucionar
el problema comentado en Excalidraw.
3. DIFERENCIA ENTRE plusvalias/minusvalias y BENEFICIOS REALIZADOS.

En la fase anterior nos dimos cuenta de que hay que hacer dos tareas previas:
El código que he realizado es correcto en cuanto a la lógica el cual 
he validado con los RAISE NOTICE, pero necesito añadir en el código 
dos cosas:

1. Crea la columna  beneficio_realizado. 
2. Hacer un SET beneficio_realizado = 0 para todo registro con "COMPRA",
   dado que no es un beneficio REALIZADO/EFECTIVO.
3. Mejoras en la CALIDAD del código. Por ejemplo, hemos visto que la variable
   beneficio_realizado toma bien dichos beneficios, pero ahora falta ACUMULARLOS
   PARA CADA ISIN, FECHA, para añadirlos a la nueva columna beneficio_realizado.

Así, implementando el siguiente código, se tiene que:
TABLA CARTERA_PRUEBAS_BENEFICIOS (INPUT):
isin             fecha          activo   tipo_de_activo         
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	2	100
"US92936U1097"	"2024-04-06"	"WPC"	     "REIT"			"COMPRA"	3	150
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"		1	108
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	4	106
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	6	110
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"		2	112

Resultado esperado:
1. Se vende 1 acción de "ES0112501012" con fecha "2024-04-08". Por lo que el beneficio_realizado
   es igual a (108-100)*1=8.

Así, la tabla queda como sigue, donde "ES0112501012" con fecha"2024-04-01" tiene ahora cantidad=1:

isin             fecha          activo   tipo_de_activo                                        beneficio_realizado       
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	1	100           0
"US92936U1097"	"2024-04-06"	"WPC"	     "REIT"			"COMPRA"	3	150           0
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"		1	108           8
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	4	106           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	6	110           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"		2	112           

NOTA: En las "COMPRA" el beneficio_realizado=0 porque no se ha hecho el beneficio efectivo... -> SIGUIENTE FASE DEL CÓDIGO

2. Se venden 2 acciones de "ES0112501012" con fecha "2024-04-15", en este caso se venden acciones
de dos registros: la acción restante de "ES0112501012" con fecha "2024-04-01" y una acción de 
"ES0112501012"con fecha "2024-04-10": (112-100)*1 + (112-106)*1 = 12 + 6 = 18 <- ¡ACUMULO siguiendo el método FIFO!

Así, la tabla queda como sigue, donde "ES0112501012" con fecha "2024-04-10" tiene ahora cantidad=2:

isin             fecha          activo   tipo_de_activo                                        beneficio_realizado       
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	1	100           0
"US92936U1097"	"2024-04-06"	"WPC"	     "REIT"			"COMPRA"	3	150           0
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"		1	108           8
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	2	106           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	6	110           0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"		2	112           18    ¡Escribo el BENEFICIO ACUMULADO!      

OUTPUT (Messages):
NOTICE:  Processing VENTA: ISIN=ES0112501012, fecha=2024-04-08, cantidad=1, beneficio_realizado_local=0 -> BIEN
NOTICE:  Updated COMPRA: ISIN=ES0112501012, fecha=2024-04-01, cantidad=1
NOTICE:  Updated BENEFICIO: ISIN=ES0112501012, fecha=2024-04-01, cantidad=1,  beneficio_realizado_local=8 
NOTICE:  Updated VENTA: ISIN=ES0112501012, fecha=2024-04-08, cantidad=0, beneficio_realizado_local=8 -> BIEN
NOTICE:  Processing VENTA: ISIN=ES0112501012, fecha=2024-04-15, cantidad=2, beneficio_realizado_local=0
NOTICE:  Updated COMPRA to 0: ISIN=ES0112501012, fecha=2024-04-01, beneficio_realizado_local=12, cantidad=0 
NOTICE:  Updated COMPRA: ISIN=ES0112501012, fecha=2024-04-10, cantidad=3
NOTICE:  Updated BENEFICIO: ISIN=ES0112501012, fecha=2024-04-10, cantidad=3,  beneficio_realizado_local=18
NOTICE:  Updated VENTA: ISIN=ES0112501012, fecha=2024-04-15, cantidad=0, beneficio_realizado_local=18 -> BIEN

Successfully run. Total query runtime: 59 msec.
1 rows affected.


*/

-- Añadir la columna BENEFICIO_REALIZADO si no existe:
/*
Aplicar un ALTER TABLE en una función es un ERROR DE DISEÑO porque el esquema (modelo) 
de datos no puede variar a medida que agregas datos. Lo mejor es rediseñar la base de 
datos (modificando el esquema, añadiendo tablas si se necesita...) de manera que no 
necesites tener que definir una función que altere las tablas mientras se ejecuta 
una operación... (ver comentario extendido más abajo). 
*/
ALTER TABLE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
ADD COLUMN IF NOT EXISTS beneficio_realizado INTEGER DEFAULT 0;



CREATE OR REPLACE FUNCTION "FINANZAS".aplicar_fifo_2()
RETURNS VOID AS $$
DECLARE
    rec RECORD; --Es de tipo FILA
    comp RECORD;
    ventas_pendientes INTEGER; -- Acumulador para controlar las CANTIDADES que se van vendiendo al aplicar FIFO
	beneficio_realizado_local INTEGER; -- Acumulador para controlar los BENEFICIOS que se van realizando al aplicar FIFO
					--Escribimos beneficio_realizado_local en lugar de solo BENEFICIO_REALIZADO por el siguiente ERROR:
					--Podría referirse tanto a una variable PL/pgSQL como a una columna de una tabla.la referencia a la columna «beneficio_realizado» es ambigua 
					--Esto ocurre porque el nombre de la VARIABLE ACUMULADORA es igual al nombre de la COLUMNA en ALTER TABLE.
BEGIN

    -- Añadir la columna BENEFICIO_REALIZADO si no existe: REALIZADO PREVIAMENTE CON ALTER TABLE
	
    -- Inicializar beneficio_realizado a 0 para todos 
	-- los registros con tipo_de_operación COMPRA (ya hecho en ALTER TABLE):
    --UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
    --SET beneficio_realizado = 0
    --WHERE tipo_de_operacion = 'COMPRA';


    -- Recorremos las operaciones por ISIN y fecha en orden ascendente
    FOR rec IN
        (SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
		WHERE tipo_de_operacion = 'VENTA'
        ORDER BY ISIN, fecha)
    LOOP
        IF rec.tipo_de_operacion = 'VENTA' THEN
            ventas_pendientes := rec.cantidad;
			beneficio_realizado_local := 0; -- Inicializamos el beneficio realizado para esta venta rec
            RAISE NOTICE 'Processing VENTA: ISIN=%, fecha=%, cantidad=%, beneficio_realizado_local=%', rec.ISIN, rec.fecha, rec.cantidad, beneficio_realizado_local;

            -- Ajustamos las compras anteriores según el método FIFO
            FOR comp IN
                (SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
                WHERE ISIN = rec.ISIN AND tipo_de_operacion = 'COMPRA' AND cantidad > 0 --Si detectamos que cantidad=0 en todas las "COMPRA". STOP.
                ORDER BY fecha)
            LOOP
                IF ventas_pendientes <= comp.cantidad THEN
                    UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
                    SET cantidad = cantidad - ventas_pendientes
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha;
                    RAISE NOTICE 'Updated COMPRA: ISIN=%, fecha=%, cantidad=%', comp.ISIN, comp.fecha, comp.cantidad - ventas_pendientes;
					beneficio_realizado_local := beneficio_realizado_local + (rec.PRECIO - comp.PRECIO)*ventas_pendientes; --ACUMULAMOS. rec.PRECIO - comp.PRECIO = PRECIO VENTA - PRECIO COMPRA
                    RAISE NOTICE 'Updated BENEFICIO: ISIN=%, fecha=%, cantidad=%,  beneficio_realizado_local=%', comp.ISIN, comp.fecha, comp.cantidad - ventas_pendientes, beneficio_realizado_local;
                    ventas_pendientes := 0; --se consume el acumulador ventas_pendientes con un solo registro
                    EXIT;
                ELSE
                    ventas_pendientes := ventas_pendientes - comp.cantidad;
					beneficio_realizado_local := beneficio_realizado_local + (rec.PRECIO - comp.PRECIO)*ventas_pendientes;
                    UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
                    SET cantidad = 0
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha;
                    RAISE NOTICE 'Updated COMPRA to 0: ISIN=%, fecha=%, beneficio_realizado_local=%, cantidad=0', comp.ISIN, comp.fecha, beneficio_realizado_local;

                END IF;
				
				/*IF ventas_pendientes = 0 THEN
					EXIT;
				END IF;*/
            END LOOP;

            -- Registramos las ventas ajustadas, poniendo todas las cantidad = 0 SÓLO para 
			-- tipo_de_operacion = 'VENTA' y registramos el acumulado de beneficio_realizado:
            UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
            SET cantidad = ventas_pendientes,
				beneficio_realizado = beneficio_realizado_local --Añadir el resultado de la VARIABLE beneficio_realizado_local
																--a la COLUMNA beneficio_realizado
            WHERE ISIN = rec.ISIN AND fecha = rec.fecha AND tipo_de_operacion = 'VENTA';
            RAISE NOTICE 'Updated VENTA: ISIN=%, fecha=%, cantidad=%, beneficio_realizado_local=%', rec.ISIN, rec.fecha, ventas_pendientes, beneficio_realizado_local;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql; --Indica que el lenguaje utilizado es PL/pgSQL una extensión de SQL que permite incluir estructuras de control 
                     --DE FLUJO (como bucles y condiciones) y UTILIZAR VARIABLES, algo que no es posible con SQL estándar.


BEGIN;
SELECT "FINANZAS".aplicar_fifo_2();



SELECT * FROM "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS";



/* En PostgreSQL, no puedes ejecutar directamente ALTER TABLE dentro de una función PL/pgSQL DECLARE 
   con un manejo de excepciones escribiendo esto:
   
    BEGIN
        ALTER TABLE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
        ADD COLUMN IF NOT EXISTS beneficio_realizado INTEGER DEFAULT 0;
    EXCEPTION
        WHEN duplicate_column THEN
            -- Si la columna ya existe, no hacer nada
            NULL;
    END;

Los ALTER TABLE SÍ pueden ejecutarse dentro de una función con algo del estilo:
CREATE FUNCTION añadir_columna(text) RETURNS void AS $$
DECLARE
	cadena ALIAS FOR $1;
	otra_cadena TEXT;
BEGIN
	otra_cadena := 'ALTER TABLE ' || cadena || ' ADD COLUMN Nueva_Columna varchar(20)';
	EXECUTE otra_cadena;
	return;
END;

$$ LANGUAGE plpgsql;

Pero es un ERROR DE DISEÑO porque el esquema (modelo) de datos no puede variar a 
medida que agregas datos. Lo mejor es rediseñar la base de datos (modificando el 
esquema, añadiendo tablas si se necesita...) de manera que no necesites tener 
que definir una función que altere las tablas mientras se ejecuta una operación...
*/





/*
FASE 2.1 (MEJORA): Aplicamos lo anterior a una nueva tabla 
llamada CARTERA_ACTUALIZADA_FIFO, que ELIMINARÁ todas las COMPRAS
cuya CANTIDAD = 0, mientras que FINANZAS_CARTERA será
la cartera HISTÓRICA (con todos los registros desde que
iniciamos la cartera de valores).


PENDIENTE: Dejar los beneficios_realizados expresados en EUROS.
Para ello tendrá que multiplicarse por tipo_de_cambio.

PENDIENTE: Mejorar lo siguiente:
Estaba entre dos opciones:

OPCIÓN 1 (Crear tabla CARTERA_ACTUALIZADA_FIFO): Y alimentar esta tabla
con el fichero excel CARTERA.csv. Después, alterar el esquema para agregar
la columna beneficios_realizados.

OPCIÓN 2 (Alimentar la tabla CARTERA_ACTUALIZADA_FIFO de otra forma):
Utilizar la tabla CARTERA_PRUEBAS_BENEFICIOS, que se alimenta del Excel
CARTERA.csv y "pasar" los datos de CARTERA_PRUEBAS_BENEFICIOS a 
CARTERA_ACTUALIZADA_FIFO. Creo que esta opción sería la mejor, dado que
si actualizamos el Excel, habría que eliminar la tabla CARTERA_ACTUALIZADA_FIFO
para volver a cargar el Excel y alterar el esquema para agregar la columna
beneficio_realizado.


Un script de Python o utilizar Talend para automatizar el proceso
y hacer:
1. alimentar cada tabla con su Excel
2. ALTER TABLE para añadir la columna beneficio_realizado
3. Ejecutar la función aplicar_fifo_2() para que rellene la
   columna beneficio_realizado anterior.
*/

CREATE TABLE "FINANZAS"."CARTERA_ACTUALIZADA_FIFO" (
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
);

--El Excel que alimenta a la tabla NO contiene la columna
--beneficio_realizado, por eso hay que alterar el esquema
--después:

ALTER TABLE "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
ADD COLUMN IF NOT EXISTS beneficio_realizado INTEGER DEFAULT 0;




CREATE OR REPLACE FUNCTION "FINANZAS".aplicar_fifo_2()
RETURNS VOID AS $$
DECLARE
    rec RECORD; --Es de tipo FILA
    comp RECORD;
    ventas_pendientes INTEGER; -- Acumulador para controlar las CANTIDADES que se van vendiendo al aplicar FIFO
	beneficio_realizado_local INTEGER; -- Acumulador para controlar los BENEFICIOS que se van realizando al aplicar FIFO
					--Escribimos beneficio_realizado_local en lugar de solo BENEFICIO_REALIZADO por el siguiente ERROR:
					--Podría referirse tanto a una variable PL/pgSQL como a una columna de una tabla.la referencia a la columna «beneficio_realizado» es ambigua 
					--Esto ocurre porque el nombre de la VARIABLE ACUMULADORA es igual al nombre de la COLUMNA en ALTER TABLE.
BEGIN

    -- Añadir la columna BENEFICIO_REALIZADO si no existe: REALIZADO PREVIAMENTE CON ALTER TABLE
	
    -- Inicializar beneficio_realizado a 0 para todos 
	-- los registros con tipo_de_operación COMPRA (ya hecho en ALTER TABLE):
    --UPDATE "FINANZAS"."CARTERA_PRUEBAS_BENEFICIOS"
    --SET beneficio_realizado = 0
    --WHERE tipo_de_operacion = 'COMPRA';


    -- Recorremos las operaciones por ISIN y fecha en orden ascendente
    FOR rec IN
        (SELECT * FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
		WHERE tipo_de_operacion = 'VENTA'
        ORDER BY ISIN, fecha)
    LOOP
        IF rec.tipo_de_operacion = 'VENTA' THEN
            ventas_pendientes := rec.cantidad;
			beneficio_realizado_local := 0; -- Inicializamos el beneficio realizado para esta venta rec
            RAISE NOTICE 'Processing VENTA: ISIN=%, fecha=%, cantidad=%, beneficio_realizado_local=%', rec.ISIN, rec.fecha, rec.cantidad, beneficio_realizado_local;

            -- Ajustamos las compras anteriores según el método FIFO
            FOR comp IN
                (SELECT * FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
                WHERE ISIN = rec.ISIN AND tipo_de_operacion = 'COMPRA' AND cantidad > 0 --Si detectamos que cantidad=0 en todas las "COMPRA". STOP.
                ORDER BY fecha)
            LOOP
                IF ventas_pendientes <= comp.cantidad THEN
                    UPDATE "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
                    SET cantidad = cantidad - ventas_pendientes
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha;
                    RAISE NOTICE 'Updated COMPRA: ISIN=%, fecha=%, cantidad=%', comp.ISIN, comp.fecha, comp.cantidad - ventas_pendientes;
					beneficio_realizado_local := beneficio_realizado_local + (rec.PRECIO - comp.PRECIO)*ventas_pendientes; --ACUMULAMOS. rec.PRECIO - comp.PRECIO = PRECIO VENTA - PRECIO COMPRA
                    RAISE NOTICE 'Updated BENEFICIO: ISIN=%, fecha=%, cantidad=%,  beneficio_realizado_local=%', comp.ISIN, comp.fecha, comp.cantidad - ventas_pendientes, beneficio_realizado_local;
                    ventas_pendientes := 0; --se consume el acumulador ventas_pendientes con un solo registro
                    EXIT;
                ELSE
                    ventas_pendientes := ventas_pendientes - comp.cantidad;
					beneficio_realizado_local := beneficio_realizado_local + (rec.PRECIO - comp.PRECIO)*ventas_pendientes;
                    UPDATE "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
                    SET cantidad = 0
                    WHERE ISIN = comp.ISIN AND fecha = comp.fecha;
                    RAISE NOTICE 'Updated COMPRA to 0: ISIN=%, fecha=%, beneficio_realizado_local=%, cantidad=0', comp.ISIN, comp.fecha, beneficio_realizado_local;

                END IF;
				
				/*IF ventas_pendientes = 0 THEN
					EXIT;
				END IF;*/
            END LOOP;

            -- Registramos las ventas ajustadas, poniendo todas las cantidad = 0 SÓLO para 
			-- tipo_de_operacion = 'VENTA' y registramos el acumulado de beneficio_realizado:
            UPDATE "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
            SET cantidad = ventas_pendientes,
				beneficio_realizado = beneficio_realizado_local --Añadir el resultado de la VARIABLE beneficio_realizado_local
																--a la COLUMNA beneficio_realizado
            WHERE ISIN = rec.ISIN AND fecha = rec.fecha AND tipo_de_operacion = 'VENTA';
            RAISE NOTICE 'Updated VENTA: ISIN=%, fecha=%, cantidad=%, beneficio_realizado_local=%', rec.ISIN, rec.fecha, ventas_pendientes, beneficio_realizado_local;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql; --Indica que el lenguaje utilizado es PL/pgSQL una extensión de SQL que permite incluir estructuras de control 
                     --DE FLUJO (como bucles y condiciones) y UTILIZAR VARIABLES, algo que no es posible con SQL estándar.


BEGIN;
SELECT "FINANZAS".aplicar_fifo_2();



SELECT * FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO";






/* PENDIENTE AUTOMATIZAR RECARGAR DE BASE DE DATOS

Un script de Python o utilizar Talend para automatizar el proceso
y hacer:
1. Alimentar cada tabla con su Excel
2. ALTER TABLE para añadir la columna beneficio_realizado
3. Ejecutar la función aplicar_fifo_2() para que rellene la
   columna beneficio_realizado anterior.
*/
































