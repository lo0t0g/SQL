/*
NECESIDAD: 

OPCIÓN 1: La razón de hacer esto en SQL es establecer los 3 CASOS (ver CONTROL_cantidad_dividendos.excalidraw) dado que 
NO PODREMOS USAR LA FUNCIÓN DAX RELATED() en PowerBI, pues la relación entre las tablas CARTERA-DIVIDENDOS
es de VARIOS A VARIOS. 
*/


SELECT * FROM
(SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
)

/*
"ES0112501012"	"2024-04-10"	"COMPRA"	3
"ES0112501012"	"2024-04-15"	"COMPRA"	6
"US92936U1097"	"2024-04-06"	"COMPRA"	3

NOTA: Importante mostrar las fechas de adquisición de esos activos pues
hay que compararlos con la fecha_ex_div
*/

SELECT * FROM
(SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad_fifo FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
) AS tabla_cartera_fifo
INNER JOIN
(
SELECT * FROM "FINANZAS"."DIVIDENDOS" 
) AS dividendos
ON tabla_cartera_fifo.ISIN = dividendos.ISIN

/*               fecha                     cantidad_fifo                             fecha_ex_div    fecha_cobro  cantidad         
"ES0112501012"	"2024-04-15"	"COMPRA"	6	"RENTA_DIVIDENDOS"	"ES0112501012"	"2024-04-01"	"2024-04-02"	2	"EBRO"	"CONSUMO BASICO"	"EUR"	1	2
"ES0112501012"	"2024-04-10"	"COMPRA"	3	"RENTA_DIVIDENDOS"	"ES0112501012"	"2024-04-01"	"2024-04-02"	2	"EBRO"	"CONSUMO BASICO"	"EUR"	1	2
"US92936U1097"	"2024-04-06"	"COMPRA"	3	"RENTA_DIVIDENDOS"	"US92936U1097"	"2024-04-06"	"2024-04-07"	3	"WPC"	"REIT"	"USD"	0.9406	2.8218
"ES0112501012"	"2024-04-15"	"COMPRA"	6	"RENTA_DIVIDENDOS"	"ES0112501012"	"2024-04-12"	"2024-04-02"	2	"EBRO"	"CONSUMO BASICO"	"EUR"	1	2
"ES0112501012"	"2024-04-10"	"COMPRA"	3	"RENTA_DIVIDENDOS"	"ES0112501012"	"2024-04-12"	"2024-04-02"	2	"EBRO"	"CONSUMO BASICO"	"EUR"	1	2
*/

--PARTE 1 / CASO 1: (MEJORA PARTE 1 más adelante)
--Esta tabla muestra los dividendos que se PODRÍAN COBRAR si mantenemos la posición hasta la fecha_ex_div:
SELECT dividendos.categoria_nivel_dos, tabla_cartera_fifo.ISIN, dividendos.activo, dividendos.tipo_de_activo,
tabla_cartera_fifo.FECHA as fecha_operacion, dividendos.fecha_ex_div, dividendos.fecha_cobro, tabla_cartera_fifo.cantidad_fifo 
FROM
(SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad_fifo FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
) AS tabla_cartera_fifo
INNER JOIN
(
SELECT * FROM "FINANZAS"."DIVIDENDOS" 
) AS dividendos
ON tabla_cartera_fifo.ISIN = dividendos.ISIN
WHERE tabla_cartera_fifo.fecha < dividendos.fecha_ex_div


--PARTE 2 / CASO 2:
--Esta tabla muestra los dividendos que se VAN A COBRAR, dado que  hemos mantenido la posición hasta la fecha_ex_div
--y no hemos llegado todavía a la fecha_cobro, que es cuando se hace efectivo el pago de dividendos:
--Y, además, el CURRENT_TIMESTAMP < fecha_cobro.

--HOY = CURRENT_TIMESTAMP = 12/06/2024:
--INSERT INTO "FINANZAS"."DIVIDENDOS" VALUES ('RENTA_DIVIDENDOS',	'ES0112501012',	'10/06/2024', '10/07/2024', '4', 'EBRO', 'CONSUMO BASICO', 'EUR', '1', '2')

SELECT dividendos.categoria_nivel_dos, tabla_cartera_fifo.ISIN, dividendos.activo, dividendos.tipo_de_activo,
tabla_cartera_fifo.FECHA as fecha_operacion, dividendos.fecha_ex_div, dividendos.fecha_cobro, tabla_cartera_fifo.cantidad_fifo 
FROM
(SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad_fifo FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
) AS tabla_cartera_fifo
INNER JOIN
(
SELECT * FROM "FINANZAS"."DIVIDENDOS" 
) AS dividendos
ON tabla_cartera_fifo.ISIN = dividendos.ISIN
WHERE tabla_cartera_fifo.fecha < dividendos.fecha_ex_div AND tabla_cartera_fifo.fecha < dividendos.fecha_cobro
AND CURRENT_TIMESTAMP >= dividendos.fecha_ex_div AND CURRENT_TIMESTAMP < dividendos.fecha_cobro


--PARTE 3 / CASO 3:
--Esta tabla muestra los dividendos que se HAN COBRADO, dado que hemos mantenido la posición hasta la fecha_ex_div
--y hemos llegado o superado la fecha_cobro, que es cuando se hace efectivo el pago de dividendos.
--Además, el CURRENT_TIMESTAMP >= fecha_cobro:

SELECT dividendos.categoria_nivel_dos, tabla_cartera_fifo.ISIN, dividendos.activo, dividendos.tipo_de_activo,
tabla_cartera_fifo.FECHA as fecha_operacion, dividendos.fecha_ex_div, dividendos.fecha_cobro, tabla_cartera_fifo.cantidad_fifo 
FROM
(SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad_fifo FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
) AS tabla_cartera_fifo
INNER JOIN
(
SELECT * FROM "FINANZAS"."DIVIDENDOS" 
) AS dividendos
ON tabla_cartera_fifo.ISIN = dividendos.ISIN
WHERE tabla_cartera_fifo.fecha < dividendos.fecha_ex_div AND CURRENT_TIMESTAMP >= dividendos.fecha_cobro




--MEJORA PARTE 1: Dividendos que se PODRÍAN COBRAR si mantenemos la posición hasta la fecha_ex_div.
/*
La tabla almacena de manera acumulativa el HISTÓRICO DE DIVIDENDOS. Imaginemos el siguiente caso:

"RENTA_DIVIDENDOS"	"ES0112501012"	"EBRO"	"CONSUMO BASICO"	"2024-04-10"	"2024-04-12"	"2024-04-02"	3
"RENTA_DIVIDENDOS"	"ES0112501012"	"EBRO"	"CONSUMO BASICO"	"2024-04-15"	"2024-06-10"	"2024-07-10"	6

y CURRENT_DATE = 20-08-2024. 
Entonces, se seguirán mostrando los registros anteriores, pues satisfacen tabla_cartera_fifo.fecha < dividendos.fecha_ex_div. 

Es por ello que necesitamos exigir además que CURRENT_TIMESTAMP < fecha_ex_div
*/


SELECT dividendos.categoria_nivel_dos, tabla_cartera_fifo.ISIN, dividendos.activo, dividendos.tipo_de_activo,
tabla_cartera_fifo.FECHA as fecha_operacion, dividendos.fecha_ex_div, dividendos.fecha_cobro, tabla_cartera_fifo.cantidad_fifo 
FROM
	(SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad_fifo FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
	WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
	GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
	) AS tabla_cartera_fifo
INNER JOIN
	(
	SELECT * FROM "FINANZAS"."DIVIDENDOS" 
	) AS dividendos
ON tabla_cartera_fifo.ISIN = dividendos.ISIN
WHERE tabla_cartera_fifo.fecha < dividendos.fecha_ex_div --AND CURRENT_TIMESTAMP < dividendos.fecha_ex_div


SELECT * FROM "FINANZAS"."DIVIDENDOS"

 

--MODIFICACIÓN AUTOMÁTICA DEL CAMPO "CANTIDAD" DE LA TABLA DIVIDENDOS A PARTIR DE LA TABLA "CARTERA_ACTUALIZADA_FIFO":
/*
El campo "CANTIDAD" debe actualizarse acorde a las diferentes operaciones de COMPRA y VENTA a la hora de recibir
los dividendos. Así, todo ISIN restante que se mantenga hasta la fecha_ex_div (INCLUIDO), serán dividendos que se
cobrarán.
*/




SELECT dividendos.categoria_nivel_dos, tabla_cartera_fifo.ISIN, dividendos.activo, dividendos.tipo_de_activo,
tabla_cartera_fifo.FECHA as fecha_operacion, dividendos.fecha_ex_div, dividendos.fecha_cobro, tabla_cartera_fifo.cantidad_fifo 
FROM
	(SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad_fifo FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
	WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
	GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
	) AS tabla_cartera_fifo
INNER JOIN
	(
	SELECT * FROM "FINANZAS"."DIVIDENDOS" 
	) AS dividendos
ON tabla_cartera_fifo.ISIN = dividendos.ISIN
WHERE tabla_cartera_fifo.fecha < dividendos.fecha_ex_div --AND CURRENT_TIMESTAMP < dividendos.fecha_ex_div


/* El registro ISIN="ES0112501012" con fecha_operacion="2024-04-10" está duplicado Y ES CORRECTO. Las únicas compras que quedan
en la tabla CARTERA_ACTUALIZADA_FIFO son 2 registros "ES0112501012"	"2024-04-15" y "ES0112501012"	"2024-04-10":
																			cantidad_fifo
"US92936U1097"	"2024-04-06"	"WPC"	"REIT"				"COMPRA"			3		   150	"USD"	0.94	423	0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"			6		   110	"EUR"	1		660	0
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"				0		   108	"EUR"	1		108	8
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"			0		   100	"EUR"	1		200	0
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"			3		   106	"EUR"	1		424	0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"				0		   112	"EUR"	1		224	18

OUTPUT de la query:
"RENTA_DIVIDENDOS"	"ES0112501012"	"EBRO"	"CONSUMO BASICO"	"2024-04-10"	"2024-04-12"	"2024-05-12"	3
"RENTA_DIVIDENDOS"	"ES0112501012"	"EBRO"	"CONSUMO BASICO"	"2024-04-15"	"2024-06-10"	"2024-07-10"	6
"RENTA_DIVIDENDOS"	"ES0112501012"	"EBRO"	"CONSUMO BASICO"	"2024-04-10"	"2024-06-10"	"2024-07-10"	3

ESTE RESULTADO ES CORRECTO. Pues "ES0112501012"	"2024-04-10" son acciones que se compraron antes del primer 
cobro de dividendos y se siguen manteniendo (o al menos la cantidad > 0), por lo que cuentan para el siguiente
cobro de dividendos con fecha_ex_div="2024-06-10".
*/








SELECT cantidad_fifo FROM (
SELECT ISIN, FECHA, TIPO_DE_OPERACION, SUM(CANTIDAD) AS cantidad_fifo FROM "FINANZAS"."CARTERA_ACTUALIZADA_FIFO"
	WHERE TIPO_DE_OPERACION = 'COMPRA' AND cantidad > 0
	GROUP BY ISIN, FECHA, TIPO_DE_OPERACION
)








/*
"US92936U1097"	"2024-04-06"	"WPC"	     "REIT"			"COMPRA"	3	150	"USD"	0.94	423	0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	6	110	"EUR"	1	    660	0
"ES0112501012"	"2024-04-08"	"EBRO"	"CONSUMO BASICO"	"VENTA"	    0	108	"EUR"	1	    108	8
"ES0112501012"	"2024-04-01"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	0	100	"EUR"	1	    200	0
"ES0112501012"	"2024-04-10"	"EBRO"	"CONSUMO BASICO"	"COMPRA"	3	106	"EUR"	1	    424	0
"ES0112501012"	"2024-04-15"	"EBRO"	"CONSUMO BASICO"	"VENTA"	    0	112	"EUR"	1	    224	18
*/






























