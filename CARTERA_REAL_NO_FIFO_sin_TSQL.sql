--CÓDIGO 1 (CORRECTO): Calcula las CANTIDADES resultantes para cada ISIN y FECHA_REFERENCIA, devolviendo una tabla SÓLO CON LAS COMPRAS
--y sus cantidades actualizadas en la columna cantidad_ajustada:

WITH tabla_ventas AS (
    -- Calculamos el impacto de las ventas en las compras y así tenemos la tabla VENTAS para hacer LEFT JOIN 
	SELECT 
        isin, 
        fecha, 
        fecha_referencia,
        tipo_de_operacion, 
        cantidad,
        CASE 
            WHEN tipo_de_operacion = 'VENTA' THEN cantidad
            ELSE 0
        END AS cantidad_venta_aplicada
    FROM "FINANZAS"."CARTERA_DEFINITIVA"
)

SELECT 
	CARTERA_DEFINITIVA.isin,
	CARTERA_DEFINITIVA.fecha,
	CARTERA_DEFINITIVA.fecha_referencia,
	CARTERA_DEFINITIVA.tipo_de_operacion,
	CARTERA_DEFINITIVA.cantidad - COALESCE(
											SUM(TABLA_VENTAS.cantidad_venta_aplicada) 
											OVER (PARTITION BY CARTERA_DEFINITIVA.isin, TABLA_VENTAS.fecha_referencia 
			  								ORDER BY CARTERA_DEFINITIVA.fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
									   , 0) AS cantidad_ajustada

FROM "FINANZAS"."CARTERA_DEFINITIVA" AS CARTERA_DEFINITIVA
LEFT JOIN tabla_ventas AS TABLA_VENTAS  --LEFT JOIN porque siempre (cantidad ventas) <= (cantidad compras)
	ON CARTERA_DEFINITIVA.isin = TABLA_VENTAS.isin
	AND CARTERA_DEFINITIVA.fecha = TABLA_VENTAS.fecha_referencia  --El LEFT JOIN cruzará los registros de VENTAS con los de COMPRAS con mismo ISIN y FECHA_REFERENCIA
--WHERE CARTERA_DEFINITIVA.tipo_de_operacion = 'COMPRA' -- Vemos que las VENTAS no están actualizadas a cantidad_ajustada = 0



--CÓDIGO 2 (CORRECTO): Calcula las CANTIDADES resultantes para cada ISIN y FECHA_REFERENCIA, devolviendo una tabla CON LAS COMPRAS
--Y VENTAS, junto con sus cantidades actualizadas en la columna cantidad_ajustada:

WITH tabla_ventas AS (
    -- Calculamos el impacto de las ventas en las compras y así tenemos la tabla VENTAS para hacer LEFT JOIN 
	
    SELECT 
        isin,
        fecha, 
        fecha_referencia,
        tipo_de_operacion, 
        cantidad,
        CASE 
            WHEN tipo_de_operacion = 'VENTA' THEN cantidad
            ELSE 0
        END AS cantidad_venta_aplicada
    FROM "FINANZAS"."CARTERA_DEFINITIVA"
)

SELECT 
	CARTERA_DEFINITIVA.isin,
	CARTERA_DEFINITIVA.fecha,
	CARTERA_DEFINITIVA.fecha_referencia,
	CARTERA_DEFINITIVA.tipo_de_operacion,
	CASE
        WHEN CARTERA_DEFINITIVA.tipo_de_operacion = 'COMPRA' THEN 
            CARTERA_DEFINITIVA.cantidad - COALESCE(
                SUM(TABLA_VENTAS.cantidad_venta_aplicada) 
                OVER (
                    PARTITION BY CARTERA_DEFINITIVA.isin, TABLA_VENTAS.fecha_referencia 
                    ORDER BY CARTERA_DEFINITIVA.fecha 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0
            )
        ELSE 0  -- Para las operaciones de VENTA, ajustamos la cantidad a cantidad_ajustada = 0
    END AS cantidad_ajustada

FROM "FINANZAS"."CARTERA_DEFINITIVA" AS CARTERA_DEFINITIVA
LEFT JOIN tabla_ventas AS TABLA_VENTAS  --LEFT JOIN porque siempre (cantidad ventas) <= (cantidad compras)
	ON CARTERA_DEFINITIVA.isin = TABLA_VENTAS.isin
	AND CARTERA_DEFINITIVA.fecha = TABLA_VENTAS.fecha_referencia  --El LEFT JOIN cruzará los registros de VENTAS con los de COMPRAS con mismo ISIN y FECHA_REFERENCIA










