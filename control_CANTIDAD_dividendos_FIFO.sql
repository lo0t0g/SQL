-- VERSIÓN DEFINITIVA 1: Es correcto, pero necesito quedarme con un solo registro...

WITH operaciones_filtradas AS (
    -- Filtrar las operaciones de COMPRA y VENTA que ocurren antes de la fecha ex-dividendo para no tener en consideración las >= fecha_ex_div en los INNER JOIN
    SELECT 
        CARTERA.isin, 
        CARTERA.fecha, 
        CARTERA.tipo_de_operacion, 
        CARTERA.cantidad, 
        DIVIDENDOS.fecha_ex_div, 
        DIVIDENDOS.dividendo_eur,
        ROW_NUMBER() OVER (PARTITION BY CARTERA.isin ORDER BY CARTERA.fecha) AS rn
    FROM "FINANZAS"."CARTERA" AS CARTERA
    JOIN "FINANZAS"."DIVIDENDOS" AS DIVIDENDOS
    ON CARTERA.isin = DIVIDENDOS.isin
    WHERE CARTERA.fecha < DIVIDENDOS.fecha_ex_div
),
compras_ventas_acumuladas AS (
    -- Calcular el acumulado de compras y ventas de acciones antes de la fecha ex-dividendo
    SELECT 
        isin, 
        fecha, 
        tipo_de_operacion, 
        cantidad, 
        fecha_ex_div, 
        dividendo_eur,
        SUM(CASE WHEN tipo_de_operacion = 'COMPRA' THEN cantidad ELSE 0 END) OVER (PARTITION BY isin ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS compras_acumuladas,
        SUM(CASE WHEN tipo_de_operacion = 'VENTA' THEN cantidad ELSE 0 END) OVER (PARTITION BY isin ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS ventas_acumuladas
    FROM operaciones_filtradas
),
acciones_con_dividendos AS (
    -- Calcular el total de acciones con derecho a dividendos para cada operación antes de la fecha ex-dividendo
    SELECT 
        isin, 
        fecha_ex_div, 
        MAX(compras_acumuladas - ventas_acumuladas) AS acciones_con_dividendos,
        dividendo_eur
    FROM compras_ventas_acumuladas
    GROUP BY isin, fecha_ex_div, dividendo_eur
)
-- Cálculo final de dividendos potenciales
SELECT 
    acc_div.isin, 
    acc_div.fecha_ex_div, 
    acc_div.acciones_con_dividendos, 
    acc_div.dividendo_eur, 
    (acc_div.acciones_con_dividendos * acc_div.dividendo_eur) AS dividendos_potenciales
FROM acciones_con_dividendos AS acc_div
ORDER BY acc_div.isin, acc_div.fecha_ex_div;



-- VERSIÓN DEFINITIVA FINAL 1: Es correcto, pero falta incorporar los DIVIENDOS EFECTIVAMENTE COBRADOS (ver VERSIÓN DEFINITIVA FINAL 2)

WITH operaciones_filtradas AS (
    -- Filtrar las operaciones de COMPRA y VENTA que ocurren antes de la fecha ex-dividendo para no tenerlas en consideración en los INNER JOIN
    SELECT 
        CARTERA.isin, 
        CARTERA.fecha, 
        CARTERA.tipo_de_operacion, 
        CARTERA.cantidad, 
        DIVIDENDOS.fecha_ex_div, 
        DIVIDENDOS.dividendo_eur,
        ROW_NUMBER() OVER (PARTITION BY CARTERA.isin ORDER BY CARTERA.fecha) AS rn
    FROM "FINANZAS"."CARTERA" CARTERA
    INNER JOIN "FINANZAS"."DIVIDENDOS" DIVIDENDOS
    ON CARTERA.isin = DIVIDENDOS.isin
    WHERE CARTERA.fecha < DIVIDENDOS.fecha_ex_div
),
compras_ventas_acumuladas AS (
    -- Calcular el acumulado de compras y ventas de acciones antes de la fecha ex-dividendo
    SELECT 
        isin, 
        fecha, 
        tipo_de_operacion, 
        cantidad, 
        fecha_ex_div, 
        dividendo_eur,
        SUM(CASE WHEN tipo_de_operacion = 'COMPRA' THEN cantidad ELSE 0 END) OVER (PARTITION BY isin ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS compras_acumuladas,
        SUM(CASE WHEN tipo_de_operacion = 'VENTA' THEN cantidad ELSE 0 END) OVER (PARTITION BY isin ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS ventas_acumuladas
    FROM operaciones_filtradas
),
acciones_con_dividendos AS (
    -- Calcular el total de acciones con derecho a dividendos para cada operación antes de la fecha ex-dividendo
	-- y seleccionar solo las filas (por isin y dividendo_eur) con el máximo número de acciones con dividendos.
	--No me queda otra que hacerlo así para el siguiente y último SELECT
    SELECT 
        isin, 
		dividendo_eur,
        MAX(compras_acumuladas - ventas_acumuladas) AS cantidad_acciones_con_dividendos
    FROM compras_ventas_acumuladas
    GROUP BY isin, dividendo_eur
)

-- Último SELECT. Mostrar cantidad final y hacer el cálculo de dividendo_eur a recibir

SELECT 
    isin, 
    cantidad_acciones_con_dividendos, 
    (cantidad_acciones_con_dividendos * dividendo_eur) AS dividendo_eur
FROM acciones_con_dividendos




-- VERSIÓN DEFINITIVA FINAL 2: INCORPORAR LOS DIVIDENDOS EFECTIVAMENTE COBRADOS (i.e, aquellos que alcanzan o superan la fecha de cobro)

--SET my.simulated_now = '2024-05-12';

WITH operaciones_filtradas AS (
    -- Filtrar las operaciones de COMPRA y VENTA que ocurren antes de la fecha ex-dividendo para no tenerlas en consideración en los INNER JOIN
    SELECT 
        CARTERA.isin, 
        CARTERA.fecha, 
        CARTERA.tipo_de_operacion, 
        CARTERA.cantidad, -- CANTIDAD la tomaremos DE LA CARTERA, NO DE LOS DIVIDENDOS ya que mantenemos actualizadas las cantidades de la tabla CARTERA
        DIVIDENDOS.fecha_ex_div, 
		DIVIDENDOS.fecha_cobro,
        DIVIDENDOS.dividendo_eur,
--		current_setting('my.simulated_now')::DATE AS current_fecha,
        ROW_NUMBER() OVER (PARTITION BY CARTERA.isin ORDER BY CARTERA.fecha) AS rn
	
    FROM "FINANZAS"."CARTERA" CARTERA
    INNER JOIN "FINANZAS"."DIVIDENDOS" DIVIDENDOS
    ON CARTERA.isin = DIVIDENDOS.isin
    WHERE CARTERA.fecha < DIVIDENDOS.fecha_ex_div
),
compras_ventas_acumuladas AS (
    -- Calcular el acumulado de compras y ventas de acciones antes de la fecha ex-dividendo
    SELECT 
        isin, 
        fecha, 
        tipo_de_operacion, 
        cantidad, 
        fecha_ex_div, 
		fecha_cobro,
        dividendo_eur,
--		current_fecha,
        SUM(CASE WHEN tipo_de_operacion = 'COMPRA' THEN cantidad ELSE 0 END) OVER (PARTITION BY isin ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS compras_acumuladas,
        SUM(CASE WHEN tipo_de_operacion = 'VENTA' THEN cantidad ELSE 0 END) OVER (PARTITION BY isin ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS ventas_acumuladas,
		SUM(CASE WHEN tipo_de_operacion = 'COMPRA' AND CURRENT_TIMESTAMP >= fecha_cobro THEN cantidad ELSE 0 END) OVER (PARTITION BY isin ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
            AS dividendos_cobrados_acumulados
    FROM operaciones_filtradas
),
acciones_con_dividendos AS (
    -- Calcular el total de acciones con derecho a dividendos para cada operación antes de la fecha ex-dividendo
	-- y calcular el total de acciones con derecho a COBRO de dividendos para cada operación  >= fecha_cobro
	
	-- y seleccionar solo las filas (por isin) con el máximo número de acciones con dividendos.
	
    SELECT 
        isin, 
		dividendo_eur, -- IMPORTANTE agrupar por dividendo_eur, ya que depende de las acciones vendidas cobraremos más o menos dividendos
        MAX(compras_acumuladas - ventas_acumuladas) AS cantidad_acciones_con_dividendos,
		MAX(dividendos_cobrados_acumulados - ventas_acumuladas) AS cantidad_acciones_con_dividendos_cobrados
	FROM compras_ventas_acumuladas
    GROUP BY isin, dividendo_eur
),

total_cantidad_dividendos AS (
	-- Mostrar cantidad*dividendo_eur final:
	SELECT 
		isin,
		acciones_con_dividendos.cantidad_acciones_con_dividendos AS cantidad_acciones_con_dividendos, 
		SUM(cantidad_acciones_con_dividendos * dividendo_eur) AS total_dividendo_eur
	FROM acciones_con_dividendos
	GROUP BY isin, cantidad_acciones_con_dividendos

)

SELECT isin, cantidad_acciones_con_dividendos, total_dividendo_eur
FROM total_cantidad_dividendos

