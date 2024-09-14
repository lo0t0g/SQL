# SQL
Proyectos realizados con SQL

Para información detallada y explicaciones de las validaciones, progresos y futuras actualizaciones, LEER LOS ARCHIVOS "nombre.png"

**CANTIDAD_dividendos_PENDIENTE.sql**: Soluciona la problemática acerca de la cantidad de dividendos que se apuntarán contablemente y/o que finalmente se cobrarán de manera efectiva. 
El problema surge dependiendo de la operativa de COMPRA y/o VENTA en fechas importantes como la fecha_ex_dividendo y fecha_cobro. 

MÁS DETALLES EN: _control_dividendos.jpg_

**CONTROL_nuevos_registros.sql**: En cada ingesta de datos estamos utilizando el mismo Excel. Añadiendo este trigger en las tablas principales establecemos:
  1. CONTROL DE LOS MOMENTOS DEL TIEMPO EN QUE LOS REGISTROS SON INGESTADOS EN LA BBDD.
  2. CONTROL A LA HORA DE INSERTAR SOLO LOS NUEVOS REGISTROS, NO VOLVIENDO A INSERTAR EN LA BBDD LOS ANTERIORES YA INSERTADOS.

MÁS DETALLES EN: _control_nuevos_registros.png_

**FIFO.sql (T-SQL)**: Cuando realizamos ventas parciales de una acción, esto es, no vendemos todas las participaciones de una empresa en la que poseemos varias
posiciones diferentes (y a diferentes precios, normalmente), proponemos dos puntos de vista para la planificación financiera personal: 
     
  1. Punto de vista de HACIENDA: Para Hacienda se aplica el método FIFO en las ventas parciales de las participaciones en una misma empresa, lo que no refleja nunca la realidad.
  3. Punto de vista PROPIO: Se tienen en cuenta las ventas parciales SIN APLICAR EL MÉTODO FIFO. Para ello se propone crear la columna FECHA_REFERENCIA, que será la FECHA_DE_OPERACIÓN
     (esto es, fecha de operación de compra), del lote de acciones que se van a vender. Esto afecta a la CANTIDAD DE DIVIDENDOS que mencionamos en el primer punto.
     En caso de que no se produzca ninguna venta, elegimos por defecto que la fecha_referencia sea _12-12-9999_.

     **NOTA IMPORTANTE**: Por facilidad en la redacción de esta breve documentación, FECHA_REFERENCIA será del tipo _DATETIME_. **No obstante, en el modelo de datos será del tipo
     TIMESTAMP (2017-07-23 00:00:00)** para asegurar la unicidad a la hora de elegir el grupo de acciones que se va a vender.

Ejemplo: Tenemos parte de una empresa llamada _A Inc._ a través de acciones. Hicimos las siguientes operaciones de compra:

          ISIN        Nombre      tipo_de_operacion      fecha_de_operacion     fecha_referencia         Cantidad      Precio
          ES9389484   A Inc.           COMPRA               01-01-2024            12-12-9999                5            10        --LOTE 1
          ES9389484   A Inc.           COMPRA               02-02-2024            12-12-9999                9            11        --LOTE 2
          ES9389484   A Inc.           COMPRA               03-03-2024            12-12-9999                10           9         --LOTE 3

El precio actual a 08-08-2024 es de 14 y decidimos realizar una venta de parte (menos de 9 acciones) o todo el LOTE 2 (todas, las 9 acciones). 
Supongamos que vendemos todas las acciones del LOTE 2:

    Plusvalías realizadas (≠ valor de transmisión) de acuerdo al punto de vista Hacienda: (14-10)*5 + (14-11)*4 = 32   --Por el método FIFO se han vendido 5 acciones de _A Inc._ del día 01-01-2024 
                                                                                                                       -- y las 4 restantes del día 02-02-2024
    Plusvalías realizadas (≠ valor de transmisión) de acuerdo al punto de vista propio: (14-11)*9 = 27 


Luego la cartera según el método FIFO resulta: 

          ISIN        Nombre      tipo_de_operacion      fecha_de_operacion   fecha_referencia       Cantidad      Precio
          ES9389484   A Inc.           COMPRA               01-01-2024           12-12-9999             0            10        --LOTE 1: Se ha vendido el LOTE entero
          ES9389484   A Inc.           COMPRA               02-02-2024           12-12-9999             5            11        --LOTE 2: Se han vendido 4 acciones de este LOTE
          ES9389484   A Inc.           COMPRA               03-03-2024           12-12-9999             10           9         --LOTE 3
          ES9389484   A Inc.          **VENTA**             08-08-2024           02-02-2024             10           9                   

Pero la cartera REAL es: 

          ISIN        Nombre      tipo_de_operacion      fecha_de_operacion   fecha_referencia   Cantidad      Precio
          ES9389484   A Inc.           COMPRA               01-01-2024            12-12-9999        5            10        --LOTE 1
          ES9389484   A Inc.           COMPRA              **02-02-2024**         12-12-9999        0            11        --LOTE 2: Se han vendido todas las acciones de este LOTE
          ES9389484   A Inc.           COMPRA               03-03-2024            12-12-9999        10           9         --LOTE 3
          ES9389484   A Inc.          **VENTA**             08-08-2024           **02-02-2024**     10           9                   

NOTA IMPORTANTE: A la hora del cálculo de los dividendos, hay que tener en cuenta la CARTERA REAL, tanto para Hacienda como para nosotros, ya que **los dividendos** son 'distintos' a nivel fiscal.
Sin embargo, para **las plusvalías**, en el caso de Hacienda hay que utilizar el método FIFO de cara a la declaración de la Renta. Aunque habrá que tener en cuenta la cartera real para futuras plusvalías
generadas (que serán distintas a nivel de Hacienda), pero importantes para la planificación financiera.

MÁS DETALLES EN: _rendimiento_portfolio.png_ y _consultas_inversiones.sql_
