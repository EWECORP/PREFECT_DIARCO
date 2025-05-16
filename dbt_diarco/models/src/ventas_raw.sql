-- Modelo src.ventas_raw

SELECT 
    id_articulo,
    id_sucursal,
    fecha,
    unidades,
    precio,
    costo,
    fecha_carga
FROM {{ source('src', 'ventas_raw') }}
