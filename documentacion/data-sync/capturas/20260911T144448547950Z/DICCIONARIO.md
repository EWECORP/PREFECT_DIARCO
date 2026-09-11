# Diccionario de datos

Tipos y longitudes extraídos de sys.columns. max_length se expresa en bytes; -1 significa MAX. Filas estimadas de particiones, sin leer datos de negocio.

## dbo._debug_base_stock_dmz

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Codigo_Articulo | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Codigo_Sucursal | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Codigo_Proveedor | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Precio_Venta | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Precio_Costo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Factor_Venta | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Ultimo_Ingreso | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Fecha_Ultimo_Ingreso | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Fecha_Ultima_Venta | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| M_Vende_Por_Peso | char | 1 | 0 | 0 | True | False |  |  |  |  |
| Venta_Unidades_1Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Unidades_2Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Unidades | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Valorizada | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Stock | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Fecha_Stock | date | 3 | 10 | 0 | True | False |  |  |  |  |
| Stock | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Transfer_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Promocion | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Lote | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| Validez_Lote | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Stock_Reserva | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Validez_Promocion | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIAS_STOCK | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| I_LISTA_CALCULADO | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_SGM | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Importe_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Bultos_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Preparacion | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo._debug_base_stock_ext

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Codigo_Articulo | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Codigo_Sucursal | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Codigo_Proveedor | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Precio_Venta | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Precio_Costo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Factor_Venta | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Ultimo_Ingreso | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Fecha_Ultimo_Ingreso | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Fecha_Ultima_Venta | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| M_Vende_Por_Peso | char | 1 | 0 | 0 | True | False |  |  |  |  |
| Venta_Unidades_1Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Unidades_2Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Unidades | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Valorizada | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Stock | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Fecha_Stock | date | 3 | 10 | 0 | True | False |  |  |  |  |
| Stock | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Transfer_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Transito_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Transfer_Pendiente_fecha | date | 3 | 10 | 0 | True | False |  |  |  |  |
| Pedido_Pendiente_fecha | date | 3 | 10 | 0 | True | False |  |  |  |  |
| Transito_Pendiente_fecha | date | 3 | 10 | 0 | True | False |  |  |  |  |
| Promocion | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Lote | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| Validez_Lote | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Stock_Reserva | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Validez_Promocion | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIAS_STOCK | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| I_LISTA_CALCULADO | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_SGM | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Importe_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Bultos_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Preparacion | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo._debug_SP_BASE_PRODUCTOS_SUCURSAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | int | 4 | 10 | 0 | False | False |  |  |  |  |
| C_ARTICULO | int | 4 | 10 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| ABASTECIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| COD_CD | nvarchar | 64 | 0 | 0 | True | False |  |  |  |  |
| HABILITADO | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| FECHA_REGISTRO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| FECHA_BAJA | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Q_PESO_UNIT_ART | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| M_VENDE_POR_PESO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| UNID_TRANSFERENCIA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_UNID_TRANSFERENCIA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| PEDIDO_MIN | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| FRENTE_LINEAL | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CAPACID_GONDOLA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| STOCK_MINIMO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| COD_COMPRADOR | int | 4 | 10 | 0 | True | False |  |  |  |  |
| PROMOCION | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_FOR_PURCHASE | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_FOR_SALE | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_ON_MIX | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| DELIVERED_ID | nvarchar | 64 | 0 | 0 | True | False |  |  |  |  |
| PRODUCT_BASE_ID | nvarchar | 200 | 0 | 0 | True | False |  |  |  |  |
| OWN_PRODUCTION | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| Q_FACTOR_COMPRA | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| FULL_CAPACITY_PALLET | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| NUMBER_OF_LAYERS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| NUMBER_OF_BOXES_PER_LAYER | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| fecha_extraccion | datetime2 | 6 | 19 | 0 | False | False |  |  | (sysdatetime()) |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| CX__debug_SP_BASE_PRODUCTOS_SUCURSAL | CLUSTERED | False | False | False |  |
| IX__debug_SP_BASE_PRODUCTOS_SUCURSAL__COD_CD | NONCLUSTERED | False | False | False |  |
| IX__debug_SP_BASE_PRODUCTOS_SUCURSAL__Flags | NONCLUSTERED | False | False | False |  |

## dbo._debug_SP_STOCK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Codigo_Articulo | int | 4 | 10 | 0 | False | False |  |  |  |  |
| Codigo_Sucursal | int | 4 | 10 | 0 | False | False |  |  |  |  |
| Codigo_Proveedor | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Precio_Venta | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Precio_Costo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Factor_Venta | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| Ultimo_Ingreso | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Fecha_Ultimo_Ingreso | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Fecha_Ultima_Venta | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| M_Vende_Por_Peso | char | 1 | 0 | 0 | True | False |  |  |  |  |
| Venta_Unidades_1Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Unidades_2Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Unidades | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Valorizada | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Stock | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Fecha_Stock | date | 3 | 10 | 0 | False | False |  |  |  |  |
| Stock | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Transfer_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Promocion | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| Lote | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| Validez_Lote | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Stock_Reserva | int | 4 | 10 | 0 | False | False |  |  |  |  |
| Validez_Promocion | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| Q_DIAS_STOCK | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | int | 4 | 10 | 0 | True | False |  |  |  |  |
| I_LISTA_CALCULADO | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_SGM | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Importe_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Bultos_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Preparacion | int | 4 | 10 | 0 | True | False |  |  |  |  |
| fecha_extraccion | datetime2 | 6 | 19 | 0 | False | False |  |  | (sysdatetime()) |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| CX__debug_SP_STOCK | CLUSTERED | False | False | False |  |
| IX__debug_SP_STOCK__Proveedor | NONCLUSTERED | False | False | False |  |

## dbo.ART_NOCOM

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c_articulo | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__ART_NOCO__0E82EE7D87F1C3ED | CLUSTERED | True | True | False |  |

## dbo.base_productos_dmz_test

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c_sucu_empr | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| c_articulo | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| c_proveedor_primario | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| abastecimiento | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| cod_cd | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| habilitado | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| fecha_registro | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| fecha_baja | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| q_peso_unit_art | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| m_vende_por_peso | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| unid_transferencia | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| q_unid_transferencia | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| pedido_min | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| frente_lineal | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| capacid_gondola | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| stock_minimo | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| cod_comprador | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| promocion | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| active_for_purchase | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| active_for_sale | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| active_on_mix | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| delivered_id | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| product_base_id | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| own_production | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| q_factor_compra | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| full_capacity_pallet | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| number_of_layers | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| number_of_boxes_per_layer | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| tag_corrida | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| f_insert | datetime | 8 | 23 | 3 | False | False |  |  | (getdate()) |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.base_productos_vigentes_old

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c_sucu_empr | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| c_articulo | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| c_proveedor_primario | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| abastecimiento | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| cod_cd | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| habilitado | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| fecha_registro | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| fecha_baja | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| q_peso_unit_art | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| m_vende_por_peso | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| unid_transferencia | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| q_unid_transferencia | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| pedido_min | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| frente_lineal | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| capacid_gondola | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| stock_minimo | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| cod_comprador | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| promocion | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| active_for_purchase | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| active_for_sale | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| active_on_mix | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| delivered_id | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| product_base_id | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| own_production | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| q_factor_compra | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| full_capacity_pallet | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| number_of_layers | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| number_of_boxes_per_layer | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| tag_corrida | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| f_insert | datetime | 8 | 23 | 3 | False | False |  |  | (getdate()) |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.base_stock_sucursal

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Codigo_Articulo | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Codigo_Sucursal | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Codigo_Proveedor | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Precio_Venta | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Precio_Costo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Factor_Venta | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Ultimo_Ingreso | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Fecha_Ultimo_Ingreso | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Fecha_Ultima_Venta | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| M_Vende_Por_Peso | char | 1 | 0 | 0 | True | False |  |  |  |  |
| Venta_Unidades_1Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Unidades_2Q | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Unidades | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Venta_Mes_Valorizada | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Stock | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Fecha_Stock | date | 3 | 10 | 0 | True | False |  |  |  |  |
| Stock | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Transfer_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_Pendiente | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Promocion | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Lote | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| Validez_Lote | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| Stock_Reserva | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Validez_Promocion | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIAS_STOCK | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| I_LISTA_CALCULADO | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Pedido_SGM | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Importe_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Bultos_Minimo | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| Dias_Preparacion | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.CLUSTER_LOGISTICO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| CLUSTER_LOG | varchar | 8 | 0 | 0 | False | False |  |  |  |  |
| FORMATO | varchar | 9 | 0 | 0 | False | False |  |  |  |  |
| ETIQUETA | nvarchar | 8000 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.ewe_articulos_logistica

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c_articulo | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| c_proveedor | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| c_configuracion_logistica | varchar | 60 | 0 | 0 | True | False |  |  |  |  |
| m_configuracion_default | bit | 1 | 1 | 0 | True | False |  |  |  |  |
| m_activo | bit | 1 | 1 | 0 | True | False |  |  |  |  |
| c_unidad_base | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| m_vende_por_peso | bit | 1 | 1 | 0 | True | False |  |  |  |  |
| c_gtin_unidad | varchar | 14 | 0 | 0 | True | False |  |  |  |  |
| c_tipo_bulto | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| c_gtin_bulto | varchar | 14 | 0 | 0 | True | False |  |  |  |  |
| q_unidades_por_bulto | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| q_peso_neto_unitario_kg | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| q_peso_bruto_unitario_kg | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| q_peso_bruto_bulto_kg | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| q_largo_bulto_cm | decimal | 9 | 12 | 3 | True | False |  |  |  |  |
| q_ancho_bulto_cm | decimal | 9 | 12 | 3 | True | False |  |  |  |  |
| q_alto_bulto_cm | decimal | 9 | 12 | 3 | True | False |  |  |  |  |
| q_volumen_bulto_m3 | decimal | 9 | 18 | 9 | True | False |  |  |  |  |
| c_metodo_volumen | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| q_bultos_por_capa | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| q_capas_por_pallet | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| q_bultos_por_pallet | numeric | 5 | 7 | 0 | True | False |  |  |  |  |
| c_tipo_pallet | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| q_largo_pallet_cm | decimal | 9 | 12 | 3 | True | False |  |  |  |  |
| q_ancho_pallet_cm | decimal | 9 | 12 | 3 | True | False |  |  |  |  |
| q_alto_pallet_cargado_cm | decimal | 9 | 12 | 3 | True | False |  |  |  |  |
| q_peso_bruto_pallet_kg | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| m_apilable | bit | 1 | 1 | 0 | True | False |  |  |  |  |
| q_max_niveles_apilado | smallint | 2 | 5 | 0 | True | False |  |  |  |  |
| m_fragil | bit | 1 | 1 | 0 | True | False |  |  |  |  |
| m_peligroso | bit | 1 | 1 | 0 | True | False |  |  |  |  |
| c_zona_temperatura | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| q_temperatura_min_c | decimal | 5 | 6 | 2 | True | False |  |  |  |  |
| q_temperatura_max_c | decimal | 5 | 6 | 2 | True | False |  |  |  |  |
| c_orientacion | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| observaciones_manipulacion | nvarchar | -1 | 0 | 0 | True | False |  |  |  |  |
| c_calidad_embalaje | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| c_calidad_peso | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| c_calidad_volumen | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| c_calidad_pallet | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| observaciones_calidad | nvarchar | -1 | 0 | 0 | True | False |  |  |  |  |
| verificado_en | datetimeoffset | 10 | 34 | 7 | True | False |  |  |  |  |
| verificado_por | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| fuente_origen | varchar | 60 | 0 | 0 | True | False |  |  |  |  |
| referencia_origen | varchar | 160 | 0 | 0 | True | False |  |  |  |  |
| atributos_adicionales | nvarchar | -1 | 0 | 0 | True | False |  |  |  |  |
| fecha_extraccion | datetimeoffset | 10 | 34 | 7 | False | False |  |  |  |  |
| cdc_lsn | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| estado_sincronizacion | smallint | 2 | 5 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.F_OC_PRECARGA_CONNEXA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | decimal | 9 | 10 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| Q_FORECAST_UNIDADES | float | 8 | 53 | 0 | False | False |  |  |  |  |
| F_ALTA_FORECAST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_FORECAST | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| Q_BULTOS_KILOS_SUPPLY | float | 8 | 53 | 0 | True | False |  |  |  |  |
| F_ALTA_SUPPLY | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| C_USUARIO_SUPPLY | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| F_GENERO_OC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| C_USUARIO_BLOQUEO | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| M_PROCESADO | char | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROCESADO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| U_PREFIJO_OC | bigint | 8 | 19 | 0 | True | False |  |  |  |  |
| U_SUFIJO_OC | bigint | 8 | 19 | 0 | True | False |  |  |  |  |
| C_COMPRA_CONNEXA | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| C_USUARIO_MODIF | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| CREATE_DATE | datetime | 8 | 23 | 3 | False | False |  |  | (getdate()) |  |
| RECEIVED_DATE | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| C_COMPRADOR | varchar | 50 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_1_CATEGORIAS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_RUBRO | varchar | 4 | 0 | 0 | True | False |  |  |  |  |
| N_RUBRO_NORMALIZADO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_1 | varchar | 4 | 0 | 0 | True | False |  |  |  |  |
| N_SUBRUBRO_1_NORMALIZADO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_2 | varchar | 4 | 0 | 0 | True | False |  |  |  |  |
| N_SUBRUBRO_2_NORMALIZADO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_3 | varchar | 4 | 0 | 0 | True | False |  |  |  |  |
| N_SUBRUBRO_3_NORMALIZADO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_10_PROVEEDORES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_DIARCO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| N_PROVEEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_CLIENTE | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ACTIVO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_LINEA_PRODUCTO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_EANEDI | char | 15 | 0 | 0 | False | False |  |  |  |  |
| N_VENDEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_CHEQ_ALAORDEN | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_ENCARG_COMPRA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_LUN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_MAR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_MIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_JUE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_VIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_SAB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_DOM | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_ENVASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_AGENTE_RET_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_COMPRA_PALETIZADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_ORIGEN_PROVEEDOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| N_ESTADO | char | 30 | 0 | 0 | False | False |  |  |  |  |
| N_PAIS | char | 30 | 0 | 0 | False | False |  |  |  |  |
| N_CIUDAD | char | 30 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_TOPE_PARA_AGENDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DIAS_TOPE_PARA_INGRESO_AGENDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_LUN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_MAR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_MIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_JUE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_VIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_SAB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_DOM | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PERIODO_ATENCION_AGENDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_CORDOBA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_MISIONES | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_INSCRIPTO_SEG_HIG_POSADAS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| ACCEPT_RETURN | bit | 1 | 1 | 0 | False | False |  |  | ((1)) |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_2_MOVIMIENTOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_TIPO_MOVIMIENTO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_3_1_FAMILIA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_FAMILIA | varchar | 4 | 0 | 0 | True | False |  |  |  |  |
| N_FAMILIA | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_3_2_FAMILIA_ARTICULO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_FAMILIA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_ARTICULO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_9_COMPRADORES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_COMPRADOR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| N_COMPRADOR | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_91_SUCURSALES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ID_TIENDA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SUC_NOMBRE | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| SUC_ABREV | varchar | 4 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_92_DEPOSITOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ID | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| DC_NOMBRE | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_93_SUSTITUTOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_PRD | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_PROD_SUSTITUTO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_94_ALTERNATIVOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_PRD | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_PROD_ALTERNATIVO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_95_SENSIBLES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_PRD | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_96_STOCK_SEGURIDAD

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_PROV | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_ART | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_SUC | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| DIA_STOCK | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.M_SUCURSALES_EXT

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id_tienda | int | 4 | 10 | 0 | False | False |  |  |  |  |
| suc_nombre | nvarchar | 100 | 0 | 0 | False | False |  |  |  |  |
| sucursal | nvarchar | 100 | 0 | 0 | False | False |  |  |  |  |
| formato | nvarchar | 40 | 0 | 0 | False | False |  |  |  |  |
| f_actividad | date | 3 | 10 | 0 | True | False |  |  |  |  |
| terreno | int | 4 | 10 | 0 | True | False |  |  |  |  |
| cubierta | int | 4 | 10 | 0 | True | False |  |  |  |  |
| s_cubierta | int | 4 | 10 | 0 | True | False |  |  |  |  |
| playa | int | 4 | 10 | 0 | True | False |  |  |  |  |
| salon | int | 4 | 10 | 0 | True | False |  |  |  |  |
| deposito | int | 4 | 10 | 0 | True | False |  |  |  |  |
| check_outs | int | 4 | 10 | 0 | True | False |  |  |  |  |
| direccion | nvarchar | -1 | 0 | 0 | True | False |  |  |  |  |
| postal | int | 4 | 10 | 0 | True | False |  |  |  |  |
| f_cierre | date | 3 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__M_SUCURS__7C49D73614306C7C | CLUSTERED | True | True | False |  |

## dbo.SP_BASE_PRODUCTOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| ABASTECIMIENTO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_CD | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| HABILITADO | char | 1 | 0 | 0 | True | False |  |  |  |  |
| FECHA_REGISTRO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| FECHA_BAJA | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| UNID_TRANSFERENCIA | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| Q_UNID_TRANSFERENCIA | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| PEDIDO_MIN | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| FRENTE_LINEAL | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| CAPACID_GONDOLA | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| STOCK_MINIMO | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| COD_COMPRADOR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| PROMOCION | char | 1 | 0 | 0 | True | False |  |  |  |  |
| ACTIVE_FOR_PURCHASE | char | 1 | 0 | 0 | True | False |  |  |  |  |
| ACTIVE_FOR_SALE | char | 1 | 0 | 0 | True | False |  |  |  |  |
| ACTIVE_ON_MIX | char | 1 | 0 | 0 | True | False |  |  |  |  |
| DELIVERED_ID | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| PRODUCT_BASE_ID | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| OWN_PRODUCTION | char | 1 | 0 | 0 | True | False |  |  |  |  |
| Q_FACTOR_COMPRA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| FULL_CAPACITY_PALLET | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| NUMBER_OF_LAYERS | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| NUMBER_OF_BOXES_PER_LAYER | varchar | 10 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.SUCURSALES_EXCLUIDAS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__SUCURSAL__7F99C09A2D38EC9F | CLUSTERED | True | True | False |  |
| IX_SUCURSALES_EXCLUIDAS | NONCLUSTERED | False | False | False |  |

## dbo.sysdiagrams

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| name | sysname | 256 | 0 | 0 | False | False |  |  |  |  |
| principal_id | int | 4 | 10 | 0 | False | False |  |  |  |  |
| diagram_id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| version | int | 4 | 10 | 0 | True | False |  |  |  |  |
| definition | varbinary | -1 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__sysdiagr__C2B05B612E0AE482 | CLUSTERED | True | True | False |  |
| UK_principal_name | NONCLUSTERED | True | False | True |  |

## dbo.T_100_SUCURSALES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | int | 4 | 10 | 0 | True | False |  |  |  |  |
| N_SUCURSAL | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| N_SUCURSAL_ABREV | char | 2 | 0 | 0 | True | False |  |  |  |  |
| N_SUCURSAL_ABREV2 | char | 10 | 0 | 0 | True | False |  |  |  |  |
| N_SUCURSAL_ABREV3 | char | 6 | 0 | 0 | True | False |  |  |  |  |
| N_CALLE | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| N_LOCALIDAD | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| C_POSTAL_INM | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| C_PROVINCIA_SUCU | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| C_ZONA | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| C_ZONA_REGIONAL | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| CLUSTER_LOG | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| C_FORMATO | int | 4 | 10 | 0 | False | False |  |  |  |  |
| FORMATO | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | True | False |  |  |  |  |
| ETIQUETA | varchar | 20 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_11_1_PROMOCIONES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_ARTICULO | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| NOMBRE_ARTICULO | varchar | 255 | 0 | 0 | True | False |  |  |  |  |
| TIPO_PROMOCION | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_PROMOCION | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_11_PRECIOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| COD_PROMOCION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| QTY_UNI_COMPRA | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| U_MEDIDA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| VALOR_PROMOCIONAL | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| FECHA_FINAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| PLAZO_ENTREGA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_5_1_MOVIMIENTOS_DIARIOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_5_MOVIMIENTOS_DIARIOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |
| NonClusteredIndex-20241211-130826 | NONCLUSTERED | False | False | False |  |

## dbo.T_5_MOVIMIENTOS_DIARIOS_1

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_5_MOVIMIENTOS_DIARIOS_2

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_5_MOVIMIENTOS_DIARIOS_3

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_5_MOVIMIENTOS_DIARIOS_4

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_5_MOVIMIENTOS_DIARIOS_5

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_5_MOVIMIENTOS_DIARIOS_6

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCURSAL | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MOVIMIENTO | date | 3 | 10 | 0 | True | False |  |  |  |  |
| C_MOVIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CANTIDAD | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_PRECIO_VTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| I_COSTO_PPP | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PIS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| DESCRIP_TIPO_MOVIMIENTO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| TIPO_OPERACION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SIGNO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_7_PEDIDOS_PENDIENTES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| FECHA_EMISION | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| FECHA_ENTREGA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_SUCU_DESTINO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| QTY_PENDIENTE | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| NUMERO_OC | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_PROVEEDOR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_710_ESTADIS_DETALLE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F_DIA | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| C_SUCU_EMPR | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_DOMINGO | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_FOLDER | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_OFERTA | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_STOCK | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_SEPA | varchar | 30 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_710_ESTADIS_REPOSICION

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| FECHA | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_30_DIAS | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_15_DIAS | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_DOMINGO | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_ESPECIAL_30_DIAS | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_ESPECIAL_15_DIAS | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_DIAS_CON_STOCK | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_REPONER | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_REPONER_INCLUIDO_SOBRE_STOCK | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_SEMAFORO_INDIVIDUAL | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_SEMAFORO_GLOBAL | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_DIARIA_NORMAL | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_DIAS_STOCK | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_DIAS_ENTREGA_PROVEEDOR | varchar | 30 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T_8_STOCK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_SUCU_EMPR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| FECHA_VIGENCIA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| Q_UNID_PESO_ARTICULO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| QTY_PENDIENTE | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PRECIO_COSTO | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PRECIO_VENTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| FLAG_OFERTA | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| FLAG_PROMO | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_87_PRECIOS_ARTICULO_SUCURSAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| FECHA_VIGENCIA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| C_SUCU_EMPR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| PRECIO_UNITARIO_MINORISTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PRECIO_MAYORISTA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| PRECIO_PROMOCIONAL | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_CLUSTER_PARAM_STOCK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CLUSTER_LOG | varchar | 8 | 0 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_COMPETENCIA_DETALLE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_ARTICULO | char | 60 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| MG% | money | 8 | 19 | 4 | True | False |  |  |  |  |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| N_COMPETIDOR | char | 30 | 0 | 0 | False | False |  |  |  |  |
| I_PRECIO_COMPETIDOR | money | 8 | 19 | 4 | False | False |  |  |  |  |
| MG2% | money | 8 | 19 | 4 | True | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_DATASET_ELASTICIDAD

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Fecha | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_STOCK | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| F_DIA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_DOMINGO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| PRECIO_VTA | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_ESTADISTICO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_PP | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| N_ARTICULO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| C_RUBRO | decimal | 9 | 10 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_1 | decimal | 9 | 10 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_2 | decimal | 9 | 10 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_3 | decimal | 9 | 10 | 0 | True | False |  |  |  |  |
| D_CODIGO_ABREV_VTA | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| D_CODIGO_ABREV_CPRA | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| Q_FACTOR_CPRA_SUCU | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| CLASIFICACION | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_ESTADISTICA_PRECIOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_MES | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_25 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_26 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_27 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_28 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_29 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_30 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_31 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_25 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_26 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_27 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_28 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_29 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_30 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO_31 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_25 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_26 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_27 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_28 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_29 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_30 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PP_31 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_25 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_26 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_27 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_28 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_29 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_30 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO_31 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_25 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_26 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_27 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_28 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_29 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_30 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO_31 | money | 8 | 19 | 4 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_FALTANTES_SUBRUBRO_LOCAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c_sucu_empr | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| vnfamilia | char | 30 | 0 | 0 | False | False |  |  |  |  |
| vnrubro | char | 30 | 0 | 0 | False | False |  |  |  |  |
| vnsubrubro | char | 30 | 0 | 0 | False | False |  |  |  |  |
| habilitada | int | 4 | 10 | 0 | True | False |  |  |  |  |
| SinStock | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Porcentaje | decimal | 5 | 8 | 2 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_HISTORIAL_OFERTAS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_VIGENCIA_DESDE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_VIGENCIA_HASTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_TIPO_PRECIO | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| I_PRECIO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_RETORNO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| PRECIO_RELATIVO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_VIGENTE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| U_PAGINA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_HISTORIAL_PRECIOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_MES | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| Dia | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Fecha | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| PRECIO_VTA | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_ESTADISTICO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_PP | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| COSTO_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_HISTORIAL_VENTAS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Fecha | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_DIA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| M_DOMINGO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_VENTA | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_STOCK | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_OC_CABECERA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| U_PREFIJO_LOTE | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_LOTE | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_OC_MADRE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OC_PARA_TRANSFERENCIA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OC_PAGOANT | char | 1 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_LIMITE_ENTREGA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO_ALT | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_EMISION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ENTREGA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| I_NETO_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNO_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO_OPERADOR | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_OPERADOR | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_CUMPLIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA4 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA5 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA6 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_COND_PAGO | char | 200 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 200 | 0 | 0 | False | False |  |  |  |  |
| F_COMP_ING_MERC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_COMP_ING_MERC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_COMP_ING_MERC | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_COMP_ING_MERC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| D_OBSERVACION_ING_MERC | char | 150 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_ENTREGA_MERCADERIA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIFICO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_MODIFICO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIFICO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_OC_ELECTRONICA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SITUAC_OC_ELECTRONICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC_OC_ELECTRONICA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ENVIADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ESP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_EDI | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| N_PROVEEDOR | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_OC_DETALLE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_EMISION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| Q_BULTOS_SUGERIDOS | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_BULTOS_PROV_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_FACTOR_PROV_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_BULTOS_PROV_BONIF | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_BULTOS_EMPR_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_FACTOR_EMPR_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_PESO_UNIT_ART | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_PESO_TOTAL_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_PESO_TOTAL_BONIF | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| C_IVA_EN_CALCULO | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_COEF_IVA | numeric | 5 | 5 | 4 | True | False |  |  |  |  |
| K_IMP_INTERNO | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| I_COSTO_BASE | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_PRECIO_COMPRA | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_PRECIO_PARTE | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_PRECIO_LISTA | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_IMP_INTERNO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_ENVASES | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_TOTAL_IMP_INTERNO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_TOTAL_ITEM | money | 8 | 19 | 4 | True | False |  |  |  |  |
| Q_UNID_CUMPLIDAS | numeric | 5 | 8 | 0 | True | False |  |  |  |  |
| Q_PESO_CUMPLIDO | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| M_CUMPLIDA_PARCIAL | char | 1 | 0 | 0 | True | False |  |  |  |  |
| C_USUARIO_CUMPLIO_PARCIAL | char | 10 | 0 | 0 | True | False |  |  |  |  |
| F_CUMPLIDA_PARCIAL | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| U_PISO_PALETIZADO_OC | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| U_ALTURA_PALETIZADO_OC | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| C_DTO1_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO1_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO2_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO2_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO3_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO3_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO4_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO4_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO5_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO5_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO6_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO6_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO7_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO7_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO8_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO8_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO9_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO9_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO10_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO10_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| Q_VENTA_DIARIA | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_VENTA_MENSUAL | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| Q_DIAS_ENTREGA_PROVEEDOR | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| STOCK_UNIDADES | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| STOCK_VALORIZADO | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| PRECIO_COMPETENCIA | numeric | 5 | 6 | 2 | True | False |  |  |  |  |
| OBSERVACIONES | varchar | 13 | 0 | 0 | False | False |  |  |  |  |
| N_ARTICULO | nvarchar | 50 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_OC_DETALLE_EXT

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_EMISION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| Q_BULTOS_SUGERIDOS | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_BULTOS_PROV_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_FACTOR_PROV_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_BULTOS_PROV_BONIF | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_BULTOS_EMPR_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_FACTOR_EMPR_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_PESO_UNIT_ART | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_PESO_TOTAL_PED | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| Q_PESO_TOTAL_BONIF | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| C_IVA_EN_CALCULO | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_COEF_IVA | numeric | 5 | 5 | 4 | True | False |  |  |  |  |
| K_IMP_INTERNO | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| I_COSTO_BASE | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_PRECIO_COMPRA | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_PRECIO_PARTE | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_PRECIO_LISTA | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_IMP_INTERNO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_ENVASES | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_TOTAL_IMP_INTERNO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_TOTAL_ITEM | money | 8 | 19 | 4 | True | False |  |  |  |  |
| Q_UNID_CUMPLIDAS | numeric | 5 | 8 | 0 | True | False |  |  |  |  |
| Q_PESO_CUMPLIDO | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| M_CUMPLIDA_PARCIAL | char | 1 | 0 | 0 | True | False |  |  |  |  |
| C_USUARIO_CUMPLIO_PARCIAL | char | 10 | 0 | 0 | True | False |  |  |  |  |
| F_CUMPLIDA_PARCIAL | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| U_PISO_PALETIZADO_OC | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| U_ALTURA_PALETIZADO_OC | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| C_DTO1_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO1_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO2_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO2_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO3_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO3_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO4_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO4_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO5_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO5_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO6_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO6_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO7_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO7_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO8_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO8_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO9_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO9_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| C_DTO10_COMP | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| K_DTO10_COMP | numeric | 5 | 6 | 5 | True | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| I_COSTO_PP | money | 8 | 19 | 4 | True | False |  |  |  |  |
| Q_VENTA_DIARIA | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VENTA_MENSUAL | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 6 | 2 | False | False |  |  |  |  |
| Q_DIAS_ENTREGA_PROVEEDOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| STOCK_UNIDADES | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| STOCK_VALORIZADO | numeric | 9 | 13 | 2 | False | False |  |  |  |  |
| PRECIO_COMPETENCIA | numeric | 9 | 13 | 2 | False | False |  |  |  |  |
| OBSERVACIONES | varchar | 13 | 0 | 0 | False | False |  |  |  |  |
| N_ARTICULO | varchar | 25 | 0 | 0 | False | False |  |  |  |  |
| PEDIDO_UNIDADES | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| PENDIENTE_UNIDADES | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| ENTREGAS_PENDIENTES | numeric | 9 | 13 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T_RECUPERO_PROVEEDORES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| N_CLIENTE | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_DOC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DOC_PREFIJO_CONTROLADOR_FISCAL | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_DOC_SUFIJO_CONTROLADOR_FISCAL | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_DOC_LETRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SUCU_ORIG_ALTA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| I_TOTAL | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| D_OBSERVACION | char | 80 | 0 | 0 | False | False |  |  |  |  |
| ORIGEN | varchar | 25 | 0 | 0 | False | False |  |  |  |  |
| FLAG_1 | int | 4 | 10 | 0 | False | False |  |  |  |  |
| FLAG_2 | int | 4 | 10 | 0 | False | False |  |  |  |  |
| FLAG_3 | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T001_TABLA_CODIGO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_TABLA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CODIGO_TABLA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_CODIGO | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_CODIGO_ABREV | char | 10 | 0 | 0 | False | False |  |  |  |  |
| D_CODIGO_OFIC | char | 4 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADO_COMBO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_DOC_DE_COBRO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_PLANILLA_CAJA_OBSERV | char | 1 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T020_PROVEEDOR_GESTION_COMPRA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 12 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T051_ARTICULOS_SUCURSAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_BAUTIZADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_FACTOR_VENTA_ESP | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_OFERTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADO_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_DEVOLUCION_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ULT_ING_STOCK | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_DIA_ANT | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ACUM | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_STOCK_A_ULT_ING | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_15DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_30DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PENDIENTE_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_PENDIENTE_OC | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_PEND_RECEP_TRANSF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_VTA_MES_ACTUAL | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ULTIMA_VTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_15DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_30DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_PEND | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_EN_PREP | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| I_PRECIO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_PRECIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_COSTO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_COMPETENCIA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_COMPETENCIA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_OFERTA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_OFERTA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_AUX | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SECTOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SECTOR_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ORDEN_CARGA_OFERTA | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VENDE_SEGUN_CANTIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_precio_minimo_vta | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| M_ALTA_RENTABILIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PERFORAR_PMV | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_FRACCION | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_PRECIO_MINIMO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Lugar_Abastecimiento | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COSTO_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SISTEMATICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T052_ARTICULOS_PROVEEDOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| U_PISO_PALETIZADO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ALTURA_PALETIZADO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO_PROVEEDOR | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T055_ARTICULOS_PARAM_STOCK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T058_ARTICULOS_TRANSF_PEND

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_DEST | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_ORIG | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_BULTOS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_FACTOR | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_TRANSF_COMPLETA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| U_ID_SINCRO | int | 4 | 10 | 0 | False | False |  |  |  |  |
| M_ENVIADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_TRANSF_PRIORIDAD | varchar | 1 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.T080_OC_PRECARGA_KIKKER

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 9 | 18 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 9 | 18 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 9 | 18 | 0 | False | False |  |  |  |  |
| Q_BULTOS_KILOS_DIARCO | numeric | 9 | 18 | 0 | True | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| C_USUARIO_GENERO_OC | char | 51 | 0 | 0 | True | False |  |  |  |  |
| C_TERMINAL_GENERO_OC | char | 51 | 0 | 0 | True | False |  |  |  |  |
| F_GENERO_OC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| C_USUARIO_BLOQUEO | char | 51 | 0 | 0 | True | False |  |  |  |  |
| M_PROCESADO | char | 1 | 0 | 0 | True | False |  |  |  |  |
| F_PROCESADO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 9 | 18 | 0 | True | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 9 | 18 | 0 | True | False |  |  |  |  |
| C_COMPRA_KIKKER | char | 51 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 51 | 0 | 0 | True | False |  |  |  |  |
| C_COMPRADOR | numeric | 9 | 18 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T085_ARTICULOS_EAN

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_EAN | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T114_RUBROS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| D_RUBRO | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_RUBRO_PADRE | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO_NIVEL | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EXCLUIDA_EN_VALORIZ | char | 1 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T710_ESTADIS_DETALLE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F_DIA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| M_DOMINGO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_VENTA | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_STOCK | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.T710_ESTADIS_REPOSICION

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_VENTA_30_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_15_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_DOMINGO | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_ESPECIAL_30_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_ESPECIAL_15_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_DIAS_CON_STOCK | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| Q_REPONER | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_REPONER_INCLUIDO_SOBRE_STOCK | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| M_SEMAFORO_INDIVIDUAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_SEMAFORO_GLOBAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_VENTA_DIARIA_NORMAL | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_ENTREGA_PROVEEDOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.TMP_Base_Productos

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| ABASTECIMIENTO | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| COD_CD | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| LINEA | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| FECHA_REGISTRO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| FECHA_BAJA | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| UNID_TRANSFERENCIA | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| Q_UNID_TRANSFERENCIA | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| PEDIDO_MIN | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| FRENTE_LINEAL | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| CAPACID_GONDOLA | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| STOCK_MINIMO | varchar | 5 | 0 | 0 | True | False |  |  |  |  |
| COD_COMPRADOR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| PROMOCION | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| ACTIVE_FOR_PURCHASE | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| ACTIVE_FOR_SALE | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| ACTIVE_ON_MIX | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| DELIVERED_ID | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| PRODUCT_BASE_ID | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| OWN_PRODUCTION | varchar | 2 | 0 | 0 | True | False |  |  |  |  |
| FULL_CAPACITY_PALLET | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| NUMBER_OF_LAYERS | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| NUMBER_OF_BOXES_PER_BALLAST | varchar | 10 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## dbo.V_OC_CABECERA_SGM

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_OC_SGM | varchar | 83 | 0 | 0 | False | False |  |  |  |  |
| F_EMISION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_SUCU_DESTINO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DIAS_LIMITE_ENTREGA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| F_ENTREGA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO_OPERADOR | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_OC_CONNEXA_MENSUAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PEDIDO_CONNEXA | char | 20 | 0 | 0 | False | False |  |  |  |  |
| Total_SUCU | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_OC_CONNEXA_SEMANAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Semana_Ano | varchar | 15 | 0 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PEDIDO_CONNEXA | char | 20 | 0 | 0 | False | False |  |  |  |  |
| Total_SUCU | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_OC_DETALLE_SGM

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_BULTOS_PEDIDOS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_BULTOS_CUMPLIDOS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| M_CUMPLIDA_PARCIAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_BULTOS_PENDIENTES | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_USUARIO_CUMPLIO_PARCIAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_CUMPLIDA_PARCIAL | datetime | 8 | 23 | 3 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_OC_RESUMEN_MENSUAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Anio_Emision | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Mes_Emision | int | 4 | 10 | 0 | True | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| Total_OC | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Bultos_Pedidos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_OC_RESUMEN_SEMANAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Semana_Ano | varchar | 15 | 0 | 0 | False | False |  |  |  |  |
| Total_OC | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Bultos_Pedidos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_T080_OC_PRECARGA_KIKKER

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_BULTOS_KILOS_DIARCO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_GENERO_OC | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_GENERO_OC | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_GENERO_OC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BLOQUEO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_PROCESADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_PROCESADO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_COMPRA_KIKKER | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_T874_OC_PRECARGA_KIKKER_HIST

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_BULTOS_KILOS_DIARCO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_GENERO_OC | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_GENERO_OC | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_GENERO_OC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BLOQUEO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_PROCESADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_PROCESADO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_COMPRA_KIKKER | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_USO_MENSUAL_COMPRADOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| Total_Prv_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Prv_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_USO_MENSUAL_PROVEEDOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_USO_SEMANAL_COMPRADOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| SEMANA | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| Total_Prv_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Prv_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## dbo.V_USO_SEMANAL_PROVEEDOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| SEMANA | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## repl.BASE_PRODUCTOS_EN_TRANSITO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_SUCU_ORIG | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DEST | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_ARTICULO | char | 60 | 0 | 0 | False | False |  |  |  |  |
| Q_UNID_PESO_TRANSF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_RECEP | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_TRANSITO | numeric | 9 | 14 | 3 | True | False |  |  |  |  |
| vNsucursalOrig | char | 50 | 0 | 0 | False | False |  |  |  |  |
| vNsucursalDest | char | 50 | 0 | 0 | False | False |  |  |  |  |
| vPrecioVtaDEsTd | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_COEF_IVA | numeric | 5 | 4 | 3 | False | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.BASE_PRODUCTOS_VIGENTES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | int | 4 | 10 | 0 | False | False |  |  |  |  |
| C_ARTICULO | int | 4 | 10 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| ABASTECIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| COD_CD | nvarchar | 64 | 0 | 0 | True | False |  |  |  |  |
| HABILITADO | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| FECHA_REGISTRO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| FECHA_BAJA | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| UNID_TRANSFERENCIA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_UNID_TRANSFERENCIA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| PEDIDO_MIN | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| FRENTE_LINEAL | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CAPACID_GONDOLA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| STOCK_MINIMO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| COD_COMPRADOR | int | 4 | 10 | 0 | True | False |  |  |  |  |
| PROMOCION | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_FOR_PURCHASE | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_FOR_SALE | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_ON_MIX | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| DELIVERED_ID | nvarchar | 64 | 0 | 0 | True | False |  |  |  |  |
| PRODUCT_BASE_ID | nvarchar | 200 | 0 | 0 | True | False |  |  |  |  |
| OWN_PRODUCTION | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| Q_FACTOR_COMPRA | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| FULL_CAPACITY_PALLET | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| NUMBER_OF_LAYERS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| NUMBER_OF_BOXES_PER_LAYER | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| fecha_extraccion | datetime2 | 6 | 19 | 0 | False | False |  |  | (sysdatetime()) |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| CX_BASE_PRODUCTOS_VIGENTES | CLUSTERED | False | False | False |  |
| IX_SP_BASE_PRODUCTOS_SUCURSAL__COD_CD | NONCLUSTERED | False | False | False |  |

## repl.BASE_PRODUCTOS_VIGENTES_OLD

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | int | 4 | 10 | 0 | False | False |  |  |  |  |
| C_ARTICULO | int | 4 | 10 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| ABASTECIMIENTO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| COD_CD | nvarchar | 64 | 0 | 0 | True | False |  |  |  |  |
| HABILITADO | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| FECHA_REGISTRO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| FECHA_BAJA | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| UNID_TRANSFERENCIA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_UNID_TRANSFERENCIA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| PEDIDO_MIN | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| FRENTE_LINEAL | int | 4 | 10 | 0 | True | False |  |  |  |  |
| CAPACID_GONDOLA | int | 4 | 10 | 0 | True | False |  |  |  |  |
| STOCK_MINIMO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| COD_COMPRADOR | int | 4 | 10 | 0 | True | False |  |  |  |  |
| PROMOCION | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_FOR_PURCHASE | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_FOR_SALE | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| ACTIVE_ON_MIX | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| DELIVERED_ID | nvarchar | 64 | 0 | 0 | True | False |  |  |  |  |
| PRODUCT_BASE_ID | nvarchar | 200 | 0 | 0 | True | False |  |  |  |  |
| OWN_PRODUCTION | bit | 1 | 1 | 0 | False | False |  |  |  |  |
| Q_FACTOR_COMPRA | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| FULL_CAPACITY_PALLET | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| NUMBER_OF_LAYERS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| NUMBER_OF_BOXES_PER_LAYER | decimal | 9 | 18 | 6 | True | False |  |  |  |  |
| fecha_extraccion | datetime2 | 6 | 19 | 0 | False | False |  |  | (sysdatetime()) |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| CX_BASE_PRODUCTOS_VIGENTES | CLUSTERED | False | False | False |  |
| IX_SP_BASE_PRODUCTOS_SUCURSAL__COD_CD | NONCLUSTERED | False | False | False |  |

## repl.fnd_site_names

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| code | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| name | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.Historico_Stock_Sucursal

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Anio | decimal | 5 | 4 | 0 | False | False |  |  |  |  |
| Mes | decimal | 5 | 2 | 0 | False | False |  |  |  |  |
| Dia | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Sucursal | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| Articulo | decimal | 5 | 6 | 0 | False | False |  |  |  |  |
| Cantidad | decimal | 9 | 11 | 3 | False | False |  |  |  |  |
| Fecha_Stock | date | 3 | 10 | 0 | True | False |  |  |  |  |
| Fecha_Procesos | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Procesado | bit | 1 | 1 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.IMPORTAR_TRANSF_CONNEXA_IN

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| c_articulo | decimal | 5 | 6 | 0 | True | False |  |  |  |  |
| c_sucu_dest | decimal | 5 | 3 | 0 | True | False |  |  |  |  |
| c_sucu_orig | decimal | 5 | 3 | 0 | True | False |  |  |  |  |
| q_requerida | decimal | 9 | 13 | 3 | True | False |  |  |  |  |
| q_bultos | decimal | 9 | 13 | 3 | True | False |  |  |  |  |
| q_factor | decimal | 5 | 6 | 0 | True | False |  |  |  |  |
| f_alta | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| m_alta_prioridad | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| vchUsuario | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| vchTerminal | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| forzarTransf | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  | ('PENDIENTE') |  |
| mensaje_error | varchar | 255 | 0 | 0 | True | False |  |  |  |  |
| u_id_sincro | int | 4 | 10 | 0 | True | False |  |  |  |  |
| f_procesado | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| connexa_header_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| connexa_detail_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| estado_vk | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje_error_vk | varchar | 255 | 0 | 0 | True | False |  |  |  |  |
| f_procesado_vk | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__IMPORTAR__3213E83F3AFBE09A | CLUSTERED | True | True | False |  |

## repl.LOGS_T000_GESTION_COMPRA_PROVEEDOR_DETA_DIA_ANT_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T00__3213E83FCABFAD49 | CLUSTERED | True | True | False |  |

## repl.LOGS_T000_SNC_PLAN_SEMANA_VIGENTE_DIA_ANT_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T00__3213E83F015A1776 | CLUSTERED | True | True | False |  |

## repl.LOGS_T020_PROV_GESTION_COMPRA_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T02__3213E83F5483A7DE | CLUSTERED | True | True | False |  |

## repl.LOGS_T020_PROVEEDOR_DIAS_ENTREGA_CABE_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T02__3213E83F229B2D43 | CLUSTERED | True | True | False |  |

## repl.LOGS_T020_PROVEEDOR_DIAS_ENTREGA_DETA_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T02__3213E83F2C6C6407 | CLUSTERED | True | True | False |  |

## repl.LOGS_T020_PROVEEDOR_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T02__3213E83FB2A7781A | CLUSTERED | True | True | False |  |

## repl.LOGS_T021_PROV_COMPROB_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T02__3213E83F7690ADFB | CLUSTERED | True | True | False |  |

## repl.LOGS_T050_ARTICULOS_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T05__3213E83F1CE53409 | CLUSTERED | True | True | False |  |

## repl.LOGS_T051_ARTICULOS_SUCURSAL_BARRIO_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T05__3213E83FDEF224B3 | CLUSTERED | True | True | False |  |

## repl.LOGS_T051_ARTICULOS_SUCURSAL_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T05__3213E83FDB8C66E2 | CLUSTERED | True | True | False |  |

## repl.LOGS_T052_ARTICULOS_PROVEEDOR_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T05__3213E83F4A977CC0 | CLUSTERED | True | True | False |  |

## repl.LOGS_T055_ART_SUCU_PROV_DIAS_ENTREGA_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__T055_ART__3213E83F0ACDDDD8 | CLUSTERED | True | True | False |  |

## repl.LOGS_T055_ARTICULOS_CONDCOMPRA_COSTOS_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T05__3213E83FA7F6E755 | CLUSTERED | True | True | False |  |

## repl.LOGS_T055_ARTICULOS_PARAM_STOCK_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T05__3213E83FB613AC8B | CLUSTERED | True | True | False |  |

## repl.LOGS_T055_LEAD_TIME_B2_SUCURSALES_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T05__3213E83F5726D038 | CLUSTERED | True | True | False |  |

## repl.LOGS_T060_STOCK_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T06__3213E83F63F51820 | CLUSTERED | True | True | False |  |

## repl.LOGS_T061_STOCK_DIARIO_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T06__3213E83F8BA19819 | CLUSTERED | True | True | False |  |

## repl.LOGS_T079_SNC_CUOTAS_CABE_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T07__3213E83FB6C47833 | CLUSTERED | True | True | False |  |

## repl.LOGS_T079_SNC_CUOTAS_DETA_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T07__3213E83F42321F7F | CLUSTERED | True | True | False |  |

## repl.LOGS_T080_OC_CABE_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T08__3213E83FD5BDC03B | CLUSTERED | True | True | False |  |

## repl.LOGS_T081_OC_DETA_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T08__3213E83F0B638120 | CLUSTERED | True | True | False |  |

## repl.LOGS_T085_ARTICULOS_EAN_EDI_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T08__3213E83F15794F3F | CLUSTERED | True | True | False |  |

## repl.LOGS_T090_COMPETENCIA_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T09__3213E83F9ADBDE4D | CLUSTERED | True | True | False |  |

## repl.LOGS_T091_COMPETENCIA_PRECIOS_CABE_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T09__3213E83F7929F156 | CLUSTERED | True | True | False |  |

## repl.LOGS_T091_COMPETENCIA_PRECIOS_DETA_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T09__3213E83F9F0F528D | CLUSTERED | True | True | False |  |

## repl.LOGS_T100_EMPRESA_SUC_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T10__3213E83F5EBD951E | CLUSTERED | True | True | False |  |

## repl.LOGS_T114_RUBROS_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T11__3213E83FF70242A5 | CLUSTERED | True | True | False |  |

## repl.LOGS_T117_COMPRADORES_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | False | False |  |  | (getdate()) |  |
| estado | varchar | 20 | 0 | 0 | False | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T11__3213E83FFEE86429 | CLUSTERED | True | True | False |  |

## repl.LOGS_T230_FACTURADOR_NEGOCIOS_ESPECIALES_POR_CANTIDAD_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T23__3213E83F40E10813 | CLUSTERED | True | True | False |  |

## repl.LOGS_T702_VTAS_BARRIO_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T70__3213E83F4164C9BB | CLUSTERED | True | True | False |  |

## repl.LOGS_T702_VTAS_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T70__3213E83F58D7D629 | CLUSTERED | True | True | False |  |

## repl.LOGS_T710_ESTADIS_OFERTA_FOLDER_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T71__3213E83FE53A45A3 | CLUSTERED | True | True | False |  |

## repl.LOGS_T710_ESTADIS_PRECIOS_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T71__3213E83F0F37FB3C | CLUSTERED | True | True | False |  |

## repl.LOGS_T710_REPOSICION_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T71__3213E83F00A905F8 | CLUSTERED | True | True | False |  |

## repl.LOGS_T710_SYNC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| fecha_ejecucion | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| registros_afectados | int | 4 | 10 | 0 | True | False |  |  |  |  |
| duracion_segundos | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__LOGS_T71__3213E83FD2DBB81E | CLUSTERED | True | True | False |  |

## repl.M_3_ARTICULOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_ARTICULO | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_1 | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_2 | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| C_SUBRUBRO_3 | numeric | 5 | 5 | 0 | True | False |  |  |  |  |
| D_CODIGO_ABREV_VTA | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| D_CODIGO_ABREV_CPRA | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| Q_FACTOR_CPRA_SUCU | decimal | 9 | 18 | 4 | True | False |  |  |  |  |
| CLASIFICACION | int | 4 | 10 | 0 | True | False |  |  |  |  |
| PLAZO_VALIDEZ | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| PLAZO_ACEPTACION | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| PLAZO_RETIRO_GONDOLA | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | decimal | 9 | 10 | 0 | True | False |  |  |  |  |
| EAN | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| ARTICULO_BASE | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| PROP_BAJA_BASE | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| COD_ERP_PROD_COMPRA | decimal | 9 | 10 | 0 | True | False |  |  |  |  |
| PRECIO_COMPRA | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| OTRA_COLUMNA | varchar | -1 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| EAN_ALTERNATIVO_1 | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| EAN_ALTERNATIVO_2 | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| EAN_ALTERNATIVO_3 | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| EAN_ALTERNATIVO_4 | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| DUN14 | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__M_3_ARTI__CED1FE6711D8B689 | CLUSTERED | True | True | False |  |

## repl.M_91_SUCURSALES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ID_TIENDA | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| SUC_NOMBRE | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| F_DATO | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| F_PROC | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| SUC_ABREV | varchar | 4 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.MV_USO_MENSUAL_COMPRADOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| Total_Prv_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Prv_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 6 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.MV_USO_MENSUAL_PROVEEDOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| MES | nvarchar | 8000 | 0 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 6 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.MV_USO_SEMANAL_COMPRADOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| SEMANA | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| Total_Prv_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_Prv_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 6 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.MV_USO_SEMANAL_PROVEEDOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| SEMANA | varchar | 15 | 0 | 0 | True | False |  |  |  |  |
| Total_Pedidos_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_CNX | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_OC_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Total_BULTOS_SGM | int | 4 | 10 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 6 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.SUCURSALES_EXCLUIDAS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T_COMPETENCIA_DETALLE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_ARTICULO | char | 60 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| MG% | money | 8 | 19 | 4 | True | False |  |  |  |  |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| N_COMPETIDOR | char | 30 | 0 | 0 | False | False |  |  |  |  |
| I_PRECIO_COMPETIDOR | money | 8 | 19 | 4 | False | False |  |  |  |  |
| MG2% | money | 8 | 19 | 4 | True | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T000_GESTION_COMPRA_PROVEEDOR_DETA_DIA_ANTERIOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_MOTIVO_SNC | char | 50 | 0 | 0 | False | False |  |  |  |  |
| I_ACT_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ACT_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT1_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT1_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT2_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT2_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ACUM_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ACUM_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T000_GESTION_COMPRA_PROVEEDOR_DETA_DIA_ANTERIOR_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_MOTIVO_SNC | char | 50 | 0 | 0 | False | False |  |  |  |  |
| I_ACT_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ACT_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT1_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT1_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT2_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ANT2_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ACUM_EMITIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ACUM_RECIBIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T000_SNC_PLAN_SEMANA_VIGENTE_DIA_ANT

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_NRO_INT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_PROVEEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| F_DESDE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_HASTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SECTOR_PARA_ESTADISTICAS | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DESCUENTO | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| I_BASE_NETO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_BASE_NETO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_IVA_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_IVA_1 | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| I_NETO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_IVA_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_IVA_2 | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| D_DESCUENTO_LEYENDA | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_DESCUENTO | char | 100 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_PLAN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_CIERRE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T000_SNC_PLAN_SEMANA_VIGENTE_DIA_ANT_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_NRO_INT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_PROVEEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| F_DESDE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_HASTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SECTOR_PARA_ESTADISTICAS | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DESCUENTO | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| I_BASE_NETO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_BASE_NETO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_IVA_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_IVA_1 | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| I_NETO_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_IVA_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_IVA_2 | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| D_DESCUENTO_LEYENDA | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_DESCUENTO | char | 100 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_PLAN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_CIERRE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T020_PROVEEDOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | decimal | 5 | 6 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_DIARCO | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| C_IB | char | 12 | 0 | 0 | False | False |  |  |  |  |
| M_EXENTO_IB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_COND_IVA | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| M_EXENTO_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_COND_GANAN | char | 4 | 0 | 0 | False | False |  |  |  |  |
| K_COEF_DIFER_RET_GCIAS | decimal | 5 | 5 | 4 | False | False |  |  |  |  |
| F_HASTA_RETEN_GANAN | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| N_PROVEEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_DOMICILIO_INM | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_LOCALIDAD_INM | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_POSTAL_INM | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_INM | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| N_DOMICILIO_ENTREGA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_LOCALIDAD_ENTREGA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_ENTREGA | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TELEFONO | char | 50 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ACTIVO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_LINEA_PRODUCTO | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_EANEDI | char | 15 | 0 | 0 | False | False |  |  |  |  |
| N_VENDEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_CHEQ_ALAORDEN | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_COND_PAGO | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_COND_FLETE | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_ENCARG_COMPRA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_WEB | char | 35 | 0 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA1 | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA2 | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA3 | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA4 | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA5 | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA6 | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_LUN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_MAR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_MIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_JUE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_VIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_SAB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_DOM | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_ENVASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_AGENTE_RET_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_REPROWEB_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_REPROWEB_VTO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_REPROWEB_VALIDACION | char | 21 | 0 | 0 | False | False |  |  |  |  |
| M_REPROWEB_SUSTITUTIVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_REPROWEB_EXCL_RET_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| K_REPROWEB_EXCL_RET_IVA | decimal | 5 | 5 | 4 | False | False |  |  |  |  |
| F_REPROWEB_EXCL_RET_IVA_VENC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_REPROWEB_EXCL_RET_IVA_RG18 | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_COMPRA_PALETIZADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EMPLEADOR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EXCL_RET_SUSS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_EXCL_RET_SUSS_VENC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| K_EXCL_RET_SUSS | decimal | 5 | 5 | 4 | False | False |  |  |  |  |
| F_CTROL_EMPLEADOR | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ALTA_MANUAL_PADRON_IB_BUENOS_AIRES | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_CM_IB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_ORIGEN_PROVEEDOR | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| N_ESTADO | char | 30 | 0 | 0 | False | False |  |  |  |  |
| N_PAIS | char | 30 | 0 | 0 | False | False |  |  |  |  |
| N_CIUDAD | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_TAX_PAY_NUMBER | char | 20 | 0 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_SALTA | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| U_CM_ANIO | decimal | 5 | 4 | 0 | False | False |  |  |  |  |
| F_CM_VTO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_PRESENTO_FORM_IB_MEND | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| F_VENC_FORM_IB_MEND | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_REG_SIMPLIFICADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PERCEP_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_TOPE_PARA_AGENDA | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DIAS_TOPE_PARA_INGRESO_AGENDA | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_LUN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_MAR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_MIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_JUE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_VIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_SAB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_DOM | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PERIODO_ATENCION_AGENDA | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_CORDOBA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_MISIONES | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_INSCRIPTO_SEG_HIG_POSADAS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL_1 | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL_2 | char | 50 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_PAGO | decimal | 5 | 4 | 0 | False | False |  |  |  |  |
| C_CLIENTE | decimal | 5 | 8 | 0 | False | False |  |  |  |  |
| M_COMPENSACION_DIRECTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_NIVEL_PROVEEDOR | decimal | 5 | 2 | 0 | False | False |  |  |  |  |
| N_EMAIL_3 | char | 100 | 0 | 0 | False | False |  |  |  |  |
| M_CERTIFICADO_CUMPLIMIENTO_FISCAL_TF | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_CERTIFICADO_CUMPLIMIENTO_FISCAL_TF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_VENCIMIENTO_F_FACTURA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_EDI | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_EDI_CPTE | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T020_PROVEEDOR_DIAS_ENTREGA_CABE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| N_CARGO_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| N_TELEFONO_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T020_PROVEEDOR_DIAS_ENTREGA_CABE_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| N_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| N_CARGO_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| N_TELEFONO_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL_CONTACTO_LOGISTICO | varchar | 50 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T020_PROVEEDOR_DIAS_ENTREGA_DETA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_1 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_2 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_3 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_4 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_5 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_6 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_7 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_1 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_2 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_3 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_4 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_5 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_6 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_7 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_OC | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_OC_LIMITE | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| I_COMPRA_MINIMA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_BULTOS_KILOS_COMPRA_MINIMA | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_DIAS_PREPARACION | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCURSAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PALLETS | numeric | 9 | 10 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T020_PROVEEDOR_DIAS_ENTREGA_DETA_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_1 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_2 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_3 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_4 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_5 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_6 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_CARGA_7 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_1 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_2 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_3 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_4 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_5 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_6 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_ENTREGA_7 | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_OC | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| U_DIA_SEMANA_OC_LIMITE | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| I_COMPRA_MINIMA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_BULTOS_KILOS_COMPRA_MINIMA | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_DIAS_PREPARACION | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCURSAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PALLETS | numeric | 9 | 10 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T020_PROVEEDOR_GESTION_COMPRA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 12 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |
| IX_T020_PROV_USUARIO_UNIQUE | NONCLUSTERED | True | False | False |  |

## repl.T020_PROVEEDOR_GESTION_COMPRA_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 12 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T020_PROVEEDOR_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_DIARCO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| C_IB | char | 12 | 0 | 0 | False | False |  |  |  |  |
| M_EXENTO_IB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_COND_IVA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_EXENTO_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_COND_GANAN | char | 4 | 0 | 0 | False | False |  |  |  |  |
| K_COEF_DIFER_RET_GCIAS | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| F_HASTA_RETEN_GANAN | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| N_PROVEEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_DOMICILIO_INM | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_LOCALIDAD_INM | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_POSTAL_INM | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_INM | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| N_DOMICILIO_ENTREGA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_LOCALIDAD_ENTREGA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_ENTREGA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TELEFONO | char | 50 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ACTIVO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_LINEA_PRODUCTO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_EANEDI | char | 15 | 0 | 0 | False | False |  |  |  |  |
| N_VENDEDOR | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_CHEQ_ALAORDEN | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_COND_PAGO | char | 50 | 0 | 0 | False | False |  |  |  |  |
| D_COND_FLETE | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_ENCARG_COMPRA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_WEB | char | 35 | 0 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA4 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA5 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA6 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_LUN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_MAR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_MIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_JUE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_VIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_SAB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_CLIE_DOM | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_ENVASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_AGENTE_RET_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_REPROWEB_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_REPROWEB_VTO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_REPROWEB_VALIDACION | char | 21 | 0 | 0 | False | False |  |  |  |  |
| M_REPROWEB_SUSTITUTIVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_REPROWEB_EXCL_RET_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| K_REPROWEB_EXCL_RET_IVA | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| F_REPROWEB_EXCL_RET_IVA_VENC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_REPROWEB_EXCL_RET_IVA_RG18 | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_COMPRA_PALETIZADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EMPLEADOR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EXCL_RET_SUSS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_EXCL_RET_SUSS_VENC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| K_EXCL_RET_SUSS | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| F_CTROL_EMPLEADOR | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ALTA_MANUAL_PADRON_IB_BUENOS_AIRES | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_CM_IB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_ORIGEN_PROVEEDOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| N_ESTADO | char | 30 | 0 | 0 | False | False |  |  |  |  |
| N_PAIS | char | 30 | 0 | 0 | False | False |  |  |  |  |
| N_CIUDAD | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_TAX_PAY_NUMBER | char | 20 | 0 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_SALTA | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| U_CM_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| F_CM_VTO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_PRESENTO_FORM_IB_MEND | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| F_VENC_FORM_IB_MEND | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_REG_SIMPLIFICADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PERCEP_IVA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_TOPE_PARA_AGENDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DIAS_TOPE_PARA_INGRESO_AGENDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_LUN | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_MAR | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_MIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_JUE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_VIE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_SAB | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ATEN_PROV_DOM | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PERIODO_ATENCION_AGENDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_CORDOBA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_COBRA_PERCEP_SEGHIG_MISIONES | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_INSCRIPTO_SEG_HIG_POSADAS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL_1 | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_EMAIL_2 | char | 50 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_PAGO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_CLIENTE | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_COMPENSACION_DIRECTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_NIVEL_PROVEEDOR | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| N_EMAIL_3 | char | 100 | 0 | 0 | False | False |  |  |  |  |
| M_CERTIFICADO_CUMPLIMIENTO_FISCAL_TF | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_CERTIFICADO_CUMPLIMIENTO_FISCAL_TF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_VENCIMIENTO_F_FACTURA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_EDI | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_EDI_CPTE | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T021_PROV_COMPROB

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_DOC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_COMP | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_COMP | numeric | 5 | 9 | 0 | False | False |  |  |  |  |
| C_LETRA_COMP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_MOTIVO_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_RUBRO_PARA_ESTADISTICAS | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNCSND | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_RETEN_SEGU_SOCIAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_RECEP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR_CONTAB | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_CUENTA_CONTAB | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_COMP | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_COMP_VTO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_CITI | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_DOC_APLIC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DOC_APLIC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR_GENERO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_DOC_GENERO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_COMP_GENERO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_COMP_GENERO | numeric | 9 | 14 | 0 | False | False |  |  |  |  |
| C_BANCO_GENERO | char | 3 | 0 | 0 | False | False |  |  |  |  |
| U_CHEQUE_GENERO | numeric | 9 | 15 | 0 | False | False |  |  |  |  |
| C_DOC_PAGO_ANT | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DOC_PAGO_ANT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_DBCR | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| I_TOTAL_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_2100 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_1050 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_2700 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2100 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_1050 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2700 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_2100_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2100_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_1050_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_1050_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PERC_BEBIDAS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PERC_GANANCIAS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PERC_IVA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_EXENTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMPUESTOS_INTER | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NOINSCRIPTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_INTER_GRAVADOS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_INTER_EXENTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ABASTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_MONOTRIBUTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_CF | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB4 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB5 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB6 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB7 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB8 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB9 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB10 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB11 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB12 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_CTA_IMPUTACION | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| D_TRANSP | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_PROGRAMA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_SEGHIG1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_SEGHIG1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_DEBITO_AUTOMATICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_TARJETA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_CUOTAS_TARJETA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| I_SERVICIOS_EVENTUALES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_SEGHIG2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_SEGHIG2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_SEGHIG3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_SEGHIG3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_AJUSTE_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_AJUSTO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_AJUSTE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_REM_CON_SNCAPLIC_EN_PAGO_A_PROV | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_LEY19640 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_CAI | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_VTO_CAI | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_FISCALIZADA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_IB13 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB14 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB15 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB16 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB17 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB18 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB19 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB20 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB21 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB22 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB23 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB24 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| SAP_CENTRO_COSTO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_CENTRO_BENEFICIO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_FORMA_PAGO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_ACTIVO_FIJO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_CUENTA_CONTABLE | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| M_PASADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| SAP_ORDEN | nvarchar | 24 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T021_PROV_COMPROB_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_DOC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_COMP | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_COMP | numeric | 5 | 9 | 0 | False | False |  |  |  |  |
| C_LETRA_COMP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_MOTIVO_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_RUBRO_PARA_ESTADISTICAS | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNCSND | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_RETEN_SEGU_SOCIAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_RECEP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR_CONTAB | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_CUENTA_CONTAB | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_COMP | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_COMP_VTO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_CITI | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_DOC_APLIC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DOC_APLIC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR_GENERO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_DOC_GENERO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_COMP_GENERO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_COMP_GENERO | numeric | 9 | 14 | 0 | False | False |  |  |  |  |
| C_BANCO_GENERO | char | 3 | 0 | 0 | False | False |  |  |  |  |
| U_CHEQUE_GENERO | numeric | 9 | 15 | 0 | False | False |  |  |  |  |
| C_DOC_PAGO_ANT | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DOC_PAGO_ANT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_DBCR | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| I_TOTAL_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_2100 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_1050 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_2700 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2100 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_1050 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2700 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_2100_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_2100_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NETO_1050_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_1050_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PERC_BEBIDAS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PERC_GANANCIAS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PERC_IVA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_EXENTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMPUESTOS_INTER | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_NOINSCRIPTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_INTER_GRAVADOS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_INTER_EXENTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COMISIONES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ABASTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_MONOTRIBUTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_CF | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB4 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB5 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB6 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB7 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB8 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB9 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB10 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB11 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB12 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_CTA_IMPUTACION | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| D_TRANSP | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_PROGRAMA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_SEGHIG1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_SEGHIG1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_DEBITO_AUTOMATICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_TARJETA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_CUOTAS_TARJETA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| I_SERVICIOS_EVENTUALES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_SEGHIG2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_SEGHIG2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_SEGHIG3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_SEGHIG3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_AJUSTE_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_AJUSTO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_AJUSTE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_REM_CON_SNCAPLIC_EN_PAGO_A_PROV | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_LEY19640 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_CAI | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_VTO_CAI | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_FISCALIZADA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_IB13 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB14 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB15 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB16 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB17 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB18 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB19 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB20 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB21 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB22 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB23 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_PROVINCIA_IB24 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PROVINCIA_IB24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| SAP_CENTRO_COSTO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_CENTRO_BENEFICIO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_FORMA_PAGO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_ACTIVO_FIJO | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| SAP_CUENTA_CONTABLE | nvarchar | 20 | 0 | 0 | False | False |  |  |  |  |
| M_PASADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| SAP_ORDEN | nvarchar | 24 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T050_ARTICULOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_1 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_2 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_FAMILIA_ANTERIOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_RUBRO_ANTERIOR | char | 4 | 0 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| N_ARTICULO | char | 60 | 0 | 0 | False | False |  |  |  |  |
| N_ARTICULO_FACT | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_MARCA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_IB | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_IVA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ENVASE | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_PESO_UNIT_ART | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| M_VENDE_POR_PESO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EXPORTACION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PROMOCION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_IMPORTADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_A_DAR_DE_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_INSTITUCIONAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_EAN | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_DUN14 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VENTA_ESP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| D_CARTEL_LIN1 | char | 20 | 0 | 0 | False | False |  |  |  |  |
| D_CARTEL_LIN2 | char | 20 | 0 | 0 | False | False |  |  |  |  |
| D_CARTEL_LIN3 | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_OCASION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| N_PAIS_ORIGEN | char | 20 | 0 | 0 | False | False |  |  |  |  |
| M_BEBIDA_ALCOHOLICA | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| C_UNIDAD_MEDIDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_UNIDAD_MEDIDA | numeric | 5 | 8 | 3 | False | False |  |  |  |  |
| M_CONSIGNACION | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_TARJETA_DE_CREDITO | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_TRANSP_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_UNICO_PRODUCTO_TRANSP_1 | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_UNID_MEDIDA_TRANSP_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CODIGO_EQUIV | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| M_CELIACO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ENVASE_VACIO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_3 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_4 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| D_AVISO_PROMOCION | char | 100 | 0 | 0 | False | False |  |  |  |  |
| M_SENSIBLE_VENTA | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| OK_COMPRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_TOP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VARIEDAD_EXTRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_SIN_VENTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_RESTO_TOP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_CLASIFICACION_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO_ALTERNATIVO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PRESENTACION | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_1 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_2 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_3 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_4 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| M_PRECIO_CUIDADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H12 | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_BULTO_TRANSPARENTE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_CONTROLO_SIEMPRE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H12_DIAS | char | 7 | 0 | 0 | False | False |  |  |  |  |
| M_PALLETS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ARTICULO_PADRE_PRECIO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ARTICULO_CON_SUSTITUTOS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H18 | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H18_DIAS | char | 7 | 0 | 0 | False | False |  |  |  |  |
| M_MERCADOLIBRE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H_NACION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H_NACION_DIAS | char | 7 | 0 | 0 | False | False |  |  |  |  |
| M_COSTO_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_IVA_VENTAS | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_IVA_VENTAS_CF | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_LIQ_PRODUCTO | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| F_LIQ_PRODUCTO_DESDE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_LIQ_PRODUCTO_HASTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_KOSHER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |
| IX_T050_ARTICULOS_BASE | NONCLUSTERED | False | False | False |  |

## repl.T050_ARTICULOS_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_1 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_2 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_FAMILIA_ANTERIOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_RUBRO_ANTERIOR | char | 4 | 0 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| N_ARTICULO | char | 60 | 0 | 0 | False | False |  |  |  |  |
| N_ARTICULO_FACT | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_MARCA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_IB | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_IVA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ENVASE | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_PESO_UNIT_ART | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| M_VENDE_POR_PESO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EXPORTACION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PROMOCION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_IMPORTADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_A_DAR_DE_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_INSTITUCIONAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_EAN | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_DUN14 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VENTA_ESP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| D_CARTEL_LIN1 | char | 20 | 0 | 0 | False | False |  |  |  |  |
| D_CARTEL_LIN2 | char | 20 | 0 | 0 | False | False |  |  |  |  |
| D_CARTEL_LIN3 | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_OCASION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| N_PAIS_ORIGEN | char | 20 | 0 | 0 | False | False |  |  |  |  |
| M_BEBIDA_ALCOHOLICA | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| C_UNIDAD_MEDIDA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_UNIDAD_MEDIDA | numeric | 5 | 8 | 3 | False | False |  |  |  |  |
| M_CONSIGNACION | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_TARJETA_DE_CREDITO | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_TRANSP_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_UNICO_PRODUCTO_TRANSP_1 | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_UNID_MEDIDA_TRANSP_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CODIGO_EQUIV | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| M_CELIACO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ENVASE_VACIO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_3 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUBRUBRO_4 | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| D_AVISO_PROMOCION | char | 100 | 0 | 0 | False | False |  |  |  |  |
| M_SENSIBLE_VENTA | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| OK_COMPRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_TOP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VARIEDAD_EXTRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_SIN_VENTA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_RESTO_TOP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_CLASIFICACION_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO_ALTERNATIVO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PRESENTACION | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_1 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_2 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_3 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| C_EAN_ALTERNATIVO_4 | char | 14 | 0 | 0 | False | False |  |  |  |  |
| M_PRECIO_CUIDADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H12 | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_BULTO_TRANSPARENTE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_CONTROLO_SIEMPRE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H12_DIAS | char | 7 | 0 | 0 | False | False |  |  |  |  |
| M_PALLETS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ARTICULO_PADRE_PRECIO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ARTICULO_CON_SUSTITUTOS | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H18 | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H18_DIAS | char | 7 | 0 | 0 | False | False |  |  |  |  |
| M_MERCADOLIBRE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H_NACION | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_H_NACION_DIAS | char | 7 | 0 | 0 | False | False |  |  |  |  |
| M_COSTO_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_IVA_VENTAS | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_IVA_VENTAS_CF | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_LIQ_PRODUCTO | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| F_LIQ_PRODUCTO_DESDE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_LIQ_PRODUCTO_HASTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_KOSHER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T051_ARTICULOS_SUCURSAL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_BAUTIZADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_FACTOR_VENTA_ESP | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_OFERTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADO_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_DEVOLUCION_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ULT_ING_STOCK | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_DIA_ANT | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ACUM | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_STOCK_A_ULT_ING | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_15DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_30DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PENDIENTE_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_PENDIENTE_OC | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_PEND_RECEP_TRANSF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_VTA_MES_ACTUAL | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ULTIMA_VTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_15DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_30DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_PEND | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_EN_PREP | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| I_PRECIO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_PRECIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_COSTO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_COMPETENCIA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_COMPETENCIA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_OFERTA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_OFERTA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_AUX | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SECTOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SECTOR_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ORDEN_CARGA_OFERTA | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VENDE_SEGUN_CANTIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_precio_minimo_vta | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| M_ALTA_RENTABILIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PERFORAR_PMV | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_FRACCION | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_PRECIO_MINIMO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Lugar_Abastecimiento | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COSTO_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SISTEMATICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK_T051_ARTICULOS_SUCURSAL | CLUSTERED | True | True | False |  |
| IX_T051_ART_SUC | NONCLUSTERED | False | False | False |  |
| IX_T051_SUC_ART | NONCLUSTERED | False | False | False |  |

## repl.T051_ARTICULOS_SUCURSAL_BARRIO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_BAUTIZADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_FACTOR_VENTA_ESP | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_OFERTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADO_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_DEVOLUCION_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ULT_ING_STOCK | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_DIA_ANT | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ACUM | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_STOCK_A_ULT_ING | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_15DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_30DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PENDIENTE_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_PENDIENTE_OC | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_PEND_RECEP_TRANSF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_VTA_MES_ACTUAL | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ULTIMA_VTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_15DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_30DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_PEND | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_EN_PREP | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| I_PRECIO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_PRECIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_COSTO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_COMPETENCIA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_COMPETENCIA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_OFERTA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_OFERTA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_AUX | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SECTOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SECTOR_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ORDEN_CARGA_OFERTA | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VENDE_SEGUN_CANTIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_precio_minimo_vta | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| M_ALTA_RENTABILIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PERFORAR_PMV | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_FRACCION | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_PRECIO_MINIMO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Lugar_Abastecimiento | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COSTO_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SISTEMATICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T051_ARTICULOS_SUCURSAL_BARRIO_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_BAUTIZADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_FACTOR_VENTA_ESP | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_OFERTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADO_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_DEVOLUCION_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ULT_ING_STOCK | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_DIA_ANT | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ACUM | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_STOCK_A_ULT_ING | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_15DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_30DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PENDIENTE_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_PENDIENTE_OC | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_PEND_RECEP_TRANSF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_VTA_MES_ACTUAL | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ULTIMA_VTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_15DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_30DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_PEND | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_EN_PREP | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| I_PRECIO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_PRECIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_COSTO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_COMPETENCIA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_COMPETENCIA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_OFERTA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_OFERTA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_AUX | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SECTOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SECTOR_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ORDEN_CARGA_OFERTA | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VENDE_SEGUN_CANTIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_precio_minimo_vta | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| M_ALTA_RENTABILIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PERFORAR_PMV | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_FRACCION | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_PRECIO_MINIMO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Lugar_Abastecimiento | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COSTO_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SISTEMATICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T051_ARTICULOS_SUCURSAL_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_BAUTIZADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_FACTOR_VENTA_ESP | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_SUCU | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_OFERTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADO_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_DEVOLUCION_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ULT_ING_STOCK | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_DIA_ANT | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ACUM | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_STOCK_A_ULT_ING | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_15DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_30DIASVTA_A_ULT_ING_STOCK | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PENDIENTE_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_PENDIENTE_OC | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_PEND_RECEP_TRANSF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_PESO_VTA_MES_ACTUAL | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ULTIMA_VTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_15DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_VTA_ULTIMOS_30DIAS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_PEND | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_TRANSF_EN_PREP | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| I_PRECIO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_PRECIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_NUEVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_USUARIO_ULT_CAMBIO_COSTO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_COMPETENCIA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_COMPETENCIA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| U_ANIO_ULT_CARGA_OFERTA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA_ULT_CARGA_OFERTA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| M_FOLDER | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_AUX | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SECTOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CARTEL_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SECTOR_2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ORDEN_CARGA_OFERTA | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA_SUCU | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VENDE_SEGUN_CANTIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| I_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_precio_minimo_vta | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| M_ALTA_RENTABILIDAD | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_PERFORAR_PMV | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_VTA_FRACCION | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_PRECIO_MINIMO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Lugar_Abastecimiento | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_COSTO_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SISTEMATICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_SEPA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T052_ARTICULOS_PROVEEDOR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| U_PISO_PALETIZADO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ALTURA_PALETIZADO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO_PROVEEDOR | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |
| IX_T052_ART_PROV | NONCLUSTERED | False | False | False |  |

## repl.T052_ARTICULOS_PROVEEDOR_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_FACTOR_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| U_PISO_PALETIZADO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ALTURA_PALETIZADO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO_PROVEEDOR | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_ART_SUCU_PROV_DIAS_ENTREGA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_ART_SUCU_PROV_DIAS_ENTREGA_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_ARTICULOS_CONDCOMPRA_COSTOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_LISTA_CALCULADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_BASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_IMP_INTERNOS | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| I_COSTO_ENVASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_OTROS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_EFECTIVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_COSTO_IVA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_COSTO_ULTIMO_CBIO_PRECIO_BASE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_ULTIMO_CBIO_PRECIO_BASE | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_DTO1_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO1_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO2_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO2_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO3_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO3_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO4_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO4_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO5_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO5_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO6_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO6_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO7_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO7_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO8_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO8_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO9_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO9_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO10_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO10_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DOC_ULT_ING | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_DOC_ULT_ING | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_DOC_ULT_ING | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| F_DOC_ULT_ING | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_ULT_ING | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ULT_ING | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| C_OC_ULT_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC_ULT_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC_ULT_COMP | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| I_PRECIO_OC_ULT_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_PARTE_OC_ULT_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_EMISION_OC_ULT_COMP | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_DTO1_ACCION | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO1_ACCION | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_ARTICULOS_CONDCOMPRA_COSTOS_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_LISTA_CALCULADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_BASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_IMP_INTERNOS | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| I_COSTO_ENVASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_OTROS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_EFECTIVO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_COSTO_IVA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_COSTO_ULTIMO_CBIO_PRECIO_BASE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_ULTIMO_CBIO_PRECIO_BASE | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_DTO1_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO1_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO2_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO2_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO3_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO3_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO4_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO4_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO5_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO5_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO6_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO6_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO7_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO7_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO8_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO8_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO9_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO9_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO10_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO10_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DOC_ULT_ING | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_DOC_ULT_ING | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_DOC_ULT_ING | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| F_DOC_ULT_ING | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_ULT_ING | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ULT_ING | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| C_OC_ULT_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC_ULT_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC_ULT_COMP | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| I_PRECIO_OC_ULT_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_PARTE_OC_ULT_COMP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_EMISION_OC_ULT_COMP | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_DTO1_ACCION | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO1_ACCION | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_ARTICULOS_PARAM_STOCK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CLAISIFICACION_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_ARTICULOS_PARAM_STOCK_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_CLAISIFICACION_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_LEAD_TIME_B2_SUCURSALES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCURSAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| DIAS_ENTREGA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T055_LEAD_TIME_B2_SUCURSALES_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCURSAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| DIAS_ENTREGA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T060_STOCK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_UNID_ARTICULO | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_STOCK_MAXIMO | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO_STOCK_MAXIMO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_DESDE_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_HASTA_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK_T060_STOCK | CLUSTERED | True | True | False |  |
| IX_T060_STOCK_C_ARTICULO | NONCLUSTERED | False | False | False |  |

## repl.T060_STOCK_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_UNID_ARTICULO | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_STOCK_MAXIMO | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO_STOCK_MAXIMO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_DESDE_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_HASTA_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T061_STOCK_DIARIO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | decimal | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_DIA_ANT | decimal | 5 | 8 | 0 | False | False |  |  | ((0)) |  |
| Q_PESO_ARTICULO_DIA_ANT | decimal | 9 | 13 | 3 | False | False |  |  | ((0)) |  |
| Q_UNID_ARTICULO_VEND | decimal | 5 | 8 | 0 | False | False |  |  | ((0)) |  |
| Q_PESO_ARTICULO_VEND | decimal | 9 | 13 | 3 | False | False |  |  | ((0)) |  |
| Q_UNID_ARTICULO_EGRE | decimal | 5 | 8 | 0 | False | False |  |  | ((0)) |  |
| Q_PESO_ARTICULO_EGRE | decimal | 9 | 13 | 3 | False | False |  |  | ((0)) |  |
| Q_UNID_ARTICULO_INGR | decimal | 5 | 8 | 0 | False | False |  |  | ((0)) |  |
| Q_PESO_ARTICULO_INGR | decimal | 9 | 13 | 3 | False | False |  |  | ((0)) |  |
| Q_UNID_ARTICULO_STOCK_MAXIMO | decimal | 5 | 8 | 0 | False | False |  |  | ((0)) |  |
| Q_PESO_ARTICULO_STOCK_MAXIMO | decimal | 9 | 13 | 3 | False | False |  |  | ((0)) |  |
| F_DESDE_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  | ('01/01/1900') |  |
| F_HASTA_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  | ('01/01/1900') |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK_T061_STOCK_DIARIO | CLUSTERED | True | True | False |  |

## repl.T061_STOCK_DIARIO_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | decimal | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_DIA_ANT | decimal | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO_DIA_ANT | decimal | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_VEND | decimal | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO_VEND | decimal | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_EGRE | decimal | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO_EGRE | decimal | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_INGR | decimal | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO_INGR | decimal | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_UNID_ARTICULO_STOCK_MAXIMO | decimal | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_ARTICULO_STOCK_MAXIMO | decimal | 9 | 13 | 3 | False | False |  |  |  |  |
| F_DESDE_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_HASTA_STOCK_MAXIMO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T079_SNC_CUOTAS_CABE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_NRO_INT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_NETO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_IVA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_DEVENGADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| U_CUOTAS | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ULTIMA_CUOTA_DEBITADA | numeric | 9 | 18 | 0 | False | False |  |  |  |  |
| C_SECTOR_PARA_ESTADISTICAS | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_TIPO_DESCUENTO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_DESCUENTO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_DESCUENTO_LEYENDA | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_DESCUENTO | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION1 | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION_PROVEEDOR | char | 100 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 15 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 15 | 0 | 0 | False | False |  |  |  |  |
| D_MOTIVO_ANULACION | char | 100 | 0 | 0 | False | False |  |  |  |  |
| M_AUTORIZACION_ACUERDO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_USUARIO_AUTORIZO_ACUERDO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T079_SNC_CUOTAS_CABE_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_NRO_INT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_NETO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_IVA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_DEVENGADO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| U_CUOTAS | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_ULTIMA_CUOTA_DEBITADA | numeric | 9 | 18 | 0 | False | False |  |  |  |  |
| C_SECTOR_PARA_ESTADISTICAS | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_MOTIVO_SNC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_TIPO_DESCUENTO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_DESCUENTO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_DESCUENTO_LEYENDA | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_DESCUENTO | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION1 | char | 100 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION_PROVEEDOR | char | 100 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 15 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 15 | 0 | 0 | False | False |  |  |  |  |
| D_MOTIVO_ANULACION | char | 100 | 0 | 0 | False | False |  |  |  |  |
| M_AUTORIZACION_ACUERDO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_USUARIO_AUTORIZO_ACUERDO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T079_SNC_CUOTAS_DETA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_NRO_INT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| U_CUOTA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_CUOTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_CUOTA_ORIGINAL | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_DEBITO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_MES_DEBITO | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| U_ANIO_DEBITO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_DOC_ASOC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DOC_ASOC_PREFIJO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_DOC_ASOC_SUFIJO | numeric | 5 | 9 | 0 | False | False |  |  |  |  |
| C_DOC_ASOC_LETRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 15 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T079_SNC_CUOTAS_DETA_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_NRO_INT | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| U_CUOTA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_CUOTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_CUOTA_ORIGINAL | money | 8 | 19 | 4 | False | False |  |  |  |  |
| F_DEBITO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_MES_DEBITO | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| U_ANIO_DEBITO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_DOC_ASOC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_DOC_ASOC_PREFIJO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_DOC_ASOC_SUFIJO | numeric | 5 | 9 | 0 | False | False |  |  |  |  |
| C_DOC_ASOC_LETRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 15 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 15 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T080_OC_CABE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| U_PREFIJO_LOTE | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_LOTE | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_OC_MADRE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OC_PARA_TRANSFERENCIA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OC_PAGOANT | char | 1 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_LIMITE_ENTREGA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO_ALT | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_EMISION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ENTREGA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| I_NETO_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNO_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO_OPERADOR | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_OPERADOR | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_CUMPLIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA4 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA5 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA6 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_COND_PAGO | char | 200 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 200 | 0 | 0 | False | False |  |  |  |  |
| F_COMP_ING_MERC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_COMP_ING_MERC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_COMP_ING_MERC | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_COMP_ING_MERC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| D_OBSERVACION_ING_MERC | char | 150 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_ENTREGA_MERCADERIA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIFICO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_MODIFICO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIFICO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_OC_ELECTRONICA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SITUAC_OC_ELECTRONICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC_OC_ELECTRONICA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ENVIADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ESP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_EDI | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FECHA_LIMITE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| ClusteredColumnStoreIndex-20250613-102906 | CLUSTERED COLUMNSTORE | False | False | False |  |
| IDX_T080_OC_CABE_FECHAS | NONCLUSTERED | False | False | False |  |
| IX_T080_OC_CABE_C_OC | NONCLUSTERED | False | False | False |  |
| IX_OC_CABE_CLAVE | NONCLUSTERED | False | False | False |  |

## repl.T080_OC_CABE_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| U_PREFIJO_LOTE | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_LOTE | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| M_OC_MADRE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OC_PARA_TRANSFERENCIA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OC_PAGOANT | char | 1 | 0 | 0 | False | False |  |  |  |  |
| U_DIAS_LIMITE_ENTREGA | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO_ALT | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SITUAC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_EMISION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_ENTREGA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| I_NETO_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IVA_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNO_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_OC | money | 8 | 19 | 4 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO_OPERADOR | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_OPERADOR | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_CUMPLIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA1 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA2 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA3 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA4 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA5 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_PLAZO_ENTREGA6 | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| D_COND_PAGO | char | 200 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 200 | 0 | 0 | False | False |  |  |  |  |
| F_COMP_ING_MERC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_COMP_ING_MERC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_COMP_ING_MERC | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_COMP_ING_MERC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| D_OBSERVACION_ING_MERC | char | 150 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_ENTREGA_MERCADERIA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIFICO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_MODIFICO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIFICO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_OC_ELECTRONICA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_SITUAC_OC_ELECTRONICA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_SITUAC_OC_ELECTRONICA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_ENVIADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_ESP | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_TIPO_PROVEEDOR_EDI | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FECHA_LIMITE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |
| Merge_Test | NONCLUSTERED | False | False | False |  |
| IX_T080_OC_CABE_STG_C_OC | NONCLUSTERED | False | False | False |  |
| IX_OC_CABE_STG_CLAVE | NONCLUSTERED | False | False | False |  |

## repl.T080_OC_PENDIENTES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SUCU_COMPRA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_DESTINO_ALT | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Pendientes | numeric | 17 | 38 | 6 | True | False |  |  |  |  |
| Q_PESO_UNIT_ART | numeric | 9 | 13 | 3 | True | False |  |  |  |  |
| M_VENDE_POR_PESO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_FACTOR_COMPRA | numeric | 5 | 6 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T081_OC_DETA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_BULTOS_SUGERIDOS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PROV_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_FACTOR_PROV_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PROV_BONIF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_EMPR_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_FACTOR_EMPR_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_PESO_UNIT_ART | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_PESO_TOTAL_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_PESO_TOTAL_BONIF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| C_IVA_EN_CALCULO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_COEF_IVA | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| K_IMP_INTERNO | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| I_COSTO_BASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COMPRA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_PARTE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_LISTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ENVASES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_IMP_INTERNO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_ITEM | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_UNID_CUMPLIDAS | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_CUMPLIDO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| M_CUMPLIDA_PARCIAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_CUMPLIO_PARCIAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_CUMPLIDA_PARCIAL | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_PISO_PALETIZADO_OC | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| U_ALTURA_PALETIZADO_OC | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_DTO1_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO1_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO2_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO2_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO3_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO3_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO4_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO4_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO5_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO5_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO6_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO6_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO7_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO7_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO8_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO8_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO9_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO9_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO10_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO10_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |
| IX_T081_OC_DETA_MERGE | NONCLUSTERED | False | False | False |  |

## repl.T081_OC_DETA_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_BULTOS_SUGERIDOS | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PROV_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_FACTOR_PROV_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_PROV_BONIF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_BULTOS_EMPR_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_FACTOR_EMPR_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_PESO_UNIT_ART | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_PESO_TOTAL_PED | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| Q_PESO_TOTAL_BONIF | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| C_IVA_EN_CALCULO | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_COEF_IVA | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| K_IMP_INTERNO | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| I_COSTO_BASE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COMPRA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_PARTE | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_LISTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_ENVASES | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_IMP_INTERNO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_TOTAL_ITEM | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_UNID_CUMPLIDAS | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| Q_PESO_CUMPLIDO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| M_CUMPLIDA_PARCIAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_CUMPLIO_PARCIAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_CUMPLIDA_PARCIAL | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_PISO_PALETIZADO_OC | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| U_ALTURA_PALETIZADO_OC | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_DTO1_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO1_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO2_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO2_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO3_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO3_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO4_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO4_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO5_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO5_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO6_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO6_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO7_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO7_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO8_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO8_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO9_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO9_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| C_DTO10_COMP | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| K_DTO10_COMP | numeric | 5 | 6 | 5 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T085_ARTICULOS_EAN_EDI

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_EAN | numeric | 9 | 15 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T085_ARTICULOS_EAN_EDI_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_EAN | numeric | 9 | 15 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T090_COMPETENCIA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| N_COMPETIDOR | char | 30 | 0 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T090_COMPETENCIA_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| N_COMPETIDOR | char | 30 | 0 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T090_COMPETENCIA_ZONAS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Id | int | 4 | 10 | 0 | False | False |  |  |  |  |
| Nombre | varchar | 30 | 0 | 0 | False | False |  |  |  |  |
| Descripcion | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FechaAlta | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T090_COMPETENCIA_ZONAS_COMPETIDORES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| IdZona | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| IdCompetidor | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FechaAlta | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T090_COMPETENCIA_ZONAS_SUCURSALES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| IdZona | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| IdSucursal | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FechaAlta | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T091_COMPETENCIA_PRECIOS_CABE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 200 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T091_COMPETENCIA_PRECIOS_CABE_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 200 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T091_COMPETENCIA_PRECIOS_DETA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COMPETIDOR | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_COMMODITY | char | 1 | 0 | 0 | False | False |  |  |  |  |
| D_COMMODITY | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_PROMO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_INFO_IMAGEN | varchar | 1000 | 0 | 0 | False | False |  |  |  |  |
| M_INDEXADO | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| I_PRECIO_REVISADO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| IDZONA | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T091_COMPETENCIA_PRECIOS_DETA_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| U_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SEMANA | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_COMPETIDOR | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_PROVEEDOR_PRIMARIO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COSTO_ESTADISTICO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COMPETIDOR | money | 8 | 19 | 4 | False | False |  |  |  |  |
| M_COMMODITY | char | 1 | 0 | 0 | False | False |  |  |  |  |
| D_COMMODITY | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_PROMO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_INFO_IMAGEN | varchar | 1000 | 0 | 0 | False | False |  |  |  |  |
| M_INDEXADO | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| I_PRECIO_REVISADO | money | 8 | 19 | 4 | True | False |  |  |  |  |
| IDZONA | numeric | 5 | 3 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T100_EMPRESA_SUC

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| N_SUCURSAL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_SUCURSAL_ABREV | char | 2 | 0 | 0 | False | False |  |  |  |  |
| N_SUCURSAL_ABREV2 | char | 10 | 0 | 0 | False | False |  |  |  |  |
| N_CALLE | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_LOCALIDAD | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_POSTAL_INM | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_TELEFONO | char | 20 | 0 | 0 | False | False |  |  |  |  |
| D_POSIC_IVA | char | 30 | 0 | 0 | False | False |  |  |  |  |
| U_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| U_IB | char | 12 | 0 | 0 | False | False |  |  |  |  |
| D_CAJA_JUBILAC | char | 30 | 0 | 0 | False | False |  |  |  |  |
| U_JUBILAC | numeric | 9 | 10 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_SUCU | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_INICIO_ACTIV_SUCU | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_ORDEN_PANTALLA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_SUCU_VIRTUAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADA_COMPRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_BACK_COLOR_SUCU | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_FORE_COLOR_SUCU | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_ZONA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| N_DIRECTORIO_MAILHORA | varchar | 70 | 0 | 0 | False | False |  |  |  |  |
| N_DIRECTORIO_DATOS_SUC | varchar | 70 | 0 | 0 | False | False |  |  |  |  |
| C_EAN | char | 15 | 0 | 0 | False | False |  |  |  |  |
| U_EMAIL_SUCU | char | 70 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| N_DIRECTORIO_CIERRE_LOCAL_SUCURSAL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_DIRECTORIO_RECEPCION_SUCURSAL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| M_SUCURSAL_IMPLEMENTADA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| N_SUCURSAL_ABREV3 | char | 6 | 0 | 0 | False | False |  |  |  |  |
| N_GERENTE_SUC | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_GERENTE_TELEFONO_FLOTA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| M_SUCURSAL_FISCALIZADA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_FISCALIZACION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_ZONA_IMPRESION | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_INCLUYE_FALTANTE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_ZONA_REGIONAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ORDEN_PANTALLA_CAJA_UNIFICADA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TAMANO_PAPEL | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| I_VALORIZACION_MERCA_SEGURO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_MARGEN_SEGURO | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| M_ABAST_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_HORA_APERTURA | char | 5 | 0 | 0 | True | False |  |  |  |  |
| C_HORA_CIERRE | char | 5 | 0 | 0 | True | False |  |  |  |  |
| C_LATITUD | nvarchar | 30 | 0 | 0 | True | False |  |  |  |  |
| C_LONGITUD | nvarchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_EXCLUIDA | char | 1 | 0 | 0 | True | False |  |  |  |  |
| M_TIPO_SUCURSAL | char | 1 | 0 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T100_EMPRESA_SUC_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| N_SUCURSAL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_SUCURSAL_ABREV | char | 2 | 0 | 0 | False | False |  |  |  |  |
| N_SUCURSAL_ABREV2 | char | 10 | 0 | 0 | False | False |  |  |  |  |
| N_CALLE | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_LOCALIDAD | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_POSTAL_INM | char | 50 | 0 | 0 | False | False |  |  |  |  |
| C_TELEFONO | char | 20 | 0 | 0 | False | False |  |  |  |  |
| D_POSIC_IVA | char | 30 | 0 | 0 | False | False |  |  |  |  |
| U_CUIT | char | 13 | 0 | 0 | False | False |  |  |  |  |
| U_IB | char | 12 | 0 | 0 | False | False |  |  |  |  |
| D_CAJA_JUBILAC | char | 30 | 0 | 0 | False | False |  |  |  |  |
| U_JUBILAC | numeric | 9 | 10 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_PROVINCIA_SUCU | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_INICIO_ACTIV_SUCU | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_ORDEN_PANTALLA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_SUCU_VIRTUAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_HABILITADA_COMPRA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_BACK_COLOR_SUCU | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_FORE_COLOR_SUCU | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_ZONA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| N_DIRECTORIO_MAILHORA | varchar | 70 | 0 | 0 | False | False |  |  |  |  |
| N_DIRECTORIO_DATOS_SUC | varchar | 70 | 0 | 0 | False | False |  |  |  |  |
| C_EAN | char | 15 | 0 | 0 | False | False |  |  |  |  |
| U_EMAIL_SUCU | char | 70 | 0 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| N_DIRECTORIO_CIERRE_LOCAL_SUCURSAL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_DIRECTORIO_RECEPCION_SUCURSAL | char | 50 | 0 | 0 | False | False |  |  |  |  |
| M_SUCURSAL_IMPLEMENTADA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| N_SUCURSAL_ABREV3 | char | 6 | 0 | 0 | False | False |  |  |  |  |
| N_GERENTE_SUC | char | 50 | 0 | 0 | False | False |  |  |  |  |
| N_GERENTE_TELEFONO_FLOTA | char | 50 | 0 | 0 | False | False |  |  |  |  |
| M_SUCURSAL_FISCALIZADA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_FISCALIZACION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_ZONA_IMPRESION | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| M_INCLUYE_FALTANTE | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_ZONA_REGIONAL | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ORDEN_PANTALLA_CAJA_UNIFICADA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_TAMANO_PAPEL | numeric | 5 | 1 | 0 | False | False |  |  |  |  |
| I_VALORIZACION_MERCA_SEGURO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| K_MARGEN_SEGURO | numeric | 5 | 5 | 4 | False | False |  |  |  |  |
| M_ABAST_LOGISTICO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_HORA_APERTURA | char | 5 | 0 | 0 | True | False |  |  |  |  |
| C_HORA_CIERRE | char | 5 | 0 | 0 | True | False |  |  |  |  |
| C_LATITUD | nvarchar | 30 | 0 | 0 | True | False |  |  |  |  |
| C_LONGITUD | nvarchar | 30 | 0 | 0 | True | False |  |  |  |  |
| M_EXCLUIDA | char | 1 | 0 | 0 | True | False |  |  |  |  |
| M_TIPO_SUCURSAL | char | 1 | 0 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T114_RUBROS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| D_RUBRO | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_RUBRO_PADRE | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO_NIVEL | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EXCLUIDA_EN_VALORIZ | char | 1 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T114_RUBROS_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_RUBRO | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| D_RUBRO | char | 30 | 0 | 0 | False | False |  |  |  |  |
| C_RUBRO_PADRE | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_RUBRO_NIVEL | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| F_ALTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_ALTA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_BAJA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_BAJA | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_EXCLUIDA_EN_VALORIZ | char | 1 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T117_COMPRADORES

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| N_COMPRADOR | char | 20 | 0 | 0 | False | False |  |  |  |  |
| N_COMPRADOR_ABREV | char | 15 | 0 | 0 | False | False |  |  |  |  |
| C_SUCU_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | varchar | 15 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T117_COMPRADORES_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_COMPRADOR | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| N_COMPRADOR | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| N_COMPRADOR_ABREV | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| C_SUCU_COMPRADOR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| M_BAJA | char | 1 | 0 | 0 | True | False |  |  |  |  |
| C_USUARIO | varchar | 50 | 0 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__T117_COM__6E27F629CD5678E0 | CLUSTERED | True | True | False |  |

## repl.T230_FACTURADOR_NEGOCIOS_ESPECIALES_POR_CANTIDAD

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | decimal | 5 | 6 | 0 | False | False |  |  |  |  |
| F_DESDE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_HASTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_DISPONIBLES | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_VENDIDOS | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_MODIF | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_SALDO | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_VIGENCIA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VENCIDA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_COMPRA_MINIMA | decimal | 9 | 13 | 3 | False | False |  |  | ((0)) |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T230_FACTURADOR_NEGOCIOS_ESPECIALES_POR_CANTIDAD_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | decimal | 5 | 6 | 0 | False | False |  |  |  |  |
| F_DESDE | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| F_HASTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| I_PRECIO_VTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_DISPONIBLES | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_VENDIDOS | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_MODIF | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_SALDO | decimal | 9 | 10 | 3 | False | False |  |  |  |  |
| F_MODIF | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_VIGENCIA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_VENCIDA | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_UNIDADES_KILOS_COMPRA_MINIMA | decimal | 9 | 13 | 3 | False | False |  |  | ((0)) |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T702_EST_VTAS_POR_ARTICULO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F_VENTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| I_PRECIO_VENTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COSTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_VENDIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_UNIDADES_VENDIDAS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNOS | money | 8 | 19 | 4 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| idx_f_venta | CLUSTERED | False | False | False |  |
| UX_T702_REPL | NONCLUSTERED | True | False | False |  |

## repl.T702_EST_VTAS_POR_ARTICULO_DBARRIO

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F_VENTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| I_PRECIO_VENTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COSTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_VENDIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_UNIDADES_VENDIDAS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNOS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |
| UX_T702_REPL_BARRIO | NONCLUSTERED | True | False | False |  |

## repl.T702_EST_VTAS_POR_ARTICULO_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| F_VENTA | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_FAMILIA | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 5 | 0 | False | False |  |  |  |  |
| I_PRECIO_VENTA | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COSTO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_VENDIDO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| Q_UNIDADES_VENDIDAS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_COSTO_PP | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PARTE_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_COMPRA_ULTIMO_INGRESO | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_IMP_INTERNOS | money | 8 | 19 | 4 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T710_ESTADIS_OFERTA_FOLDER

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_MES | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA1 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA1 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA2 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA2 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA3 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA3 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA4 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA4 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA5 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA5 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA6 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA6 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA7 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA7 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA8 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA8 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA9 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA9 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA10 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA10 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA11 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA11 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA12 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA12 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA13 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA13 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA14 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA14 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA15 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA15 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA16 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA16 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA17 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA17 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA18 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA18 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA19 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA19 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA20 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA20 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA21 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA21 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA22 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA22 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA23 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA23 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA24 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA24 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA25 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA25 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA26 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA26 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA27 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA27 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA28 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA28 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA29 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA29 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA30 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA30 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA31 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA31 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| CL_IDX_ESTADIS_OFERTA_FOLDER | CLUSTERED | False | False | False |  |

## repl.T710_ESTADIS_OFERTA_FOLDER_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_MES | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA1 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA1 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA2 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA2 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA3 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA3 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA4 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA4 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA5 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA5 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA6 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA6 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA7 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA7 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA8 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA8 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA9 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA9 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA10 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA10 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA11 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA11 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA12 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA12 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA13 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA13 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA14 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA14 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA15 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA15 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA16 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA16 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA17 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA17 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA18 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA18 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA19 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA19 | varchar | 1 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA20 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA20 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA21 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA21 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA22 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA22 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA23 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA23 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA24 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA24 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA25 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA25 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA26 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA26 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA27 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA27 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA28 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA28 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA29 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA29 | varchar | 2 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA30 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA30 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| M_OFERTA_DIA31 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| M_FOLDER_DIA31 | varchar | 3 | 0 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T710_ESTADIS_PRECIOS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_MES | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_25 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_26 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_27 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_28 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_29 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_30 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_31 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK_T710_ANIO_MES_ARTICULO_SUCURSAL | CLUSTERED | True | True | False |  |

## repl.T710_ESTADIS_PRECIOS_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ANIO | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| C_MES | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| I_PRECIO_VTA_1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_3 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_4 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_5 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_6 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_7 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_8 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_9 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_10 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_11 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_12 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_13 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_14 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_15 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_16 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_17 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_18 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_19 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_20 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_21 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_22 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_23 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_24 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_25 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_26 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_27 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_28 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_29 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_30 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| I_PRECIO_VTA_31 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T710_ESTADIS_REPOSICION

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_VENTA_30_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_15_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_DOMINGO | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_ESPECIAL_30_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_VENTA_ESPECIAL_15_DIAS | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_DIAS_CON_STOCK | numeric | 5 | 2 | 0 | False | False |  |  |  |  |
| Q_REPONER | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_REPONER_INCLUIDO_SOBRE_STOCK | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| M_SEMAFORO_INDIVIDUAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_SEMAFORO_GLOBAL | char | 1 | 0 | 0 | False | False |  |  |  |  |
| Q_VENTA_DIARIA_NORMAL | numeric | 9 | 18 | 3 | False | False |  |  |  |  |
| Q_DIAS_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_DIAS_ENTREGA_PROVEEDOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK_T710_ESTADIS_REPOSICION | CLUSTERED | True | True | False |  |

## repl.T710_ESTADIS_REPOSICION_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_EMPR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_30_DIAS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_VENTA_15_DIAS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_VENTA_DOMINGO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_VENTA_ESPECIAL_30_DIAS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_VENTA_ESPECIAL_15_DIAS | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIAS_CON_STOCK | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_REPONER | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_REPONER_INCLUIDO_SOBRE_STOCK | int | 4 | 10 | 0 | True | False |  |  |  |  |
| M_SEMAFORO_INDIVIDUAL | char | 1 | 0 | 0 | True | False |  |  |  |  |
| M_SEMAFORO_GLOBAL | char | 1 | 0 | 0 | True | False |  |  |  |  |
| Q_VENTA_DIARIA_NORMAL | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| Q_DIAS_STOCK | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| Q_DIAS_SOBRE_STOCK | decimal | 9 | 18 | 2 | True | False |  |  |  |  |
| Q_DIAS_ENTREGA_PROVEEDOR | int | 4 | 10 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| CDC_LSN | varchar | 100 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T710_ESTADIS_STOCK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ANIO | decimal | 5 | 4 | 0 | False | False |  |  |  |  |
| C_MES | decimal | 5 | 2 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | decimal | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | decimal | 5 | 6 | 0 | False | False |  |  |  |  |
| Q_DIA1 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA2 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA3 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA4 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA5 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA6 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA7 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA8 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA9 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA10 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA11 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA12 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA13 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA14 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA15 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA16 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA17 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA18 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA19 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA20 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA21 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA22 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA23 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA24 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA25 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA26 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA27 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA28 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA29 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA30 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Q_DIA31 | decimal | 9 | 11 | 3 | False | False |  |  | ((0)) |  |
| Fecha_Proceso | datetime2 | 8 | 27 | 7 | True | False |  |  |  |  |
| procesado_ok | bit | 1 | 1 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 50 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK_T710_ESTADIS_STOCK | CLUSTERED | True | True | False |  |

## repl.T710_ESTADIS_STOCK_STG

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_ANIO | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_MES | int | 4 | 10 | 0 | True | False |  |  |  |  |
| C_SUCU_EMPR | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| C_ARTICULO | varchar | 30 | 0 | 0 | True | False |  |  |  |  |
| Q_DIA1 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA2 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA3 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA4 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA5 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA6 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA7 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA8 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA9 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA10 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA11 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA12 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA13 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA14 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA15 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA16 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA17 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA18 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA19 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA20 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA21 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA22 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA23 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA24 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA25 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA26 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA27 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA28 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA29 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA30 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| Q_DIA31 | int | 4 | 10 | 0 | True | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 50 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T804_HIST_MARCA_LISTO_PARA_VENTA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_SUCU_ORIG_ALTA | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA_ANT | char | 1 | 0 | 0 | False | False |  |  |  |  |
| M_LISTO_PARA_VENTA_ACT | char | 1 | 0 | 0 | False | False |  |  |  |  |
| D_FECHAHORA | char | 30 | 0 | 0 | False | False |  |  |  |  |
| D_OBSERVACION | char | 100 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 17 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.T874_PRECARGA_CONNEXA_HIST

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C_PROVEEDOR | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_ARTICULO | numeric | 5 | 6 | 0 | False | False |  |  |  |  |
| C_SUCU_EMPR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| Q_BULTOS_KILOS_DIARCO | numeric | 9 | 13 | 3 | False | False |  |  |  |  |
| F_ALTA_SIST | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_GENERO_OC | char | 10 | 0 | 0 | False | False |  |  |  |  |
| C_TERMINAL_GENERO_OC | char | 10 | 0 | 0 | False | False |  |  |  |  |
| F_GENERO_OC | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| C_USUARIO_BLOQUEO | char | 10 | 0 | 0 | False | False |  |  |  |  |
| M_PROCESADO | char | 1 | 0 | 0 | False | False |  |  |  |  |
| F_PROCESADO | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| U_PREFIJO_OC | numeric | 5 | 4 | 0 | False | False |  |  |  |  |
| U_SUFIJO_OC | numeric | 5 | 8 | 0 | False | False |  |  |  |  |
| C_COMPRA_CONNEXA | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_USUARIO_MODIF | char | 20 | 0 | 0 | False | False |  |  |  |  |
| C_COMPRADOR | numeric | 5 | 3 | 0 | False | False |  |  |  |  |
| FUENTE_ORIGEN | varchar | 17 | 0 | 0 | False | False |  |  |  |  |
| FECHA_EXTRACCION | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| CDC_LSN | varbinary | 10 | 0 | 0 | True | False |  |  |  |  |
| ESTADO_SINCRONIZACION | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.TRANSF_CONNEXA_IN

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | int | 4 | 10 | 0 | False | True | 1 | 1 |  |  |
| c_articulo | decimal | 5 | 6 | 0 | True | False |  |  |  |  |
| c_sucu_dest | decimal | 5 | 3 | 0 | True | False |  |  |  |  |
| c_sucu_orig | decimal | 5 | 3 | 0 | True | False |  |  |  |  |
| q_requerida | decimal | 9 | 13 | 3 | True | False |  |  |  |  |
| q_bultos | decimal | 9 | 13 | 3 | True | False |  |  |  |  |
| q_factor | decimal | 5 | 6 | 0 | True | False |  |  |  |  |
| f_alta | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| m_alta_prioridad | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| vchUsuario | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| vchTerminal | varchar | 10 | 0 | 0 | True | False |  |  |  |  |
| forzarTransf | varchar | 1 | 0 | 0 | True | False |  |  |  |  |
| estado | varchar | 20 | 0 | 0 | True | False |  |  | ('PENDIENTE') |  |
| mensaje_error | varchar | 255 | 0 | 0 | True | False |  |  |  |  |
| u_id_sincro | int | 4 | 10 | 0 | True | False |  |  |  |  |
| f_procesado | datetime | 8 | 23 | 3 | True | False |  |  |  |  |
| connexa_header_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| connexa_detail_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| estado_vk | varchar | 20 | 0 | 0 | True | False |  |  |  |  |
| mensaje_error_vk | varchar | 255 | 0 | 0 | True | False |  |  |  |  |
| f_procesado_vk | datetime | 8 | 23 | 3 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__TRANSF_C__3213E83F7E9C9A1A | CLUSTERED | True | True | False |  |
| IX_TRANSF_CONNEXA_IN_header_estado | NONCLUSTERED | False | False | False |  |
| IX_TRANSF_CONNEXA_IN_header_estado_procesado | NONCLUSTERED | False | False | False |  |
| UX_TRANSF_CONNEXA_IN_connexa_detail_uuid | NONCLUSTERED | True | False | False | ([connexa_detail_uuid] IS NOT NULL) |
| IX_TRANSF_CONNEXA_IN_DETAIL_UUID | NONCLUSTERED | False | False | False |  |

## repl.TRANSF_CONNEXA_RETORNO_ACK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connexa_header_uuid | uniqueidentifier | 16 | 0 | 0 | False | False |  |  |  |  |
| informado_at | datetime2 | 6 | 19 | 0 | False | False |  |  | (sysdatetime()) |  |
| resultado | varchar | 10 | 0 | 0 | False | False |  |  |  |  |
| mensaje_error | varchar | 255 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
| PK__TRANSF_C__17D3D3A1035E8426 | CLUSTERED | True | True | False |  |

## repl.TRANSF_CONNEXA_VK_BASE_FULL

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connexa_header_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| connexa_detail_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| u_id_sincro | int | 4 | 10 | 0 | True | False |  |  |  |  |
| INIId | numeric | 9 | 12 | 0 | False | False |  |  |  |  |
| INIIdSincro | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIEst | char | 3 | 0 | 0 | False | False |  |  |  |  |
| INIFecEnt | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIFecReg | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIFecEst | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIDepId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIEntId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIArtId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIArtC | char | 20 | 0 | 0 | False | False |  |  |  |  |
| INIUxB | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INICnt1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INIMRecibido | char | 1 | 0 | 0 | False | False |  |  |  |  |
| INIMotPed | nchar | 6 | 0 | 0 | False | False |  |  |  |  |
| INICnt2Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INILinPrio | char | 1 | 0 | 0 | False | False |  |  |  |  |
| EmpId | smallint | 2 | 5 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.TRANSFERENCIAS_IMPORTAR

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| c_articulo | int | 4 | 10 | 0 | False | False |  |  |  |  |
| c_sucu_dest | tinyint | 1 | 3 | 0 | False | False |  |  |  |  |
| c_sucu_orig | tinyint | 1 | 3 | 0 | False | False |  |  |  |  |
| q_requerida | smallint | 2 | 5 | 0 | False | False |  |  |  |  |
| q_bultos | smallint | 2 | 5 | 0 | False | False |  |  |  |  |
| q_factor | tinyint | 1 | 3 | 0 | True | False |  |  |  |  |
| f_alta | datetime2 | 8 | 27 | 7 | True | False |  |  |  |  |
| m_alta_prioridad | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| vchUsuario | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| vchTerminal | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| forzarTransf | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |
| estado | nvarchar | 100 | 0 | 0 | True | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
|  | HEAP | False | False | False |  |

## repl.V_CONNEXA_RETORNO_CABECERAS_VK

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connexa_header_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| total_lineas | int | 4 | 10 | 0 | True | False |  |  |  |  |
| cant_aco | int | 4 | 10 | 0 | True | False |  |  |  |  |
| cant_pre | int | 4 | 10 | 0 | True | False |  |  |  |  |
| cant_rem | int | 4 | 10 | 0 | True | False |  |  |  |  |
| cant_etr | int | 4 | 10 | 0 | True | False |  |  |  |  |
| cant_otro | int | 4 | 10 | 0 | True | False |  |  |  |  |
| resultado | varchar | 9 | 0 | 0 | False | False |  |  |  |  |
| cerrable | int | 4 | 10 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## repl.V_CONNEXA_VK_BASE

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connexa_header_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| connexa_detail_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| u_id_sincro | int | 4 | 10 | 0 | True | False |  |  |  |  |
| INIId | numeric | 9 | 12 | 0 | False | False |  |  |  |  |
| INIIdSincro | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIEst | char | 3 | 0 | 0 | False | False |  |  |  |  |
| INIFecEnt | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIFecReg | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIFecEst | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIDepId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIEntId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIArtId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIArtC | char | 20 | 0 | 0 | False | False |  |  |  |  |
| INIUxB | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INICnt1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INIMRecibido | char | 1 | 0 | 0 | False | False |  |  |  |  |
| INIMotPed | nchar | 6 | 0 | 0 | False | False |  |  |  |  |
| INICnt2Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INILinPrio | char | 1 | 0 | 0 | False | False |  |  |  |  |
| EmpId | smallint | 2 | 5 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## repl.V_CONNEXA_VK_ULTIMO_ESTADO_LINEA

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| connexa_header_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| connexa_detail_uuid | uniqueidentifier | 16 | 0 | 0 | True | False |  |  |  |  |
| u_id_sincro | int | 4 | 10 | 0 | True | False |  |  |  |  |
| INIId | numeric | 9 | 12 | 0 | False | False |  |  |  |  |
| INIIdSincro | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIEst | char | 3 | 0 | 0 | False | False |  |  |  |  |
| INIFecEnt | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIFecReg | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIFecEst | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIDepId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIEntId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIArtId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIArtC | char | 20 | 0 | 0 | False | False |  |  |  |  |
| INIUxB | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INICnt1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INIMRecibido | char | 1 | 0 | 0 | False | False |  |  |  |  |
| INIMotPed | nchar | 6 | 0 | 0 | False | False |  |  |  |  |
| INICnt2Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INILinPrio | char | 1 | 0 | 0 | False | False |  |  |  |  |
| EmpId | smallint | 2 | 5 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |

## repl.V_VALKIMIA_ESTADO_TRANFERENCIAS

| name | tipo | max_length | precision | scale | is_nullable | is_identity | identity_seed | identity_increment | default_definition | computed_definition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INIId | numeric | 9 | 12 | 0 | False | False |  |  |  |  |
| INIFecEnt | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIDepId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIEntId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIObs | varchar | 100 | 0 | 0 | False | False |  |  |  |  |
| INIArtId | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIArtC | char | 20 | 0 | 0 | False | False |  |  |  |  |
| INIUxB | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INICnt1 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2 | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INIEst | char | 3 | 0 | 0 | False | False |  |  |  |  |
| INIFecReg | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIUsuReg | char | 20 | 0 | 0 | False | False |  |  |  |  |
| EmpId | smallint | 2 | 5 | 0 | False | False |  |  |  |  |
| INIFecEst | datetime | 8 | 23 | 3 | False | False |  |  |  |  |
| INIIdSincro | int | 4 | 10 | 0 | False | False |  |  |  |  |
| INIMRecibido | char | 1 | 0 | 0 | False | False |  |  |  |  |
| INIMotPed | nchar | 6 | 0 | 0 | False | False |  |  |  |  |
| INICnt2Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Rem | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt2Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INICnt1Pre | money | 8 | 19 | 4 | False | False |  |  |  |  |
| INILinPrio | char | 1 | 0 | 0 | False | False |  |  |  |  |

| name | type_desc | is_unique | is_primary_key | is_unique_constraint | filter_definition |
| --- | --- | --- | --- | --- | --- |
