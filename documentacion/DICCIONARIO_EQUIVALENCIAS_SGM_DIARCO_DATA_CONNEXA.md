# Diccionario de equivalencias SGM → diarco_data → Connexa

**Corte documental:** 22/10/2026. **Alcance:** maestros de artículos, proveedores y sucursales, sus relaciones y la base de productos vigentes. Este documento describe las reglas presentes en el código; no certifica el esquema ni las corridas actuales de las bases.

## Cómo leerlo

- **Directo:** el campo conserva su significado y sólo cambia de mayúsculas a minúsculas, o se normaliza el tipo/código. El aplicador CDC empareja columnas por nombre en minúsculas y agrega columnas técnicas del destino ([código](../scripts/cdc/cdc_replicar_tabla.py)).
- **Derivado:** una expresión combina campos, aplica una condición o usa un valor fijo. La expresión indicada es la regla de la implementación citada.
- **Shadow:** comparación de lectura de `CNX_DIARCO_SYNC_V2`, sin escritura. Las [actualizaciones legacy de maestros están pausadas](../../CNX_DIARCO_SYNC_V2/docs/PAUSA_ACTUALIZACIONES_MAESTROS_20260925.md). Una equivalencia shadow es un **contrato candidato**, no una publicación activa.
- Los identificadores `id`, las claves foráneas UUID, las imágenes y los campos de auditoría propios de Connexa no son copias de códigos SGM. Las relaciones se resuelven por códigos externos.
- `fecha_extraccion` y `fuente_origen` son metadatos de integración, no campos de negocio SGM.

## Equivalencias de tablas

Las primeras 12 relaciones tienen configuración explícita en los [seeds CDC](../cdc/postgres/), desde `DiarcoP.dbo` hacia `diarco_data.src`. La tabla `repl.*` de `data-sync` puede coexistir como réplica intermedia legacy; no es una tabla de Connexa.

| SGM (`DiarcoP.dbo`) | `diarco_data` | Connexa: interfaz candidata o consumidor | Evidencia / límite |
|---|---|---|---|
| `T050_ARTICULOS` | `src.t050_articulos` | `inventory.inv_product` o `public.fnd_product` | [CDC](../cdc/postgres/003_seed_pilot_t050_articulos.sql), [shadow](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/sources/diarco_data.py) |
| `T020_PROVEEDOR` | `src.t020_proveedor` | `inventory.inv_supplier` o `public.fnd_supplier` | [CDC](../cdc/postgres/011_seed_pilot_t020_proveedor.sql); shadow limita a `c_tipo_proveedor_diarco IN (1,4)` |
| `T052_ARTICULOS_PROVEEDOR` | `src.t052_articulos_proveedor` | `inventory.inv_product_supplier` | [CDC](../cdc/postgres/021_seed_pilot_t052_articulos_proveedor.sql), [shadow](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/product_suppliers.py) |
| `T100_EMPRESA_SUC` | `src.t100_empresa_suc` | `foundation.fnd_site` | [CDC](../cdc/postgres/041_seed_pilot_t100_empresa_suc.sql); la interfaz nueva sólo acepta `foundation` |
| `T114_RUBROS` | `src.t114_rubros` | `inventory.inv_category`, pendiente | [CDC](../cdc/postgres/051_seed_pilot_t114_rubros.sql); fuente observada desactualizada según [arquitectura](../../CNX_DIARCO_SYNC_V2/docs/ARQUITECTURA_INTEGRAL_SGM_CONNEXA.md) |
| `T117_COMPRADORES` | `src.t117_compradores` | `procurement.*`, por definir | [CDC](../cdc/postgres/061_seed_pilot_t117_compradores.sql), [arquitectura](../../CNX_DIARCO_SYNC_V2/docs/ARQUITECTURA_INTEGRAL_SGM_CONNEXA.md) |
| `T051_ARTICULOS_SUCURSAL` | `src.t051_articulos_sucursal` | `inventory.inv_product_site` | [CDC](../cdc/postgres/071_seed_pilot_t051_articulos_sucursal.sql), [shadow](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/pipeline/product_sites_shadow.py) |
| `T020_PROVEEDOR_DIAS_ENTREGA_CABE` | `src.t020_proveedor_dias_entrega_cabe` | Extensiones de proveedor, por definir | [CDC](../cdc/postgres/081_seed_pilot_t020_proveedor_dias_entrega_cabe.sql) |
| `T020_PROVEEDOR_DIAS_ENTREGA_DETA` | `src.t020_proveedor_dias_entrega_deta` | Extensiones de proveedor, por definir | [CDC](../cdc/postgres/091_seed_pilot_t020_proveedor_dias_entrega_deta.sql) |
| `T085_ARTICULOS_EAN_EDI` | `src.t085_articulos_ean_edi` | `inventory.inv_barcode`, complemento pendiente | [CDC](../cdc/postgres/101_seed_pilot_t085_articulos_ean_edi.sql); shadow lo controla por frescura, sin usarlo como fuente principal |
| `T055_ARTICULOS_PARAM_STOCK` | `src.t055_articulos_param_stock` | Planificación/producto-sucursal, por definir | [CDC](../cdc/postgres/111_seed_pilot_t055_articulos_param_stock.sql) |
| `T055_ARTICULOS_CONDCOMPRA_COSTOS` | `src.t055_articulos_condcompra_costos` | Planificación/condiciones comerciales, por definir | [CDC](../cdc/postgres/121_seed_pilot_t055_articulos_condcompra_costos.sql) |

En CDC, las columnas de negocio con igual nombre (sin distinguir mayúsculas) se copian a `src`; esto **no** implica que todas tengan destino Connexa. La lista exhaustiva de columnas y sus tipos debe extraerse de `sys.columns` e `information_schema.columns` de cada ambiente, pues el repositorio no contiene un DDL completo y vigente de las 12 tablas.

## Producto

Fuentes: [lectura de `src`](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/sources/diarco_data.py), [normalización shadow](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/products.py) y [perfil de destino](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/targets/postgres.py). El baseline es `T050`; pertenecer a `src.m_3_articulos` delimita provisoriamente el universo comercial, pero `m_3_articulos` es un derivado y no equivale a `T050` completo.

| SGM `T050_ARTICULOS` | `diarco_data` | Connexa producto | Regla / estado |
|---|---|---|---|
| `C_ARTICULO` | `src.t050_articulos.c_articulo` | `ext_code`; `sku` | Código entero canónico convertido a texto; shadow |
| `N_ARTICULO` | `src.t050_articulos.n_articulo` | `description` | Espacios normalizados; shadow |
| `N_ARTICULO_FACT` | `src.t050_articulos.n_articulo_fact` | `short_description` | Espacios normalizados; shadow |
| `M_VENDE_POR_PESO` | `src.t050_articulos.m_vende_por_peso` | `weight_variable` | `S → true`; shadow |
| `M_SIN_VENTA` | `src.t050_articulos.m_sin_venta` | `sale_allowed` | `S → false`; shadow |
| `OK_COMPRA` | `src.t050_articulos.ok_compra` | `purchase_enabled` | `S → true`; shadow |
| `M_BAJA` | `src.t050_articulos.m_baja` | estado activo (`status_id`) | `S → inactive`; requiere resolver catálogo de estados; shadow |
| `F_MODIF` | `src.t050_articulos.f_modif` | — | Marca temporal fuente para control; no se copia al producto Connexa |
| — | `src.t050_articulos.fecha_extraccion` | — | Marca temporal ETL para frescura; no es fecha de modificación SGM |

Los destinos del perfil modular y legacy son distintos: `inventory.inv_product` y `public.fnd_product`, respectivamente. La disponibilidad de campos opcionales se descubre en el esquema de cada destino. `category_id`, imagen y atributos logísticos requieren contratos separados; no se derivan del listado anterior.

## Proveedor

Fuente: [lectura y filtro](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/sources/diarco_data.py) y [normalización](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/suppliers.py). El shadow sólo considera mercadería (`C_TIPO_PROVEEDOR_DIARCO IN (1,4)`). Destino modular `inventory.inv_supplier`; perfil legacy `public.fnd_supplier`.

| SGM `T020_PROVEEDOR` | `diarco_data` | Connexa proveedor | Regla / estado |
|---|---|---|---|
| `C_PROVEEDOR` | `src.t020_proveedor.c_proveedor` | `ext_code` | Código numérico canónico como texto; shadow |
| `N_PROVEEDOR` | `src.t020_proveedor.n_proveedor` | `name` | Espacios normalizados; shadow |
| `C_CUIT` | `src.t020_proveedor.c_cuit` | `tax_identification` | Texto normalizado; shadow |
| `N_DOMICILIO_INM` | `src.t020_proveedor.n_domicilio_inm` | `address` | Directo; shadow |
| `N_LOCALIDAD_INM` | `src.t020_proveedor.n_localidad_inm` | `city` | Directo; shadow |
| `N_PAIS`, `N_ESTADO` | `src.t020_proveedor.n_pais`, `.n_estado` | `country`, `state` | Directo; shadow |
| `C_POSTAL_INM` | `src.t020_proveedor.c_postal_inm` | `zip_code` | Directo; shadow |
| `N_VENDEDOR` | `src.t020_proveedor.n_vendedor` | `default_contact` | Directo; shadow |
| `N_EMAIL`, `N_WEB` | `src.t020_proveedor.n_email`, `.n_web` | `email`, `website` | Directo; shadow |
| `C_NIVEL_PROVEEDOR` | `src.t020_proveedor.c_nivel_proveedor` | `supplier_level_code` | Código numérico opcional; shadow |
| `C_ORIGEN_PROVEEDOR` | `src.t020_proveedor.c_origen_proveedor` | `supplier_origin_code` | Código numérico opcional; shadow |
| `C_TIPO_PROVEEDOR` | `src.t020_proveedor.c_tipo_proveedor` | `ext_supplier_type_code` | Código numérico opcional; shadow |
| `C_TIPO_PROVEEDOR_DIARCO` | `src.t020_proveedor.c_tipo_proveedor_diarco` | — | Filtro 1/4; no equivale a `ext_supplier_type_code` |
| `M_ACTIVO`, `M_BAJA` | `src.t020_proveedor.m_activo`, `.m_baja` | estado activo | Activo si `M_ACTIVO='S' AND M_BAJA<>'S'`; shadow |
| `F_MODIF` | `src.t020_proveedor.f_modif` | — | Control temporal fuente |

## Sucursal

Fuentes: [lectura](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/sources/diarco_data.py), [normalización](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/sites.py) y [perfil](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/targets/postgres.py). Esta interfaz compara únicamente `foundation.fnd_site`; `inventory.inv_site` no es su tabla destino.

| SGM `T100_EMPRESA_SUC` | `diarco_data` | `foundation.fnd_site` | Regla / estado |
|---|---|---|---|
| `C_SUCU_EMPR` | `src.t100_empresa_suc.c_sucu_empr` | `code` | Código entero canónico como texto; shadow |
| `N_SUCURSAL` | `src.t100_empresa_suc.n_sucursal` | `name` | Espacios normalizados; shadow |
| `N_CALLE`, `N_LOCALIDAD`, `C_POSTAL_INM` | columnas homónimas en `src.t100_empresa_suc` | `address` | Concatenación de partes presentes con `, `; shadow |
| `M_ABAST_LOGISTICO` | `src.t100_empresa_suc.m_abast_logistico` | `type` | `S → LOGISTICS`; otro valor → `POINT_OF_SALE`; shadow |
| `M_BAJA` | `src.t100_empresa_suc.m_baja` | estado activo en plan | Activa si `M_BAJA='N'`; shadow; no es columna comparable de `fnd_site` en el perfil leído |
| `M_SUCU_VIRTUAL` | `src.t100_empresa_suc.m_sucu_virtual` | — | Señal de sucursal virtual; las relaciones producto-sucursal excluyen `S` |
| `F_MODIF` | `src.t100_empresa_suc.f_modif` | — | Control temporal fuente |

## Relaciones y códigos de barras

| SGM | `diarco_data` | Connexa | Regla / estado |
|---|---|---|---|
| `T052.C_ARTICULO`, `.C_PROVEEDOR` | `src.t052_articulos_proveedor.c_articulo`, `.c_proveedor` | `inventory.inv_product_supplier.product_id`, `.supplier_id` | Resolver UUID por `inv_product.ext_code` y `inv_supplier.ext_code`; clave lógica `(artículo, proveedor)`; shadow y [legacy pausado](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/legacy/syncs/product_suppliers.py) |
| `T052.C_ARTICULO_PROVEEDOR` | `src.t052_articulos_proveedor.c_articulo_proveedor` | `supplier_product_code` | Texto normalizado; [shadow](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/domain/product_suppliers.py) |
| `T052.Q_FACTOR_PROVEEDOR` | `src.t052_articulos_proveedor.q_factor_proveedor` | `purchase_factor` | Decimal; autoridad comercial por aprobar |
| `T050.N_ARTICULO_FACT` | `src.t050_articulos.n_articulo_fact` | `inv_product_supplier.name` | Descripción de la relación en shadow; no proviene de `T052` |
| `T051.C_ARTICULO`, `.C_SUCU_EMPR` | `src.t051_articulos_sucursal.c_articulo`, `.c_sucu_empr` | `inventory.inv_product_site.product_id`, `.site_id` | Resolver por códigos externos; [shadow](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/pipeline/product_sites_shadow.py) |
| `T051.M_HABILITADO_SUCU` | `src.t051_articulos_sucursal.m_habilitado_sucu` | `active` | `S → true`; shadow |
| `T051.M_LISTO_PARA_VENTA_SUCU` | `src.t051_articulos_sucursal.m_listo_para_venta_sucu` | `active_for_sale` | `N → false`, otro valor incluso NULL → true; shadow |
| `T051.M_HABILITADO_SUCU` + `T050.M_A_DAR_DE_BAJA`, `.C_CLASIFICACION_COMPRA` | mismas columnas en `src` | `active_for_purchase` | false si baja propuesta, clasificación 4 o sucursal deshabilitada; shadow |
| `T050.C_FAMILIA`, `.M_A_DAR_DE_BAJA` | mismas columnas en `src` | `active_on_assortment` | false si familia 4 o baja propuesta; shadow |
| `T051.C_SISTEMATICA` | `src.t051_articulos_sucursal.c_sistematica` | `supply_type` | Código convertido a texto; semántica de catálogo por validar |
| EAN y DUN de la generación de `M_3_ARTICULOS` | `src.m_3_articulos.ean`, `.ean_alternativo_1`…`4`, `.dun14` | `inventory.inv_barcode` o `public.fnd_barcode`.`barcode` | Un registro por código no vacío, con validación; [lectura shadow](../../CNX_DIARCO_SYNC_V2/src/cnx_diarco_sync/sources/diarco_data.py). El linaje hasta la columna SGM de cada EAN requiere revisar la generación de `M_3_ARTICULOS` |
| `T052.Q_FACTOR_PROVEEDOR` | `src.t052_articulos_proveedor.q_factor_proveedor` | barcode `quantity` para DUN14 | Máximo factor por artículo, o 1; la autoridad del factor requiere validación |
| `T050.M_VENDE_POR_PESO` | `src.t050_articulos.m_vende_por_peso` | barcode `uom_id` | La unidad se resuelve en el destino; no es copia directa de `S/N` |

El shadow de producto-sucursal **no** lee `src.base_productos_vigentes` como contrato. Exige una sucursal no virtual en `T100`, un artículo presente en `M_3_ARTICULOS` y una fila en `T050`. Para barcodes, `T085_ARTICULOS_EAN_EDI` es fuente complementaria pendiente de frescura; el contrato actual lee EAN/DUN de `M_3_ARTICULOS`.

## Base derivada `src.base_productos_vigentes`

El [SP SQL Server](../scripts/sql/sp_SP_BASE_PRODUCTOS_DMZ.sql) y el [flujo de carga](../scripts/push/obtener_base_productos_vigentes.py) materializan un surtido por `(c_sucu_empr, c_articulo)`. El archivo [`nuevo_BASE_PRODUCTOS_DMZ.sql`](../scripts/sql/nuevo_BASE_PRODUCTOS_DMZ.sql) replica la proyección del SP y se usa aquí como referencia de trabajo; la selección efectiva del flujo por defecto es `dbo.SP_BASE_PRODUCTOS_DMZ`. Esta base es un derivado de `T051`, `T050`, `T052`, `T100`, stock `T060`, historial `T804` y marcas Barrio. Su presencia no equivale a que todos sus atributos sean maestros Connexa.

| `src.base_productos_vigentes` | Fuente principal | Regla |
|---|---|---|
| `c_sucu_empr`, `c_articulo` | `T051.C_SUCU_EMPR`, `.C_ARTICULO` | Clave lógica del surtido |
| `c_proveedor_primario`, `cod_comprador` | `T050.C_PROVEEDOR_PRIMARIO`, `.C_COMPRADOR` | Copia con conversión numérica |
| `abastecimiento`, `cod_cd`, `delivered_id` | `T051.C_SISTEMATICA`, sucursal y `T052.C_PROVEEDOR` | `abastecimiento` directo; los otros dos son códigos derivados por `CASE` |
| `habilitado` | `T051.M_HABILITADO_SUCU` o marca Barrio | `N → 0`; otro valor → 1 según rango de sucursal |
| `fecha_registro`, `fecha_baja` | `T051.F_ALTA` + `T804.F_ALTA_SIST` | Historial de altas/bajas; baja nula → `2099-12-31` |
| `q_peso_unit_art`, `m_vende_por_peso` | `T050.Q_PESO_UNIT_ART`, `.M_VENDE_POR_PESO` | Peso numérico; `S → 1`, otro → 0 |
| `promocion`, `active_for_sale` | `T051.M_OFERTA_SUCU`, `.M_LISTO_PARA_VENTA_SUCU` | `N → 0`, otro → 1 |
| `active_for_purchase` | `T050.M_A_DAR_DE_BAJA`, `.C_CLASIFICACION_COMPRA`; habilitación sucursal/Barrio | `0` si alguna condición inhabilitante; distinto del booleano del shadow producto-sucursal para Barrio |
| `active_on_mix` | `T050.C_FAMILIA`, `.M_A_DAR_DE_BAJA` | `0` si familia 4 o baja propuesta |
| `q_factor_compra`, `full_capacity_pallet`, `number_of_layers`, `number_of_boxes_per_layer` | `T052.Q_FACTOR_PROVEEDOR`, `.U_PISO_PALETIZADO`, `.U_ALTURA_PALETIZADO`; `T050.M_VENDE_POR_PESO` | Factor y capacidad derivados del proveedor primario |
| `unid_transferencia`, `q_unid_transferencia`, `pedido_min`, `frente_lineal`, `capacid_gondola`, `stock_minimo`, `product_base_id`, `own_production` | — | Constantes de la implementación; **no equivalencias SGM** |

La base usa por defecto el modo `sqlserver_sp`; `hybrid_src` está disponible y `pg_src` está marcado experimental en el código. Antes de comparar esta base con `inventory.inv_product_site` hay que validar diferencias de alcance y las reglas de habilitación para Barrio.

## Pendientes para un diccionario físico exhaustivo

1. Extraer catálogos `schema/table/column/type/nullability/PK` de SGM, `diarco_data` y cada perfil Connexa con acceso de sólo lectura. La evidencia aquí es código versionado, no introspección de bases.
2. Confirmar el linaje SGM de `M_3_ARTICULOS` y de cada EAN/DUN; definir autoridad de categoría, precio, factor DUN14 y `primary_barcode`.
3. Definir interfaces de `T114`, `T117`, días de entrega y `T055`, y contratos de ventas, stock y órdenes si entran en el alcance del diccionario completo.
4. Conciliar reglas entre el SP de `base_productos_vigentes` y los shadows nuevos, especialmente `active_for_purchase`, sucursales Barrio y universo comercial.
5. Registrar por ambiente la fecha de extracción, conteos y reconciliaciones antes de considerar una correspondencia lista para escribir en Connexa.
