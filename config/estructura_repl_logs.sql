-- ================================================================
-- Esquema REPL para datos replicados con CDC o extracción masiva
-- ================================================================
CREATE SCHEMA IF NOT EXISTS repl;

-- Tabla de Artículos
CREATE TABLE repl.articulos (
    id_articulo INT PRIMARY KEY,
    descripcion VARCHAR(255),
    rubro VARCHAR(100),
    subrubro VARCHAR(100),
    fuente_origen VARCHAR(100),
    fecha_extraccion DATETIME,
    cdc_lsn VARBINARY(10),
    estado_sincronizacion TINYINT
);

-- Tabla de Stock Actual
CREATE TABLE repl.stock_actual (
    id_articulo INT,
    id_sucursal INT,
    stock_unidades DECIMAL(18,2),
    stock_valorizado DECIMAL(18,2),
    fuente_origen VARCHAR(100),
    fecha_extraccion DATETIME,
    cdc_lsn VARBINARY(10),
    estado_sincronizacion TINYINT,
    PRIMARY KEY (id_articulo, id_sucursal)
);

-- Tabla de Ventas
CREATE TABLE repl.ventas (
    id_articulo INT,
    id_sucursal INT,
    fecha DATE,
    unidades_vendidas DECIMAL(18,2),
    precio DECIMAL(18,2),
    costo DECIMAL(18,2),
    fuente_origen VARCHAR(100),
    fecha_extraccion DATETIME,
    cdc_lsn VARBINARY(10),
    estado_sincronizacion TINYINT,
    PRIMARY KEY (id_articulo, id_sucursal, fecha)
);

-- Tabla de Precarga de OC
CREATE TABLE repl.oc_precarga (
    id_oc INT IDENTITY(1,1) PRIMARY KEY,
    id_articulo INT,
    id_sucursal INT,
    proveedor VARCHAR(100),
    cantidad DECIMAL(18,2),
    usuario VARCHAR(100),
    fecha_precarga DATETIME,
    fuente_origen VARCHAR(100),
    fecha_extraccion DATETIME,
    cdc_lsn VARBINARY(10),
    estado_sincronizacion TINYINT
);

-- ================================================================
-- Esquema LOGS para trazabilidad de ejecuciones
-- ================================================================
CREATE SCHEMA IF NOT EXISTS logs;

-- Tabla de logs de procesos Prefect
CREATE TABLE logs.procesos_etl (
    id INT IDENTITY(1,1) PRIMARY KEY,
    nombre_flujo VARCHAR(100),
    nombre_tarea VARCHAR(100),
    fecha_inicio DATETIME,
    fecha_fin DATETIME,
    estado VARCHAR(50),
    registros_afectados INT,
    mensaje_error TEXT,
    contexto JSON
);