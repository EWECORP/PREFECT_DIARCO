/****** Object: StoredProcedure usp_replicar_T080_OC_PENDIENTES **/

USE [data-sync]
GO

/****** Object:  StoredProcedure [repl].[usp_replicar_T080_OC_PENDIENTES]    Script Date: 09/06/2026 11:59:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER     PROCEDURE [repl].[usp_replicar_T080_OC_PENDIENTES]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio DATETIME = GETDATE();
    DECLARE @mensaje NVARCHAR(4000);
    DECLARE @total INT = 0;

    BEGIN TRY
        -- 1) Vaciar staging
        TRUNCATE TABLE repl.T080_OC_PENDIENTES;

        -- 2) Insertar desde linked server + enriquecer con T050 y T052
        INSERT INTO repl.T080_OC_PENDIENTES (
            SUCU_COMPRA,
            C_SUCU_DESTINO,
            C_SUCU_DESTINO_ALT,
            U_PREFIJO_OC,
            U_SUFIJO_OC,
            C_PROVEEDOR,
            C_ARTICULO,
            Pendientes,

            -- Campos enriquecidos
            Q_PESO_UNIT_ART,
            M_VENDE_POR_PESO,
            Q_FACTOR_COMPRA,

            FUENTE_ORIGEN,
            FECHA_EXTRACCION,
            CDC_LSN,
            ESTADO_SINCRONIZACION
        )
        SELECT 
            OC_CABE.C_SUCU_COMPRA  AS SUCU_COMPRA,
            OC_CABE.C_SUCU_DESTINO AS C_SUCU_DESTINO,
            OC_CABE.C_SUCU_DESTINO_ALT AS C_SUCU_DESTINO_ALT,
            OC_CABE.U_PREFIJO_OC   AS U_PREFIJO_OC,
            OC_CABE.U_SUFIJO_OC    AS U_SUFIJO_OC,
            OC_CABE.C_PROVEEDOR    AS C_PROVEEDOR,
            OC_DETA.C_ARTICULO     AS C_ARTICULO,

            -- Pendientes condicional (pesable vs no pesable)
            SUM(
                CASE 
                    WHEN ART.M_VENDE_POR_PESO = 'S' THEN
                        (OC_DETA.Q_BULTOS_PROV_PED * OC_DETA.Q_FACTOR_PROV_PED * ISNULL(ART.Q_PESO_UNIT_ART, 0))
                        - ISNULL(OC_DETA.Q_PESO_CUMPLIDO, 0)
                    ELSE
                        (OC_DETA.Q_BULTOS_PROV_PED * OC_DETA.Q_FACTOR_PROV_PED)
                        - ISNULL(OC_DETA.Q_UNID_CUMPLIDAS, 0)
                END
            ) AS Pendientes,

            -- T050_ARTICULOS (nivel artículo)
            MAX(ART.Q_PESO_UNIT_ART)          AS Q_PESO_UNIT_ART,
            MAX(ART.M_VENDE_POR_PESO)         AS M_VENDE_POR_PESO,

            -- T052_ARTICULOS_PROVEEDOR (nivel artículo-proveedor)
            MAX(AP.Q_FACTOR_PROVEEDOR)        AS Q_FACTOR_COMPRA,

            'DIARCOP001'                      AS FUENTE_ORIGEN,
            GETDATE()                         AS FECHA_EXTRACCION,
            CONVERT(VARBINARY(10), NULL)      AS CDC_LSN,
            0                                 AS ESTADO_SINCRONIZACION
        FROM [DIARCOP001].[DIARCOP].DBO.T080_OC_CABE OC_CABE
        INNER JOIN [DIARCOP001].[DIARCOP].DBO.T081_OC_DETA OC_DETA  
            ON OC_DETA.C_OC = OC_CABE.C_OC  
            AND OC_DETA.U_PREFIJO_OC = OC_CABE.U_PREFIJO_OC  
            AND OC_DETA.U_SUFIJO_OC = OC_CABE.U_SUFIJO_OC 

        LEFT JOIN [DIARCOP001].[DIARCOP].DBO.T050_ARTICULOS ART
            ON ART.C_ARTICULO = OC_DETA.C_ARTICULO

        LEFT JOIN [DIARCOP001].[DIARCOP].DBO.T052_ARTICULOS_PROVEEDOR AP
            ON AP.C_ARTICULO = OC_DETA.C_ARTICULO
            AND AP.C_PROVEEDOR = OC_CABE.C_PROVEEDOR

        WHERE
			OC_DETA.M_CUMPLIDA_PARCIAL ='N'
            AND OC_CABE.C_SITUAC = 1
            AND (
                CASE 
                    WHEN ART.M_VENDE_POR_PESO = 'S' THEN
                        (OC_DETA.Q_BULTOS_PROV_PED * OC_DETA.Q_FACTOR_PROV_PED * ISNULL(ART.Q_PESO_UNIT_ART, 0))
                        - ISNULL(OC_DETA.Q_PESO_CUMPLIDO, 0)
                    ELSE
                        (OC_DETA.Q_BULTOS_PROV_PED * OC_DETA.Q_FACTOR_PROV_PED)
                        - ISNULL(OC_DETA.Q_UNID_CUMPLIDAS, 0)
                END
            ) <> 0

        GROUP BY 
            OC_CABE.C_SUCU_COMPRA,
            OC_CABE.C_SUCU_DESTINO,
            OC_CABE.C_SUCU_DESTINO_ALT,
            OC_CABE.U_PREFIJO_OC,
            OC_CABE.U_SUFIJO_OC,
            OC_CABE.C_PROVEEDOR,
            OC_DETA.C_ARTICULO;

        SET @total = @@ROWCOUNT;

        SET @mensaje = CONCAT(
            'Replicación exitosa. Registros insertados: ', @total,
            '. Tiempo: ', DATEDIFF(SECOND, @inicio, GETDATE()), ' segundos.'
        );

    END TRY
    BEGIN CATCH
        SET @mensaje = CONCAT(
            'Error en replicación: ', ERROR_MESSAGE(),
            '. Línea: ', ERROR_LINE(),
            '. Estado: ', ERROR_STATE()
        );
        RAISERROR(@mensaje, 16, 1);
    END CATCH
END;
GO


