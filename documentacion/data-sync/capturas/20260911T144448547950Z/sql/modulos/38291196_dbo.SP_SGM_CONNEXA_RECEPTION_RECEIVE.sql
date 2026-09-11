-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE      PROCEDURE [dbo].SP_SGM_CONNEXA_RECEPTION_RECEIVE    
    @JSON NVARCHAR(MAX)    
AS    
BEGIN    
        declare @Response varchar(max)    
             
        set @Response  = (SELECT     
            200 AS [status],    
            'Success' AS [title],    
            'Proceso completado exitosamente' AS [message],    
            JSON_QUERY((SELECT '' AS extCode FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)) AS [json],    
            1 AS [processedItems],    
            GETDATE() AS [timestamp]    
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)    
                
    
        select @Response    
end
GO
