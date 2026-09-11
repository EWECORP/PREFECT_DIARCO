-- Definición capturada; revisar dependencias y redacciones antes de ejecutar.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   FUNCTION dbo.CONCAT_WS_SIMULADO
(
    @sep NVARCHAR(10),
    @val1 NVARCHAR(MAX) = NULL,
    @val2 NVARCHAR(MAX) = NULL,
    @val3 NVARCHAR(MAX) = NULL,
    @val4 NVARCHAR(MAX) = NULL,
    @val5 NVARCHAR(MAX) = NULL
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX) = ''

    IF @val1 IS NOT NULL SET @result = @val1
    IF @val2 IS NOT NULL SET @result = CASE WHEN @result <> '' THEN @result + @sep + @val2 ELSE @val2 END
    IF @val3 IS NOT NULL SET @result = CASE WHEN @result <> '' THEN @result + @sep + @val3 ELSE @val3 END
    IF @val4 IS NOT NULL SET @result = CASE WHEN @result <> '' THEN @result + @sep + @val4 ELSE @val4 END
    IF @val5 IS NOT NULL SET @result = CASE WHEN @result <> '' THEN @result + @sep + @val5 ELSE @val5 END

    RETURN @result
END

GO
