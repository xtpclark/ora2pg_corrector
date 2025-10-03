CREATE FUNCTION dbo.CountBusinessDays (
    @StartDate DATE,
    @EndDate DATE
) RETURNS INT
AS
BEGIN
    DECLARE @Count INT = 0;
    DECLARE @CurrentDate DATE = @StartDate;
    
    WHILE @CurrentDate <= @EndDate
    BEGIN
        IF DATEPART(WEEKDAY, @CurrentDate) NOT IN (1, 7)
        BEGIN
            SET @Count = @Count + 1;
        END
        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
    
    RETURN @Count;
END;
GO
