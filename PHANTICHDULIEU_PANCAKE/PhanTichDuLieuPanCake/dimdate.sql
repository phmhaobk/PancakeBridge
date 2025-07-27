DECLARE @DateToAdd DATE = DATEADD(DAY, -1, GETDATE()); -- Ng�y h�m qua

IF NOT EXISTS (SELECT 1 FROM dim_date WHERE date = @DateToAdd)
BEGIN
    INSERT INTO dim_date (date)
    VALUES (@DateToAdd);
END;
