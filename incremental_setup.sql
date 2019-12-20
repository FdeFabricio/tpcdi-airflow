
ALTER TABLE tpcdi.FactCashBalances ADD COLUMN (
	CT_CA_ID NUMERIC (11),
	Date date NOT NULL,
	DayTotal NUMERIC (15,2) NOT NULL
);

DROP TRIGGER IF EXISTS tpcdi.INC_FactCashBalances;
delimiter $$
CREATE TRIGGER `INC_FactCashBalances` BEFORE INSERT ON `FactCashBalances`
FOR EACH ROW
BEGIN
	DECLARE _cID, _aID NUMERIC(11);

	SELECT DimAccount.SK_cID, DimAccount.SK_aID
	INTO @_cID, @_aID
	FROM DimAccount
	WHERE DimAccount.AccountID = NEW.CT_CA_ID AND
	DimAccount.IsCurrent =  1;
	
	SET NEW.SK_cID = @_cID;
	SET NEW.SK_aID = @_aID;	
	
	SET NEW.SK_DateID = (
		SELECT DimDate.SK_DateID
		FROM DimDate
		WHERE DimDate.DateValue = NEW.Date
	);
	
	SET NEW.Cash = NEW.DayTotal + IFNULL((
		SELECT FactCashBalances.Cash
		FROM FactCashBalances
		WHERE NEW.SK_aID = FactCashBalances.SK_aID
		ORDER BY SK_DateID DESC
		LIMIT 1
	),0);
END;
$$
delimiter ;