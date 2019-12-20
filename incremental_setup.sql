
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

-- ---------------------------------------------------------------------

DROP TRIGGER IF EXISTS tpcdi.INC_FactHoldings;
delimiter $$
CREATE TRIGGER `INC_FactHoldings` BEFORE INSERT ON `FactHoldings`
FOR EACH ROW
BEGIN
    DECLARE _customerID, _accountID, _securityID, _companyID, _dateID, _timeID NUMERIC(11);
    DECLARE _price NUMERIC(8,2);

    SELECT DimTrade.SK_CustomerID, DimTrade.SK_AccountID, DimTrade.SK_SecurityID, DimTrade.SK_CompanyID, DimTrade.SK_CloseDateID, DimTrade.SK_CloseTimeID, DimTrade.TradePrice
    INTO @_customerID, @_accountID, @_securityID, @_companyID, @_dateID, @_timeID, @_price
    FROM DimTrade
    WHERE NEW.TradeID = DimTrade.TradeID;

    SET NEW.SK_CustomerID = @_customerID;
    SET NEW.SK_AccountID = @_accountID;
    SET NEW.SK_SecurityID = @_securityID;
    SET NEW.SK_CompanyID = @_companyID;
    SET NEW.SK_DateID = @_dateID;
    SET NEW.SK_TimeID = @_timeID;
    SET NEW.CurrentPrice = @_price;
END;
$$
delimiter ;