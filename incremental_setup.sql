
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


-- -----------------------------------------------------------------------

ALTER TABLE tpcdi.FactMarketHistory ADD COLUMN (
    Date DATE,
    Symbol CHAR(15),
    FiftyTwoWeekLowDate DATE,
    FiftyTwoWeekHighDate DATE,
    prev1_quarter NUMERIC(1),
    prev2_quarter NUMERIC(1),
    prev3_quarter NUMERIC(1),
    prev4_quarter NUMERIC(1),
    prev1_year NUMERIC(4),
    prev2_year NUMERIC(4),
    prev3_year NUMERIC(4),
    prev4_year NUMERIC(4)
);


DROP TRIGGER IF EXISTS tpcdi.INC_FactMarketHistory;
delimiter $$
CREATE TRIGGER `INC_FactMarketHistory` BEFORE INSERT ON `FactMarketHistory`
FOR EACH ROW
BEGIN
    DECLARE _sec_id, _cmp_id NUMERIC(11);
    DECLARE _dividend NUMERIC(10,2);
	DECLARE _FiftyTwoMoreHigh NUMERIC(8,2);
	DECLARE _FiftyTwoMoreHigh_SK_Date NUMERIC(11);
	DECLARE _FiftyTwoLessLow NUMERIC(8,2);
	DECLARE _FiftyTwoLessLow_SK_Date NUMERIC(11);

    SELECT DimSecurity.SK_SecurityID, DimSecurity.SK_CompanyID, DimSecurity.Dividend
    INTO @_sec_id, @_cmp_id, @_dividend
    FROM DimSecurity
    WHERE DimSecurity.Symbol = NEW.Symbol AND
          DimSecurity.IsCurrent = 1;

    SET NEW.SK_SecurityID = @_sec_id;
    SET NEW.SK_CompanyID = @_cmp_id;

	
	SELECT FiftyTwoWeekHigh, SK_FiftyTwoWeekHighDate
	INTO @_FiftyTwoMoreHigh, @_FiftyTwoMoreHigh_SK_Date
	FROM FactMarketHistory
	WHERE FactMarketHistory.SK_SecurityID = @_sec_id AND
		  DATEDIFF(
			(SELECT DateValue FROM DimDate WHERE SK_DateID = FactMarketHistory.SK_DateID),
			New.Date
		  ) <=365 
		  AND FactMarketHistory.FiftyTwoWeekHigh > NEW.FiftyTwoWeekHigh;
	
	
    SET NEW.SK_DateID = (
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.Date
    );

	
	SET NEW.FiftyTwoWeekHigh = IFNULL(@_FiftyTwoMoreHigh,NEW.FiftyTwoWeekHigh);

    SET NEW.SK_FiftyTwoWeekHighDate = IFNULL(@_FiftyTwoMoreHigh_SK_Date,(
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.FiftyTwoWeekHighDate
    ));


	SELECT FiftyTwoWeekLow, SK_FiftyTwoWeekHighLowDate
	INTO @_FiftyTwoLessLow, @_FiftyTwoLessLow_SK_Date
	FROM FactMarketHistory
	WHERE FactMarketHistory.SK_SecurityID = @_sec_id AND
		  DATEDIFF(
			(SELECT DateValue FROM DimDate WHERE SK_DateID = FactMarketHistory.SK_DateID),
			New.Date
		  ) <=365 
		  AND FactMarketHistory.FiftyTwoWeekLow < NEW.FiftyTwoWeekLow;
	
	SET NEW.FiftyTwoWeekLow = IFNULL(@_FiftyTwoLessLow,NEW.FiftyTwoWeekLow);


    SET NEW.SK_FiftyTwoWeekLowDate = IFNULL(@_FiftyTwoLessLow_SK_Date,(
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.FiftyTwoWeekLowDate
    ));

    SET NEW.PERatio = (
        SELECT NEW.ClosePrice / SUM(Financial.FI_BASIC_EPS)
        FROM Financial
        WHERE Financial.SK_CompanyID = @_cmp_id AND (
              (Financial.FI_YEAR = NEW.prev1_year AND Financial.FI_QTR = NEW.prev1_quarter ) OR
              (Financial.FI_YEAR = NEW.prev2_year AND Financial.FI_QTR = NEW.prev2_quarter ) OR
              (Financial.FI_YEAR = NEW.prev3_year AND Financial.FI_QTR = NEW.prev3_quarter ) OR
              (Financial.FI_YEAR = NEW.prev4_year AND Financial.FI_QTR = NEW.prev4_quarter ))
    );

    IF (NEW.PERatio IS NULL) THEN
        INSERT INTO DImessages (BatchID, MessageSource, MessageText, MessageType, MessageData)
        VALUES (1, "FactMarketHistory", "No earnings for company", "Alert", CONCAT("DM_S_SYMB = ", NEW.Symbol));
    END IF;

    SET NEW.Yield = @_dividend * 100 / NEW.ClosePrice;
END;
$$
delimiter ;


-- -----------------------------------------------------------------------
ALTER TABLE tpcdi.FactWatches ADD COLUMN (
    CustomerID NUMERIC(11),
    Symbol CHAR(15),
    Date DATE,
    DateRemoved DATE
);

DROP TRIGGER IF EXISTS tpcdi.INC_FactWatches;
delimiter $$
CREATE TRIGGER `INC_FactWatches` BEFORE INSERT ON `FactWatches`
FOR EACH ROW
BEGIN
    SET NEW.SK_CustomerID = (
        SELECT DimCustomer.SK_CustomerID
        FROM DimCustomer
        WHERE DimCustomer.CustomerID = NEW.CustomerID AND
            DimCustomer.IsCurrent = 1
    );
    SET NEW.SK_SecurityID = (
        SELECT DimSecurity.SK_SecurityID
        FROM DimSecurity
        WHERE DimSecurity.Symbol = NEW.Symbol AND
            DimSecurity.IsCurrent = 1
    );
    SET NEW.SK_DateID_DatePlaced = (
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.Date
    );
    SET NEW.SK_DateID_DateRemoved = (
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.DateRemoved
    );
END;
$$
delimiter ;
