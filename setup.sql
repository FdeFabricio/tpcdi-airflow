DROP TABLE IF EXISTS DimDate;
CREATE TABLE DimDate (
    SK_DateID INTEGER NOT NULL PRIMARY KEY,
    DateValue DATE NOT NULL,
    DateDesc CHAR(20) NOT NULL,
    CalendarYearID NUMERIC(4) NOT NULL,
    CalendarYearDesc CHAR(20) NOT NULL,
    CalendarQtrID NUMERIC(5) NOT NULL,
    CalendarQtrDesc CHAR(20) NOT NULL,
    CalendarMonthID NUMERIC(6) NOT NULL,
    CalendarMonthDesc CHAR(20) NOT NULL,
    CalendarWeekID NUMERIC(6) NOT NULL,
    CalendarWeekDesc CHAR(20) NOT NULL,
    DayOfWeekNum NUMERIC(1) NOT NULL,
    DayOfWeekDesc CHAR(10) NOT NULL,
    FiscalYearID NUMERIC(4) NOT NULL,
    FiscalYearDesc CHAR(20) NOT NULL,
    FiscalQtrID NUMERIC(5) NOT NULL,
    FiscalQtrDesc CHAR(20) NOT NULL,
    HolidayFlag BOOLEAN
);

DROP TABLE IF EXISTS DimTime;
CREATE TABLE DimTime (
    SK_TimeID NUMERIC(11) NOT NULL PRIMARY KEY,
    TimeValue TIME NOT NULL,
    HourID numeric(2) NOT NULL,
    HourDesc CHAR(20) NOT NULL,
    MinuteID numeric(2) NOT NULL,
    MinuteDesc CHAR(20) NOT NULL,
    SecondID numeric(2) NOT NULL,
    SecondDesc CHAR(20) NOT NULL,
    MarketHoursFlag BOOLEAN,
    OfficeHoursFlag BOOLEAN
);

DROP TABLE IF EXISTS DimAccount;
CREATE TABLE DimAccount (
    SK_AccountID NUMERIC(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
    AccountID INTEGER NOT NULL,
    SK_BrokerID NUMERIC(11) NOT NULL,
    SK_CustomerID NUMERIC(11) NOT NULL,
    Status CHAR(10) NOT NULL,
    AccountDesc CHAR(50),
    TaxStatus NUMERIC(1) NOT NULL CHECK (TaxStatus IN (0, 1, 2)),
    IsCurrent BOOLEAN NOT NULL,
    BatchID NUMERIC(5) NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL
);

DROP TABLE IF EXISTS DimSecurity;
CREATE TABLE DimSecurity(
    SK_SecurityID NUMERIC(11) NOT NULL PRIMARY KEY,
    Symbol CHAR(15) NOT NULL,
    Issue CHAR(6) NOT NULL,
    Status CHAR(10) NOT NULL,
    Name CHAR(70) NOT NULL,
    ExchangeID CHAR(6) NOT NULL,
    SK_CompanyID NUMERIC(11) NOT NULL,
    SharesOutstanding INTEGER NOT NULL,
    FirstTrade DATE NOT NULL,
    FirstTradeOnExchange DATE NOT NULL,
    Dividend INTEGER NOT NULL,
    IsCurrent BOOLEAN NOT NULL,
    BatchID NUMERIC(5) NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL
);

DROP TABLE IF EXISTS Prospect;
CREATE TABLE Prospect(
    AgencyID CHAR(30) NOT NULL,
    SK_RecordDateID NUMERIC(11) NOT NULL,
    SK_UpdateDateID NUMERIC(11) NOT NULL,
    BatchID NUMERIC(5) NOT NULL,
    IsCustomer BOOLEAN NOT NULL,
    LastName CHAR(30) NOT NULL,
    FirstName CHAR(30) NOT NULL,
    MiddleInitial CHAR(1),
    Gender CHAR(1) CHECK (Gender IN ('F', 'M', 'U')),
    AddressLine1 CHAR(80),
    AddressLine2 CHAR(80),
    PostalCode CHAR(12),
    City CHAR(25) NOT NULL,
    State CHAR(20) NOT NULL,
    Country CHAR(24),
    Phone CHAR(30),
    Income NUMERIC(9),
    NumberCars NUMERIC(2),
    NumberChildren NUMERIC(2),
    MaritalStatus CHAR(1) CHECK (MaritalStatus IN ('S', 'M', 'D', 'W', 'U')),
    Age NUMERIC(3),
    CreditRating NUMERIC(4),
    OwnOrRentFlag CHAR(1) CHECK (OwnOrRentFlag IN ('O', 'R', 'U')),
    Employer CHAR(30),
    NumberCreditCards NUMERIC(2),
    NetWorth NUMERIC(12),
    MarketingNameplate CHAR(100),

    Date DATE NOT NULL,
    ProspectKey CHAR(232)
);

DROP TABLE IF EXISTS FactMarketHistory;
CREATE TABLE FactMarketHistory(
    SK_SecurityID NUMERIC(11) NOT NULL,
    SK_CompanyID NUMERIC(11) NOT NULL REFERENCES DimCompany (SK_CompanyID),
    SK_DateID NUMERIC(11) NOT NULL REFERENCES DimDate (SK_DateID),
    PERatio NUMERIC(10,2),
    Yield NUMERIC(5,2) NOT NULL,
    FiftyTwoWeekHigh NUMERIC(8,2) NOT NULL,
    SK_FiftyTwoWeekHighDate NUMERIC(11) NOT NULL,
    FiftyTwoWeekLow NUMERIC(8,2) NOT NULL,
    SK_FiftyTwoWeekLowDate NUMERIC(11) NOT NULL,
    ClosePrice NUMERIC(8,2) NOT NULL,
    DayHigh NUMERIC(8,2) NOT NULL,
    DayLow NUMERIC(8,2) NOT NULL,
    Volume NUMERIC(12) NOT NULL,
    BatchID NUMERIC(5),
    Date DATE NOT NULL,
    Symbol CHAR(15) NOT NULL,
    FiftyTwoWeekLowDate DATE NOT NULL,
    FiftyTwoWeekHighDate DATE NOT NULL,
    prev1_quarter NUMERIC(1) NOT NULL,
    prev2_quarter NUMERIC(1) NOT NULL,
    prev3_quarter NUMERIC(1) NOT NULL,
    prev4_quarter NUMERIC(1) NOT NULL,
    prev1_year NUMERIC(4) NOT NULL,
    prev2_year NUMERIC(4) NOT NULL,
    prev3_year NUMERIC(4) NOT NULL,
    prev4_year NUMERIC(4) NOT NULL
);

DROP TABLE IF EXISTS FactWatches;
CREATE TABLE FactWatches (
    SK_CustomerID NUMERIC(11) NOT NULL,
    SK_SecurityID NUMERIC(11) NOT NULL,
    SK_DateID_DatePlaced NUMERIC(11) NOT NULL,
    SK_DateID_DateRemoved NUMERIC(11),
    BatchID NUMERIC(5) NOT NULL,
    CustomerID NUMERIC(11) NOT NULL,
    Symbol CHAR(15) NOT NULL,
    Date DATE NOT NULL,
    DateRemoved DATE
);

DROP TABLE IF EXISTS FactHoldings;
CREATE TABLE FactHoldings (
    TradeID NUMERIC(11) NOT NULL,
    CurrentTradeID NUMERIC(11) NOT NULL,
    SK_CustomerID NUMERIC(11) NOT NULL,
    SK_AccountID NUMERIC(11) NOT NULL,
    SK_SecurityID NUMERIC(11) NOT NULL,
    SK_CompanyID NUMERIC(11) NOT NULL,
    SK_DateID NUMERIC(11) NOT NULL,
    SK_TimeID NUMERIC(11) NOT NULL,
    CurrentPrice NUMERIC(8,2) NOT NULL,
    CurrentHolding NUMERIC(6) NOT NULL,
    BatchID NUMERIC(5) NOT NULL
);

DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_CustomerID;
DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_BrokerID;
DROP TRIGGER IF EXISTS tpcdi.ADD_Prospect_DateID;
DROP TRIGGER IF EXISTS tpcdi.ADD_FactMarketHistory;
DROP TRIGGER IF EXISTS tpcdi.ADD_FactWatches;
DROP TRIGGER IF EXISTS tpcdi.ADD_FactHoldings;

delimiter $$

CREATE TRIGGER `ADD_DimAccount_SK_CustomerID` BEFORE INSERT ON `DimAccount`
FOR EACH ROW
BEGIN
    SET NEW.SK_CustomerID = (
        SELECT DimCustomer.SK_CustomerID
        FROM DimCustomer
        WHERE DimCustomer.CustomerID = NEW.SK_CustomerID AND NEW.EndDate <= DimCustomer.EndDate
        LIMIT 1
    );
END;

$$

CREATE TRIGGER `ADD_DimAccount_SK_BrokerID` BEFORE INSERT ON `DimAccount`
FOR EACH ROW
BEGIN
    SET NEW.SK_BrokerID = (
        SELECT DimBroker.SK_BrokerID
        FROM DimBroker
        WHERE DimBroker.BrokerID = NEW.SK_BrokerID
    );
END;

$$

CREATE TRIGGER `ADD_Prospect_DateID` BEFORE INSERT ON `Prospect`
FOR EACH ROW
BEGIN
    DECLARE _date_id DATE;
    SELECT DimDate.SK_DateID INTO @_date_id FROM DimDate WHERE DimDate.DateValue = NEW.Date;
    SET NEW.SK_RecordDateID = @_date_id;
    SET NEW.SK_UpdateDateID = @_date_id;
    IF EXISTS (
        SELECT SK_CustomerID
        FROM DimCustomer WHERE Status = "ACTIVE" AND ProspectKey = NEW.ProspectKey
    ) THEN
       SET NEW.IsCustomer = TRUE;
    ELSE
        SET NEW.IsCustomer = FALSE;
    END IF;
END;

$$

CREATE TRIGGER `ADD_FactMarketHistory` BEFORE INSERT ON `FactMarketHistory`
FOR EACH ROW
BEGIN
    DECLARE _sec_id, _cmp_id INT;
    DECLARE _dividend FLOAT;

    SELECT DimSecurity.SK_SecurityID, DimSecurity.SK_CompanyID, DimSecurity.Dividend
    INTO @_sec_id, @_cmp_id, @_dividend
    FROM DimSecurity
    WHERE DimSecurity.Symbol = NEW.Symbol AND
          DimSecurity.EffectiveDate <= NEW.Date AND
          DimSecurity.EndDate > NEW.Date;

    SET NEW.SK_SecurityID = @_sec_id;
    SET NEW.SK_CompanyID = @_cmp_id;

    SET NEW.SK_DateID = (
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.Date
    );

    SET NEW.SK_FiftyTwoWeekHighDate = (
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.FiftyTwoWeekHighDate
    );

    SET NEW.SK_FiftyTwoWeekLowDate = (
        SELECT DimDate.SK_DateID
        FROM DimDate
        WHERE DimDate.DateValue = NEW.FiftyTwoWeekLowDate
    );

    SET NEW.PERatio = (
        SELECT NEW.ClosePrice / SUM(Financial.FI_BASIC_EPS)
        FROM Financial
        WHERE Financial.SK_CompanyID = @_cmp_id AND (
              (Financial.FI_YEAR = NEW.prev1_year AND Financial.FI_QTR = NEW.prev1_quarter ) OR
              (Financial.FI_YEAR = NEW.prev2_year AND Financial.FI_QTR = NEW.prev2_quarter ) OR
              (Financial.FI_YEAR = NEW.prev3_year AND Financial.FI_QTR = NEW.prev3_quarter ) OR
              (Financial.FI_YEAR = NEW.prev4_year AND Financial.FI_QTR = NEW.prev4_quarter ))
    );

    SET NEW.Yield = @_dividend * 100 / NEW.ClosePrice;

END;

$$

CREATE TRIGGER `ADD_FactWatches` BEFORE INSERT ON `FactWatches`
FOR EACH ROW
BEGIN

    SET NEW.SK_CustomerID = (
        SELECT DimCustomer.SK_CustomerID
        FROM DimCustomer
        WHERE DimCustomer.CustomerID = NEW.CustomerID AND
            DimCustomer.EffectiveDate <= NEW.Date AND
            DimCustomer.EndDate > NEW.Date
    );

    SET NEW.SK_SecurityID = (
        SELECT DimSecurity.SK_SecurityID
        FROM DimSecurity
        WHERE DimSecurity.Symbol = NEW.Symbol AND
            DimSecurity.EffectiveDate <= NEW.Date AND
            DimSecurity.EndDate > NEW.Date
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

CREATE TRIGGER `ADD_FactHoldings` BEFORE INSERT ON `FactHoldings`
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
