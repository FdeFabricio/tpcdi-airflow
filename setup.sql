DROP TABLE IF EXISTS DimAccount;
CREATE TABLE DimAccount (
    SK_AccountID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    AccountID INTEGER NOT NULL,
    SK_BrokerID INTEGER NOT NULL,
    SK_CustomerID INTEGER NOT NULL,
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
    SK_SecurityID INTEGER NOT NULL PRIMARY KEY,
    Symbol CHAR(15) NOT NULL,
    Issue CHAR(6) NOT NULL,
    Status CHAR(10) NOT NULL,
    Name CHAR(70) NOT NULL,
    ExchangeID CHAR(6) NOT NULL,
    SK_CompanyID INTEGER NOT NULL,
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
    SK_RecordDateID INTEGER NOT NULL,
    SK_UpdateDateID INTEGER NOT NULL,
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
    SK_SecurityID INTEGER NOT NULL,
    SK_CompanyID INTEGER NOT NULL REFERENCES DimCompany (SK_CompanyID),
    SK_DateID INTEGER NOT NULL REFERENCES DimDate (SK_DateID),
    PERatio NUMERIC(10,2),
    Yield NUMERIC(5,2) NOT NULL,
    FiftyTwoWeekHigh NUMERIC(8,2) NOT NULL,
    SK_FiftyTwoWeekHighDate INTEGER NOT NULL,
    FiftyTwoWeekLow NUMERIC(8,2) NOT NULL,
    SK_FiftyTwoWeekLowDate INTEGER NOT NULL,
    ClosePrice NUMERIC(8,2) NOT NULL,
    DayHigh NUMERIC(8,2) NOT NULL,
    DayLow NUMERIC(8,2) NOT NULL,
    Volume NUMERIC(12) NOT NULL,
    BatchID NUMERIC(5),
    Date DATE NOT NULL,
    Symbol CHAR(15) NOT NULL,
    FiftyTwoWeekLowDate DATE NOT NULL,
    FiftyTwoWeekHighDate DATE NOT NULL,
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

DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_CustomerID;
DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_BrokerID;
DROP TRIGGER IF EXISTS tpcdi.ADD_Prospect_DateID;
DROP TRIGGER IF EXISTS tpcdi.ADD_FactMarketHistory;
DROP TRIGGER IF EXISTS tpcdi.ADD_FactWatches;

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

    SELECT DimSecurity.SK_SecurityID, DimSecurity.SK_CompanyID
    INTO @_sec_id, @_cmp_id
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

delimiter ;
