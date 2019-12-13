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

DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_CustomerID;
DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_BrokerID;
DROP TRIGGER IF EXISTS tpcdi.ADD_Prospect_DateID;

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

delimiter ;
