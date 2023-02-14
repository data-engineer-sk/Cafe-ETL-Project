-- Create Database for libraries data (Act & Culture)
CREATE database if not exists project5_db;

use project5_db;

-- Create OrderTable Table
CREATE TABLE OrderTable (
	OrderNo CHAR(10),
    RecNo INT,
    TxDate DATETIME,
    StoreNo CHAR(10),
    Store_Name VARCHAR(100),
	CustNo CHAR(10),
    Cust_Name VARCHAR(100),
    Payment CHAR(4),
    TotPrice DECIMAL(12,2),
    PRIMARY KEY (OrderNo)
);

-- Create OrderDetak.Table Table
CREATE TABLE OrderDetailTable (
    RecNo BIGINT,
	OrderNO CHAR(10),
	Size VARCHAR(10),
    ProdNo CHAR(10),
	ProdName VARCHAR(100),
    UnitPrice DECIMAL(6,2),
	PRIMARY KEY (RecNo),
	FOREIGN KEY (OrderNo) REFERENCES OrderTable(OrderNo)
);

-- Create CustomerTable Table
CREATE TABLE CustomerTable (
	CustNo CHAR(10),
    RecNo INT,
    Cust_Name VARCHAR(100),
    PRIMARY KEY (CustNo)
);

-- Create StoreTable Table
CREATE TABLE StoreTable (
	StoreNo CHAR(10),
    RecNo INT,
    Store_Name VARCHAR(100),
    PRIMARY KEY (StoreNo)
);

CREATE TABLE ProductTable (
	ProdNo CHAR(10),
	Size VARCHAR(10),
	ProdName VARCHAR(100),
    UnitPrice DECIMAL(6,2),
    PRIMARY KEY (ProdNo)
);