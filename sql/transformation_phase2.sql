-- Second Phase Project5 Transformation 
use project5_db;

Update OrderTable
Set OrderTable.CustNo = (SELECT CustNo FROM CustomerTable WHERE Cust_Name='Francis Strayhorn');

-- Update the ProdNo based on the Size and Prod_Name
UPDATE OrderDetailTable
SET ProdNo = (
SELECT ProdNo
FROM ProductTable
WHERE ProductTable.ProdName = OrderDetailTable.ProdName
And Trim(ProductTable.Size) = Trim(OrderDetailTable.Size)
);

-- Below are ETL Phase III Process -------------------------------------------
-- Update the StoreNo based on the Store_Name
UPDATE OrderTable
SET StoreNo = (
SELECT StoreNo
FROM StoreTable
WHERE Trim(StoreTable.Store_Name) = Trim(StoreTable.Store_Name)
);

-- Update ETL Data the StoreNo based on the Cust_Name
-- Update ETL Data the CustNo based on the RecNo in both OrderTable and CustomerTable
UPDATE OrderTable
SET CustNo = (
SELECT CustNo FROM CustomerTable
WHERE OrderTable.RecNo = CustomerTable.RecNo);

ALTER TABLE OrderTable DROP COLUMN RecNo, DROP COLUMN Store_Name, DROP COLUMN Cust_Name; 
ALTER TABLE StoreTable Drop COLUMN RecNo;
ALTER TABLE CustomerTable Drop COLUMN RecNo;