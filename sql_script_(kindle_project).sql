CREATE OR REPLACE TABLE `focus-empire-363510.customer2.cleansed_data` AS
(SELECT
  InvoiceNo, StockCode, Description, Quantity,PARSE_DATETIME("%m/%d/%Y %H:%M", InvoiceDate) AS InvoiceDate,
  UnitPrice, CustomerID, Country, (Quantity*UnitPrice) AS ItemTotal
FROM
  `focus-empire-363510.customer2.E-Commerce`
WHERE Quantity > 0 and CustomerID is not NULL);

CREATE OR REPLACE TABLE customer2.temp1 as 
(SELECT *,
    CAST(EXTRACT(day FROM InvoiceDate) AS int64) AS Day,
    FORMAT_DATE('%m-%Y', InvoiceDate) AS Month      
 FROM `focus-empire-363510.customer2.cleansed_data`);

CREATE OR REPLACE TABLE `focus-empire-363510.customer2.cleansed_data2` AS
(SELECT InvoiceNo, StockCode, Description, Quantity,InvoiceDate, UnitPrice, CustomerID, Country,ItemTotal,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
    FROM `focus-empire-363510.customer2.temp1` AS T1
    INNER JOIN (SELECT  
                    Month 
                FROM `focus-empire-363510.customer2.temp1`
                GROUP BY Month
                HAVING COUNT(DISTINCT Day)  >= 22) AS T2
  ON T1.MONTH = T2.MONTH);

CREATE OR REPLACE TABLE `focus-empire-363510.customer2.ONLINE_RETAIL` AS
(SELECT
  InvoiceNo, StockCode, Description, Quantity,InvoiceDate,
  UnitPrice, CustomerID, Country, ItemTotal
FROM `focus-empire-363510.customer2.cleansed_data2`);

CREATE OR REPLACE TABLE `focus-empire-363510.customer2.CUSTOMER_SUMMARY` AS
(SELECT
  CustomerID,
  ROUND(SUM(ItemTotal),2) AS TotalSales,
  COUNT(InvoiceNo) AS OrderCount,
  ROUND(AVG(ItemTotal),2) AS AvgOrderValue
FROM `focus-empire-363510.customer2.cleansed_data2`
GROUP BY CustomerID
ORDER BY OrderCount DESC);

CREATE OR REPLACE TABLE `focus-empire-363510.customer2.SALES_SUMMARY` AS
(SELECT 
  Country, 
  TotalSales, 
  ROUND((TotalSales*100/S),2) AS PercentofCountrySales 
FROM
  (SELECT Country, ROUND(SUM(ItemTotal),2) AS TotalSales
    FROM `focus-empire-363510.customer2.cleansed_data2`
    GROUP BY 1)
CROSS JOIN (SELECT SUM(ItemTotal) AS S FROM `focus-empire-363510.customer2.cleansed_data2`)
ORDER BY PercentofCountrySales DESC);
