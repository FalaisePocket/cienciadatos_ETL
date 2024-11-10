'''import pandas as pd
from connection_script import connect_databases
from datetime import datetime



db_op, db_etl = connect_databases()



#Extract


# Extract required data
# Sales online orders
sales_order = pd.read_sql_query("""
        SELECT *,[dbo].[ufnGetProductStandardCost](t2.ProductID, t1.OrderDate) AS ProductStandardCost
        FROM Sales.SalesOrderHeader AS t1
        INNER JOIN Sales.SalesOrderDetail AS t2
        ON t1.SalesOrderID = t2.SalesOrderID
        WHERE t1.OnlineOrderFlag = 1;
        """, db_op)
sales_order = sales_order.loc[:,~sales_order.columns.duplicated()]
sales_order


# Load required dimensions
dim_product =  pd.read_sql_query('SELECT * FROM "DimProduct";', db_etl)
dim_product

operational_products = pd.read_sql_query('SELECT ProductID, ProductNumber FROM [Production].[Product];', db_op)

dim_product = dim_product.merge(operational_products, left_on="ProductAlternateKey", right_on="ProductNumber")
dim_product = dim_product.drop_duplicates(subset=["ProductID"])

dim_customer = pd.read_sql_query('SELECT * FROM "DimCustomer";', db_etl)

dim_promotion = pd.read_sql_query('SELECT * FROM "DimPromotion";', db_etl)

dim_sales_territory = pd.read_sql_query('SELECT * FROM "DimSalesTerritory";', db_etl)


dim_dates = pd.read_sql_query('SELECT * FROM "DimDate";', db_etl)


dim_currency = pd.read_sql_query('SELECT * FROM "DimCurrency";', db_etl)

currency_rate = pd.read_sql_query('SELECT CurrencyRateID, ToCurrencyCode FROM [Sales].[CurrencyRate]', db_op)

sales_order = sales_order.merge(currency_rate, on="CurrencyRateID", how="left")
sales_order["ToCurrencyCode"] = sales_order["ToCurrencyCode"].apply(lambda x: 'USD' if pd.isna(x) else x )


sales_order = sales_order.merge(dim_currency, left_on="ToCurrencyCode", right_on="CurrencyAlternateKey")

sales_order = sales_order.drop(columns=["ToCurrencyCode", "CurrencyAlternateKey", "CurrencyRateID"])

sales_order["OrderDate"] = sales_order["OrderDate"].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
sales_order["DueDate"] = sales_order["DueDate"].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
sales_order["ShipDate"] = sales_order["ShipDate"].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))


#####Transform##########################

sales_order = sales_order.merge(dim_product, left_on="ProductID", right_on="ProductID")


sales_order = sales_order.merge(dim_customer, left_on="CustomerID", right_on="CustomerKey")

sales_order = sales_order.merge(dim_promotion, left_on="SpecialOfferID", right_on="PromotionKey")

sales_order = sales_order.merge(dim_sales_territory, left_on="TerritoryID", right_on="SalesTerritoryKey")

sales_order = sales_order.merge(dim_dates.rename(columns={'DateKey':'OrderDateKey'}), left_on="OrderDate", right_on="FullDateAlternateKey")
sales_order = sales_order.merge(dim_dates.rename(columns={'DateKey':'DueDateKey'}), left_on="DueDate", right_on="FullDateAlternateKey")
sales_order = sales_order.merge(dim_dates.rename(columns={'DateKey':'ShipDateKey'}), left_on="ShipDate", right_on="FullDateAlternateKey")

sales_order.info(verbose=True)

sales_order = sales_order.rename(columns={
    "OrderQty":"OrderQuantity",
    "DiscountPct":"UnitPriceDiscountPct",
    "ToCurrencyCode":"CurrencyKey",
    "PurchaseOrderNumber":"CustomerPONumber",
})

quantity_unitprice = zip(list(sales_order["OrderQuantity"]), list(sales_order["UnitPrice"]))
sales_order = sales_order.assign(ExtendedAmount=[x*y for x, y in quantity_unitprice])


discountpct_discountamount = zip(list(sales_order["ExtendedAmount"]), list(sales_order["UnitPriceDiscountPct"]))
sales_order = sales_order.assign(DiscountAmount=[x*y for x, y in discountpct_discountamount])

total_product_cost = zip(list(sales_order["OrderQuantity"]), list(sales_order["ProductStandardCost"]))
sales_order = sales_order.assign(TotalProductCost=[x*y for x, y in total_product_cost])

sales_order = sales_order.assign(SalesAmount=list(sales_order["ExtendedAmount"]))

sales_order['SalesOrderLineNumber'] = sales_order.groupby(['SalesOrderID']).cumcount() + 1


result = sales_order[[
                    "ProductKey",
                    "OrderDateKey",
                      "DueDateKey",
                      "ShipDateKey",
                      "CustomerKey", "PromotionKey",
                      "CurrencyKey",
                      "SalesTerritoryKey",
                      "SalesOrderNumber",
                      "SalesOrderLineNumber",
                      "RevisionNumber",
                      "OrderQuantity",
                      "UnitPrice",
                      "ExtendedAmount",
                      "UnitPriceDiscountPct",
                      "DiscountAmount",
                      "ProductStandardCost",
                      "TotalProductCost",
                      "SalesAmount",
                      "TaxAmt",
                      "Freight",
                      "CarrierTrackingNumber",
                      "CustomerPONumber", 
                      "OrderDate", "DueDate", "ShipDate"
                     ]]


db_op, db_etl = connect_databases()
result.to_sql('FactInternetSales', db_etl, if_exists='replace', index=False)




'''