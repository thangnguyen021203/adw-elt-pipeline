
{{ config(materialized='view') }}

SELECT 
    "SalesOrderID" AS sales_order_id,
    "RevisionNumber" AS revision_number,
    "OrderDate" AS order_date,
    "DueDate" AS due_date,
    "ShipDate" AS ship_date,
    "Status" AS status,
    "OnlineOrderFlag" AS online_order_flag,
    "SalesOrderNumber" AS sales_order_number,
    "PurchaseOrderNumber" AS purchase_order_number,
    "AccountNumber" AS account_number,
    "CustomerID" AS customer_id,
    "SalesPersonID" AS salesperson_id,
    "TerritoryID" AS territory_id,
    "BillToAddressID" AS bill_to_address_id,
    "ShipToAddressID" AS ship_to_address_id,
    "ShipMethodID" AS ship_method_id,
    "CreditCardID" AS credit_card_id,
    "CreditCardApprovalCode" AS credit_card_approval_code,
    "CurrencyRateID" AS currency_rate_id,
    "SubTotal" AS sub_total,
    "TaxAmt" AS tax_amt,
    "Freight" AS freight,
    "TotalDue" AS total_due,
    "Comment" AS comment,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_SalesOrderHeader') }}
