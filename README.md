# adw-elt-pipeline

Execute install-sqlserver-ubuntu.sh


systemctl start mssql-server
systemctl status mssql-server
journalctl -u mssql-server --no-pager


mkdir -p /var/opt/mssql/backups
chown mssql:mssql /var/opt/mssql/backups
chmod 755 /var/opt/mssql/backups

cp CompanyX.bak /var/opt/mssql/backups/

sqlcmd -S localhost -U sa -P 'yourStrong(!)Password' -Q "RESTORE FILELISTONLY FROM DISK = '/var/opt/mssql/backups/CompanyX.bak'"
sqlcmd -S localhost -U sa -P 'yourStrong(!)Password' -Q "
RESTORE DATABASE AdventureWorks2022
FROM DISK = N'/var/opt/mssql/backups/CompanyX.bak'
WITH MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks2022.mdf',
     MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks2022_log.ldf',
     REPLACE;"

sudo ACCEPT_EULA=Y apt install -y msodbcsql18
odbcinst -q -d

source /opt/conda/etc/profile.d/conda.sh
conda activate adw_env

dbt run --models dim_date
dbt run --models dim_product
dbt run --models dim_store
dbt run --models dim_customer
dbt run --models dim_promotion
dbt run --models dim_currency
dbt run --models sales_territory

dbt run --models fact_currency_rate
dbt run --models fact_internet_sales
dbt run --models fact_reseller_sales
dbt run --models fact_store_performance
dbt run --models fact_customer_value

# Business Intelligence
**Dashboard 1: General Analysis**
![Genaral Analysis](images\1.png "General Analysis")

**Dashboard 2: Personal Customer Analysis**
![Personal Customer Analysis](images\2.png "Personal Customer Analysis")

**Dashboard 3: Business Customer Analysis**
![Business Customer Analysis](images\3.png "Business Customer Analysis")

**Dashboard 4: Customer Segmentation**
![Customer Segmentation](images\4.png "Customer Segmentation")
