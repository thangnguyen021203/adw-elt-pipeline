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
