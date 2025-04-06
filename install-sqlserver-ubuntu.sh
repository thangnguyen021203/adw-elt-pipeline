#!/bin/bash

set -e

echo "Step 1: Import Microsoft GPG key (fallback to legacy trusted.gpg.d)..."
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc

echo "Step 2: Register SQL Server 2022 repository..."
sudo rm -f /etc/apt/sources.list.d/mssql-server-2022.list
curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/mssql-server-2022.list | sudo tee /etc/apt/sources.list.d/mssql-server-2022.list

echo "Step 3: Update package lists..."
sudo apt-get update

echo "Step 4: Install SQL Server..."
sudo apt-get install -y mssql-server

echo "Step 5: Run mssql-conf setup..."
sudo /opt/mssql/bin/mssql-conf setup

echo "Step 6: Verify SQL Server service is running..."
sudo systemctl status mssql-server --no-pager

echo "Step 7: Register Microsoft repo for tools..."
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

echo "Step 8: Import Microsoft GPG key again for tools (legacy fallback)..."
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc

echo "Step 9: Update packages and install command-line tools..."
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev

echo "Step 10: Add tools to PATH (bashrc and bash_profile)..."
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bash_profile
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bash_profile || true
source ~/.bashrc || true

echo "Installation and configuration completed."
echo "You can now connect using: sqlcmd -S localhost -U sa -P 'YourPassword'"
