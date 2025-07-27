@echo off
sqlcmd -S 103.141.144.46 -d Hallure_datawarehouse -U hallure_dev -P 123456 -i "C:\PHANTICHDULIEU_PANCAKE\dimdate.sql"

