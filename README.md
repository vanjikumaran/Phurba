# Phurba
Project Phurba is aimed to provide the tool set to plan and execute the migration on WSO2 APIM and IS product stack. 

Main focus of this project is to define the synchronization process of data while the migration is done using A/B  deployment pattern. Since the database synchronization is unidirectional, we can identify the source database and target database for a particular synchronization task. The database used by production deployment will be the source and the inactive database will be the target.

This will contain two parts mainly,

* Database triggers and audit log tables

Each table that is required to be synchronized has a corresponding table which acts as an audit log in the source database. Triggers will keep track of the rows that are created or updated, and add a row to sync audit log table, which consist of the primary key of actual data table and auto-incremented sync-id. Table name would be [DATABAE_TABLE_NAME]_SYNC

* External Java program for periodic synchronization

An external task will be responsible for reading the database table with sync log of the source database and write them into the target database periodically. Program will also keep track of the status of the synchronization using a database table in target database, where it will be used to resume synchronization where it left off. Database table name would be [DATABAE_TABLE_NAME]_VERSION