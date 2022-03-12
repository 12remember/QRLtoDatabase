# QRLtoDB

The aim for this project is to make QRL blockchain data accessible in popular databases. So data can be analyzed for projects like Quantascan


## In progress
- Currently Postgres, SQL and Mongodb are working
- Data completeness needs to be checked
- Script still needs a long run to check for errors
- Neo4j is not working yet, but in the pipeline



# Getting started


## Installing
1. Run a QRL node locally, for more information see [How to run a QRL node](https://docs.theqrl.org/node/QRLnode/ "How to run a QRL node") 

2. Install database and utilities
  - Postgres, SQL, Mongodb on Windows SUBsystem [Getting started with databases on windows subsystem for linux](https://docs.microsoft.com/en-us/windows/wsl/tutorials/wsl-database "tutorial")
  - Neo4J [Get Neo4j]( https://neo4j.com/download/ "Download Neo4j")

3. install utilities 
  - Postgres [pgadmin](https://www.pgadmin.org/download "pgAdmin download")
  - Sqlite [sqlitebrowser](https://sqlitebrowser.org/ "sqlite browser")


## Running
1. inside directory run:
  - to create env
    ```
    pip install pipenv
    pipenv install 
    pipenv shell
    ```
  - Create database and tables/Collections 
    ``` 
    python QRLtoDB.py --create_db -db (your chosen database)
    python QRLtoDB.py --create_tables -db (your chosen database)
    ```
  - Get the data
    ```
    python QRLtoDB.py --get_data -db (your chosen database) -s ~/.qrl/data/state/
    ```


## Arguments 

```
-h, --help            show this help message and exit
--create_db           Create Database and Tables/Collections
--create_tables       Create Tables/Collections
--drop_db             DROPS the entire database !!
--drop_tables         DROPS all Tables/Collections
--truncate_tables     Empty all Tables/Collections
--recreate_tables     Recreate all Tables/Collections
--get_data            Gets blockchain data and stores it in the given Database

-db, --database Choose the database {sqlite,postgres,neo4j,mongodb} 
-s, --source Need to select source (QRL state folder, ex: /home/ubuntu/.qrl/data/state)
-n, --node Choose to run on while the node is offline {offline}  ! Warning 

  ```
## Warning
Before using -n offline. the node NEEDS to be stopt. otherwise data is corrupted!

# Special thanks
0xFF (https://github.com/0xFF0) for 
- Creating an example for getting blockchain data to sqlite db [QRLtoSQLite](https://github.com/0xFF0/QRLtoSQLite "QRL to SQlite") 
- Creating the script so node can run simultaneously with this script  [QRL bootstrap ](https://github.com/0xFF0/QRL_bootstrap "QRL bootstrap") 



# Needing help with 
Getting the following data from plyvel.DB 
- BlockDataPoint
- BlockExtended

  See for more information [The qrl API](https://api.theqrl.org/?python#block "The QRL API") 


# More information

[Quantascan.io](https://www.quantascan.io "Quantascan.io")

[The Quantum resistant Ledger](https://www.theqrl.org/ "The QRL homepage")

[Discord Chat](https://discord.gg/RcR9WzX "Discord Chat") @12remember



## License: MIT


