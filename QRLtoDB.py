
"""

    QRL Blockchain to DB
	v1.0

"""

__author__ = ['12remember']
__co_author__ = ['0xff0']
__version__ = "1.0"
__date__ = '2022.01.29'


import argparse
import sys
import subprocess
import os

from databases.postgresql import PostgresDB
from databases.sqlite import SqliteDB
from databases.mongodb import MongoDB
from databases.neo4j import Neo4jDB

          
  
def createDB(database):
    if database == 'postgres':
        PostgresDB.createDB()
    elif database == 'sqlite':
        SqliteDB.createTables()
        print('also created the tables')
    elif database == 'mongodb':
        print('database will be created when writing the data')
    elif database == 'neo4j':
        Neo4jDB.createDB()
    else:
        print('db unknown')

def createTables(database):
    if database == 'postgres':
        PostgresDB.createTables()
    elif database == 'sqlite':
        SqliteDB.createTables()
    elif database == 'mongodb':
        print('collections will be created when writing the data')
    elif database == 'neo4j':
        print('collections will be created when writing the data')
    else:
        print('db unknown')

        
def dropDB(database):
    if database == 'postgres':
        PostgresDB.dropDB()
    elif database == 'sqlite':
        print('Sqlite doenst support drop Database, Manually delete the local database inside the directory')
    elif database == 'mongodb':
        MongoDB.dropDB()
    elif database == 'neo4j':
        Neo4jDB.dropDB()
    else:
        print('db unknown')

    
def dropTables(database):
    if database == 'postgres':
        PostgresDB.dropTables()
    elif database == 'sqlite':
        SqliteDB.dropTables()
    elif database == 'mongodb':
        MongoDB.dropCollections()
    elif database == 'neo4j':
        Neo4jDB.dropCollections()
    else:
        print('db unknown')


def truncateTables(database):
    if database == 'postgres':
        PostgresDB.truncateTables()
    elif database == 'sqlite':
        SqliteDB.truncateTables()
    elif database == 'mongodb':
        MongoDB.truncateCollections()
    elif database == 'neo4j':
        Neo4jDB.dropCollections()
    else:
        print('db unknown')


def recreateTables(database):
    if database == 'postgres':
        PostgresDB.recreateTables()
    elif database == 'sqlite':
        SqliteDB.recreateTables()
    elif database == 'mongodb':
        MongoDB.dropCollections()
    elif database == 'neo4j':
        Neo4jDB.dropCollections()
    else:
        print('db unknown')


def getData(source, database):
    if database == 'postgres':
        PostgresDB.getBlockData(source) 
    elif database == 'sqlite':
        SqliteDB.getBlockData(source)
    elif database == 'mongodb':
        MongoDB.getBlockData(source)
    elif database == 'neo4j':
        print('not supported yet') #Neo4jDB.getBlockData(source)
    else:
        print('db unknown')
        
    
def ask_confirm(msg="Are you sure?", yes=None, no=None):
    if yes is None:
        yes = ["yes"]
    if no is None:
        no = ["no"]
    if isinstance(yes, str):
        yes = [yes]
    if isinstance(no, str):
        no = [no]

    answer = ""
    while answer not in yes and answer not in no:
        answer = input(msg + " [{}/{}]".format(yes[0], no[0]))
    return (True if answer in yes else False)
    

if __name__ == "__main__":
    desc = "QRL Blockchain to Database " + __version__
    parser = argparse.ArgumentParser(  
                                    usage='use "%(prog)s --help" for more information',
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
                                    description= ''' \
                                    ''' + desc +
                                     
                                                                     
                                '''
                            \
                                \
                                \
                                     
                                        Get QRL Blockchain data!
                                    --------------------------------
                                Directly gets data from the qrl blockchain
                                Saved in db of choice (sqlite, postgres, neo4j, mongodb)
                                        
                                         
\
\
\
    Special Thanks to 0xFF (https://github.com/0xFF0) for 
    - Creating an example for getting blockchain data to sqlite db
    - Creating the script so node can run simultaneously with this script  
\
\
             

'''
                                                                    
                                    )
    
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--create_db", help="Create Database and Tables/Collections",  action='store_true')
    action.add_argument("--create_tables", help="Create Tables/Collections",  action='store_true')
    action.add_argument("--drop_db", help="DROPS the entire database !!",  action='store_true')
    action.add_argument("--drop_tables", help="DROPS all Tables/Collections", action='store_true')
    action.add_argument("--truncate_tables", help="Empty all Tables/Collections", action='store_true')
    action.add_argument("--recreate_tables", help="Recreate all Tables/Collections", action='store_true')
    action.add_argument("--get_data", help="Gets blockchain data and stores it in the given Database",  action='store_true')

    parser.add_argument("-db", "--database", help="Choose the database", choices=['sqlite', 'postgres', 'neo4j', 'mongodb'], type=str)
    parser.add_argument("-s", "--source", help="Need to select source (QRL state folder, example: ~/.qrl/data/state/)")
    parser.add_argument("-n", "--node", help="Choose to run on while the node is offline", choices=['offline'], type=str)
    
    
    # Read arguments from the command line
    args = parser.parse_args()

    if len(sys.argv)<=1:
        parser.print_help()
        sys.exit(1)

    
    if args.create_db:
        if args.database is None:
            parser.error('Select a database, example: -db sqlite')
        else:     
            createDB(args.database) 


    if args.create_tables:
        if args.database is None:
            parser.error('Select a database, example: -db sqlite')        
        else:             
            createTables(args.database)


    if args.drop_db :
        if args.database is None:
            parser.error('Select a database, example: -db sqlite')
        elif ask_confirm("Are you sure you want to DROP the DATABASE ? All data will be removed !!!!"):        
            dropDB(args.database)              
        else:
            pass  


    if args.drop_tables:
        if args.database is None:
            parser.error('Select a database, example: -db sqlite')
        elif ask_confirm("Are you sure you want to DROP ALL tables? All data will be removed !!!!"):        
            dropTables(args.database)              
        else:
            pass  


    if args.truncate_tables:
        if args.database is None:
            parser.error('Select a database, example: -db sqlite')
        elif ask_confirm("Are you sure you want to truncate ALL tables? All data will be removed !!!!"):        
            truncateTables(args.database)              
        else:
            pass    

    
    if args.recreate_tables:
        if args.database is None:
            parser.error('Select a database, example: -db sqlite')
        elif ask_confirm("Are you sure you want to RECREATE ALL tables? All data will be removed !!!!"):        
            recreateTables(args.database)              
        else:
            pass              
            
    
    if args.get_data:
        if args.database is None:
            parser.error('Select a database, example: -db sqlite')            
        elif args.source is None:
            parser.error('Select source (qrl state folder), example: -s ~/.qrl/data/state/')                         
        elif args.node == 'offline':
            if ask_confirm("Confirm that the Node is NOT running !"):
                getData(args.source, args.database)
                        
        else:      
            subprocess.call(['bash', './utils/bootstrap/CreateQRLBootstrap.sh', args.source])
            path = os.path.expanduser("~/qrl_bootstrap/Mainnet/state/")  
            getData(path, args.database)
               
        
        



