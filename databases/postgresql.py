

from datetime import datetime
import json
import psycopg2
import psycopg2.extras
from psycopg2 import sql
import traceback

from google.protobuf.json_format import MessageToJson, Parse, MessageToDict

from utils.sqlQueries import *
from utils.getData import *


DB_CON = {
    "hostname": "127.0.0.1",
    "username": "postgres",
    "password": "postgres",
    "database": "qrldata",
    "port": "5432",
}




class PostgresDB(object):
    
    
    def connect():
        DB_CONNECTION = psycopg2.connect(host=DB_CON["hostname"], user=DB_CON["username"], password=DB_CON["password"], port=DB_CON["port"], dbname=DB_CON["database"])
        return DB_CONNECTION

    def insertData(table, data):
        DB_CONNECTION = PostgresDB.connect()
        with DB_CONNECTION.cursor() as cur:
            try: 
                
                if data[0] != None:   # why is data[0] sometimes none ??
                    columns = list(data[0].keys())  # ugly coding [0], but it works > data is sent in a list otherwhise got error querey_string, dont know why 
                    values  = [tuple(value.values()) for value in data]
                    query_string = sql.SQL(SQL_INSERT_DATA_DYNAMIC).format(
                        table = sql.Identifier(table),
                        columns= sql.SQL(', ').join(map(sql.Identifier, columns)),
                        values= sql.SQL(', ').join(sql.Placeholder()*len(values)),
                        ).as_string(cur)
                    cur.execute(query_string, values)
                    DB_CONNECTION.commit()
            
            except psycopg2.errors.UniqueViolation:
                print('already got that one')
                pass
                        
            except (psycopg2.Error, psycopg2.DatabaseError , psycopg2.DataError) as e:   
                print(e)
                print(traceback.format_exc())
                eData={}
                eData["date"], eData["location"], eData["traceback"], eData["data"] =  datetime.now(), 'insertData', str(traceback.format_exc()), str(data)
                eData["error"], eData["blocknumber"] = str(e), ""            
                table, data = "errors" , [eData]
                PostgresDB.insertData(table, data) 
                print('Psycopg2 error while inserting data into Database')
                raise 
        
            except Exception as e:  
                print(e)
                eData={}
                eData["date"], eData["location"], eData["traceback"], eData["data"] =  datetime.now(), 'insertData', str(traceback.format_exc()), str(data)
                eData["error"], eData["blocknumber"] =  str(e), ""            
                table, data = "errors" , [eData]
                PostgresDB.insertData(table, data)      
                print('Psycopg2 error while inserting data into Database')
                raise 
    
            finally:
                cur.close()
    
    
    def updateData(table, data, idkey, idval ):
        DB_CONNECTION = PostgresDB.connect()
        with DB_CONNECTION.cursor() as cur:
            try:                        
                query_string = sql.SQL(SQL_UPDATE_DATA_DYNAMIC).format(
                    table = sql.Identifier(table),
                    data=sql.SQL(', ').join(sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in data.keys()),
                    idkey = sql.Identifier(idkey),
                    idval = sql.Placeholder('idval')
                    ).as_string(cur)
                data.update(idval=idval)

                cur.execute(query_string, data)
                DB_CONNECTION.commit()
                
            except (psycopg2.Error, psycopg2.DatabaseError , psycopg2.DataError) as e:
                print(e)
                print(data)
                print('Psycopg2 while updating data from Database') 
                raise
            except Exception as e:
                print(e)
                print('Exception while updating data from Database') 
                raise
            finally:
                cur.close()

    
    def createDB():
        try:
            SERVER_CONNECTION = psycopg2.connect(host=DB_CON["hostname"], user=DB_CON["username"], password=DB_CON["password"], port=DB_CON["port"])
            SERVER_CONNECTION.autocommit = True   
            with SERVER_CONNECTION.cursor() as cur:            
                cur.execute(" ".join([SQL_CREATE_DATABASE, DB_CON["database"]]))
                print('Database Created')
                PostgresDB.createTables()
            
        except (psycopg2.Error, psycopg2.DatabaseError , psycopg2.DataError) as e:
            print(e)
            print('Psycopg2 error while creating Database') 
            raise        
        except Exception as e:
            print(e)
            print('Error while creating Database')    
            raise
        finally:
            cur.close()

    
    def createTables():
        DB_CONNECTION = PostgresDB.connect()
        with DB_CONNECTION.cursor() as cur:
            try:
                for query in SQL_CREATE_TABLES_RAW :
                    cur.execute(query) 
                
                for query in SQL_CREATE_TABLES_AGG:
                    cur.execute(query)   
                
                DB_CONNECTION.commit()    
                print('Tables Created')        
            except (psycopg2.Error, psycopg2.DatabaseError , psycopg2.DataError) as e:
                print(e)
                print(data)
                print('Psycopg2 error while Creating Tables') 
                raise
            except Exception as e:
                print(e)
                print('Error while Creating Tables') 
                raise
            finally:
                cur.close()             
        
    
    def dropDB():
        SERVER_CONNECTION = psycopg2.connect(host=DB_CON["hostname"], user=DB_CON["username"], password=DB_CON["password"], port=DB_CON["port"])
        with SERVER_CONNECTION.cursor() as cur:
            try:
                cur.execute(SQL_SELECT_DATABASE) 
                exists = cur.fetchone()
                
                if exists:
                    cur.execute(SQL_STOP_DATABASE_ACTIVITY)    
                    cur.execute(SQL_DROP_DATABASE) 
                    SERVER_CONNECTION.commit() 
                    print('Database Dropped')
                else:
                    print('Database Doenst Exists')
                    pass
            except (psycopg2.Error, psycopg2.DatabaseError , psycopg2.DataError) as e:
                print(e)
                print(data)
                print('Psycopg2 error while dropping Database')
                raise                            
            except Exception as e:
                print(e)
                print('Error while dropping Database')
                raise  
            finally:
                cur.close()        

    
    def dropTables():
        DB_CONNECTION = PostgresDB.connect()
        with DB_CONNECTION.cursor() as cur:
            try:
                cur.execute(SQL_SELECT_TABLES_POSTGRES)
                rows = cur.fetchall()
                
                for row in rows:
                    print(" ".join(["Dropping table:" , row[1]]))
                    cur.execute(" ".join(["drop table" , row[1] , "cascade"]))
                
                DB_CONNECTION.commit() 
            
            except (psycopg2.Error, psycopg2.DatabaseError , psycopg2.DataError) as e:
                print(e)
                print(data)
                print('Psycopg2 error while dropping Tables')
                raise                    
            except Exception as e:
                print(e)
                print('Error while dropping Tables')
            finally:
                cur.close()

    
    def truncateTables():
        DB_CONNECTION = PostgresDB.connect()
        with DB_CONNECTION.cursor() as cur:
            try:
                cur.execute(SQL_SELECT_TABLES_POSTGRES)
                rows = cur.fetchall()
                
                for row in rows:
                    print("".join(["Truncate table:" , row[1]]))
                    cur.execute("".join(["truncate table:" , row[1], "cascade"]))

                DB_CONNECTION.commit() 

            except (psycopg2.Error, psycopg2.DatabaseError , psycopg2.DataError) as e:
                print(e)
                print(data)
                print('Psycopg2 error while truncate Tables')
                raise                   
            except Exception as e:
                print(e)
                print('Error while Truncate Tables')
                raise
            finally:
                cur.close()

    
    def recreateTables():
        PostgresDB.dropTables()
        PostgresDB.createTables()
        
    
    def getBlockData(source):
        DB_CONNECTION = PostgresDB.connect()
        with DB_CONNECTION.cursor() as cur:
            try:                
                blockheightBC = getData.getBlockHeight(source)                
                cur.execute(SQL_GET_BLOCKHEIGHT_IN_DB_POSTGRES)     
                blockHeightInDB = cur.fetchone()
                if blockHeightInDB != None: 
                    blockHeightInDB = int(blockHeightInDB[0]) # check latest block in database
                else:
                    blockHeightInDB = 0
        
                for i in range(blockHeightInDB , blockheightBC+1):                       
                    print(" ".join(["Parsing block" , str(i) , "/" , str(blockheightBC)]))  
                    
                    blockData = getData.getBlockData(i, source)                
                    table, data, transactions = 'blocks', [blockData], blockData["transactions"]
                    del blockData["transactions"] 
    
                    PostgresDB.insertData(table, data)
                        
                    for t in transactions:
                        PostgresDB.getTransactionData(t, source, blockData["block_number"], blockData["timestamp"] )
                            
            except Exception as e:            
                print(e)
                print('Error while getting block data')
            
                eData={}
                eData["date"], eData["location"], eData["traceback"] =  datetime.now(), 'getBlockData', str(traceback.format_exc())
                eData["error"], eData["blocknumber"] = str(e), ""  
                table, data = "errors" , [eData]
                PostgresDB.insertData(table, data)      
                raise

                    
            finally:
                cur.close()   
                        
    
    def getTransactionData(t, source, block_number, timestamp):
        DB_CONNECTION = PostgresDB.connect()
        with DB_CONNECTION.cursor() as cur:
            try:
                transactionProcessed = False
                tData = {}
                if "masterAddr" in t:
                    PostgresDB.getAddressData(source, t["masterAddr"], timestamp, block_number)
                
                
                if "coinbase" in t:        
                    table, data = 'transactions_coinbase', [getData.getTransactionDataCoinbase(t, block_number, timestamp)]
                    PostgresDB.getAddressData(source, t["coinbase"]["addrTo"], timestamp, block_number)
                    PostgresDB.insertData(table, data)
                    transactionProcessed = True
                    
                if "transfer" in t:       
                    if "addrsTo" in t["transfer"]:
                        addrs_to = t["transfer"]["addrsTo"]
                        amounts = t["transfer"]["amounts"]
                        transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i]} for i in range(len(addrs_to))]

                        for transfer in transfers:                                             
                            table, data = 'transactions_transfer', [getData.getTransactionDataTransfer(t, block_number, timestamp, transfer)] 
                            PostgresDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                            PostgresDB.insertData(table, data)
                        transactionProcessed = True

                if "token" in t:
                    table, data = 'transactions_token', [getData.getTransactionDataToken(t, block_number, timestamp)] 
                    PostgresDB.getAddressData(source, t["token"]["owner"] , timestamp, block_number)
                    PostgresDB.insertData(table, data)
                    transactionProcessed = True

                if "message" in t:
                    table, data = 'transactions_message', [getData.getTransactionDataMessage(t, block_number, timestamp)] 
                    PostgresDB.insertData(table, data)                              
                    transactionProcessed = True

                if "latticePk" in t:                                                    
                    table, data = "transactions_latticePk", [getData.getTransactionDataLatticePk(t, block_number, timestamp)]                 
                    PostgresDB.insertData(table, data)            
                    transactionProcessed = True

                if "slave" in t:
                    if "slavePks" in t["slave"]:
                        slave_pks = t["slave"]["slavePks"]
                        access_types = t["slave"]["accessTypes"]
                        transfers = [{"slave_pk" : slave_pks[i], "access_type":access_types[i]} for i in range(len(slave_pks))]
                        
                        for transfer in transfers:
                            table, data = "transactions_slave", [getData.getTransactionDataSlave(t, block_number, timestamp, transfer)]  
                            PostgresDB.insertData(table, data)
                        transactionProcessed = True
                                                
                if "transferToken" in t:
                    if "addrsTo" in t["transferToken"]:
                        addrs_to = t["transferToken"]["addrsTo"]
                        amounts = t["transferToken"]["amounts"]
                        token_txhash = t["transferToken"]["tokenTxhash"]
                        transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i], "token_txhash":token_txhash} for i in range(len(addrs_to))]

                        for transfer in transfers:                        
                            table, data = "transactions_transfertoken", [getData.getTransactionDataTransferToken(t, block_number, timestamp, transfer)] 
                            PostgresDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                            PostgresDB.insertData(table, data)
                        transactionProcessed = True
                                                    
                if not transactionProcessed:
                    table, data = "transactions_others", [getData.getTransactionDataOthers(t, block_number, timestamp)]                 
                    PostgresDB.insertData(table, data)

                                                          
            except Exception as e:            
                print(e)
                print('Error while getting transaction data')
                eData={}
                eData["date"], eData["location"], eData["traceback"] =  datetime.now(), 'getTransactionData', str(traceback.format_exc())
                eData["error"], eData["blocknumber"] =  str(e), ""  
                table, data = "errors" , [eData]
                PostgresDB.insertData(table, data)                
                raise

            finally:
                cur.close()          

    
    def getAddressData(source, b64Addr, timeStamp, block_number):
        DB_CONNECTION = PostgresDB.connect() 
        with DB_CONNECTION.cursor() as cur:        
            
            addressData = getData.getAddressData(source, b64Addr, timeStamp)
                                        
            try:            
                cur.execute(SQL_SELECT_ADDRESS_POSTGRES, (addressData["address"],)) 
                dup_check = len(cur.fetchall())
                
                if dup_check == 0:                    
                    table, data   = 'addresses', [addressData]
                    PostgresDB.insertData(table, data)
                else:                    
                    table, idkey, idval, data  = 'addresses', 'address', addressData["address"], addressData 
                    del addressData["address"]  
                    del addressData["first_seen"]
                    PostgresDB.updateData(table, data, idkey, idval)

            except Exception as e:                    
                print(e)
                print('Error while getting address data')
                eData={}
                eData["date"], eData["location"], eData["traceback"], eData["data"] = datetime.now(), 'getAddressData', str(traceback.format_exc()), b64Addr 
                eData["error"], eData["blocknumber"] = str(e), block_number
                table, data = "errors" , [eData]
                PostgresDB.insertData(table, data)                   
                raise
            
            finally:
                cur.close() 


       

PostgresDB()
