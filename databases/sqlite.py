

#import base64
#import binascii
from datetime import datetime
import json
import traceback

import sqlite3

from google.protobuf.json_format import MessageToJson, Parse, MessageToDict

from utils.sqlQueries import *
from utils.getData import *



DB_CONNECTION = sqlite3.connect("QRL_BC_DATA.sqlite3")


class SqliteDB(object):

        def insertData(table, data):
            cur = DB_CONNECTION.cursor()
            try:                         
                if 'initial_balances' in data:
                    del data["initial_balances"]    #sqlite cant handle this value

                columns = list(data.keys())
                values  = list(data.values())  
                                                   
                query_string = ' '.join(['INSERT INTO ', table,  ' (', ', '.join(columns), ') VALUES (', ', '.join(['?'] * len(columns)), ')'])   
                cur.execute(query_string, values)
                DB_CONNECTION.commit()
    
            except sqlite3.IntegrityError:
                print('already got that one')
                pass

                                
            except Exception as e:  
                print(e)
                print(traceback.format_exc())
                eData={}
                dataKeys = [k for k in data[0].keys()]
                dataKeyType = [type(k) for k in data[0].keys()]
                eData["date"], eData["location"], eData["traceback"], eData["data"] =  datetime.now(), 'insertData', str(traceback.format_exc()), str(data)
                eData["data_keys"], eData["data_key_type"], eData["error"], eData["blocknumber"] =  str(dataKeys), str(dataKeyType), str(e), "" 
                table, data = "errors" , eData
                SqliteDB.insertData(table, data)      
                print('Error while inserting data into Database')
                raise     
            finally:
                cur.close()
        
        
        def updateData(table, data, idkey, idval ):
            cur = DB_CONNECTION.cursor()
            try:
                values  = list(data.values())
                values.append(idval)                       
                columnList = []
                for k, v in data.items():
                    test = " ".join([k, "= ?"])
                    columnList.append(test)
                query_string = ' '.join(['UPDATE', table, 'SET', ', '.join(columnList), 'WHERE', idkey , '= ?'])
                cur.execute(query_string, values)
                DB_CONNECTION.commit()
                
            except Exception as e:
                print(e)
                eData={}
                eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'updateData', str(traceback.format_exc()), str(data) , str(e), " "
                table, data = "errors" , eData
                SqliteDB.insertData(table, data)      
                print('Error while updating data into Database')
                raise 
            finally:
                cur.close()
    
            
        def createTables():
            cur = DB_CONNECTION.cursor()
            try:

                for query in SQL_CREATE_TABLES_RAW :
                    cur.execute(query) 
                
                for query in SQL_CREATE_TABLES_AGG:
                    cur.execute(query)     
                
                DB_CONNECTION.commit()    
                print('Tables Created')        

            except Exception as e:
                print(e)
                print('Error while Creating Tables') 
                raise
            finally:
                cur.close()             
            
            
        
        def dropTables():
            cur = DB_CONNECTION.cursor()
            try:
                cur.execute(SQL_SELECT_TABLES_SQLITE)
                rows = cur.fetchall()
                rowList = [i[0] for i in rows]
                
                for row in rowList:
                    print(" ".join(["Dropping table:" , row]))
                    cur.execute(" ".join(["DROP TABLE" , row]))
                
                DB_CONNECTION.commit() 
                          
            except Exception as e:
                print(e)
                print('Error while dropping Tables')
            finally:
                cur.close()
    
        
        def truncateTables():
            cur = DB_CONNECTION.cursor()
            try:
                cur.execute(SQL_SELECT_TABLES_SQLITE)
                rows = cur.fetchall()
                rowList = [i[0] for i in rows]
                
                for row in rowList:
                    print(" ".join(["DELETE table:" , row]))
                    cur.execute(" ".join(["DELETE FROM" , row]))
                DB_CONNECTION.commit() 
            except Exception as e:
                print(e)
                print('Error while DELETING Tables')
                raise
            finally:
                cur.close()
    
        
        def recreateTables():
            SqliteDB.dropTables()
            SqliteDB.createTables()
            
        
        def getBlockData(source):
            cur = DB_CONNECTION.cursor()
            try:                
                blockheightBC = getData.getBlockHeight(source)                
                cur.execute(SQL_GET_BLOCKHEIGHT_IN_DB_SQLITE)     
                blockHeightInDB = cur.fetchone()
                if blockHeightInDB != None: 
                    blockHeightInDB = int(blockHeightInDB[0]) # check latest block in database
                else:
                    blockHeightInDB = 0
        
                for i in range(blockHeightInDB , blockheightBC+1):                       
                    print(" ".join(["Parsing block" , str(i) , "/" , str(blockheightBC)]))  
                    
                    blockData = getData.getBlockData(i, source)                
                    table, data, transactions = 'blocks', blockData, blockData["transactions"]
                    del blockData["transactions"] 
    
                    SqliteDB.insertData(table, data)
                        
                    for t in transactions:
                        SqliteDB.getTransactionData(t, source, blockData["block_number"], blockData["timestamp"] )
                            
            except Exception as e:            
                print(e)
                print('Error while getting block data')
            
                eData={}
                eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getBlockData', str(traceback.format_exc()), " " , str(e), " "
                table, data = "errors" , eData
                SqliteDB.insertData(table, data)      
                raise

                    
            finally:
                cur.close()   
                            
        
        def getTransactionData(t, source, block_number, timestamp):
            cur = DB_CONNECTION.cursor()
            try:
                transactionProcessed = False
                tData = {}
                if "masterAddr" in t:
                    SqliteDB.getAddressData(source, t["masterAddr"], timestamp, block_number)
                
                
                if "coinbase" in t:        
                    table, data = 'transactions_coinbase', getData.getTransactionDataCoinbase(t, block_number, timestamp)
                    SqliteDB.getAddressData(source, t["coinbase"]["addrTo"], timestamp, block_number)
                    SqliteDB.insertData(table, data)
                    transactionProcessed = True
                    
                if "transfer" in t:       
                    if "addrsTo" in t["transfer"]:
                        addrs_to = t["transfer"]["addrsTo"]
                        amounts = t["transfer"]["amounts"]
                        transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i]} for i in range(len(addrs_to))]

                        for transfer in transfers:                                             
                            table, data = 'transactions_transfer', getData.getTransactionDataTransfer(t, block_number, timestamp, transfer) 
                            SqliteDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                            SqliteDB.insertData(table, data)
                        transactionProcessed = True

                if "token" in t:
                    table, data = 'transactions_token', getData.getTransactionDataToken(t, block_number, timestamp) 
                    SqliteDB.getAddressData(source, t["token"]["owner"] , timestamp, block_number)
                    SqliteDB.insertData(table, data)
                    transactionProcessed = True

                if "message" in t:
                    table, data = 'transactions_message', getData.getTransactionDataMessage(t, block_number, timestamp) 
                    SqliteDB.insertData(table, data)                              
                    transactionProcessed = True

                if "latticePk" in t:                                                    
                    table, data = "transactions_latticePk", getData.getTransactionDataLatticePk(t, block_number, timestamp)                 
                    SqliteDB.insertData(table, data)            
                    transactionProcessed = True

                if "slave" in t:
                    if "slavePks" in t["slave"]:
                        slave_pks = t["slave"]["slavePks"]
                        access_types = t["slave"]["accessTypes"]
                        transfers = [{"slave_pk" : slave_pks[i], "access_type":access_types[i]} for i in range(len(slave_pks))]
                        
                        for transfer in transfers:
                            table, data = "transactions_slave", getData.getTransactionDataSlave(t, block_number, timestamp, transfer)  
                            SqliteDB.insertData(table, data)
                        transactionProcessed = True
                                                
                if "transferToken" in t:
                    if "addrsTo" in t["transferToken"]:
                        addrs_to = t["transferToken"]["addrsTo"]
                        amounts = t["transferToken"]["amounts"]
                        token_txhash = t["transferToken"]["tokenTxhash"]
                        transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i], "token_txhash":token_txhash} for i in range(len(addrs_to))]

                        for transfer in transfers:                        
                            table, data = "transactions_transfertoken", getData.getTransactionDataTransferToken(t, block_number, timestamp, transfer) 
                            SqliteDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                            SqliteDB.insertData(table, data)
                        transactionProcessed = True
                                                    
                if not transactionProcessed:
                    table, data = "transactions_others", getData.getTransactionDataOthers(t, block_number, timestamp)                 
                    SqliteDB.insertData(table, data)

                                                          
            except Exception as e:            
                print(e)
                print('Error while getting transaction data')
                eData={}
                eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getTransactionData', str(traceback.format_exc()),  str(t) , str(e), block_number
                table, data = "errors" , eData
                SqliteDB.insertData(table, data)                
                raise

            finally:
                cur.close()          
    
        
        def getAddressData(source, b64Addr, timeStamp, block_number): 
            cur = DB_CONNECTION.cursor()                    
            addressData = getData.getAddressData(source, b64Addr, timeStamp)
                                
            try:            
                cur.execute(SQL_SELECT_ADDRESS_SQLITE % addressData["address"]) 
                dup_check = len(cur.fetchall())
                
                if dup_check == 0:                    
                    table, data   = 'addresses', addressData
                    SqliteDB.insertData(table, data)
                else:                    
                    table, idkey, idval, data  = 'addresses', 'address', addressData["address"], addressData 
                    del addressData["address"]  
                    del addressData["first_seen"]
                    SqliteDB.updateData(table, data, idkey, idval)

            except Exception as e:                    
                print(e)
                print('Error while getting address data')
                eData={}
                eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getAddressData', str(traceback.format_exc()), b64Addr , str(e), block_number
                table, data = "errors" , eData
                SqliteDB.insertData(table, data)                   
                raise
            
            finally:
                cur.close() 
                    
    
