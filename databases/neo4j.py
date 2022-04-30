
#import argparse
#import base64
import binascii
from datetime import datetime
import json
import traceback

from neo4j import GraphDatabase

from google.protobuf.json_format import MessageToJson, Parse, MessageToDict

from utils.getData import *


DB_CON = {
    "uri": "bolt://localhost:7687",
    "user": "neo4j",
    "pwd": "root",
}



DB_CONNECTION = GraphDatabase.driver(DB_CON["uri"], auth=(DB_CON["user"], DB_CON["pwd"]))

class Neo4jDB(object):

    def connect():
        DB_CONNECTION = GraphDatabase.driver(DB_CON["uri"], auth=(DB_CON["user"], DB_CON["pwd"]))
        return DB_CONNECTION


    def insertData(coll, data):
        DB_CONNECTION = Neo4jDB.connect()
        with DB_CONNECTION.session() as session:
        
            try:
                if  (coll == 'addresses' and  'address' in data):

                #    query = "'CREATE (' + data['address'] + ':Address)' + str(data)" #"CREATE (friend:Person {name: 'Mark'})"
                    query_string = 'CREATE (a {name:"node", text: "", color: "red", size: "7 m"})' #' '.join(['CREATE (', data['address'],  ':Address)', {'name':"node", 'text': "", 'color': "red", 'size': "7 m"}]) 
                    session.run(query_string)
                    #coll.session(coll, data)
                                    
            except Exception as e:  
                print(e)
                print('Exception while inserting data into Database')
                raise 
            finally:
                session.close()
    
    
    def updateData(coll, data, idkey, idval ):
        DB_CONNECTION = Neo4jDB.connect()
        with DB_CONNECTION.session() as session:
            coll = DB_CONNECTION[coll]    
            try:
                query = {idkey: idval}
                newVal = { "$set": data  }                         
                coll.update_one(query, newVal)     
                
            except Exception as e:
                print(e)
                print('Exception while updating data from Database') 
                raise
            finally:
                session.close()

    def createDB():
        try:
            DB_CONNECTION = Neo4jDB.connect()
            with DB_CONNECTION.session() as session:
                query_string = '''CREATE DATABASE qrldata IF NOT EXISTS '''
                session.run(query_string)   
                print('Database Created')
                print('please restart the server')
        except Exception as e:
            print(e)
            print('Database Could Not Be Created')    
        finally:
            session.close()

                     
    def dropDB():
        try:
            DB_CONNECTION = Neo4jDB.connect()
            with DB_CONNECTION.session() as session:
                query_string = '''STOP DATABASE qrldata '''
                print('stopping database')
                session.run(query_string)   
                query_string2 = '''DROP DATABASE qrldata '''
                session.run(query_string2)   
                print('Database Dropped')
                print('please restart the server')
        except Exception as e:
            print(e)
            print('Error while dropping Database')
            raise         
        finally:
            session.close()


    def dropCollections():
        try:
            DB_CONNECTION = Neo4jDB.connect()
            with DB_CONNECTION.session() as session:
                query_string = '''MATCH (n) DETACH DELETE n '''
                session.run(query_string)
                print('Collections Dropped')                        
        except Exception as e:
            print(e)
            print('Error while dropping Collections')
        finally:
            session.close()
    

    def getBlockData(source):
        try:
            DB_CONNECTION = Neo4jDB.connect()
            with DB_CONNECTION.session() as session:                
                blockheightBC = getData.getBlockHeight(source)
                try:
                    query = '''MATCH (block_number:block_number)
                                ORDER BY block_number DESC
                                RETURN max(block_number)'''
                    blockHeightInDB = session.run(query) #DB_CONNECTION.blocks.find_one(sort=[("block_number", -1)])
                    print('blockHeightInDB')
                    print(blockHeightInDB)
                    blockHeightInDB = blockHeightInDB["block_number"]
                except:
                    blockHeightInDB = 0    

                if blockheightBC < blockHeightInDB:
                    print('current blockheight in database is heigher than node')
                    
                print(" ".join(["Parsing block" , str(blockHeightInDB) , "/" , str(blockheightBC)])) 
                for i in range(blockHeightInDB+1 , blockheightBC+1):                       
                    print(" ".join(["Parsing block" , str(i) , "/" , str(blockheightBC)]))  
                    blockData = getData.getBlockData(i, source)                
                    coll, data, transactions = 'blocks', blockData, blockData["transactions"]
                    del blockData["transactions"] 
                    Neo4jDB.insertData(coll, data)
                    for t in transactions:
                        Neo4jDB.getTransactionData(t, source, blockData["block_number"], blockData["timestamp"] )
                        
        except Exception as e:            
            print(e)
            print('Error while getting block data')    
            eData={}
            eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getBlockData', str(traceback.format_exc()), "" , str(e), ""
            coll, data = "errors_get_data" , eData
            Neo4jDB.insertData(coll, data)      
            raise
        finally:
            session.close()
                        

    def getTransactionData(t, source, block_number, timestamp):
        try:
            transactionProcessed = False
            tData = {}
            if "masterAddr" in t:
                Neo4jDB.getAddressData(source, t["masterAddr"], timestamp, block_number)
            
            
            if "coinbase" in t:        
                coll, data = 'transactions_coinbase', getData.getTransactionDataCoinbase(t, block_number, timestamp)
                Neo4jDB.getAddressData(source, t["coinbase"]["addrTo"], timestamp, block_number)
                Neo4jDB.insertData(coll, data)
                transactionProcessed = True
                
            if "transfer" in t:       
                if "addrsTo" in t["transfer"]:
                    addrs_to = t["transfer"]["addrsTo"]
                    amounts = t["transfer"]["amounts"]
                    transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i]} for i in range(len(addrs_to))]

                    for transfer in transfers:                                             
                        coll, data = 'transactions_transfer', getData.getTransactionDataTransfer(t, block_number, timestamp, transfer) 
                        Neo4jDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                        Neo4jDB.insertData(coll, data)
                    transactionProcessed = True

            if "token" in t:
                coll, data = 'transactions_token', getData.getTransactionDataToken(t, block_number, timestamp) 
                Neo4jDB.getAddressData(source, t["token"]["owner"] , timestamp, block_number)
                Neo4jDB.insertData(coll, data)
                transactionProcessed = True

            if "message" in t:
                coll, data = 'transactions_message', getData.getTransactionDataMessage(t, block_number, timestamp) 
                Neo4jDB.insertData(coll, data)                              
                transactionProcessed = True

            if "latticePk" in t:                                                    
                coll, data = "transactions_latticePk", getData.getTransactionDataLatticePk(t, block_number, timestamp)                 
                Neo4jDB.insertData(coll, data)            
                transactionProcessed = True

            if "slave" in t:
                if "slavePks" in t["slave"]:
                    slave_pks = t["slave"]["slavePks"]
                    access_types = t["slave"]["accessTypes"]
                    transfers = [{"slave_pk" : slave_pks[i], "access_type":access_types[i]} for i in range(len(slave_pks))]
                    
                    for transfer in transfers:
                        coll, data = "transactions_slave", getData.getTransactionDataSlave(t, block_number, timestamp, transfer)  
                        Neo4jDB.insertData(coll, data)
                    transactionProcessed = True
                                            
            if "transferToken" in t:
                if "addrsTo" in t["transferToken"]:
                    addrs_to = t["transferToken"]["addrsTo"]
                    amounts = t["transferToken"]["amounts"]
                    token_txhash = t["transferToken"]["tokenTxhash"]
                    transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i], "token_txhash":token_txhash} for i in range(len(addrs_to))]

                    for transfer in transfers:                        
                        coll, data = "transactions_transfertoken", getData.getTransactionDataTransferToken(t, block_number, timestamp, transfer) 
                        Neo4jDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                        Neo4jDB.insertData(coll, data)
                    transactionProcessed = True
                                                
            if not transactionProcessed:
                coll, data = "transactions_others", getData.getTransactionDataOthers(t, block_number, timestamp)                 
                Neo4jDB.insertData(coll, data)

                                                      
        except Exception as e:            
            print(e)
            print('Error while getting transaction data')
            eData={}
            eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getTransactionData', str(traceback.format_exc()),  str(t) , str(e), block_number
            coll, data = "errors_get_data" , eData
            Neo4jDB.insertData(coll, data)                
            raise



    def getAddressData(source, b64Addr, timeStamp, block_number): 

        addressData = getData.getAddressData(source, b64Addr, timeStamp)                                
        try:            
            dup_check = None #DB_CONNECTION.addresses .find_one({"address": addressData["address"]})      
            
            if dup_check == None:                    
                coll, data   = 'addresses', addressData
                Neo4jDB.insertData(coll, data)
            else:                                
                coll, idkey, idval, data  = 'addresses', 'address', addressData["address"], addressData 
                del addressData["address"]  
                del addressData["first_seen"]
                Neo4jDB.updateData(coll, data, idkey, idval)

        except Exception as e:                    
            print(e)
            print('Error while getting address data')
            eData={}
            eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getAddressData', str(traceback.format_exc()), b64Addr , str(e), block_number
            coll, data = "errors_get_data" , eData
            Neo4jDB.insertData(coll, data)                   
            raise

                        

Neo4jDB()