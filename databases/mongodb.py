
#import base64
#import binascii
from datetime import datetime
import json
import traceback

from decimal import Decimal
from bson.decimal128 import Decimal128
from pymongo import MongoClient
import pymongo 
from google.protobuf.json_format import MessageToJson, Parse, MessageToDict

from utils.getData import *


DB_CON = {
    "host": "localhost",
    "port": 27017,
    "database":"qrldata"
}


SERVER_CONNECTION = MongoClient(host=DB_CON["host"], port=DB_CON["port"])
DB_CONNECTION = SERVER_CONNECTION.qrldata


class MongoDB(object):

    def insertData(coll, data):
        try:  

            if data is not None: # why is data sometimes none ??
                   
                if  (coll == 'addresses' and  'address' in data):
                    data["_id"] = data["address"]
                #    del data["address"]    
                
                if (coll == 'blocks' and 'block_number' in data):
                    data["_id"] = data["block_number"]  

                if 'extra_nonce' in data:
                    data["extra_nonce"] = str(data["extra_nonce"]) # sometimes int length passes max length mongodb can handel > therefor it will be saved as an string

                
                coll = DB_CONNECTION[coll]
                coll.insert_one(data)


        except pymongo.errors.DuplicateKeyError:
            print('already got that one')
            pass
                        
        except Exception as e:  
            print(e)
            print(traceback.format_exc())
            eData={}
            try:
                dataKeys = [k for k in data.keys()]
                dataKeyType = [type(k) for k in data.keys()]
            except:
                dataKeys = ''
                dataKeyType = ''
                    
            eData["date"], eData["location"], eData["traceback"], eData["data"] =  datetime.now(), 'insertData', str(traceback.format_exc()), str(data)
            eData["data_keys"], eData["data_key_type"], eData["error"], eData["blocknumber"] =  str(dataKeys), str(dataKeyType), str(e), ""  
            coll, data = "errors_get_data" , eData
            MongoDB.insertData(coll, data)      
            print('Error while inserting data into Database')
            raise 

    
    
    def updateData(coll, data, idkey, idval ):
        coll = DB_CONNECTION[coll]    
        try:
            query = {idkey: idval}
            newVal = { "$set": data  }                         
            coll.update_one(query, newVal)             
        except Exception as e:
            print(e)
            print('Exception while updating data from Database') 
            raise
      
    
    def dropDB():
        try:                    
            if DB_CON["database"] in SERVER_CONNECTION.list_database_names():                
                SERVER_CONNECTION.drop_database(DB_CON["database"])
                print('Database Dropped')
            else:
                print('Database Doenst Exists')
                pass
              
        except Exception as e:
            print(e)
            print('Error while dropping Database')
            raise  
      

    
    def dropCollections():
        try:
            collections = DB_CONNECTION.list_collection_names()
            for collection in collections:
                print(" ".join(["Dropping collection:" , collection]))
                DB_CONNECTION.drop_collection(collection)
                           
        except Exception as e:
            print(e)
            print('Error while dropping Collections')

    
    def truncateCollections():
        try:
            collections = DB_CONNECTION.list_collection_names()
            for collection in collections:
                print("".join(["Truncate collection:" , collection]))
                DB_CONNECTION.collection.remove({}) #can a var be used here ?        
        except Exception as e:
            print(e)
            print('Error while Truncate Collections')
            raise

        
    
    def getBlockData(source):
        try:                
            blockheightBC = getData.getBlockHeight(source)
            blockHeightInDB = DB_CONNECTION.blocks.find_one(sort=[("block_number", -1)])
     
            if blockHeightInDB != None: 
                blockHeightInDB = blockHeightInDB["block_number"]
            else:
                blockHeightInDB = 0
            
            if blockheightBC < blockHeightInDB:
                print('current blockheight in database is heigher than node')  
                  
            for i in range(blockHeightInDB , blockheightBC+1):                       
                print(" ".join(["Parsing block" , str(i) , "/" , str(blockheightBC)]))  
                blockData = getData.getBlockData(i, source)                
                coll, data, transactions = 'blocks', blockData, blockData["transactions"]
                del blockData["transactions"] 
                MongoDB.insertData(coll, data)
                for t in transactions:
                    MongoDB.getTransactionData(t, source, blockData["block_number"], blockData["timestamp"] )
                        
        except Exception as e:            
            print(e)
            print('Error while getting block data')    
            eData={}
            eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getBlockData', str(traceback.format_exc()), "" , str(e), ""
            coll, data = "errors_get_data" , eData
            MongoDB.insertData(coll, data)      
            raise

                        
    
    def getTransactionData(t, source, block_number, timestamp):
        try:
            transactionProcessed = False
            tData = {}
            if "masterAddr" in t:
                MongoDB.getAddressData(source, t["masterAddr"], timestamp, block_number)
            
            
            if "coinbase" in t:        
                coll, data = 'transactions_coinbase', getData.getTransactionDataCoinbase(t, block_number, timestamp)
                MongoDB.getAddressData(source, t["coinbase"]["addrTo"], timestamp, block_number)
                MongoDB.insertData(coll, data)
                transactionProcessed = True
                
            if "transfer" in t:       
                if "addrsTo" in t["transfer"]:
                    addrs_to = t["transfer"]["addrsTo"]
                    amounts = t["transfer"]["amounts"]
                    transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i]} for i in range(len(addrs_to))]

                    for transfer in transfers:                                             
                        coll, data = 'transactions_transfer', getData.getTransactionDataTransfer(t, block_number, timestamp, transfer) 
                        MongoDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                        MongoDB.insertData(coll, data)
                    transactionProcessed = True

            if "token" in t:
                coll, data = 'transactions_token', getData.getTransactionDataToken(t, block_number, timestamp) 
                MongoDB.getAddressData(source, t["token"]["owner"] , timestamp, block_number)
                MongoDB.insertData(coll, data)
                transactionProcessed = True

            if "message" in t:
                coll, data = 'transactions_message', getData.getTransactionDataMessage(t, block_number, timestamp) 
                MongoDB.insertData(coll, data)                              
                transactionProcessed = True

            if "latticePk" in t:                                                    
                coll, data = "transactions_latticePk", getData.getTransactionDataLatticePk(t, block_number, timestamp)                 
                MongoDB.insertData(coll, data)            
                transactionProcessed = True

            if "slave" in t:
                if "slavePks" in t["slave"]:
                    slave_pks = t["slave"]["slavePks"]
                    access_types = t["slave"]["accessTypes"]
                    transfers = [{"slave_pk" : slave_pks[i], "access_type":access_types[i]} for i in range(len(slave_pks))]
                    
                    for transfer in transfers:
                        coll, data = "transactions_slave", getData.getTransactionDataSlave(t, block_number, timestamp, transfer)  
                        MongoDB.insertData(coll, data)
                    transactionProcessed = True
                                            
            if "transferToken" in t:
                if "addrsTo" in t["transferToken"]:
                    addrs_to = t["transferToken"]["addrsTo"]
                    amounts = t["transferToken"]["amounts"]
                    token_txhash = t["transferToken"]["tokenTxhash"]
                    transfers = [{"addr_to" : addrs_to[i], "amount":amounts[i], "token_txhash":token_txhash} for i in range(len(addrs_to))]

                    for transfer in transfers:                        
                        coll, data = "transactions_transfertoken", getData.getTransactionDataTransferToken(t, block_number, timestamp, transfer) 
                        MongoDB.getAddressData(source, transfer["addr_to"] , timestamp, block_number)
                        MongoDB.insertData(coll, data)
                    transactionProcessed = True
                                                
            if not transactionProcessed:
                coll, data = "transactions_others", getData.getTransactionDataOthers(t, block_number, timestamp)                 
                MongoDB.insertData(coll, data)

                                                      
        except Exception as e:            
            print(e)
            print('Error while getting transaction data')
            eData={}
            eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getTransactionData', str(traceback.format_exc()),  str(t) , str(e), block_number
            coll, data = "errors_get_data" , eData
            MongoDB.insertData(coll, data)                
            raise
  

    
    def getAddressData(source, b64Addr, timeStamp, block_number): 
    
        addressData = getData.getAddressData(source, b64Addr, timeStamp)                                
        try:            
            dup_check = DB_CONNECTION.addresses .find_one({"address": addressData["address"]})      
            
            if dup_check == None:                    
                coll, data   = 'addresses', addressData
                MongoDB.insertData(coll, data)
            else:                                
                coll, idkey, idval, data  = 'addresses', 'address', addressData["address"], addressData 
                del addressData["address"]  
                del addressData["first_seen"]
                MongoDB.updateData(coll, data, idkey, idval)

        except Exception as e:                    
            print(e)
            print('Error while getting address data')
            eData={}
            eData["date"], eData["location"], eData["traceback"], eData["data"], eData["error"], eData["blocknumber"] = datetime.now(), 'getAddressData', str(traceback.format_exc()), b64Addr , str(e), block_number
            coll, data = "errors_get_data" , eData
            MongoDB.insertData(coll, data)                   
            raise

                

