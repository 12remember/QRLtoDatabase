SQL_CREATE_DATABASE = 'CREATE DATABASE'

SQL_CREATE_TABLES_RAW = [
'''CREATE TABLE IF NOT EXISTS addresses 
    (address TEXT PRIMARY KEY, 
    balance BIGINT,         
    nonce INT,
    ots_bitfield TEXT,
    transaction_hashes TEXT,
    tokens TEXT,
    latticePK_list TEXT,
    slave_pks_access_type TEXT,
    ots_counter INT,            
    first_seen TIMESTAMP WITHOUT TIME ZONE, 
    last_seen TIMESTAMP WITHOUT TIME ZONE, 
    custom_name text
    )''',   

'''CREATE TABLE IF NOT EXISTS blocks 
    (block_number BIGINT PRIMARY KEY,
    hash_header TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    hash_header_prev TEXT,
    reward_block BIGINT,
    reward_fee BIGINT,
    merkle_root TEXT,
    mining_nonce BIGINT,
    extra_nonce TEXT,
    genesis_balance BIGINT,
    size BIGINT          
    )''',                       

'''CREATE TABLE IF NOT EXISTS transactions_coinbase 
    (transaction_hash TEXT PRIMARY KEY,
    block_number BIGINT REFERENCES blocks(block_number),
    public_key TEXT,
    master_addr TEXT REFERENCES addresses(address),
    signature TEXT,
    nonce BIGINT,
    addr_to TEXT REFERENCES addresses(address),
    amount BIGINT,
    timestamp TIMESTAMP WITHOUT TIME ZONE
    )''',  

'''CREATE TABLE IF NOT EXISTS transactions_transfer
    (transaction_hash TEXT,
    block_number BIGINT REFERENCES blocks(block_number),
    master_addr TEXT REFERENCES addresses(address) ,
    fee BIGINT,
    public_key TEXT,
    signature TEXT,
    nonce BIGINT,
    addr_to TEXT REFERENCES addresses(address),
    amount BIGINT,
    timestamp TIMESTAMP WITHOUT TIME ZONE
    )''',  

'''CREATE TABLE IF NOT EXISTS transactions_token
    (transaction_hash TEXT PRIMARY KEY,
    block_number BIGINT REFERENCES blocks(block_number),
    fee BIGINT,
    public_key TEXT,
    signature TEXT,
    nonce BIGINT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    symbol TEXT,
    name TEXT,
    owner TEXT REFERENCES addresses(address),
    decimals BIGINT,
    initial_balances TEXT
    )''',  

'''CREATE TABLE IF NOT EXISTS transactions_message
    (transaction_hash TEXT PRIMARY KEY,
    message_hash TEXT,
    message_text TEXT,  
    block_number BIGINT REFERENCES blocks(block_number),
    master_addr TEXT,
    fee BIGINT,
    public_key TEXT,
    signature TEXT,
    nonce TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE
    )''',  

'''CREATE TABLE IF NOT EXISTS transactions_latticePk
    (transaction_hash TEXT PRIMARY KEY,
    block_number BIGINT REFERENCES blocks(block_number),
    master_addr TEXT,
    fee BIGINT,
    public_key TEXT,
    signature TEXT,
    nonce BIGINT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    kyber_pk TEXT,
    dilithium_pk TEXT
    )''', 

'''CREATE TABLE IF NOT EXISTS transactions_slave
    (transaction_hash TEXT,
    block_number BIGINT REFERENCES blocks(block_number),
    public_key TEXT,
    fee BIGINT,
    signature TEXT,
    nonce BIGINT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    slave_pk TEXT,
    access_type BIGINT
    )''', 

'''CREATE TABLE IF NOT EXISTS transactions_transfertoken
    (token_txhash TEXT,
    transaction_hash TEXT,
    block_number BIGINT REFERENCES blocks(block_number),
    fee BIGINT,
    public_key TEXT,
    signature TEXT,
    nonce BIGINT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    addr_to TEXT REFERENCES addresses(address),
    amount BIGINT
    )''',  

'''CREATE TABLE IF NOT EXISTS transactions_others
    (transaction_hash TEXT PRIMARY KEY,
    block_number BIGINT REFERENCES blocks(block_number),
    master_addr TEXT,
    type TEXT,
    data TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE
    )''', 


'''CREATE TABLE IF NOT EXISTS errors 
    (date TIMESTAMP WITHOUT TIME ZONE,
    location TEXT,
    data TEXT,
    error TEXT,
    traceback TEXT,
    blocknumber TEXT,
    data_keys TEXT,
    data_key_type TEXT                                
    )''',   

]

SQL_CREATE_TABLES_AGG = [

'''CREATE TABLE IF NOT EXISTS analytics_topstats_transactions_day 
    (block_number BIGINT PRIMARY KEY,
    address TEXT REFERENCES addresses(address)    
    )''',    

'''CREATE TABLE IF NOT EXISTS analytics_topstats_transactions_week 
    (block_number BIGINT PRIMARY KEY,
    address TEXT REFERENCES addresses(address)    
    )''', 

]


SQL_GET_BLOCKHEIGHT_IN_DB_POSTGRES = 'SELECT "block_number" FROM public."blocks" ORDER BY "block_number" DESC LIMIT 1'
SQL_GET_BLOCKHEIGHT_IN_DB_SQLITE = 'SELECT "block_number" FROM "blocks" ORDER BY "block_number" DESC LIMIT 1'
SQL_STOP_DATABASE_ACTIVITY = "SELECT pg_terminate_backend (pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'quantascan'"
SQL_SELECT_DATABASE = "SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'quantascan'"
SQL_DROP_DATABASE = "DROP DATABASE quantascan"
SQL_SELECT_TABLES_POSTGRES = "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name"
SQL_SELECT_TABLES_SQLITE = "SELECT name FROM sqlite_master WHERE type='table';"

SQL_UPDATE_DATA_DYNAMIC = 'UPDATE {table} SET {data} WHERE {idkey} = {idval}'
SQL_INSERT_DATA_DYNAMIC = 'INSERT INTO {table} ({columns}) VALUES {values}'
SQL_SELECT_ADDRESS_POSTGRES = 'SELECT "address" FROM public."addresses" WHERE "address" = %s'
SQL_SELECT_ADDRESS_SQLITE = 'SELECT "address" FROM "addresses" WHERE "address" = "%s"'