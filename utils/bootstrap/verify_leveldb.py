#!/usr/bin/python
#Code based on https://wiki.lyrasis.org/display/FEDORA46/Backup+and+Restore#BackupandRestore-LevelDBBackup
import sys
import plyvel
import os
from datetime import datetime


if (len(sys.argv) != 2) :
  print('Usage:')
  print('python verify_leveldb.py /path/to/backupfiles')
  sys.exit(1)

dbpath = os.path.join(sys.argv[1], "state")
blockheightPath = os.path.join(sys.argv[1], "blockheight.txt")

try:
  startTime = datetime.now()
  print(sys.argv[0], ": Inspecting db: ", dbpath)
  db = plyvel.DB(dbpath, create_if_missing=False, paranoid_checks=True)
  blockheight = int.from_bytes(db.get(b'blockheight'), byteorder='big', signed=False)
  with open(blockheightPath, 'w') as f:
    f.write(str(blockheight))
    
  records = 0
  for key, value in db:
    records = records + 1
  print(sys.argv[0], ": Backup verfiication successful!")
  print(sys.argv[0], ": Total records: ", records)
  print(sys.argv[0], ": Time taken:", datetime.now() - startTime)
  print(sys.argv[0], ": Blockheight:", blockheight)
except:
  print(sys.argv[0], ": Backup verfication failed!")
  print(sys.argv[0], ": Unexpected error:", sys.exc_info()[0])
  raise
  exit(1)

