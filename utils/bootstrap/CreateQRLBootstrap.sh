#!/bin/bash
#Code based on https://wiki.lyrasis.org/display/FEDORA46/Backup+and+Restore#BackupandRestore-LevelDBBackup
# code made by - 0xFF0  https://github.com/0xFF0/QRL_bootstrap

NET_NAME=Mainnet
QRL_DATA=$1 #~/.qrl/data/state
BACKUP_PATH=~/qrl_bootstrap
BOOTSTRAP_LOGS=$BACKUP_PATH/qrl_bootstrap.logs
ATTEMPTS=10  # Max Backup Attempts on failure
BOOTSTRAP_FILE_NAME=QRL_"$NET_NAME"_State.tar.gz
CHECKSUM_FILE=$BACKUP_PATH/$NET_NAME/"$NET_NAME"_State_Checksums.txt
VERSION="v1.0"

echo "----------------------------------------------" | tee -a $BOOTSTRAP_LOGS 
echo "Create QRL bootstrap $VERSION" | tee -a $BOOTSTRAP_LOGS  
echo "----------------------------------------------" | tee -a $BOOTSTRAP_LOGS  

echo "["`date -u`"] Deleting old backup files in $BACKUP_PATH/$NET_NAME/" | tee -a $BOOTSTRAP_LOGS
# Delete old backup
rm -rf $BACKUP_PATH/$NET_NAME/

if [ ! -d $BACKUP_PATH/$NET_NAME/"state" ]; then
  echo "["`date -u`"] Saving bootstrap files in $BACKUP_PATH/$NET_NAME/state" | tee -a $BOOTSTRAP_LOGS  
  mkdir -p $BACKUP_PATH/$NET_NAME/state
fi

echo "["`date -u`"] QRL data dir: $QRL_DATA" | tee -a $BOOTSTRAP_LOGS
echo "["`date -u`"] Backup dir: $BACKUP_PATH" | tee -a $BOOTSTRAP_LOGS
echo "["`date -u`"] Max attempts on failure: $ATTEMPTS" | tee -a $BOOTSTRAP_LOGS
echo "["`date -u`"] Backing up leveldb." | tee -a $BOOTSTRAP_LOGS 


backup_succeeded=false
attempts=$ATTEMPTS

while [ $attempts -gt 0 ]; do
 
  MANIFEST_FILE=`ls $QRL_DATA/MANIFEST-*`
  MANIFEST_MD5=`md5sum $MANIFEST_FILE`
  echo "["`date -u`"] MD5 manifest file: $MANIFEST_MD5" | tee -a $BOOTSTRAP_LOGS

  echo "["`date -u`"] Starting Rsync." | tee -a $BOOTSTRAP_LOGS 
  rsync -a --info=progress2 $QRL_DATA/ $BACKUP_PATH/$NET_NAME/"state" --delete
  copy_success=$?
  echo "["`date`"] Rsync exit code: $copy_success" | tee -a $BOOTSTRAP_LOGS

  MANIFEST_FILE_POST_BACKUP=`ls $QRL_DATA/MANIFEST-*`
  MANIFEST_MD5_POST_BACKUP=`md5sum $MANIFEST_FILE_POST_BACKUP`
  echo "["`date -u`"] MD5 manifest file post backup: $MANIFEST_MD5_POST_BACKUP" | tee -a $BOOTSTRAP_LOGS
  
  if [ "$MANIFEST_MD5" = "$MANIFEST_MD5_POST_BACKUP" ] && [ "$copy_success" = "0" ]; then
    echo "["`date -u`"] Backup succeeded." | tee -a $BOOTSTRAP_LOGS
    backup_succeeded=true
    break;
  fi
  
  attempts=$((attempts - 1))
  echo "["`date -u`"] Leveldb manifest changed during backup process! $attempts attempts remaining." | tee -a $BOOTSTRAP_LOGS
  echo -e "\n\n\n\n" | tee -a $BOOTSTRAP_LOGS
  
done
 
 
 
if [ "$backup_succeeded" = false ]; then
    echo "["`date -u`"] Failed to backup with a consistent leveldb manifest!" | tee -a $BOOTSTRAP_LOGS
    exit 1
else
    echo "["`date -u`"] Backup created and verified leveldb manifest consistency!" | tee -a $BOOTSTRAP_LOGS
    echo "["`date -u`"] tar -zcf $BACKUP_PATH/$NET_NAME/$BOOTSTRAP_FILE_NAME -C $BACKUP_PATH/$NET_NAME state" | tee -a $BOOTSTRAP_LOGS	
    
    if tar -zcf $BACKUP_PATH/$NET_NAME/$BOOTSTRAP_FILE_NAME -C $BACKUP_PATH/$NET_NAME state 
    then
    	echo "["`date -u`"] Tar backup created." | tee -a $BOOTSTRAP_LOGS
    else
    	echo "["`date -u`"] Tar backup failed. Retrying one more time." | tee -a $BOOTSTRAP_LOGS
    	if tar -zcf $BACKUP_PATH/$NET_NAME/$BOOTSTRAP_FILE_NAME -C $BACKUP_PATH/$NET_NAME state 
    	then
    	  echo "["`date -u`"] Tar backup created." | tee -a $BOOTSTRAP_LOGS
    	else
    	  echo "["`date -u`"] Tar backup failed." | tee -a $BOOTSTRAP_LOGS
    	  exit 1
    	fi
    fi
    
    
fi


 
# Verify using python script
echo "["`date -u`"] Verifying bootstrap with python script." | tee -a $BOOTSTRAP_LOGS
python3 verify_leveldb.py $BACKUP_PATH/$NET_NAME/ | tee -a $BOOTSTRAP_LOGS


if [ "$?" != "0" ]; then
    echo "["`date -u`"] Discovered backup corruption! Attempting to repair!"  | tee -a $BOOTSTRAP_LOGS
    mv "QRL_"$NET_NAME"_State.tar.gz" "QRL_"$NET_NAME"_State-BAD.tar.gz" 
	
else
    echo "["`date -u`"] Backup passed corruption verification!"  | tee -a $BOOTSTRAP_LOGS
    echo "["`date -u`"] Generating checksums."  | tee -a $BOOTSTRAP_LOGS
    
    echo "******************************" | tee -a $CHECKSUM_FILE
    echo "      QRL State Checksums 	 " | tee -a $CHECKSUM_FILE
    echo "******************************" | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE
    
    echo Verification for file: $BOOTSTRAP_FILE_NAME | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE
    
    echo -------- SHA3-512 Sum -------- | tee -a $CHECKSUM_FILE
    sha3=($(openssl dgst -sha3-512 $BACKUP_PATH/$NET_NAME/$BOOTSTRAP_FILE_NAME))
    echo ${sha3[1]} | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE
    
    echo Verify from linux cli with: | tee -a $CHECKSUM_FILE    
    echo openssl dgst -sha3-512 $BOOTSTRAP_FILE_NAME | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE
    
    echo -------- SHA-256 Sum -------- | tee -a $CHECKSUM_FILE
    sha256=($(sha256sum $BACKUP_PATH/$NET_NAME/$BOOTSTRAP_FILE_NAME))
    echo $sha256 | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE
    
    echo Verify from linux cli with: | tee -a $CHECKSUM_FILE
    echo sha256sum $BOOTSTRAP_FILE_NAME | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE

    echo -------- MD5 Sum -------- | tee -a $CHECKSUM_FILE
    md5=($(md5sum $BACKUP_PATH/$NET_NAME/$BOOTSTRAP_FILE_NAME))
    echo $md5 | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE
    
    echo Verify from linux cli with: | tee -a $CHECKSUM_FILE
    echo md5sum  $BOOTSTRAP_FILE_NAME | tee -a $CHECKSUM_FILE
    echo | tee -a $CHECKSUM_FILE    
    
    cat $CHECKSUM_FILE >> $BOOTSTRAP_LOGS
    
    echo "["`date -u`"] Finish."  | tee -a $BOOTSTRAP_LOGS
fi
 

