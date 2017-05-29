"""
Author      : Swadhin Goswami
Gmail       : gsmswadhin@gmail.com
Description : This is a utility python library for below operations. 
              - Handling DB operation 
			  - CSV file handling ( converting a csv file to list or dictionary using delimeter )
			  - Convert file/directory size from KB to MB,GB or TB 
              
"""


#!/usr/bin/python
import subprocess
import commands
import sys
import cx_Oracle

connstr='exampleuser/passabc@oracle_server:1521/db'


TABLE = {
  'CS_CLUSTER_TBL' : 'INSERT INTO CS_CLUSTER_TBL'
                     '('
                      'RUN_DATE_TIME,'
                      'CLUSTER_NAME,'
                      'MART_NAME,'
                      'NODE_NAME,'
                      'PARTITION_ID,'
                      'PRODCON_CODE_LINK,'
                      'IQS_CODE_LINK,'
                      'DATA_LOAD_START_TIME,'
                      'DATA_LOAD_END_TIME,'
                      'ITEM_LOAD_START_TIME,'
                      'ITEM_LOAD_END_TIME,'
                      'VENUE_LOAD_START_TIME,'
                      'VENUE_LOAD_END_TIME,'
                      'HAND_SHAKE_FINISHED'
                     ')'
                     'VALUES'
                     '('
                      'TO_CHAR( SYSDATE, \'DD-MM-YY HH24:MI:SS AM\' ),'
                      ':CLUSTER_NAME,'
                      ':MART_NAME,'
                      ':NODE_NAME,'
                      ':PARTITION_ID,'
                      ':PRODCON_CODE_LINK,'
                      ':IQS_CODE_LINK,'
                      'TO_TIMESTAMP( :DATA_LOAD_START_TIME ),'
                      ':DATA_LOAD_END_TIME,'
                      ':ITEM_LOAD_START_TIME,'
                      ':ITEM_LOAD_END_TIME,'
                      ':VENUE_LOAD_START_TIME,'
                      ':VENUE_LOAD_END_TIME,'
                      ':HAND_SHAKE_FINISHED'
                     ')',

  'CS_MART'       : 'INSERT INTO CS_MART'
                    '('
                     'RUN_DATE_TIME,'
                     'MARTNAME,'
                     'CLUSTER_NAME,'
                     'MART_TYPE,'
                     'NO_OF_SIDES,'
                     'HEAD1,'
                     'HEAD2,'
                     'NO_OF_PARTITIONS,'
                     'START_TIME_PERIOD,'
                     'END_TIME_PERIOD'
                    ')'
                    'VALUES'
                    '('
                     'TO_CHAR( SYSDATE, \'DD-MM-YY HH24:MI:SS AM\' ),'
                     ':MARTNAME,'
                     ':CLUSTER_NAME,'
                     ':MART_TYPE,'
                     ':NO_OF_SIDES,'
                     ':HEAD1,'
                     ':HEAD2,'
                     ':NO_OF_PARTITIONS,'
                     ':START_TIME_PERIOD,'
                     ':END_TIME_PERIOD'
                    ')',
  'CS_DIMENTIONS' : 'INSERT INTO CS_DIMENTIONS'
                    '('
                     'RUN_DATE_TIME,'
                     'MARTNAME,'
                     'CLUSTER_NAME,'
                     'DIMENTION_NAME,'
                     'NUMBER_OF_MEMBERS,'
                     'IS_SHARED,'
                     'SHARED_CPU_IDX'
                    ')'
                    'VALUES'
                    '('
                     'TO_CHAR( SYSDATE, \'DD-MM-YY HH24:MI:SS AM\' ),'
                     ':MARTNAME,'
                     ':CLUSTER_NAME,'
                     ':DIMENTION_NAME,'
                     ':NUMBER_OF_MEMBERS,'
                     ':IS_SHARED,'
                     ':SHARED_CPU_IDX'
                    ')'
}

def InsertIntoDB( TableName , ValueList ):
  conn = cx_Oracle.connect( connstr )
  curs = conn.cursor()
  TableName = TableName.upper()
  curs.execute( TABLE[ TableName ], ValueList )
  conn.commit()
  curs.close()
  conn.close()
  return;

def StoreInDictionary( Dictionary, String, Delemeter = '=', flag = 0 ):
  mDictionary = Dictionary
  flag = String.count( Delemeter )
  if( String.find( Delemeter ) != -1 ):
      if flag == 1:
        ( mKey, mValue1 ) = String.split( Delemeter )
      else:
        ( mKey, mValue1, mValue2 ) = String.split( Delemeter )
      if mValue1 not in mDictionary.values():
        mDictionary.setdefault( mKey, mValue1.rstrip() )
  return;


def StoreInList( List, String, Delemeter ):
  mList = List;
  if ( String.find( Delemeter ) != -1 ):
    ( mKey, mValue ) = String.split( Delemeter )
    mList.append( mKey );
  return;


def CfgFileReader( Dictionary, FileName, Delemeter = '=', flag = 0 ):
  mDictionary = Dictionary;
  mFileDescriptor = open( FileName , 'r' )

  for Line in mFileDescriptor:
    StoreInDictionary( mDictionary, Line, Delemeter, flag );

  mFileDescriptor.close();
  return;


def execRemoteCmd( List ):
   for curr_node in List:
      remote_cmd = "sudo -u " + app_id + " ssh " + curr_node + " " + in_cmd_str;
      print ("remote_cmd: \n");
      print (remote_cmd);
      output = subprocess.check_output( remote_cmd, shell=True )
      print(output + "\n");


def ExecuteCmd( command ):
   print ("remote_cmd: "+ command +"\n");
   output = subprocess.check_output( command, shell=True )
   print(output + "\n");
   return;

def dataFormat( input ):
   splitDateList = input.split()
   dateFormat = "";
   delimiter = "-";
   count = 0;
   flag = 0;
   num = 0;
   val ="";
   for item in splitDateList:
     if ( ( count == 2 ) or ( count == 5 ) ):
       delimiter = " "
     if count == 3:
       delimiter = ":"
       if item > 12:
         flag = 1
       num = int( item ) - 12
       item = str( num )
     dateFormat = dateFormat + item + delimiter
     count = count + 1

   if flag == 1:
     dateFormat = dateFormat + "PM"
   else:
     dateFormat = dateFormat + "AM"
   return dateFormat

def DataSizeFormatNumber( number ):
  val = int( number )
  if( val >= ( 1024 * 1024 * 1024 ) ):
    NewVal = val/( 1024*1024*1024 )
    NewValAns = str(NewVal)+'T'
  elif( val >= ( 1024 * 1024 ) ):
    NewVal = val/( 1024 * 1024 )
    NewValAns = str(NewVal)+'G'
  elif( val >= 1024 ):
    NewVal = val/1024
    NewValAns = str(NewVal)+'M'
  elif( val == 0 ):
    return "0"
  else:
    NewValAns = str(val)+'K'
  return NewValAns


def DataSizeFormat( number ):
  val = float( number )
  if( val >= ( 1024 * 1024 * 1024 ) ):
    NewVal = val/( 1024*1024*1024 )
    NewValFormat = float("{0:.2f}".format( NewVal ))
    NewValAns = str( NewValFormat )+'T'
  elif( val >= ( 1024 * 1024 ) ):
    NewVal = val/( 1024 * 1024 )
    NewValFormat = float("{0:.2f}".format( NewVal ))
    NewValAns = str( NewValFormat )+'G'
  elif( val >= 1024 ):
    NewVal = val/1024
    NewValFormat = float("{0:.2f}".format( NewVal ))
    NewValAns = str( NewValFormat )+'M'
  elif( val == 0 ):
    return "0"
  else:
    NewValFormat = float("{0:.2f}".format( val ))
    NewValAns = str( val )+'K'
  return NewValAns

