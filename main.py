#!/usr/bin/python3
import pymysql
import argparse
import os
import logging

parser = argparse.ArgumentParser(description='InnoDB数据库恢复工具')
parser.add_argument('--user','-u', type=str,required=True, default = 'root')
parser.add_argument('--password','-p', type=str,required=True, default = None)
parser.add_argument('--port', '-o',type=str,default = '3306')
parser.add_argument('--srcdir','-s', type=str,required=True, default = None)
parser.add_argument('--destDB','-d', type=str,required=True, default = None)

db = None
cursor = None
datadir = ""
dbname = ""
srcdir = ""
def connectToNewDB(args):
    user = args.user
    password = args.password
    port = args.port
    srcdir = args.srcdir
    dbname = args.destDB
    conn = pymysql.connect('localhost',user,password)
    cursor = conn.cursor()

def setConfigAndCrateRestoreDB():
    sql="SET GLOBAL innodb_file_format='Barracuda';"
    cursor.execute(sql)
    sql="set global innodb_file_per_table='on';"
    cursor.execute(sql)
    sql="set names utf8;"
    cursor.execute(sql)
    sql="create database " + dbname
    cursor.execute(sql)
    sql="select @@datadir"
    cursor.execute(sql)
    results = cursor.fetchall()
    datadir = results[0]
    db.select_db(dbname)

def ReadFrmAndRestoreOneTable(filenameAll):
    tablename = os.path.split(filenameAll).split('.')[0] #获取文件名作表名
    try:
        with open(filenameAll,'rwb') as f:
            b = bytearray(f.read())
            if b[0]!=0xfe or b[1]!=0x01 :
                logging.info("not a table frm")
                return False
            if b[3]!=0x0c:  #table type innodb
                if b[3]==0x09:
                    logging.info("this is a MyISAM db, copy it.")
                    #文件拷贝
                else:
                    logging.info("unknown table type")
                return False
            logging.debug("offset----s---offset")
            offset = struct.unpack('<L',b[6]) #io_size
            logging.debug(offset)
            offset += struct.unpack('<L',b[0x0e]) #tmp_key_length
            logging.debug(offset)
            offset += struct.unpack('<L',b[0x10]) #rec_length
            logging.debug(offset)
            offset += 2 # 00 00
            logging.debug("offset----e---offset")
            length = struct.unpack('<L',b[offset]) #type string length,in word
            offset += 2 # 00 00
            if b[offset:offset+len].decode('utf-8')=='InnoDB':
                logging.info("not an innodb frm")
                return False
            b[3]=6;
            #这里执行数组覆盖复制 貌似不是这么写
            b[offset:offset+6] = [0x4d,0x45,0x4d,0x4f,0x52,0x59]  
            dbname,suffix = os.path.splitext(filenameAll)
            ibdfilepath = dbname + '.idb'
            if not os.path.exists(ibdfilepath):
                logging.info("can not found ibd:"+ibdfilepath)
                return False
            f.write(b)
            sql="flush tables;"
            cursor.execute(sql)
            sql="SHOW CREATE TABLE `" + tablename + "`"
            cursor.execute(sql)
            results = cursor.fetchall()
            createsql = results[1].replace(" ENGINE=MEMORY"," ENGINE=InnoDB")

            sql="drop table `" + tablename + "`"
            cursor.execute(sql)
            cursor.execute(createsql)
            sql="ALTER TABLE `" + tablename + "` DISCARD TABLESPACE;"
            cursor.execute(sql)
            #copy idb
            try:
                sql="ALTER TABLE `" + tablename + "` IMPORT TABLESPACE;"
                cursor.execute(sql)
            except Exception as e:
                logging.error(e)



    except Exception as e:
        logging.error(e)

def __main__():
    args = parser.parse_args()
    connectToNewDB(args)
    for s in os.listdir(srcdir):
        logging.info('restoring:'+s)
        if os.path.splitext(s)[1] =="frm":
            if not (ReadFrmAndRestoreOneTable(s)):
                continue
    db.close()

    



