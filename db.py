# coding=utf-8
# -*- coding: utf-8 -*-
#encoding='utf-8'

import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling
import hashlib
import time

class CrawlDatabaseManager:
  def __init__(self, max_num_thread, **db_config):
    self.dbconfig = {
      "user": db_config['user'],
      "host": db_config['host'],
      "password": db_config['password']
      
    }
    
    self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool",
                                                               pool_size=max_num_thread,
                                                               **self.dbconfig)
    self.DB_NAME = db_config['db_name']
    self.TABLE_NAME = db_config['table_name']
    self.PRIMARY_KEY = db_config['primary_key']
    self.ASSIGN_LENGTH_COLUMN = db_config['assign_length_column']
    self.database = None
    self.table = None
    
  def _get_connection(self):
    try:
      con = self.cnxpool.get_connection()
      cursor = con.cursor(buffered=True)
      print(self.DB_NAME)
      if not self.database:
        try:
          cursor.execute("set names utf8")
          sql = "CREATE DATABASE IF NOT EXISTS %s default character set utf8 COLLATE utf8_general_ci" % (self.DB_NAME,)
          print(sql)
          cursor.execute(sql)
          print("OK")
          self.database = self.DB_NAME
        except mysql.connector.Error as err:
          print("Failed creating database: {}".format(err))
          cursor.close()
          con.close()
      try:
        cursor.execute("USE " + self.DB_NAME)
      except Exception as e:
        if not(cursor.execute("SELECT DATABASE()")):
          print("Error, No databse select!")
    except Exception as e:
      print(e)
      con.close()
      cursor.close()
    return con, cursor
    
  def _create_table(self, savemap, unique=[]):
    not_default_length_field = [x[0] for x in self.ASSIGN_LENGTH_COLUMN]
    save_kyes = list(savemap.keys())
    sql = "CREATE TABLE IF NOT EXISTS " + self.TABLE_NAME + "(Idx INT(22) AUTO_INCREMENT UNIQUE, "
    first_field_statement = str(save_kyes[0]) + " VARCHAR(255) NOT NULL, "
    sql += first_field_statement
    for key_filed in save_kyes[1:]:
      field_length = 255 if key_filed not in not_default_length_field else self.ASSIGN_LENGTH_COLUMN[not_default_length_field.index(key_filed)][1]
      field_statement = str(key_filed) + " VARCHAR(" + str(field_length) + "), "
      sql += field_statement
    if self.PRIMARY_KEY:
      sql += "PRIMARY KEY ("
      for pk in self.PRIMARY_KEY:
        sql += str(pk) + "), "
    for uq in unique:
      sql += "UNIQUE ("+str(uq)+"), "
    sql = sql[:sql.rfind(',')] + ")"
    
    print("#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#")
    try:
      con, cursor = self._get_connection()
      print(cursor.execute("SELECT DATABASE()"))
      print(con, cursor)
      print(sql)
      cursor.execute(sql)
      
      self.table = self.TABLE_NAME
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("create tables error ALREADY EXISTS")
      else:
        print('create tables error ' + err.msg)
    finally:
      cursor.close()
      con.close
  
  def store(self, dic):
    if not self.table:
      self._create_table(dic)
      
    try:
      con, cursor = self._get_connection()
      cursor.execute("set names utf8")

      ROWstr = ''  # 行
      COLstr = ''  # 列
      for key in dic.keys():
        COLstr = (COLstr + '"%s"' + ',') % (key)
        ROWstr = (ROWstr + '"%s"' + ',') % (dic[key])
        
        COLstr = COLstr.replace("\"", "")
      sql = "INSERT INTO %s(%s) VALUES (%s)" % (self.table, COLstr[:-1], ROWstr[:-1])
      print(sql)
      cursor.execute(sql)
      con.commit()
    except Exception as e:
      print(e)
    finally:
      cursor.close()
      con.close()


db_config = {"host":"127.0.0.1", "user":"root", "password":"zyai", "db_name":"test", "table_name":"first","primary_key":["orz"], "assign_length_column":[("a", 100), ("b", 200)]}
dbt  = CrawlDatabaseManager(10, **db_config)
save_map = {'orz':4000, 'a':1, 'b':2, 'c':3, 'd':4, 'e':5}
dbt.store(save_map)
save_map = {'orz':2000, 'a':10, 'b':11, 'c':12, 'd':13, 'd':14}
dbt.store(save_map)
