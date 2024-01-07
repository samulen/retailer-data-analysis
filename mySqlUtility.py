# -*- coding: utf-8 -*-
"""
Created on Sat Nov 11 18:51:13 2023

@author: SL01

Routine per connessione a MySQL 
conn_mysql

Rimpiazza la tabella su MySQL a partire da un df
replace_table_fromdf(df, table)

"""

import constants as c

import mysql.connector as mysql
import sqlalchemy 


def conn_mysql():
    mydb = mysql.connect(
        host = 'localhost',
        user = c.database_username,
        passwd = c.database_password
        )
    return mydb


def close(mydb):
    mydb.close()
    
    

"""
Sostituzione tabella da df
"""
def append_df_to_table(df, table):
    database_username = c.database_username
    database_password = c.database_password
    database_ip       = c.database_ip
    database_name     = c.database_name
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    df.to_sql(name=table, con=database_connection, 
                       if_exists='append', index=False)
    
