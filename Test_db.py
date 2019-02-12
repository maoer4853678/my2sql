# -*- coding: utf-8 -*-
"""
Created on Fri Feb 01 14:11:39 2019

@author: dongz
"""
import my2sql
import pandas as pd


conn = my2sql.Mysql_obj(user='root',password='root',engine='s',dbname='test1')

print (conn.list_table())
conn.creat_table('test',col={"name":"text","age":"text"})
conn.creat_key('test',perkey='name')
conn.alter_table('test','update',{"age":"int"})
sc = conn.show_schema("pathdata")
print (sc)
df = pd.DataFrame([['a',1]],columns = ['name','age'])
conn.insert_df('test',df)
conn.update_data('test',{"age":2},{"name":'a'})
df1 = conn.show_df("test")
print (df1)
conn.delete_table('test',kind=1)
print (conn.get_count('test'))
conn.delete_table('test')
