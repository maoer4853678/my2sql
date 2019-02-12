#coding=utf-8
import pandas as pd
import MySQLdb,sqlite3,psycopg2
import numpy as np
import cx_Oracle
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

#class dbobj()

class oracle_obj():
    actions = {"add":"ADD ","del":"drop column ","update":" modify  "}
    
    def __init__(self,dbname ='dbname',user='root', password='root', host='127.0.0.1'):
        self.conn=cx_Oracle.connect('%s/%s@%s/%s'%(user,password,host,dbname))
        
    def list_table(self):
        sql = 'select table_name from user_tables'
        return sql
    
    def show_schema(self,tablename):
        sql = "select COLUMN_NAME,DATA_TYPE,DATA_LENGTH from user_tab_cols where table_name='%s'"%(tablename.upper())
        columns = ['Field','Type','Null']
        return sql,columns
        
    def alter_table(self,tablename,action,col):
        sql ='alter table %s ' %tablename.upper()
        if action=="del":
            sql += ','.join(map(lambda i:"%s %s "%(self.actions[action],i),col.keys()))
        else:
            for i in col.items():
                sql +=" %s %s %s"%(self.actions[action],i[0],i[1])
        return sql

    def colcom(self,g):
        return "("+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))+")"   
      
    def insert_df(self,tablename, df,columns):
        sql = '''into %s (%s) values %s '''
        df1 = df.apply(lambda x:sql%(tablename,','.join(columns),\
                                     str(tuple(x.values.tolist()))),axis=1)
        sql1 = 'insert all '+ ' \n '.join(df1.values)+"\nselect 1 from dual "
        return sql1
        

class postgre_obj():
    actions = {"add":"ADD ","del":"DROP COLUMN ","update":" ALTER "}
    
    def __init__(self,dbname ='dbname',user='root', password='root', host='127.0.0.1'):
        self.conn =psycopg2.connect( user=user, password=password, host=host,database =dbname)
    
    def list_table(self):
        sql = "select tablename from pg_tables where schemaname='public'"
        return sql
        
    def show_schema(self,tablename):
        sql = '''SELECT col_description(a.attrelid,a.attnum) as comment,format_type(a.atttypid,a.atttypmod)
                        as type,a.attname as name, a.attnotnull as notnull FROM pg_class as c,pg_attribute as a 
                    where c.relname = '%s' and a.attrelid = c.oid and a.attnum>0  '''%(tablename.lower())
        columns = ['Null','Type',"Field",'Key']
        return sql,columns
    
    def alter_table(self,tablename,action,col):
        sql = 'alter table %s ' %tablename
        if action=="del":
            sql += ','.join(map(lambda i:"%s %s "%(self.actions[action],i),col.keys()))
        if action=="add":
            sql += ','.join(map(lambda i:"%s %s %s"%(self.actions[action],i[0],i[1]),col.items()))
        if action=="update":
            sql += ','.join(map(lambda i:"%s %s type %s using %s::%s"%(self.actions[action],i[0],i[1],i[0],i[1]),col.items()))
        return sql
        
    def colcom(self,g):
        return "("+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))+")"   
   
    def insert_df(self,tablename, df,columns):
        col = ','.join(["%s" for i in range(len(columns))]) % (tuple(columns))      
        col1 = ','.join(df.apply(self.colcom,axis = 1))
        sql = "INSERT INTO %s (%s) values %s;" % (tablename,col,col1)
        return sql
        
        
class mysql_obj():
    actions = {"add":"ADD ","del":"DROP COLUMN ","update":' modify column'}
    
    def __init__(self,dbname ='dbname',user='root', password='root', host='127.0.0.1'):
        self.conn = MySQLdb.connect(host,user,password,dbname)

    def list_table(self):
        sql = "show tables"
        return sql 
        
    def show_schema(self,tablename):
        sql = "desc %s"%(tablename.lower())
        columns = ['Field','Type','Null','Key','Default','Extra']
        return sql,columns
    
    def alter_table(self,tablename,action,col):
        sql = 'alter table %s ' %tablename
        if action=="del":
            sql += ','.join(map(lambda i:"%s %s "%(self.actions[action],i),col.keys()))
        else:
            sql += ','.join(map(lambda i:"%s %s %s"%(self.actions[action],i[0],i[1]),col.items()))
        return sql
    
    def colcom(self,g):
        return "("+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))+")"    
    
    def insert_df(self,tablename, df,columns):
        col = ','.join(["%s" for i in range(len(columns))]) % (tuple(columns))      
        col1 = ','.join(df.apply(self.colcom,axis = 1))
        sql = "INSERT INTO %s (%s) values %s;" % (tablename,col,col1)
        return sql
    
        
class sqlite_obj():
    actions = {"add":"ADD "}
    
    def __init__(self,dbname ='dbname',user='root', password='root', host='127.0.0.1'):
        if dbname=='memory' or dbname=='':
            self.conn = sqlite3.connect(':memory:')
        else:
            if '.db' not in dbname and '.sqlite' not in dbname:
                dbname = dbname+'.db'
            self.conn = sqlite3.connect(dbname)
            
    def list_table(self):
        sql = "SELECT name FROM sqlite_master WHERE type='table' order by name"
        return sql 
        
    def show_schema(self,tablename):
        sql = "PRAGMA table_info(%s)"%(tablename.lower())
        columns = ['Id','Field','Type','Extra','Null','Key']
        return sql,columns      
    
    def alter_table(self,tablename,action,col):
        for i in col.items():
            sql ='alter table %s ' %tablename+"%s %s %s"%(self.actions[action],i[0],i[1])
        return sql

    def colcom(self,g):
        return ' UNION ALL SELECT '+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))
         
    def insert_df(self,tablename, df,columns):
        col = ','.join(["%s" for i in range(len(columns))]) % (tuple(columns))
        col1 = ''.join(df.apply(self.colcom,axis = 1))
        col1 = col1.replace("UNION ALL SELECT",'SELECT',1)
        sql = "INSERT INTO %s (%s) %s;" % (tablename,col,col1)
        return sql
        

class Mysql_obj():
    def __init__(self,engine,dbname ='dbname',user='root', password='root', host='127.0.0.1'):
        u'''
        初始化Mysql类, 支持 Mysql，Postgresql 和Sqlite3 引擎
        engine :数据库引擎
            mysql : Mysql数据库
            oracle : Oracle数据库
            postgresql : Postgresql数据库
            sqlite : Sqlite 轻量级数据库 本地文件/内存
            
        sqlite :针对sqlite引擎适用，默认为空，即内存数据库，如果是.db或.sqlite文件路径，则是本地文件数据库
        dbname :数据库名称
        user :用户名
        password :密码
        host :数据库地址  
        '''
        self.perkey ={}
        engins = {"m":"m","mysql":"m","postgresql":"p","postgre":"p","p":"p","sqlite":'s',"s":"s",'o':"o",'oracle':"o"}
        objs = {"o": oracle_obj,"p":postgre_obj,"m":mysql_obj,"s":sqlite_obj}
        if engine.lower() in engins:
            self.enginetype = engins[engine.lower()]
            self.obj = objs[self.enginetype](dbname,user,password,host)
            self.cur = self.obj.conn.cursor()
            try:
                self.conn.set_client_encoding('UTF8')
            except:
                pass
        else:
            print (u'初始化失败 未能找到相应数据库引擎，请输入 help(Mysql) 进行查询')

    def execute(self,sql,msg=u'SQL语句执行成功',error=u'SQL语句执行失败: '):
        try:
            self.cur.execute(sql)
            self.obj.conn.commit()
            print (msg)
        except Exception as e:
            print (error,e)
            print ("Error : ",sql)
            self.obj.conn.rollback()
    
    def exec_(self,sql):
        u'''
        执行sql语句
        sql : sql语句 [字符串]
        '''
        self.execute(sql,u'SQL语句执行成功',u'SQL语句执行失败: ')
        rows = self.cur.fetchall()
        return rows  

    def list_table(self):
        u'''
        获取数据表列表
        '''
        sql = self.obj.list_table()
        self.execute(sql,u'获取数据表成功',u'获取数据表失败: ')
        rows = [i[0] for i in self.cur.fetchall()]
        return rows     

    def creat_table(self,tablename,col,perkey = None,default = {}):
        u'''
        创建数据表
        tablename : 数据表名称 [字符串]
        col : 创建表需要的字段配置 [字典] ，key为字段名称，value是 字段类型
        perkey : 创建表的主键
            [字符串] 可以是单一列
            [列表] 多列的列表
        foreign : 创建外键 [字典] ，key为字段名称，value是 外链表名(主键)
        auto : 设置主键为自增长 [布尔]
        default : 设置字段默认值 [字典] ，key为字段名称，value是 字段默认值
        '''
        if tablename in self.list_table():
            print (u'数据表创建失败: %s已存在'%(tablename))
        else:
            if perkey!= None:
                if type(perkey)==type([]):
                    perkey = ','.join(perkey)
                Psql = ',PRIMARY KEY (%s)' % (perkey)
            else:
                Psql =''
            defs = {'m':" default '%s'",'s':" default('%s')",'p':" default('%s')",'o':" default(%s)"} 
            for key in default.keys():
                col[key] = col[key]+ defs[self.enginetype]%default[key]
                   
            sql = ','.join(map(lambda i:"%s %s"%(i[0],i[1]),col.items()))
            sql = '''CREATE TABLE %s (%s%s) ''' % (tablename,sql,Psql)
            self.execute(sql,u'数据表创建成功',u'数据表创建失败: ')

    def delete_table(self,tablename,kind=0):
        u'''
        删除数据表
        tablename : 数据表名称 [字符串]
        kind: 删除模式 [整形]
            0 : 即删除数据表，
            1 : 则保留表结构，清空数据
        '''
        k = 'DROP' if kind==0 else "truncate"
        sql = '%s TABLE %s ' % (k,tablename)
        self.execute(sql,u'删除表成功',u'删除表创建失败: ')
            
    def show_schema(self,tablename):
        u'''
        获取数据表字段配置
        tablename : 数据表名称 [字符串]
        '''
        sql,columns= self.obj.show_schema(tablename)
        self.execute(sql,u'获取表结构成功',u'获取表结构失败: ')
        rows = self.cur.fetchall()
        df = pd.DataFrame(list(rows),columns = columns)
        df = df[df['Type']!='-']
        return df
           
    def alter_table(self,tablename,action,col ={} ):
        u'''
        修改表结构
        tablename : 数据表名称 [字符串]
        action : 执行动作 [字符串]
            add : 为表增加新字段
            del : 为表删除字段
            update : 为表修改字段类型
        col : 字段配置表 [字典]  key为字段名称，value是 类型 ，若action为del ，value可为空
        '''
        if action in self.obj.actions:
            sql = self.obj.alter_table(tablename,action,col)
            self.execute(sql,u'修改表结构成功',u'修改表结构失败: ')     
        else:
           print (u'修改表结构失败: 执行动作不存在')
           
    def insert_df(self,tablename,df):
        u'''
        插入数据
        tablename : 数据表名称 [字符串]
        df [一维/二维 向量/列表]:
            DataFrame: 要插入的数据DataFrame，df.columns需与数据表的字段配置保持一致
            Series: 要插入的数据Series，df.index需与数据表的字段配置保持一致
            [[]]: 要插入的数据[[]]，df的列数需与数据表的字段个数一致
            []: 要插入的数据[]，df的长度需与数据表的字段个数一致
        '''
        bz =True
        if type(df) == type([]):
            columns = self.show_schema(tablename)["Field"].tolist()
            if type(df[0]) == type([]):
                bz= (len(columns)==len(df[0]))
                df = pd.DataFrame(df,columns = columns)
            else:
                bz= (len(columns)==len(df))
                df = pd.Series(df,index = columns).to_frame().T
            
        elif type(df) == type(pd.Series()):
            columns = df.index.tolist()
            df = df.to_frame().T
            
        elif type(df) == type(pd.DataFrame()):
            columns = df.columns.tolist()

            if bz:
                ## sqlite 批量插入只支持一次 500条数据
                sql = self.obj.insert_df(tablename,df,columns)
                self.execute(sql,u'插入数据表成功 %d行 ' % (len(df)),u'插入数据失败: ')
            
    def show_df(self,tablename,columns = "*",condition = '',count=-1):
        u'''
        获取数据
        tablename : 数据表名称 [字符串]
        columns : 筛选字段名称
            默认是 * 全部字段
            [] 部分字段列表
            [字符串] 单个字段或者部分字段
    
        condition : 条件 [字符串] 默认是 '' 无条件 ，若有筛选条件，输入条件字符串
        tablename : 数据表名称 [字符串]
        df [一维/二维 向量/列表]:
            DataFrame: 要插入的数据DataFrame，df.columns需与数据表的字段配置保持一致
            Series: 要插入的数据Series，df.index需与数据表的字段配置保持一致
            [[]]: 要插入的数据[[]]，df的列数需与数据表的字段个数一致
            []: 要插入的数据[]，df的长度需与数据表的字段个数一致
        '''
        if type(columns) == type([]):
            columns = ','.join(columns)
        sql = 'select %s from %s '% (columns,tablename)
        if condition!="":
            if "where" not in condition.lower() :
                condition = "WHERE "+condition
            sql += " "+condition    
        if count>=0:
            sql += " LIMIT %d"%count

        self.execute(sql,u'获取数据成功',u'获取数据失败: ')
        rows = self.cur.fetchall()
        columns = self.show_schema(tablename)['Field']
        df = pd.DataFrame(list(rows),columns = columns) ##  pd.read_sql_query(sql,self.obj.conn)
        return df
            
    def update_data(self,tablename,data,condition):
        u'''
        更新数据
        tablename : 数据表名称 [字符串]
        data : 更新数据 [字典] key为要更新的字段名称，value是更新字段的值
        condition : 更新条件 [字典] key为更新条件的字段名称，value是更新条件的字段的值 (=)
              各键直接是与的关系
        '''
        chemainit = self.show_schema(tablename)
        chemainit['Field'] = chemainit['Field'].str.lower()
        chemainit = dict(zip(chemainit['Field'],chemainit['Type']))
        col = []
        for i in range(len(data.keys())):
            if "int" in chemainit[data.items()[i][0]].lower() or 'double' in chemainit[data.items()[i][0]].lower():
                tmp = "%s = %s"
            else:
                tmp = "%s = '%s'"
            col.append(tmp)
        col = ' , '.join(col)
        col1 = []
        for i in range(len(condition.keys())):
            if 'int' in chemainit[condition.items()[i][0]].lower() or 'double' in chemainit[condition.items()[i][0]].lower():
                tmp = "%s = %s"
            else:
                tmp = "%s = '%s'"
            col1.append(tmp)
        col1 = ' and '.join(col1)
        sql = '''UPDATE %s SET %s WHERE %s'''% (tablename,col,col1)                                       
        tmp = []
        [tmp.extend(i) for i in data.items()]
        [tmp.extend(i) for i in condition.items()]
        sql = sql % tuple(tmp)
        self.execute(sql,u'更新数据成功',u'更新数据失败: ')
            
    def creat_key(self,tablename,perkey=[],foreign ={}):
        u'''
        创建主键/外键
        tablename : 数据表名称 [字符串]
        pperkey : 创建表的主键
            [字符串] 可以是单一列
            [列表] 多列的列表
        foreign : 外键约束 [字典] key为 外键字段，value是关联的其他表 表名(字段)
        
        eg:
            creat_key("test",perkey={"per":"col1"},foreign ={"for":['col2','test1(col1)']})
        '''
        if self.enginetype=='s':
            print (u'创建主键/外键失败 : sqlite 数据库暂不支持该操作')
        else:
            if len(perkey)!=0:
                if type(perkey)==type([]):
                    perkey = ','.join(perkey)
                sql = 'alter table %s add primary key(%s)' %(tablename,perkey)
                self.execute(sql,u'创建主键成功',u'创建主键失败: ')
                    
            if len(foreign)!=0:
                for key in foreign:
                    sql = 'alter table %s add foreign key (%s) references %s' %(tablename,key,foreign[key])
                self.execute(sql,u'创建外键成功',u'创建外键失败: ')
                    
    def get_count(self,tablename):
        u'''
        获取表行数
        tablename : 数据表名称 [字符串]
        '''
        sql ='select count(*) from %s'%tablename
        self.execute(sql,u'获取表行数成功',u'获取表行数失败: ')
        rows = self.cur.fetchone()
        return rows[0]
        
    def close(self):
        u'''关闭数据库'''
        self.obj.conn.close()
        
        
        
        
