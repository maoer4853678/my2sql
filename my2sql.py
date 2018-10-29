#coding=utf-8
import pandas as pd
from sqlalchemy import create_engine
import MySQLdb,sqlite3,psycopg2

class Mysql():
    def __init__(self,engine,sqlite = None,dbname ='dbname',user='root', password='root', host='127.0.0.1'):
        u'''
        初始化Mysql类, 支持 Mysql，Postgresql 和Sqlite3 引擎
        engine :数据库引擎
            mysql : Mysql数据库
            postgresql : Postgresql数据库
            sqlite : Sqlite 轻量级数据库 本地文件/内存
            
        sqlite :针对sqlite引擎适用，默认为空，即内存数据库，如果是.db或.sqlite文件路径，则是本地文件数据库
        dbname :数据库名称
        user :用户名
        password :密码
        host :数据库地址  
        '''
        engins = {"m":"mysql","mysql":"mysql"}
        engins1 = {"postgresql":"postgresql","postgre":"postgresql","p":"postgresql"}
        self.perkey = {}
        if engine in engins.keys():
            self.engine = create_engine('%s://%s:%s@%s/%s?charset=utf8'%(engins[engine],user,password,host,dbname))
            self.enginetype = 'm'
            self.conn = MySQLdb.connect(host,user,password,dbname)
        elif engine in engins1.keys():
            self.engine = create_engine('%s://%s:%s@%s/%s?charset=utf8'%(engins1[engine],user,password,host,dbname))
            self.enginetype = 'p'
            self.conn =psycopg2.connect( user=user, password=password, host=host,database =dbname)
        elif engine == 'sqlite' or engine == 's' :
            self.enginetype = 's'
            if not sqlite:
                self.engine = create_engine('sqlite:///:memory:')
                self.conn = sqlite3.connect(':memory:')
            else:
                self.engine = create_engine('sqlite:////%s'%sqlite)
                self.conn = sqlite3.connect(sqlite)
        else:
            print (u'初始化失败 未能找到相应数据库引擎，请输入 help(Mysql) 进行查询')

        self.cur = self.conn.cursor()
        try:
            self.conn.set_client_encoding('UTF8')
        except:
            pass

    def creat_table(self,tablename,col,perkey = None,foreign = None,auto = False,default = {}):
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
            try:
                if perkey!= None:
                    if type(perkey)==type([]):
                        perkey = ','.join(perkey)
                    Psql = ',PRIMARY KEY (%s)' % (perkey)
                else:
                    Psql =''
                if foreign!= None:           
                    Fsql = ","+",".join(map(lambda i:"foreign key(%s) references %s"%(i[0],i[1]),foreign.items()))
                else:
                    Fsql = ''
                if auto and perkey!= None:
                    if self.enginetype == "p":
                        col[perkey] = " SERIAL"
                    else:
                        col[perkey] =  col[perkey]+ ' AUTO_INCREMENT'

                defs = {'m':" default '%s'",'s':" default('%s')",'p':" default('%s')"}
                for key in default.keys():
                    col[key] = col[key]+ defs[self.enginetype]%default[key]

                sql = ','.join(map(lambda i:"%s %s"%(i[0],i[1]),col.items()))
                creat_sql = '''CREATE TABLE %s (%s%s%s) ;''' % (tablename,sql,Psql,Fsql)
                self.cur.execute(creat_sql)
                self.conn.commit()
                print (u'数据表创建成功')
                self.perkey[tablename] = perkey
            except Exception,e:
                print (u'数据表创建失败: ',e)
                self.conn.rollback()
            

    def delete_table(self,tablename,kind=0):
        u'''
        删除数据表
        tablename : 数据表名称 [字符串]
        kind: 删除模式 [整形]
            0 : 即删除数据表，
            1 : 则保留表结构，清空数据
        '''
        try:
            k = 'DROP' if kind==0 else "truncate"
            sql = '%s TABLE %s ;' % (k,tablename)
            self.cur.execute(sql)
            self.conn.commit()
            print (u'删除表成功')
        except Exception,e:
            print (u'删除表创建失败: ',e)
            self.conn.rollback()

    def list_table(self):
        u'''
        获取数据表列表
        '''
        lists = {"m":"show tables","p":"select tablename from pg_tables where schemaname='public'","s":"SELECT name FROM sqlite_master WHERE type='table' order by name"}
        self.cur.execute(lists[self.enginetype])
        rows = [i[0] for i in self.cur.fetchall()  ]
        return rows
        
    def exec_(self,sql):
        u'''
        执行sql语句
        sql : sql语句 [字符串]
        '''
        try:
            self.cur.execute(sql)
            rows = self.cur.fetchall()
            self.conn.commit()
            return rows
        except Exception,e:
            print (u'SQL语句执行失败: ',e)
            self.conn.rollback()
            
    def show_schema(self,tablename):
        u'''
        获取数据表字段配置
        tablename : 数据表名称 [字符串]
        '''
        lists = {"m":{"sql":"desc %s","columns":['Field','Type','Null','Key','Default','Extra']},\
                 "p":{"sql":'''SELECT col_description(a.attrelid,a.attnum) as comment,format_type(a.atttypid,a.atttypmod)
                        as type,a.attname as name, a.attnotnull as notnull FROM pg_class as c,pg_attribute as a 
                    where c.relname = '%s' and a.attrelid = c.oid and a.attnum>0  ''',"columns":['Null','Type',"Field",'Key']},\
                 "s":{"sql":"PRAGMA table_info(%s)","columns":['Id','Field','Type','Extra','Null','Key']}
                 }
        try:
            self.cur.execute(lists[self.enginetype]["sql"]%(tablename.lower()))
            rows = self.cur.fetchall()
            df = pd.DataFrame(list(rows),columns = lists[self.enginetype]["columns"])
            df = df[df['Type']!='-']
            return df
        except Exception,e:
            print (u'获取表结构失败: ',e)
            self.conn.rollback()

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
        
        sql = 'alter table %s ' %tablename
        actions = {"add":"ADD ","del":"DROP COLUMN ","update":" ALTER "}
        bz = True
        if action in actions:
            try:
                if self.enginetype=='s':
                    if action=="add":
                        for i in col.items():
                            sql ='alter table %s ' %tablename+"%s %s %s"%(actions[action],i[0],i[1])
                            self.cur.execute(sql)
                    else:
                        bz =False
                        print u'修改表结构失败 : sqlite 数据库暂不支持该操作'
                
                if self.enginetype=='p':
                    if action=="del":
                        sql += ','.join(map(lambda i:"%s %s "%(actions[action],i),col.keys()))
                    if action=="add":
                        sql += ','.join(map(lambda i:"%s %s %s"%(actions[action],i[0],i[1]),col.items()))
                    if action=="update":
                        sql += ','.join(map(lambda i:"%s %s type %s using %s::%s"%(actions[action],i[0],i[1],i[0],i[1]),col.items()))
                    self.cur.execute(sql)
                    
                if self.enginetype=='m':
                    actions["update"] = ' modify column'
                    if action=="del":
                        sql += ','.join(map(lambda i:"%s %s "%(actions[action],i),col.keys()))
                    else:
                        sql += ','.join(map(lambda i:"%s %s %s"%(actions[action],i[0],i[1]),col.items()))
                    self.cur.execute(sql)
                      
                self.conn.commit()
                msg=  u'修改表结构成功' if bz else u'修改表结构失败'
                print (msg)
            except Exception,e:
                print (u'修改表结构失败: ',e)
                self.conn.rollback()            
        else:
           print u'修改表结构失败: 执行动作不存在'

    def colcom(self,g):
        if self.enginetype != 's':
            return "("+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))+")"
        else:
            return ' UNION ALL SELECT '+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))
           
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
        try:
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
                col = ','.join(["%s" for i in range(len(columns))]) % (tuple(columns))
                if self.enginetype=='s':
                    col1 = ''.join(df.apply(self.colcom,axis = 1))
                    col1 = col1.replace("UNION ALL SELECT",'SELECT',1)
                    sql = "INSERT INTO %s (%s) %s;" % (tablename,col,col1)
                else:
                    col1 = ','.join(df.apply(self.colcom,axis = 1))
                    sql = "INSERT INTO %s (%s) values %s;" % (tablename,col,col1)
                    
                self.cur.execute(sql)
                self.conn.commit()
                print (u'插入数据表成功 %d行 ' % (len(df)))
            else:
                print (u'插入数据失败，输入的列表长度和数据表结构不符')
        
        except Exception,e:
            print (u'插入数据失败: ',e)
            self.conn.rollback()
            
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
        try:
            if type(columns) == type([]):
                columns = ','.join(columns)
            select_sql = 'select %s from %s '% (columns,tablename)
            if condition!="":
                if "where" not in condition.lower() :
                    condition = "WHERE "+condition
                select_sql = select_sql+ " "+condition    
            if count>=0:
                select_sql = select_sql+" LIMIT %d"%count
            df = pd.read_sql_query(select_sql,self.conn)
            self.conn.commit()
            return df
            
        except Exception,e:
            print (u'获取数据失败: ',e)
            self.conn.rollback()     
        
    
    def delete_data(self,tablename,condition):
        u'''
        删除数据
        tablename : 数据表名称 [字符串]
        condition : 删除条件 [字典] key为删除条件的字段名称，value是删除条件的字段的值 (=)
              各键直接是与的关系
        '''
        sql = ' and '.join(["%s = '%s'" for i in range(len(condition.keys()))])
        sql = '''delete from %s where %s'''% (tablename,sql)
        tmp = []
        [tmp.extend(i) for i in condition.items()]
        delete_sql = sql % tuple(tmp)
        try:
            self.cur.execute(delete_sql)
            self.conn.commit()
            print (u'删除数据成功' )
        except Exception,e:
            print (u'删除数据失败: ' ,e)
            self.conn.rollback()

    def update_data(self,tablename,data,condition):
        u'''
        更新数据
        tablename : 数据表名称 [字符串]
        data : 更新数据 [字典] key为要更新的字段名称，value是更新字段的值
        condition : 更新条件 [字典] key为更新条件的字段名称，value是更新条件的字段的值 (=)
              各键直接是与的关系
        '''
        chemainit = self.show_schema(tablename)
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
        updata_sql = sql % tuple(tmp)
        try:
            self.cur.execute(updata_sql)
            self.conn.commit()
            print (u'更新数据成功' )
        except Exception,e:
            print (u'更新数据失败: ',e)
            self.conn.rollback()

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
                try:
                    self.cur.execute(sql)
                    self.conn.commit()
                    print (u'创建主键成功')
                except Exception,e:
                    print (u'创建主键失败: ',e)
                    self.conn.rollback()
                    
            if len(foreign)!=0:
                try:
                    for key in foreign:
                        sql = 'alter table %s add foreign key (%s) references %s' %(tablename,key,foreign[key])
                        self.cur.execute(sql)
                    self.conn.commit()
                    print (u'创建外键成功')
                except Exception,e:
                    print (u'创建外键失败: ',e)
                    self.conn.rollback()
                    
    def get_count(self,tablename):
        u'''
        获取表行数
        tablename : 数据表名称 [字符串]
        '''
        sql ='select count(*) from %s'%tablename
        try:
            self.cur.execute(sql)
            rows = self.cur.fetchone()
            return rows[0]
        except Exception,e:
            print (u'获取表行数失败: ',e)
            self.conn.rollback()
     

