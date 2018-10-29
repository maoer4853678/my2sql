# my2sql
## 兼容不同数据库引擎的Mysql

  Mysql 目前支持 Mysql，Postgresql 和Sqlite3 引擎，此类便于不同引擎数据库的统一API操作，提高pandas和数据库的连接性能
  预留了 sqlalchemy 的 create_engine方法 扩展
  
  使用步骤：
  
  实例化 Mysql，需要指定数据库引擎
  
  连接 Mysql数据库 
  x = Mysql(engine = 'm',dbname ='dbname',user='root', password='root', host='127.0.0.1') or  x = Mysql(engine = 'mysql')
  
  连接 postgresql 数据库
  x = Mysql(engine = 'p',dbname ='dbname',user='root', password='root', host='127.0.0.1') or  x = Mysql(engine = 'postgre')
  
  连接 sqlite 数据库
  x = Mysql(engine = 's')  ## 内存数据库 'sqlite:///:memory:' 形式 
  x = Mysql(engine = 's', sqlite ='data.db')  ## 本地数据库
  
  
  Mysql方法调用
  # 创建数据表
  x.creat_table(tablename,col,perkey = None,foreign = None,auto = False,default = {}))
    tablename : 数据表名称 [字符串]
    col : 创建表需要的字段配置 [字典] ，key为字段名称，value是 字段类型
    perkey : 创建表的主键
        [字符串] 可以是单一列
        [列表] 多列的列表
    foreign : 创建外键 [字典] ，key为字段名称，value是 外链表名(主键)
    auto : 设置主键为自增长 [布尔]
    default : 设置字段默认值 [字典] ，key为字段名称，value是 字段默认值
    return None

  # 删除数据表
  x.delete_table(tablename,kind=0)
    tablename : 数据表名称 [字符串]
    kind: 删除模式 [整形]
        0 : 即删除数据表，
        1 : 则保留表结构，清空数据
    return None
        
  # 获取数据表列表
  x.list_table()
  
  # 获取数据表字段配置
  x.show_schema(tablename)
    tablename : 数据表名称 [字符串]
    return DataFrame 
    
  # 修改表结构
  x.alter_table(ablename,action,col ={})
    tablename : 数据表名称 [字符串]
    action : 执行动作 [字符串]
        add : 为表增加新字段
        del : 为表删除字段
        update : 为表修改字段类型
    col : 字段配置表 [字典]  key为字段名称，value是 类型 ，若action为del ，value可为空
    return None
  
  # 插入数据
  x.insert_df(tablename,df)
    tablename : 数据表名称 [字符串]
    df [一维/二维 向量/列表]:
        DataFrame: 要插入的数据DataFrame，df.columns需与数据表的字段配置保持一致
        Series: 要插入的数据Series，df.index需与数据表的字段配置保持一致
        [[]]: 要插入的数据[[]]，df的列数需与数据表的字段个数一致
        []: 要插入的数据[]，df的长度需与数据表的字段个数一致
    return None
  
  # 获取数据
  x.show_df(tablename,columns = "*",condition = '',count=-1)
    tablename : 数据表名称 [字符串]
    columns : 筛选字段名称
        默认是 * 全部字段
        [] 部分字段列表
        [字符串] 单个字段或者部分字段

    condition : 条件 [字符串] 默认是 '' 无条件 ，若有筛选条件，输入条件字符串
    count : 返回数据行数，默认-1 即全部行
    return DataFrame 类型
    
  # 删除数据
  x.delete_data(tablename,condition)
    tablename : 数据表名称 [字符串]
    condition : 删除条件 [字典] key为删除条件的字段名称，value是删除条件的字段的值 (=)
          各键直接是与的关系
    return None

  # 更新数据
  x.update_data(tablename,data,condition)
    tablename : 数据表名称 [字符串]
    data : 更新数据 [字典] key为要更新的字段名称，value是更新字段的值
    condition : 更新条件 [字典] key为更新条件的字段名称，value是更新条件的字段的值 (=)
          各键直接是与的关系
    return None

  # 创建主键/外键
  x.creat_key(tablename,perkey=[],foreign ={})
    tablename : 数据表名称 [字符串]
    pperkey : 创建表的主键
        [字符串] 可以是单一列
        [列表] 多列的列表
    foreign : 外键约束 [字典] key为 外键字段，value是关联的其他表 表名(字段)
    return None
    
  # 获取表行数
  x.get_count(tablename)
    tablename : 数据表名称 [字符串]
    return [[]] 类型
  
  # 执行sql语句
  x.exec_(sql)
    sql : sql语句 [字符串]
    return [[]] 类型

