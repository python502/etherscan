#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : MysqldbOperate.py
# @Software: PyCharm
# @Desc    :
import MySQLdb
import decimal
from logger import logger
MYSQL_BATCH_NUM = 20
class MysqldbOperate(object):
    '''
    classdocs
    '''
    __instance = None
    def __new__(cls, *args, **kwargs):
        if MysqldbOperate.__instance is None:
            MysqldbOperate.__instance = object.__new__(cls, *args, **kwargs)
        return MysqldbOperate.__instance

    def __init__(self,dict_mysql):
        self.conn = None
        self.cur = None
        if not dict_mysql.has_key('host') or not dict_mysql.has_key('user') or not dict_mysql.has_key('passwd')\
         or not dict_mysql.has_key('db') or not dict_mysql.has_key('port'):
            logger.error('input parameter error')
            raise ValueError
        else:
            try:
                self.conn = MySQLdb.connect(host=dict_mysql['host'], user=dict_mysql['user'], \
                                    passwd=dict_mysql['passwd'], db=dict_mysql['db'],port=dict_mysql['port'],charset='utf8',connect_timeout=30)
                # self.cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
                #更改为流式游标，查询数据也改为使用生成器
                self.cur = self.conn.cursor(MySQLdb.cursors.SSCursor)
            except Exception,e:
                logger.error('__init__ fail:{}'.format(e))
                raise
    
    def __del__(self):
        self.cur.close()
        self.cur = None
        self.conn.close()
        self.conn = None
        
    def _select_infos(self, cur):
        result = cur.fetchone()
        while result:
            yield result
            result = cur.fetchone()
        return

    def sql_query(self,sql):
        try:
            if not sql:
                raise ValueError('select sql not input')
            self.cur.execute(sql)
            select_info = self._select_infos(self.cur)
            return select_info
        except Exception, e:
            logger.error('sql_query error:{}'.format(e))
            logger.error('sql_query select sql:{}'.format(sql))
            raise

    def sql_exec(self, sql, value=''):
        if not sql:
            return False
        try:
            if value:
                self.cur.execute(sql,value)
            else:
                self.cur.execute(sql)
            self.conn.commit()
            return True
        except Exception, e:
            self.conn.rollback()
            logger.error('sql_exec error:{}'.format(e))
            return False

    '''
    function:insert_batch 批量insert or replace数据
    @param operate_type:sql 操作 insert or replace
    @param table:表名
    @param columns:表的操作的列信息
    @param datas:数据信息 [{},{}]
    @return '检索结果'
    '''
    def insert_batch(self, operate_type, table, columns, datas):
        exec_sql = '{} INTO {}({}) VALUES'.format(operate_type.upper(), table, ','.join(columns))
        batch_list = []
        counts = 0
        sql = ''
        try:
            for item in datas:
                batch_list.append(self.__multipleRows(columns, item))
                try:
                    if len(batch_list) == MYSQL_BATCH_NUM:
                        sql = "%s %s " % (exec_sql, ','.join(batch_list))
                        logger.debug('sql:{}'.format(sql))
                        self.cur.execute(sql)
                        self.conn.commit()
                        batch_list = []
                        counts += MYSQL_BATCH_NUM
                except Exception,e:
                    self.conn.rollback()
                    logger.error('sql:{}'.format(sql))
                    logger.error('e:{}'.format(e))
                    continue
            if len(batch_list):
                sql = "%s %s " % (exec_sql, ','.join(batch_list))
                self.cur.execute(sql)
                self.conn.commit()
            counts += len(batch_list)
            logger.info('finished {}: {}'.format(exec_sql[0], counts))
            if counts:
                return True
            else:
                return False
        except Exception, e:
            self.conn.rollback()
            logger.error('sql:{}'.format(sql))
            logger.error('e:{}'.format(e))
            return False

    '''
    function: 返回可用于multiple rows的sql拼装值
    @columns：list 表的列信息
    @params：dict 每行的数据信息
    @return: True or raise
    '''
    def __multipleRows(self, columns, params):
        try:
            ret = []
            # 根据不同值类型分别进行sql语法拼装
            for column in columns:
                param = params.get(column.lower())
                if param == 0:
                    ret.append(str(param))
                    continue
                if not param:
                    ret.append('""')
                    continue
                if isinstance(param, (int, long, float, bool, decimal.Decimal)):
                    ret.append(str(param))
                elif isinstance(param, str):
                    param = param.replace('"','\'')
                    ret.append('"' + param + '"')
                elif isinstance(param, unicode):
                    param = param.replace('"', '\'')
                    ret.append('"' + param.encode('utf8') + '"')
                else:
                    logger.error('unsupport value: '.format(param))
            return '(' + ','.join(ret) + ')'
        except Exception,e:
            logger.error('__multipleRows error:{}'.format(e))
            raise
def main():
    DICT_MYSQL={'host':'127.0.0.1','user':'root','passwd':'111111','db':'capture','port':3306}
    omysql = MysqldbOperate(DICT_MYSQL)
    sql = 'SELECT * FROM capture.transfer_token_no where from_account="s"'
    g = omysql.sql_query(sql)
    print g.next()
    for x in g:
        print x
def main1():
    DICT_MYSQL={'host':'127.0.0.1','user':'root','passwd':'111111','db':'capture','port':3306}
    omysql = MysqldbOperate(DICT_MYSQL)
    columns = ['metastasis_info','patent_id','create_date']
    type = 'insert'
    table = 'website_servicepatent'


    datas=[{'metastasis_info':1,'patent_id':2,'create_date':'2016-07-12 21:14:38'},{'metastasis_info':4,},{'metastasis_info':2,'create_date':'2016-07-12 21:14:38'},{'metastasis_info':4,'patent_id':5,'create_date':u'20142016-07-12 21:14:38555'}]
    omysql.insert_batch(type,table,columns,datas)



def main2():
    DICT_MYSQL={'host':'127.0.0.1','user':'root','passwd':'111111','db':'capture','port':3306}
    omysql = MysqldbOperate(DICT_MYSQL)
    sql = 'INSERT INTO market_varify_raw(IMAGE_URL,VARIFY_CODE) VALUES (%s,%s)'
    data = ('1','d')
    omysql.sql_exec(sql , data)
if __name__ == '__main__':
    main()