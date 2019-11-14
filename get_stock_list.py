import cx_Oracle
import numpy as np
import pandas as pd
import datetime as dt
import sys


class OracleSql(object):
    '''
    Oracle数据库数据访问

    '''
    def __init__(self):
        '''
        初始化数据库连接
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'

    def __enter__(self):
        self.conn = self.__connect_to_oracle()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __connect_to_oracle(self):
        dsn = self.host + ':' + self.oracle_port + '/' + self.db
        try:
            connection = cx_Oracle.connect(self.user, self.pwd, dsn, encoding="UTF-8", nencoding="UTF-8")
            connection.current_schema = self.current_schema
            print('连接oracle数据库')
        except Exception:
            print('不能连接oracle数据库')
            connection = None
        return connection

    def query(self, sql):
        '''
        查询并返回数据

        '''
        return pd.read_sql(sql, self.conn)

    def execute(self, sql):
        '''
        对数据库执行插入、修改等数据上行操作

        '''
        self.conn.cursor().execute(sql)
        self.conn.commit()


def stock_pool():
    sql_line1 = '''
            SELECT
                S_INFO_WINDCODE
            FROM
                ASHAREST 
            WHERE
                REMOVE_DT IS NULL 
    	'''

    sql_line2 = '''
            SELECT
                S_INFO_WINDCODE
            FROM
                wind.ASHAREDESCRIPTION 
            WHERE
                S_INFO_DELISTDATE IS NULL 
                AND substr( S_INFO_WINDCODE, 1, 1 ) != 'A'
            '''

    sql_line3 = '''
            SELECT
                S_INFO_WINDCODE, S_INFO_NAME
            FROM
                ASHAREDESCRIPTION
            '''

    with OracleSql() as sql:
        STStock_df = sql.query(sql_line1)
        ListedStock_df = sql.query(sql_line2)
        stock_name_df = sql.query(sql_line3)
    forbidden_set = set(STStock_df["S_INFO_WINDCODE"].tolist()) | {"601901.SH", "600601.SH", "002581.SZ", "000788.SZ",
                                                                   "600730.SH", "600285.SH"}
    listed_stock_set = set(ListedStock_df["S_INFO_WINDCODE"].tolist())
    ticker_list = sorted(list(listed_stock_set - forbidden_set))
    ticker_df = pd.DataFrame(ticker_list, columns=["ticker",])
    stock_name_df.columns=["ticker", "name"]
    pool = pd.merge(ticker_df, stock_name_df, on="ticker")
    pool.to_csv("股票池-" + str(dt.datetime.now().date()) + ".csv", index=None, encoding="gbk")
    return pool



if __name__ == "__main__":
    print(stock_pool())