#coding=utf-8
import psycopg2
import psycopg2.extras
import psycopg2.pool

class DBBase(object):
    def __init__(self, glb_conn_pool):
        if not isinstance(glb_conn_pool, psycopg2.pool.AbstractConnectionPool):
            raise 

        super().__init__()
        self.conn_pool = glb_conn_pool
        
    def _return_connection(self, conn):
        self.conn_pool.putconn(conn)

    def _get_connection(self):
        try:
            conn=self.conn_pool.getconn()
        except Exception as e:
            raise
        else:
            return conn

    def query(self, sql_tmpl, sql_params=()):        
        try:
            conn=self._get_connection()                
            cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)            
            sql = cur.mogrify(sql_tmpl, sql_params)
            cur.execute(sql)
            conn.commit()            
            if cur.rowcount > 0:
                return cur.fetchall()
            else:
                return []
        except Exception as e:
            raise 
        finally:
            self._return_connection(conn)               

    def insert(self, sql_tmpl, sql_params=()):
        try:
            conn=self._get_connection()                
            cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = cur.mogrify(sql_tmpl, sql_params)
            print("insert, sql={}".format(sql.decode('utf-8')))
            cur.execute(sql)
            conn.commit()
            insert_id=cur.fetchone()['id']
            return insert_id 
        except Exception as e:
            raise
        finally:
            self._return_connection(conn)        

    def execute(self, sql_tmpl, sql_params=()):
        try:
            conn=self._get_connection()                
            cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = cur.mogrify(sql_tmpl, sql_params)
            print("execute, sql={}".format(sql.decode('utf-8')))
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            raise 
        finally:
            self._return_connection(conn)            




