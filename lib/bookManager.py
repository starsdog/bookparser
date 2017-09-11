#coding=utf-8
import psycopg2
from lib.DBBase import DBBase
import traceback

class bookManager(DBBase):
    def __init__(self, glb_conn_pool, log_name=None):
        super().__init__(glb_conn_pool)        
        self.table_size=100000

    def _generate_table(self, ISBN):
        '''
        978986 978957 are fixed, 
        per table has 100K data
        ex: ISBN=9789860523904
        substring=0523904/100000=5
        book is in book_05                
        '''        
        if '978' in ISBN and len(ISBN)==13:            
            sub_isbn_no=int(ISBN[6:])
            table_no=int(sub_isbn_no/self.table_size)
            table_name='book_{}'.format(table_no)
            print("generate table case 1={}".format(table_name))
        elif '978' not in ISBN and len(ISBN)==10 :
            sub_isbn_no=int(ISBN[3:])
            table_no=int(sub_isbn_no/self.table_size)
            table_name='book_{}'.format(table_no)
            print("generate_table case 2={}".format(table_name))
        else:
            print("generate_table case 3")
            table_name='book_other'

        return table_name    

    def _create_book_table(self, table_name):
        try:
            create_sql="""
                CREATE TABLE {table}
                (
                  id integer NOT NULL DEFAULT nextval('book_id_seq'::regclass),
                  ISBN_no character varying(20) NOT NULL,
                  name character varying(50) NOT NULL,
                  author character varying(20) NOT NULL,
                  author_id integer,
                  publish_date date NOT NULL,
                  description character varying(2000) NOT NULL,
                  tag character(1)[],
                  genre character(1)[],
                  publisher character varying(50) NOT NULL,
                  CONSTRAINT book_pkey PRIMARY KEY (ISBN_no)
                )
                """.format(table=table_name)
            print("create table={}".format(create_sql))
            self.execute(create_sql)    
        except Exception as e:
            raise   

    def add_book(self, book_attr):
        '''
        1. query author_id. if no, insert author
        2. according ISBN, generate table_name. if table not exist, create table
        3. insert book. get book_id
        4. update book_id to author table
        '''
        try:
            author_id=self.check_author(book_attr['author'])
            print("add_book, author id={}".format(author_id))
            book_table=self._generate_table(book_attr['ISBN_no'])            

            field_list=list()
            param_list=list()
            for key in book_attr:
                if 'link' in key:                
                    continue

                field_list.append(key)
                param_list.append('%('+key+')s')

            del book_attr['link']

            insert_sql="insert into {table} (".format(table=book_table)+",".join(field_list)+") VALUES ("+ ",".join(param_list)+")"
            print("add_book sql={}, book_attr={}".format(insert_sql, book_attr))
            self.insert(insert_sql, book_attr)        
        except psycopg2.ProgrammingError as e:
            if e.pgcode == '42P01': #table doesn't exsit
                self._create_book_table(book_table)
                self.insert(insert_sql, book_attr) 
            else:    
                raise
        except Exception as e:
            raise    


    def check_author(self, author_name):
        '''
        1. query author_name
        2. if not exist, insert author and get new id
        3. if exist, net author id
        4. return author id
        '''
        try:
            query_sql="select * from author where name=%(name)s"
            result=self.query(query_sql, {"name":author_name})
            if len(result):                
                return result[0]['id']
            else:
                insert_sql="insert into author (name) VALUES (%(name)s) RETURNING id"
                insert_id=self.insert(insert_sql, {"name":author_name})
                return insert_id
        except Exception as e:
            print(traceback.format_exc())
            raise    

