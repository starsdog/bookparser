#coding=utf-8
import psycopg2
from lib.DBBase import DBBase
import traceback
import re

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
            sub_isbn_no=ISBN[6:]
            is_digit=sub_isbn_no.isdigit()
            if is_digit:
                table_no=int(int(sub_isbn_no)/self.table_size)
            else:
                sub_isbn_no = re.sub('[a-zA-Z]', '', sub_isbn_no)
                table_no=int(int(sub_isbn_no)/self.table_size)
            table_name='book_{}'.format(table_no)
            print("generate table case 1={}".format(table_name))
        elif '978' not in ISBN and len(ISBN)==10 :
            sub_isbn_no=ISBN[3:]
            is_digit=sub_isbn_no.isdigit()
            if is_digit:
                table_no=int(int(sub_isbn_no)/self.table_size)
            else:
                sub_isbn_no = re.sub('[a-zA-Z]', '', sub_isbn_no)
                table_no=int(int(sub_isbn_no)/self.table_size)
            table_name='book_{}'.format(table_no)
            print("generate_table case 2={}".format(table_name))
        else:
            print("generate_table case 3")
            table_no='other'
            table_name='book_other'

        return table_name, table_no

    def _create_book_table(self, table_name):
        try:
            create_sql="""
                CREATE TABLE {table}
                (
                  id integer NOT NULL DEFAULT nextval('book_id_seq'::regclass),
                  ISBN_no text NOT NULL,
                  title text NOT NULL,
                  author text,
                  translator text,
                  author_id integer,
                  publish_date date,
                  description text,
                  tag text[],
                  genre text[],
                  link text[],
                  publisher text,
                  CONSTRAINT {table}_pkey PRIMARY KEY (ISBN_no)
                )
                """.format(table=table_name)
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
            if '讀冊生活,TAAZE,買書,二手書,電子書,電子雜誌,簡體書' in book_attr['title']:
                return 

            author_id=-1
            if 'author' in book_attr.keys():
                author_id, book_list=self.get_author_or_insert(book_attr['author'])
            print("add_book, author id={}".format(author_id))
            book_table, table_no=self._generate_table(book_attr['ISBN_no'])            

            field_list=list()
            param_list=list()
            for key in book_attr:
                if type(book_attr[key])==list and len(book_attr[key])==0:
                    continue

                field_list.append(key)
                param_list.append('%('+key+')s')


            if author_id!=-1 and 'author_id' not in book_attr:
                field_list.append('author_id')
                param_list.append('%(author_id)s')
                book_attr['author_id']=author_id   
                #print(field_list)
                
            insert_sql="insert into {table} (".format(table=book_table)+",".join(field_list)+") VALUES ("+ ",".join(param_list)+")"
            insert_sql += ' RETURNING id'
            print("add_book sql={}, book_attr={}".format(insert_sql, book_attr))
            book_id=self.insert(insert_sql, book_attr)        
            print("add book success id={}".format(book_id))
            book_unique_id=str(table_no)+'_'+str(book_id)
            if author_id!=-1:
                book_list.append(book_unique_id)
                book_unique_list=list(set(book_list))
                self.updatebook_in_author(author_id, book_unique_list)
        except psycopg2.ProgrammingError as e:
            print(e.pgcode)
            if e.pgcode == '42P01': #table doesn't exsit
                print('create book table')
                self._create_book_table(book_table)
                self.add_book(book_attr) 
            else: 
                print(traceback.format_exc())   
                raise
        except psycopg2.IntegrityError as e:
            if e.pgcode == '23505': # duplicate key
                print('duplicate key')
                book_info=self.query_book_with_table('ISBN_no', book_attr['ISBN_no'], book_table)
                if book_info!=None:
                    book_unique_id=str(table_no)+'_'+str(book_info['id'])
                    if author_id!=-1:
                        book_list.append(book_unique_id)
                        book_unique_list=list(set(book_list))
                        self.updatebook_in_author(author_id, list(book_unique_list))
            else:
                print(traceback.format_exc())
                raise
        except psycopg2.DataError as e:
            print('wrong publish date {}'.format(e.pgcode))
            del book_attr['publish_date']
            self.add_book(book_attr) 
        except Exception as e:
            print(traceback.format_exc())
            raise    

    def query_book_by_author(self, author_name):
        author_info_list=self.get_similar_author(author_name)
        author_book_info_list=[]
        for author_info in author_info_list:
            book_list=author_info['book_list']
            for book in book_list:
                info=book.split('_')
                if len(info)==2:
                    book_table='book_'+info[0]
                    book_id=int(info[1])   
                    book_info=self.query_book_with_table('id', book_id, book_table)
                    if book_info!=None:
                        author_book_info_list.append(book_info)
        return author_book_info_list            

    def query_book_with_ISBN(self, ISBN_no):
        book_table, table_no=self._generate_table(ISBN_no)
        book_info=self.query_book_with_table('ISBN_no', ISBN_no, book_table)
        return book_info  

    def query_book_with_table(self, attr, value, book_table):
        try:
            query_sql="select * from {} where {}=%(value)s".format(book_table, attr)
            result=self.query(query_sql, {"value":value})
            if len(result):
                return result[0]
            else:
                return None
        except Exception as e:
            print(traceback.format_exc())
            raise    
                
    def get_similar_author(self, author_name):
        try:
            query_sql="select * from author where name like %(name)s"
            result=self.query(query_sql, {"name":author_name})
            if len(result):
                return result
            else:
                return None
        except Exception as e:
            print(traceback.format_exc())
            raise            

    def get_author_or_insert(self, author_name):
        '''
        1. query author_name
        2. if not exist, insert author and get new id
        3. if exist, net author id
        4. return author id
        '''
        try:
            book_list=[]
            query_sql="select * from author where name=%(name)s"
            result=self.query(query_sql, {"name":author_name})
            if len(result):
                author_id=result[0]['id']
                book_list=list(set(book_list+result[0]['book_list']))               
            else:
                insert_sql="insert into author (name, book_list) VALUES (%(name)s, %(book)s) RETURNING id"
                insert_id=self.insert(insert_sql, {"name":author_name, "book":[]})
                author_id=insert_id
            return author_id, book_list        
        except Exception as e:
            print(traceback.format_exc())
            raise    

    def updatebook_in_author(self, author_id, book_list):
        try:
            update_sql='update author set book_list=%(book)s where id=%(author_id)s'
            self.execute(update_sql, {"book":book_list, "author_id":author_id})
        except Exception as e:
            print(traceback.format_exc())
            raise        

    '''
    def _clean_author(self, name):
        name=name.replace('/著','').replace('/編著','').replace('/編', '').replace('/撰文','').replace('/總編輯','').replace('/繪','')
        name=name.replace('/譯','').replace('/小說改編','').replace('/原著劇本','').replace('/資料提供','').replace('/企劃主編','')
        name=name.replace('/改編','').replace('/原著','').replace('/口述','').replace('/作','').replace('/繪，文','').replace(' ','')
        name_list=[]
        if '，' in name:
            name_list=name.split('，')
        elif '、' in name:
            name_list=name.split('、')
        else:
            name_list.append(name)
        return name_list

    
    def clean_author_DB(self):
        try:
            query_sql="select * from author"
            all_result=self.query(query_sql)
            for item in all_result:
                org_name=item['name']
                author_id=item['id']
                if '/' in org_name or '，' in org_name or '、' in org_name:
                    org_book_list=item['book_list']
                    name_list=self._clean_author(org_name)
                    for name in name_list:
                        query_sql="select * from author where name=%(name)s"
                        result=self.query(query_sql, {"name":name})
                        if len(result):
                            exist_author_id=result[0]['id']
                            exist_book_list=list(set(org_book_list+result[0]['book_list']))
                            self.updatebook_in_author(exist_author_id, exist_book_list)
                        else:
                            insert_sql="insert into author (name, book_list) VALUES (%(name)s, %(book)s) RETURNING id"
                            insert_id=self.insert(insert_sql, {"name":name, "book":org_book_list})  

                    delete_sql="delete from author where id=%(author_id)s"
                    self.execute(delete_sql, {"author_id":author_id})            
        except Exception as e:
            print(traceback.format_exc())
            raise        
        '''
