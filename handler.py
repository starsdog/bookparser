import argparse
import os
import glob
import json
import psycopg2
import psycopg2.pool
import traceback
from lib.bookManager import bookManager

class bookHandler(object):
    def __init__(self, config_file):                
        with open(config_file) as file:
            config = json.load(file)
            for key in config.keys():
                setattr(self, key, config[key])

        db_pool = psycopg2.pool.ThreadedConnectionPool(self.db_pool['min_conn'],
                                                       self.db_pool['max_conn'],
                                                       **self.db_conf)

        self.dbManager=bookManager(db_pool)        

    def insert_by_folder(self):
        for file_path in glob.glob(os.path.join(self.json_folder,'*')):
            try:
                file_handler=open(file_path)
                content=json.load(file_handler)
                self.dbManager.add_book(content)
            except Exception as e:
                print(traceback.format_exc())
                raise

    def insert_by_one(self, file_path):                
        try:
            file_handler=open(file_path)
            content=json.load(file_handler)
            self.dbManager.add_book(content)
        except Exception as e:
            print(traceback.format_exc())
            raise

if __name__=="__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t', '--task', metavar='parse', type=str, nargs=1, required=True,
        help='Specify a task to do. (parse)')
     
    args = arg_parser.parse_args()

    if args.task!=None:
        task=args.task[0]

    with open('config.json') as file:
        project_config=json.load(file)

    handler=bookHandler('config.json')
    root_dir=os.path.dirname(os.path.abspath(__file__))
    if task=='test_handle':    
        filename="{}.json".format(project_config['test_isbn'])
        foldername="{}_{}_json".format(project_config['start'], project_config['end'])
        filepath=os.path.join(root_dir, 'json', foldername, filename)
        handler.insert_by_one(filepath)
    else:
        print("no match job")