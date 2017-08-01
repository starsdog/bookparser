from lxml import etree
from bs4 import BeautifulSoup
import json
import os

class htmlParser(object):
    def __init__(self, config_file):
        with open(config_file) as file:
            config = json.load(file)
            for key in config.keys():
                setattr(self, key, config[key])
    
    def parse_html(self, filename):
        parser=etree.HTMLParser()
        tree=etree.parse(filename, parser=parser)
        prod_info=tree.xpath("//li//span")
        for p in prod_info:
            if p.text!= None and '作者' in p.text:
                print("hello {}".format(p.text))
                #print("{}".format(p))
                sub_info=p.xpath("//span")
                for sub in sub_info:
                    print("sub={}".format(sub.text))
        
if __name__=="__main__":
    parser=htmlParser("config.json")
    parser.parse_html("11100821124.html")
