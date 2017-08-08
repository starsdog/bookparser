from lxml import etree
from lxml import html
from lxml.html.clean import Cleaner
import json
import os
import requests
import argparse
import csv
import time
class htmlParser(object):
    def __init__(self, config_file):
        self.header={"Connection":"close"}
        with open(config_file) as file:
            config = json.load(file)
            for key in config.keys():
                setattr(self, key, config[key])
    
    def parse_html(self, filename):
        print(filename)
        parser=etree.HTMLParser()
        try:
            tree=html.parse(filename)
        except:
            return False, ''

        prod_info=tree.xpath("//li//span")
        content={}
        for p in prod_info:
            #print("p={}".format(p.text))
            if p.text != None and '作者' in p.text:
                for info in p.iter('a'):
                    content['author']=info.text
            elif p.text != None and '譯者' in p.text:
                for info in p.iter('a'):
                    content['translator']=info.text
            elif p.text != None and '出版社' in p.text:
                for info in p.iter('a'):
                    content['publisher']=info.text
            elif p.text != None and '出版日期' in p.text:
                for info in p.iter('span'):
                    content['publish_date']=info.text
            elif p.text != None and 'ISBN' in p.text:
                for info in p.iter('span'):
                    content['ISBN_no']=info.text
            elif p.text != None and '類別' in p.text:
                content['genre']=[]
                for info in tree.xpath('//li//span/following-sibling::span/a'):
                    if info.attrib.get('class')=='linkStyle02': 
                        #print("info={}, {}".format(info.getparent().tag, info.text))
                        content['genre'].append(info.text)
        
        tag_info=tree.xpath("//a[@class='tag']")
        content['tag']=[]
        for p in tag_info:
            #print("p={}".format(p.text))
            content['tag'].append(p.text)

        brief_info=tree.xpath("//div[@id='prodPfDiv']")
        desc=''
        ad_word=['關鍵特色', '好評推薦', '作者簡介', '佳評如潮', '暢銷書', '本書特色']
        if len(brief_info):
            for child in brief_info[0]:
                if child.text != None and '作者簡介' in child.text:
                    break

                cleaner = Cleaner()
                cleaner.remove_tags = ['p','br','span','font','b','center']
                innertext=etree.tostring(child, encoding='unicode', method='html').replace("<div>","").replace("</div>","")
                
                cleaned=cleaner.clean_html(innertext)
                if len(cleaned):
                    cleaned=cleaned.replace("<div>","").replace("</div>","")
            
                ad_exist=False
                for word in ad_word:
                    if word in cleaned:
                        ad_exist=True

                if ad_exist==True:
                    break

                desc += cleaned  
        content['desc']=desc

        head, tail=os.path.split(filename)
        tazze_link='http://www.taaze.tw/sing.html?pid='+tail[:-5]
        content['link']={'tazze':tazze_link}
        
        #print("content={}".format(content))

        if 'ISBN_no' not in content.keys():
            content['ISBN_no']=tail[:-5]
        filename="{}.json".format(content['ISBN_no'])
        file_path=os.path.join(self.json_folder, filename)
        output=open(file_path, "w")
        output.write(json.dumps(content, ensure_ascii=False))
        output.close()
        
        return True, content['ISBN_no']

    def download_html(self, pid):
        url='http://www.taaze.tw/sing.html?pid='+str(pid)
        r=requests.get(url, headers=self.header)
        if r.status_code==200:
            filename='{}.html'.format(pid)
            output_file=os.path.join(self.html_folder, filename)
            output=open(output_file, "wb")
            output.write(r.content)
            output.close()
            return True
        else: 
            return False
    
    def download_img(self, pid, isbn):
        large=False
        small=False

        url='http://media.taaze.tw/showLargeImage.html?sc='+str(pid)
        r=requests.get(url, headers=self.header)
        if r.status_code==200:
            filename='{}.jpg'.format(isbn)
            output_file=os.path.join(self.large_media_folder, filename)
            output=open(output_file, "wb")
            output.write(r.content)
            output.close()
            large=True
            url='https://media.taaze.tw/showProdImage.html?sc='+str(pid)
            r=requests.get(url, headers=self.header)
            if r.status_code==200:
                output_file=os.path.join(self.small_media_folder, filename)
                output=open(output_file, "wb")
                output.write(r.content)
                output.close()
                small=True
            
        return large, small

if __name__=="__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t', '--task', metavar='parse', type=str, nargs=1, required=True,
        help='Specify a task to do. (parse)')
     
    args = arg_parser.parse_args()

    if args.task!=None:
        task=args.task[0]

    with open('config.json') as file:
        project_config=json.load(file)

    parser=htmlParser("config.json")
    root_dir=os.path.dirname(os.path.abspath(__file__))
    
    if task=='parse':
        status_output=open(project_config['parse_status_csv'], 'a')
        writer = csv.DictWriter(status_output, fieldnames=['pid','isbn', 'status'])
        for pid in range(project_config['start'], project_config['end']):
            ISBN='' 
            filename="{}.html".format(pid)
            filepath=os.path.join(root_dir, 'html', filename)
            if os.path.exists(filepath):
                status, ISBN=parser.parse_html(filepath)
            else:
                status=False
            writer.writerow({"pid":pid, 'isbn':ISBN, "status":status})
            time.sleep(0.1)
        status_output.close()    
    elif task=='download':    
        status_output=open(project_config['download_status_csv'], 'a')
        writer = csv.DictWriter(status_output, fieldnames=['pid','status'])
        for pid in range(project_config['start'], project_config['end']):
            status=parser.download_html(pid)
            writer.writerow({"pid":pid, "status":status})
            time.sleep(1)
        status_output.close()
    elif task=='image':
        status_output=open(project_config['image_status_csv'], 'a')
        isbn_input=open(project_config['parse_status_csv'], 'r')
        reader=csv.DictReader(isbn_input)
        writer=csv.DictWriter(status_output, fieldnames=['pid','isbn','large','small'])
        for row in reader:
            if row['status']=='True':
                large, small=status=parser.download_img(row['pid'], row['isbn'])
                writer.writerow({"pid":row['pid'], 'isbn':row['isbn'], "large":large, 'small':small})
                time.sleep(0.1)
        status_output.close()
        isbn_input.close()
    elif task=='parse_test':
        filename="{}.html".format(project_config['test_pid'])
        filepath=os.path.join(root_dir, 'html', filename)
        if os.path.exists(filepath):
            status, ISBN=parser.parse_html(filepath)
        else:
            status=False
        print("parse test={}".format(status))    
    else:
        print("no match job!")
