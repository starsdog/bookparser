from lxml import etree
from lxml import html
from lxml.html.clean import Cleaner
import json
import os
import requests
import argparse
import csv

class htmlParser(object):
    def __init__(self, config_file):
        with open(config_file) as file:
            config = json.load(file)
            for key in config.keys():
                setattr(self, key, config[key])
    
    def parse_html(self, filename):
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
                        print("info={}, {}".format(info.getparent().tag, info.text))
                        content['genre'].append(info.text)
        
        tag_info=tree.xpath("//a[@class='tag']")
        content['tag']=[]
        for p in tag_info:
            print("p={}".format(p.text))
            content['tag'].append(p.text)

        brief_info=tree.xpath("//div[@id='prodPfDiv']")
        desc=''
        ad_word=['關鍵特色', '好評推薦', '作者簡介', '佳評如潮', '暢銷書']
        for child in brief_info[0]:
            if child.text != None and '作者簡介' in child.text:
                break

            cleaner = Cleaner()
            cleaner.remove_tags = ['p','br','span','font','b','center']
            innertext=etree.tostring(child, encoding='unicode', method='html').replace("<div>","").replace("</div>","")

            cleaned=cleaner.clean_html(innertext).replace("<div>","").replace("</div>","")
            
            ad_exist=False
            for word in ad_word:
                if word in cleaned:
                    ad_exist=True

            if ad_exist==True:
                break

            desc += cleaned  
        content['desc']=desc

        head, tail=os.path.split(filename)
        tazze_link='https://www.taaze.tw/sing.html?pid='+tail[:-5]
        content['link']={'tazze':tazze_link}
        
        print("content={}".format(content))

        filename="{}.json".format(content['ISBN_no'])
        file_path=os.path.join(self.json_folder, filename)
        output=open(file_path, "w")
        output.write(json.dumps(content, ensure_ascii=False))
        output.close()
        
        return True, content['ISBN_no']

    def download_html(self, pid):
        url='https://www.taaze.tw/sing.html?pid='+str(pid)
        header={"Connection":"close"}
        r=requests.get(url, headers=header)
        if r.status_code==200:
            filename='{}.html'.format(pid)
            output_file=os.path.join(self.html_folder, filename)
            output=open(output_file, "wb")
            output.write(r.content)
            output.close()
            return True
        else: 
            return False

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
        status_output.close()    
    elif task=='download':    
        status_output=open(project_config['download_status_csv'], 'a')
        writer = csv.DictWriter(status_output, fieldnames=['pid','status'])
        for pid in range(project_config['start'], project_config['end']):
            status=parser.download_html(pid)
            writer.writerow({"pid":pid, "status":status})
        status_output.close()
    else:
        print("no match job!")
