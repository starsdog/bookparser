#coding=utf-8
from lxml import etree
from lxml import html
from lxml.html.clean import Cleaner
import json
import os
import requests
import argparse
import csv
import time
import traceback

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

        content={}
        title=tree.xpath("//meta[@name='keywords']/@content")[0].replace('- TAAZE 讀冊生活','')
        content['title']=title

        prod_info=tree.xpath("//li//span")
        for p in prod_info:
            #print("p={}".format(p.text))
            if p.text != None and '作者' in p.text:
                for info in p.iter('a'):
                    author=info.text.replace('/著','').replace('/編著','').replace('/編', '').replace('/撰文','').replace('/總編輯','').replace('/繪','')
                    author=author.replace('/譯','').replace('/小說改編','').replace('/原著劇本','').replace('/資料提供','').replace('/企劃主編','')
                    author=author.replace('/改編','').replace('/原著','').replace('/口述','').replace('/作','').replace('/繪，文','').replace(' ','')
                    content['author']=author
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
                cleaner.remove_tags = ['p','br','span','font','b','center', 'u', 'strong']
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
        content['description']=desc

        head, tail=os.path.split(filename)
        tazze_link='http://www.taaze.tw/sing.html?pid='+tail[:-5]
        content['link']=[tazze_link]
        
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
            url='http://media.taaze.tw/showProdImage.html?sc='+str(pid)
            r=requests.get(url, headers=self.header)
            if r.status_code==200:
                output_file=os.path.join(self.small_media_folder, filename)
                output=open(output_file, "wb")
                output.write(r.content)
                output.close()
                small=True
            
        return large, small

    def download_books_index(self, target, page_number):
        for page in range(1, page_number+1):
            url='http://www.books.com.tw/web/sys_bbotm/books/'+str(target)+'?o=1&v=1&page='+str(page)
            r=requests.get(url, headers=self.header)
            if r.status_code==200:
                filename='{}_{}.html'.format(target, page)
                output_file=os.path.join(self.books_index_folder, filename)
                output=open(output_file, "wb")
                output.write(r.content)
                output.close()

    def download_books_html(self):
        for dirPath, dirNames, fileNames in os.walk(self.books_link_folder):        
            for f in fileNames:
                if '.json' in f:
                    filename="{}".format(os.path.join(dirPath, f))
                    file_handler=open(filename)
                    download_link=json.load(file_handler)
                    for link in download_link:
                        basename_index=link.rfind('/')
                        #loc_index=link.find('loc=')
                        link_filename=link[basename_index+1:]
                        filename='{}.html'.format(link_filename)
                        output_file=os.path.join(self.books_html_folder, filename)
                        if os.path.exists(output_file):
                            continue
    
                        r=requests.get(link, headers=self.header)
                        if r.status_code==200:
                            output=open(output_file, "wb")
                            output.write(r.content)
                            output.close()
                        time.sleep(0.5)  
                        
    def parse_books_index(self, filename):
        parser=etree.HTMLParser()
        try:
            tree=html.parse(filename)
        except:
            return False

        download_link=[]    
        item_list=tree.xpath("//div[@class='item']")
        for item in item_list:
            item_info=item.iter('a')
            for info in item_info:
                #print(info.attrib.get('href'))
                download_link.append(info.attrib.get('href'))
                break

        filename=filename[:-5]+'.json'
        filename=os.path.basename(filename)
        output_file=os.path.join(self.books_link_folder, filename)
        output=open(output_file, "w")
        output.write(json.dumps(download_link))
        output.close()       
        return True

    def parse_books_html(self, filename):
        parser=etree.HTMLParser()
        try:
            tree=html.parse(filename)
        except:
            return False, '', False

        try:
            content={}
            title=tree.xpath("//title")[0]
            if title==None:
                return False, '', False

            content['title']=title.text.replace('博客來-', '')

            property_info=tree.xpath("//meta[@name='description']")[0].attrib.get('content')
            property_list=property_info.split("，")
            for item in property_list:
                if 'ISBN' in item:
                    content['ISBN_no']=item[5:]
                elif '出版社' in item:
                    content['publisher']=item[4:]
                elif '作者' in item:
                    content['author']=item[3:]
                elif '譯者' in item:
                    content['translator']=item[3:]
                elif '出版日期' in item:
                    content['publish_date']=item[5:].replace('/','-')
            
            genre_info=tree.xpath("//div[@class='mod_b type02_m058 clearfix']//ul[@class='sort']")
            for p in genre_info:
                content['genre']=[]
                for item in p.iter('a'):
                    content['genre'].append(item.text)

            brief_info=tree.xpath("//div[@itemprop='description']")
            desc=''
            ad_word=['關鍵特色', '好評推薦', '作者簡介', '佳評如潮', '暢銷書', '本書特色']
            if len(brief_info):
                for child in brief_info[0]:
                    if child.text != None and '作者簡介' in child.text:
                        break
                    cleaner = Cleaner()
                    cleaner.remove_tags = ['p','br','span','font','b','center','u','strong']
                    innertext=etree.tostring(child, encoding='unicode', method='html').replace("<div>","").replace("</div>","").replace("\u3000",'').replace('\n','').replace('\r', '')
                    
                    cleaned=cleaner.clean_html(innertext)
                    if len(cleaned):
                        cleaned=cleaned.replace("<div>","").replace("</div>","")
                
                    desc += cleaned  
            content['description']=desc

            head, tail=os.path.split(filename)
            loc_idx=tail.find('loc=')
            pid=tail[:loc_idx-1]        
            content['link']=['http://www.books.com.tw/products/'+tail[:-5]]
            if 'ISBN_no' not in content.keys():
                content['ISBN_no']=pid
            #download image
            img_link=tree.xpath("//meta[@property='og:image']")[0].attrib.get('content')
            r=requests.get(img_link, headers=self.header)
            image_status=False
            if r.status_code==200:
                filename='{}.jpg'.format(content['ISBN_no'])
                output_file=os.path.join(self.books_img_folder, filename)
                if not os.path.exists(output_file):
                    output=open(output_file, "wb")
                    output.write(r.content)
                    output.close()  
                image_status=True

            filename="{}.json".format(content['ISBN_no'])
            file_path=os.path.join(self.books_json_folder, filename)
            output=open(file_path, "w")
            output.write(json.dumps(content, ensure_ascii=False))
            output.close()
          
            return True, content['ISBN_no'], image_status
        except Exception as e:
            print(filename)
            print(traceback.format_exc())
            return False, '', False  
        
    def parse_books_index_folder(self):
        for dirPath, dirNames, fileNames in os.walk(self.books_index_folder):        
            for f in fileNames:
                if '.html' in f:
                    filename="{}".format(os.path.join(dirPath, f))
                    status=self.parse_books_index(filename)

    def parse_books_html_folder(self): 
        status_output=open(project_config['books_parse_status_csv'], 'a')
        writer = csv.DictWriter(status_output, fieldnames=['filename', 'isbn', 'status', 'image'])
        for dirPath, dirNames, fileNames in os.walk(self.books_html_folder):        
            for f in fileNames:
                if '.html' in f:
                    filename="{}".format(os.path.join(dirPath, f))   
                    parse_status, isbn, image_status=self.parse_books_html(filename)  
                    writer.writerow({'filename':f, 'isbn':isbn, "status":parse_status, "image":image_status})       
        status_output.close()

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
            filepath=os.path.join(project_config['html_folder'], filename)
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
            time.sleep(1)
        status_output.close()
    elif task=='image':
        status_output=open(project_config['image_status_csv'], 'a')
        isbn_input=open(project_config['parse_status_csv'], 'r')
        reader=csv.DictReader(isbn_input)
        writer=csv.DictWriter(status_output, fieldnames=['pid','isbn','large','small'])
        for row in reader:
            pid=int(row['pid'])
            if row['status']=='True' and pid in range(project_config['start'], project_config['end']):
                large, small=status=parser.download_img(row['pid'], row['isbn'])
                writer.writerow({"pid":row['pid'], 'isbn':row['isbn'], "large":large, 'small':small})
                time.sleep(0.1)
        status_output.close()
        isbn_input.close()
    elif task=='parse_test':
        filename="{}.html".format(project_config['test_pid'])
        filepath=os.path.join(project_config['html_folder'], filename)
        if os.path.exists(filepath):
            status, ISBN=parser.parse_html(filepath)
        else:
            status=False
        print("parse test={}".format(status))    
    elif task=='download_books_index':
        books_target_list=project_config['books_folder_list']
        for target in books_target_list:
            parser.download_books_index(target, books_target_list[target])
    elif task=='parse_books_index_folder':
        result=parser.parse_books_index_folder()
        print(result)        
    elif task=='download_books_html':
        parser.download_books_html()
    elif task=='parse_books_html':
        filename='/Users/ling/Documents/books/html/0010358341?loc=P_003_095.html'
        parser.parse_books_html(filename)    
    elif task=="parse_books_html_folder":
        parser.parse_books_html_folder()
    else:
        print("no match job!")
