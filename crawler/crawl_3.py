#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, os, urllib2, urllib, cookielib, re, gzip, StringIO
from lxml import etree
from urllib import quote
from datetime import datetime, timedelta
from datetime import datetime, timedelta
import time, logging, logging.handlers
from pprint import pprint
from binascii import crc32
import traceback

from utils import *
from crawl import CrawlerComics
from db import DB

class CrawlerComics_3(CrawlerComics):
    def __init__(self, verbose = False, id_task = None, mode = "0"):
        self.verbose = verbose
        
        # 0 -> complete
        # 1 -> only updates and deletes
        self.mode_complete = mode == "0"

        self.parser = etree.HTMLParser()
        
        #config
        self.config = {}
        config_file = os.path.join(os.path.dirname(__file__), "crawler_comics_3.conf")
        execfile(config_file, self.config)
        
        #logger
        self.logger = logging.getLogger('CRAWLER')
        hdlr = logging.handlers.TimedRotatingFileHandler(os.path.join(os.path.dirname(__file__), \
          self.config['log_file'].replace(".log", "%s.log" % id_task)),"d",2)
        hdlr.suffix = "-%s" % id_task if id_task else "%Y-%m-%d-%H-%M"
        formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.INFO)
        self.logger.info("[__init__]")
        
        self.xpaths = {"name" : ['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[2]/span//text()'],
            "title" : ['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[2]/span//text()'],
            "labels_date" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[4]//text()', ".*FECHA [^:]*:([^:|]*).*"],
            "description" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[2]/div[1]//text()'],
            "extended_description" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[2]/div[1]//text()'],
            "price2" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[2]/div[6]//text()'],
            "thumbnail" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[1]/a/img/@src'],
            "image1" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[1]/a/img/@src', "(.*)_156(\..*)"],
            "extra_field_4" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[4]//text()', ".* FORMATO:(.*),.*"],
            "extra_field_5" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[4]//text()', ".* FORMATO:.*,(.*) p.*"],
            "extra_field_10_a" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[4]//text()', ".*GUI[^:]*N:([^:]*)\|\|"],
            "extra_field_10_b" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]/div[4]//text()', ".*DIBUJO:(.*)\|\|"],
            "labels_categories" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[1]/span//text()'],
            "content" :['//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div[2]//text()'],
            }
        
        
        #~ self.category_alias = {"BABEL" : "COMIC EUROPEO"
            #~ , u"BD - Autores Européos" : "COMIC EUROPEO"
            #~ , u"BD - AUTORES EUROPEOS" : "COMIC EUROPEO"
            #~ , u"Colección Trazado" : "COMIC INDEPENDIENTE"
            #~ , u"Cómics Clásicos" : "HUMOR"
            #~ , u"Cómics Españoles" : u"COMIC ESPAÑOL"
            #~ , u"Cómics Star Wars" : u"COMIC USA"
            #~ , u"Guías Ilustradas Star Wars" : u"COMIC USA"
            #~ , u"Independientes USA" : u"COMIC USA"
            #~ , u"Novelas Star Wars" : u"COMIC USA"
            #~ }
            
        self.category_alias = {}
            
        self.category_ban = {}
        
        self.db = DB(self.logger, config_file)
        self.db.init_model()
        
        if not id_task:
            self.id_task = self.db.start_new_task()
        else:
            self.id_task = int(id_task)
            
        #initialite csv
        self.filename_csv = os.path.join(os.path.dirname(__file__), "csv/%s" % self.config['csv_filename'] % self.id_task)
        
        self.print_line(self.config["csv_header"], True)
        self.cj = None
        self.cj_fool = None
        
        self.urls_seen = []
        

    
    def init_metas(self, previous_metas = False):
        self.metas = {"distributor" : self.config['distributor']
        ,"manufacturer" : self.config['distributor'], "tax_code" : "IVL", "extra_field_13": "Cambio" if previous_metas else "Novedad"}
        
    def download_url(self, url, level = False):
        
        if self.cj_fool is None:
            self.cj_fool = cookielib.CookieJar()
        
        cj = self.cj_fool
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        opener.addheaders = [('User-agent', self.config['user_agent'])]

        urllib2.install_opener(opener)
        
        url = quote(url.encode("utf-8"),":/?=")
        for b in re.findall(".*?(%[0-9a-zA-Z]{2}).*?", url):
            url = url.replace(b, "")
        
        req = urllib2.Request(url)
        
        downloaded = False
        tries = 0
        while not downloaded:
            try:
                tries += 1
                resp = opener.open(req)
                #~ resp = urllib2.urlopen(req)
                downloaded = True
            except urllib2.URLError as e:
                self.logger.info("[download_url] Error descargando %s - %s" % (url, str(e)))
                if tries == 5 and not level:
                    return self.download_url(url, True)
                if tries > 5:
                    return False
                else:
                    self.logger.info("[download_url] Reintentando ...")
                time.sleep(tries)
            
        data = resp.read()
        
        return data
    
    def download_url_login(self, url, level = False):
        
        if self.cj is None:
            self.cj = cookielib.CookieJar()
        
        cj = self.cj
        
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        opener.addheaders = [('User-agent', self.config['user_agent'])]

        urllib2.install_opener(opener)

        req = urllib2.Request(self.config['url_login'], self.config['str_post'].encode("utf-8"))
        
        downloaded = False
        tries = 0
        while not downloaded:
            try:
                tries += 1
                resp = opener.open(req)
                #~ resp = urllib2.urlopen(req)
                downloaded = True
            except urllib2.URLError as e:
                self.logger.info("[download_url] Error descargando %s - %s" % (url, str(e)))
                if tries == 5 and not level:
                    return self.download_url(url, True)
                if tries > 5:
                    raise
                else:
                    self.logger.info("[download_url] Reintentando ...")
                time.sleep(tries)
            
        foo = resp.read()
        
        
        url = quote(url.encode("utf-8"),":/&?=")
        
        req = urllib2.Request(url)
        
        downloaded = False
        tries = 0
        while not downloaded:
            try:
                tries += 1
                
                resp = opener.open(req)
                
                #~ resp = urllib2.urlopen(req)
                downloaded = True
            except urllib2.URLError as e:
                self.logger.info("[download_url_login] Error descargando %s - %s" % (url, str(e)))
                if tries > 5:
                    raise
                else:
                    self.logger.info("[download_url_login] Reintentando ...")
                time.sleep(tries)
            
        if 'content-encoding' in resp.headers and resp.headers['content-encoding'] == 'gzip':
            try:
                data = gzip.GzipFile(fileobj = StringIO.StringIO(resp.read())).read()
            except IOError:
                return None
        else:
            data = resp.read()
            
            
        return data
        
        
    def extract_product(self, url):
        """extract metadata from product page"""
        
        if url in self.urls_seen:
            return False
        
        self.logger.info("[extract_product] %s" % url)
        
        n_products = 0
        data_url = self.download_url(url)
        
        self.tree = etree.fromstring(data_url, self.parser)
        
        proximamente = "novedades.php" in url
        
        self.metas = self.db.load_data(url)
        
        previous_metas = {}
        
        if self.metas:
            
            #date in pass?
            now = datetime.now()
            date_created = time.strptime(self.metas['extra_field_1'].strip(), "%d/%m/%Y")
            d_created = datetime(date_created.tm_year, date_created.tm_mon, date_created.tm_mday)
                
                
            if d_created < now and not proximamente and "PROXIMAMENTE" in self.metas['categories']:
                #not modified but publish date exceeded
                
                #to detect change
                previous_metas['stock'] = self.metas['stock'] + "0"
            else:
                #has been seen before
                content = normalize_content("".join(self.extracts(self.xpaths['content'][0])))
                
            
                if crc32(content.strip().encode("utf-8")) == self.metas['crc_content']:
                    #no modifications
                    self.db.refresh_seen(url, self.id_task)
                    #ensure images
                    if self.config['check_images_without_changes']:
                        self.upload_images()
                    return 0
                    
                previous_metas['stock'] = self.metas['stock']
                previous_metas['price'] = self.metas['price']
        
        self.init_metas(previous_metas)
        
        for meta, _xpath in self.xpaths.items():
            
            xpath = _xpath[0]
            
            extract = "".join([e for e in self.extracts(xpath) if isinstance(e, basestring)])
            
            if not extract:
                if self.verbose:
                    print "\t", meta, _xpath
                continue
            if self.verbose:
                print meta, extract, _xpath
            try:
                if len(_xpath) > 1:
                    if meta == "image1":
                        self.metas[meta] = "".join(re.findall(_xpath[1],extract)[0])
                    else:
                        try:
                            self.metas[meta] = re.findall(_xpath[1],extract)[0]
                        except IndexError:
                            self.logger.warning("[extract_product] No se ha podido extraer %s" % meta)
                else:
                    self.metas[meta] = extract
                
                
                
            except Exception as e:
                self.logger.error("[extract_product] No se ha podido extraer %s" % meta)
                try:
                    print "Ha fallado: ", meta, extract, _xpath
                except:
                    pass
                raise e
                
            if meta in self.metas:
                self.metas[meta] = self.metas[meta].strip()
                
        test = self.metas["title"]
        
        #~ import chardet
        #~ try:
            #~ print repr(test)
            #~ _test = test.encode("latin-1").decode("GB2312").encode("utf-8")
            #~ print "---------", chardet.detect(_test)
            #~ print repr(_test)
            #~ print _test
        #~ except:
            #~ print
            #~ print
            #~ print
            #~ print
            #~ raise
            #~ 
        
        return self.process_metas(url, proximamente, previous_metas)
        
        
        
                    
    def process_metas(self, url, proximamente = False, previous_metas = None):
        """ prepare data """
        
        #id 
        self.metas['id'] = self.metas['mfgid'] = "ECC.%s" % normalize_id(self.normalize_category("".join([w[:2] for w in self.metas['title'].split()])))
        
        #categories
        self.metas['category'] = "MERCHANDISING" if "Little mates" in self.metas["labels_categories"] \
          else "ACCESORIOS" if "Ultimate guard" in self.metas["labels_categories"] else "COMICS"
          
        if self.metas['category'] == "COMICS":
            self.metas['subcategory'] = "MANGA" if "Manga" in self.metas["labels_categories"] else "COMIC USA"
        else:
            self.metas['subcategory'] = "ULTIMATE GUARD" if \
              "Ultimate guard" in self.metas["labels_categories"] else "LITTLE MATES"
 
        #category validations
        if self.metas['subcategory'] in self.category_alias:
            self.metas['subcategory'] = self.category_alias[self.metas['subcategory']]
        
        #category bans
        if self.metas['subcategory'] in self.category_ban:
            return 0
            
        #date
        
        labels_date = self.metas['labels_date'].split(" de ")
        self.metas['extra_field_1'] = "5/%s/%s" %(month2number(labels_date[0]), labels_date[1])
        
        date_created = time.strptime(self.metas['extra_field_1'].strip(), "%d/%m/%Y")
        self.metas['date_created'] = time.strftime("%m/%d/%Y", date_created)
        
        d_created = datetime(date_created.tm_year, date_created.tm_mon, date_created.tm_mday)
        
        now = datetime.now()
        
        if d_created > now:
            proximamente = True
            
        
        #categories
        title_collection = get_title_collection(unicode(self.metas['title'].encode("latin-1"), "utf-8"), self.metas['category'], self.metas['manufacturer'])
        
        manufacturer = self.metas['manufacturer'] if self.metas['manufacturer'] else "VARIOS"
        
        if not proximamente: 
        
            if "MERCHANDISING" in self.metas['category']:
                level_2 = title_collection.replace(manufacturer, "").strip()
                self.metas['categories'] = "%s@%s/%s@%s/%s/%s@%s/%s/%s/%s" % \
                  (self.metas['category'], self.metas['category'], level_2, \
                  self.metas['category'], level_2, self.metas['subcategory'], \
                  self.metas['category'], level_2, self.metas['subcategory'], manufacturer)

            else:
                #CATEGORIA_PRINCIPAL@CATEGORIA_PRINCIPAL/SUBCATEGORIA@CATEGORIA_PRINCIPAL/SUBCATEGORIA/EDITORIAL@CATEGORIA_PRINCIPAL/SUBCATEGORIA/EDITORIAL/TITULO -(menos ó sin) NUMERO COLECCION
                self.metas['categories'] = "%s@%s/%s@%s/%s/%s@%s/%s/%s/%s" % \
                  (self.metas['category'], self.metas['category'], self.metas['subcategory'], \
                  self.metas['category'], self.metas['subcategory'], manufacturer, \
                  self.metas['category'], self.metas['subcategory'], manufacturer, \
                  title_collection)
        else:
            #comming
            if "MERCHANDISING" in self.metas['category']:
                self.metas['categories'] = "PROXIMAMENTE@PROXIMAMENTE/%s@PROXIMAMENTE/%s/%s@PROXIMAMENTE/%s/%s/%s" % \
                  (self.metas['category'], self.metas['category'], self.metas['subcategory'], \
                  self.metas['category'], title_collection, self.metas['subcategory'])
            else:
                self.metas['categories'] = "PROXIMAMENTE@PROXIMAMENTE/%s@PROXIMAMENTE/%s/%s@PROXIMAMENTE/%s/%s/%s" % \
                  (self.metas['category'], self.metas['category'], self.metas['subcategory'], \
                  self.metas['category'], self.metas['subcategory'], manufacturer)
              
        try:
            self.metas['categories'] = "@".join([self.normalize_category(unicode(c.encode("latin-1"), "utf-8")) for c in self.metas['categories'].split("@")])
        except UnicodeDecodeError:
            self.metas['categories'] = "@".join([self.normalize_category(c) for c in self.metas['categories'].split("@")])
            
        
        if "PVP recomendado:" in self.metas['price2']:
            self.metas['price2'] = self.metas['price2'].replace("PVP recomendado:","")
        
        #price and cost
        if "por confirmar" in self.metas['price2'].lower():
            self.metas['price2'] = "0"
        else:
            #without euro symbol
            self.metas['price2'] = self.metas['price2'].split()[0].replace(u"\xa0\u20ac","").replace(u"\u20ac","").replace(u"\x80","")


        
        
            
        self.metas['cost'] = float(self.metas['price2'].replace(".","").replace(",",".")) * 0.7
            
        self.metas['price'] = float(self.metas['price2'].replace(".","").replace(",",".")) * 0.95
        
        
        
        #descriptions
        def smart_truncate(content, length=100, suffix=''):
            if len(content) <= length:
                return content
            else:
                return ' '.join(content[:length+1].split(' ')[0:-1]) + suffix
        
        
                    
        if 'description' in self.metas:
            if self.metas['description'].startswith("Sinopsis:"):
                self.metas['description'] = self.metas['description'][9:]
            if self.metas['extended_description'].startswith("Sinopsis:"):
                self.metas['extended_description'] = self.metas['extended_description'][9:]
                
            self.metas['description'] = smart_truncate(clean_spaces(self.metas['description']))
            self.metas['extended_description'] = clean_spaces(self.metas['extended_description'])
            
        
        #stock & instock_message
        isbn, in_novedades = self.get_external(self.metas['name'])
        
        if isbn:
            self.metas['extra_field_7'] = self.metas['extra_field_11'] = isbn
        
            
        self.metas['stock'] = 40 if (proximamente or in_novedades) else 10
        
        
        if self.metas['stock'] == 10:
            if not isbn:
                self.metas['stock'] = 0
                

        self.metas['instock_message'] = "Pre-Reserva" if self.metas['stock'] == 40 \
          else "Añadir a Lista de Espera" if self.metas['stock'] == 0 \
          else "Envío 5 a 7 Días"
          
        #all products of this categories when stock > 0 have a custom message
        categories_order = ['ACCESORIOS', 'DVD-BLU RAY', 'MERCHANDISING', 'JUEGOS']
        if self.metas['stock'] > 0 and any(cat in self.metas['category'] for cat in categories_order):
            self.metas['instock_message'] = "Disponible Bajo Pedido"
          
        for key_image, sufix in {'thumbnail':'_tb', 'image1':''}.items():
            if key_image in self.metas:
                #~ print self.metas[key_image]
                if not "http" in self.metas[key_image]:
                    self.metas[key_image] = ("http://%s/%s" % (self.config['domain'], self.metas[key_image])).replace("../","")
                    
                    
                
                filename = "%s%s.jpg" % (self.metas["id"], sufix)
                #~ if not self.download_img(self.metas[key_image], filename , thumbnail = key_image == "thumbnail" ):
                if not self.download_img(unicode(self.metas[key_image].encode("latin-1"), "utf-8"), filename , thumbnail = key_image == "thumbnail" ):
                    del self.metas[key_image]
                    continue
                
                
                finalname = "%s%s/%s/%s/%s" % (self.config['url_images'], self.metas['category'], self.metas['subcategory'], \
                  self.metas['manufacturer'], filename)
                self.metas[key_image] = self.normalize_path(finalname)


        #homespecial
        self.metas['homespecial'] = 1 if abs((now - d_created).days) <10 else 0
        
        #reward_points
        self.metas['reward_points'] = int(self.metas['price'] * 20 if d_created > now else self.metas['price'] * 10)
        
        
        manufacturers = list(set(self.metas['extra_field_10_a'].split(",") \
            if "extra_field_10_a" in self.metas else [] \
            + self.metas['extra_field_10_b'].split(",") \
            if "extra_field_10_b" in self.metas else []))
        
        self.metas['extra_field_10'] = ",".join(manufacturers)
        
        if len(self.metas['extra_field_10']) == 0:
            del self.metas['extra_field_10']
        
        #keywords & metatags
        keys_keywords = ["category", "subcategory", "manufacturer", "title", "extra_field_10", "extra_field_3"]
        
        
        self.metas['keywords'] = ", ".join(self.metas[i].strip() for i in keys_keywords if i in self.metas and len(self.metas[i])>1)

        
        def cut_last_comma(s):
            if s[-1] == ",":
                s = s[:-1]
            if len(s) > 1 and s[-2] == ", ":
                s = s[:-2]
            return s
        
        self.metas['keywords'] = cut_last_comma(self.metas['keywords'].upper())
        
        if 'extra_field_10' in self.metas:
            self.metas['extra_field_10'] = cut_last_comma(self.metas['extra_field_10'])
        
        self.metas['metatags'] = '<META NAME="KEYWORDS" CONTENT="%s">' % self.metas['keywords']
        
        if previous_metas:
            #has been seen already
            if previous_metas['stock'] == self.metas['stock'] and previous_metas['price'] == self.metas['price']:
                #has modifications but not in price or stock. Dont update.
                return 0
                
        #extra_field_11  
        if 'extra_field_11' in self.metas and self.metas['extra_field_11']:
            self.metas['extra_field_11'] = "<div>%s</div>" % self.metas['extra_field_11']
        
        self.metas['price2'] = self.metas['price2'].replace(",", ".")
        
        self.metas['content'] =  normalize_content(self.metas['content'])
        
        for meta in self.metas:
            if isinstance(self.metas[meta],float):
                self.metas[meta] = str(round(self.metas[meta],2))
            else:
                if isinstance(self.metas[meta],basestring):
                    try:
                        self.metas[meta] = unicode(self.metas[meta].encode("latin-1"), "utf-8").encode("utf-8")
                    except (UnicodeDecodeError, UnicodeEncodeError):
                        try:
                            self.metas[meta] = self.metas[meta].encode("utf-8")
                        except UnicodeDecodeError:
                            pass
                            #~ print meta
                            #~ print repr(self.metas[meta])
                            #~ raise
                        
            #~ print meta, self.metas[meta]
            
        
        
        
        
        self.db.save_data(url, self.metas, self.id_task)
        self.upload_images()
        self.urls_seen.append(url)
        
        return 1
        
    def get_external(self, name):
        
        html = self.download_url_login(self.config['url_search'] % name)
        
        tree = etree.fromstring(html, self.parser)
        
        find = etree.XPath('//*[@id="aspnetForm"]/div[2]/div/div[3]/div/div/div[3]/ul//li')
        products = find(tree)
        for product in products:
            t = clean_spaces(product.xpath(".//div/div/div[1]/span//text()")[0])
            if t == name:
                #todo: ?
                in_novedades = "novedad" in  "".join(product.xpath(".//text()"))
                
                isbn = "".join(product.xpath(".//div/div/div[4]/text()")).split()[-1]
                
                return isbn, in_novedades
                
        
        return None, None
        
        
    def extract_category(self, url, second_level = False):
        """ crawl a category page"""
        
        if url in self.urls_seen:
            return False
        
        html = self.download_url(url)
        tree = etree.fromstring(html, self.parser)
        
        f = open("a.html", "w")
        f.write(html)
        f.close()
    
        if second_level:
            find = etree.XPath('//a/@href')
        else:
            find = etree.XPath('//*[@id="aspnetForm"]/div[2]/div[3]/div[2]/div/div//a/@href')
        links = find(tree)
        self.logger.info("[extract_category] recorriendo %s" % url)
        
        self.urls_seen.append(url)
        
        
        for link in links:
            if "/comics/" in link and (not second_level or "?p=" in link):
                self.extract_category(link, True)
            if "/comic/" in link and second_level:
                self.extract_product(link)
                
        
            
    def run(self):
        """start complete crawler"""
        
        self.logger.info("[run] iniciando(Completo=%s)" % self.mode_complete)
        
        try:
            self.db.init_task(self.id_task)
            
            
            html = self.download_url(self.config['discover_url'])
            tree = etree.fromstring(html, self.parser)
        
            find = etree.XPath('//a/@href')
            links = find(tree)
            for link in links:
                if "/comics/" in link :
                    self.logger.info("[run] recorriendo %s" % link)
                    self.extract_category(link)
                    
            self.generate_csv()
            
            self.db.finish_task(self.id_task)
        except Exception as e:
            self.db.finish_task(self.id_task, True)
            
            exc_type, exc_obj, exc_tb = sys.exc_info()

            self.logger.error("%s\n %d: %s" %(traceback.format_exc(), exc_tb.tb_lineno, str(e)))
            raise
        

        
if __name__ == '__main__':
    
    
    if len(sys.argv) == 1:
        crawl = CrawlerComics_3()
        crawl.run()
    else:
        if "http" in sys.argv[1]:
            for url in sys.argv[1:]:
                crawl = CrawlerComics_3()
                crawl.extract_product(url)
                crawl.generate_csv()
            
                crawl.db.finish_task(crawl.id_task)
        else:
            crawl = CrawlerComics_3(id_task = sys.argv[1], mode = sys.argv[2])
            crawl.run()
