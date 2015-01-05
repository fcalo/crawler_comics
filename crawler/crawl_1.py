#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, os, urllib2, urllib, cookielib, re, gzip, StringIO
from lxml import etree
from urllib import quote
from datetime import datetime, timedelta
import time, logging, logging.handlers
from pprint import pprint
from binascii import crc32
import traceback

from utils import *
from crawl import CrawlerComics
from db import DB

class CrawlerComics_1(CrawlerComics):
    def __init__(self, verbose = False, id_task = None, mode = "0"):
        self.verbose = verbose
        
        # 0 -> complete
        # 1 -> only updates and deletes
        self.mode_complete = mode == "0"
        

        self.parser = etree.HTMLParser()
        
        #config
        self.config = {}
        config_file = os.path.join(os.path.dirname(__file__), "crawler_comics_1.conf")
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
        
        
        #key: [xpath, "label(s)"]
        self.xpaths = {"name":['//*[@id="capsaParams"]/h1//text()'],
            "title":['//*[@id="capsaParams"]/h1//text()'],
            "subcategory":['//*[@id="cami"]/ul/li[3]/a//text()'],
            "extra_field_10" : ['//*[@id="capsaParams"]/p[1]//text()', 'Autores'],
            "extra_field_4" : ['//*[@id="dreta_FICHA"]/div/p[1]/b//text()'],
            "extra_field_6" : ['//*[@id="dreta_FICHA"]/div/p[2]/b//text()'],
            "extra_field_5" : ['//*[@id="dreta_FICHA"]/div/p[3]/b[1]//text()'],
            "extra_field_7" : ['//*[@id="dreta_FICHA"]/div/p[$1]/b//text()', u'ISBN'],
            "extra_field_11" : ['//*[@id="dreta_FICHA"]/div/p[4]/b//text()'],
            "description" : ['//*[@id="esquerra_FICHA"]/table/tr/td[2]/div/div[2]/div/p//node()'],
            "extended_description" : ['//*[@id="esquerra_FICHA"]/table/tr/td[2]/div/div[2]/div/p//text()'],
            "extra_field_3" : ['//*[@id="capsaParams"]/p[3]/a//text()', 'Serie'],
            "price2" : ['//*[@id="dreta_FICHA"]/div/p[$1]/b//text()', u'PVP'],
            "content" : ['//div[@id="all"]/div[2]//text()']
            }
        self.category_alias = {"EUROPEO" : "COMIC EUROPEO", "USA" : "COMIC USA",
        "COMIC AMERCIANO": "COMIC USA"}
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


    def download_url_login(self, url):
        
        
        cj = cookielib.CookieJar()
            
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        opener.addheaders = [('User-agent', self.config['user_agent']), 
        ('HTTP_ACCEPT', 'text/html,application/xhtml+xml,application/xml; q=0.9,*/*; q=0.8'),
        ('Content-Type', 'application/x-www-form-urlencoded'),
        ('Host', 'www.zonalibros.com'),
        ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
        ('Accept-Language', 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3'),
        ('referer', 'http://www.zonalibros.com/login.aspx'),
        ('Connection', 'keep-alive'),
        ('Cache-Control', 'max-age=0'),
        ('Accept-Encoding', 'gzip,deflate')]

        urllib2.install_opener(opener)
        
        url = quote(url.encode("utf-8"),":/?&=")
        for b in re.findall(".*?(%[0-9a-zA-Z]{2}).*?", url):
            url = url.replace(b, "")
        
        _url = self.config['url_login']
        _url = quote(_url.encode("utf-8"),":/&?")
        
        #~ req = urllib2.Request(_url, urllib.urlencode(post).replace("None", ""))
        req = urllib2.Request(_url, self.config['str_post'])
        
        ck = {}
        downloaded = False
        tries = 0
        while not downloaded:
            try:
                tries += 1
                resp = opener.open(req)
                #~ resp = urllib2.urlopen(req)
                
                ck = {c.name : c.value for c in cj}
                
                downloaded = 'ZONALIBROS.ASPXAUTH' in ck
                if not downloaded:
                    self.logger.warning("Respuesta correcta pero sin cookie de autorización")
                    if tries > 5:
                        raise Exception("No login")
            except urllib2.URLError as e:
                self.logger.warning("[download_url_login] Error descargando %s - %s" % (url, str(e)))
                if tries > 5:
                    raise
                else:
                    self.logger.warning("[download_url_login] Reintentando ...")
                time.sleep(tries)
            
        #~ if 'content-encoding' in resp.headers and resp.headers['content-encoding'] == 'gzip':
            #~ try:
                #~ foo = gzip.GzipFile(fileobj = StringIO.StringIO(resp.read())).read()
            #~ except IOError:
                #~ return None
        #~ else:
            #~ foo = resp.read()
        
        
        
        
        
        #~ ck['ZONALIBROS.ASPXAUTH'] = "01015218467FD833D108FEFFD772926D28CA2B0110450044004900300035003000340030007C00320039007C0035003000340030000743006C00690065006E0074006500012F00FF"
        #final url
        
        opener.addheaders = [('User-agent', self.config['user_agent']), 
        ('HTTP_ACCEPT', 'text/html,application/xhtml+xml,application/xml; q=0.9,*/*; q=0.8'),
        ('Content-Type', 'application/x-www-form-urlencoded'),
        ('Host', 'www.zonalibros.com'),
        ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
        ('Accept-Language', 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3'),
        ('referer', 'http://www.zonalibros.com/Clientes/Inicio.aspx'),
        ('Connection', 'keep-alive'),
        ('Cookie', 'ASP.NET_SessionId=%s; User=; CambioDist=N; ZONALIBROS.ASPXAUTH=%s; __utma=177133998.490927137.1399005954.1399005954.1399005954.1; __utmb=177133998.1.10.1399005954; __utmc=177133998; __utmz=177133998.1399005954.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)' % (ck['ASP.NET_SessionId'], ck['ZONALIBROS.ASPXAUTH'])),
        ('Cache-Control', 'max-age=0'),
        ('Accept-Encoding', 'gzip,deflate')]
        
        #~ url = "http://www.zonalibros.com/Clientes/FichaArticulo.aspx?libro=108.629861"
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
        
        
    def download_url(self, url, level = False):
        
        
        
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        opener.addheaders = [('User-agent', self.config['user_agent'])]

        urllib2.install_opener(opener)
        
        url = quote(url.encode("utf-8"),":/?")
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
    
    def init_metas(self, previous_metas = False):
        self.metas = {"distributor" : self.config['distributor'], "category": "COMICS",
        "manufacturer" : self.config['manufacturer'], "tax_code" : "IVL", "extra_field_13": 0 if previous_metas else 2}
        
    def get_external(self, extra_field_7):
        #~ f = open("a.html", "w")
        #~ f.write(self.download_url_login(self.config['url_external'] % extra_field_7))
        #~ f.close()
        
        tree = etree.fromstring(self.download_url_login(self.config['url_external'] % extra_field_7.replace("-", "")), self.parser)
        find = etree.XPath('//*[@id="ctl00_MainContent_txtCI"]//text()')
        try:
            ref = find(tree)[0]
            find = etree.XPath('//*[@id="ctl00_MainContent_txtPublicacion"]//text()')
            date = find(tree)[0]
            find = etree.XPath('//*[@id="ctl00_MainContent_lblDisponible"]//text()')
            stock_label = find(tree)[0]
            stock = 0 if "agotado" in stock_label.lower() else 10
            return ref, ref, date, stock
        except IndexError:
            return None, None, None, None
        #ctl00_MainContent_txtCI
        
    def extract_product(self, url, proximamente = False):
        """extract metadata from product page"""
        
        self.logger.info("[extract_product] %s" % url)
        
        data_url = self.download_url(url)
        
        if not data_url:
            self.logger.warning("No se ha podido extraer %s" % url)
            return True
        
        self.tree = etree.fromstring(data_url, self.parser)
        
        self.metas = self.db.load_data(url)
        
        now = datetime.now()
        
        previous_metas = {}
        
        if self.metas:
            date_created = time.strptime(self.metas['extra_field_1'].strip(), "%d/%m/%Y")
        
            d_created = datetime(date_created.tm_year, date_created.tm_mon, date_created.tm_mday)
            
            if now > d_created and "PROXIMAMENTE" in self.metas['categories']:
                #not modified but publish date exceeded
                
                #to detect change
                previous_metas['stock'] = self.metas['stock'] + "0"
            else:
            
                #has been seen before
                content = "".join(self.extracts(self.xpaths['content'][0]))
                if crc32(content.strip().encode("utf-8")) == self.metas['crc_content']:
                    #no modifications
                    self.db.refresh_seen(url, self.id_task)
                    #ensure images
                    if self.config['check_images_without_changes']:
                        self.upload_images()
                    return True
                    
                previous_metas['stock'] = self.metas['stock']
                previous_metas['price'] = self.metas['price']
        
        self.init_metas(previous_metas)
        
        
        extracted = False
        for meta, _xpath in self.xpaths.items():
            with_label = len(_xpath) > 1
            #~ print meta, _xpath, with_label
            
            #search in 8 posible positions if has label
            for i in xrange(0, 1 if not with_label else 9):
                xpath = _xpath[0].replace("$1",str(i+1))
                
                #~ if with_label:
                    #~ print "\t", xpath
                
                extract = self.extract(xpath) if meta != "content" and \
                  not "description" in meta else "".join([e for e in self.extracts(xpath) if isinstance(e, basestring)])
                  
                if with_label:
                    extract_parent = self.extract(xpath.replace("/b/","/"))
                    
                  
                #~ print "\t", "\t", extract
                
                if not extract:
                    if self.verbose:
                        print "\t", meta, _xpath
                    continue
                extracted = True
                if self.verbose:
                    print meta, extract, _xpath
                try:
                    if not with_label:
                        self.metas[meta] = extract
                    else:
                        if "|" in _xpath[1]:
                            ok = False
                            for label in _xpath[1].split("|"):
                                if label in extract.upper():
                                    ok = True
                                    break
                        else:
                            ok = _xpath[1] in extract.upper() or _xpath[1] in extract_parent.upper()
                        
                        if ok:
                            self.metas[meta] = extract.split(":")[1] if ":" in extract else extract
                            break
                        else:
                            continue
                            #~ self.metas[meta] = "*********" + extract + "|" + _xpath[1]
                        
                    
                except:
                    print "Ha fallado: ", meta, extract, _xpath
                    raise
        
            if meta in self.metas:
                self.metas[meta] = self.metas[meta].strip()
        
        if not extracted:
            self.logger.warning("No se ha podido extraer %s" % url)
            return True
        
        
        if 'alt="proximamente"' in data_url:
            proximamente = True
            
        if u"CÓMIC" in self.metas['subcategory']:
            self.metas['subcategory'] = self.normalize_category(" ".join(self.metas['subcategory'].split(" ")[1:]))
        else:
            self.metas['subcategory'] = self.normalize_category(self.metas['subcategory'])
        
        (self.metas['id'], self.metas['mfgid'], self.metas['extra_field_1'], stock_external) = self.get_external(self.metas['extra_field_7'])
        
        for x in xrange(1, 5):
            if not self.metas['id']:
                (self.metas['id'], self.metas['mfgid'], self.metas['extra_field_1'], stock_external) = self.get_external("NOR.%s" % url.split("/")[5][x:])
                
        
        
        if not self.metas['id'] and not proximamente:
            try:
                fix = re.findall(".*([0-9]{4}-[0-9]{3}).*?",self.metas['extra_field_7'])[0]
            except IndexError:
                try:
                    fix = re.findall(".*([0-9]{3}-[0-9]{4}).*?",self.metas['extra_field_7'])[0]
                except IndexError:
                    try:
                        fix = re.findall(".*([0-9]{5}-[0-9]{2}).*?",self.metas['extra_field_7'])[0]
                    except IndexError:
                        if not "-" in self.metas['extra_field_7']:
                            try:
                                fix = re.findall(".*?84([0-9]{7}).*?",self.metas['extra_field_7'])[0]
                            except IndexError:
                                self.logger.warning("No se encuentra el ISBN, No se ha podido extraer %s" % url)
                                return True
                        else:
                            #~ raise IndexError()
                            self.logger.warning("No se encuentra el ISBN, No se ha podido extraer %s" % url)
                            return True
                
            sufix = 0
            while not self.metas['id']:
                isbn = "978-84-%s-%d" % (fix, sufix)
                self.logger.info("[extract_product] No localizado. Intentando localizar %s" % isbn)
                #~ print isbn

                (self.metas['id'], self.metas['mfgid'], self.metas['extra_field_1'], stock_external) = self.get_external(isbn)
                sufix += 1
                if sufix > 9:
                    break
                    
            
        
        if not self.metas['id']: 
        
            def normalize_id(s):
                chars = '/"'
                for c in chars:
                    s = s.replace(c,"")
                return s
            
            
            _id = normalize_id(self.normalize_category("".join([w[:2] for w in self.metas['title'].split()])))
            if proximamente:
                
                self.metas['id'] = self.metas['mfgid'] = "NORPROX.%s" % _id
                
                comming_date = datetime.now() + timedelta(days = 15)
                self.metas['extra_field_1'] = comming_date.strftime("%d/%m/%Y")
                
            else:
                self.metas['id'] = self.metas['mfgid'] = "NORNO.%s" % _id
                self.metas['extra_field_1'] = "1/1/2008"
                
            has_comprar = False
            
        else:
            has_comprar = stock_external == 10
        
        #date
        date_created = time.strptime(self.metas['extra_field_1'].strip(), "%d/%m/%Y")
        self.metas['date_created'] = time.strftime("%m/%d/%Y", date_created)
        d_created = datetime(date_created.tm_year, date_created.tm_mon, date_created.tm_mday)
        
        if d_created > now:
            proximamente = True
        
        #category validations
        if self.metas['subcategory'] in self.category_alias:
            self.metas['subcategory'] = self.category_alias[self.metas['subcategory']]
        
        
        
        #category bans
        if self.metas['category'] in self.category_ban:
            if isinstance(self.category_ban[self.metas['category']], basestring):
                return False
            else:
                #subcategory
                if self.metas['subcategory'] in self.category_ban[self.metas['category']]:
                    return False
        
        
        
        
        #~ if not self.metas['id']:
            #~ raise Exception("No se pudo localizar el id")
        
        #categories
        title_collection = get_title_collection(self.metas['title'], self.metas['category'], self.metas['manufacturer'])
        manufacturer = self.metas['manufacturer'] if self.metas['manufacturer'] else "VARIOS"
        
        
        
        if not proximamente: 
            
            #CATEGORIA_PRINCIPAL@CATEGORIA_PRINCIPAL/SUBCATEGORIA@CATEGORIA_PRINCIPAL/SUBCATEGORIA/EDITORIAL@CATEGORIA_PRINCIPAL/SUBCATEGORIA/EDITORIAL/TITULO -(menos ó sin) NUMERO COLECCION
            self.metas['categories'] = "%s@%s/%s@%s/%s/%s@%s/%s/%s/%s" % \
              (self.metas['category'], self.metas['category'], self.metas['subcategory'], \
              self.metas['category'], self.metas['subcategory'], manufacturer, \
              self.metas['category'], self.metas['subcategory'], manufacturer, \
              title_collection)
        else:
            #comming
            self.metas['categories'] = "PROXIMAMENTE@PROXIMAMENTE/%s@PROXIMAMENTE/%s/%s@PROXIMAMENTE/%s/%s/%s" % \
              (self.metas['category'], self.metas['category'], self.metas['subcategory'], \
              self.metas['category'], self.metas['subcategory'], manufacturer)
              
        self.metas['categories'] = "@".join([self.normalize_category(c) for c in self.metas['categories'].split("@")])
        
        #price and cost
        if "por confirmar" in self.metas['price2'].lower():
            self.metas['price2'] = "0"
        else:
            #without euro symbol
            self.metas['price2'] = self.metas['price2'].replace(u"\xa0\u20ac","").replace(u"\x80","")
        
        self.metas['cost'] = float(self.metas['price2'].replace(".","").replace(",",".")) * 0.7
            
        self.metas['price'] = float(self.metas['price2'].replace(".","").replace(",",".")) * 0.95
        
        
        #date
        date_created = time.strptime(self.metas['extra_field_1'].strip(), "%d/%m/%Y")
        self.metas['date_created'] = time.strftime("%m/%d/%Y", date_created)
        
        #descriptions
        def smart_truncate(content, length=100, suffix=''):
            if len(content) <= length:
                return content
            else:
                return ' '.join(content[:length+1].split(' ')[0:-1]) + suffix
        
        def clean_spaces(s):
            s = ' '.join(s.splitlines())
            while "  " in s:
                s = s.replace("  ", " ")
            return s
                    
        if 'description' in self.metas:
            self.metas['description'] = smart_truncate(clean_spaces(self.metas['description']))
            self.metas['extended_description'] = clean_spaces(self.metas['extended_description'])
        


        #stock & instock_message
        #~ has_comprar = "ficha_comprar.gif" in data_url
        
        self.metas['stock'] = 40 if proximamente else 10 if has_comprar else 0

        self.metas['instock_message'] = "Pre-Reserva" if self.metas['stock'] == 40 \
          else "Añadir a Lista de Espera" if self.metas['stock'] == 0 \
          else "En Stock - 3/5 Días"
          
        #images
        self.metas['thumbnail'] = self.metas['image1'] = self.config['img_norma'] % url.split("/")[-3]
        
        try:
            self.metas['image2'] = self.extracts('//*[@id="gallery-in"]/div[3]')[0].attrib['style'].split("background-image:url(")[1].split(")")[0]
            self.metas['image3'] = self.extracts('//*[@id="gallery-in"]/div[4]')[0].attrib['style'].split("background-image:url(")[1].split(")")[0]
            self.metas['image4'] = self.extracts('//*[@id="gallery-in"]/div[5]')[0].attrib['style'].split("background-image:url(")[1].split(")")[0]
        except:
            pass
            
            
        for key_image, sufix in {'thumbnail':'_tb', 'image1':'', 'image2':'_2', 'image3':'_3', 'image4':'_4'}.items():
            if key_image in self.metas:
                #~ print self.metas[key_image]
                if not "http" in self.metas[key_image]:
                    self.metas[key_image] = "http://%s%s" % (self.config['domain'], self.metas[key_image])
                filename = "%s%s.jpg" % (self.metas["id"], sufix)
                if not self.download_img(self.metas[key_image], filename , thumbnail = key_image == "thumbnail" ):
                    del self.metas[key_image]
                    continue
                
                
                finalname = "%s%s/%s/%s/%s" % (self.config['url_images'], self.metas['category'], self.metas['subcategory'], \
                  self.metas['manufacturer'], filename)
                self.metas[key_image] = self.normalize_path(finalname)

        #homespecial
        now = datetime.now()
        date_created = time.strptime(self.metas['extra_field_1'].strip(), "%d/%m/%Y")
        
        d_created = datetime(date_created.tm_year, date_created.tm_mon, date_created.tm_mday)
        
        self.metas['homespecial'] = 1 if abs((now - d_created).days) <10 else 0
        
        #reward_points
        self.metas['reward_points'] = int(self.metas['price'] * 20 if d_created > now else self.metas['price'] * 10)
        
        
        #keywords & metatags
        keys_keywords = ["category", "subcategory", "manufacturer", "title", "extra_field_10", "extra_field_3"]
        self.metas['keywords'] = ", ".join(self.metas[i].strip() for i in keys_keywords if i in self.metas and len(self.metas[i])>1)
        
        def cut_last_comma(s):
            if s[-1] == ",":
                s = s[:-1]
            if s[-2] == ", ":
                s = s[:-2]
            return s
        
        self.metas['keywords'] = cut_last_comma(self.metas['keywords'])
        if 'extra_field_10' in self.metas:
            self.metas['extra_field_10'] = cut_last_comma(self.metas['extra_field_10'])
        
        self.metas['metatags'] = '<META NAME="KEYWORDS" CONTENT="%s">' % self.metas['keywords']
        
            
        if previous_metas:
            #has been seen already
            if previous_metas['stock'] == self.metas['stock'] and previous_metas['price'] == self.metas['price']:
                #has modifications but not in price or stock. Dont update.
                return True
            
          
        
        #extra_field_11  
        if 'extra_field_11' in self.metas and self.metas['extra_field_11']:
            self.metas['extra_field_11'] = "<div>%s</div>" % self.metas['extra_field_11']
        
        self.metas['price2'] = self.metas['price2'].replace(",", ".")
            
        #~ encode_keys = ["id", "mfgid", "title", "name", "categories", "extra_field_10", "thumbnail", \
          #~ "image1", "image2", "image3", "image4", "content", "extra_field_3", "extra_field_2", \
          #~ "extra_field_4", "extra_field_5", "extra_field_6", "manufacturer"] 
        #~ for encode_key in encode_keys:
            #~ if encode_key in self.metas:
                #~ try:
                    #~ self.metas[encode_key] = self.metas[encode_key].encode("utf-8")
                #~ except:
                    #~ print encode_key, self.metas[encode_key], repr(self.metas[encode_key])
                    #~ 
                    #~ raise

        for meta in self.metas:
            if isinstance(self.metas[meta],float):
                self.metas[meta] = str(round(self.metas[meta],2))
            else:
                if isinstance(self.metas[meta],basestring):
                    try:
                        self.metas[meta] = self.metas[meta].encode("utf-8")
                    except UnicodeDecodeError:
                        pass
            #~ print meta, self.metas[meta]
            
        
            
        self.db.save_data(url, self.metas, self.id_task)
        #~ self.print_line(self.get_metas_orderer())
        self.upload_images()

    def run(self):
        """start complete crawler"""
        
        self.logger.info("[run] iniciando(Completo=%s)" % self.mode_complete)
        
        try:
            self.db.init_task(self.id_task)
            
            #comming
            page = 0
            n_products = 1
            #~ while n_products > 0:
                #~ url = self.config['discover_url'].replace("catalogo", "proximo") % ("X", "0", page)
                #~ self.logger.info("[run] recorriendo %s" % url)
                #~ exit()
                #~ 
                #~ try:
                    #~ self.tree = etree.fromstring(self.download_url(url), self.parser)
                #~ except ValueError:
                    #~ break
                #~ 
                #~ n_products = 0
                #~ products = self.extracts('//div[@class="SUB_centro_IZ_CATALOGO_ficha"]')
                #~ for product in products:
                    #~ self.extract_product("http://%s/%s" % (self.config['domain'], product.xpath(".//a/@href")[0]), True)
                    #~ 
                    #~ n_products += 1
                #~ 
                #~ page += 1
            
            
            #novedades
            
            
            
            for category in "185":
                #by category
                page = 0
                n_products = 1
                while n_products > 0:
                    url = self.config['discover_url'] % ("T", category, page)
                    self.logger.info("[run] recorriendo %s" % url)
                    self.tree = etree.fromstring(self.download_url(url), self.parser)
                    
                    n_products = 0
                    products = self.extracts('//div[@class="llistaBOTT"]/ul/li')
                    for product in products:
                        #exclude comming
                        self.extract_product(product.xpath(".//a/@href")[0])
                        
                        n_products += 1
                    
                    
                    page += 18
                    
            self.generate_csv()
            
            self.db.finish_task(self.id_task)
        except Exception as e:
            self.db.finish_task(self.id_task, True)
            
            exc_type, exc_obj, exc_tb = sys.exc_info()

            self.logger.error("%s\n %d: %s" %(traceback.format_exc(), exc_tb.tb_lineno, str(e)))
            raise
        

        
if __name__ == '__main__':
    
    
    if len(sys.argv) == 1:
        crawl = CrawlerComics_1()
        crawl.run()
    else:
        if "http" in sys.argv[1]:
            for url in sys.argv[1:]:
                crawl = CrawlerComics_1()
                crawl.extract_product(url)
                crawl.generate_csv()
            
                crawl.db.finish_task(crawl.id_task)
        else:
            crawl = CrawlerComics_1(id_task = sys.argv[1], mode = sys.argv[2])
            crawl.run()
