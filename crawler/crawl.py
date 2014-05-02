#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, os, urllib2, urllib, cookielib
from lxml import etree
from PIL import Image
import time, logging, logging.handlers
from datetime import datetime
from pprint import pprint
import csv, shutil, re
from ftplib import FTP_TLS, error_perm
from binascii import crc32
from db import DB
from utils import *
from urllib import quote
import traceback


#hack
import ssl
class mFTP_TLS(FTP_TLS):
	def storbinary(self, cmd, fp, blocksize=8192, callback=None, rest=None):
		self.voidcmd('TYPE I')
		conn = self.transfercmd(cmd, rest)
		try:
			while 1:
				buf = fp.read(blocksize)
				if not buf: break
				conn.sendall(buf)
				if callback: callback(buf)
			# shutdown ssl layer
			if isinstance(conn, ssl.SSLSocket):
				#conn.unwrap()
				conn.close
		finally:
			conn.close()
		return self.voidresp()


class CrawlerComics(object):
	def __init__(self, verbose = False, id_task = None, mode = "0"):
		
		self.verbose = verbose
		
		# 0 -> complete
		# 1 -> only updates and deletes
		self.mode_complete = mode == "0"
		
		
		self.parser = etree.HTMLParser()
		self.config = {}
		config_file = os.path.join(os.path.dirname(__file__), "crawler_comics.conf")
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
		
		self.init_metas()
		
		#key: [xpath, "label(s)"]
		self.xpaths= {"id":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[1]//text()","REF"],
			"mfgid":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[1]//text()","REF"],
			"name":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h2//text()"],
			"title":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h2//text()"],
			"manufacturer":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h4[$1]//text()","EDITORIAL|FABRICANTE|PRODUCTORA"],
			"date":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[$1]//text()","FECHA"],
			"rprice":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[2]/td/table/tr/td[2]//text()"],
			"lprice":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[2]/td/table/tr/td/h4//text()"],
			"description":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h6//text()"],
			"thumbnail":['/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[1]/div/a/img[1]/@src'],
			"image1":['/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[1]/div/a/img[1]/@src'],
			"image2":['/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[1]/div/a[2]/img[1]/@src'],
			"image3":['/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[1]/div/a[3]/img[1]/@src'],
			"image4":['/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[1]/div/a[4]/img[1]/@src'],
			"extended_description":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h6//text()"],
			"label_stock":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[$1]//text()", u"PRÓXIMA|DISPONIBLE|PENDIENTE|AGOTADO|SALDADO"],
			"extra_field_10":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h4[$1]//text()","AUTOR"],
			"extra_field_2":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h4[$1]//text()","EDITORIAL|FABRICANTE"],
			"extra_field_3":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[$1]//text()",u"COLECCIÓN"],
			"extra_field_4a":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h4[$1]//text()",u"ENCUADERNACIÓN|PRESENTACIÓN"],
			"extra_field_4b":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[$1]//text()",u"ENCUADERNACIÓN|PRESENTACIÓN"],
			"extra_field_5":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h4[$1]//text()",u"PÁGINAS"],
			"extra_field_7":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[$1]//text()","ISBN"],
			"extra_field_9":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[$1]//text()","MATERIAL"],
			"extra_field_11":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]/td[2]/h3[$1]//text()","EAN"],
			"content":["/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/table/tr[1]//text()"]
			}
		
		
		self.category_alias = {"LIBROS" : {
		    "ANIMACION" : "ILUSTRACIONES",
		    "COMIC ARGENTINO" : "COMIC IBEROAMERICANO", 
		    "COMIC MEXICANO" : "COMIC IBEROAMERICANO", 
		    "COMIC MANGA" : "MANGA", 
		    "ILUSTRACION FOTOGRAFICA" : "ILUSTRACIONES", 
		    "ILUSTRADO" : "ILUSTRACIONES", 
		    "LIBRO AVENTURA" : "ILUSTRACIONES"} ,
		  "COMICS": {
		    "COMIC ARGENTINO" : "COMIC IBEROAMERICANO",
		    "COMIC MEXICANO" : "COMIC MEXICANO",
		    "COMIC MANGA" : "MANGA",
		    "FANZINES" : "REVISTA DE COMICS",
		    "ILUSTRACION FOTOGRAFICA" : "ILUSTRACIONES",
		    "ILUSTRADO" : "ILUSTRACIONES",
		    "MANHWA" : "MANGA", 
			"TEMATICA GAY" : "ADULTO EROTICO",
			"TERROR" : "COMIC",
			"VARIOS-OTROS" : "REVISTA DE COMICS"},
		   "MERCHANDISING-JUGUETES" : "MERCHANDISING",
		   "MERCHANDISING - JUGUETES" : "MERCHANDISING",
		   "MERCHANDISING LIBROS" : "MERCHANDISING",
		   "PANINI MARVEL EXCLUSIVA" : "PANINI MARVEL",
		   "COMPLEMENTOS" : "ACCESORIOS", 
		   "JUEGOS DE CARTAS COLECC" : "JUEGOS",
		   "JUEGOS DE MESA" : "JUEGOS"}
		   
		self.category_ban = {"LIBROS" : ["BIOGRAFIA", "MUSICA"],
		   "COMICS" : ["ENSAYO", "PAPELERIA"],
		   "VARIOS" : ""} 

		
		#initialite DB
		self.db = DB(self.logger, config_file)
		self.db.init_model()
		
		if not id_task:
			self.id_task = self.db.start_new_task()
		else:
			self.id_task = int(id_task)
			
		#initialite csv
		self.filename_csv = os.path.join(os.path.dirname(__file__), "csv/%s" % self.config['csv_filename'] % self.id_task)
		
		self.print_line(self.config["csv_header"], True)
			
			
	def normalize_category(self, cat):
		replace_chars = {u"¿" : "" , u"?" : "" , u"!" : "" , u"¡" : "", 
		  u"%" : "", u"#" : "" , u"@" : "" , u"Á" : "A", u"É" : "E" , 
		  u"Í" : "I" , u"Ó" : "O" , u"Ú" : "U"}
		
		for c1, c2 in replace_chars.items():
			cat = cat.replace(c1, c2)
			
		return cat
	
	def normalize_path(self, path):
		replace_chars = {u"Ñ" : "N"}
		
		for c1, c2 in replace_chars.items():
			try:
				path = path.replace(c1, c2)
			except UnicodeDecodeError:
				path = path.decode("utf-8").replace(c1, c2)
			
		return path
	
			
	def init_metas(self):
		self.metas = {"distributor" : self.config['distributor'], "extra_field_13": 0}

	
	def extract(self, xpath):
		try:
			find = etree.XPath(xpath)
			return find(self.tree)[0]
		except:
			if self.verbose:
				print "No se ha podido extrar ", xpath
			
			return ""
	
	def extracts(self, xpath):
		try:
			find = etree.XPath(xpath)
			return find(self.tree)
		except:
			if self.verbose:
				print "No se ha podido extrar ", xpath
			
			return ""
			
	def img_has_border(self, im, boxpos):
		
		rgb_im = im.convert('RGB')
		#lef top
		limit = 200
		
		
		a_pos = []
		if boxpos == "top":
			for x in xrange(0, rgb_im.size[0]):
				a_pos.append(( x, 0))
				
		if boxpos == "bottom":
			for x in xrange(0, rgb_im.size[0]):
				a_pos.append(( x, rgb_im.size[1]-1))
		if boxpos == "left":
			for x in xrange(0, rgb_im.size[1]):
				a_pos.append(( 0, x))
				
		if boxpos == "right":
			for x in xrange(0, rgb_im.size[1]):
				a_pos.append(( rgb_im.size[0] - 1, x))
		
		
		#~ print boxpos, pos, im.size
		idx = 0
		white = 0
		non_white = 0
		for pos in a_pos:
			try:
				r, g, b = rgb_im.getpixel(pos)
			except IndexError:
				print boxpos, a_pos, pos, rgb_im.size
				raise
			#~ print boxpos, idx, r, g, b
			idx += 1
			if (r < limit or g < limit or b < limit) and r + b < 508 and r + g < 508 and g + b < 508:
				non_white +=1
			else:
				white +=1
				
			#5% no white -> no border
			if non_white > len(a_pos) / 20:
				return False
				
		return True
		
			
		
			
		#~ #left bottom
		#~ r, g, b = rgb_im.getpixel((0, rgb_im.size[1]-1))
		#~ print "\t",  r, g, b
		#~ if r >= limit and g >= limit and b >= limit:
			#~ return True
#~ 
		#~ #right top
		#~ print "\t\t",  r, g, b
		#~ r, g, b = rgb_im.getpixel((rgb_im.size[0]-1, 0))
		#~ if r >= limit and g >= limit and b >= limit:
			#~ return True
#~ 
#~ 
		#~ #right bottom
		#~ print "\t\t\t",  r, g, b
		#~ r, g, b =  rgb_im.getpixel((rgb_im.size[0]-1, rgb_im.size[1]-1))
		#~ if r >= limit and g >= limit and b >= limit:
			#~ return True
		
		#~ return False
		
	def img_background_white(self, im):
		colors = im.getcolors()
		if colors:
			return  colors[0][0] / float(sum(c[0] for c in colors)) > 0.8
		
			
	def download_img(self, url, filename, thumbnail = False):
		
		path = os.path.join( os.path.dirname(__file__), "imgs/%s" % self.normalize_path(filename.encode("utf-8")))
		max_border = 100
	
		url = quote(url.encode("utf-8"),":/")	
		if url.endswith("No_Disponible.gif"):
			#no image
			shutil.copy(os.path.join(os.path.dirname(__file__), \
			  "imgs/SuperComicsImagenNoDisponible.jpg"), \
			  os.path.join(os.path.dirname(__file__), path))
		else:
			r = urllib2.urlopen(url)
			f = open(path, "w")
			f.write(r.read())
			f.close()
		im = Image.open(path)
		
		
		
		ori_size = im.size
		save = True
		#crop border
		#~ while self.img_has_border(im):
			#~ size = (int(im.size[0]*0.99), int(im.size[1]*0.99))
			#~ diff_size = (im.size[0] - size[0], im.size[1] - size[1])
			#~ im = im.crop((diff_size[0] / 2, diff_size[1] / 2, im.size[0] - (diff_size[0] / 2), im.size[1] - diff_size[1] / 2))
			#~ if ori_size[0] - im.size[0] > max_border:
				#~ save = False
				#~ break
				
		if not self.img_background_white(im):
			for boxpos in ["top", "bottom", "left", "right"]:
				while self.img_has_border(im, boxpos):
					if boxpos == "top":
						im = im.crop((0, 1, im.size[0], im.size[1]))
					if boxpos == "bottom":
						im = im.crop((0, 0, im.size[0], im.size[1]-1))
					if boxpos == "left":
						im = im.crop((1, 0, im.size[0], im.size[1]))
					if boxpos == "right":
						im = im.crop((0, 0, im.size[0] - 1, im.size[1]))
					if im.size[0] < 100 or im.size[1] < 100:
						save = False
						break
					
					if ori_size[0] - im.size[0] > max_border:
						save = False
						break
						
				if not save:
					break
					
			
			
		if save:
			im.save(path, "JPEG")
		else:
			im = Image.open(path)
			
		#resize
		
		r_size = (109, 146) if thumbnail else (263, 400)
		
		if im.size[0] > r_size[0] or im.size[1] > r_size[1]:
			im.thumbnail(r_size, Image.ANTIALIAS)
			im.save(path, "JPEG")
		


			
	def download_url(self, url):
		
		cj = cookielib.CookieJar()
		opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

		opener.addheaders = [('User-agent', self.config['user_agent'])]

		urllib2.install_opener(opener)

		authentication_url = self.config['url_login']
		
		url = quote(url.encode("utf-8"),":/")
		for b in re.findall(".*?(%[0-9a-zA-Z]{2}).*?", url):
			url = url.replace(b, "")
		
		
		post = {'URLx':quote(url.encode("utf-8"),":/").split(self.config['domain'])[1],
		        'login':self.config['login'],
		        'Password':self.config['password'],
		        'Ok': 'OK',
		        'modo' : 'Cercador'}


		req = urllib2.Request(self.config['url_login'], urllib.urlencode(post))
		
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
				if tries == 5:
					return self.download_url(url)
				if tries > 5:
					raise
				else:
					self.logger.info("[download_url] Reintentando ...")
				time.sleep(tries)
			
		data = resp.read()
		
		return data
        
        
	
	def extract_product(self, url, category, subcategory):
		"""extract metadata from product page"""
		
		self.logger.info("[extract_product] %s" % url)
		
		self.tree = etree.fromstring(self.download_url(url), self.parser)
		
		self.metas = self.db.load_data(url)
		
		
		now = datetime.now()
		
		previous_metas = None
		
		if self.metas:
			date_created = time.strptime(self.metas['extra_field_1'].strip(), "%d/%m/%Y")
		
			d_created = datetime(date_created.tm_year, date_created.tm_mon, date_created.tm_mday)
			
			if now > d_created and "PROXIMAMENTE" in self.metas['categories']:
				#not modified but publish date exceeded
				pass
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
		
		self.init_metas()
		self.metas['category'] = category.upper()
		self.metas['subcategory'] = subcategory.upper()
		
		#remove tildes
		self.metas['category'] = self.normalize_category(self.metas['category'])
		self.metas['subcategory'] = self.normalize_category(self.metas['subcategory'])
		
		is_merchandising = "MERCHANDISING" in self.metas['category']
		
		#category validations
		if self.metas['category'] in self.category_alias:
			if isinstance(self.category_alias[self.metas['category']], basestring):
				self.metas['category'] = self.category_alias[self.metas['category']]
			else:
				#subcategory
				if self.metas['subcategory'] in self.category_alias[self.metas['category']]:
					self.metas['subcategory'] = self.category_alias[self.metas['category']][self.metas['subcategory']]
		
		
		#category bans
		if self.metas['category'] in self.category_ban:
			if isinstance(self.category_ban[self.metas['category']], basestring):
				return False
			else:
				#subcategory
				if self.metas['subcategory'] in self.category_ban[self.metas['category']]:
					return False
		
		
		
		#~ self.tree = etree.parse(url, self.parser)
		
		
		
		for meta, _xpath in self.xpaths.items():
			with_label = len(_xpath) > 1
			
			#search in 8 posible positions if has label
			for i in xrange(0, 1 if not with_label else 9):
				xpath = _xpath[0].replace("$1",str(i+1))
				extract = self.extract(xpath) if meta != "content" else "".join(self.extracts(xpath))
				
				if not extract:
					if self.verbose:
						print "\t", meta, _xpath
					continue
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
							ok = _xpath[1] in extract.upper()
						
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
				
		if "lprice" in self.metas and "rprice" in self.metas:	
			self.metas['price2'] = self.metas['lprice'] if "PRECIO FINAL" in self.metas['rprice'] else self.metas['rprice']
			self.metas['price2'] = self.metas['price2'].split(":")[1].strip()
			del self.metas['lprice']
			del self.metas['rprice']
		
		
		
		date_created = time.strptime(self.metas['date'].strip(), "%d.%m.%Y")
		self.metas['extra_field_1'] = time.strftime("%d/%m/%Y", date_created)
		self.metas['date_created'] = time.strftime("%m/%d/%Y", date_created)
		
		d_created = datetime(date_created.tm_year, date_created.tm_mon, date_created.tm_mday)
		
		title_collection = get_title_collection(self.metas['title'], self.metas['category'], self.metas['manufacturer'])

		
		
		if now > d_created: 
			#comming
			#CATEGORIA_PRINCIPAL@CATEGORIA_PRINCIPAL/SUBCATEGORIA@CATEGORIA_PRINCIPAL/SUBCATEGORIA/EDITORIAL@CATEGORIA_PRINCIPAL/SUBCATEGORIA/EDITORIAL/TITULO -(menos ó sin) NUMERO COLECCION
			self.metas['categories'] = "%s@%s/%s@%s/%s/%s@%s/%s/%s/%s" % \
			  (self.metas['category'], self.metas['category'], self.metas['subcategory'], \
			  self.metas['category'], self.metas['subcategory'], self.metas['manufacturer'], \
			  self.metas['category'], self.metas['subcategory'], self.metas['manufacturer'], \
			  title_collection)
		else:
			self.metas['categories'] = "PROXIMAMENTE@PROXIMAMENTE/%s@PROXIMAMENTE/%s/%s@PROXIMAMENTE/%s/%s/%s@PROXIMAMENTE/%s/%s/%s/%s" % \
			  (self.metas['category'], self.metas['category'], self.metas['subcategory'], \
			  self.metas['category'], self.metas['subcategory'], self.metas['manufacturer'], \
			  self.metas['category'], self.metas['subcategory'], self.metas['manufacturer'], \
			  title_collection)
		
		self.metas['homespecial'] = 1 if abs((now - d_created).days) <10 else 0
		
		
		for key_image, sufix in {'thumbnail':'_tb', 'image1':'', 'image2':'_2', 'image3':'_3', 'image4':'_4'}.items():
			if key_image in self.metas:
				if not "http" in self.metas[key_image]:
					self.metas[key_image] = "http://%s%s" % (self.config['domain'], self.metas[key_image])
				filename = "%s%s.jpg" % (self.metas["id"], sufix)
				self.download_img(self.metas[key_image], filename , thumbnail = key_image == "thumbnail" )
				
				
				finalname = "%s%s/%s/%s/%s" % (self.config['url_images'], self.metas['category'], self.metas['subcategory'], \
				  self.metas['manufacturer'], filename)
				self.metas[key_image] = self.normalize_path(finalname)
				
		
		if "por confirmar" in self.metas['price2'].lower():
			self.metas['price2'] = "0"
		else:
			#without euro symbol
			self.metas['price2'] = self.metas['price2'].replace(u"\xa0\u20ac","")
		
		self.metas['cost'] = float(self.metas['price2'].replace(".","").replace(",",".")) * 0.7
			
		self.metas['price'] = float(self.metas['price2'].replace(".","").replace(",",".")) * 0.95
		
		
		if not 'category' in self.metas:
			self.metas['category'] = "UNK"
		self.metas['tax_code'] = 'IVL' if self.metas['category'] in ['LIBROS', 'COMICS', 'DVD-BLU RAY', 'REVISTAS'] else 'IVO'
		
		def smart_truncate(content, length=100, suffix=''):
			if len(content) <= length:
				return content
			else:
				return ' '.join(content[:length+1].split(' ')[0:-1]) + suffix
		
		def clean_spaces(s):
			while "  " in s:
				s = s.replace("  ", " ")
			return s
				
		
		self.metas['description'] = smart_truncate(clean_spaces(self.metas['description'])).encode("utf-8")
		self.metas['extended_description'] = clean_spaces(self.metas['extended_description']).encode("utf-8")
		
		
		keys_keywords = ["category", "subcategory", "manufacturer", "title", "extra_field_10", "extra_field_3"]
		self.metas['keywords'] = ", ".join(self.metas[i].strip() for i in keys_keywords if i in self.metas and len(self.metas[i])>1).encode("utf-8")
		
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
		
		l_stock = self.metas['label_stock'].lower()
		
		self.metas['stock'] = 40 if u"próxima" in l_stock else 10 \
		  if "saldado" in l_stock or "disponible" in l_stock else 0
		  
		if d_created > now: 
			self.metas['stock'] = 40
			
		if previous_metas:
			#has been seen already
			if previos_metas['stock'] == self.metas['stock'] and previos_metas['price'] == self.metas['prive']:
				#has modifications but not in price or stock. Dont update.
				return True
			
		  
		self.metas['instock_message'] = "Pre-Reserva" if self.metas['stock'] == 40 \
		  else "Añadir a Lista de Espera" if self.metas['stock'] == 0 \
		  else "En Stock - 3/5 Días"
		
		  
		self.metas['reward_points'] = int(self.metas['price'] * 20 if d_created > now else self.metas['price'] * 10)
		
		self.metas['extra_field_4'] = self.metas['extra_field_4a'].encode("utf-8") if 'extra_field_4a' in self.metas \
		  and self.metas['extra_field_4a'] else self.metas['extra_field_4b'].encode("utf-8") \
		  if 'extra_field_4b' in self.metas else ""
		  
		if 'extra_field_11' in self.metas and self.metas['extra_field_11']:
			self.metas['extra_field_11'] = "<div>%s</div>" % self.metas['extra_field_11']
			
		encode_keys = ["id", "mfgid", "title", "name", "categories", "extra_field_10", "thumbnail", \
		  "image1", "image2", "image3", "image4", "content", "extra_field_3", "extra_field_2", "extra_field_5", "manufacturer"] 
		for encode_key in encode_keys:
			if encode_key in self.metas:
				try:
					self.metas[encode_key] = self.metas[encode_key].encode("utf-8")
				except:
					print encode_key, self.metas[encode_key], repr(self.metas[encode_key])
					
					raise

		for meta in self.metas:
			if isinstance(self.metas[meta],float):
				self.metas[meta] = str(round(self.metas[meta],2))
			#~ print meta, self.metas[meta]
			
		self.db.save_data(url, self.metas, self.id_task)
		#~ self.print_line(self.get_metas_orderer())
		self.upload_images()
		
	def upload_images(self):
		
		
		connected = False
		tries = 0
		while not connected:
			try:
				ftps = mFTP_TLS(self.config['ftp_host'], timeout = 60)
				connected = True
			except:
				tries +=1
				if tries > 5:
					raise
				time.sleep(tries)
			
		
		ftps.login(self.config['ftp_user'], self.config['ftp_pass'])
		ftps.prot_p()
		
		#~ print ftps.retrlines('LIST')
		
		for key_image in ['thumbnail', 'image1', 'image2', 'image3', 'image4']:
			ftps.cwd(self.config['path_images'])
			if key_image in self.metas and self.metas[key_image]:
				
				self.logger.info("[upload_images] subiendo %s" % self.metas[key_image].replace(self.config['url_images'],""))
				paths = ["nuevoSD"] + self.metas[key_image].replace(self.config['url_images'],"").split("/")[:-1]
				filename = self.metas[key_image].replace(self.config['url_images'],"").split("/")[-1]
				local_filename = os.path.join(os.path.dirname(__file__),"imgs/%s" % filename)
				for path in paths:
					try:
						ftps.cwd(path)
					except:
						created = False
						tries = 0
						while not created:
							try:
								ftps.mkd(path)
								created = True
							except:
								tries +=1
								if tries > 5:
									raise
								time.sleep(tries)
						
						ftps.cwd(path)
				
				#check if exists
				try:
					if ftps.size(filename) == os.path.getsize(local_filename):
						#exists
						self.logger.info("[upload_images] ya habia copia en el ftp de %s" % self.metas[key_image].replace(self.config['url_images'],""))
						continue
				except error_perm as e:
					if not "550" in str(e):
						raise
					else:
						#file not exists
						pass
				
				f = open(local_filename, 'rb')
				uploaded = False
				tries = 0
				while not uploaded:
					try:
						ftps.storbinary('STOR ' + filename, f, 1024)
						uploaded = True
					except ssl.SSLError:
						tries +=1
						if tries > 5:
							raise
						time.sleep(tries)
				
					
		ftps.quit()
		
	
		
	def get_metas_orderer(self):
		"""select metas required"""
		

		return [self.metas[meta] if meta in self.metas and self.metas[meta] \
		  or (not "extra_field" in meta) else "N/A" for meta in self.config['csv_header']]

		
	def print_line(self, line, header = False):
		"""print line in csv"""
		
		#~ pprint([str(i).replace(",", ".") if is_number(i) else i for i in line])
		#~ pprint([is_number(i) for i in line])
		with open(self.filename_csv, 'wb' if header else 'ab') as csvfile:
			csvwriter = csv.writer(csvfile, delimiter='\t',quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
			csvwriter.writerow(line)
			
	def run(self):
		"""start complete crawler"""
		
		self.logger.info("[run] iniciando(Completo=%s)" % self.mode_complete)
		
		try:
			self.db.init_task(self.id_task)
	
			self.tree = etree.fromstring(self.download_url(self.config['start_url']), self.parser)
			start_week = self.extract("/html/body/table[2]/tr[1]/td[3]/table[3]/tr[6]/td/a[1]/@href").split("/")[-2]
			
			week = int(start_week[-2:])
			year = int(start_week[:4])
			
			#forward
			n_products = 1
			while n_products > 0:
				print year, week
				n_products = self.extract_products(year, week)
				print year, week, n_products
				self.logger.info("[run] extraidos %s productos en %s/%s" % ( n_products, year, week))
				week += 1
				if week > 52:
					week = 1
					year += 1
					
			#back
			week = int(start_week[-2:])
			year = int(start_week[:4])
			n_products = 1
			current_week = True
			while n_products > 0:
				#skip the current
				if not current_week:
					print year, week
					n_products = self.extract_products(year, week)
					print year, week, n_products
				current_week = False
				week -= 1
				if week < 1:
					week = 52
					year -= 1
					
			self.generate_csv()
			
			self.db.finish_task(self.id_task)
		except Exception as e:
			self.db.finish_task(self.id_task, True)
			
			exc_type, exc_obj, exc_tb = sys.exc_info()
			#~ fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			#~ print(exc_type, fname, exc_tb.tb_lineno)
			#~ 
			self.logger.error("%s\n %d: %s" %(traceback.format_exc(), exc_tb.tb_lineno, str(e)))
			raise
		
	
	def generate_csv(self):
		"""generate csv from db"""
		
		self.logger.info("[generate_csv] task: %s modo:%s" % (self.id_task, self.mode_complete))
		datas = self.db.get_data_task(self.id_task, self.mode_complete)
		
		self.logger.info("[generate_csv] %d urls" % len(datas))
		
		for url_data in datas:
			self.metas = url_data
			self.metas['category'] = self.metas['categories'].split("@")[0]
			self.metas['manufacturer'] = self.metas['categories'].split("/")[-2]
			
			title_collection = get_title_collection(self.metas['title'], self.metas['category'], self.metas['manufacturer'])
			if title_collection != self.metas['title']:
				number_collection = get_number_collection(self.metas['title'], self.metas['id'], self.metas['category'])
				
				related = self.db.get_related(title_collection, number_collection, self.id_task)
				accesories =  self.db.get_accesories(title_collection, number_collection, self.id_task)
				
				#related
				if related:
					self.metas['related'] = ",".join([r for r in related if r != url_data['id']])
				
				#accesories
				if accesories:
					self.metas['accesories'] = ",".join([a for a in accesories if a != url_data['id']])
					
			
			self.print_line(self.get_metas_orderer())
		
		datas = self.db.get_data_task_removed(self.id_task)
		
		self.logger.info("[generate_csv] %d urls removed" % len(datas))
		
		for url_data in datas:
			self.metas = url_data
			self.metas['category'] = self.metas['categories'].split("@")[0]
			self.metas['manufacturer'] = self.metas['categories'].split("/")[-2]
			
			title_collection = get_title_collection(self.metas['title'], self.metas['category'], self.metas['manufacturer'])
			if title_collection != self.metas['title']:
				number_collection = get_number_collection(self.metas['title'], self.metas['id'], self.metas['category'], )
				
				related = self.db.get_related(title_collection, number_collection, self.id_task)
				accesories =  self.db.get_accesories(title_collection, number_collection, self.id_task)
				
				#related
				if related:
					self.metas['related'] = ",".join(related)
				
				#accesories
				if accesories:
					self.metas['accesories'] = ",".join(accesories)
					
			#removed
			self.metas['extra_field_13'] = 1
			self.metas['stock'] =  0
			self.metas['instock_message'] = "Añadir a Lista de Espera"
			
			self.print_line(self.get_metas_orderer())
	
	def extract_products(self, year, week):
		"""extract all products of the page"""
		
		url = self.config['discover_url'] % (int(year), int(week))
		print url
		
		self.logger.info("[extract_products] recorriendo %s" % url)
		self.tree = etree.fromstring(self.download_url(url), self.parser)
		
		
		n_products = 0
		products = self.extracts('//div[@class="reg1"]')
		for product in products:
			try:
				subcategory, category = product.xpath("./a//node()")
			except ValueError:
				subcategory = "VARIOS"
				category = product.xpath("./a//node()")[0]
			
			self.extract_product("http://%s%s" % (self.config['domain'] , \
					product.xpath("./a/@href")[0]), category, subcategory)	
			
			n_products += 1
			
			
		return n_products
		
		
		
if __name__ == '__main__':
	
	
	if len(sys.argv) == 1:
		crawl = CrawlerComics()
		crawl.run()
	else:
		if "http" in sys.argv[1]:
			for url in sys.argv[1:]:
				crawl = CrawlerComics()
				crawl.extract_product(url, "a", "b")
				crawl.generate_csv()
			
				crawl.db.finish_task(crawl.id_task)
		else:
			crawl = CrawlerComics(id_task = sys.argv[1], mode = sys.argv[2])
			crawl.run()
			
	
		
		
