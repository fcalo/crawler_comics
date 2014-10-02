#!/usr/bin/env python
# -*- coding: utf-8 -*-

from warnings import filterwarnings
import MySQLdb
import MySQLdb.cursors
from MySQLdb import IntegrityError
from binascii import crc32
from pprint import pprint

from utils import get_title_collection, get_number_collection

filterwarnings('ignore', category = MySQLdb.Warning)


class DB(object):
	"""handle db access"""
	def __init__(self, logger, conf_file):
		
		self.logger = logger
		self.logger.info("[DB.__init__]")
		self.config = {}
		execfile(conf_file, self.config)
		
		self.con = MySQLdb.connect(self.config['mysql_host'], self.config['mysql_user'], self.config['mysql_pass'], cursorclass=MySQLdb.cursors.DictCursor)
		self.con.autocommit(True)

		#if not root fail:
		
		self.table_name= self.config['distributor'].replace(" ", "_").lower()
		
		self.cur = self.con.cursor()
		if self.config['create_model']:
			self.cur.execute("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET =utf8 " % self.config['mysql_db'])
		self.con.select_db(self.config['mysql_db'])
		self.cur = self.con.cursor()
		
		self.con.set_character_set('utf8')
		self.cur.execute('SET NAMES utf8;')
		self.cur.execute('SET CHARACTER SET utf8;')
		self.cur.execute('SET character_set_connection=utf8;')
		
		
		
	def init_model(self):
		
		self.logger.info("[DB.init_model]")
		
		if self.config['create_model']:
			
			#task table
			sql = "CREATE TABLE IF NOT EXISTS task \
			  ( id_task int(6) NOT NULL AUTO_INCREMENT PRIMARY KEY, \
			  start_date DATETIME, state INT(1), mode INT(1)) CHARACTER SET=utf8;"

			self.cur.execute(sql)
			  
			#url table
			sql = "CREATE TABLE IF NOT EXISTS %s ( \
			  url VARCHAR(255) PRIMARY KEY, \
			  crc_content int(4), \
			  last_seen_date datetime, \
			  last_seen_task int(6) , \
			  last_mod_task int(6) , %s, \
			  INDEX last_seen_task (last_seen_task) \
			  ) CHARACTER SET=utf8;" % \
			  (self.table_name, ",".join(["%s TEXT NULL" % k for k in self.config['csv_header']]))
			  
			self.cur.execute(sql)
			self.con.commit()
		
	def start_new_task(self):
		""" create new task and return id """
		
		self.logger.info("[DB.start_new_task]")
		self.cur.execute("INSERT INTO task (start_date, state) VALUES ( NOW(), 0);")
		_id = self.con.insert_id()
		
		self.con.commit()
		self.logger.info("[DB.start_new_task] creada tarea %d" % _id)
		return _id
		
	def create_auto_task(self, start_date, mode, type_task, week_day, hour):
		""" create new automatic task  """
		
		self.logger.info("[DB.create_auto_task]")
		self.cur.execute("INSERT INTO task \
		  (start_date, state, mode, type_task, week_day, hour) VALUES \
		  ( '%s', 0, '%s', '%s', '%s', '%s');" % (start_date, mode, type_task, week_day, hour))
		_id = self.con.insert_id()
		
		self.con.commit()
		self.logger.info("[DB.create_auto_task] creada tarea %d" % _id)
		return True
		
	def save_data(self, url, metas, id_task):
		""" save all data for url """
		
		self.logger.info("[DB.save_data] salvando datos para %s" % url)
		try:
			
			
			sql = u"INSERT INTO %s (url, crc_content, last_seen_date, last_seen_task, %s)\
			  VALUES ('%s', %d, NOW(), %d, '%s')" %  \
			  (self.table_name, ",".join([k for k in self.config['csv_header'] if k in metas]), \
			  self.escape(url), crc32(metas['content']), id_task, \
			  u"', '".join([self.escape(metas[k].decode("utf-8")) if isinstance(metas[k], basestring) else self.escape(str(metas[k])) for k in self.config['csv_header'] if k in metas]))
			  
		except Exception as e:
			
			try:
				print sql
			except:
				pass
			#~ for meta in metas.values():
				#~ print meta
				#~ if isinstance(meta, basestring):
					#~ print meta.decode("utf-8")
			
			pprint(metas)
			print repr(self.escape(url))
			raise e
		  
		  
		try:
			self.cur.execute(sql)
		except IntegrityError:
			#modification
			self.cur.execute("DELETE FROM %s WHERE url = '%s'" % (self.table_name, self.escape(url)))
			#one field more
			sql_mod = sql.replace(", last_seen_task,", ", last_seen_task, last_mod_task,")
			sql_mod = sql_mod.replace("NOW(), %d," % id_task, "NOW(), %d, %d," % (id_task, id_task)) 
			self.cur.execute(sql_mod)
		except Exception as e:
			try:
				print sql
			except:
				pass
			raise e
			
		return self.con.commit()
		  
	def refresh_seen(self, url, id_task):
		""" update last seen for url """
		
		self.logger.info("[DB.refresh_seen] %s" % url)
		
		self.cur.execute("UPDATE %s set last_seen_date = NOW(), last_seen_task = %d \
		  WHERE url = '%s'" % (self.table_name, id_task, self.escape(url)))
		
		return self.con.commit()
		
	def escape(self, url):
		
		return url.replace("'", "\\'") if isinstance(url, basestring) else url
		#~ try:
			#~ return self.con.escape_string(url)
		#~ except UnicodeEncodeError:
			#~ return self.con.escape_string(url.encode("utf-8"))
		
		
	def load_data(self, url):
		""" return data for url if exists"""
		
		self.logger.info("[DB.load_data] intentando recuperar data para %s" % url)
		
		metas = None
		try:
			sql = "SELECT * FROM %s WHERE url = '%s'" % (self.table_name, self.escape(url))
			
			self.cur.execute(sql)
		except:
			print sql
			raise
		
		try:
			data = self.cur.fetchall()[0]
		except IndexError:
			return False
		
		metas = {k:data[k] if isinstance(data[k], basestring) else data[k] \
		  for k in self.config['csv_header'] + ['crc_content'] if k in data and data[k]}
		
		return metas
		
	def get_data_task(self, id_task, complete):
		""" return all data collected by one task """
		self.logger.info("[DB.get_data_task] devolviendo datos de la tarea %s" % id_task)
		
		
		if complete:
			self.cur.execute("SELECT * FROM %s WHERE last_seen_task = %s ORDER BY last_seen_date ASC" % (self.table_name, id_task))
		else:
			self.cur.execute("SELECT * FROM %s WHERE last_mod_task = %s ORDER BY last_seen_date ASC" % (self.table_name, id_task))
			#TODO: deletes
			#self.cur.execute("SELECT * FROM url WHERE last_seen_task < %s ORDER BY last_seen_date ASC" % id_task)
			
		
		return self.cur.fetchall()
	
	def get_data_task_removed(self, id_task):
		""" return all data collected by one task """
		self.logger.info("[DB.get_data_task] devolviendo datos de los borrados de la tarea %s" % id_task)
		
		self.cur.execute("SELECT * FROM %s WHERE last_seen_task < (%s - 1) ORDER BY last_seen_date ASC" % (self.table_name, id_task))
		
		return self.cur.fetchall()
		
	
	def get_same_collection(self, title_collection, number_collection, id_task, asc = True):
		""" search the titles for the same collection """
		
		self.cur.execute("SELECT id, title, categories FROM %s WHERE title like '%s%%' and last_seen_task = '%s'" % (self.table_name, self.escape(title_collection), id_task))
		
		same = {}
		for data in self.cur.fetchall():
			if number_collection:
				n_collection = get_number_collection(data['title'], data['id'], data['categories'].split("@")[0])
				if (asc and n_collection > number_collection) or \
				 (not asc and n_collection < number_collection):
					same[n_collection] = data['id']
			else:
				same[len(same)] = data['id']
		
		if not same:
			return None
			
			
		sorted_keys = list(reversed(sorted(same))) if asc else sorted(same)
		
		return [same[k] for k in sorted_keys[:6]]
				
	
		
	def get_related(self, title_collection, number_collection, id_task):
		""" search the titles for the same collection """
		
		self.logger.info("[DB.get_related] buscando relacionados %s - %s(%s)" % (title_collection, number_collection, id_task))
		return self.get_same_collection(title_collection, number_collection, id_task, True)
	
	def get_accesories(self, title_collection, number_collection, id_task):
		""" search the titles for the same collection """
		
		self.logger.info("[DB.get_related] buscando accesories %s - %s(%s)" % (title_collection, number_collection, id_task))
		return self.get_same_collection(title_collection, number_collection, id_task, False)
		
		
	def init_task(self, id_task):
		""" change state to running (1) to task """
		
		self.logger.info("[DB.init_task] %s" % id_task)
		
		self.cur.execute("UPDATE task set state=1 WHERE id_task = '%s'" % (id_task))
		
		return self.con.commit()
		
	def finish_task(self, id_task, with_errors = False):
		""" change state to finised with errors (2) or finised without errors to task """
		
		self.logger.info("[DB.finish_task] %s con errores : %s" % (id_task , with_errors))
		
		self.cur.execute("UPDATE task set state=%d WHERE id_task = '%s'" % (2 if with_errors else 3, id_task))
		
		return self.con.commit()
		
	def init_newsletter(self, id_newsletter):
		""" change state to running (1) to newsletter """
		
		self.logger.info("[DB.init_newsletter] %s" % id_newsletter)
		
		self.cur.execute("UPDATE newsletter set state=1 WHERE id_newsletter = '%s'" % (id_newsletter))
		
		return self.con.commit()
		
		
	def finish_newsletter(self, id_newsletter, with_errors = False):
		""" change state to finised with errors (2) or finised without errors to task """
		
		self.logger.info("[DB.finish_newsletter] %s con errores : %s" % (id_newsletter , with_errors))
		
		self.cur.execute("UPDATE newsletter set state=%d WHERE id_newsletter = '%s'" % (2 if with_errors else 3, id_newsletter))
		
		return self.con.commit()
		
		
	def get_waiting_task(self):
		""" search for the task in state waiting """
		
		self.cur.execute("SELECT t.*,tt.scriptname file  FROM  task t \
		  INNER JOIN type_task tt ON tt.id_type_task = t.type_task \
		  WHERE state = 0 AND start_date < NOW() \
          AND NOT EXISTS (SELECT 1 FROM task \
		  WHERE type_task = t.type_task AND state = 1)")
		try:
			return self.cur.fetchall()[0]
		except IndexError:
			return None
			
	def get_waiting_newsletter(self):
		""" search for the newsletter in state waiting """
		
		self.cur.execute("SELECT id_newsletter  FROM  newsletter n \
		  WHERE state = 0 ORDER BY id_newsletter limit 1")
		try:
			return self.cur.fetchall()[0]
		except IndexError:
			return None
			
	def get_data_supplier(self, supplier):
		""" return all data from one supplier table """
		
		self.cur.execute("SELECT * FROM %s order by id, last_seen_task DESC" % supplier)
		
		return self.cur.fetchall()
		
	def get_last_task_supplier(self, supplier):
		""" return all data from one supplier table """
		
		self.cur.execute("SELECT max(last_seen_task) task FROM %s" % supplier)
		
		return self.cur.fetchall()[0]['task']
	
	def get_name_supplier(self, supplier):
		""" return all data from one supplier table """
		
		self.cur.execute("SELECT name FROM supplier where `table`='%s'" % supplier)
		
		return self.cur.fetchall()[0]['name']
	
	
	def get_info_newsletter(self, id_newsletter):
		""" return all data from one supplier table """
		
		self.cur.execute("SELECT * FROM newsletter where id_newsletter='%s'" % id_newsletter)
		try:
			data_newsletter = self.cur.fetchall()[0]
		except IndexError:
			data_newsletter = None
		
		if data_newsletter:
			
			self.cur.execute("SELECT category FROM newsletter_category where id_newsletter='%s'" % id_newsletter)
		
			data_newsletter['categories'] = []
			for cat in self.cur.fetchall():
				data_newsletter['categories'].append(cat['category'])
				
		
		#~ return {d:data_newsletter[d].encode("utf-8") 
		  #~ if isinstance(data_newsletter[d], basestring) else data_newsletter[d]
		  #~ for d in data_newsletter}
		  
		return data_newsletter
		
	def add_category(self, category):
		
		return self.cur.execute("INSERT IGNORE INTO category VALUES ( '%s');" % category)
	
		
		
		
	
		
		
	
		
		
